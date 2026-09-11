// Copyright 2025 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package template

import (
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
	"github.com/google/cel-go/common/types/traits"
	"github.com/google/cel-go/interpreter"
)

// errBreak and errContinue unwind the node walk for the {{ break }} and {{ continue }}
// statements. They never escape the loop which handles them.
var (
	errBreak    = errors.New("break")
	errContinue = errors.New("continue")
	// ErrOutputLimit is returned when a render exceeds the configured output size limit.
	ErrOutputLimit = errors.New("template: output size limit exceeded")
)

// renderState is the per-render context shared by a template and the sub-templates it calls.
type renderState struct {
	cfg *config
	out *limitWriter
}

// limitWriter caps the total number of bytes a single render may produce.
type limitWriter struct {
	w         io.Writer
	remaining int
	limited   bool
}

func newLimitWriter(w io.Writer, limit int) *limitWriter {
	return &limitWriter{w: w, remaining: limit, limited: limit > 0}
}

func (l *limitWriter) WriteString(s string) error {
	if l.limited {
		if len(s) > l.remaining {
			return ErrOutputLimit
		}
		l.remaining -= len(s)
	}
	_, err := io.WriteString(l.w, s)
	return err
}

// Render writes the template to w using vars as the input to its declared variables.
//
// vars may be a map[string]any, a cel.Activation, or any value accepted by the CEL
// interpreter. Rendering is safe for concurrent use.
func (t *Template) Render(w io.Writer, vars any) error {
	act, err := interpreter.NewActivation(orEmpty(vars))
	if err != nil {
		return fmt.Errorf("template %q: %w", t.name, err)
	}
	st := &renderState{cfg: t.cfg, out: newLimitWriter(w, t.cfg.maxOutput)}
	if err := t.renderBody(st, t.body, act); err != nil {
		return unwrapJump(t, err)
	}
	return nil
}

// RenderString renders the template to a string.
func (t *Template) RenderString(vars any) (string, error) {
	var sb strings.Builder
	if err := t.Render(&sb, vars); err != nil {
		return "", err
	}
	return sb.String(), nil
}

// renderNested renders a sub-template invoked from a CEL expression. Sub-templates are
// hermetic: only the declared parameters are in scope.
func (t *Template) renderNested(args []ref.Val) (string, error) {
	bindings := make(map[string]any, len(args))
	for i, p := range t.params {
		if i < len(args) {
			bindings[p.name] = args[i]
		}
	}
	var sb strings.Builder
	st := &renderState{cfg: t.cfg, out: newLimitWriter(&sb, t.cfg.maxOutput)}
	if err := t.renderBody(st, t.body, &varActivation{vars: bindings}); err != nil {
		return "", fmt.Errorf("in {{ define %s }}: %w", t.name, unwrapJump(t, err))
	}
	return sb.String(), nil
}

// unwrapJump converts a break or continue which escaped its loop into a diagnosable error.
// The compiler rejects these statically, so reaching this point indicates a bug.
func unwrapJump(t *Template, err error) error {
	if errors.Is(err, errBreak) || errors.Is(err, errContinue) {
		return fmt.Errorf("template %q: %w outside of a loop", t.name, err)
	}
	return err
}

func orEmpty(vars any) any {
	if vars == nil {
		return map[string]any{}
	}
	return vars
}

// renderBody walks a block, writing its output and threading variable bindings introduced by
// let statements through the remaining siblings.
func (t *Template) renderBody(st *renderState, body []node, act interpreter.Activation) error {
	for _, n := range body {
		switch n := n.(type) {
		case *textNode:
			if err := st.out.WriteString(n.text); err != nil {
				return err
			}
		case *interpNode:
			if err := t.renderInterp(st, n, act); err != nil {
				return err
			}
		case *ifNode:
			if err := t.renderIf(st, n, act); err != nil {
				return err
			}
		case *forNode:
			if err := t.renderFor(st, n, act); err != nil {
				return err
			}
		case *letNode:
			val, err := t.eval(n.expr, act)
			if err != nil {
				return err
			}
			act = &varActivation{parent: act, vars: map[string]any{n.name: val}}
		case *jumpNode:
			if n.isContinue {
				return errContinue
			}
			return errBreak
		}
	}
	return nil
}

func (t *Template) renderInterp(st *renderState, n *interpNode, act interpreter.Activation) error {
	val, err := t.eval(n.expr, act)
	if err != nil {
		return err
	}
	text, err := formatValue(val)
	if err != nil {
		return renderErrorf(t.source, n.loc, err, "%v", err)
	}
	if st.cfg.escaper != nil && !n.raw {
		text = st.cfg.escaper(text)
	}
	return st.out.WriteString(text)
}

func (t *Template) renderIf(st *renderState, n *ifNode, act interpreter.Activation) error {
	for _, arm := range n.arms {
		val, err := t.eval(arm.cond, act)
		if err != nil {
			return err
		}
		b, ok := val.(types.Bool)
		if !ok {
			return renderErrorf(t.source, arm.cond.loc, nil,
				"condition evaluated to %s, expected a bool", val.Type())
		}
		if bool(b) {
			return t.renderBody(st, arm.body, act)
		}
	}
	return t.renderBody(st, n.alt, act)
}

func (t *Template) renderFor(st *renderState, n *forNode, act interpreter.Activation) error {
	val, err := t.eval(n.iter, act)
	if err != nil {
		return err
	}
	switch iter := val.(type) {
	case traits.Mapper:
		return t.rangeMap(st, n, act, iter)
	case traits.Lister:
		return t.rangeList(st, n, act, iter)
	default:
		return renderErrorf(t.source, n.iter.loc, nil,
			"cannot range over %s; {{ for }} requires a list or a map", val.Type())
	}
}

func (t *Template) rangeList(st *renderState, n *forNode, act interpreter.Activation, list traits.Lister) error {
	size, ok := list.Size().(types.Int)
	if !ok {
		return renderErrorf(t.source, n.iter.loc, nil, "cannot determine the size of the range expression")
	}
	if size == 0 {
		return t.renderBody(st, n.alt, act)
	}
	for i := types.Int(0); i < size; i++ {
		vars := map[string]any{}
		if len(n.vars) == 1 {
			vars[n.vars[0]] = list.Get(i)
		} else {
			vars[n.vars[0]] = i
			vars[n.vars[1]] = list.Get(i)
		}
		stop, err := t.renderIteration(st, n, &varActivation{parent: act, vars: vars})
		if err != nil {
			return err
		}
		if stop {
			break
		}
	}
	return nil
}

func (t *Template) rangeMap(st *renderState, n *forNode, act interpreter.Activation, m traits.Mapper) error {
	var keys []ref.Val
	it := m.Iterator()
	for it.HasNext() == types.True {
		keys = append(keys, it.Next())
	}
	if len(keys) == 0 {
		return t.renderBody(st, n.alt, act)
	}
	// Maps have no intrinsic order, so iterate keys in sorted order to keep output stable.
	sortValues(keys)
	for _, k := range keys {
		vars := map[string]any{n.vars[0]: k}
		if len(n.vars) == 2 {
			vars[n.vars[1]] = m.Get(k)
		}
		stop, err := t.renderIteration(st, n, &varActivation{parent: act, vars: vars})
		if err != nil {
			return err
		}
		if stop {
			break
		}
	}
	return nil
}

// renderIteration renders one pass of a loop body, reporting whether the loop should stop.
func (t *Template) renderIteration(st *renderState, n *forNode, act interpreter.Activation) (bool, error) {
	err := t.renderBody(st, n.body, act)
	switch {
	case err == nil:
		return false, nil
	case errors.Is(err, errBreak):
		return true, nil
	case errors.Is(err, errContinue):
		return false, nil
	default:
		return false, err
	}
}

// eval runs a compiled expression, converting CEL errors into located template errors.
func (t *Template) eval(e *expression, act interpreter.Activation) (ref.Val, error) {
	val, _, err := e.prg.Eval(act)
	if err != nil {
		return nil, renderErrorf(t.source, e.loc, err, "%v", err)
	}
	if types.IsUnknown(val) {
		return nil, renderErrorf(t.source, e.loc, nil, "expression produced an unknown value")
	}
	return val, nil
}

// varActivation binds a small, fixed set of names on top of a parent activation.
type varActivation struct {
	parent interpreter.Activation
	vars   map[string]any
}

// ResolveName looks up only the names bound by this activation; the interpreter walks to the
// parent on its own.
func (a *varActivation) ResolveName(name string) (any, bool) {
	v, found := a.vars[name]
	return v, found
}

func (a *varActivation) Parent() interpreter.Activation {
	return a.parent
}
