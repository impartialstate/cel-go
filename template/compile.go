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
	"fmt"
	"sort"
	"strings"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/env"
	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"

	celast "github.com/google/cel-go/common/ast"
)

// iterKind records what a `for` action ranges over, as determined by the type checker.
type iterKind int

const (
	iterList iterKind = iota
	iterMap
	// iterDynamic is used when the range expression is dyn-typed and the shape can only be
	// established at render time.
	iterDynamic
)

// compiler carries the state of a single Compile call.
type compiler struct {
	cfg     *config
	baseEnv *cel.Env
	rootEnv *cel.Env
	defines map[string]*define
	order   []*define
	errs    *Errors
}

// scope is the compile-time environment for a block, tracking the CEL environment along with
// the structural context needed to validate statements.
type scope struct {
	env      *cel.Env
	source   string
	loopings int
}

func (s scope) with(env *cel.Env) scope {
	s.env = env
	return s
}

func (s scope) inLoop() scope {
	s.loopings++
	return s
}

// compileSet type-checks every parsed source and produces a renderable Set.
func compileSet(cfg *config, baseEnv *cel.Env, sources []*parsedSource) (*Set, error) {
	c := &compiler{cfg: cfg, baseEnv: baseEnv, defines: map[string]*define{}, errs: &Errors{}}
	c.collectDefines(sources)
	funcEnv, err := c.declareDefines()
	if err != nil {
		return nil, err
	}
	rootEnv, err := funcEnv.Extend(cfg.varOpts...)
	if err != nil {
		return nil, fmt.Errorf("template: invalid variable declarations: %w", err)
	}
	c.rootEnv = rootEnv
	// Sub-template bodies are hermetic: they see their declared parameters and the shared
	// function namespace, but never the caller's variables.
	for _, d := range c.order {
		paramEnv, err := funcEnv.Extend(paramDecls(d)...)
		if err != nil {
			c.errs.report(d.source, d.loc, "invalid parameter declarations: %v", err)
			continue
		}
		sc := scope{env: paramEnv, source: d.source}
		c.compileBody(sc, d.body)
		d.tmpl = &Template{name: d.name, source: d.source, body: d.body, params: d.params, cfg: cfg}
		d.calls = c.calledDefines(d.body)
	}
	c.checkCycles()
	set := &Set{cfg: cfg, templates: map[string]*Template{}, env: rootEnv}
	for _, ps := range sources {
		sc := scope{env: rootEnv, source: ps.name}
		c.compileBody(sc, ps.body)
		set.templates[ps.name] = &Template{name: ps.name, source: ps.name, body: ps.body, cfg: cfg}
		set.names = append(set.names, ps.name)
	}
	if !c.errs.empty() {
		return nil, c.errs.sorted()
	}
	return set, nil
}

// collectDefines indexes every define in the set, rejecting duplicates.
func (c *compiler) collectDefines(sources []*parsedSource) {
	for _, ps := range sources {
		for _, d := range ps.defines {
			if prev, found := c.defines[d.name]; found {
				c.errs.report(d.source, d.loc, "template %q is already defined at %s:%s",
					d.name, prev.source, prev.loc)
				continue
			}
			c.defines[d.name] = d
			c.order = append(c.order, d)
		}
	}
}

// declareDefines resolves parameter types and extends the base environment with one CEL
// function per define. Declaring all of them up front means a define may reference any other
// define regardless of source or declaration order.
func (c *compiler) declareDefines() (*cel.Env, error) {
	provider := c.baseEnv.CELTypeProvider()
	opts := make([]cel.EnvOption, 0, len(c.order))
	for _, d := range c.order {
		ok := true
		for i := range d.params {
			p := &d.params[i]
			t, err := parseTypeName(provider, p.typeName)
			if err != nil {
				c.errs.report(d.source, d.loc, "parameter %q of template %q: %v", p.name, d.name, err)
				ok = false
				continue
			}
			p.typ = t
		}
		if !ok {
			continue
		}
		opts = append(opts, defineFunction(d))
	}
	funcEnv, err := c.baseEnv.Extend(opts...)
	if err != nil {
		c.errs.report("", location{line: 1, col: 1}, "unable to declare templates: %v", err)
		return nil, c.errs.sorted()
	}
	return funcEnv, nil
}

// defineFunction exposes a define to CEL as a function returning the rendered string. The
// binding closes over the define, whose body is filled in later in compilation, which is what
// lets templates call each other in any order.
func defineFunction(d *define) cel.EnvOption {
	args := make([]*cel.Type, len(d.params))
	for i, p := range d.params {
		args[i] = p.typ
	}
	overloadID := "template_" + d.name
	binding := func(vals ...ref.Val) ref.Val {
		if d.tmpl == nil {
			return types.NewErr("template %q is not compiled", d.name)
		}
		out, err := d.tmpl.renderNested(vals)
		if err != nil {
			return types.WrapErr(err)
		}
		return types.String(out)
	}
	return cel.Function(d.name,
		cel.Overload(overloadID, args, cel.StringType, cel.FunctionBinding(binding)))
}

func paramDecls(d *define) []cel.EnvOption {
	opts := make([]cel.EnvOption, 0, len(d.params))
	for _, p := range d.params {
		opts = append(opts, cel.Variable(p.name, p.typ))
	}
	return opts
}

// parseTypeName converts a CEL type specifier such as `list<string>` into a checked type.
func parseTypeName(provider types.Provider, name string) (*cel.Type, error) {
	td, err := env.ParseTypeDesc(name)
	if err != nil {
		return nil, err
	}
	t, err := td.AsCELType(provider)
	if err != nil {
		return nil, err
	}
	return t, nil
}

// compileBody type-checks a block. Statements such as `let` extend the environment for their
// following siblings only, so the environment is threaded through the loop rather than shared.
func (c *compiler) compileBody(sc scope, body []node) {
	for _, n := range body {
		switch n := n.(type) {
		case *textNode:
		case *interpNode:
			if !c.compileExpr(sc, n.expr) {
				continue
			}
			n.raw = c.isRawExpr(n.expr)
		case *ifNode:
			for _, arm := range n.arms {
				if c.compileExpr(sc, arm.cond) {
					c.requireBool(sc, arm.cond)
				}
				c.compileBody(sc, arm.body)
			}
			c.compileBody(sc, n.alt)
		case *forNode:
			c.compileFor(sc, n)
		case *letNode:
			if !c.compileExpr(sc, n.expr) {
				continue
			}
			next, err := sc.env.Extend(cel.Variable(n.name, n.expr.typ))
			if err != nil {
				c.errs.report(sc.source, n.loc, "unable to bind %q: %v", n.name, err)
				continue
			}
			sc = sc.with(next)
		case *jumpNode:
			if sc.loopings == 0 {
				kw := "break"
				if n.isContinue {
					kw = "continue"
				}
				c.errs.report(sc.source, n.loc, "{{ %s }} is only valid inside {{ for }}", kw)
			}
		}
	}
}

// compileFor type-checks a range action and binds its loop variables.
func (c *compiler) compileFor(sc scope, n *forNode) {
	if !c.compileExpr(sc, n.iter) {
		return
	}
	var decls []cel.EnvOption
	switch t := n.iter.typ; t.Kind() {
	case types.ListKind:
		elem := t.Parameters()[0]
		switch len(n.vars) {
		case 1:
			decls = []cel.EnvOption{cel.Variable(n.vars[0], elem)}
		case 2:
			decls = []cel.EnvOption{cel.Variable(n.vars[0], cel.IntType), cel.Variable(n.vars[1], elem)}
		}
	case types.MapKind:
		n.kind = iterMap
		key, val := t.Parameters()[0], t.Parameters()[1]
		switch len(n.vars) {
		case 1:
			decls = []cel.EnvOption{cel.Variable(n.vars[0], key)}
		case 2:
			decls = []cel.EnvOption{cel.Variable(n.vars[0], key), cel.Variable(n.vars[1], val)}
		}
	case types.DynKind, types.AnyKind:
		n.kind = iterDynamic
		for _, v := range n.vars {
			decls = append(decls, cel.Variable(v, cel.DynType))
		}
	default:
		c.errs.report(sc.source, n.iter.loc,
			"cannot range over %s; {{ for }} requires a list or a map", t)
		return
	}
	next, err := sc.env.Extend(decls...)
	if err != nil {
		c.errs.report(sc.source, n.loc, "unable to bind loop variables: %v", err)
		return
	}
	c.compileBody(sc.with(next).inLoop(), n.body)
	c.compileBody(sc, n.alt)
}

// requireBool rejects the implicit truthiness other template languages allow, since silently
// treating a non-empty string or a non-zero number as true is a common source of template bugs.
func (c *compiler) requireBool(sc scope, e *expression) {
	switch e.typ.Kind() {
	case types.BoolKind, types.DynKind, types.AnyKind:
		return
	}
	hint := ""
	switch e.typ.Kind() {
	case types.StringKind, types.BytesKind:
		hint = fmt.Sprintf("; did you mean %s != %q", strings.TrimSpace(e.src), "")
	case types.ListKind, types.MapKind:
		hint = fmt.Sprintf("; did you mean size(%s) > 0", strings.TrimSpace(e.src))
	case types.IntKind, types.UintKind, types.DoubleKind:
		hint = fmt.Sprintf("; did you mean %s != 0", strings.TrimSpace(e.src))
	case types.OpaqueKind:
		if e.typ.TypeName() == "optional_type" {
			hint = fmt.Sprintf("; did you mean %s.hasValue()", strings.TrimSpace(e.src))
		}
	}
	c.errs.report(sc.source, e.loc, "condition must be a bool, found %s%s", e.typ, hint)
}

// compileExpr type-checks an embedded expression and plans its program.
func (c *compiler) compileExpr(sc scope, e *expression) bool {
	ast, issues := sc.env.Compile(e.src)
	if issues != nil && issues.Err() != nil {
		c.errs.reportIssues(sc.source, e.loc, issues)
		return false
	}
	prg, err := sc.env.Program(ast, c.cfg.progOpts...)
	if err != nil {
		c.errs.report(sc.source, e.loc, "program construction error: %v", err)
		return false
	}
	e.ast = ast
	e.prg = prg
	e.typ = ast.OutputType()
	return true
}

// isRawExpr reports whether an interpolation should bypass the configured escaper. Rendering a
// sub-template produces markup which has already been escaped, and raw() is the explicit
// opt-out for everything else.
func (c *compiler) isRawExpr(e *expression) bool {
	call := e.ast.NativeRep().Expr()
	if call.Kind() != celast.CallKind {
		return false
	}
	name := call.AsCall().FunctionName()
	if name == rawFunc {
		return true
	}
	_, isTemplate := c.defines[name]
	return isTemplate
}

// calledDefines lists the sub-templates reachable from a body's expressions.
func (c *compiler) calledDefines(body []node) []string {
	seen := map[string]bool{}
	var out []string
	visit := func(e *expression) {
		if e == nil || e.ast == nil {
			return
		}
		celast.PostOrderVisit(e.ast.NativeRep().Expr(), celast.NewExprVisitor(func(ex celast.Expr) {
			if ex.Kind() != celast.CallKind {
				return
			}
			name := ex.AsCall().FunctionName()
			if _, found := c.defines[name]; found && !seen[name] {
				seen[name] = true
				out = append(out, name)
			}
		}))
	}
	walkExprs(body, visit)
	sort.Strings(out)
	return out
}

// walkExprs applies fn to every expression in a body, including nested blocks.
func walkExprs(body []node, fn func(*expression)) {
	for _, n := range body {
		switch n := n.(type) {
		case *interpNode:
			fn(n.expr)
		case *letNode:
			fn(n.expr)
		case *ifNode:
			for _, arm := range n.arms {
				fn(arm.cond)
				walkExprs(arm.body, fn)
			}
			walkExprs(n.alt, fn)
		case *forNode:
			fn(n.iter)
			walkExprs(n.body, fn)
			walkExprs(n.alt, fn)
		}
	}
}

// checkCycles rejects mutually recursive templates. CEL itself has no unbounded loops, so
// keeping the call graph acyclic preserves the guarantee that rendering always terminates.
func (c *compiler) checkCycles() {
	const (
		white = 0
		grey  = 1
		black = 2
	)
	state := map[string]int{}
	var stack []string
	var visit func(name string) bool
	visit = func(name string) bool {
		switch state[name] {
		case black:
			return false
		case grey:
			cycle := append(append([]string{}, stack[indexOf(stack, name):]...), name)
			d := c.defines[name]
			c.errs.report(d.source, d.loc, "template %q is recursive: %s", name,
				strings.Join(cycle, " -> "))
			return true
		}
		state[name] = grey
		stack = append(stack, name)
		for _, callee := range c.defines[name].calls {
			if visit(callee) {
				state[name] = black
				stack = stack[:len(stack)-1]
				return true
			}
		}
		state[name] = black
		stack = stack[:len(stack)-1]
		return false
	}
	for _, d := range c.order {
		visit(d.name)
	}
}

func indexOf(names []string, name string) int {
	for i, n := range names {
		if n == name {
			return i
		}
	}
	return 0
}
