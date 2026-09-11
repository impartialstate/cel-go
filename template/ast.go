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
	"strings"

	"github.com/google/cel-go/cel"
)

// node is a single element of a parsed template body.
type node interface {
	at() location
}

// expression is an embedded CEL expression along with the compiled program produced for it.
type expression struct {
	loc location
	src string
	ast *cel.Ast
	prg cel.Program
	typ *cel.Type
}

// textNode is a run of literal template text.
type textNode struct {
	loc  location
	text string
}

func (n *textNode) at() location { return n.loc }

// interpNode is a `{{ expr }}` action whose value is formatted into the output.
type interpNode struct {
	loc  location
	expr *expression
	// raw suppresses the configured escaper, either because the expression is wrapped in
	// raw() or because it is a direct call to a defined template.
	raw bool
}

func (n *interpNode) at() location { return n.loc }

// condNode is a single `if` or `else if` arm.
type condNode struct {
	loc  location
	cond *expression
	body []node
}

// ifNode is an `{{ if }} ... {{ else }} ... {{ end }}` chain.
type ifNode struct {
	loc  location
	arms []*condNode
	// alt is the `else` body, or nil when the chain has no else arm.
	alt []node
}

func (n *ifNode) at() location { return n.loc }

// forNode is a `{{ for x in xs }} ... {{ end }}` loop.
type forNode struct {
	loc location
	// vars holds one or two loop variable names. For a list the one-variable form binds the
	// element and the two-variable form binds the index and the element; for a map the
	// one-variable form binds the key and the two-variable form binds the key and the value.
	vars []string
	iter *expression
	body []node
	// alt is the `else` body evaluated when the range is empty, or nil when absent.
	alt []node
	// kind records what the range expression iterates, as resolved by the type checker.
	kind iterKind
}

func (n *forNode) at() location { return n.loc }

// letNode binds a name to the value of an expression for the remainder of the enclosing block.
type letNode struct {
	loc  location
	name string
	expr *expression
}

func (n *letNode) at() location { return n.loc }

// jumpNode is a `break` or `continue` statement.
type jumpNode struct {
	loc        location
	isContinue bool
}

func (n *jumpNode) at() location { return n.loc }

// param is a single declared parameter of a defined template.
type param struct {
	loc      location
	name     string
	typeName string
	typ      *cel.Type
}

// define is a named, parameterized sub-template. Each define is exposed to CEL as a function
// which returns the rendered string, so sub-templates compose with the rest of the expression
// language rather than forming a separate call mechanism.
type define struct {
	loc    location
	source string
	name   string
	params []param
	body   []node
	// calls lists the names of other defines reachable from this body, used to reject cycles.
	calls []string
	tmpl  *Template
}

// signature renders the define as it would be declared, for use in diagnostics.
func (d *define) signature() string {
	var sb strings.Builder
	sb.WriteString(d.name)
	sb.WriteString("(")
	for i, p := range d.params {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(p.name)
		sb.WriteString(" ")
		sb.WriteString(p.typeName)
	}
	sb.WriteString(")")
	return sb.String()
}
