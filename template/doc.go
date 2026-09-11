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

// Package template implements a text templating language which uses CEL as its expression
// language.
//
// Templates are plain text with `{{ ... }}` actions embedded in them, in the tradition of Go's
// text/template, Jinja, and Helm. What differs is that every expression inside an action is a
// CEL expression, checked against a declared type environment when the template is compiled.
// A template that compiles has no undeclared variables, no misspelled fields, no type
// mismatches, and no calls to functions that do not exist, for any input that matches the
// declared types.
//
// # Actions
//
//	{{ expr }}                      evaluate expr and write it to the output
//	{{# a comment }}                discarded
//	{{ if expr }} … {{ end }}       conditionals, with {{ else if expr }} and {{ else }}
//	{{ for x in xs }} … {{ end }}   iteration, with an {{ else }} arm for an empty range
//	{{ break }} / {{ continue }}    loop control
//	{{ let name = expr }}           bind a value for the remainder of the enclosing block
//	{{ define name(p T) }} … {{ end }}
//	                                declare a sub-template
//
// # Sub-templates are functions
//
// A {{ define }} declares a named, parameterized sub-template with typed parameters, and
// registers it in the CEL environment as a function returning the rendered string. Calling a
// sub-template is therefore an ordinary expression:
//
//	{{ define row(name string, qty int) }}
//	- name: {{ name }}
//	  qty: {{ qty }}
//	{{ end }}
//
//	items:{{ for i in order.items }}{{ row(i.name, i.qty) }}{{ end }}
//
// Because a call is an expression, sub-templates compose with everything else the language
// offers: they can be indented into a surrounding document with nindent, joined inside a
// comprehension, or passed through any other CEL function.
//
//	spec:{{ toYAML(config).nindent(2) }}
//	{{ order.items.map(i, row(i.name, i.qty)).join("") }}
//
// Sub-templates are hermetic. They see their declared parameters and nothing else, so a
// sub-template cannot silently depend on a variable that happens to be in scope at one of its
// call sites. Because their call graph is required to be acyclic and CEL has no unbounded
// loops, rendering always terminates.
//
// # Iteration
//
// The one-variable form of {{ for }} binds list elements or map keys. The two-variable form
// binds a list index and element, or a map key and value:
//
//	{{ for name in names }}…{{ end }}
//	{{ for i, name in names }}…{{ end }}
//	{{ for key, value in labels }}…{{ end }}
//
// Map iteration visits keys in sorted order, so rendering the same data twice always produces
// the same bytes.
//
// # Typing
//
// Conditions must be bool. There is no implicit truthiness, so a non-empty string or a
// non-zero number is not a condition; write the comparison out. A missing map key or struct
// field is an error at render time rather than an empty string in the output. Optional values
// make an intentionally absent value explicit:
//
//	{{ if has(cfg.replicas) }}replicas: {{ cfg.replicas }}{{ end }}
//	replicas: {{ cfg.?replicas.orValue(1) }}
//
// # Whitespace
//
// A line containing nothing but a single control action, such as {{ if }} or {{ end }}, is
// removed from the output along with its newline. Disable this with StandaloneLines(false).
// The explicit markers {{- and -}} trim all whitespace to the left and right of an action, and
// are always available.
//
// # Determinism and sandboxing
//
// A template is a pure function of its inputs. There is no now(), no random(), and no way to
// read a file or reach the network, so a given template and a given input always produce the
// same output, which is what makes rendered output safe to commit, diff, and review.
//
// Evaluation is bounded: CostLimit bounds the work each expression performs, MaxOutputBytes
// bounds the size of the rendered document, and the acyclic call graph bounds expansion. A
// template can only call the functions its environment declares.
//
// # Example
//
//	tmpl, err := template.Compile("greeting.tmpl",
//		`Hello {{ user.name }}{{ if user.admin }} (admin){{ end }}`,
//		template.Variable("user", cel.MapType(cel.StringType, cel.DynType)))
//	if err != nil {
//		return err
//	}
//	out, err := tmpl.RenderString(map[string]any{
//		"user": map[string]any{"name": "Ada", "admin": true},
//	})
package template
