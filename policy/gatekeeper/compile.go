// Copyright 2025 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package gatekeeper

import (
	"context"
	"fmt"
	"os"
	"reflect"

	"cel.dev/cel-go/cel"
	"cel.dev/cel-go/common"
	"cel.dev/cel-go/common/types"
	"cel.dev/cel-go/common/types/ref"
	"cel.dev/cel-go/policy"
)

// Template is the compiled CEL of a Gatekeeper ConstraintTemplate, ready to
// review admission requests.
type Template struct {
	name        string
	description string
	policy      *policy.Policy
	env         *cel.Env
	ast         *cel.Ast
	program     cel.Program
	config      *config
}

// Violation is a message reported by a validation which the reviewed object
// did not satisfy.
type Violation struct {
	// Message is the result of the validation's `message` or
	// `messageExpression`.
	Message string
}

// Review is an admission request to evaluate a template against. Every field is
// optional, and a field which is not set is null during evaluation, as it is in
// a cluster.
type Review struct {
	// Object is the object being admitted, bound to `object`. It is null for a
	// DELETE request.
	Object any

	// OldObject is the object as it exists in the cluster, bound to
	// `oldObject`. It is null for a CREATE request.
	OldObject any

	// Request is the attributes of the admission request, bound to `request`.
	Request any

	// NamespaceObject is the namespace of the object under review, bound to
	// `namespaceObject`. It is null for cluster-scoped objects.
	NamespaceObject any

	// Constraint is the constraint resource which instantiates the template,
	// bound to `params`. Its spec.parameters is what `variables.params` holds.
	Constraint any

	// Parameters is the parameters of the constraint under review. It is a
	// convenience for tests, which usually care about the parameters rather
	// than the constraint which carries them, and is ignored when Constraint is
	// set.
	Parameters any
}

// CompileFile reads and compiles a ConstraintTemplate from a file.
func CompileFile(path string, opts ...Option) (*Template, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return Compile(policy.ByteSource(content, path), opts...)
}

// Compile parses and compiles the CEL of a ConstraintTemplate.
//
// Errors are reported against the position of the expression within the
// template, so a mistake points at the line and column which contains it:
//
//	ERROR: template.yaml:31:24: undefined field 'lables'
//	 |               expression: "variables.params.lables"
//	 | .......................^
func Compile(src *policy.Source, opts ...Option) (*Template, error) {
	c := newConfig(opts...)
	parser, err := policy.NewParser(ParserOption(opts...))
	if err != nil {
		return nil, err
	}
	p, iss := parser.Parse(src)
	if iss.Err() != nil {
		return nil, iss.Err()
	}
	schema, _ := ParamsSchema(p)
	env, err := cel.NewEnv(append(environmentOptions(c), inputOptions(c, p.Name().Value, schema)...)...)
	if err != nil {
		return nil, err
	}
	// The validations of a template are composed into one expression which
	// yields the messages of those the object fails, so a review reports every
	// violation rather than stopping at the first, as Gatekeeper does.
	ast, iss := policy.Compile(env, p)
	if iss.Err() != nil {
		return nil, compileError(p, iss)
	}
	program, err := env.Program(ast, c.programOptions()...)
	if err != nil {
		return nil, err
	}
	return &Template{
		name:        p.Name().Value,
		description: p.Description().Value,
		policy:      p,
		env:         env,
		ast:         ast,
		program:     program,
		config:      c,
	}, nil
}

// compileError renders the mistakes in a template, dropping the ones the
// compiler reports more than once for the same expression: a policy author
// needs one message per mistake, at the position which holds it.
func compileError(p *policy.Policy, iss *cel.Issues) error {
	errs := common.NewErrors(p.Source())
	seen := map[string]bool{}
	unique := make([]*common.Error, 0, len(iss.Errors()))
	for _, err := range iss.Errors() {
		key := fmt.Sprintf("%d:%d:%s", err.Location.Line(), err.Location.Column(), err.Message)
		if seen[key] {
			continue
		}
		seen[key] = true
		unique = append(unique, err)
	}
	return cel.NewIssues(errs.Append(unique)).Err()
}

// Name is the metadata.name of the ConstraintTemplate.
func (t *Template) Name() string {
	return t.name
}

// Description is the description annotation of the ConstraintTemplate.
func (t *Template) Description() string {
	return t.description
}

// Env is the CEL environment the template was compiled against.
func (t *Template) Env() *cel.Env {
	return t.env
}

// AST is the whole template compiled into a single CEL expression, which yields
// the messages of the validations an object fails. It is the form a policy
// engine would evaluate, and the form the CEL test runner measures coverage
// against.
func (t *Template) AST() *cel.Ast {
	return t.ast
}

// Policy is the parsed representation of the template.
func (t *Template) Policy() *policy.Policy {
	return t.policy
}

// ParamsSchema is the openAPIV3Schema which describes the parameters accepted
// by constraints of this template, and reports whether one was declared.
func (t *Template) ParamsSchema() (map[string]any, bool) {
	return ParamsSchema(t.policy)
}

// ValidateParams checks constraint parameters against the schema the template
// declares for them, returning one error per mismatch.
func (t *Template) ValidateParams(params any) []error {
	schema, found := t.ParamsSchema()
	if !found {
		return nil
	}
	return ValidateParams(schema, params)
}

// Review evaluates every validation of the template against an admission
// request, returning the violations the request did not satisfy.
//
// An empty result means the request is admitted.
func (t *Template) Review(ctx context.Context, r Review) ([]Violation, error) {
	// Evaluation runs asynchronous calls on their own goroutines, which are
	// abandoned when the review returns, so they are cancelled rather than left
	// running.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	// The lookups of a review are resolved once and shared by its validations,
	// so a policy which reads the same object from two validations reads it
	// once.
	data := newResolver(t.config.provider)
	vars := r.activation()
	vars[InventoryVar] = &inventoryValue{resolver: data}
	vars[ExternalDataVar] = &externalDataValue{resolver: data}
	vars[DataVar] = newDataValue(data)
	out, err := t.evaluate(ctx, data, vars)
	if err != nil {
		return nil, err
	}
	return violations(out)
}

// evaluate runs the validations of a template, resolving the referential data
// they read.
//
// A lookup made through the inventory functions is an asynchronous call: it
// runs on its own goroutine and the evaluator carries on with the rest of the
// expression meanwhile, so the lookups a policy makes overlap.
//
// The reads a policy makes through the data namespace are index operations
// rather than calls, which cannot be asynchronous. They yield an unknown value
// instead, which CEL propagates without failing the expression, so a pass
// gathers the reads the policy reached and the next one runs with the answers.
// A read the policy short-circuits past is never fetched, and one which depends
// on an earlier result resolves in a later pass.
func (t *Template) evaluate(ctx context.Context, data *resolver, vars map[string]any) (ref.Val, error) {
	for pass := 0; pass < t.config.maxRounds; pass++ {
		result := <-t.program.ConcurrentEval(ctx, vars)
		if result.Err != nil {
			return nil, result.Err
		}
		if !types.IsUnknown(result.Val) {
			return result.Val, nil
		}
		if !data.hasPending() {
			// The policy cannot make progress: an unknown reached the result
			// without a read to resolve it.
			return nil, fmt.Errorf("evaluation produced an unresolved value")
		}
		if err := data.fetchPending(ctx); err != nil {
			return nil, err
		}
	}
	return nil, fmt.Errorf("referential data did not resolve after %d passes, "+
		"which means the policy's reads through the data namespace depend on each other "+
		"more deeply than MaxDataRounds allows", t.config.maxRounds)
}

// activation binds the variables of the Gatekeeper admission engine. Inputs
// which are not set are null, as they are in a cluster.
func (r Review) activation() map[string]any {
	constraint := r.Constraint
	if constraint == nil {
		// A constraint always exists in a cluster, so one is synthesized to
		// carry the parameters under review. An absent parameters field leaves
		// `variables.params` null, as it is for a constraint which sets none.
		spec := map[string]any{}
		if r.Parameters != nil {
			spec["parameters"] = r.Parameters
		}
		constraint = map[string]any{"spec": spec}
	}
	return map[string]any{
		ObjectVar:          r.Object,
		OldObjectVar:       r.OldObject,
		RequestVar:         r.Request,
		NamespaceObjectVar: r.NamespaceObject,
		ParamsVar:          constraint,
	}
}

// violations reads the result of a policy: the messages of the validations the
// object failed. A policy whose match conditions did not hold yields no value
// at all, which is a request the policy does not apply to.
func violations(out ref.Val) ([]Violation, error) {
	if opt, ok := out.(*types.Optional); ok {
		if !opt.HasValue() {
			return nil, nil
		}
		out = opt.GetValue()
	}
	messages, err := out.ConvertToNative(reflect.TypeOf([]string{}))
	if err != nil {
		return nil, fmt.Errorf("unexpected policy result %v: %w", out.Type(), err)
	}
	reported := messages.([]string)
	result := make([]Violation, 0, len(reported))
	for _, message := range reported {
		result = append(result, Violation{Message: message})
	}
	return result, nil
}
