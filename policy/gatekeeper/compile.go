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

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
	"github.com/google/cel-go/policy"
)

// Template is the compiled CEL of a Gatekeeper ConstraintTemplate, ready to
// review admission requests.
type Template struct {
	name        string
	description string
	policy      *policy.Policy
	env         *cel.Env
	ast         *cel.Ast
	config      *config
	validations []*validation
}

// validation is a single entry of the template's `validations` list, compiled
// as its own program so that a review reports every violation rather than
// stopping at the first, as Gatekeeper does.
type validation struct {
	program cel.Program
	ast     *cel.Ast
}

// Violation is a message reported by a validation which the reviewed object
// did not satisfy.
type Violation struct {
	// Message is the result of the validation's `message` or
	// `messageExpression`.
	Message string

	// Index is the position of the failing validation within the template's
	// `validations` list.
	Index int
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
	singles := splitValidations(p)
	if len(singles) == 0 {
		return nil, fmt.Errorf("%s: the %s source declares no validations", src.Description(), EngineName)
	}
	// The whole template is compiled first so that every mistake in it is
	// reported once, rather than once per validation which shares a variable.
	composed, iss := policy.Compile(env, p)
	if iss.Err() != nil {
		return nil, iss.Err()
	}
	t := &Template{
		name:        p.Name().Value,
		description: p.Description().Value,
		policy:      p,
		env:         env,
		ast:         composed,
		config:      c,
	}
	for i, single := range singles {
		ast, iss := policy.Compile(env, single)
		if iss.Err() != nil {
			return nil, iss.Err()
		}
		prg, err := env.Program(ast)
		if err != nil {
			return nil, fmt.Errorf("validation %d: %w", i, err)
		}
		t.validations = append(t.validations, &validation{program: prg, ast: ast})
	}
	return t, nil
}

// splitValidations returns one policy per validation of the template, each
// carrying the variables and match conditions which apply to it.
//
// Gatekeeper reports every validation an object fails, while a CEL policy
// yields the first result which matches, so each validation is compiled on its
// own. The policies share the source of the template they came from, which
// keeps errors and coverage reported against it.
func splitValidations(p *policy.Policy) []*policy.Policy {
	prelude := p.Rule()
	if len(prelude.Matches()) != 1 || !prelude.Matches()[0].HasRule() {
		return []*policy.Policy{p}
	}
	guard := prelude.Matches()[0]
	rule := guard.Rule()
	policies := make([]*policy.Policy, 0, len(rule.Matches()))
	for _, match := range rule.Matches() {
		single := policy.NewRule(match.SourceID())
		single.AddVariables(rule.Variables())
		single.AddMatch(match)

		guarded := policy.NewMatch(guard.SourceID())
		guarded.SetCondition(guard.Condition())
		guarded.SetRule(single)

		outer := policy.NewRule(guard.SourceID())
		outer.AddVariables(prelude.Variables())
		outer.AddMatch(guarded)

		clone := policy.NewPolicy(p.Source(), p.SourceInfo())
		clone.SetName(p.Name())
		clone.SetRule(outer)
		policies = append(policies, clone)
	}
	return policies
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

// AST is the whole template compiled into a single CEL expression, in which the
// validations are evaluated in order until one reports a violation. It is the
// form a policy engine would evaluate, and the form the CEL test runner
// measures coverage against.
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
	// The lookups of a review are resolved once and shared by its validations,
	// so a policy which reads the same object from two validations reads it
	// once.
	data := newResolver(t.config.provider)
	vars := r.activation()
	vars[InventoryVar] = &inventoryValue{resolver: data}
	vars[ExternalDataVar] = &externalDataValue{resolver: data}
	var violations []Violation
	for i, v := range t.validations {
		out, err := t.evaluate(ctx, v.program, data, vars)
		if err != nil {
			return nil, fmt.Errorf("validation %d: %w", i, err)
		}
		message, violated, err := violationMessage(out)
		if err != nil {
			return nil, fmt.Errorf("validation %d: %w", i, err)
		}
		if violated {
			violations = append(violations, Violation{Message: message, Index: i})
		}
	}
	return violations, nil
}

// evaluate runs a validation, resolving the referential data it reads.
//
// A lookup which has not been answered evaluates to an unknown value, which CEL
// propagates without failing the expression. The lookups the expression reached
// are then fetched together and the expression is evaluated again, so that
// independent lookups are fetched in one batch, and a lookup the expression
// short-circuits past is never fetched at all.
func (t *Template) evaluate(ctx context.Context, prg cel.Program, data *resolver, vars map[string]any) (ref.Val, error) {
	for round := 0; round < t.config.maxRounds; round++ {
		out, _, err := prg.ContextEval(ctx, vars)
		if err != nil {
			return nil, err
		}
		if !types.IsUnknown(out) {
			return out, nil
		}
		if !data.hasPending() {
			// The expression cannot make progress: an unknown reached the
			// result without a lookup to resolve it.
			return nil, fmt.Errorf("evaluation produced an unresolved value: %v", out)
		}
		if err := data.fetch(ctx); err != nil {
			return nil, err
		}
	}
	return nil, fmt.Errorf("referential data did not resolve after %d rounds, "+
		"which means the policy's lookups depend on each other more deeply than MaxDataRounds allows", t.config.maxRounds)
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

// violationMessage interprets the result of a validation. A validation which
// the object satisfies yields no value, as its match condition did not hold.
func violationMessage(out ref.Val) (string, bool, error) {
	if opt, ok := out.(*types.Optional); ok {
		if !opt.HasValue() {
			return "", false, nil
		}
		out = opt.GetValue()
	}
	switch value := out.(type) {
	case types.String:
		return string(value), true, nil
	case types.Bool:
		// A validation whose message is a boolean cannot happen through the
		// template syntax, but a policy built by other means might produce one.
		return "", bool(value), nil
	default:
		return "", false, fmt.Errorf("unexpected validation result type: %v", out.Type())
	}
}
