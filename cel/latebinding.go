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

package cel

import (
	"fmt"

	"github.com/google/cel-go/common/decls"
	"github.com/google/cel-go/common/functions"
	"github.com/google/cel-go/interpreter"
)

// FunctionResolver resolves function implementations supplied at evaluation time for functions
// which were declared with the LateFunctionBinding() overload option.
//
// Activations may implement this interface directly in order to serve function implementations
// from the same object which serves variables.
type FunctionResolver = interpreter.FunctionResolver

// FunctionBindings is a set of function implementations which may be supplied at evaluation time
// for functions declared with the LateFunctionBinding() overload option.
//
// Use NewLateFunctionBindings to create an instance, and NewLateBindingActivation to combine it
// with the variables for an evaluation.
type FunctionBindings = interpreter.FunctionBindings

// LateFunctionBindingsOpt is a functional option for configuring a set of late function bindings.
type LateFunctionBindingsOpt func([]*functions.Overload) ([]*functions.Overload, error)

// NewLateFunctionBindings creates the set of function implementations to supply to an evaluation
// of a program which declares one or more functions with the LateFunctionBinding() option.
//
// The bindings are supplied to an evaluation by combining them with the variables:
//
//	env, _ := cel.NewEnv(
//	    cel.Variable("user", cel.StringType),
//	    cel.Function("is_admin",
//	        cel.Overload("is_admin_string", []*cel.Type{cel.StringType}, cel.BoolType,
//	            cel.LateFunctionBinding())),
//	)
//	ast, _ := env.Compile(`is_admin(user)`)
//	prg, _ := env.Program(ast)
//
//	// Per-evaluation function implementations.
//	fns, _ := cel.NewLateFunctionBindings(
//	    cel.LateFunction("is_admin",
//	        cel.Overload("is_admin_string", []*cel.Type{cel.StringType}, cel.BoolType,
//	            cel.UnaryBinding(func(u ref.Val) ref.Val { return types.Bool(admins[u]) }))),
//	)
//	vars, _ := cel.NewLateBindingActivation(map[string]any{"user": "alice"}, fns)
//	out, _, err := prg.Eval(vars)
//
// A FunctionBindings value is immutable once created and may be shared across evaluations.
func NewLateFunctionBindings(opts ...LateFunctionBindingsOpt) (*FunctionBindings, error) {
	var overloads []*functions.Overload
	var err error
	for _, opt := range opts {
		overloads, err = opt(overloads)
		if err != nil {
			return nil, err
		}
	}
	return interpreter.NewFunctionBindings(overloads...)
}

// LateFunction defines a function implementation to supply at evaluation time using the same
// options which configure functions within an environment, e.g. Overload, MemberOverload,
// UnaryBinding, BinaryBinding, and FunctionBinding.
//
// The overload ids used within the late binding must match the overload ids declared within the
// environment; otherwise, the call will resolve by function name. Only the bindings are used from
// the declaration, so the argument and result types must agree with the environment declaration in
// order for the runtime type-guards to admit the call.
func LateFunction(name string, opts ...FunctionOpt) LateFunctionBindingsOpt {
	return func(overloads []*functions.Overload) ([]*functions.Overload, error) {
		fn, err := decls.NewFunction(name, opts...)
		if err != nil {
			return nil, err
		}
		return appendLateBindings(overloads, fn)
	}
}

// LateFunctionDecls defines a set of function implementations to supply at evaluation time from
// pre-built function declarations.
func LateFunctionDecls(funcs ...*decls.FunctionDecl) LateFunctionBindingsOpt {
	return func(overloads []*functions.Overload) ([]*functions.Overload, error) {
		var err error
		for _, fn := range funcs {
			overloads, err = appendLateBindings(overloads, fn)
			if err != nil {
				return nil, err
			}
		}
		return overloads, nil
	}
}

// LateOverloads defines a set of function implementations to supply at evaluation time from raw
// interpreter overloads, keyed by either overload id or function name.
//
// Unlike LateFunction, the overloads provided here are not wrapped in runtime type-guards.
func LateOverloads(overloads ...*functions.Overload) LateFunctionBindingsOpt {
	return func(acc []*functions.Overload) ([]*functions.Overload, error) {
		return append(acc, overloads...), nil
	}
}

// NewLateBindingActivation returns an Activation which resolves variables from the `vars` input
// and late-bound function implementations from the supplied resolvers.
//
// The `vars` value may be an Activation or any other input supported by the NewActivation call.
func NewLateBindingActivation(vars any, fns ...FunctionResolver) (Activation, error) {
	return interpreter.NewLateBindingActivation(vars, fns...)
}

// appendLateBindings expands a function declaration into its runtime overloads.
func appendLateBindings(overloads []*functions.Overload, fn *decls.FunctionDecl) ([]*functions.Overload, error) {
	if fn == nil {
		return nil, fmt.Errorf("function declaration must be non-nil")
	}
	if fn.HasLateBinding() {
		return nil, fmt.Errorf("late function binding must provide an implementation: %s", fn.Name())
	}
	bindings, err := fn.Bindings()
	if err != nil {
		return nil, err
	}
	if len(bindings) == 0 {
		return nil, fmt.Errorf("late function binding must provide an implementation: %s", fn.Name())
	}
	return append(overloads, bindings...), nil
}
