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

package interpreter

import (
	"errors"
	"fmt"

	"github.com/google/cel-go/common/functions"
)

// FunctionResolver resolves function implementations which are supplied when an expression is
// evaluated rather than when it is planned.
//
// Functions whose overloads are declared with the decls.LateFunctionBinding() option are planned
// without an implementation. When such a function is called, its implementation is resolved from
// the input Activation, which makes it possible to evaluate a single compiled program with
// implementations that vary per-evaluation, e.g. functions bound to a request context, a set of
// caller credentials, or a request-scoped cache.
//
// Implementations may be supplied in either of two ways:
//
//   - The input Activation itself implements this interface.
//   - A separate FunctionBindings value is combined with the variables using the
//     NewLateBindingActivation call.
//
// A FunctionResolver found anywhere within an Activation hierarchy will be used, so late bindings
// may also be layered underneath hierarchical, partial, and comprehension activations.
type FunctionResolver interface {
	// ResolveFunction returns the overload bound to the given overload id or function name, and
	// whether a binding could be found.
	//
	// Calls are resolved by overload id first, falling back to the function name when the
	// expression is parse-only or when the overload id is ambiguous.
	ResolveFunction(name string) (*functions.Overload, bool)
}

// FunctionBindings is a set of late-bound function implementations which may be supplied to an
// evaluation alongside, but independently of, the variable bindings.
//
// FunctionBindings values are safe for concurrent use by multiple evaluations so long as they are
// not mutated with Add after being shared.
type FunctionBindings struct {
	overloads map[string]*functions.Overload
}

// NewFunctionBindings returns a FunctionBindings value for the given set of overloads.
//
// Overloads are keyed by their Operator value which may be either an overload id or a function
// name, mirroring the way in which function implementations are registered with a Dispatcher.
func NewFunctionBindings(overloads ...*functions.Overload) (*FunctionBindings, error) {
	b := &FunctionBindings{overloads: make(map[string]*functions.Overload, len(overloads))}
	if err := b.Add(overloads...); err != nil {
		return nil, err
	}
	return b, nil
}

// Add associates one or more overloads with the binding set, returning an error if an overload of
// the same name has already been provided, or if the overload does not declare an implementation.
func (b *FunctionBindings) Add(overloads ...*functions.Overload) error {
	for _, o := range overloads {
		if o == nil {
			return errors.New("overload must be non-nil")
		}
		if o.LateBound {
			return fmt.Errorf("overload is a late-binding placeholder and has no implementation: %s", o.Operator)
		}
		if o.Unary == nil && o.Binary == nil && o.Function == nil {
			return fmt.Errorf("overload has no implementation: %s", o.Operator)
		}
		if _, found := b.overloads[o.Operator]; found {
			return fmt.Errorf("overload already exists '%s'", o.Operator)
		}
		b.overloads[o.Operator] = o
	}
	return nil
}

// ResolveFunction implements the FunctionResolver interface method.
func (b *FunctionBindings) ResolveFunction(name string) (*functions.Overload, bool) {
	if b == nil {
		return nil, false
	}
	o, found := b.overloads[name]
	return o, found
}

// OverloadIds returns the set of all overload identifiers configured within the binding set.
func (b *FunctionBindings) OverloadIds() []string {
	if b == nil {
		return []string{}
	}
	ids := make([]string, 0, len(b.overloads))
	for name := range b.overloads {
		ids = append(ids, name)
	}
	return ids
}

// NewLateBindingActivation returns an Activation which resolves variables from the `bindings`
// input and late-bound function implementations from the given resolvers.
//
// The `bindings` value may be any input supported by the NewActivation call. Resolvers are
// consulted in the order provided, and if none of them can resolve a function, resolution falls
// through to any FunctionResolver reachable from the `bindings` activation.
func NewLateBindingActivation(bindings any, fns ...FunctionResolver) (Activation, error) {
	vars, err := NewActivation(bindings)
	if err != nil {
		return nil, err
	}
	resolvers := make(functionResolvers, 0, len(fns))
	for i, fn := range fns {
		if fn == nil {
			return nil, fmt.Errorf("function resolver must be non-nil: index %d", i)
		}
		resolvers = append(resolvers, fn)
	}
	return &lateBindingActivation{Activation: vars, fns: resolvers}, nil
}

// lateBindingActivation associates a set of function resolvers with a variable activation.
type lateBindingActivation struct {
	Activation
	fns functionResolvers
}

// ResolveFunction implements the FunctionResolver interface method, deferring to the activation
// which supplies the variables if none of the configured resolvers match.
func (a *lateBindingActivation) ResolveFunction(name string) (*functions.Overload, bool) {
	if o, found := a.fns.ResolveFunction(name); found {
		return o, true
	}
	if fns, found := AsFunctionResolver(a.Activation); found {
		return fns.ResolveFunction(name)
	}
	return nil, false
}

// AsPartialActivation preserves the partial activation behavior of the wrapped activation.
func (a *lateBindingActivation) AsPartialActivation() (PartialActivation, bool) {
	return AsPartialActivation(a.Activation)
}

// functionResolvers is an ordered set of FunctionResolver values treated as a single resolver.
type functionResolvers []FunctionResolver

// ResolveFunction implements the FunctionResolver interface method.
func (r functionResolvers) ResolveFunction(name string) (*functions.Overload, bool) {
	for _, fns := range r {
		if o, found := fns.ResolveFunction(name); found {
			return o, true
		}
	}
	return nil, false
}

// functionResolverConverter indicates whether an Activation implementation supports conversion to
// a FunctionResolver.
type functionResolverConverter interface {
	// AsFunctionResolver returns the FunctionResolver associated with the activation, if present.
	AsFunctionResolver() (FunctionResolver, bool)
}

// AsFunctionResolver walks the activation hierarchy and returns the first FunctionResolver found.
func AsFunctionResolver(vars Activation) (FunctionResolver, bool) {
	if vars == nil {
		return nil, false
	}
	if conv, ok := vars.(functionResolverConverter); ok {
		if fns, found := conv.AsFunctionResolver(); found {
			return fns, true
		}
	} else if fns, ok := vars.(FunctionResolver); ok {
		return fns, true
	}
	// Activations which hide local state, such as the @block() activation, expose the activation
	// which carries the user-provided input via the Unwrap method.
	if wrapper, ok := vars.(activationWrapper); ok {
		return AsFunctionResolver(wrapper.Unwrap())
	}
	if parent := vars.Parent(); parent != nil {
		return AsFunctionResolver(parent)
	}
	return nil, false
}

// resolveLateBoundOverload resolves a late-bound function implementation from the Activation,
// preferring a match on the overload id before falling back to the function name.
func resolveLateBoundOverload(vars Activation, function, overload string) (*functions.Overload, bool) {
	fns, found := AsFunctionResolver(vars)
	if !found {
		return nil, false
	}
	if overload != "" {
		if impl, found := fns.ResolveFunction(overload); found && isBoundOverload(impl) {
			return impl, true
		}
	}
	if impl, found := fns.ResolveFunction(function); found && isBoundOverload(impl) {
		return impl, true
	}
	return nil, false
}

// isBoundOverload returns whether the overload declares an implementation.
func isBoundOverload(o *functions.Overload) bool {
	return o != nil && !o.LateBound && (o.Unary != nil || o.Binary != nil || o.Function != nil)
}
