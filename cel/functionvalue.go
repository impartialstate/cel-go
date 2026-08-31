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
	"sort"
	"strings"

	"github.com/google/cel-go/checker"
	celast "github.com/google/cel-go/common/ast"
	"github.com/google/cel-go/common/functions"
	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
	"github.com/google/cel-go/interpreter"
)

// FunctionValueOption configures how an expression is converted into a function value.
type FunctionValueOption func(*functionValueOptions) *functionValueOptions

// functionValueOptions holds the configurable parts of a function value built from an expression.
type functionValueOptions struct {
	estimator  checker.CostEstimator
	estimate   *CallEstimate
	costOpts   []checker.CostOption
	programOpt []ProgramOption
}

// FunctionValueCostEstimator supplies the estimator used to compute the declared cost of calling
// the function value from the cost of the expression which implements it.
//
// An estimator which bounds the size of the parameters produces a tighter estimate, and is worth
// providing whenever the cost of the expression depends on the size of its inputs.
func FunctionValueCostEstimator(estimator checker.CostEstimator, opts ...checker.CostOption) FunctionValueOption {
	return func(o *functionValueOptions) *functionValueOptions {
		o.estimator = estimator
		o.costOpts = opts
		return o
	}
}

// FunctionValueCallEstimate declares the cost of calling the function value and the size of its
// result, in place of the estimate derived from the expression.
//
// Declare the estimate directly when the cost of the expression cannot be bounded statically, as
// the declared cost is what each invocation is charged against the evaluation's budget.
func FunctionValueCallEstimate(estimate CallEstimate) FunctionValueOption {
	return func(o *functionValueOptions) *functionValueOptions {
		o.estimate = &estimate
		return o
	}
}

// FunctionValueProgramOptions configures the program which evaluates the expression.
func FunctionValueProgramOptions(opts ...ProgramOption) FunctionValueOption {
	return func(o *functionValueOptions) *functionValueOptions {
		o.programOpt = opts
		return o
	}
}

// FunctionValue converts a checked expression into a function value which may be bound to a
// variable of the resulting function type, or passed to a higher-order function.
//
// The params name the variables of the expression which become the parameters of the function, in
// the order the function is called with them, and their declared types become the argument types
// of the function type. The result type of the function is the output type of the expression.
//
//	env, _ := cel.NewEnv(cel.Variable("a", cel.IntType), cel.Variable("b", cel.IntType))
//	ast, _ := env.Compile(`a < b`)
//	lessThan, _ := env.FunctionValue("lessThan", ast, []string{"a", "b"})
//	// lessThan has the type (int, int) -> bool
//
// A checked expression is fully specified, so the parameters must account for every variable it
// references: a reference to any other variable is reported as an error here rather than becoming
// a value the function would have no way to supply. The expression is therefore closed over its
// parameters, and is evaluated against them alone - it cannot observe the variables of whatever
// expression calls it, and the same value may be called from any evaluation.
//
// The cost declared for a call is the estimated cost of the expression, which each invocation is
// charged against the calling evaluation's budget. Use FunctionValueCostEstimator to tighten that
// estimate, or FunctionValueCallEstimate to declare it outright.
func (e *Env) FunctionValue(name string, a *Ast, params []string, opts ...FunctionValueOption) (*types.Function, error) {
	if a == nil || !a.IsChecked() {
		return nil, fmt.Errorf("function value %q requires a checked expression", name)
	}
	cfg := &functionValueOptions{estimator: unboundedSizes{}}
	for _, opt := range opts {
		cfg = opt(cfg)
	}
	paramTypes, paramSet, err := e.functionValueParams(name, params)
	if err != nil {
		return nil, err
	}
	if err := checkClosedOver(name, a.NativeRep(), paramSet); err != nil {
		return nil, err
	}
	estimate, err := e.functionValueEstimate(a, cfg)
	if err != nil {
		return nil, err
	}
	prg, err := e.Program(a, cfg.programOpt...)
	if err != nil {
		return nil, err
	}
	fnType := FunctionType(estimate, a.OutputType(), paramTypes...)
	names := make([]string, len(params))
	copy(names, params)
	return FunctionVal(name, fnType, estimate,
		func(frame functions.ExecutionFrame, args ...ref.Val) ref.Val {
			// The expression references nothing but its parameters, so the arguments are the whole
			// of the activation it is evaluated against.
			out, _, err := prg.Eval(&paramActivation{names: names, args: args})
			if err != nil {
				return types.WrapErr(err)
			}
			return out
		}), nil
}

// functionValueParams resolves the declared types of the named parameters.
func (e *Env) functionValueParams(name string, params []string) ([]*Type, map[string]bool, error) {
	declared := make(map[string]*Type, len(e.Variables()))
	for _, v := range e.Variables() {
		declared[v.Name()] = v.Type()
	}
	paramTypes := make([]*Type, len(params))
	paramSet := make(map[string]bool, len(params))
	for i, p := range params {
		if paramSet[p] {
			return nil, nil, fmt.Errorf("function value %q declares parameter %q more than once", name, p)
		}
		t, found := declared[p]
		if !found {
			return nil, nil, fmt.Errorf("function value %q has an undeclared parameter: %s", name, p)
		}
		paramTypes[i] = t
		paramSet[p] = true
	}
	return paramTypes, paramSet, nil
}

// functionValueEstimate determines the cost declared for a call of the function value.
func (e *Env) functionValueEstimate(a *Ast, cfg *functionValueOptions) (CallEstimate, error) {
	if cfg.estimate != nil {
		return *cfg.estimate, nil
	}
	cost, err := e.EstimateCost(a, cfg.estimator, cfg.costOpts...)
	if err != nil {
		return UnknownCallEstimate(), err
	}
	return CallEstimate{CostEstimate: cost}, nil
}

// checkClosedOver reports an error when the expression references a variable which is not one of
// its parameters, and so could not be supplied by a call.
func checkClosedOver(name string, a *celast.AST, params map[string]bool) error {
	free := map[string]bool{}
	collectFreeVars(a, a.Expr(), map[string]bool{}, free)
	if len(free) == 0 {
		return nil
	}
	unbound := []string{}
	for v := range free {
		if !params[v] {
			unbound = append(unbound, v)
		}
	}
	if len(unbound) == 0 {
		return nil
	}
	sort.Strings(unbound)
	quoted := make([]string, len(unbound))
	for i, v := range unbound {
		quoted[i] = fmt.Sprintf("%q", v)
	}
	if len(quoted) == 1 {
		return fmt.Errorf(
			"function value %q references %s which is not a parameter of the function",
			name, quoted[0])
	}
	return fmt.Errorf(
		"function value %q references %s and %s which are not parameters of the function",
		name, strings.Join(quoted[:len(quoted)-1], ", "), quoted[len(quoted)-1])
}

// collectFreeVars gathers the names of the variables an expression reads from its activation,
// skipping the variables bound by the comprehensions within it.
func collectFreeVars(a *celast.AST, e celast.Expr, bound, free map[string]bool) {
	if e == nil {
		return
	}
	switch e.Kind() {
	case celast.IdentKind:
		name := e.AsIdent()
		if bound[name] {
			return
		}
		// Function references and type names are resolved when the expression is planned, and
		// constants such as enum values are resolved by the check, so none of them are read from
		// the activation.
		if ref, found := a.ReferenceMap()[e.ID()]; found {
			if len(ref.OverloadIDs) != 0 || ref.Value != nil {
				return
			}
		}
		if a.GetType(e.ID()).Kind() == types.TypeKind {
			return
		}
		free[name] = true
	case celast.SelectKind:
		collectFreeVars(a, e.AsSelect().Operand(), bound, free)
	case celast.CallKind:
		call := e.AsCall()
		if call.IsMemberFunction() {
			collectFreeVars(a, call.Target(), bound, free)
		}
		for _, arg := range call.Args() {
			collectFreeVars(a, arg, bound, free)
		}
	case celast.ListKind:
		for _, elem := range e.AsList().Elements() {
			collectFreeVars(a, elem, bound, free)
		}
	case celast.MapKind:
		for _, entry := range e.AsMap().Entries() {
			mapEntry := entry.AsMapEntry()
			collectFreeVars(a, mapEntry.Key(), bound, free)
			collectFreeVars(a, mapEntry.Value(), bound, free)
		}
	case celast.StructKind:
		for _, field := range e.AsStruct().Fields() {
			collectFreeVars(a, field.AsStructField().Value(), bound, free)
		}
	case celast.ComprehensionKind:
		comp := e.AsComprehension()
		// The range and the initial accumulator value are evaluated in the enclosing scope.
		collectFreeVars(a, comp.IterRange(), bound, free)
		collectFreeVars(a, comp.AccuInit(), bound, free)
		// The loop and the result are evaluated with the iteration and accumulator variables in
		// scope, shadowing any variable of the same name.
		inner := make(map[string]bool, len(bound)+3)
		for v := range bound {
			inner[v] = true
		}
		inner[comp.IterVar()] = true
		if comp.HasIterVar2() {
			inner[comp.IterVar2()] = true
		}
		inner[comp.AccuVar()] = true
		collectFreeVars(a, comp.LoopCondition(), inner, free)
		collectFreeVars(a, comp.LoopStep(), inner, free)
		collectFreeVars(a, comp.Result(), inner, free)
	}
}

// paramActivation binds the arguments of a call to the parameter names of the expression which
// implements it, and resolves nothing else.
type paramActivation struct {
	names []string
	args  []ref.Val
}

// ResolveName implements the interpreter.Activation interface method.
func (a *paramActivation) ResolveName(name string) (any, bool) {
	for i, n := range a.names {
		if n == name {
			return a.args[i], true
		}
	}
	return nil, false
}

// Parent implements the interpreter.Activation interface method, and is always nil as the
// expression is closed over its parameters.
func (a *paramActivation) Parent() interpreter.Activation {
	return nil
}

// unboundedSizes is the default cost estimator, which bounds nothing.
type unboundedSizes struct{}

func (unboundedSizes) EstimateSize(checker.AstNode) *checker.SizeEstimate { return nil }

func (unboundedSizes) EstimateCallCost(function, overloadID string,
	target *checker.AstNode, args []checker.AstNode) *checker.CallEstimate {
	return nil
}
