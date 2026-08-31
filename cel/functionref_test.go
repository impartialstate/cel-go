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
	"strings"
	"testing"

	"github.com/google/cel-go/checker"
	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
	"github.com/google/cel-go/common/types/traits"
)

// testFunctionRefEnv declares a handful of simple functions and function-typed variables which
// exercise function references and function value invocation.
func testFunctionRefEnv(t *testing.T, opts ...EnvOption) *Env {
	t.Helper()
	baseOpts := []EnvOption{
		Function("greaterThan",
			Overload("greater_than_int", []*Type{IntType, IntType}, BoolType,
				BinaryBinding(func(lhs, rhs ref.Val) ref.Val {
					return types.Bool(lhs.(types.Int) > rhs.(types.Int))
				}))),
		Function("twice",
			Overload("twice_int", []*Type{IntType}, IntType,
				UnaryBinding(func(v ref.Val) ref.Val { return v.(types.Int) * 2 }))),
		Function("thrice",
			Overload("thrice_int", []*Type{IntType}, IntType,
				UnaryBinding(func(v ref.Val) ref.Val { return v.(types.Int) * 3 }))),
		Function("acme.negate",
			Overload("acme_negate_bool", []*Type{BoolType}, BoolType,
				UnaryBinding(func(v ref.Val) ref.Val { return !v.(types.Bool) }))),
		Function("varArity",
			Overload("var_arity_int", []*Type{IntType}, IntType,
				UnaryBinding(func(v ref.Val) ref.Val { return v })),
			Overload("var_arity_int_int", []*Type{IntType, IntType}, IntType,
				BinaryBinding(func(lhs, rhs ref.Val) ref.Val { return lhs.(types.Int) + rhs.(types.Int) }))),
		Function("describe",
			Overload("describe_int", []*Type{IntType}, StringType,
				UnaryBinding(func(v ref.Val) ref.Val { return types.String("int") })),
			Overload("describe_string", []*Type{StringType}, StringType,
				UnaryBinding(func(v ref.Val) ref.Val { return types.String("string") }))),
		Function("memberOnly",
			MemberOverload("int_member_only", []*Type{IntType}, IntType,
				UnaryBinding(func(v ref.Val) ref.Val { return v }))),
		Variable("cmp", FunctionType(cmpEstimate, BoolType, IntType, IntType)),
		Variable("acme.cmp", FunctionType(cmpEstimate, BoolType, IntType, IntType)),
		Variable("notAFunction", IntType),
	}
	env, err := NewEnv(append(baseOpts, opts...)...)
	if err != nil {
		t.Fatalf("NewEnv() failed: %v", err)
	}
	return env
}

// cmpEstimate is the declared cost of the 'cmp' function values used by the tests below.
var cmpEstimate = FixedCallEstimate(2)

// lessThanVal is the function value bound to the 'cmp' variables in the tests below.
func lessThanVal() *types.Function {
	return FunctionVal("cmp", FunctionType(cmpEstimate, BoolType, IntType, IntType), cmpEstimate,
		func(frame ExecutionFrame, args ...ref.Val) ref.Val {
			return types.Bool(args[0].(types.Int) < args[1].(types.Int))
		})
}

func TestFunctionRefEval(t *testing.T) {
	tests := []struct {
		expr string
		out  ref.Val
		typ  *Type
	}{
		{
			expr: `greaterThan(2, 1)`,
			out:  types.True,
			typ:  BoolType,
		},
		{
			expr: `cmp(1, 2)`,
			out:  types.True,
			typ:  BoolType,
		},
		{
			expr: `acme.cmp(2, 1)`,
			out:  types.False,
			typ:  BoolType,
		},
		{
			// A reference to a function with a single global overload adopts its signature.
			expr: `type(twice) == type(thrice)`,
			out:  types.True,
			typ:  BoolType,
		},
		{
			// A reference to a namespaced function.
			expr: `acme.negate(true)`,
			out:  types.False,
			typ:  BoolType,
		},
		{
			// Function values may be collected into lists and invoked from a comprehension.
			expr: `[twice, thrice].exists(f, f(2) == 6)`,
			out:  types.True,
			typ:  BoolType,
		},
		{
			// A function reference which is never invoked still evaluates to a value.
			expr: `[twice].size()`,
			out:  types.Int(1),
			typ:  IntType,
		},
	}
	env := testFunctionRefEnv(t)
	for _, tst := range tests {
		tc := tst
		t.Run(tc.expr, func(t *testing.T) {
			ast, iss := env.Compile(tc.expr)
			if iss.Err() != nil {
				t.Fatalf("env.Compile(%q) failed: %v", tc.expr, iss.Err())
			}
			if !ast.OutputType().IsExactType(tc.typ) {
				t.Errorf("OutputType() got %v, wanted %v", ast.OutputType(), tc.typ)
			}
			prg, err := env.Program(ast)
			if err != nil {
				t.Fatalf("env.Program() failed: %v", err)
			}
			out, _, err := prg.Eval(map[string]any{
				"cmp":      lessThanVal(),
				"acme.cmp": lessThanVal(),
			})
			if err != nil {
				t.Fatalf("prg.Eval() failed: %v", err)
			}
			if out.Equal(tc.out) != types.True {
				t.Errorf("prg.Eval() got %v, wanted %v", out, tc.out)
			}
		})
	}
}

func TestFunctionRefType(t *testing.T) {
	env := testFunctionRefEnv(t)
	tests := []struct {
		expr string
		typ  *Type
	}{
		{expr: `greaterThan`, typ: FunctionType(UnknownCallEstimate(), BoolType, IntType, IntType)},
		{expr: `twice`, typ: FunctionType(UnknownCallEstimate(), IntType, IntType)},
		{expr: `acme.negate`, typ: FunctionType(UnknownCallEstimate(), BoolType, BoolType)},
		{expr: `cmp`, typ: FunctionType(cmpEstimate, BoolType, IntType, IntType)},
		// A function with multiple global overloads is described by the most general signature
		// which accepts all of them and dispatches by argument type at runtime.
		{expr: `size`, typ: FunctionType(UnknownCallEstimate(), IntType, DynType)},
		{expr: `describe`, typ: FunctionType(UnknownCallEstimate(), StringType, DynType)},
	}
	for _, tst := range tests {
		tc := tst
		t.Run(tc.expr, func(t *testing.T) {
			ast, iss := env.Compile(tc.expr)
			if iss.Err() != nil {
				t.Fatalf("env.Compile(%q) failed: %v", tc.expr, iss.Err())
			}
			if !ast.OutputType().IsExactType(tc.typ) {
				t.Errorf("OutputType() got %v, wanted %v", ast.OutputType(), tc.typ)
			}
		})
	}
}

func TestFunctionRefDynamicDispatch(t *testing.T) {
	env := testFunctionRefEnv(t)
	ast, iss := env.Compile(`size`)
	if iss.Err() != nil {
		t.Fatalf("env.Compile() failed: %v", iss.Err())
	}
	prg, err := env.Program(ast)
	if err != nil {
		t.Fatalf("env.Program() failed: %v", err)
	}
	out, _, err := prg.Eval(NoVars())
	if err != nil {
		t.Fatalf("prg.Eval() failed: %v", err)
	}
	sizeFn, ok := out.(*types.Function)
	if !ok {
		t.Fatalf("prg.Eval() got %v, wanted a function value", out)
	}
	if got := sizeFn.Invoke(nil, types.String("hello")); got != types.Int(5) {
		t.Errorf("Invoke(string) got %v, wanted 5", got)
	}
	if got := sizeFn.Invoke(nil, types.DefaultTypeAdapter.NativeToValue([]int64{1, 2})); got != types.Int(2) {
		t.Errorf("Invoke(list) got %v, wanted 2", got)
	}

	// Overloads declared by the environment dispatch in the same manner.
	ast, iss = env.Compile(`describe`)
	if iss.Err() != nil {
		t.Fatalf("env.Compile() failed: %v", iss.Err())
	}
	prg, err = env.Program(ast)
	if err != nil {
		t.Fatalf("env.Program() failed: %v", err)
	}
	out, _, err = prg.Eval(NoVars())
	if err != nil {
		t.Fatalf("prg.Eval() failed: %v", err)
	}
	describeFn, ok := out.(*types.Function)
	if !ok {
		t.Fatalf("prg.Eval() got %v, wanted a function value", out)
	}
	if got := describeFn.Invoke(nil, types.Int(1)); got != types.String("int") {
		t.Errorf("Invoke(int) got %v, wanted 'int'", got)
	}
	if got := describeFn.Invoke(nil, types.String("a")); got != types.String("string") {
		t.Errorf("Invoke(string) got %v, wanted 'string'", got)
	}
	if got := describeFn.Invoke(nil, types.Double(1.0)); !types.IsError(got) {
		t.Errorf("Invoke(double) got %v, wanted an error", got)
	}
}

func TestFunctionRefRuntimeErrors(t *testing.T) {
	env := testFunctionRefEnv(t)
	tests := []struct {
		name string
		expr string
		vars map[string]any
		err  string
	}{
		{
			name: "unbound function variable",
			expr: `cmp(1, 2)`,
			vars: map[string]any{},
			err:  "no such attribute",
		},
		{
			name: "variable is not a function",
			expr: `cmp(1, 2)`,
			vars: map[string]any{"cmp": 1},
			err:  "is not a function",
		},
		{
			name: "error propagation from the function value",
			expr: `cmp(1, 2)`,
			vars: map[string]any{
				"cmp": FunctionVal("cmp", FunctionType(cmpEstimate, BoolType, IntType, IntType), cmpEstimate,
					func(frame ExecutionFrame, args ...ref.Val) ref.Val { return types.NewErr("boom") }),
			},
			err: "boom",
		},
	}
	for _, tst := range tests {
		tc := tst
		t.Run(tc.name, func(t *testing.T) {
			ast, iss := env.Compile(tc.expr)
			if iss.Err() != nil {
				t.Fatalf("env.Compile(%q) failed: %v", tc.expr, iss.Err())
			}
			prg, err := env.Program(ast)
			if err != nil {
				t.Fatalf("env.Program() failed: %v", err)
			}
			_, _, err = prg.Eval(tc.vars)
			if err == nil || !strings.Contains(err.Error(), tc.err) {
				t.Errorf("prg.Eval() got %v, wanted error containing %q", err, tc.err)
			}
		})
	}
}

func TestFunctionRefCompileErrors(t *testing.T) {
	env := testFunctionRefEnv(t)
	tests := []struct {
		expr string
		err  string
	}{
		{
			expr: `greaterThan(1)`,
			err:  "found no matching overload for 'greaterThan' applied to '(int)'",
		},
		{
			expr: `cmp(1)`,
			err:  "found no matching overload for 'cmp' applied to '(int)'",
		},
		{
			expr: `cmp('a', 'b')`,
			err:  "found no matching overload for 'cmp' applied to '(string, string)'",
		},
		{
			// A call passes exactly the arguments the function type declares.
			expr: `cmp(1, 2, 3)`,
			err:  "found no matching overload for 'cmp' applied to '(int, int, int)'",
		},
		{
			expr: `cmp()`,
			err:  "found no matching overload for 'cmp' applied to '()'",
		},
		{
			expr: `notAFunction(1)`,
			err:  "undeclared reference to 'notAFunction'",
		},
		{
			expr: `undeclared(1)`,
			err:  "undeclared reference to 'undeclared'",
		},
		{
			// Only global overloads may be referenced as values.
			expr: `memberOnly`,
			err:  "function 'memberOnly' cannot be used as a value: no global overload is declared",
		},
		{
			// Overloads with differing argument counts cannot be described by a single signature.
			expr: `varArity`,
			err:  "function 'varArity' cannot be used as a value: global overloads accept differing argument counts",
		},
		{
			// The timestamp getters are only declared as member functions.
			expr: `getFullYear`,
			err:  "function 'getFullYear' cannot be used as a value: no global overload is declared",
		},
		{
			expr: `twice(1, 2)`,
			err:  "found no matching overload for 'twice' applied to '(int, int)'",
		},
	}
	for _, tst := range tests {
		tc := tst
		t.Run(tc.expr, func(t *testing.T) {
			_, iss := env.Compile(tc.expr)
			if iss.Err() == nil {
				t.Fatalf("env.Compile(%q) succeeded, wanted error %q", tc.expr, tc.err)
			}
			if !strings.Contains(iss.Err().Error(), tc.err) {
				t.Errorf("env.Compile(%q) got %v, wanted error containing %q", tc.expr, iss.Err(), tc.err)
			}
		})
	}
}

func TestFunctionRefUnparse(t *testing.T) {
	env := testFunctionRefEnv(t, EnableMacroCallTracking())
	tests := []string{
		`[twice, thrice].exists(f, f(2) == 6)`,
		`cmp(1, 2)`,
		`acme.cmp(1, 2)`,
		`greaterThan(1, 2)`,
	}
	for _, tst := range tests {
		tc := tst
		t.Run(tc, func(t *testing.T) {
			ast, iss := env.Compile(tc)
			if iss.Err() != nil {
				t.Fatalf("env.Compile(%q) failed: %v", tc, iss.Err())
			}
			src, err := AstToString(ast)
			if err != nil {
				t.Fatalf("AstToString() failed: %v", err)
			}
			if src != tc {
				t.Errorf("AstToString() got %q, wanted %q", src, tc)
			}
		})
	}
}

func TestFunctionRefCheckedExprRoundTrip(t *testing.T) {
	env := testFunctionRefEnv(t)
	ast, iss := env.Compile(`[twice, thrice].exists(f, f(2) == 6) && cmp(1, 2)`)
	if iss.Err() != nil {
		t.Fatalf("env.Compile() failed: %v", iss.Err())
	}
	checked, err := AstToCheckedExpr(ast)
	if err != nil {
		t.Fatalf("AstToCheckedExpr() failed: %v", err)
	}
	prg, err := env.Program(CheckedExprToAst(checked))
	if err != nil {
		t.Fatalf("env.Program() failed: %v", err)
	}
	out, _, err := prg.Eval(map[string]any{"cmp": lessThanVal()})
	if err != nil {
		t.Fatalf("prg.Eval() failed: %v", err)
	}
	if out != types.True {
		t.Errorf("prg.Eval() got %v, wanted true", out)
	}
}

// costTestEstimator provides no estimates of its own so that the cost of an expression is derived
// entirely from the declarations within it.
type costTestEstimator struct{}

func (costTestEstimator) EstimateSize(checker.AstNode) *checker.SizeEstimate { return nil }

func (costTestEstimator) EstimateCallCost(function, overloadID string,
	target *checker.AstNode, args []checker.AstNode) *checker.CallEstimate {
	return nil
}

// costEnv declares a function and a function-typed variable which both declare the cost of
// invoking them, along with a higher-order 'applyTwice' function implemented with a frame binding.
func costEnv(t *testing.T) *Env {
	t.Helper()
	env, err := NewEnv(
		Function("expensive",
			Overload("expensive_int", []*Type{IntType}, IntType,
				OverloadCallEstimate(FixedCallEstimate(10)),
				UnaryBinding(func(v ref.Val) ref.Val { return v.(types.Int) * 2 }))),
		Function("cheap",
			Overload("cheap_int", []*Type{IntType}, IntType,
				OverloadCallEstimate(FixedCallEstimate(1)),
				UnaryBinding(func(v ref.Val) ref.Val { return v.(types.Int) + 1 }))),
		Function("applyTwice",
			Overload("apply_twice_int",
				[]*Type{FunctionType(UnknownCallEstimate(), IntType, IntType), IntType}, IntType,
				FrameBinding(func(frame ExecutionFrame, args ...ref.Val) ref.Val {
					fn, ok := args[0].(traits.Invoker)
					if !ok {
						return types.NewErr("not a function: %v", args[0].Type())
					}
					once := fn.Invoke(frame, args[1])
					if types.IsUnknownOrError(once) {
						return once
					}
					return fn.Invoke(frame, once)
				}))),
		Variable("costlyCmp", FunctionType(FixedCallEstimate(5), BoolType, IntType, IntType)),
		// A higher-order function accounts for the calls it makes by reading the cost declared
		// by the function type of its argument.
		CostEstimatorOptions(
			checker.OverloadCostEstimate("apply_twice_int",
				func(estimator checker.CostEstimator,
					target *checker.AstNode, args []checker.AstNode) *checker.CallEstimate {
					if len(args) != 2 {
						return nil
					}
					callee := types.FunctionCallEstimate(args[0].Type())
					if callee.IsUnknown() {
						return nil
					}
					cost := callee.CostEstimate.
						Multiply(checker.FixedCostEstimate(2)).
						Add(checker.FixedCostEstimate(1))
					return &checker.CallEstimate{CostEstimate: cost}
				}),
		),
	)
	if err != nil {
		t.Fatalf("NewEnv() failed: %v", err)
	}
	return env
}

func TestFunctionRefDeclaredCost(t *testing.T) {
	env := costEnv(t)
	tests := []struct {
		expr string
		typ  *Type
	}{
		// The declared cost of a referenced function becomes the cost of its function type.
		{expr: `expensive`, typ: FunctionType(FixedCallEstimate(10), IntType, IntType)},
		{expr: `cheap`, typ: FunctionType(FixedCallEstimate(1), IntType, IntType)},
	}
	for _, tst := range tests {
		tc := tst
		t.Run(tc.expr, func(t *testing.T) {
			ast, iss := env.Compile(tc.expr)
			if iss.Err() != nil {
				t.Fatalf("env.Compile(%q) failed: %v", tc.expr, iss.Err())
			}
			got := types.FunctionCallEstimate(ast.OutputType())
			want := types.FunctionCallEstimate(tc.typ)
			if got != want {
				t.Errorf("FunctionCallEstimate() got %v, wanted %v", got, want)
			}
		})
	}
}

func TestFunctionRefCost(t *testing.T) {
	env := costEnv(t)
	tests := []struct {
		name       string
		expr       string
		estimated  checker.CostEstimate
		actualCost uint64
	}{
		{
			// ident (1) + invocation dispatch (1) + the variable's declared cost (5)
			name:       "invoke function variable",
			expr:       `costlyCmp(1, 2)`,
			estimated:  checker.FixedCostEstimate(7),
			actualCost: 7,
		},
		{
			// call dispatch (1) + two invocations of the declared cost 10 function
			name:       "higher-order call",
			expr:       `applyTwice(expensive, 1)`,
			estimated:  checker.FixedCostEstimate(21),
			actualCost: 21,
		},
		{
			name:       "higher-order call with a cheap function",
			expr:       `applyTwice(cheap, 1)`,
			estimated:  checker.FixedCostEstimate(3),
			actualCost: 3,
		},
	}
	for _, tst := range tests {
		tc := tst
		t.Run(tc.name, func(t *testing.T) {
			ast, iss := env.Compile(tc.expr)
			if iss.Err() != nil {
				t.Fatalf("env.Compile(%q) failed: %v", tc.expr, iss.Err())
			}
			est, err := env.EstimateCost(ast, costTestEstimator{})
			if err != nil {
				t.Fatalf("env.EstimateCost() failed: %v", err)
			}
			if est != tc.estimated {
				t.Errorf("env.EstimateCost() got %v, wanted %v", est, tc.estimated)
			}
			prg, err := env.Program(ast, CostTracking(nil))
			if err != nil {
				t.Fatalf("env.Program() failed: %v", err)
			}
			_, det, err := prg.Eval(map[string]any{"costlyCmp": costlyCmpVal()})
			if err != nil {
				t.Fatalf("prg.Eval() failed: %v", err)
			}
			if det.ActualCost() == nil {
				t.Fatal("prg.Eval() produced no actual cost")
			}
			if *det.ActualCost() != tc.actualCost {
				t.Errorf("ActualCost() got %d, wanted %d", *det.ActualCost(), tc.actualCost)
			}
		})
	}
}

func TestFunctionRefCostLimit(t *testing.T) {
	env := costEnv(t)
	ast, iss := env.Compile(`applyTwice(expensive, 1)`)
	if iss.Err() != nil {
		t.Fatalf("env.Compile() failed: %v", iss.Err())
	}
	prg, err := env.Program(ast, CostTracking(nil), CostLimit(15))
	if err != nil {
		t.Fatalf("env.Program() failed: %v", err)
	}
	_, _, err = prg.Eval(NoVars())
	if err == nil || !strings.Contains(err.Error(), "actual cost limit exceeded") {
		t.Errorf("prg.Eval() got %v, wanted a cost limit error", err)
	}
}

// costlyCmpVal is the function value bound to the 'costlyCmp' variable, declaring a cost of 5.
func costlyCmpVal() *types.Function {
	estimate := FixedCallEstimate(5)
	return FunctionVal("costlyCmp", FunctionType(estimate, BoolType, IntType, IntType), estimate,
		func(frame ExecutionFrame, args ...ref.Val) ref.Val {
			return types.Bool(args[0].(types.Int) < args[1].(types.Int))
		})
}

func TestFrameBinding(t *testing.T) {
	env, err := NewEnv(
		Variable("budget", IntType),
		// An extension which reads a variable of the evaluation rather than of its arguments.
		Function("budgetRemaining",
			Overload("budget_remaining", []*Type{}, IntType,
				FrameBinding(func(frame ExecutionFrame, args ...ref.Val) ref.Val {
					budget, found := frame.ResolveName("budget")
					if !found {
						return types.NewErr("no budget")
					}
					return types.DefaultTypeAdapter.NativeToValue(budget)
				}))),
		// An extension which reports what the evaluation has cost so far, and which charges for
		// the work it performs itself.
		Function("costSoFar",
			Overload("cost_so_far", []*Type{}, IntType,
				FrameBinding(func(frame ExecutionFrame, args ...ref.Val) ref.Val {
					if err := frame.ChargeCost(3); err != nil {
						return types.WrapErr(err)
					}
					return types.Int(frame.Cost())
				}))),
		// A frame-bound function with more than one overload dispatches over them at runtime.
		Function("describe",
			Overload("describe_int", []*Type{IntType}, StringType,
				FrameBinding(func(frame ExecutionFrame, args ...ref.Val) ref.Val {
					return types.String("int")
				})),
			Overload("describe_string", []*Type{StringType}, StringType,
				FrameBinding(func(frame ExecutionFrame, args ...ref.Val) ref.Val {
					return types.String("string")
				}))),
	)
	if err != nil {
		t.Fatalf("NewEnv() failed: %v", err)
	}
	tests := []struct {
		expr string
		out  ref.Val
	}{
		{expr: `budgetRemaining()`, out: types.Int(42)},
		{expr: `[describe(1), describe('a')]`,
			out: types.DefaultTypeAdapter.NativeToValue([]string{"int", "string"})},
	}
	for _, tst := range tests {
		tc := tst
		t.Run(tc.expr, func(t *testing.T) {
			ast, iss := env.Compile(tc.expr)
			if iss.Err() != nil {
				t.Fatalf("env.Compile(%q) failed: %v", tc.expr, iss.Err())
			}
			prg, err := env.Program(ast)
			if err != nil {
				t.Fatalf("env.Program() failed: %v", err)
			}
			out, _, err := prg.Eval(map[string]any{"budget": 42})
			if err != nil {
				t.Fatalf("prg.Eval() failed: %v", err)
			}
			if out.Equal(tc.out) != types.True {
				t.Errorf("prg.Eval() got %v, wanted %v", out, tc.out)
			}
		})
	}

	// A frame binding charges the evaluation's cost tracker directly.
	ast, iss := env.Compile(`costSoFar()`)
	if iss.Err() != nil {
		t.Fatalf("env.Compile() failed: %v", iss.Err())
	}
	prg, err := env.Program(ast, CostTracking(nil))
	if err != nil {
		t.Fatalf("env.Program() failed: %v", err)
	}
	out, det, err := prg.Eval(NoVars())
	if err != nil {
		t.Fatalf("prg.Eval() failed: %v", err)
	}
	if out != types.Int(3) {
		t.Errorf("prg.Eval() got %v, wanted the cost charged from within the call", out)
	}
	if det.ActualCost() == nil || *det.ActualCost() != 4 {
		t.Errorf("ActualCost() got %v, wanted 4", det.ActualCost())
	}

	// Evaluations without cost tracking charge nothing and report a zero cost.
	prg, err = env.Program(ast)
	if err != nil {
		t.Fatalf("env.Program() failed: %v", err)
	}
	out, _, err = prg.Eval(NoVars())
	if err != nil {
		t.Fatalf("prg.Eval() failed: %v", err)
	}
	if out != types.Int(0) {
		t.Errorf("prg.Eval() got %v, wanted 0 when cost tracking is disabled", out)
	}
}

func TestFunctionRefOptimize(t *testing.T) {
	// A checked expression which invokes a function value is re-checked when it is optimized, so
	// the invocation must type-check in the form the checker produced it in.
	env := testFunctionRefEnv(t, EnableMacroCallTracking())
	folder, err := NewConstantFoldingOptimizer()
	if err != nil {
		t.Fatalf("NewConstantFoldingOptimizer() failed: %v", err)
	}
	opt, err := NewStaticOptimizer(folder)
	if err != nil {
		t.Fatalf("NewStaticOptimizer() failed: %v", err)
	}
	tests := []struct {
		expr string
		want string
	}{
		{expr: `cmp(1, 2)`, want: `cmp(1, 2)`},
		{expr: `acme.cmp(1, 2)`, want: `acme.cmp(1, 2)`},
		// Function references resolve to a fixed implementation when an expression is planned, so
		// a comprehension over them folds to its result.
		{expr: `[twice, thrice].exists(f, f(2) == 6)`, want: `true`},
		// Calls of declared functions with constant arguments still fold.
		{expr: `greaterThan(2, 1)`, want: `true`},
	}
	for _, tst := range tests {
		tc := tst
		t.Run(tc.expr, func(t *testing.T) {
			ast, iss := env.Compile(tc.expr)
			if iss.Err() != nil {
				t.Fatalf("env.Compile(%q) failed: %v", tc.expr, iss.Err())
			}
			optimized, iss := opt.Optimize(env, ast)
			if iss.Err() != nil {
				t.Fatalf("opt.Optimize() failed: %v", iss.Err())
			}
			src, err := AstToString(optimized)
			if err != nil {
				t.Fatalf("AstToString() failed: %v", err)
			}
			if src != tc.want {
				t.Errorf("AstToString() got %q, wanted %q", src, tc.want)
			}
			prg, err := env.Program(optimized)
			if err != nil {
				t.Fatalf("env.Program() failed: %v", err)
			}
			out, _, err := prg.Eval(map[string]any{"cmp": lessThanVal(), "acme.cmp": lessThanVal()})
			if err != nil {
				t.Fatalf("prg.Eval() failed: %v", err)
			}
			if out.Type() != types.BoolType {
				t.Errorf("prg.Eval() got %v, wanted a bool result", out)
			}
		})
	}
}

func TestFunctionValueIsSealedFromCaller(t *testing.T) {
	// A function value which reports whether it can see the variables of the expression which
	// called it. The same value is invoked through a higher-order function in the lists
	// extension tests, which cannot be exercised here as ext depends on this package.
	probeType := FunctionType(FixedCallEstimate(1), BoolType, IntType)
	probe := FunctionVal("probe", probeType, FixedCallEstimate(1),
		func(frame ExecutionFrame, args ...ref.Val) ref.Val {
			_, found := frame.ResolveName("secret")
			return types.Bool(found)
		})
	env, err := NewEnv(
		Variable("secret", IntType),
		Variable("probe", probeType),
		// By contrast, an extension declared with a frame binding evaluates on the frame of its
		// caller and can read the same variable.
		Function("secretVisible",
			Overload("secret_visible", []*Type{}, BoolType,
				FrameBinding(func(frame ExecutionFrame, args ...ref.Val) ref.Val {
					_, found := frame.ResolveName("secret")
					return types.Bool(found)
				}))),
	)
	if err != nil {
		t.Fatalf("NewEnv() failed: %v", err)
	}
	tests := []struct {
		expr string
		out  ref.Val
	}{
		{expr: `secretVisible()`, out: types.True},
		{expr: `probe(1)`, out: types.False},
		{expr: `[probe(1), probe(2)]`,
			out: types.DefaultTypeAdapter.NativeToValue([]bool{false, false})},
	}
	for _, tst := range tests {
		tc := tst
		t.Run(tc.expr, func(t *testing.T) {
			ast, iss := env.Compile(tc.expr)
			if iss.Err() != nil {
				t.Fatalf("env.Compile(%q) failed: %v", tc.expr, iss.Err())
			}
			prg, err := env.Program(ast)
			if err != nil {
				t.Fatalf("env.Program() failed: %v", err)
			}
			out, _, err := prg.Eval(map[string]any{"secret": 42, "probe": probe})
			if err != nil {
				t.Fatalf("prg.Eval() failed: %v", err)
			}
			if out.Equal(tc.out) != types.True {
				t.Errorf("prg.Eval() got %v, wanted %v", out, tc.out)
			}
		})
	}
}

func TestFunctionRefPartialEval(t *testing.T) {
	env := testFunctionRefEnv(t)
	ast, iss := env.Compile(`cmp(1, 2)`)
	if iss.Err() != nil {
		t.Fatalf("env.Compile() failed: %v", iss.Err())
	}
	prg, err := env.Program(ast, EvalOptions(OptPartialEval))
	if err != nil {
		t.Fatalf("env.Program() failed: %v", err)
	}
	vars, err := PartialVars(map[string]any{}, AttributePattern("cmp"))
	if err != nil {
		t.Fatalf("PartialVars() failed: %v", err)
	}
	out, _, err := prg.Eval(vars)
	if err != nil {
		t.Fatalf("prg.Eval() failed: %v", err)
	}
	if !types.IsUnknown(out) {
		t.Errorf("prg.Eval() got %v, wanted unknown", out)
	}
}
