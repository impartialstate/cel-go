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

	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
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
		Variable("cmp", FunctionType(BoolType, IntType, IntType)),
		Variable("acme.cmp", FunctionType(BoolType, IntType, IntType)),
		Variable("notAFunction", IntType),
	}
	env, err := NewEnv(append(baseOpts, opts...)...)
	if err != nil {
		t.Fatalf("NewEnv() failed: %v", err)
	}
	return env
}

// lessThanVal is the function value bound to the 'cmp' variables in the tests below.
func lessThanVal() *types.Function {
	return types.NewFunctionVal("cmp", FunctionType(BoolType, IntType, IntType),
		func(args ...ref.Val) ref.Val {
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
		{expr: `greaterThan`, typ: FunctionType(BoolType, IntType, IntType)},
		{expr: `twice`, typ: FunctionType(IntType, IntType)},
		{expr: `acme.negate`, typ: FunctionType(BoolType, BoolType)},
		{expr: `cmp`, typ: FunctionType(BoolType, IntType, IntType)},
		// A function with multiple global overloads is described by the most general signature
		// which accepts all of them and dispatches by argument type at runtime.
		{expr: `size`, typ: FunctionType(IntType, DynType)},
		{expr: `describe`, typ: FunctionType(StringType, DynType)},
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
	if got := sizeFn.Invoke(types.String("hello")); got != types.Int(5) {
		t.Errorf("Invoke(string) got %v, wanted 5", got)
	}
	if got := sizeFn.Invoke(types.DefaultTypeAdapter.NativeToValue([]int64{1, 2})); got != types.Int(2) {
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
	if got := describeFn.Invoke(types.Int(1)); got != types.String("int") {
		t.Errorf("Invoke(int) got %v, wanted 'int'", got)
	}
	if got := describeFn.Invoke(types.String("a")); got != types.String("string") {
		t.Errorf("Invoke(string) got %v, wanted 'string'", got)
	}
	if got := describeFn.Invoke(types.Double(1.0)); !types.IsError(got) {
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
				"cmp": types.NewFunctionVal("cmp", FunctionType(BoolType, IntType, IntType),
					func(args ...ref.Val) ref.Val { return types.NewErr("boom") }),
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
