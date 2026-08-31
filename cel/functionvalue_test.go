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
)

// lambdaEnv declares the parameters which the expressions below are compiled against.
func lambdaEnv(t *testing.T, opts ...EnvOption) *Env {
	t.Helper()
	baseOpts := []EnvOption{
		Variable("a", IntType),
		Variable("b", IntType),
		Variable("s", StringType),
		Variable("l", ListType(IntType)),
	}
	env, err := NewEnv(append(baseOpts, opts...)...)
	if err != nil {
		t.Fatalf("NewEnv() failed: %v", err)
	}
	return env
}

func TestEnvFunctionValue(t *testing.T) {
	tests := []struct {
		name   string
		expr   string
		params []string
		typ    *Type
		args   []ref.Val
		out    ref.Val
	}{
		{
			name:   "comparator",
			expr:   `a < b`,
			params: []string{"a", "b"},
			typ:    FunctionType(UnknownCallEstimate(), BoolType, IntType, IntType),
			args:   []ref.Val{types.Int(1), types.Int(2)},
			out:    types.True,
		},
		{
			name:   "parameter order follows the declaration",
			expr:   `a - b`,
			params: []string{"b", "a"},
			typ:    FunctionType(UnknownCallEstimate(), IntType, IntType, IntType),
			args:   []ref.Val{types.Int(1), types.Int(10)},
			out:    types.Int(9),
		},
		{
			name:   "key extractor",
			expr:   `s.size()`,
			params: []string{"s"},
			typ:    FunctionType(UnknownCallEstimate(), IntType, StringType),
			args:   []ref.Val{types.String("hello")},
			out:    types.Int(5),
		},
		{
			name:   "constant function",
			expr:   `42`,
			params: []string{},
			typ:    FunctionType(UnknownCallEstimate(), IntType),
			args:   []ref.Val{},
			out:    types.Int(42),
		},
		{
			// A comprehension binds its own variables, which are not parameters of the function.
			name:   "comprehension over a parameter",
			expr:   `l.exists(e, e > a)`,
			params: []string{"l", "a"},
			typ: FunctionType(UnknownCallEstimate(), BoolType,
				ListType(IntType), IntType),
			args: []ref.Val{types.DefaultTypeAdapter.NativeToValue([]int64{1, 5}), types.Int(3)},
			out:  types.True,
		},
	}
	for _, tst := range tests {
		tc := tst
		t.Run(tc.name, func(t *testing.T) {
			env := lambdaEnv(t)
			ast, iss := env.Compile(tc.expr)
			if iss.Err() != nil {
				t.Fatalf("env.Compile(%q) failed: %v", tc.expr, iss.Err())
			}
			fn, err := env.FunctionValue("fn", ast, tc.params)
			if err != nil {
				t.Fatalf("env.FunctionValue() failed: %v", err)
			}
			if !fn.Signature().IsExactType(tc.typ) {
				t.Errorf("Signature() got %v, wanted %v", fn.Signature(), tc.typ)
			}
			if got := fn.Invoke(nil, tc.args...); got.Equal(tc.out) != types.True {
				t.Errorf("Invoke() got %v, wanted %v", got, tc.out)
			}
		})
	}
}

func TestEnvFunctionValueErrors(t *testing.T) {
	tests := []struct {
		name    string
		expr    string
		params  []string
		checked bool
		err     string
	}{
		{
			name:    "unchecked expression",
			expr:    `a < b`,
			params:  []string{"a", "b"},
			checked: false,
			err:     "requires a checked expression",
		},
		{
			name:   "undeclared parameter",
			expr:   `a < b`,
			params: []string{"a", "c"},
			err:    "undeclared parameter: c",
		},
		{
			name:   "duplicate parameter",
			expr:   `a < b`,
			params: []string{"a", "a"},
			err:    `declares parameter "a" more than once`,
		},
		{
			// Every variable the expression reads has to be supplied by the call.
			name:   "variable which is not a parameter",
			expr:   `a < b`,
			params: []string{"a"},
			err:    `references "b" which is not a parameter of the function`,
		},
		{
			name:   "several variables which are not parameters",
			expr:   `a < b && s == 'x'`,
			params: []string{},
			err:    `references "a", "b" and "s" which are not parameters of the function`,
		},
		{
			// The range of a comprehension is evaluated outside of the loop's scope.
			name:   "variable read by a comprehension range",
			expr:   `l.exists(e, e > 0)`,
			params: []string{},
			err:    `references "l" which is not a parameter of the function`,
		},
		{
			name:   "variable read by a comprehension step",
			expr:   `l.exists(e, e > a)`,
			params: []string{"l"},
			err:    `references "a" which is not a parameter of the function`,
		},
	}
	for _, tst := range tests {
		tc := tst
		t.Run(tc.name, func(t *testing.T) {
			env := lambdaEnv(t)
			ast, iss := env.Parse(tc.expr)
			if iss.Err() != nil {
				t.Fatalf("env.Parse(%q) failed: %v", tc.expr, iss.Err())
			}
			if tc.name != "unchecked expression" {
				ast, iss = env.Check(ast)
				if iss.Err() != nil {
					t.Fatalf("env.Check(%q) failed: %v", tc.expr, iss.Err())
				}
			}
			_, err := env.FunctionValue("fn", ast, tc.params)
			if err == nil {
				t.Fatalf("env.FunctionValue() succeeded, wanted error %q", tc.err)
			}
			if !strings.Contains(err.Error(), tc.err) {
				t.Errorf("env.FunctionValue() got %v, wanted error containing %q", err, tc.err)
			}
		})
	}
}

func TestEnvFunctionValueIgnoresNonVariables(t *testing.T) {
	// Function references, type names and enum constants are resolved before evaluation, so they
	// are not inputs the caller has to supply.
	env := lambdaEnv(t,
		Function("twice",
			Overload("twice_int", []*Type{IntType}, IntType,
				UnaryBinding(func(v ref.Val) ref.Val { return v.(types.Int) * 2 }))),
	)
	ast, iss := env.Compile(`[twice].size() + int(a) + (type(a) == int ? 1 : 0)`)
	if iss.Err() != nil {
		t.Fatalf("env.Compile() failed: %v", iss.Err())
	}
	fn, err := env.FunctionValue("fn", ast, []string{"a"})
	if err != nil {
		t.Fatalf("env.FunctionValue() failed: %v", err)
	}
	if got := fn.Invoke(nil, types.Int(4)); got != types.Int(6) {
		t.Errorf("Invoke() got %v, wanted 6", got)
	}
}

func TestEnvFunctionValueCost(t *testing.T) {
	env := lambdaEnv(t)
	ast, iss := env.Compile(`a < b`)
	if iss.Err() != nil {
		t.Fatalf("env.Compile() failed: %v", iss.Err())
	}
	// The declared cost is the estimated cost of the expression.
	fn, err := env.FunctionValue("cmp", ast, []string{"a", "b"})
	if err != nil {
		t.Fatalf("env.FunctionValue() failed: %v", err)
	}
	if got := fn.CallEstimate().CostEstimate; got != checker.FixedCostEstimate(3) {
		t.Errorf("CallEstimate() got %v, wanted a fixed cost of 3", got)
	}
	// Which may be declared outright instead.
	fn, err = env.FunctionValue("cmp", ast, []string{"a", "b"},
		FunctionValueCallEstimate(FixedCallEstimate(7)))
	if err != nil {
		t.Fatalf("env.FunctionValue() failed: %v", err)
	}
	if got := fn.CallCost(); got != 7 {
		t.Errorf("CallCost() got %d, wanted 7", got)
	}
}

func TestEnvFunctionValueIsSealed(t *testing.T) {
	// A function value built from an expression is closed over its parameters, so a variable of
	// the calling evaluation with the same name as a parameter is not visible to it.
	lambdas := lambdaEnv(t)
	ast, iss := lambdas.Compile(`a`)
	if iss.Err() != nil {
		t.Fatalf("env.Compile() failed: %v", iss.Err())
	}
	identity, err := lambdas.FunctionValue("identity", ast, []string{"a"})
	if err != nil {
		t.Fatalf("env.FunctionValue() failed: %v", err)
	}
	env, err := NewEnv(
		Variable("a", IntType),
		Variable("identity", identity.Signature()),
	)
	if err != nil {
		t.Fatalf("NewEnv() failed: %v", err)
	}
	callAst, iss := env.Compile(`identity(1)`)
	if iss.Err() != nil {
		t.Fatalf("env.Compile() failed: %v", iss.Err())
	}
	prg, err := env.Program(callAst)
	if err != nil {
		t.Fatalf("env.Program() failed: %v", err)
	}
	out, _, err := prg.Eval(map[string]any{"a": 99, "identity": identity})
	if err != nil {
		t.Fatalf("prg.Eval() failed: %v", err)
	}
	if out != types.Int(1) {
		t.Errorf("prg.Eval() got %v, wanted the argument rather than the caller's variable", out)
	}
}
