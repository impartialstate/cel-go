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
	"context"
	"strings"
	"testing"

	"github.com/google/cel-go/common/decls"
	"github.com/google/cel-go/common/functions"
	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
)

// isAdminIn returns a unary binding which reports whether the input value is in the admin set.
func isAdminIn(admins ...string) functions.UnaryOp {
	set := make(map[string]bool, len(admins))
	for _, a := range admins {
		set[a] = true
	}
	return func(u ref.Val) ref.Val {
		s, ok := u.(types.String)
		if !ok {
			return types.MaybeNoSuchOverloadErr(u)
		}
		return types.Bool(set[string(s)])
	}
}

func lateBoundEnv(t testing.TB, opts ...EnvOption) *Env {
	t.Helper()
	opts = append([]EnvOption{
		Variable("user", StringType),
		Function("is_admin",
			Overload("is_admin_string", []*Type{StringType}, BoolType,
				LateFunctionBinding())),
	}, opts...)
	env, err := NewEnv(opts...)
	if err != nil {
		t.Fatalf("NewEnv() failed: %v", err)
	}
	return env
}

func TestLateFunctionBindingEval(t *testing.T) {
	env := lateBoundEnv(t)
	ast, iss := env.Compile(`is_admin(user)`)
	if iss.Err() != nil {
		t.Fatalf("Compile() failed: %v", iss.Err())
	}
	prg, err := env.Program(ast)
	if err != nil {
		t.Fatalf("Program() failed: %v", err)
	}
	// The same program may be evaluated with different implementations of the same function.
	tests := []struct {
		admins []string
		user   string
		out    ref.Val
	}{
		{admins: []string{"alice"}, user: "alice", out: types.True},
		{admins: []string{"alice"}, user: "bob", out: types.False},
		{admins: []string{"bob"}, user: "bob", out: types.True},
	}
	for _, tc := range tests {
		fns, err := NewLateFunctionBindings(
			LateFunction("is_admin",
				Overload("is_admin_string", []*Type{StringType}, BoolType,
					UnaryBinding(isAdminIn(tc.admins...)))),
		)
		if err != nil {
			t.Fatalf("NewLateFunctionBindings() failed: %v", err)
		}
		vars, err := NewLateBindingActivation(map[string]any{"user": tc.user}, fns)
		if err != nil {
			t.Fatalf("NewLateBindingActivation() failed: %v", err)
		}
		out, _, err := prg.Eval(vars)
		if err != nil {
			t.Fatalf("Eval() failed: %v", err)
		}
		if out != tc.out {
			t.Errorf("Eval(%q) with admins %v got %v, wanted %v", tc.user, tc.admins, out, tc.out)
		}
	}
}

func TestLateFunctionBindingMissing(t *testing.T) {
	env := lateBoundEnv(t)
	ast, iss := env.Compile(`is_admin(user)`)
	if iss.Err() != nil {
		t.Fatalf("Compile() failed: %v", iss.Err())
	}
	prg, err := env.Program(ast)
	if err != nil {
		t.Fatalf("Program() failed: %v", err)
	}
	_, _, err = prg.Eval(map[string]any{"user": "alice"})
	if err == nil || !strings.Contains(err.Error(), "no such overload: is_admin") {
		t.Errorf("Eval() got error %v, wanted 'no such overload: is_admin'", err)
	}
}

func TestLateFunctionBindingEvalOptions(t *testing.T) {
	env := lateBoundEnv(t)
	ast, iss := env.Compile(`is_admin(user) && is_admin('bob')`)
	if iss.Err() != nil {
		t.Fatalf("Compile() failed: %v", iss.Err())
	}
	fns, err := NewLateFunctionBindings(
		LateFunction("is_admin",
			Overload("is_admin_string", []*Type{StringType}, BoolType,
				UnaryBinding(isAdminIn("alice", "bob")))),
	)
	if err != nil {
		t.Fatalf("NewLateFunctionBindings() failed: %v", err)
	}
	tests := []struct {
		name string
		opts []ProgramOption
	}{
		{name: "default"},
		{name: "optimize", opts: []ProgramOption{EvalOptions(OptOptimize)}},
		{name: "track state", opts: []ProgramOption{EvalOptions(OptTrackState)}},
		{name: "exhaustive", opts: []ProgramOption{EvalOptions(OptExhaustiveEval)}},
		{name: "track cost", opts: []ProgramOption{EvalOptions(OptTrackCost)}},
		{name: "partial eval", opts: []ProgramOption{EvalOptions(OptPartialEval)}},
	}
	for _, tst := range tests {
		tc := tst
		t.Run(tc.name, func(t *testing.T) {
			prg, err := env.Program(ast, tc.opts...)
			if err != nil {
				t.Fatalf("Program() failed: %v", err)
			}
			vars, err := NewLateBindingActivation(map[string]any{"user": "alice"}, fns)
			if err != nil {
				t.Fatalf("NewLateBindingActivation() failed: %v", err)
			}
			out, det, err := prg.Eval(vars)
			if err != nil {
				t.Fatalf("Eval() failed: %v", err)
			}
			if out != types.True {
				t.Errorf("Eval() got %v, wanted true", out)
			}
			if tc.name == "track cost" {
				if cost := det.ActualCost(); cost == nil || *cost == 0 {
					t.Errorf("ActualCost() got %v, wanted a non-zero cost", cost)
				}
			}
		})
	}
}

func TestLateFunctionBindingContextEval(t *testing.T) {
	env := lateBoundEnv(t)
	ast, iss := env.Compile(`is_admin(user)`)
	if iss.Err() != nil {
		t.Fatalf("Compile() failed: %v", iss.Err())
	}
	prg, err := env.Program(ast, EvalOptions(OptTrackCost))
	if err != nil {
		t.Fatalf("Program() failed: %v", err)
	}
	fns, err := NewLateFunctionBindings(
		LateFunction("is_admin",
			Overload("is_admin_string", []*Type{StringType}, BoolType,
				UnaryBinding(isAdminIn("alice")))),
	)
	if err != nil {
		t.Fatalf("NewLateFunctionBindings() failed: %v", err)
	}
	vars, err := NewLateBindingActivation(map[string]any{"user": "alice"}, fns)
	if err != nil {
		t.Fatalf("NewLateBindingActivation() failed: %v", err)
	}
	out, _, err := prg.ContextEval(context.Background(), vars)
	if err != nil {
		t.Fatalf("ContextEval() failed: %v", err)
	}
	if out != types.True {
		t.Errorf("ContextEval() got %v, wanted true", out)
	}
}

func TestLateFunctionBindingPartialEval(t *testing.T) {
	env := lateBoundEnv(t, Variable("resource", StringType))
	ast, iss := env.Compile(`is_admin(user) || resource == 'public'`)
	if iss.Err() != nil {
		t.Fatalf("Compile() failed: %v", iss.Err())
	}
	prg, err := env.Program(ast, EvalOptions(OptTrackState, OptPartialEval))
	if err != nil {
		t.Fatalf("Program() failed: %v", err)
	}
	fns, err := NewLateFunctionBindings(
		LateFunction("is_admin",
			Overload("is_admin_string", []*Type{StringType}, BoolType,
				UnaryBinding(isAdminIn("bob")))),
	)
	if err != nil {
		t.Fatalf("NewLateFunctionBindings() failed: %v", err)
	}
	lateVars, err := NewLateBindingActivation(map[string]any{"user": "alice"}, fns)
	if err != nil {
		t.Fatalf("NewLateBindingActivation() failed: %v", err)
	}
	vars, err := PartialVars(lateVars, AttributePattern("resource"))
	if err != nil {
		t.Fatalf("PartialVars() failed: %v", err)
	}
	out, det, err := prg.Eval(vars)
	if err != nil {
		t.Fatalf("Eval() failed: %v", err)
	}
	if !types.IsUnknown(out) {
		t.Fatalf("Eval() got %v, wanted unknown", out)
	}
	residual, err := env.ResidualAst(ast, det)
	if err != nil {
		t.Fatalf("ResidualAst() failed: %v", err)
	}
	expr, err := AstToString(residual)
	if err != nil {
		t.Fatalf("AstToString() failed: %v", err)
	}
	if expr != `resource == "public"` {
		t.Errorf("AstToString() got %q, wanted 'resource == \"public\"'", expr)
	}
}

func TestLateFunctionBindingParseOnly(t *testing.T) {
	env := lateBoundEnv(t)
	ast, iss := env.Parse(`is_admin('alice')`)
	if iss.Err() != nil {
		t.Fatalf("Parse() failed: %v", iss.Err())
	}
	prg, err := env.Program(ast)
	if err != nil {
		t.Fatalf("Program() failed: %v", err)
	}
	fns, err := NewLateFunctionBindings(
		LateFunction("is_admin",
			Overload("is_admin_string", []*Type{StringType}, BoolType,
				UnaryBinding(isAdminIn("alice")))),
	)
	if err != nil {
		t.Fatalf("NewLateFunctionBindings() failed: %v", err)
	}
	vars, err := NewLateBindingActivation(NoVars(), fns)
	if err != nil {
		t.Fatalf("NewLateBindingActivation() failed: %v", err)
	}
	out, _, err := prg.Eval(vars)
	if err != nil {
		t.Fatalf("Eval() failed: %v", err)
	}
	if out != types.True {
		t.Errorf("Eval() got %v, wanted true", out)
	}
}

func TestLateFunctionBindingMemberOverload(t *testing.T) {
	env, err := NewEnv(
		Variable("user", StringType),
		Function("isAdmin",
			MemberOverload("string_is_admin", []*Type{StringType}, BoolType,
				LateFunctionBinding())),
	)
	if err != nil {
		t.Fatalf("NewEnv() failed: %v", err)
	}
	ast, iss := env.Compile(`user.isAdmin()`)
	if iss.Err() != nil {
		t.Fatalf("Compile() failed: %v", iss.Err())
	}
	prg, err := env.Program(ast)
	if err != nil {
		t.Fatalf("Program() failed: %v", err)
	}
	fns, err := NewLateFunctionBindings(
		LateFunction("isAdmin",
			MemberOverload("string_is_admin", []*Type{StringType}, BoolType,
				UnaryBinding(isAdminIn("alice")))),
	)
	if err != nil {
		t.Fatalf("NewLateFunctionBindings() failed: %v", err)
	}
	vars, err := NewLateBindingActivation(map[string]any{"user": "alice"}, fns)
	if err != nil {
		t.Fatalf("NewLateBindingActivation() failed: %v", err)
	}
	out, _, err := prg.Eval(vars)
	if err != nil {
		t.Fatalf("Eval() failed: %v", err)
	}
	if out != types.True {
		t.Errorf("Eval() got %v, wanted true", out)
	}
}

func TestLateFunctionBindingTypeGuard(t *testing.T) {
	env, err := NewEnv(
		Variable("value", DynType),
		Function("is_admin",
			Overload("is_admin_string", []*Type{DynType}, BoolType,
				LateFunctionBinding())),
	)
	if err != nil {
		t.Fatalf("NewEnv() failed: %v", err)
	}
	ast, iss := env.Compile(`is_admin(value)`)
	if iss.Err() != nil {
		t.Fatalf("Compile() failed: %v", iss.Err())
	}
	prg, err := env.Program(ast)
	if err != nil {
		t.Fatalf("Program() failed: %v", err)
	}
	// The runtime binding declares a string argument, so the generated type-guard rejects an int.
	fns, err := NewLateFunctionBindings(
		LateFunction("is_admin",
			Overload("is_admin_string", []*Type{StringType}, BoolType,
				UnaryBinding(isAdminIn("alice")))),
	)
	if err != nil {
		t.Fatalf("NewLateFunctionBindings() failed: %v", err)
	}
	vars, err := NewLateBindingActivation(map[string]any{"value": 42}, fns)
	if err != nil {
		t.Fatalf("NewLateBindingActivation() failed: %v", err)
	}
	_, _, err = prg.Eval(vars)
	if err == nil || !strings.Contains(err.Error(), "no such overload") {
		t.Errorf("Eval() got error %v, wanted 'no such overload'", err)
	}
}

func TestLateFunctionBindingMultipleOverloads(t *testing.T) {
	env, err := NewEnv(
		Variable("value", DynType),
		Function("describe",
			Overload("describe_string", []*Type{StringType}, StringType, LateFunctionBinding()),
			Overload("describe_int", []*Type{IntType}, StringType, LateFunctionBinding())),
	)
	if err != nil {
		t.Fatalf("NewEnv() failed: %v", err)
	}
	fns, err := NewLateFunctionBindings(
		LateFunction("describe",
			Overload("describe_string", []*Type{StringType}, StringType,
				UnaryBinding(func(v ref.Val) ref.Val { return types.String("string:" + string(v.(types.String))) })),
			Overload("describe_int", []*Type{IntType}, StringType,
				UnaryBinding(func(v ref.Val) ref.Val { return types.String("int") }))),
	)
	if err != nil {
		t.Fatalf("NewLateFunctionBindings() failed: %v", err)
	}
	tests := []struct {
		expr      string
		unchecked bool
		out       ref.Val
	}{
		{expr: `describe('hi')`, out: types.String("string:hi")},
		{expr: `describe(1)`, out: types.String("int")},
		// Parse-only expressions dispatch dynamically by function name.
		{expr: `describe('hi')`, unchecked: true, out: types.String("string:hi")},
		{expr: `describe(1)`, unchecked: true, out: types.String("int")},
	}
	for _, tc := range tests {
		var ast *Ast
		var iss *Issues
		if tc.unchecked {
			ast, iss = env.Parse(tc.expr)
		} else {
			ast, iss = env.Compile(tc.expr)
		}
		if iss.Err() != nil {
			t.Fatalf("Compile(%q) failed: %v", tc.expr, iss.Err())
		}
		prg, err := env.Program(ast)
		if err != nil {
			t.Fatalf("Program() failed: %v", err)
		}
		vars, err := NewLateBindingActivation(NoVars(), fns)
		if err != nil {
			t.Fatalf("NewLateBindingActivation() failed: %v", err)
		}
		out, _, err := prg.Eval(vars)
		if err != nil {
			t.Fatalf("Eval(%q) failed: %v", tc.expr, err)
		}
		if out.Equal(tc.out) != types.True {
			t.Errorf("Eval(%q) got %v, wanted %v", tc.expr, out, tc.out)
		}
	}
}

func TestLateFunctionBindingComprehension(t *testing.T) {
	env := lateBoundEnv(t, Variable("users", ListType(StringType)))
	ast, iss := env.Compile(`users.filter(u, is_admin(u))`)
	if iss.Err() != nil {
		t.Fatalf("Compile() failed: %v", iss.Err())
	}
	prg, err := env.Program(ast)
	if err != nil {
		t.Fatalf("Program() failed: %v", err)
	}
	fns, err := NewLateFunctionBindings(
		LateFunction("is_admin",
			Overload("is_admin_string", []*Type{StringType}, BoolType,
				UnaryBinding(isAdminIn("alice", "carl")))),
	)
	if err != nil {
		t.Fatalf("NewLateFunctionBindings() failed: %v", err)
	}
	vars, err := NewLateBindingActivation(
		map[string]any{"user": "alice", "users": []string{"alice", "bob", "carl"}}, fns)
	if err != nil {
		t.Fatalf("NewLateBindingActivation() failed: %v", err)
	}
	out, _, err := prg.Eval(vars)
	if err != nil {
		t.Fatalf("Eval() failed: %v", err)
	}
	want := types.DefaultTypeAdapter.NativeToValue([]string{"alice", "carl"})
	if out.Equal(want) != types.True {
		t.Errorf("Eval() got %v, wanted %v", out, want)
	}
}

func TestLateFunctionBindingsOptions(t *testing.T) {
	isAdmin, err := decls.NewFunction("is_admin",
		decls.Overload("is_admin_string", []*Type{StringType}, BoolType,
			decls.UnaryBinding(isAdminIn("alice"))))
	if err != nil {
		t.Fatalf("decls.NewFunction() failed: %v", err)
	}
	tests := []struct {
		name string
		opt  LateFunctionBindingsOpt
	}{
		{name: "function decls", opt: LateFunctionDecls(isAdmin)},
		{
			name: "raw overloads",
			opt: LateOverloads(&functions.Overload{
				Operator: "is_admin_string",
				Unary:    isAdminIn("alice"),
			}),
		},
	}
	env := lateBoundEnv(t)
	ast, iss := env.Compile(`is_admin(user)`)
	if iss.Err() != nil {
		t.Fatalf("Compile() failed: %v", iss.Err())
	}
	prg, err := env.Program(ast)
	if err != nil {
		t.Fatalf("Program() failed: %v", err)
	}
	for _, tst := range tests {
		tc := tst
		t.Run(tc.name, func(t *testing.T) {
			fns, err := NewLateFunctionBindings(tc.opt)
			if err != nil {
				t.Fatalf("NewLateFunctionBindings() failed: %v", err)
			}
			vars, err := NewLateBindingActivation(map[string]any{"user": "alice"}, fns)
			if err != nil {
				t.Fatalf("NewLateBindingActivation() failed: %v", err)
			}
			out, _, err := prg.Eval(vars)
			if err != nil {
				t.Fatalf("Eval() failed: %v", err)
			}
			if out != types.True {
				t.Errorf("Eval() got %v, wanted true", out)
			}
		})
	}
}

func TestNewLateFunctionBindingsErrors(t *testing.T) {
	tests := []struct {
		name string
		opts []LateFunctionBindingsOpt
		err  string
	}{
		{
			name: "declaration without an implementation",
			opts: []LateFunctionBindingsOpt{
				LateFunction("is_admin",
					Overload("is_admin_string", []*Type{StringType}, BoolType)),
			},
			err: "late function binding must provide an implementation: is_admin",
		},
		{
			name: "declaration with a late binding",
			opts: []LateFunctionBindingsOpt{
				LateFunction("is_admin",
					Overload("is_admin_string", []*Type{StringType}, BoolType,
						LateFunctionBinding())),
			},
			err: "late function binding must provide an implementation: is_admin",
		},
		{
			name: "invalid declaration",
			opts: []LateFunctionBindingsOpt{
				LateFunction("is_admin",
					Overload("is_admin_string", []*Type{StringType}, BoolType,
						UnaryBinding(isAdminIn("alice")),
						UnaryBinding(isAdminIn("bob")))),
			},
			err: "already has a binding",
		},
		{
			name: "nil declaration",
			opts: []LateFunctionBindingsOpt{LateFunctionDecls(nil)},
			err:  "function declaration must be non-nil",
		},
		{
			name: "duplicate bindings",
			opts: []LateFunctionBindingsOpt{
				LateFunction("is_admin",
					Overload("is_admin_string", []*Type{StringType}, BoolType,
						UnaryBinding(isAdminIn("alice")))),
				LateFunction("is_admin",
					Overload("is_admin_string", []*Type{StringType}, BoolType,
						UnaryBinding(isAdminIn("bob")))),
			},
			err: "overload already exists 'is_admin_string'",
		},
	}
	for _, tst := range tests {
		tc := tst
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewLateFunctionBindings(tc.opts...)
			if err == nil || !strings.Contains(err.Error(), tc.err) {
				t.Errorf("NewLateFunctionBindings() got error %v, wanted %q", err, tc.err)
			}
		})
	}
}
