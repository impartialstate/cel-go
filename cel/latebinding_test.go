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

	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
	"github.com/google/cel-go/interpreter"
)

// isAdminIn returns a unary function which reports whether the input value is in the admin set.
func isAdminIn(admins ...string) func(ref.Val) ref.Val {
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
		out, _, err := prg.Eval(map[string]any{
			"user":     tc.user,
			"is_admin": isAdminIn(tc.admins...),
		})
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

func TestLateFunctionBindingSiblingActivation(t *testing.T) {
	env := lateBoundEnv(t)
	ast, iss := env.Compile(`is_admin(user)`)
	if iss.Err() != nil {
		t.Fatalf("Compile() failed: %v", iss.Err())
	}
	prg, err := env.Program(ast)
	if err != nil {
		t.Fatalf("Program() failed: %v", err)
	}
	// The functions may be kept in an activation of their own, separate from the variables.
	fns, err := NewActivation(map[string]any{"is_admin_string": isAdminIn("alice")})
	if err != nil {
		t.Fatalf("NewActivation() failed: %v", err)
	}
	vars, err := NewActivation(map[string]any{"user": "alice"})
	if err != nil {
		t.Fatalf("NewActivation() failed: %v", err)
	}
	out, _, err := prg.Eval(interpreter.NewHierarchicalActivation(fns, vars))
	if err != nil {
		t.Fatalf("Eval() failed: %v", err)
	}
	if out != types.True {
		t.Errorf("Eval() got %v, wanted true", out)
	}
}

func TestLateFunctionBindingGlobals(t *testing.T) {
	env := lateBoundEnv(t)
	ast, iss := env.Compile(`is_admin(user)`)
	if iss.Err() != nil {
		t.Fatalf("Compile() failed: %v", iss.Err())
	}
	// A binding may also be supplied once for the lifetime of the program.
	prg, err := env.Program(ast, Globals(map[string]any{"is_admin": isAdminIn("alice")}))
	if err != nil {
		t.Fatalf("Program() failed: %v", err)
	}
	out, _, err := prg.Eval(map[string]any{"user": "alice"})
	if err != nil {
		t.Fatalf("Eval() failed: %v", err)
	}
	if out != types.True {
		t.Errorf("Eval() got %v, wanted true", out)
	}
}

func TestLateFunctionBindingEvalOptions(t *testing.T) {
	env := lateBoundEnv(t)
	ast, iss := env.Compile(`is_admin(user) && is_admin('bob')`)
	if iss.Err() != nil {
		t.Fatalf("Compile() failed: %v", iss.Err())
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
			out, det, err := prg.Eval(map[string]any{
				"user":     "alice",
				"is_admin": isAdminIn("alice", "bob"),
			})
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
	out, _, err := prg.ContextEval(context.Background(), map[string]any{
		"user":     "alice",
		"is_admin": isAdminIn("alice"),
	})
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
	vars, err := PartialVars(
		map[string]any{"user": "alice", "is_admin": isAdminIn("bob")},
		AttributePattern("resource"))
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
	out, _, err := prg.Eval(map[string]any{"is_admin": isAdminIn("alice")})
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
	out, _, err := prg.Eval(map[string]any{
		"user":            "alice",
		"string_is_admin": isAdminIn("alice"),
	})
	if err != nil {
		t.Fatalf("Eval() failed: %v", err)
	}
	if out != types.True {
		t.Errorf("Eval() got %v, wanted true", out)
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
	// Each overload is bound by its own id, while the function name serves parse-only
	// expressions which have not resolved an overload.
	fns := map[string]any{
		"describe_string": func(v ref.Val) ref.Val { return types.String("string:" + string(v.(types.String))) },
		"describe_int":    func(v ref.Val) ref.Val { return types.String("int") },
		"describe": func(v ref.Val) ref.Val {
			if _, ok := v.(types.String); ok {
				return types.String("string:" + string(v.(types.String)))
			}
			return types.String("int")
		},
	}
	tests := []struct {
		expr      string
		unchecked bool
		out       ref.Val
	}{
		{expr: `describe('hi')`, out: types.String("string:hi")},
		{expr: `describe(1)`, out: types.String("int")},
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
		out, _, err := prg.Eval(fns)
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
	out, _, err := prg.Eval(map[string]any{
		"user":     "alice",
		"users":    []string{"alice", "bob", "carl"},
		"is_admin": isAdminIn("alice", "carl"),
	})
	if err != nil {
		t.Fatalf("Eval() failed: %v", err)
	}
	want := types.DefaultTypeAdapter.NativeToValue([]string{"alice", "carl"})
	if out.Equal(want) != types.True {
		t.Errorf("Eval() got %v, wanted %v", out, want)
	}
}
