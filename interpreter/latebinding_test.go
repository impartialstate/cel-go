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
	"strings"
	"testing"

	"github.com/google/cel-go/common/decls"
	"github.com/google/cel-go/common/functions"
	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
	"github.com/google/cel-go/common/types/traits"
)

// lateBoundFunc declares a function whose overloads are resolved at evaluation time.
func lateBoundFunc(t testing.TB, name string, opts ...decls.FunctionOpt) *decls.FunctionDecl {
	t.Helper()
	fn, err := decls.NewFunction(name, opts...)
	if err != nil {
		t.Fatalf("decls.NewFunction(%q) failed: %v", name, err)
	}
	return fn
}

// isAlice reports whether the input value is the string 'alice'.
func isAlice(v ref.Val) ref.Val {
	return types.Bool(v.Equal(types.String("alice")) == types.True)
}

func TestLateBindingCalls(t *testing.T) {
	greet := func(args ...ref.Val) ref.Val {
		parts := make([]string, len(args))
		for i, arg := range args {
			parts[i] = string(arg.(types.String))
		}
		return types.String("hello " + strings.Join(parts, ", "))
	}
	tests := []struct {
		name      string
		expr      string
		unchecked bool
		vars      []*decls.VariableDecl
		funcs     []*decls.FunctionDecl
		in        map[string]any
		out       ref.Val
		err       string
	}{
		{
			name: "zero arity",
			expr: `request_id()`,
			funcs: []*decls.FunctionDecl{
				lateBoundFunc(t, "request_id",
					decls.Overload("request_id", []*types.Type{}, types.StringType,
						decls.LateFunctionBinding())),
			},
			in:  map[string]any{"request_id": func(...ref.Val) ref.Val { return types.String("abc") }},
			out: types.String("abc"),
		},
		{
			name: "unary",
			expr: `is_admin(user)`,
			vars: []*decls.VariableDecl{decls.NewVariable("user", types.StringType)},
			funcs: []*decls.FunctionDecl{
				lateBoundFunc(t, "is_admin",
					decls.Overload("is_admin_string", []*types.Type{types.StringType}, types.BoolType,
						decls.LateFunctionBinding())),
			},
			in:  map[string]any{"user": "alice", "is_admin_string": isAlice},
			out: types.True,
		},
		{
			name: "unary bound to the function name",
			expr: `is_admin(user)`,
			vars: []*decls.VariableDecl{decls.NewVariable("user", types.StringType)},
			funcs: []*decls.FunctionDecl{
				lateBoundFunc(t, "is_admin",
					decls.Overload("is_admin_string", []*types.Type{types.StringType}, types.BoolType,
						decls.LateFunctionBinding())),
			},
			in:  map[string]any{"user": "alice", "is_admin": isAlice},
			out: types.True,
		},
		{
			name: "unary bound as a functions.UnaryOp",
			expr: `is_admin(user)`,
			vars: []*decls.VariableDecl{decls.NewVariable("user", types.StringType)},
			funcs: []*decls.FunctionDecl{
				lateBoundFunc(t, "is_admin",
					decls.Overload("is_admin_string", []*types.Type{types.StringType}, types.BoolType,
						decls.LateFunctionBinding())),
			},
			in:  map[string]any{"user": "alice", "is_admin": functions.UnaryOp(isAlice)},
			out: types.True,
		},
		{
			name: "overload id preferred over function name",
			expr: `is_admin(user)`,
			vars: []*decls.VariableDecl{decls.NewVariable("user", types.StringType)},
			funcs: []*decls.FunctionDecl{
				lateBoundFunc(t, "is_admin",
					decls.Overload("is_admin_string", []*types.Type{types.StringType}, types.BoolType,
						decls.LateFunctionBinding())),
			},
			in: map[string]any{
				"user":            "alice",
				"is_admin_string": isAlice,
				"is_admin":        func(v ref.Val) ref.Val { return types.False },
			},
			out: types.True,
		},
		{
			name: "binary",
			expr: `can_access(user, 'secrets')`,
			vars: []*decls.VariableDecl{decls.NewVariable("user", types.StringType)},
			funcs: []*decls.FunctionDecl{
				lateBoundFunc(t, "can_access",
					decls.Overload("can_access_string_string",
						[]*types.Type{types.StringType, types.StringType}, types.BoolType,
						decls.LateFunctionBinding())),
			},
			in: map[string]any{
				"user": "alice",
				"can_access": func(u, res ref.Val) ref.Val {
					return types.Bool(u.Equal(types.String("alice")) == types.True &&
						res.Equal(types.String("secrets")) == types.True)
				},
			},
			out: types.True,
		},
		{
			name: "var args",
			expr: `greet('alice', 'bob', 'carl')`,
			funcs: []*decls.FunctionDecl{
				lateBoundFunc(t, "greet",
					decls.Overload("greet_strings",
						[]*types.Type{types.StringType, types.StringType, types.StringType},
						types.StringType, decls.LateFunctionBinding())),
			},
			in:  map[string]any{"greet": greet},
			out: types.String("hello alice, bob, carl"),
		},
		{
			name: "unary resolved by a var args binding",
			expr: `greet('alice')`,
			funcs: []*decls.FunctionDecl{
				lateBoundFunc(t, "greet",
					decls.Overload("greet_string", []*types.Type{types.StringType}, types.StringType,
						decls.LateFunctionBinding())),
			},
			in:  map[string]any{"greet": functions.FunctionOp(greet)},
			out: types.String("hello alice"),
		},
		{
			name: "member overload",
			expr: `user.isAdmin()`,
			vars: []*decls.VariableDecl{decls.NewVariable("user", types.StringType)},
			funcs: []*decls.FunctionDecl{
				lateBoundFunc(t, "isAdmin",
					decls.MemberOverload("string_is_admin", []*types.Type{types.StringType},
						types.BoolType, decls.LateFunctionBinding())),
			},
			in:  map[string]any{"user": "alice", "string_is_admin": isAlice},
			out: types.True,
		},
		{
			name: "resolved within comprehension",
			expr: `['alice', 'bob'].filter(u, is_admin(u))`,
			funcs: []*decls.FunctionDecl{
				lateBoundFunc(t, "is_admin",
					decls.Overload("is_admin_string", []*types.Type{types.StringType}, types.BoolType,
						decls.LateFunctionBinding())),
			},
			in:  map[string]any{"is_admin": isAlice},
			out: types.DefaultTypeAdapter.NativeToValue([]string{"alice"}),
		},
		{
			name:      "parse-only resolved by function name",
			expr:      `is_admin('alice')`,
			unchecked: true,
			funcs: []*decls.FunctionDecl{
				lateBoundFunc(t, "is_admin",
					decls.Overload("is_admin_string", []*types.Type{types.StringType}, types.BoolType,
						decls.LateFunctionBinding())),
			},
			in:  map[string]any{"is_admin": isAlice},
			out: types.True,
		},
		{
			name: "multiple overloads dispatch by overload id",
			expr: `size_of('hello') + size_of([1, 2])`,
			funcs: []*decls.FunctionDecl{
				lateBoundFunc(t, "size_of",
					decls.Overload("size_of_string", []*types.Type{types.StringType}, types.IntType,
						decls.LateFunctionBinding()),
					decls.Overload("size_of_list",
						[]*types.Type{types.NewListType(types.IntType)}, types.IntType,
						decls.LateFunctionBinding())),
			},
			in: map[string]any{
				"size_of_string": func(v ref.Val) ref.Val { return types.Int(len(string(v.(types.String)))) },
				"size_of_list":   func(v ref.Val) ref.Val { return v.(traits.Sizer).Size() },
			},
			out: types.Int(7),
		},
		{
			name: "operand trait satisfied",
			expr: `count('hello')`,
			funcs: []*decls.FunctionDecl{
				lateBoundFunc(t, "count",
					decls.Overload("count_any", []*types.Type{types.DynType}, types.IntType,
						decls.LateFunctionBinding(),
						decls.OverloadOperandTrait(traits.SizerType))),
			},
			in:  map[string]any{"count": func(v ref.Val) ref.Val { return v.(traits.Sizer).Size() }},
			out: types.Int(5),
		},
		{
			name: "operand trait unsatisfied",
			expr: `count(1)`,
			funcs: []*decls.FunctionDecl{
				lateBoundFunc(t, "count",
					decls.Overload("count_any", []*types.Type{types.DynType}, types.IntType,
						decls.LateFunctionBinding(),
						decls.OverloadOperandTrait(traits.SizerType))),
			},
			in:  map[string]any{"count": func(v ref.Val) ref.Val { return v.(traits.Sizer).Size() }},
			err: "no such overload: count",
		},
		{
			name: "non-strict binding observes unknown",
			expr: `describe(unknown_var)`,
			vars: []*decls.VariableDecl{decls.NewVariable("unknown_var", types.DynType)},
			funcs: []*decls.FunctionDecl{
				lateBoundFunc(t, "describe",
					decls.Overload("describe_any", []*types.Type{types.DynType}, types.StringType,
						decls.LateFunctionBinding(), decls.OverloadIsNonStrict())),
			},
			in: map[string]any{
				"unknown_var": types.NewUnknown(1, nil),
				"describe": func(v ref.Val) ref.Val {
					if types.IsUnknown(v) {
						return types.String("unknown")
					}
					return types.String("known")
				},
			},
			out: types.String("unknown"),
		},
		{
			name: "strict binding propagates unknown",
			expr: `describe(unknown_var)`,
			vars: []*decls.VariableDecl{decls.NewVariable("unknown_var", types.DynType)},
			funcs: []*decls.FunctionDecl{
				lateBoundFunc(t, "describe",
					decls.Overload("describe_any", []*types.Type{types.DynType}, types.StringType,
						decls.LateFunctionBinding())),
			},
			in: map[string]any{
				"unknown_var": types.NewUnknown(1, nil),
				"describe":    func(v ref.Val) ref.Val { return types.String("known") },
			},
			out: types.NewUnknown(1, nil),
		},
		{
			name: "overload binding supplies traits and strictness",
			expr: `describe(unknown_var)`,
			vars: []*decls.VariableDecl{decls.NewVariable("unknown_var", types.DynType)},
			funcs: []*decls.FunctionDecl{
				lateBoundFunc(t, "describe",
					decls.Overload("describe_any", []*types.Type{types.DynType}, types.StringType,
						decls.LateFunctionBinding())),
			},
			in: map[string]any{
				"unknown_var": types.NewUnknown(1, nil),
				"describe": &functions.Overload{
					Operator:  "describe_any",
					NonStrict: true,
					Unary: func(v ref.Val) ref.Val {
						if types.IsUnknown(v) {
							return types.String("unknown")
						}
						return types.String("known")
					},
				},
			},
			out: types.String("unknown"),
		},
		{
			name: "no binding supplied",
			expr: `is_admin('alice')`,
			funcs: []*decls.FunctionDecl{
				lateBoundFunc(t, "is_admin",
					decls.Overload("is_admin_string", []*types.Type{types.StringType}, types.BoolType,
						decls.LateFunctionBinding())),
			},
			err: "no such overload: is_admin",
		},
		{
			name: "binding for a different overload",
			expr: `is_admin('alice')`,
			funcs: []*decls.FunctionDecl{
				lateBoundFunc(t, "is_admin",
					decls.Overload("is_admin_string", []*types.Type{types.StringType}, types.BoolType,
						decls.LateFunctionBinding()),
					decls.Overload("is_admin_int", []*types.Type{types.IntType}, types.BoolType,
						decls.LateFunctionBinding())),
			},
			in:  map[string]any{"is_admin_int": isAlice},
			err: "no such overload: is_admin",
		},
		{
			name: "binding is not a function",
			expr: `is_admin('alice')`,
			funcs: []*decls.FunctionDecl{
				lateBoundFunc(t, "is_admin",
					decls.Overload("is_admin_string", []*types.Type{types.StringType}, types.BoolType,
						decls.LateFunctionBinding())),
			},
			in:  map[string]any{"is_admin": "not a function"},
			err: "no such overload: is_admin",
		},
		{
			name: "variable of the same name does not shadow the binding",
			expr: `is_admin('alice')`,
			funcs: []*decls.FunctionDecl{
				lateBoundFunc(t, "is_admin",
					decls.Overload("is_admin_string", []*types.Type{types.StringType}, types.BoolType,
						decls.LateFunctionBinding())),
			},
			in:  map[string]any{"is_admin": true, "is_admin_string": isAlice},
			out: types.True,
		},
		{
			name: "zero arity bound to a unary function",
			expr: `request_id()`,
			funcs: []*decls.FunctionDecl{
				lateBoundFunc(t, "request_id",
					decls.Overload("request_id", []*types.Type{}, types.StringType,
						decls.LateFunctionBinding())),
			},
			in:  map[string]any{"request_id": func(v ref.Val) ref.Val { return v }},
			err: "no such overload: request_id()",
		},
	}
	for _, tst := range tests {
		tc := tst
		t.Run(tc.name, func(t *testing.T) {
			prg, vars, err := program(t, &testCase{
				name:      tc.name,
				expr:      tc.expr,
				vars:      tc.vars,
				funcs:     tc.funcs,
				in:        tc.in,
				unchecked: tc.unchecked,
			})
			if err != nil {
				t.Fatalf("program() failed: %v", err)
			}
			out := prg.Eval(vars)
			if tc.err != "" {
				if !types.IsError(out) {
					t.Fatalf("Eval() got %v, wanted error %q", out, tc.err)
				}
				if !strings.Contains(out.(*types.Err).Error(), tc.err) {
					t.Errorf("Eval() got error %v, wanted %q", out, tc.err)
				}
				return
			}
			if types.IsError(out) {
				t.Fatalf("Eval() failed: %v", out)
			}
			if unk, isUnk := tc.out.(*types.Unknown); isUnk {
				gotUnk, ok := out.(*types.Unknown)
				if !ok || gotUnk.String() != unk.String() {
					t.Errorf("Eval() got %v, wanted unknown %v", out, unk)
				}
				return
			}
			if out.Equal(tc.out) != types.True {
				t.Errorf("Eval() got %v, wanted %v", out, tc.out)
			}
		})
	}
}

// TestLateBindingActivationHierarchy verifies that a binding is found wherever it is supplied
// within an activation hierarchy, since resolution reuses ordinary variable name resolution.
func TestLateBindingActivationHierarchy(t *testing.T) {
	prg, vars, err := program(t, &testCase{
		expr: `is_admin(user)`,
		vars: []*decls.VariableDecl{decls.NewVariable("user", types.StringType)},
		funcs: []*decls.FunctionDecl{
			lateBoundFunc(t, "is_admin",
				decls.Overload("is_admin_string", []*types.Type{types.StringType}, types.BoolType,
					decls.LateFunctionBinding())),
		},
		in: map[string]any{"user": "alice"},
	})
	if err != nil {
		t.Fatalf("program() failed: %v", err)
	}
	// The functions may be supplied as a sibling of the variables rather than alongside them.
	fns, err := NewActivation(map[string]any{"is_admin_string": isAlice})
	if err != nil {
		t.Fatalf("NewActivation() failed: %v", err)
	}
	partialVars, err := NewPartialActivation(NewHierarchicalActivation(fns, vars))
	if err != nil {
		t.Fatalf("NewPartialActivation() failed: %v", err)
	}
	tests := []struct {
		name string
		vars Activation
	}{
		{name: "functions in the parent", vars: NewHierarchicalActivation(fns, vars)},
		{name: "functions in the child", vars: NewHierarchicalActivation(vars, fns)},
		{name: "partial activation", vars: partialVars},
		{name: "activation wrapper", vars: &testActivationWrapper{
			Activation: NewHierarchicalActivation(fns, vars), name: "wrapper"}},
		{name: "state tracking activation", vars: evalStateActivation{
			vars: NewHierarchicalActivation(fns, vars), state: NewEvalState()}},
	}
	for _, tst := range tests {
		tc := tst
		t.Run(tc.name, func(t *testing.T) {
			out := prg.Eval(tc.vars)
			if out != types.True {
				t.Errorf("Eval() got %v, wanted true", out)
			}
		})
	}
}
