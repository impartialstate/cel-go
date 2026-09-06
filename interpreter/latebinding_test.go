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
	"sort"
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

// testFunctionActivation is an Activation which serves both variables and late-bound functions.
type testFunctionActivation struct {
	Activation
	fns map[string]*functions.Overload
}

func (a *testFunctionActivation) ResolveFunction(name string) (*functions.Overload, bool) {
	fn, found := a.fns[name]
	return fn, found
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
		bindings  []*functions.Overload
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
			bindings: []*functions.Overload{
				{Operator: "request_id", Function: func(...ref.Val) ref.Val { return types.String("abc") }},
			},
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
			in: map[string]any{"user": "alice"},
			bindings: []*functions.Overload{
				{Operator: "is_admin_string", Unary: func(u ref.Val) ref.Val {
					return types.Bool(u.Equal(types.String("alice")) == types.True)
				}},
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
			in: map[string]any{"user": "alice"},
			bindings: []*functions.Overload{
				{Operator: "can_access_string_string", Binary: func(u, res ref.Val) ref.Val {
					return types.Bool(u.Equal(types.String("alice")) == types.True &&
						res.Equal(types.String("secrets")) == types.True)
				}},
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
			bindings: []*functions.Overload{
				{Operator: "greet_strings", Function: greet},
			},
			out: types.String("hello alice, bob, carl"),
		},
		{
			name: "unary resolved by function op",
			expr: `greet('alice')`,
			funcs: []*decls.FunctionDecl{
				lateBoundFunc(t, "greet",
					decls.Overload("greet_string", []*types.Type{types.StringType}, types.StringType,
						decls.LateFunctionBinding())),
			},
			bindings: []*functions.Overload{
				{Operator: "greet_string", Function: greet},
			},
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
			in: map[string]any{"user": "alice"},
			bindings: []*functions.Overload{
				{Operator: "string_is_admin", Unary: func(u ref.Val) ref.Val {
					return types.Bool(u.Equal(types.String("alice")) == types.True)
				}},
			},
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
			bindings: []*functions.Overload{
				{Operator: "is_admin_string", Unary: func(u ref.Val) ref.Val {
					return types.Bool(u.Equal(types.String("alice")) == types.True)
				}},
			},
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
			bindings: []*functions.Overload{
				{Operator: "is_admin", Unary: func(u ref.Val) ref.Val { return types.True }},
			},
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
			bindings: []*functions.Overload{
				{Operator: "size_of_string", Unary: func(v ref.Val) ref.Val {
					return types.Int(len(string(v.(types.String))))
				}},
				{Operator: "size_of_list", Unary: func(v ref.Val) ref.Val {
					return v.(traits.Sizer).Size()
				}},
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
			bindings: []*functions.Overload{
				{Operator: "count_any", Unary: func(v ref.Val) ref.Val { return v.(traits.Sizer).Size() }},
			},
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
			bindings: []*functions.Overload{
				{Operator: "count_any", Unary: func(v ref.Val) ref.Val { return v.(traits.Sizer).Size() }},
			},
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
			in: map[string]any{"unknown_var": types.NewUnknown(1, nil)},
			bindings: []*functions.Overload{
				{Operator: "describe_any", NonStrict: true, Unary: func(v ref.Val) ref.Val {
					if types.IsUnknown(v) {
						return types.String("unknown")
					}
					return types.String("known")
				}},
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
			in: map[string]any{"unknown_var": types.NewUnknown(1, nil)},
			bindings: []*functions.Overload{
				{Operator: "describe_any", Unary: func(v ref.Val) ref.Val { return types.String("known") }},
			},
			out: types.NewUnknown(1, nil),
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
			bindings: []*functions.Overload{
				{Operator: "is_admin_int", Unary: func(v ref.Val) ref.Val { return types.True }},
			},
			err: "no such overload: is_admin",
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
			fns, err := NewFunctionBindings(tc.bindings...)
			if err != nil {
				t.Fatalf("NewFunctionBindings() failed: %v", err)
			}
			lateVars, err := NewLateBindingActivation(vars, fns)
			if err != nil {
				t.Fatalf("NewLateBindingActivation() failed: %v", err)
			}
			out := prg.Eval(lateVars)
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

func TestLateBindingActivationResolvesFunctions(t *testing.T) {
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
	isAdmin := &functions.Overload{
		Operator: "is_admin_string",
		Unary:    func(u ref.Val) ref.Val { return types.Bool(u.Equal(types.String("alice")) == types.True) },
	}
	fns, err := NewFunctionBindings(isAdmin)
	if err != nil {
		t.Fatalf("NewFunctionBindings() failed: %v", err)
	}
	lateVars, err := NewLateBindingActivation(map[string]any{"user": "alice"}, fns)
	if err != nil {
		t.Fatalf("NewLateBindingActivation() failed: %v", err)
	}
	partialVars, err := NewPartialActivation(lateVars)
	if err != nil {
		t.Fatalf("NewPartialActivation() failed: %v", err)
	}
	// An activation which serves both variables and functions.
	selfResolving := &testFunctionActivation{
		Activation: vars,
		fns:        map[string]*functions.Overload{"is_admin_string": isAdmin},
	}
	tests := []struct {
		name string
		vars Activation
	}{
		{name: "late binding activation", vars: lateVars},
		{name: "self-resolving activation", vars: selfResolving},
		{name: "hierarchical child", vars: NewHierarchicalActivation(vars, lateVars)},
		{name: "hierarchical parent", vars: NewHierarchicalActivation(lateVars, vars)},
		{name: "partial activation", vars: partialVars},
		{name: "activation wrapper", vars: &testActivationWrapper{Activation: lateVars, name: "wrapper"}},
		{name: "state tracking activation", vars: evalStateActivation{vars: lateVars, state: NewEvalState()}},
		{name: "late bindings over an existing resolver",
			vars: mustLateBindingActivation(t, selfResolving)},
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

func mustLateBindingActivation(t testing.TB, vars any, fns ...FunctionResolver) Activation {
	t.Helper()
	a, err := NewLateBindingActivation(vars, fns...)
	if err != nil {
		t.Fatalf("NewLateBindingActivation() failed: %v", err)
	}
	return a
}

func TestNewFunctionBindingsErrors(t *testing.T) {
	tests := []struct {
		name      string
		overloads []*functions.Overload
		err       string
	}{
		{
			name:      "nil overload",
			overloads: []*functions.Overload{nil},
			err:       "overload must be non-nil",
		},
		{
			name:      "late-bound placeholder",
			overloads: []*functions.Overload{{Operator: "size_of", LateBound: true}},
			err:       "late-binding placeholder",
		},
		{
			name:      "missing implementation",
			overloads: []*functions.Overload{{Operator: "size_of"}},
			err:       "overload has no implementation: size_of",
		},
		{
			name: "duplicate overload",
			overloads: []*functions.Overload{
				{Operator: "size_of", Unary: func(v ref.Val) ref.Val { return types.Int(0) }},
				{Operator: "size_of", Unary: func(v ref.Val) ref.Val { return types.Int(1) }},
			},
			err: "overload already exists 'size_of'",
		},
	}
	for _, tst := range tests {
		tc := tst
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewFunctionBindings(tc.overloads...)
			if err == nil || !strings.Contains(err.Error(), tc.err) {
				t.Errorf("NewFunctionBindings() got error %v, wanted %q", err, tc.err)
			}
		})
	}
}

func TestNewLateBindingActivationErrors(t *testing.T) {
	if _, err := NewLateBindingActivation(nil); err == nil {
		t.Error("NewLateBindingActivation(nil) succeeded, wanted error")
	}
	if _, err := NewLateBindingActivation(map[string]any{}, nil); err == nil {
		t.Error("NewLateBindingActivation() with a nil resolver succeeded, wanted error")
	}
}

func TestFunctionBindingsOverloadIds(t *testing.T) {
	var nilBindings *FunctionBindings
	if ids := nilBindings.OverloadIds(); len(ids) != 0 {
		t.Errorf("OverloadIds() got %v, wanted empty", ids)
	}
	if _, found := nilBindings.ResolveFunction("size_of"); found {
		t.Error("ResolveFunction() found a binding in a nil binding set")
	}
	fns, err := NewFunctionBindings(
		&functions.Overload{Operator: "size_of", Unary: func(v ref.Val) ref.Val { return types.Int(0) }},
		&functions.Overload{Operator: "size_of_string", Unary: func(v ref.Val) ref.Val { return types.Int(1) }},
	)
	if err != nil {
		t.Fatalf("NewFunctionBindings() failed: %v", err)
	}
	ids := fns.OverloadIds()
	sort.Strings(ids)
	if len(ids) != 2 || ids[0] != "size_of" || ids[1] != "size_of_string" {
		t.Errorf("OverloadIds() got %v, wanted [size_of size_of_string]", ids)
	}
}

func TestAsFunctionResolver(t *testing.T) {
	if _, found := AsFunctionResolver(nil); found {
		t.Error("AsFunctionResolver(nil) found a resolver")
	}
	if _, found := AsFunctionResolver(EmptyActivation()); found {
		t.Error("AsFunctionResolver(EmptyActivation()) found a resolver")
	}
	sizeOf := &functions.Overload{Operator: "size_of", Unary: func(v ref.Val) ref.Val { return types.Int(0) }}
	fns, err := NewFunctionBindings(sizeOf)
	if err != nil {
		t.Fatalf("NewFunctionBindings() failed: %v", err)
	}
	childFns, err := NewFunctionBindings(
		&functions.Overload{Operator: "size_of_string", Unary: func(v ref.Val) ref.Val { return types.Int(1) }})
	if err != nil {
		t.Fatalf("NewFunctionBindings() failed: %v", err)
	}
	parent := mustLateBindingActivation(t, map[string]any{}, fns)
	child := mustLateBindingActivation(t, map[string]any{}, childFns)
	resolver, found := AsFunctionResolver(NewHierarchicalActivation(parent, child))
	if !found {
		t.Fatal("AsFunctionResolver() found no resolver in a hierarchical activation")
	}
	// Both the child and parent bindings are visible.
	if _, found := resolver.ResolveFunction("size_of"); !found {
		t.Error("ResolveFunction('size_of') found no parent binding")
	}
	if _, found := resolver.ResolveFunction("size_of_string"); !found {
		t.Error("ResolveFunction('size_of_string') found no child binding")
	}
	if _, found := resolver.ResolveFunction("missing"); found {
		t.Error("ResolveFunction('missing') found a binding")
	}
}
