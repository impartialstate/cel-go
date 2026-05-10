// Copyright 2024 Google LLC
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
	"testing"
	"time"

	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
)

func TestConcurrentEval(t *testing.T) {
	tests := []struct {
		name    string
		envOpts []EnvOption
		expr    string
		prgOpts []ProgramOption
		in      any
		out     ref.Val
	}{
		{
			name:    "basic_addition",
			envOpts: []EnvOption{Variable("x", IntType)},
			expr:    `x + 1`,
			in:      map[string]any{"x": 10},
			out:     types.Int(11),
		},
		{
			name: "async_function",
			envOpts: []EnvOption{
				Function("async_func",
					Overload("async_func_int", []*Type{IntType}, IntType,
						AsyncBinding(func(ctx context.Context, args ...ref.Val) ref.Val {
							time.Sleep(10 * time.Millisecond)
							return args[0]
						}),
					),
				),
			},
			expr: `async_func(42) + 1`,
			in:   map[string]any{},
			out:  types.Int(43),
		},
	}

	for _, tst := range tests {
		tc := tst
		t.Run(tc.name, func(t *testing.T) {
			env, err := NewEnv(tc.envOpts...)
			if err != nil {
				t.Fatalf("NewEnv() failed: %v", err)
			}
			ast, iss := env.Compile(tc.expr)
			if iss.Err() != nil {
				t.Fatalf("env.Compile() failed: %v", iss.Err())
			}
			prg, err := env.Program(ast, tc.prgOpts...)
			if err != nil {
				t.Fatalf("env.Program() failed: %v", err)
			}
			resCh := prg.ConcurrentEval(context.Background(), tc.in)
			select {
			case res := <-resCh:
				if res.Err != nil {
					t.Fatalf("ConcurrentEval() returned error: %v", res.Err)
				}
				if res.Val.Equal(tc.out) != types.True {
					t.Errorf("ConcurrentEval() = %v, want %v", res.Val, tc.out)
				}
			case <-time.After(time.Second):
				t.Fatal("ConcurrentEval() timed out")
			}
		})
	}
}

func TestConcurrentEval_Unknowns(t *testing.T) {
	tests := []struct {
		name    string
		envOpts []EnvOption
		expr    string
		prgOpts []ProgramOption
		in      any
	}{
		{
			name:    "partial_variable",
			envOpts: []EnvOption{Variable("x", IntType)},
			expr:    `x + 1`,
			prgOpts: []ProgramOption{EvalOptions(OptPartialEval)},
			in: func() any {
				pvars, _ := PartialVars(map[string]any{}, AttributePattern("x"))
				return pvars
			}(),
		},
	}

	for _, tst := range tests {
		tc := tst
		t.Run(tc.name, func(t *testing.T) {
			env, err := NewEnv(tc.envOpts...)
			if err != nil {
				t.Fatalf("NewEnv() failed: %v", err)
			}
			ast, iss := env.Compile(tc.expr)
			if iss.Err() != nil {
				t.Fatalf("env.Compile() failed: %v", iss.Err())
			}
			prg, err := env.Program(ast, tc.prgOpts...)
			if err != nil {
				t.Fatalf("env.Program() failed: %v", err)
			}
			resCh := prg.ConcurrentEval(context.Background(), tc.in)
			select {
			case res := <-resCh:
				if res.Err != nil {
					t.Fatalf("ConcurrentEval() returned error: %v", res.Err)
				}
				if !types.IsUnknown(res.Val) {
					t.Errorf("ConcurrentEval() = %v, want Unknown", res.Val)
				}
			case <-time.After(time.Second):
				t.Fatal("ConcurrentEval() timed out")
			}
		})
	}
}

func TestConcurrentEval_Cancel(t *testing.T) {
	env, err := NewEnv(
		Function("long_func",
			Overload("long_func", []*Type{}, IntType,
				AsyncBinding(func(ctx context.Context, args ...ref.Val) ref.Val {
					<-ctx.Done()
					return types.NewErr("cancelled")
				}),
			),
		),
	)
	if err != nil {
		t.Fatalf("NewEnv() failed: %v", err)
	}

	ast, iss := env.Compile(`long_func()`)
	if iss.Err() != nil {
		t.Fatalf("env.Compile() failed: %v", iss.Err())
	}

	prg, err := env.Program(ast)
	if err != nil {
		t.Fatalf("env.Program() failed: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	resCh := prg.ConcurrentEval(ctx, NoVars())

	// Cancel the context immediately
	cancel()

	select {
	case res := <-resCh:
		if res.Err == nil || (res.Err.Error() != "context canceled" && res.Err.Error() != "context deadline exceeded") {
			t.Errorf("ConcurrentEval() expected context canceled error, got: %v", res.Err)
		}
	case <-time.After(time.Second):
		t.Fatal("ConcurrentEval() timed out")
	}
}

func TestConcurrentEval_NilContext(t *testing.T) {
	env, _ := NewEnv()
	ast, _ := env.Compile(`1 + 1`)
	prg, _ := env.Program(ast)

	resCh := prg.ConcurrentEval(nil, NoVars())
	res := <-resCh
	if res.Err == nil || res.Err.Error() != "context can not be nil" {
		t.Errorf("ConcurrentEval(nil) expected error, got %v", res.Err)
	}
}

func TestConcurrentEval_Observable(t *testing.T) {
	asyncIdentityBinding := AsyncBinding(func(ctx context.Context, args ...ref.Val) ref.Val {
		time.Sleep(10 * time.Millisecond)
		return args[0]
	})
	asyncDoubleBinding := AsyncBinding(func(ctx context.Context, args ...ref.Val) ref.Val {
		time.Sleep(10 * time.Millisecond)
		v := args[0].(types.Int)
		return v * 2
	})

	tests := []struct {
		name    string
		expr    string
		envOpts []EnvOption
		prgOpts []ProgramOption
		in      any
		out     ref.Val
	}{
		{
			name: "logical_or",
			expr: `a || b == "b"`,
			envOpts: []EnvOption{
				Variable("a", BoolType),
				Variable("b", StringType),
			},
			in: map[string]any{
				"a": true,
				"b": "b",
			},
			out: types.True,
		},
		{
			name: "conditional",
			expr: `a ? b < 1.0 : c == ['hello']`,
			envOpts: []EnvOption{
				Variable("a", BoolType),
				Variable("b", DoubleType),
				Variable("c", ListType(StringType)),
			},
			in: map[string]any{
				"a": true,
				"b": 0.999,
				"c": []string{"hello"},
			},
			out: types.True,
		},
		{
			name: "exhaustive_eval",
			expr: `{k: true}[k] || v != false`,
			envOpts: []EnvOption{
				Variable("k", StringType),
				Variable("v", BoolType),
			},
			in: map[string]any{
				"k": "key",
				"v": true,
			},
			out: types.True,
		},
		{
			name: "exhaustive_async_only",
			expr: `async_id(1) + async_dbl(3) == 7`,
			envOpts: []EnvOption{
				Function("async_id",
					Overload("async_identity_int", []*Type{IntType}, IntType, asyncIdentityBinding),
				),
				Function("async_dbl",
					Overload("async_double_int", []*Type{IntType}, IntType, asyncDoubleBinding),
				),
			},
			in:  map[string]any{},
			out: types.True,
		},
		{
			name: "exhaustive_mixed_sync_async",
			expr: `async_id(5) + negate(3) == 2`,
			envOpts: []EnvOption{
				Function("async_id",
					Overload("async_identity_int", []*Type{IntType}, IntType, asyncIdentityBinding),
				),
				Function("negate",
					Overload("sync_negate_int", []*Type{IntType}, IntType,
						UnaryBinding(func(v ref.Val) ref.Val {
							return -(v.(types.Int))
						}),
					),
				),
			},
			in:  map[string]any{},
			out: types.True,
		},
	}

	for _, tst := range tests {
		tc := tst
		t.Run(tc.name, func(t *testing.T) {
			env, err := NewEnv(tc.envOpts...)
			if err != nil {
				t.Fatalf("NewEnv() failed: %v", err)
			}
			ast, iss := env.Compile(tc.expr)
			if iss.Err() != nil {
				t.Fatalf("env.Compile() failed: %v", iss.Err())
			}

			prgOpts := append([]ProgramOption{EvalOptions(OptExhaustiveEval)}, tc.prgOpts...)
			prg, err := env.Program(ast, prgOpts...)
			if err != nil {
				t.Fatalf("env.Program() failed: %v", err)
			}

			resCh := prg.ConcurrentEval(context.Background(), tc.in)

			select {
			case res := <-resCh:
				if res.Err != nil {
					t.Fatalf("ConcurrentEval() returned error: %v", res.Err)
				}
				if res.Val.Equal(tc.out) != types.True {
					t.Errorf("ConcurrentEval() = %v, want %v", res.Val, tc.out)
				}
				if res.EvalDetails == nil {
					t.Fatal("ConcurrentEval() did not return EvalDetails")
				}
				s := res.EvalDetails.State()
				if s == nil {
					t.Fatal("EvalDetails.State() returned nil")
				}
				if len(s.IDs()) == 0 {
					t.Error("EvalState should contain tracked values, but was empty")
				}
			case <-time.After(time.Second):
				t.Fatal("ConcurrentEval() timed out")
			}
		})
	}
}

func TestConcurrentEval_ObservableUnknowns(t *testing.T) {
	ch := make(chan ref.Val, 1)
	defer close(ch)
	asyncIdentityBinding := AsyncBinding(func(ctx context.Context, args ...ref.Val) ref.Val {
		select {
		case v := <-ch:
			return v
		case <-ctx.Done():
			return types.NewErr(ctx.Err().Error())
		}
	})

	env, err := NewEnv(
		Variable("x", IntType),
		Variable("y", IntType),
		Function("async_id",
			Overload("async_identity_int", []*Type{IntType}, IntType, asyncIdentityBinding),
		),
	)
	if err != nil {
		t.Fatalf("NewEnv() failed: %v", err)
	}

	ast, iss := env.Compile(`async_id(x) + y == 11`)
	if iss.Err() != nil {
		t.Fatalf("env.Compile() failed: %v", iss.Err())
	}
	prg, err := env.Program(ast, EvalOptions(OptExhaustiveEval|OptPartialEval))
	if err != nil {
		t.Fatalf("env.Program() failed: %v", err)
	}

	pvars, err := PartialVars(
		map[string]any{"x": 1},
		AttributePattern("y"),
	)
	if err != nil {
		t.Fatalf("PartialVars() failed: %v", err)
	}
	resCh := prg.ConcurrentEval(context.Background(), pvars)
	ch <- types.Int(10)

	select {
	case res := <-resCh:
		if res.Err != nil {
			t.Fatalf("ConcurrentEval() returned error: %v", res.Err)
		}
		if !types.IsUnknown(res.Val) {
			t.Errorf("ConcurrentEval() = %v, want Unknown", res.Val)
		}
		unk := res.Val.(*types.Unknown)
		for _, id := range unk.IDs() {
			trails, found := unk.GetAttributeTrails(id)
			if !found {
				t.Fatalf("unk.GetAttributeTrails(id) failed for unknown id: %d", id)
			}
			if len(trails) == 1 && trails[0].Variable() == "y" {
				goto found
			}
		}
		t.Errorf("Unknown value does not contain y")
	found:
		if res.EvalDetails == nil {
			t.Fatal("ConcurrentEval() did not return EvalDetails")
		}
		s := res.EvalDetails.State()
		if s == nil {
			t.Fatal("EvalDetails.State() returned nil")
		}
		if len(s.IDs()) == 0 {
			t.Error("EvalState should contain tracked values, but was empty")
		}
	case <-time.After(time.Second):
		t.Fatal("ConcurrentEval() timed out")
	}
}
