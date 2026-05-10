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

func TestConcurrentEval_Basic(t *testing.T) {
	env, err := NewEnv(
		Variable("x", IntType),
	)
	if err != nil {
		t.Fatalf("NewEnv() failed: %v", err)
	}

	ast, iss := env.Compile(`x + 1`)
	if iss.Err() != nil {
		t.Fatalf("env.Compile() failed: %v", iss.Err())
	}

	prg, err := env.Program(ast)
	if err != nil {
		t.Fatalf("env.Program() failed: %v", err)
	}

	ctx := context.Background()
	resCh := prg.ConcurrentEval(ctx, map[string]any{"x": 10})

	select {
	case res := <-resCh:
		if res.Err != nil {
			t.Errorf("ConcurrentEval() returned error: %v", res.Err)
		}
		if res.Val.Equal(types.Int(11)) != types.True {
			t.Errorf("ConcurrentEval() returned %v, wanted 11", res.Val)
		}
	case <-time.After(time.Second):
		t.Fatal("ConcurrentEval() timed out")
	}
}

func TestConcurrentEval_Async(t *testing.T) {
	env, err := NewEnv(
		Function("async_func",
			Overload("async_func_int", []*Type{IntType}, IntType,
				AsyncBinding(func(ctx context.Context, args ...ref.Val) <-chan ref.Val {
					ch := make(chan ref.Val, 1)
					go func() {
						// Simulate async work
						time.Sleep(10 * time.Millisecond)
						ch <- args[0]
						close(ch)
					}()
					return ch
				}),
			),
		),
	)
	if err != nil {
		t.Fatalf("NewEnv() failed: %v", err)
	}

	ast, iss := env.Compile(`async_func(42) + 1`)
	if iss.Err() != nil {
		t.Fatalf("env.Compile() failed: %v", iss.Err())
	}

	prg, err := env.Program(ast)
	if err != nil {
		t.Fatalf("env.Program() failed: %v", err)
	}

	ctx := context.Background()
	resCh := prg.ConcurrentEval(ctx, NoVars())

	select {
	case res := <-resCh:
		if res.Err != nil {
			t.Errorf("ConcurrentEval() returned error: %v", res.Err)
		}
		if res.Val.Equal(types.Int(43)) != types.True {
			t.Errorf("ConcurrentEval() returned %v, wanted 43", res.Val)
		}
	case <-time.After(time.Second):
		t.Fatal("ConcurrentEval() timed out")
	}
}

func TestConcurrentEval_Cancel(t *testing.T) {
	env, err := NewEnv(
		Function("long_func",
			Overload("long_func", []*Type{}, IntType,
				AsyncBinding(func(ctx context.Context, args ...ref.Val) <-chan ref.Val {
					ch := make(chan ref.Val, 1)
					go func() {
						// Wait for context cancellation
						<-ctx.Done()
						ch <- types.NewErr("cancelled")
					}()
					return ch
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

func TestConcurrentEval_Unknowns(t *testing.T) {
	env, err := NewEnv(
		Variable("x", IntType),
	)
	if err != nil {
		t.Fatalf("NewEnv() failed: %v", err)
	}

	ast, iss := env.Compile(`x + 1`)
	if iss.Err() != nil {
		t.Fatalf("env.Compile() failed: %v", iss.Err())
	}

	prg, err := env.Program(ast, EvalOptions(OptPartialEval))
	if err != nil {
		t.Fatalf("env.Program() failed: %v", err)
	}

	pvars, err := PartialVars(map[string]any{}, AttributePattern("x"))
	if err != nil {
		t.Fatalf("PartialVars() failed: %v", err)
	}

	ctx := context.Background()
	resCh := prg.ConcurrentEval(ctx, pvars)

	select {
	case res := <-resCh:
		if res.Err != nil {
			t.Errorf("ConcurrentEval() returned error: %v", res.Err)
		}
		if !types.IsUnknown(res.Val) {
			t.Errorf("ConcurrentEval() returned %v, wanted Unknown", res.Val)
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
	asyncIdentityBinding := AsyncBinding(func(ctx context.Context, args ...ref.Val) <-chan ref.Val {
		ch := make(chan ref.Val, 1)
		go func() {
			time.Sleep(10 * time.Millisecond)
			ch <- args[0]
			close(ch)
		}()
		return ch
	})
	asyncDoubleBinding := AsyncBinding(func(ctx context.Context, args ...ref.Val) <-chan ref.Val {
		ch := make(chan ref.Val, 1)
		go func() {
			time.Sleep(10 * time.Millisecond)
			v := args[0].(types.Int)
			ch <- v * 2
			close(ch)
		}()
		return ch
	})

	tests := []struct {
		name    string
		expr    string
		vars    []EnvOption
		funcs   []EnvOption
		prgOpts []ProgramOption
		in      any
		out     ref.Val
		isUnk   bool
	}{
		{
			name: "logical_or",
			expr: `a || b == "b"`,
			vars: []EnvOption{
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
			vars: []EnvOption{
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
			vars: []EnvOption{
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
			funcs: []EnvOption{
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
			funcs: []EnvOption{
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
		{
			name: "exhaustive_async_partial_vars",
			expr: `async_id(x) + y == 11`,
			funcs: []EnvOption{
				Function("async_id",
					Overload("async_identity_int", []*Type{IntType}, IntType, asyncIdentityBinding),
				),
			},
			prgOpts: []ProgramOption{
				EvalOptions(OptPartialEval),
			},
			vars: []EnvOption{
				Variable("x", IntType),
				Variable("y", IntType),
			},
			in: func() any {
				pvars, _ := PartialVars(
					map[string]any{"x": 1},
					AttributePattern("y"),
				)
				return pvars
			}(),
			isUnk: true,
		},
	}

	for _, tst := range tests {
		tc := tst
		t.Run(tc.name, func(t *testing.T) {
			envOpts := make([]EnvOption, 0, len(tc.vars)+len(tc.funcs))
			envOpts = append(envOpts, tc.vars...)
			envOpts = append(envOpts, tc.funcs...)
			env, err := NewEnv(envOpts...)
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

			ctx := context.Background()
			resCh := prg.ConcurrentEval(ctx, tc.in)

			select {
			case res := <-resCh:
				if res.Err != nil {
					t.Errorf("ConcurrentEval() returned error: %v", res.Err)
				}
				if tc.isUnk {
					if !types.IsUnknown(res.Val) {
						t.Errorf("ConcurrentEval() returned %v, wanted Unknown", res.Val)
					}
				} else if res.Val.Equal(tc.out) != types.True {
					t.Errorf("ConcurrentEval() returned %v, wanted %v", res.Val, tc.out)
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
