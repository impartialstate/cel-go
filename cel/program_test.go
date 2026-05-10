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

	"github.com/google/cel-go/common/functions"
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
			Overload("async_func_int", []*Type{IntType}, IntType),
		),
	)
	if err != nil {
		t.Fatalf("NewEnv() failed: %v", err)
	}

	ast, iss := env.Compile(`async_func(42) + 1`)
	if iss.Err() != nil {
		t.Fatalf("env.Compile() failed: %v", iss.Err())
	}

	// Use cel.Functions ProgramOption to add async overloads
	prgOpts := []ProgramOption{
		Functions(&functions.Overload{
			Operator: "async_func_int",
			Async: func(ctx context.Context, args ...ref.Val) <-chan ref.Val {
				ch := make(chan ref.Val, 1)
				go func() {
					// Simulate async work
					time.Sleep(10 * time.Millisecond)
					ch <- args[0]
					close(ch)
				}()
				return ch
			},
		}),
	}
	prg, err := env.Program(ast, prgOpts...)
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
			Overload("long_func", []*Type{}, IntType),
		),
	)
	if err != nil {
		t.Fatalf("NewEnv() failed: %v", err)
	}

	ast, iss := env.Compile(`long_func()`)
	if iss.Err() != nil {
		t.Fatalf("env.Compile() failed: %v", iss.Err())
	}

	prgOpts := []ProgramOption{
		Functions(&functions.Overload{
			Operator: "long_func",
			Async: func(ctx context.Context, args ...ref.Val) <-chan ref.Val {
				ch := make(chan ref.Val, 1)
				go func() {
					// Wait for context cancellation
					<-ctx.Done()
					ch <- types.NewErr("cancelled")
				}()
				return ch
			},
		}),
	}
	prg, err := env.Program(ast, prgOpts...)
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
	tests := []struct {
		name string
		expr string
		vars []EnvOption
		in   map[string]any
		out  ref.Val
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
			name: "exhaustive_eval_unknowns",
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
	}

	for _, tst := range tests {
		tc := tst
		t.Run(tc.name, func(t *testing.T) {
			env, err := NewEnv(tc.vars...)
			if err != nil {
				t.Fatalf("NewEnv() failed: %v", err)
			}
			ast, iss := env.Compile(tc.expr)
			if iss.Err() != nil {
				t.Fatalf("env.Compile() failed: %v", iss.Err())
			}

			prg, err := env.Program(ast, EvalOptions(OptExhaustiveEval))
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
				if res.Val.Equal(tc.out) != types.True {
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
