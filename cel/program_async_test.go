// Copyright 2026 Google LLC
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

package cel_test

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/cel/async"
	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
	"github.com/google/cel-go/test"
)

// awaitEval runs ConcurrentEval and returns the result or fails on timeout.
func awaitEval(t *testing.T, prg cel.Program, ctx context.Context, in any) cel.EvalResult {
	t.Helper()
	select {
	case res := <-prg.ConcurrentEval(ctx, in):
		return res
	case <-time.After(5 * time.Second):
		t.Fatal("ConcurrentEval() timed out")
		return cel.EvalResult{}
	}
}

func TestConcurrentEvalSync(t *testing.T) {
	env, err := cel.NewEnv(cel.Variable("x", cel.IntType))
	if err != nil {
		t.Fatalf("NewEnv() failed: %v", err)
	}
	ast, iss := env.Compile(`x + 1`)
	if iss.Err() != nil {
		t.Fatalf("Compile() failed: %v", iss.Err())
	}
	prg, err := env.Program(ast)
	if err != nil {
		t.Fatalf("Program() failed: %v", err)
	}
	res := awaitEval(t, prg, context.Background(), map[string]any{"x": 10})
	if res.Err != nil {
		t.Fatalf("ConcurrentEval() error: %v", res.Err)
	}
	if res.Val.Equal(types.Int(11)) != types.True {
		t.Errorf("ConcurrentEval() = %v, want 11", res.Val)
	}
}

func TestConcurrentEvalSingleAsync(t *testing.T) {
	env, err := cel.NewEnv(
		cel.Function("async_func",
			cel.Overload("async_func_int", []*cel.Type{cel.IntType}, cel.IntType,
				cel.AsyncBinding(func(ctx context.Context, args ...ref.Val) ref.Val {
					time.Sleep(10 * time.Millisecond)
					return args[0]
				}),
			),
		),
	)
	if err != nil {
		t.Fatalf("NewEnv() failed: %v", err)
	}
	ast, iss := env.Compile(`async_func(42) + 1`)
	if iss.Err() != nil {
		t.Fatalf("Compile() failed: %v", iss.Err())
	}
	prg, err := env.Program(ast)
	if err != nil {
		t.Fatalf("Program() failed: %v", err)
	}
	res := awaitEval(t, prg, context.Background(), cel.NoVars())
	if res.Err != nil {
		t.Fatalf("ConcurrentEval() error: %v", res.Err)
	}
	if res.Val.Equal(types.Int(43)) != types.True {
		t.Errorf("ConcurrentEval() = %v, want 43", res.Val)
	}
}

// rpcEnv builds an env with an async string rpc() function that delays before echoing its input.
func rpcEnv(t *testing.T, opt cel.ProgramOption, binding cel.BlockingAsyncOp) (cel.Program, error) {
	t.Helper()
	env, err := cel.NewEnv(
		cel.Function("rpc",
			cel.Overload("rpc_string", []*cel.Type{cel.StringType}, cel.StringType,
				cel.AsyncBinding(binding)),
		),
	)
	if err != nil {
		t.Fatalf("NewEnv() failed: %v", err)
	}
	ast, iss := env.Compile(`rpc("a") + rpc("b") + rpc("c")`)
	if iss.Err() != nil {
		t.Fatalf("Compile() failed: %v", iss.Err())
	}
	if opt != nil {
		return env.Program(ast, opt)
	}
	return env.Program(ast)
}

func TestConcurrentEvalFanOutDrainStrategies(t *testing.T) {
	binding := func(ctx context.Context, args ...ref.Val) ref.Val {
		time.Sleep(10 * time.Millisecond)
		return args[0]
	}
	strategies := map[string]cel.ProgramOption{
		"default":     nil,
		"drain_all":   cel.ConcurrentDrainStrategy(async.DrainAll()),
		"drain_ready": cel.ConcurrentDrainStrategy(async.DrainReady(5 * time.Millisecond)),
		"drain_none":  cel.ConcurrentDrainStrategy(async.DrainNone()),
	}
	for name, opt := range strategies {
		t.Run(name, func(t *testing.T) {
			prg, err := rpcEnv(t, opt, binding)
			if err != nil {
				t.Fatalf("Program() failed: %v", err)
			}
			res := awaitEval(t, prg, context.Background(), cel.NoVars())
			if res.Err != nil {
				t.Fatalf("ConcurrentEval() error: %v", res.Err)
			}
			if res.Val.Equal(types.String("abc")) != types.True {
				t.Errorf("ConcurrentEval() = %v, want 'abc'", res.Val)
			}
		})
	}
}

func TestConcurrentEvalErrorPropagation(t *testing.T) {
	prg, err := rpcEnv(t, nil, func(ctx context.Context, args ...ref.Val) ref.Val {
		return types.NewErr("rpc failed")
	})
	if err != nil {
		t.Fatalf("Program() failed: %v", err)
	}
	res := awaitEval(t, prg, context.Background(), cel.NoVars())
	if res.Err == nil {
		t.Fatalf("ConcurrentEval() expected error, got val %v", res.Val)
	}
}

func TestConcurrentEvalCancel(t *testing.T) {
	prg, err := rpcEnv(t, nil, func(ctx context.Context, args ...ref.Val) ref.Val {
		<-ctx.Done()
		return types.NewErr("cancelled")
	})
	if err != nil {
		t.Fatalf("Program() failed: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	resCh := prg.ConcurrentEval(ctx, cel.NoVars())
	cancel()
	select {
	case res := <-resCh:
		if res.Err == nil {
			t.Errorf("ConcurrentEval() expected cancellation error, got val %v", res.Val)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("ConcurrentEval() timed out after cancel")
	}
}

func TestConcurrentEvalNilContext(t *testing.T) {
	env, _ := cel.NewEnv()
	ast, _ := env.Compile(`1 + 1`)
	prg, _ := env.Program(ast)
	res := <-prg.ConcurrentEval(nil, cel.NoVars())
	if res.Err == nil || res.Err.Error() != "context can not be nil" {
		t.Errorf("ConcurrentEval(nil) = %v, want 'context can not be nil'", res.Err)
	}
}

type countingObserver struct {
	started  atomic.Int32
	finished atomic.Int32
}

func (o *countingObserver) OnCallStarted(callID int64, function, overload string, args []ref.Val) {
	o.started.Add(1)
}
func (o *countingObserver) OnCallFinished(callID int64, function, overload string, res ref.Val) {
	o.finished.Add(1)
}

func TestConcurrentEvalObserver(t *testing.T) {
	obs := &countingObserver{}
	prg, err := rpcEnv(t, cel.AsyncCallObserver(obs), func(ctx context.Context, args ...ref.Val) ref.Val {
		time.Sleep(5 * time.Millisecond)
		return args[0]
	})
	if err != nil {
		t.Fatalf("Program() failed: %v", err)
	}
	res := awaitEval(t, prg, context.Background(), cel.NoVars())
	if res.Err != nil {
		t.Fatalf("ConcurrentEval() error: %v", res.Err)
	}
	// Three distinct rpc calls in the expression.
	if got := obs.started.Load(); got != 3 {
		t.Errorf("OnCallStarted count = %d, want 3", got)
	}
	if got := obs.finished.Load(); got != 3 {
		t.Errorf("OnCallFinished count = %d, want 3", got)
	}
}

func TestConcurrentEvalMaxConcurrency(t *testing.T) {
	var concurrent atomic.Int32
	var maxConcurrent atomic.Int32
	binding := func(ctx context.Context, args ...ref.Val) ref.Val {
		cur := concurrent.Add(1)
		for {
			old := maxConcurrent.Load()
			if cur <= old || maxConcurrent.CompareAndSwap(old, cur) {
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
		concurrent.Add(-1)
		return args[0]
	}
	prg, err := rpcEnv(t, cel.AsyncMaxConcurrency(1), binding)
	if err != nil {
		t.Fatalf("Program() failed: %v", err)
	}
	res := awaitEval(t, prg, context.Background(), cel.NoVars())
	if res.Err != nil {
		t.Fatalf("ConcurrentEval() error: %v", res.Err)
	}
	if res.Val.Equal(types.String("abc")) != types.True {
		t.Errorf("ConcurrentEval() = %v, want 'abc'", res.Val)
	}
	if got := maxConcurrent.Load(); got > 1 {
		t.Errorf("max observed concurrency = %d, want <= 1", got)
	}
}

func TestConcurrentEvalFakeRPC(t *testing.T) {
	prg, err := rpcEnv(t, nil, test.FakeRPC(time.Second))
	if err != nil {
		t.Fatalf("Program() failed: %v", err)
	}
	res := awaitEval(t, prg, context.Background(), cel.NoVars())
	if res.Err != nil {
		t.Fatalf("ConcurrentEval() error: %v", res.Err)
	}
	want := types.String("a success!b success!c success!")
	if res.Val.Equal(want) != types.True {
		t.Errorf("ConcurrentEval() = %v, want %v", res.Val, want)
	}
}

type retryableErr struct{ retryable bool }

func (e retryableErr) Error() string     { return "retryable error" }
func (e retryableErr) IsRetryable() bool { return e.retryable }

func TestConcurrentEvalRetryBinding(t *testing.T) {
	var attempts atomic.Int32
	env, err := cel.NewEnv(
		cel.Function("flaky",
			cel.Overload("flaky_int", []*cel.Type{}, cel.IntType,
				async.RetryBinding(func(ctx context.Context, args ...ref.Val) ref.Val {
					if attempts.Add(1) < 3 {
						return types.WrapErr(retryableErr{retryable: true})
					}
					return types.Int(7)
				}, async.RetryBackoff(time.Millisecond), async.RetryAttempts(5)),
			),
		),
	)
	if err != nil {
		t.Fatalf("NewEnv() failed: %v", err)
	}
	ast, iss := env.Compile(`flaky()`)
	if iss.Err() != nil {
		t.Fatalf("Compile() failed: %v", iss.Err())
	}
	prg, err := env.Program(ast)
	if err != nil {
		t.Fatalf("Program() failed: %v", err)
	}
	res := awaitEval(t, prg, context.Background(), cel.NoVars())
	if res.Err != nil {
		t.Fatalf("ConcurrentEval() error: %v", res.Err)
	}
	if res.Val.Equal(types.Int(7)) != types.True {
		t.Errorf("ConcurrentEval() = %v, want 7", res.Val)
	}
	if got := attempts.Load(); got != 3 {
		t.Errorf("attempts = %d, want 3", got)
	}
}

func TestConcurrentEvalRetryNonRetryable(t *testing.T) {
	var attempts atomic.Int32
	env, err := cel.NewEnv(
		cel.Function("flaky",
			cel.Overload("flaky_int", []*cel.Type{}, cel.IntType,
				async.RetryBinding(func(ctx context.Context, args ...ref.Val) ref.Val {
					attempts.Add(1)
					return types.WrapErr(retryableErr{retryable: false})
				}, async.RetryBackoff(time.Millisecond), async.RetryAttempts(5)),
			),
		),
	)
	if err != nil {
		t.Fatalf("NewEnv() failed: %v", err)
	}
	ast, _ := env.Compile(`flaky()`)
	prg, err := env.Program(ast)
	if err != nil {
		t.Fatalf("Program() failed: %v", err)
	}
	res := awaitEval(t, prg, context.Background(), cel.NoVars())
	if res.Err == nil {
		t.Fatalf("ConcurrentEval() expected error, got %v", res.Val)
	}
	if got := attempts.Load(); got != 1 {
		t.Errorf("attempts = %d, want 1 (non-retryable must not retry)", got)
	}
}

func TestConcurrentEvalTimeoutBinding(t *testing.T) {
	env, err := cel.NewEnv(
		cel.Function("slow",
			cel.Overload("slow_int", []*cel.Type{}, cel.IntType,
				async.TimeoutBinding(func(ctx context.Context, args ...ref.Val) ref.Val {
					select {
					case <-time.After(time.Second):
						return types.Int(1)
					case <-ctx.Done():
						return types.NewErr("timed out: %v", ctx.Err())
					}
				}, 20*time.Millisecond),
			),
		),
	)
	if err != nil {
		t.Fatalf("NewEnv() failed: %v", err)
	}
	ast, _ := env.Compile(`slow()`)
	prg, err := env.Program(ast)
	if err != nil {
		t.Fatalf("Program() failed: %v", err)
	}
	res := awaitEval(t, prg, context.Background(), cel.NoVars())
	if res.Err == nil {
		t.Errorf("ConcurrentEval() expected timeout error, got %v", res.Val)
	}
}

func TestConcurrentEvalCachedBinding(t *testing.T) {
	var calls atomic.Int32
	env, err := cel.NewEnv(
		cel.Function("lookup",
			cel.Overload("lookup_string", []*cel.Type{cel.StringType}, cel.StringType,
				async.CachedBinding(func(ctx context.Context, args ...ref.Val) ref.Val {
					calls.Add(1)
					return args[0]
				})),
		),
	)
	if err != nil {
		t.Fatalf("NewEnv() failed: %v", err)
	}
	ast, iss := env.Compile(`lookup("k")`)
	if iss.Err() != nil {
		t.Fatalf("Compile() failed: %v", iss.Err())
	}
	prg, err := env.Program(ast)
	if err != nil {
		t.Fatalf("Program() failed: %v", err)
	}
	// First evaluation computes and caches the value.
	res := awaitEval(t, prg, context.Background(), cel.NoVars())
	if res.Err != nil {
		t.Fatalf("ConcurrentEval() error: %v", res.Err)
	}
	if res.Val.Equal(types.String("k")) != types.True {
		t.Errorf("ConcurrentEval() = %v, want 'k'", res.Val)
	}
	// A subsequent evaluation is served from the cache without re-invoking the function.
	// (Note: the cache deduplicates across evaluations, not across concurrent call sites within
	// a single evaluation, since it does not single-flight.)
	res = awaitEval(t, prg, context.Background(), cel.NoVars())
	if res.Err != nil {
		t.Fatalf("ConcurrentEval() error: %v", res.Err)
	}
	if res.Val.Equal(types.String("k")) != types.True {
		t.Errorf("ConcurrentEval() = %v, want 'k'", res.Val)
	}
	if got := calls.Load(); got != 1 {
		t.Errorf("underlying fn called %d times, want 1 (second eval served from cache)", got)
	}
}

func TestConcurrentEvalAsyncInComprehension(t *testing.T) {
	// Regression: an async call inside a comprehension is evaluated once per element with a
	// different loop-variable binding but the same AST node id. The evaluator must track each
	// element's call independently; otherwise re-evaluation relaunches every element forever and
	// never converges.
	var launches atomic.Int32
	env, err := cel.NewEnv(
		cel.Function("dbl",
			cel.Overload("dbl_int", []*cel.Type{cel.IntType}, cel.IntType,
				cel.AsyncBinding(func(ctx context.Context, args ...ref.Val) ref.Val {
					launches.Add(1)
					time.Sleep(5 * time.Millisecond)
					return args[0].(types.Int) * 2
				}))),
	)
	if err != nil {
		t.Fatalf("NewEnv() failed: %v", err)
	}
	ast, iss := env.Compile(`[1, 2, 3].map(i, dbl(i))`)
	if iss.Err() != nil {
		t.Fatalf("Compile() failed: %v", iss.Err())
	}
	prg, err := env.Program(ast)
	if err != nil {
		t.Fatalf("Program() failed: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	select {
	case res := <-prg.ConcurrentEval(ctx, cel.NoVars()):
		if res.Err != nil {
			t.Fatalf("ConcurrentEval() error: %v (launches=%d)", res.Err, launches.Load())
		}
		want := types.DefaultTypeAdapter.NativeToValue([]int64{2, 4, 6})
		if res.Val.Equal(want) != types.True {
			t.Errorf("ConcurrentEval() = %v, want [2, 4, 6]", res.Val)
		}
		if got := launches.Load(); got != 3 {
			t.Errorf("async launches = %d, want exactly 3 (one per element, no relaunch)", got)
		}
	case <-time.After(6 * time.Second):
		t.Fatalf("ConcurrentEval() did not converge; launches=%d", launches.Load())
	}
}

func TestConcurrentEvalBoundsLaunchConcurrency(t *testing.T) {
	// A wide async fan-out inside a comprehension must not launch more concurrent calls than the
	// configured limit, and must still converge to the correct result.
	var live, maxLive atomic.Int32
	env, err := cel.NewEnv(
		cel.Function("rpc",
			cel.Overload("rpc_int", []*cel.Type{cel.IntType}, cel.IntType,
				cel.AsyncBinding(func(ctx context.Context, args ...ref.Val) ref.Val {
					cur := live.Add(1)
					for {
						old := maxLive.Load()
						if cur <= old || maxLive.CompareAndSwap(old, cur) {
							break
						}
					}
					time.Sleep(10 * time.Millisecond)
					live.Add(-1)
					return args[0]
				}))),
	)
	if err != nil {
		t.Fatalf("NewEnv() failed: %v", err)
	}
	ast, iss := env.Compile(`[1, 2, 3, 4, 5, 6, 7, 8].map(i, rpc(i))`)
	if iss.Err() != nil {
		t.Fatalf("Compile() failed: %v", iss.Err())
	}
	const limit = 2
	prg, err := env.Program(ast, cel.AsyncMaxConcurrency(limit), cel.ConcurrentDrainStrategy(async.DrainAll()))
	if err != nil {
		t.Fatalf("Program() failed: %v", err)
	}
	res := awaitEval(t, prg, context.Background(), cel.NoVars())
	if res.Err != nil {
		t.Fatalf("ConcurrentEval() error: %v", res.Err)
	}
	want := types.DefaultTypeAdapter.NativeToValue([]int64{1, 2, 3, 4, 5, 6, 7, 8})
	if res.Val.Equal(want) != types.True {
		t.Errorf("ConcurrentEval() = %v, want [1..8]", res.Val)
	}
	if got := maxLive.Load(); got > limit {
		t.Errorf("max concurrent async launches = %d, want <= %d", got, limit)
	}
}

func TestContextEvalRejectsAsync(t *testing.T) {
	env, err := cel.NewEnv(
		cel.Function("rpc",
			cel.Overload("rpc_string", []*cel.Type{cel.StringType}, cel.StringType,
				cel.AsyncBinding(func(ctx context.Context, args ...ref.Val) ref.Val { return args[0] }))),
	)
	if err != nil {
		t.Fatalf("NewEnv() failed: %v", err)
	}
	ast, iss := env.Compile(`rpc("a")`)
	if iss.Err() != nil {
		t.Fatalf("Compile() failed: %v", iss.Err())
	}
	prg, err := env.Program(ast)
	if err != nil {
		t.Fatalf("Program() failed: %v", err)
	}
	_, _, err = prg.ContextEval(context.Background(), cel.NoVars())
	if err == nil || !strings.Contains(err.Error(), "ConcurrentEval") {
		t.Errorf("ContextEval() on async expr = %v, want error mentioning ConcurrentEval", err)
	}
}

func TestEvalRejectsAsync(t *testing.T) {
	env, err := cel.NewEnv(
		cel.Function("rpc",
			cel.Overload("rpc_string", []*cel.Type{cel.StringType}, cel.StringType,
				cel.AsyncBinding(func(ctx context.Context, args ...ref.Val) ref.Val { return args[0] }))),
	)
	if err != nil {
		t.Fatalf("NewEnv() failed: %v", err)
	}
	ast, _ := env.Compile(`rpc("a")`)
	prg, err := env.Program(ast)
	if err != nil {
		t.Fatalf("Program() failed: %v", err)
	}
	_, _, err = prg.Eval(cel.NoVars())
	if err == nil || !strings.Contains(err.Error(), "ConcurrentEval") {
		t.Errorf("Eval() on async expr = %v, want error mentioning ConcurrentEval", err)
	}
}

func TestContextEvalAllowsPartialUnknown(t *testing.T) {
	// A variable unknown from partial evaluation must NOT be mistaken for an async call.
	env, err := cel.NewEnv(cel.Variable("x", cel.IntType))
	if err != nil {
		t.Fatalf("NewEnv() failed: %v", err)
	}
	ast, iss := env.Compile(`x + 1`)
	if iss.Err() != nil {
		t.Fatalf("Compile() failed: %v", iss.Err())
	}
	prg, err := env.Program(ast, cel.EvalOptions(cel.OptPartialEval))
	if err != nil {
		t.Fatalf("Program() failed: %v", err)
	}
	pvars, err := cel.PartialVars(map[string]any{}, cel.AttributePattern("x"))
	if err != nil {
		t.Fatalf("PartialVars() failed: %v", err)
	}
	out, _, err := prg.ContextEval(context.Background(), pvars)
	if err != nil {
		t.Fatalf("ContextEval() with partial unknown returned error: %v", err)
	}
	if !types.IsUnknown(out) {
		t.Errorf("ContextEval() = %v, want Unknown", out)
	}
}

func TestSyncEvalRejectsAsyncBeforeEvaluating(t *testing.T) {
	// The async guard must fire at the entry point, before any evaluation: the async function
	// must never be invoked (no goroutines launched, no work done) for Eval or ContextEval.
	var called atomic.Int32
	env, err := cel.NewEnv(
		cel.Function("rpc",
			cel.Overload("rpc_string", []*cel.Type{cel.StringType}, cel.StringType,
				cel.AsyncBinding(func(ctx context.Context, args ...ref.Val) ref.Val {
					called.Add(1)
					return args[0]
				}))),
	)
	if err != nil {
		t.Fatalf("NewEnv() failed: %v", err)
	}
	ast, iss := env.Compile(`rpc("a")`)
	if iss.Err() != nil {
		t.Fatalf("Compile() failed: %v", iss.Err())
	}
	prg, err := env.Program(ast)
	if err != nil {
		t.Fatalf("Program() failed: %v", err)
	}

	if _, _, err := prg.Eval(cel.NoVars()); err == nil || !strings.Contains(err.Error(), "ConcurrentEval") {
		t.Errorf("Eval() = %v, want ConcurrentEval error", err)
	}
	if _, _, err := prg.ContextEval(context.Background(), cel.NoVars()); err == nil || !strings.Contains(err.Error(), "ConcurrentEval") {
		t.Errorf("ContextEval() = %v, want ConcurrentEval error", err)
	}
	if got := called.Load(); got != 0 {
		t.Errorf("async function invoked %d times; the guard must reject before evaluating", got)
	}
}

func TestSyncEvalRejectedInAsyncEnv(t *testing.T) {
	// Env-level rejection: an environment that declares any async function rejects the synchronous
	// entry points even for an expression that does not call the async function. Callers needing
	// synchronous evaluation should build a separate, non-async environment.
	env, err := cel.NewEnv(
		cel.Variable("x", cel.IntType),
		cel.Function("rpc",
			cel.Overload("rpc_string", []*cel.Type{cel.StringType}, cel.StringType,
				cel.AsyncBinding(func(ctx context.Context, args ...ref.Val) ref.Val { return args[0] }))),
	)
	if err != nil {
		t.Fatalf("NewEnv() failed: %v", err)
	}
	ast, iss := env.Compile(`x + 1`) // pure, synchronous, does not use rpc
	if iss.Err() != nil {
		t.Fatalf("Compile() failed: %v", iss.Err())
	}
	prg, err := env.Program(ast)
	if err != nil {
		t.Fatalf("Program() failed: %v", err)
	}
	if _, _, err := prg.Eval(map[string]any{"x": 1}); err == nil || !strings.Contains(err.Error(), "ConcurrentEval") {
		t.Errorf("Eval() in async env = %v, want ConcurrentEval error", err)
	}

	// A separate non-async env evaluates the same expression synchronously.
	syncEnv, err := cel.NewEnv(cel.Variable("x", cel.IntType))
	if err != nil {
		t.Fatalf("NewEnv() failed: %v", err)
	}
	syncAst, _ := syncEnv.Compile(`x + 1`)
	syncPrg, err := syncEnv.Program(syncAst)
	if err != nil {
		t.Fatalf("Program() failed: %v", err)
	}
	out, _, err := syncPrg.Eval(map[string]any{"x": 1})
	if err != nil {
		t.Fatalf("Eval() in non-async env returned error: %v", err)
	}
	if out.Equal(types.Int(2)) != types.True {
		t.Errorf("Eval() = %v, want 2", out)
	}
}
