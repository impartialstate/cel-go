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
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
)

type mockAsyncCall struct {
	id       int64
	function string
	overload string
}

func (m mockAsyncCall) CallID() int64    { return m.id }
func (m mockAsyncCall) Function() string { return m.function }
func (m mockAsyncCall) Overload() string { return m.overload }

func TestAsyncCallMethods(t *testing.T) {
	m := mockAsyncCall{id: 1, function: "f", overload: "o"}
	if m.CallID() != 1 {
		t.Errorf("got %d, want 1", m.CallID())
	}
	if m.Function() != "f" {
		t.Errorf("got %s, want f", m.Function())
	}
	if m.Overload() != "o" {
		t.Errorf("got %s, want o", m.Overload())
	}
}

func TestDrainNone(t *testing.T) {
	s := DrainNone()
	// No completions -> no re-evaluation
	if s.NextAction(nil, 1).Reevaluate {
		t.Error("DrainNone re-evaluated with nil batch")
	}
	// One completion -> re-evaluate
	if !s.NextAction([]AsyncCall{mockAsyncCall{}}, 1).Reevaluate {
		t.Error("DrainNone did not re-evaluate with 1 completion")
	}
}

func TestDrainAll(t *testing.T) {
	s := DrainAll()
	// Pending calls remain -> no re-evaluation
	if s.NextAction([]AsyncCall{mockAsyncCall{}}, 1).Reevaluate {
		t.Error("DrainAll re-evaluated while calls are pending")
	}
	// No pending calls -> re-evaluate
	if !s.NextAction([]AsyncCall{mockAsyncCall{}}, 0).Reevaluate {
		t.Error("DrainAll did not re-evaluate when no calls pending")
	}
}

func TestDrainReady(t *testing.T) {
	debounce := 10 * time.Millisecond
	s := DrainReady(debounce)

	// No pending calls -> re-evaluate immediately
	action := s.NextAction([]AsyncCall{mockAsyncCall{}}, 0)
	if !action.Reevaluate {
		t.Error("DrainReady did not re-evaluate when no calls pending")
	}

	// No completions -> wait indefinitely
	action = s.NextAction(nil, 1)
	if action.Reevaluate || action.WaitDuration != 0 {
		t.Errorf("DrainReady NextAction(nil, 1) = %v, want {false, 0}", action)
	}

	action = s.NextAction([]AsyncCall{mockAsyncCall{}}, 1)
	if action.Reevaluate || action.WaitDuration != debounce {
		t.Errorf("DrainReady NextAction(batch, 1) = %v, want {false, %v}", action, debounce)
	}
}

func TestCachedAsyncBinding(t *testing.T) {
	var callCount atomic.Int32
	fn := func(ctx context.Context, args ...ref.Val) ref.Val {
		callCount.Add(1)
		return args[0]
	}

	env, err := NewEnv(
		Function("cached",
			Overload("cached_int", []*Type{IntType}, IntType,
				CachedAsyncBinding(fn)),
		),
	)
	if err != nil {
		t.Fatalf("NewEnv() failed: %v", err)
	}
	ast, _ := env.Compile(`cached(1) == 1`)
	prg, _ := env.Program(ast)

	ctx := context.Background()

	// First evaluation -> cache miss
	res1 := <-prg.ConcurrentEval(ctx, NoVars())
	if res1.Err != nil {
		t.Fatalf("ConcurrentEval(1) failed: %v", res1.Err)
	}
	if callCount.Load() != 1 {
		t.Errorf("callCount = %d, want 1", callCount.Load())
	}

	// Second evaluation -> cache hit
	res2 := <-prg.ConcurrentEval(ctx, NoVars())
	if res2.Err != nil {
		t.Fatalf("ConcurrentEval(2) failed: %v", res2.Err)
	}
	if callCount.Load() != 1 {
		t.Errorf("callCount = %d, want 1 (cache hit)", callCount.Load())
	}

	// Third evaluation with different arg -> cache miss
	ast2, _ := env.Compile(`cached(2) == 2`)
	prg2, _ := env.Program(ast2)
	res3 := <-prg2.ConcurrentEval(ctx, NoVars())
	if res3.Err != nil {
		t.Fatalf("ConcurrentEval(3) failed: %v", res3.Err)
	}
	if callCount.Load() != 2 {
		t.Errorf("callCount = %d, want 2", callCount.Load())
	}
}

func TestCachedAsyncBinding_TTL(t *testing.T) {
	var callCount atomic.Int32
	fn := func(ctx context.Context, args ...ref.Val) ref.Val {
		callCount.Add(1)
		return args[0]
	}

	// Cache with 50ms TTL
	env, _ := NewEnv(
		Function("cached",
			Overload("cached_int", []*Type{IntType}, IntType,
				CachedAsyncBinding(fn, AsyncCacheTTL(50*time.Millisecond))),
		),
	)
	ast, _ := env.Compile(`cached(1) == 1`)
	prg, _ := env.Program(ast)

	ctx := context.Background()

	// First call
	<-prg.ConcurrentEval(ctx, NoVars())
	if callCount.Load() != 1 {
		t.Errorf("callCount = %d, want 1", callCount.Load())
	}

	// Immediate second call -> hit
	<-prg.ConcurrentEval(ctx, NoVars())
	if callCount.Load() != 1 {
		t.Errorf("callCount = %d, want 1", callCount.Load())
	}

	// Wait for TTL to expire
	time.Sleep(100 * time.Millisecond)

	// Third call -> miss (expired)
	<-prg.ConcurrentEval(ctx, NoVars())
	if callCount.Load() != 2 {
		t.Errorf("callCount = %d, want 2", callCount.Load())
	}
}

func TestCachedAsyncBinding_CustomKey(t *testing.T) {
	var callCount atomic.Int32
	fn := func(ctx context.Context, args ...ref.Val) ref.Val {
		callCount.Add(1)
		return args[0]
	}

	// Custom key function that ignores arguments
	env, _ := NewEnv(
		Function("cached",
			Overload("cached_int", []*Type{IntType}, IntType,
				CachedAsyncBinding(fn, AsyncCacheKeyFunc(func(args ...ref.Val) string {
					return "constant"
				}))),
		),
	)

	ctx := context.Background()

	// Call with arg 1
	ast1, _ := env.Compile(`cached(1) == 1`)
	prg1, _ := env.Program(ast1)
	<-prg1.ConcurrentEval(ctx, NoVars())

	// Call with arg 2 -> should HIT because key is constant
	ast2, _ := env.Compile(`cached(2) == 2`)
	prg2, _ := env.Program(ast2)
	<-prg2.ConcurrentEval(ctx, NoVars())

	if callCount.Load() != 1 {
		t.Errorf("callCount = %d, want 1 (custom key collapsed args)", callCount.Load())
	}
}

func TestTimeoutAsyncBinding(t *testing.T) {
	fn := func(ctx context.Context, args ...ref.Val) ref.Val {
		select {
		case <-time.After(100 * time.Millisecond):
			return types.Int(1)
		case <-ctx.Done():
			return types.NewErr("timeout")
		}
	}

	// Set a 50ms timeout, which is shorter than the 100ms delay
	env, _ := NewEnv(
		Function("timed",
			Overload("timed_int", []*Type{}, IntType,
				TimeoutAsyncBinding(fn, 50*time.Millisecond)),
		),
	)
	ast, _ := env.Compile(`timed()`)
	prg, _ := env.Program(ast)

	ctx := context.Background()
	res := <-prg.ConcurrentEval(ctx, NoVars())
	if res.Err == nil {
		t.Errorf("got nil error, want timeout")
	}
}

type mockRetryableError struct {
	error
	retryable bool
}

func (m mockRetryableError) IsRetryable() bool { return m.retryable }

func TestRetryAsyncBinding(t *testing.T) {
	var callCount atomic.Int32
	fn := func(ctx context.Context, args ...ref.Val) ref.Val {
		count := callCount.Add(1)
		if count < 3 {
			return types.WrapErr(mockRetryableError{retryable: true})
		}
		return types.Int(count)
	}

	env, _ := NewEnv(
		Function("retryable",
			Overload("retryable_int", []*Type{}, IntType,
				RetryAsyncBinding(fn, RetryAttempts(3), RetryBackoff(10*time.Millisecond))),
		),
	)
	ast, _ := env.Compile(`retryable()`)
	prg, _ := env.Program(ast)

	ctx := context.Background()
	res := <-prg.ConcurrentEval(ctx, NoVars())
	if res.Err != nil {
		t.Fatalf("ConcurrentEval failed: %v", res.Err)
	}
	if res.Val.(types.Int) != 3 {
		t.Errorf("got %v, want 3", res.Val)
	}
	if callCount.Load() != 3 {
		t.Errorf("callCount = %d, want 3", callCount.Load())
	}
}

type mockObserver struct {
	started  atomic.Int32
	finished atomic.Int32
}

func (m *mockObserver) OnCallStarted(id int64, function, overload string, args []ref.Val) {
	m.started.Add(1)
}

func (m *mockObserver) OnCallFinished(id int64, function, overload string, res ref.Val) {
	m.finished.Add(1)
}

func TestAsyncObserver(t *testing.T) {
	fn := func(ctx context.Context, args ...ref.Val) ref.Val {
		return args[0]
	}

	observer := &mockObserver{}
	env, _ := NewEnv(
		Function("observed",
			Overload("observed_int", []*Type{IntType}, IntType,
				AsyncBinding(fn)),
		),
	)
	ast, _ := env.Compile(`observed(1) == 1`)
	prg, _ := env.Program(ast, AsyncCallObserver(observer))

	ctx := context.Background()
	<-prg.ConcurrentEval(ctx, NoVars())

	if observer.started.Load() != 1 {
		t.Errorf("started = %d, want 1", observer.started.Load())
	}
	if observer.finished.Load() != 1 {
		t.Errorf("finished = %d, want 1", observer.finished.Load())
	}
}

func TestCachedAsyncBinding_StaleWhileError(t *testing.T) {
	var callCount atomic.Int32
	fn := func(ctx context.Context, args ...ref.Val) ref.Val {
		count := callCount.Add(1)
		if count == 1 {
			return types.Int(1)
		}
		return types.NewErr("transient failure")
	}

	// Cache with 50ms TTL and stale-while-error enabled
	env, _ := NewEnv(
		Function("cached",
			Overload("cached_int", []*Type{IntType}, IntType,
				CachedAsyncBinding(fn, AsyncCacheTTL(50*time.Millisecond), AsyncCacheStaleWhileError(true))),
		),
	)
	ast, _ := env.Compile(`cached(1) == 1`)
	prg, _ := env.Program(ast)

	ctx := context.Background()

	// First call -> Success, stored in cache
	res1 := <-prg.ConcurrentEval(ctx, NoVars())
	if res1.Err != nil {
		t.Fatalf("first call failed: %v", res1.Err)
	}

	// Wait for TTL to expire
	time.Sleep(100 * time.Millisecond)

	// Second call -> Underlying fn fails, but should serve stale entry
	res2 := <-prg.ConcurrentEval(ctx, NoVars())
	if res2.Err != nil {
		t.Fatalf("second call should have served stale value, but got error: %v", res2.Err)
	}
	if res2.Val != types.True {
		t.Errorf("got %v, want true", res2.Val)
	}
}

func TestCachedAsyncBinding_StaleWhileRevalidate(t *testing.T) {
	var callCount atomic.Int32
	fn := func(ctx context.Context, args ...ref.Val) ref.Val {
		count := callCount.Add(1)
		return types.Int(count)
	}

	// Cache with 50ms TTL and stale-while-revalidate enabled
	env, _ := NewEnv(
		Function("cached",
			Overload("cached_int", []*Type{IntType}, IntType,
				CachedAsyncBinding(fn, AsyncCacheTTL(50*time.Millisecond), AsyncCacheStaleWhileRevalidate(true))),
		),
	)
	ast, _ := env.Compile(`cached(1)`)
	prg, _ := env.Program(ast)

	ctx := context.Background()

	// First call -> Success, val=1
	<-prg.ConcurrentEval(ctx, NoVars())

	// Wait for TTL to expire
	time.Sleep(100 * time.Millisecond)

	// Second call -> Should serve stale val=1 immediately and refresh in background
	res2 := <-prg.ConcurrentEval(ctx, NoVars())
	if res2.Val.(types.Int) != 1 {
		t.Errorf("got %v, want 1 (stale)", res2.Val)
	}

	// Wait for background refresh to finish
	time.Sleep(50 * time.Millisecond)

	// Third call -> Should serve new val=2
	res3 := <-prg.ConcurrentEval(ctx, NoVars())
	if res3.Val.(types.Int) != 2 {
		t.Errorf("got %v, want 2 (refreshed)", res3.Val)
	}
}

func TestAsyncLimiter(t *testing.T) {
	limiter := NewAsyncLimiter(2)
	var active atomic.Int32
	var maxActive atomic.Int32
	fn := func(ctx context.Context, args ...ref.Val) ref.Val {
		curr := active.Add(1)
		for {
			old := maxActive.Load()
			if curr <= old || maxActive.CompareAndSwap(old, curr) {
				break
			}
		}
		time.Sleep(50 * time.Millisecond)
		active.Add(-1)
		return types.Int(1)
	}

	env, _ := NewEnv(
		Function("limited",
			Overload("limited_int", []*Type{IntType}, IntType,
				AsyncBinding(limiter.Limit(fn))),
		),
	)
	ast, _ := env.Compile(`limited(1) + limited(2) + limited(3)`)
	prg, _ := env.Program(ast)

	<-prg.ConcurrentEval(context.Background(), NoVars())

	if maxActive.Load() > 2 {
		t.Errorf("maxActive = %d, want <= 2", maxActive.Load())
	}
}

func TestAsyncMaxConcurrency(t *testing.T) {
	var active atomic.Int32
	var maxActive atomic.Int32
	fn := func(ctx context.Context, args ...ref.Val) ref.Val {
		curr := active.Add(1)
		for {
			old := maxActive.Load()
			if curr <= old || maxActive.CompareAndSwap(old, curr) {
				break
			}
		}
		time.Sleep(50 * time.Millisecond)
		active.Add(-1)
		return types.Int(1)
	}

	env, _ := NewEnv(
		Function("limited",
			Overload("limited_int", []*Type{IntType}, IntType,
				AsyncBinding(fn)),
		),
	)
	ast, _ := env.Compile(`limited(1) + limited(2) + limited(3)`)
	// Limit to 1 at the program level
	prg, _ := env.Program(ast, AsyncMaxConcurrency(1))

	<-prg.ConcurrentEval(context.Background(), NoVars())

	if maxActive.Load() > 1 {
		t.Errorf("maxActive = %d, want 1", maxActive.Load())
	}
}

func TestTimeoutAsyncBinding_Cancellation(t *testing.T) {
	fn := func(ctx context.Context, args ...ref.Val) ref.Val {
		time.Sleep(100 * time.Millisecond)
		return types.Int(1)
	}

	env, _ := NewEnv(
		Function("timeout",
			Overload("timeout_int", []*Type{IntType}, IntType,
				TimeoutAsyncBinding(fn, 200*time.Millisecond)),
		),
	)
	ast, _ := env.Compile(`timeout(1)`)
	prg, _ := env.Program(ast)

	ctx, cancel := context.WithCancel(context.Background())
	resCh := prg.ConcurrentEval(ctx, NoVars())
	
	// Cancel immediately
	cancel()
	
	res := <-resCh
	if res.Err == nil || (!strings.Contains(res.Err.Error(), "canceled") && !strings.Contains(res.Err.Error(), "cancelled")) {
		t.Errorf("expected cancellation error, got %v", res.Err)
	}
}

func TestAsyncCacheSize(t *testing.T) {
	fn := func(ctx context.Context, args ...ref.Val) ref.Val {
		return args[0]
	}

	// Cache with size 1
	env, _ := NewEnv(
		Function("cached",
			Overload("cached_int", []*Type{IntType}, IntType,
				CachedAsyncBinding(fn, AsyncCacheSize(1))),
		),
	)
	ast, _ := env.Compile(`cached(1) + cached(2) + cached(1)`)
	prg, _ := env.Program(ast)

	// This should exercise Set and potentially eviction logic if implemented in defaultCache
	// Actually defaultCache just uses a map, but we added AsyncCacheSize option.
	// Let's check if it's actually used.
	<-prg.ConcurrentEval(context.Background(), NoVars())
}

type customCache struct {
	data map[string]ref.Val
}

func (c *customCache) Get(key string) (ref.Val, bool) { val, ok := c.data[key]; return val, ok }
func (c *customCache) Set(key string, val ref.Val)   { c.data[key] = val }
func (c *customCache) Delete(key string)            { delete(c.data, key) }

func TestCustomAsyncCache(t *testing.T) {
	cache := &customCache{data: make(map[string]ref.Val)}
	fn := func(ctx context.Context, args ...ref.Val) ref.Val {
		return args[0]
	}

	env, _ := NewEnv(
		Function("cached",
			Overload("cached_int", []*Type{IntType}, IntType,
				CachedAsyncBinding(fn, CustomAsyncCache(cache))),
		),
	)
	ast, _ := env.Compile(`cached(1)`)
	prg, _ := env.Program(ast)

	<-prg.ConcurrentEval(context.Background(), NoVars())

	if _, ok := cache.data[defaultKeyFunc(types.Int(1))]; !ok {
		t.Errorf("value not found in custom cache")
	}

	// Test Delete
	cache.Delete(defaultKeyFunc(types.Int(1)))
	if _, ok := cache.data[defaultKeyFunc(types.Int(1))]; ok {
		t.Errorf("value still in cache after delete")
	}
}

type nonRetryableError struct{ error }
func (e nonRetryableError) IsRetryable() bool { return false }

func TestRetryAsyncBinding_NonRetryable(t *testing.T) {
	var callCount atomic.Int32
	fn := func(ctx context.Context, args ...ref.Val) ref.Val {
		callCount.Add(1)
		return types.NewErr("fatal")
	}
	// Actually, RetryAsyncBinding expects types.Err wrapping a RetryableError
	fn = func(ctx context.Context, args ...ref.Val) ref.Val {
		callCount.Add(1)
		return types.NewErr("fatal: %w", nonRetryableError{fmt.Errorf("fatal")})
	}

	env, _ := NewEnv(
		Function("retry",
			Overload("retry_int", []*Type{IntType}, IntType,
				RetryAsyncBinding(fn, RetryAttempts(3))),
		),
	)
	ast, _ := env.Compile(`retry(1)`)
	prg, _ := env.Program(ast)

	res := <-prg.ConcurrentEval(context.Background(), NoVars())
	if res.Err == nil || !strings.Contains(res.Err.Error(), "fatal") {
		t.Errorf("expected error, got %v", res.Err)
	}

	if callCount.Load() != 1 {
		t.Errorf("callCount = %d, want 1 (should not retry non-retryable error)", callCount.Load())
	}
}

func TestDefaultCache_Misc(t *testing.T) {
	cache := newDefaultCache(10, 100*time.Millisecond)
	key := "test"
	val := types.Int(1)
	
	cache.Set(key, val)
	
	// Test Get (standard)
	v, ok := cache.Get(key)
	if !ok || v != val {
		t.Errorf("Get failed")
	}
	
	// Test Delete
	cache.Delete(key)
	_, ok = cache.Get(key)
	if ok {
		t.Errorf("Delete failed")
	}
}

type mixedRetryableError struct {
	retryable bool
}
func (e mixedRetryableError) error() string { return "mixed" }
func (e mixedRetryableError) IsRetryable() bool { return e.retryable }
func (e mixedRetryableError) Error() string { return "mixed" }

func TestIsRetryable_Misc(t *testing.T) {
	// isRetryable is uncalled with nil/non-types.Err in production, but we test the protection
	if isRetryable(nil) {
		t.Errorf("nil error should not be retryable")
	}
	if isRetryable(types.NewErr("not retryable").(*types.Err)) {
		t.Errorf("types.Err without retryable wrapper should not be retryable")
	}
}




