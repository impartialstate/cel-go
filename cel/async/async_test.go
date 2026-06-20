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

package async_test

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/cel-go/cel/async"
	"github.com/google/cel-go/common/decls"
	"github.com/google/cel-go/common/functions"
	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
)

type retryableTestErr struct{}

func (retryableTestErr) Error() string     { return "retry me" }
func (retryableTestErr) IsRetryable() bool { return true }

// buildZeroArgAsync builds a zero-arity async overload from an option and returns its AsyncOp.
func buildZeroArgAsync(t *testing.T, opt decls.OverloadOpt) functions.AsyncOp {
	t.Helper()
	fnDecl, err := decls.NewFunction("fn", decls.Overload("fn_zero", []*types.Type{}, types.IntType, opt))
	if err != nil {
		t.Fatalf("NewFunction() failed: %v", err)
	}
	bindings, err := fnDecl.Bindings()
	if err != nil {
		t.Fatalf("Bindings() failed: %v", err)
	}
	for _, b := range bindings {
		if b.Async != nil {
			return b.Async
		}
	}
	t.Fatal("no async binding produced")
	return nil
}

func TestRetryBindingCancellation(t *testing.T) {
	var attempts atomic.Int32
	op := buildZeroArgAsync(t, async.RetryBinding(func(ctx context.Context, args ...ref.Val) ref.Val {
		attempts.Add(1)
		return types.WrapErr(retryableTestErr{})
	}, async.RetryAttempts(5), async.RetryBackoff(500*time.Millisecond)))

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(40 * time.Millisecond)
		cancel()
	}()

	start := time.Now()
	var res ref.Val
	select {
	case res = <-op(ctx):
	case <-time.After(2 * time.Second):
		t.Fatal("retry op did not return after cancellation")
	}
	elapsed := time.Since(start)

	if !types.IsError(res) || !strings.Contains(res.(*types.Err).Error(), "cancelled") {
		t.Errorf("result = %v, want a cancellation error", res)
	}
	// Cancellation must interrupt the backoff wait rather than running it to completion.
	if elapsed >= 500*time.Millisecond {
		t.Errorf("retry waited the full backoff (%v); cancellation did not interrupt it", elapsed)
	}
}

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
	s := async.DrainNone()
	// No completions, calls still pending -> no re-evaluation
	if s.NextAction(nil, 1).Reevaluate {
		t.Error("DrainNone re-evaluated with nil batch")
	}
	// One completion -> re-evaluate
	if !s.NextAction([]async.Call{mockAsyncCall{}}, 1).Reevaluate {
		t.Error("DrainNone did not re-evaluate with 1 completion")
	}
	// No pending -> re-evaluate regardless of batch
	if !s.NextAction(nil, 0).Reevaluate {
		t.Error("DrainNone did not re-evaluate when nothing pending")
	}
}

func TestDrainAll(t *testing.T) {
	s := async.DrainAll()
	// Pending calls remain -> no re-evaluation
	if s.NextAction([]async.Call{mockAsyncCall{}}, 1).Reevaluate {
		t.Error("DrainAll re-evaluated while calls are pending")
	}
	// No pending calls -> re-evaluate
	if !s.NextAction([]async.Call{mockAsyncCall{}}, 0).Reevaluate {
		t.Error("DrainAll did not re-evaluate when no calls pending")
	}
}

func TestDrainReady(t *testing.T) {
	debounce := 10 * time.Millisecond
	s := async.DrainReady(debounce)

	// No pending calls -> re-evaluate immediately
	action := s.NextAction([]async.Call{mockAsyncCall{}}, 0)
	if !action.Reevaluate {
		t.Error("DrainReady did not re-evaluate when no calls pending")
	}

	// No completions -> wait indefinitely
	action = s.NextAction(nil, 1)
	if action.Reevaluate || action.WaitDuration != 0 {
		t.Errorf("DrainReady NextAction(nil, 1) = %v, want {false, 0}", action)
	}

	// Some completions, still pending -> wait for the debounce window
	action = s.NextAction([]async.Call{mockAsyncCall{}}, 1)
	if action.Reevaluate || action.WaitDuration != debounce {
		t.Errorf("DrainReady NextAction(batch, 1) = %v, want {false, %v}", action, debounce)
	}
}

func TestLimiterConcurrency(t *testing.T) {
	limiter := async.NewLimiter(2)
	var active atomic.Int32
	var maxActive atomic.Int32
	release := make(chan struct{})
	fn := limiter.Limit(func(ctx context.Context, args ...ref.Val) ref.Val {
		curr := active.Add(1)
		for {
			old := maxActive.Load()
			if curr <= old || maxActive.CompareAndSwap(old, curr) {
				break
			}
		}
		<-release
		active.Add(-1)
		return types.Int(1)
	})

	var wg sync.WaitGroup
	const n = 6
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			fn(context.Background())
		}()
	}
	// Allow goroutines to contend for the two slots before releasing.
	time.Sleep(50 * time.Millisecond)
	close(release)
	wg.Wait()

	if got := maxActive.Load(); got > 2 {
		t.Errorf("maxActive = %d, want <= 2", got)
	}
}

func TestLimiterCancel(t *testing.T) {
	l := async.NewLimiter(1)

	// Occupy the only slot with a call that blocks until its context is cancelled.
	occupyCtx, occupyCancel := context.WithCancel(context.Background())
	defer occupyCancel()
	fn1 := l.Limit(func(ctx context.Context, args ...ref.Val) ref.Val {
		<-ctx.Done()
		return types.Int(1)
	})
	go fn1(occupyCtx)

	// Ensure fn1 has acquired the slot.
	time.Sleep(10 * time.Millisecond)

	// A second call must block on the limiter and then fail when its context is cancelled.
	ctx, cancel := context.WithCancel(context.Background())
	fn2 := l.Limit(func(ctx context.Context, args ...ref.Val) ref.Val {
		return types.Int(2)
	})
	cancel()
	res := fn2(ctx)
	if !types.IsError(res) {
		t.Fatalf("expected cancellation error, got %v", res)
	}
	msg := res.(*types.Err).Error()
	if !strings.Contains(msg, "cancel") {
		t.Errorf("expected cancellation error, got %q", msg)
	}
}

func TestTimeoutBindingEnforcesAgainstContextIgnoringOp(t *testing.T) {
	// The wrapped op sleeps well past the timeout without consulting its context. TimeoutBinding
	// must still return at the deadline rather than waiting for the op to finish.
	op := buildZeroArgAsync(t, async.TimeoutBinding(func(ctx context.Context, args ...ref.Val) ref.Val {
		time.Sleep(2 * time.Second) // ignores ctx entirely
		return types.Int(1)
	}, 50*time.Millisecond))

	start := time.Now()
	var res ref.Val
	select {
	case res = <-op(context.Background()):
	case <-time.After(2 * time.Second):
		t.Fatal("TimeoutBinding did not return; the op was not abandoned")
	}
	elapsed := time.Since(start)

	if !types.IsError(res) || !strings.Contains(res.(*types.Err).Error(), "timed out") {
		t.Errorf("result = %v, want a timeout error", res)
	}
	if elapsed >= 500*time.Millisecond {
		t.Errorf("TimeoutBinding waited %v; the timeout must abandon a ctx-ignoring op", elapsed)
	}
}
