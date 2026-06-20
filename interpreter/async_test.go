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

package interpreter

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/cel-go/common/functions"
	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
)

// asyncReturning returns an AsyncOp that immediately produces the given value, while counting the
// number of times the implementation is invoked.
func asyncReturning(val ref.Val, calls *atomic.Int32) functions.AsyncOp {
	return func(ctx context.Context, args ...ref.Val) <-chan ref.Val {
		if calls != nil {
			calls.Add(1)
		}
		ch := make(chan ref.Val, 1)
		ch <- val
		close(ch)
		return ch
	}
}

// newTestFrame creates an ExecutionFrame with an evaluation context attached.
func newTestFrame(t *testing.T, ctx context.Context) (*ExecutionFrame, func()) {
	t.Helper()
	frame, err := NewExecutionFrame(EmptyActivation())
	if err != nil {
		t.Fatalf("NewExecutionFrame() failed: %v", err)
	}
	if err := frame.SetContext(ctx, 0); err != nil {
		t.Fatalf("SetContext() failed: %v", err)
	}
	return frame, frame.Close
}

// awaitResult re-evaluates ComputeResult until it returns a non-Unknown value or the deadline fires.
func awaitResult(t *testing.T, frame *ExecutionFrame, completions <-chan int64, id int64, fn, overload string, impl functions.AsyncOp, args ...ref.Val) ref.Val {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for {
		res := frame.ComputeResult(id, fn, overload, impl, args)
		if !types.IsUnknown(res) {
			return res
		}
		select {
		case callID := <-completions:
			frame.NotifyCompletion(callID)
		case <-deadline:
			t.Fatal("timed out waiting for async result")
		}
	}
}

func TestComputeResultWithoutContext(t *testing.T) {
	frame, err := NewExecutionFrame(EmptyActivation())
	if err != nil {
		t.Fatalf("NewExecutionFrame() failed: %v", err)
	}
	defer frame.Close()
	// No SetContext call, so async tracking is uninitialized.
	res := frame.ComputeResult(1, "fn", "fn_overload", asyncReturning(types.Int(1), nil), nil)
	if !types.IsError(res) {
		t.Errorf("ComputeResult() got %v, wanted error when tracking uninitialized", res)
	}
	if frame.PendingAsyncCalls() != 0 {
		t.Errorf("PendingAsyncCalls() = %d, wanted 0", frame.PendingAsyncCalls())
	}
	if call := frame.AsyncCall(1); call != nil {
		t.Errorf("AsyncCall() = %v, wanted nil", call)
	}
}

func TestComputeResultResolves(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	frame, closeFrame := newTestFrame(t, ctx)
	defer closeFrame()

	completions := make(chan int64, 1)
	frame.SetCompletions(completions)

	var calls atomic.Int32
	impl := asyncReturning(types.Int(42), &calls)
	res := awaitResult(t, frame, completions, 1, "fn", "fn_int", impl, types.Int(1))
	if res.Equal(types.Int(42)) != types.True {
		t.Errorf("async result = %v, wanted 42", res)
	}
	if got := calls.Load(); got != 1 {
		t.Errorf("impl invoked %d times, wanted exactly 1", got)
	}
}

func TestTrackerDedupAndCallIDs(t *testing.T) {
	// Exercise the tracker directly to keep bookkeeping assertions isolated from the
	// process-global frame/tracker pools used by ExecutionFrame.
	tracker := newAsyncCallStateTracker()
	completions := make(chan int64, 4)
	impl := asyncReturning(types.Int(1), nil)

	// Same node id, same args, invoked repeatedly: must dedup to a single registered call.
	a1 := tracker.getOrCreate(1, "fn", "fn_int", []ref.Val{types.Int(1)}, impl, completions)
	a2 := tracker.getOrCreate(1, "fn", "fn_int", []ref.Val{types.Int(1)}, impl, completions)
	if a1 != a2 {
		t.Error("getOrCreate returned distinct states for identical (id, args)")
	}
	if a1.CallID() != a2.CallID() {
		t.Errorf("dedup callIDs differ: %d vs %d", a1.CallID(), a2.CallID())
	}
	if got := tracker.getByID(a1.CallID()); got != a1 {
		t.Errorf("getByID(%d) did not return the registered call", a1.CallID())
	}

	// Same node id, different args: a new call must be registered (re-evaluation with new inputs).
	a3 := tracker.getOrCreate(1, "fn", "fn_int", []ref.Val{types.Int(2)}, impl, completions)
	if a3.CallID() == a1.CallID() {
		t.Error("arg change reused the prior callID, wanted a fresh call")
	}
	if got := tracker.getByID(a3.CallID()); got != a3 {
		t.Errorf("getByID(%d) did not return the second registered call", a3.CallID())
	}
	// Registration alone does not mark a call as in-flight; pending counts launched calls only.
	if got := tracker.pendingCount(); got != 0 {
		t.Errorf("pendingCount() after registration only = %d, wanted 0", got)
	}
}

func TestHashCall(t *testing.T) {
	// Stable for equal inputs.
	if hashCall(1, "ov", []ref.Val{types.String("a")}) != hashCall(1, "ov", []ref.Val{types.String("a")}) {
		t.Error("hashCall not stable for equal inputs")
	}
	// String and bool values participate in the hash.
	if hashCall(1, "ov", []ref.Val{types.String("a")}) == hashCall(1, "ov", []ref.Val{types.String("b")}) {
		t.Error("distinct string args produced the same hash")
	}
	if hashCall(1, "ov", []ref.Val{types.Bool(true)}) == hashCall(1, "ov", []ref.Val{types.Bool(false)}) {
		t.Error("distinct bool args produced the same hash")
	}
	// Node id and overload participate in the hash.
	if hashCall(1, "ov", nil) == hashCall(2, "ov", nil) {
		t.Error("distinct node ids produced the same hash")
	}
	if hashCall(1, "a", nil) == hashCall(1, "b", nil) {
		t.Error("distinct overloads produced the same hash")
	}
	// Numeric and complex values are intentionally excluded from the hash; distinct numeric args
	// share a bucket and rely on equals for disambiguation.
	if hashCall(1, "ov", []ref.Val{types.Int(1)}) != hashCall(1, "ov", []ref.Val{types.Int(2)}) {
		t.Error("numeric args must not contribute to the hash")
	}
	if hashCall(1, "ov", []ref.Val{types.Double(1.5)}) != hashCall(1, "ov", []ref.Val{types.Double(9.9)}) {
		t.Error("double args must not contribute to the hash")
	}
	// Argument positions are separated to avoid cross-argument collisions.
	if hashCall(1, "ov", []ref.Val{types.String("a"), types.String("bc")}) ==
		hashCall(1, "ov", []ref.Val{types.String("ab"), types.String("c")}) {
		t.Error("adjacent string args collided across the boundary")
	}
}

func TestTrackerComprehensionReuse(t *testing.T) {
	// Regression: a single AST node id evaluated repeatedly with distinct argument values (as
	// happens for an async call inside a comprehension) must register one call per distinct
	// argument set, and re-lookups must return the existing state rather than relaunching. A
	// node-id-keyed map collapses these into a single slot, causing every re-evaluation pass to
	// relaunch every iteration and never converge.
	tracker := newAsyncCallStateTracker()
	completions := make(chan int64, 8)
	impl := asyncReturning(types.Int(0), nil)
	const id = int64(1)

	args := [][]ref.Val{
		{types.Int(1)},
		{types.Int(2)},
		{types.Int(3)},
	}
	states := make([]*asyncCallState, len(args))
	for i, a := range args {
		states[i] = tracker.getOrCreate(id, "fn", "fn_int", a, impl, completions)
	}

	// Each distinct argument set is a distinct, uniquely-identified call.
	seen := map[int64]bool{}
	for _, s := range states {
		if seen[s.CallID()] {
			t.Errorf("duplicate callID %d across distinct args", s.CallID())
		}
		seen[s.CallID()] = true
	}

	// Re-evaluation: every prior (id, args) tuple must resolve to its existing state and must
	// not register a new call.
	for i, a := range args {
		if got := tracker.getOrCreate(id, "fn", "fn_int", a, impl, completions); got != states[i] {
			t.Errorf("re-lookup of args %v returned a new state, wanted the existing one", a)
		}
	}

	// Identical string arguments at the same node dedup to a single call.
	s1 := tracker.getOrCreate(2, "fn", "fn_str", []ref.Val{types.String("k")}, impl, completions)
	s2 := tracker.getOrCreate(2, "fn", "fn_str", []ref.Val{types.String("k")}, impl, completions)
	if s1 != s2 {
		t.Error("identical string args did not dedup to a single call")
	}
}

func TestTrackerRegistrationLookup(t *testing.T) {
	tracker := newAsyncCallStateTracker()
	completions := make(chan int64, 2)
	impl := asyncReturning(types.Int(1), nil)

	a := tracker.getOrCreate(1, "fn", "a", []ref.Val{types.Int(1)}, impl, completions)
	b := tracker.getOrCreate(2, "fn", "b", []ref.Val{types.Int(2)}, impl, completions)

	// Registered calls must be retrievable by callID.
	for _, want := range []*asyncCallState{a, b} {
		if got := tracker.getByID(want.CallID()); got != want {
			t.Errorf("getByID(%d) returned a different call record", want.CallID())
		}
	}
	// Unknown callIDs resolve to nil rather than panicking.
	if got := tracker.getByID(99999); got != nil {
		t.Errorf("getByID(unknown) = %v, wanted nil", got)
	}
}

func TestAsyncObserverLifecycle(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	frame, closeFrame := newTestFrame(t, ctx)
	defer closeFrame()

	obs := &recordingObserver{}
	frame.SetAsyncObserver(obs)
	completions := make(chan int64, 1)
	frame.SetCompletions(completions)

	awaitResult(t, frame, completions, 1, "fn", "fn_int", asyncReturning(types.Int(7), nil), types.Int(1))

	if got := obs.started.Load(); got != 1 {
		t.Errorf("OnCallStarted called %d times, wanted 1", got)
	}
	if got := obs.finished.Load(); got != 1 {
		t.Errorf("OnCallFinished called %d times, wanted 1", got)
	}
}

// asyncControllable returns a channel-based AsyncOp whose calls block until release is closed,
// tracking the number of concurrently live calls and the high-water mark.
func asyncControllable(release <-chan struct{}, live, maxLive *atomic.Int32) functions.AsyncOp {
	return func(ctx context.Context, args ...ref.Val) <-chan ref.Val {
		ch := make(chan ref.Val, 1)
		go func() {
			cur := live.Add(1)
			for {
				old := maxLive.Load()
				if cur <= old || maxLive.CompareAndSwap(old, cur) {
					break
				}
			}
			select {
			case <-release:
			case <-ctx.Done():
			}
			live.Add(-1)
			ch <- args[0]
			close(ch)
		}()
		return ch
	}
}

func TestLaunchAdmissionAndBounding(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	frame, closeFrame := newTestFrame(t, ctx)
	defer closeFrame()

	const limit = 2
	const total = 5
	frame.SetAsyncMaxConcurrency(limit)
	completions := make(chan int64, total*2)
	frame.SetCompletions(completions)

	release := make(chan struct{})
	var live, maxLive atomic.Int32
	impl := asyncControllable(release, &live, &maxLive)

	// A single evaluation pass: attempt to launch all calls (distinct node ids).
	pass := func() {
		for i := 0; i < total; i++ {
			frame.ComputeResult(int64(i+1), "fn", "fn_int", impl, []ref.Val{types.Int(int64(i))})
		}
	}

	// First pass admits only `limit` launches; the rest are deferred. pending is updated
	// synchronously as calls are admitted, so it must equal the limit immediately.
	pass()
	if got := frame.PendingAsyncCalls(); got != limit {
		t.Fatalf("PendingAsyncCalls() after first pass = %d, wanted %d", got, limit)
	}

	// Drive completions and re-evaluate until every call has resolved, simulating ConcurrentEval.
	close(release)
	done := map[int64]bool{}
	deadline := time.After(5 * time.Second)
	for len(done) < total {
		pass() // re-evaluate: launch any newly-admissible calls
		select {
		case id := <-completions:
			frame.NotifyCompletion(id)
			done[id] = true
		case <-deadline:
			t.Fatalf("only %d/%d calls completed; maxLive=%d", len(done), total, maxLive.Load())
		}
	}

	if got := maxLive.Load(); got > limit {
		t.Errorf("max concurrent launches = %d, wanted <= %d", got, limit)
	}
	if got := frame.PendingAsyncCalls(); got != 0 {
		t.Errorf("PendingAsyncCalls() after all completions = %d, wanted 0", got)
	}
}

func TestLaunchUnlimitedWhenNoSemaphore(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	frame, closeFrame := newTestFrame(t, ctx)
	defer closeFrame()
	// No SetAsyncMaxConcurrency -> nil semaphore -> all calls admitted in one pass.
	completions := make(chan int64, 8)
	frame.SetCompletions(completions)

	const total = 5
	for i := 0; i < total; i++ {
		frame.ComputeResult(int64(i+1), "fn", "fn_int", asyncReturning(types.Int(int64(i)), nil), []ref.Val{types.Int(int64(i))})
	}
	if got := frame.PendingAsyncCalls(); got != total {
		t.Errorf("PendingAsyncCalls() with no limit = %d, wanted %d", got, total)
	}
}

func TestAsyncCallStateCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	frame, closeFrame := newTestFrame(t, ctx)
	defer closeFrame()

	completions := make(chan int64, 1)
	frame.SetCompletions(completions)

	var exited sync.WaitGroup
	exited.Add(1)
	blocking := func(ctx context.Context, args ...ref.Val) <-chan ref.Val {
		ch := make(chan ref.Val) // never written; goroutine must exit via ctx.Done
		go func() {
			defer exited.Done()
			<-ctx.Done()
		}()
		return ch
	}
	res := frame.ComputeResult(1, "fn", "fn_int", blocking, []ref.Val{types.Int(1)})
	if !types.IsUnknown(res) {
		t.Fatalf("ComputeResult() = %v, wanted Unknown while pending", res)
	}
	cancel()

	done := make(chan struct{})
	go func() { exited.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("async impl goroutine did not exit on cancellation")
	}
	// No completion should have been delivered for the cancelled call.
	select {
	case callID := <-completions:
		t.Errorf("unexpected completion for cancelled call: %d", callID)
	default:
	}
}

func TestAsyncTrackerPoolReleaseClearsState(t *testing.T) {
	tracker := newAsyncCallStateTracker()
	completions := make(chan int64, 4)
	for i := int64(1); i <= 3; i++ {
		tracker.getOrCreate(i, "fn", "ov", []ref.Val{types.Int(i)}, asyncReturning(types.Int(i), nil), completions)
	}
	// Simulate launched calls so the release path is exercised against non-zero in-flight state.
	tracker.pendingCalls.Store(3)
	if tracker.getByID(2) == nil {
		t.Fatal("getByID(2) = nil before release, wanted a call record")
	}

	asyncCallStateTrackerPool.release(tracker)

	if got := tracker.pendingCount(); got != 0 {
		t.Errorf("pendingCount() after release = %d, wanted 0", got)
	}
	if got := len(tracker.calls); got != 0 {
		t.Errorf("calls map size after release = %d, wanted 0", got)
	}
	if got := len(tracker.callsByID); got != 0 {
		t.Errorf("callsByID map size after release = %d, wanted 0", got)
	}
	if got := tracker.nextCallID.Load(); got != 0 {
		t.Errorf("nextCallID after release = %d, wanted 0", got)
	}
	// A released tracker is safe to reuse: callIDs restart and bookkeeping is fresh.
	acs := tracker.getOrCreate(1, "fn", "ov", []ref.Val{types.Int(1)}, asyncReturning(types.Int(1), nil), completions)
	if acs.CallID() != 1 {
		t.Errorf("reused tracker assigned callID %d, wanted 1", acs.CallID())
	}
}

func TestAsyncCallStateEquals(t *testing.T) {
	mk := func(fn, ov string, args ...ref.Val) *asyncCallState {
		return newAsyncCallState(1, fn, ov, args, nil)
	}
	base := mk("fn", "ov", types.Int(1), types.String("a"))
	tests := []struct {
		name  string
		other *asyncCallState
		want  bool
	}{
		{"identical", mk("fn", "ov", types.Int(1), types.String("a")), true},
		{"diff function", mk("other", "ov", types.Int(1), types.String("a")), false},
		{"diff overload", mk("fn", "other", types.Int(1), types.String("a")), false},
		{"diff arg value", mk("fn", "ov", types.Int(2), types.String("a")), false},
		{"diff arity", mk("fn", "ov", types.Int(1)), false},
		{"nil other", nil, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := base.equals(tc.other); got != tc.want {
				t.Errorf("equals() = %v, wanted %v", got, tc.want)
			}
		})
	}
}

func TestExecutionFrameChildSharesAsyncContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	frame, closeFrame := newTestFrame(t, ctx)
	defer closeFrame()

	completions := make(chan int64, 1)
	frame.SetCompletions(completions)

	// Counts are taken relative to a baseline, since ExecutionFrame draws its tracker from a
	// process-global pool whose starting pending count is not guaranteed to be zero.
	base := frame.PendingAsyncCalls()

	child := frame.Push(EmptyActivation())
	if got := child.PendingAsyncCalls(); got != base {
		t.Errorf("child PendingAsyncCalls() = %d, wanted %d (shared parent ctx)", got, base)
	}

	// A call launched from the child frame must be visible through the shared parent tracker.
	child.ComputeResult(1, "fn", "fn_int", asyncReturning(types.Int(5), nil), []ref.Val{types.Int(1)})
	if got := child.PendingAsyncCalls(); got != base+1 {
		t.Errorf("child PendingAsyncCalls() after launch = %d, wanted %d", got, base+1)
	}

	parent := child.Pop()
	if parent != frame {
		t.Fatalf("Pop() did not return the parent frame")
	}
	if got := frame.PendingAsyncCalls(); got != base+1 {
		t.Errorf("parent PendingAsyncCalls() after Pop = %d, wanted %d", got, base+1)
	}
	// The launched call is retrievable via the parent frame's AsyncCall accessor.
	select {
	case callID := <-completions:
		if call := frame.AsyncCall(callID); call == nil {
			t.Errorf("AsyncCall(%d) = nil, wanted a call record from the shared tracker", callID)
		}
		frame.NotifyCompletion(callID)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for child call completion")
	}
}

type recordingObserver struct {
	started  atomic.Int32
	finished atomic.Int32
}

func (o *recordingObserver) OnCallStarted(callID int64, function, overload string, args []ref.Val) {
	o.started.Add(1)
}

func (o *recordingObserver) OnCallFinished(callID int64, function, overload string, res ref.Val) {
	o.finished.Add(1)
}
