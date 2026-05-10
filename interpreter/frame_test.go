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
	"testing"
	"time"

	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
)

func TestFrameInterrupt(t *testing.T) {
	frame := NewExecutionFrame(EmptyActivation())
	defer frame.Close()

	ctx, cancel := context.WithCancel(context.Background())
	frame.SetContext(ctx, 1)

	if frame.CheckInterrupt() {
		t.Error("frame.CheckInterrupt() returned true, wanted false")
	}

	cancel()

	if !frame.CheckInterrupt() {
		t.Error("frame.CheckInterrupt() returned false, wanted true")
	}
}

func TestFrameAsyncErrors(t *testing.T) {
	frame := NewExecutionFrame(EmptyActivation())
	defer frame.Close()

	// ComputeResult without SetContext should return an error
	res := frame.ComputeResult(1, "test", "test", nil, nil)
	if !types.IsError(res) {
		t.Errorf("ComputeResult() returned %v, wanted error", res)
	}
}

func TestFrameCompletions(t *testing.T) {
	frame := NewExecutionFrame(EmptyActivation())
	defer frame.Close()

	ctx := context.Background()
	frame.SetContext(ctx, 1)

	completionCh := make(chan int64, 1)
	frame.SetCompletions(completionCh)

	syncCh := make(chan ref.Val, 1)
	impl := func(ctx context.Context, args ...ref.Val) <-chan ref.Val {
		return syncCh
	}

	// First call returns unknown
	res := frame.ComputeResult(10, "test", "test", impl, nil)
	if !types.IsUnknown(res) {
		t.Fatalf("ComputeResult() returned %v, wanted unknown", res)
	}

	// Send result
	syncCh <- types.String("done")

	// Wait for completion signal
	select {
	case id := <-completionCh:
		if id != res.(*types.Unknown).IDs()[0] {
			t.Errorf("completion signal ID mismatch: got %d, wanted %d", id, res.(*types.Unknown).IDs()[0])
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for completion signal")
	}

	// Second call should return the result
	res2 := frame.ComputeResult(10, "test", "test", impl, nil)
	if res2.Equal(types.String("done")) != types.True {
		t.Errorf("ComputeResult() returned %v, wanted 'done'", res2)
	}
}

func TestEvalAsyncFunc(t *testing.T) {
	frame := NewExecutionFrame(EmptyActivation())
	defer frame.Close()
	frame.SetContext(context.Background(), 1)

	impl := func(ctx context.Context, args ...ref.Val) <-chan ref.Val {
		ch := make(chan ref.Val, 1)
		ch <- types.Int(args[0].(types.Int) + 1)
		return ch
	}

	fn := &evalAsyncFunc{
		id:       1,
		function: "inc",
		overload: "inc_int",
		args:     []InterpretableV2{NewConstValue(1, types.Int(10))},
		impl:     impl,
	}

	// First exec triggers the call
	res := fn.Exec(frame)
	if !types.IsUnknown(res) {
		t.Fatalf("Exec() returned %v, wanted unknown", res)
	}

	// Test Args()
	if len(fn.Args()) != 1 {
		t.Errorf("Args() returned %d, wanted 1", len(fn.Args()))
	}

	// Wait for async result (it's buffered in the impl channel in this test)
	time.Sleep(10 * time.Millisecond)

	// Test Eval() returns result
	res3 := fn.Eval(frame)
	if res3.Equal(types.Int(11)) != types.True {
		t.Errorf("Eval() returned %v, wanted 11", res3)
	}
}

func TestEvalAsyncFuncShortCircuit(t *testing.T) {
	frame := NewExecutionFrame(EmptyActivation())
	defer frame.Close()
	frame.SetContext(context.Background(), 1)

	fn := &evalAsyncFunc{
		id:       1,
		function: "inc",
		args:     []InterpretableV2{NewConstValue(2, types.NewErr("bad arg"))},
	}

	res := fn.Exec(frame)
	if !types.IsError(res) {
		t.Errorf("Exec() returned %v, wanted error", res)
	}

	fn.args = []InterpretableV2{NewConstValue(3, types.NewUnknown(4, nil))}
	res = fn.Exec(frame)
	if !types.IsUnknown(res) {
		t.Errorf("Exec() returned %v, wanted unknown", res)
	}
}
