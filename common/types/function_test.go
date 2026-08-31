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

package types

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/google/cel-go/common"
	"github.com/google/cel-go/common/functions"
	"github.com/google/cel-go/common/types/ref"
	"github.com/google/cel-go/common/types/traits"
)

var (
	lessThanEstimate = common.FixedCallEstimate(2)
	lessThanType     = NewFunctionType(lessThanEstimate, BoolType, IntType, IntType)
	lessThanFn       = NewFunctionVal("lessThan", lessThanType, lessThanEstimate,
		func(frame functions.ExecutionFrame, args ...ref.Val) ref.Val {
			return Bool(args[0].(Int) < args[1].(Int))
		})
)

// testFrame is a minimal ExecutionFrame whose copies share the cost charged to them, and which
// fails charges beyond an optional limit.
type testFrame struct {
	cost  *uint64
	limit *uint64
	vars  functions.Bindings
}

var _ functions.ExecutionFrame = &testFrame{}

func newTestFrame(vars functions.Bindings, limit *uint64) *testFrame {
	return &testFrame{cost: new(uint64), limit: limit, vars: vars}
}

func (f *testFrame) ResolveName(name string) (any, bool) {
	if f.vars == nil {
		return nil, false
	}
	return f.vars.ResolveName(name)
}

func (f *testFrame) WithBindings(vars functions.Bindings) functions.ExecutionFrame {
	return &testFrame{cost: f.cost, limit: f.limit, vars: vars}
}

func (f *testFrame) ChargeCost(cost uint64) error {
	*f.cost += cost
	if f.limit != nil && *f.cost > *f.limit {
		return fmt.Errorf("cost limit exceeded")
	}
	return nil
}

func (f *testFrame) Cost() uint64 { return *f.cost }

// testBindings is a fixed set of variable bindings.
type testBindings map[string]any

func (b testBindings) ResolveName(name string) (any, bool) {
	v, found := b[name]
	return v, found
}

func TestFunctionType(t *testing.T) {
	if !IsFunctionType(lessThanType) {
		t.Error("IsFunctionType() got false, wanted true")
	}
	if IsFunctionType(NewOpaqueType("function")) {
		t.Error("IsFunctionType() got true for a parameterless opaque type, wanted false")
	}
	if IsFunctionType(NewOpaqueType("optional_type", IntType)) || IsFunctionType(nil) {
		t.Error("IsFunctionType() got true, wanted false")
	}
	if lessThanType.Kind() != OpaqueKind {
		t.Errorf("Kind() got %v, wanted opaque", lessThanType.Kind())
	}
	if !lessThanType.HasTrait(traits.InvokerType) {
		t.Error("HasTrait(InvokerType) got false, wanted true")
	}
	if got := FunctionResultType(lessThanType); got != BoolType {
		t.Errorf("FunctionResultType() got %v, wanted bool", got)
	}
	if got := FunctionArgTypes(lessThanType); len(got) != 2 || got[0] != IntType || got[1] != IntType {
		t.Errorf("FunctionArgTypes() got %v, wanted [int, int]", got)
	}
	if FunctionResultType(StringType) != nil || FunctionArgTypes(StringType) != nil {
		t.Error("function accessors returned a value for a non-function type")
	}
	if got := lessThanType.String(); got != "(int, int) -> bool" {
		t.Errorf("String() got %s, wanted (int, int) -> bool", got)
	}
	// Function types with differing signatures must not be exact or equivalent matches.
	if lessThanType.IsExactType(NewFunctionType(lessThanEstimate, BoolType, IntType)) {
		t.Error("IsExactType() got true for differing arities, wanted false")
	}
	if !lessThanType.IsExactType(NewFunctionType(lessThanEstimate, BoolType, IntType, IntType)) {
		t.Error("IsExactType() got false for identical signatures, wanted true")
	}
	if !lessThanType.IsAssignableRuntimeType(lessThanFn) {
		t.Error("IsAssignableRuntimeType() got false, wanted true")
	}
}

func TestFunctionInvoke(t *testing.T) {
	if got := lessThanFn.Invoke(nil, Int(1), Int(2)); got != True {
		t.Errorf("Invoke(1, 2) got %v, wanted true", got)
	}
	if got := lessThanFn.Invoke(nil, Int(2), Int(1)); got != False {
		t.Errorf("Invoke(2, 1) got %v, wanted false", got)
	}
	if got := lessThanFn.Arity(); got != 2 {
		t.Errorf("Arity() got %d, wanted 2", got)
	}
	if got := lessThanFn.Name(); got != "lessThan" {
		t.Errorf("Name() got %s, wanted lessThan", got)
	}
	if got := lessThanFn.Signature(); got != lessThanType {
		t.Errorf("Signature() got %v, wanted %v", got, lessThanType)
	}
}

func TestFunctionInvokeArityMismatch(t *testing.T) {
	got := lessThanFn.Invoke(nil, Int(1))
	if !IsError(got) || !strings.Contains(got.(*Err).String(), "expects 2 arguments, got 1") {
		t.Errorf("Invoke(1) got %v, wanted an arity error", got)
	}
}

func TestFunctionInvokeStrict(t *testing.T) {
	wantErr := NewErr("no such key")
	if got := lessThanFn.Invoke(nil, wantErr, Int(1)); got != wantErr {
		t.Errorf("Invoke() got %v, wanted the error argument", got)
	}
	unk := NewUnknown(1, nil)
	if got := lessThanFn.Invoke(nil, Int(1), unk); !IsUnknown(got) {
		t.Errorf("Invoke() got %v, wanted an unknown", got)
	}
	unbound := NewFunctionVal("unbound",
		NewFunctionType(lessThanEstimate, BoolType), lessThanEstimate, nil)
	if got := unbound.Invoke(nil); !IsError(got) {
		t.Errorf("Invoke() got %v, wanted an error for a function without an implementation", got)
	}
}

func TestFunctionCallEstimate(t *testing.T) {
	if got := lessThanFn.CallEstimate(); got != lessThanEstimate {
		t.Errorf("CallEstimate() got %v, wanted %v", got, lessThanEstimate)
	}
	if got := lessThanFn.CallCost(); got != 2 {
		t.Errorf("CallCost() got %d, wanted 2", got)
	}
	if got := FunctionCallEstimate(lessThanType); got != lessThanEstimate {
		t.Errorf("FunctionCallEstimate() got %v, wanted %v", got, lessThanEstimate)
	}
	// A type which is not a function type, and a function type built without an estimate, both
	// report an unknown estimate.
	if got := FunctionCallEstimate(StringType); !got.IsUnknown() {
		t.Errorf("FunctionCallEstimate(string) got %v, wanted unknown", got)
	}
	opaque := NewOpaqueType(FunctionTypeName, BoolType, IntType)
	if got := FunctionCallEstimate(opaque); !got.IsUnknown() {
		t.Errorf("FunctionCallEstimate() got %v, wanted unknown", got)
	}
	// Functions which do not declare a cost are charged the baseline call cost.
	undeclared := NewFunctionVal("undeclared", opaque, common.UnknownCallEstimate(),
		func(frame functions.ExecutionFrame, args ...ref.Val) ref.Val { return True })
	if got := undeclared.CallCost(); got != DefaultCallCost {
		t.Errorf("CallCost() got %d, wanted %d", got, DefaultCallCost)
	}
	// The declared estimate is not part of type identity.
	if !lessThanType.IsExactType(NewFunctionType(common.FixedCallEstimate(100), BoolType, IntType, IntType)) {
		t.Error("IsExactType() got false for types which differ only by cost, wanted true")
	}
}

func TestFunctionInvokeChargesCost(t *testing.T) {
	frame := newTestFrame(nil, nil)
	if got := lessThanFn.Invoke(frame, Int(1), Int(2)); got != True {
		t.Errorf("Invoke() got %v, wanted true", got)
	}
	if frame.Cost() != 2 {
		t.Errorf("frame cost got %d, wanted 2", frame.Cost())
	}
	if got := lessThanFn.Invoke(frame, Int(1), Int(2)); got != True {
		t.Errorf("Invoke() got %v, wanted true", got)
	}
	if frame.Cost() != 4 {
		t.Errorf("frame cost got %d, wanted 4", frame.Cost())
	}
	// Arguments which are not evaluated do not incur a cost.
	if got := lessThanFn.Invoke(frame, NewErr("boom")); !IsError(got) {
		t.Errorf("Invoke() got %v, wanted error", got)
	}
	if frame.Cost() != 4 {
		t.Errorf("frame cost got %d, wanted 4", frame.Cost())
	}
}

func TestFunctionInvokeCostLimit(t *testing.T) {
	limit := uint64(3)
	frame := newTestFrame(nil, &limit)
	if got := lessThanFn.Invoke(frame, Int(1), Int(2)); got != True {
		t.Errorf("Invoke() got %v, wanted true", got)
	}
	got := lessThanFn.Invoke(frame, Int(1), Int(2))
	if !IsError(got) || !strings.Contains(got.(*Err).String(), "cost limit exceeded") {
		t.Errorf("Invoke() got %v, wanted a cost limit error", got)
	}
}

func TestFunctionInvokeRebindsFrame(t *testing.T) {
	// The caller's variables are visible on the frame it holds.
	caller := newTestFrame(testBindings{"secret": Int(42)}, nil)
	if _, found := caller.ResolveName("secret"); !found {
		t.Fatal("ResolveName() got false for the caller's own frame, wanted true")
	}
	// They are not visible on the frame the implementation is called with, and neither is
	// rebinding able to recover them.
	var called bool
	fn := NewFunctionVal("peek", NewFunctionType(lessThanEstimate, BoolType, IntType),
		lessThanEstimate,
		func(frame functions.ExecutionFrame, args ...ref.Val) ref.Val {
			called = true
			if _, found := frame.ResolveName("secret"); found {
				t.Error("ResolveName() got true within a call, wanted false")
			}
			rebound := frame.WithBindings(testBindings{"arg": args[0]})
			if _, found := rebound.ResolveName("secret"); found {
				t.Error("ResolveName() got true after rebinding, wanted false")
			}
			if v, found := rebound.ResolveName("arg"); !found || v != Int(1) {
				t.Errorf("ResolveName() got (%v, %t), wanted the rebound argument", v, found)
			}
			// Cost is charged against the same budget across both frames.
			if err := rebound.ChargeCost(5); err != nil {
				t.Errorf("ChargeCost() failed: %v", err)
			}
			return True
		})
	if got := fn.Invoke(caller, Int(1)); got != True {
		t.Errorf("Invoke() got %v, wanted true", got)
	}
	if !called {
		t.Error("Invoke() did not call the implementation")
	}
	// 2 for the declared cost of the call, 5 charged from within it.
	if caller.Cost() != 7 {
		t.Errorf("frame cost got %d, wanted 7", caller.Cost())
	}
}

func TestFunctionConvertToType(t *testing.T) {
	if got := lessThanFn.ConvertToType(TypeType); got != lessThanType {
		t.Errorf("ConvertToType(type) got %v, wanted %v", got, lessThanType)
	}
	if got := lessThanFn.ConvertToType(StringType); got != String("lessThan(int, int) -> bool") {
		t.Errorf("ConvertToType(string) got %v, wanted the function signature", got)
	}
	if got := lessThanFn.ConvertToType(IntType); !IsError(got) {
		t.Errorf("ConvertToType(int) got %v, wanted error", got)
	}
	if got := lessThanFn.Type(); got != lessThanType {
		t.Errorf("Type() got %v, wanted %v", got, lessThanType)
	}
	if lessThanFn.Value() == nil {
		t.Error("Value() got nil, wanted the function implementation")
	}
}

func TestFunctionConvertToNative(t *testing.T) {
	val, err := lessThanFn.ConvertToNative(reflect.TypeOf(lessThanFn))
	if err != nil {
		t.Fatalf("ConvertToNative() failed: %v", err)
	}
	if val != lessThanFn {
		t.Errorf("ConvertToNative() got %v, wanted %v", val, lessThanFn)
	}
	invoker, err := lessThanFn.ConvertToNative(reflect.TypeOf((*traits.Invoker)(nil)).Elem())
	if err != nil {
		t.Fatalf("ConvertToNative() failed: %v", err)
	}
	if got := invoker.(traits.Invoker).Invoke(nil, Int(1), Int(2)); got != True {
		t.Errorf("Invoke() got %v, wanted true", got)
	}
	fn, err := lessThanFn.ConvertToNative(reflect.TypeOf(functions.FrameOp(nil)))
	if err != nil {
		t.Fatalf("ConvertToNative() failed: %v", err)
	}
	if got := fn.(functions.FrameOp)(nil, Int(1), Int(2)); got != True {
		t.Errorf("Invoke() got %v, wanted true", got)
	}
	if _, err := lessThanFn.ConvertToNative(reflect.TypeOf("")); err == nil {
		t.Error("ConvertToNative(string) got nil error, wanted error")
	}
}

func TestFunctionEqual(t *testing.T) {
	if lessThanFn.Equal(lessThanFn) != True {
		t.Error("Equal() got false for the same function value, wanted true")
	}
	other := NewFunctionVal("lessThan", lessThanType, lessThanEstimate, lessThanFn.impl)
	if lessThanFn.Equal(other) != False {
		t.Error("Equal() got true for distinct function values, wanted false")
	}
	if lessThanFn.Equal(String("lessThan")) != False {
		t.Error("Equal() got true for a non-function value, wanted false")
	}
}
