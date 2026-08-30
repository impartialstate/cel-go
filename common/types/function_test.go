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
	"reflect"
	"strings"
	"testing"

	"github.com/google/cel-go/common/types/ref"
	"github.com/google/cel-go/common/types/traits"
)

var (
	lessThanType = NewFunctionType(BoolType, IntType, IntType)
	lessThanFn   = NewFunctionVal("lessThan", lessThanType,
		func(args ...ref.Val) ref.Val {
			return Bool(args[0].(Int) < args[1].(Int))
		})
)

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
	if lessThanType.IsExactType(NewFunctionType(BoolType, IntType)) {
		t.Error("IsExactType() got true for differing arities, wanted false")
	}
	if !lessThanType.IsExactType(NewFunctionType(BoolType, IntType, IntType)) {
		t.Error("IsExactType() got false for identical signatures, wanted true")
	}
	if !lessThanType.IsAssignableRuntimeType(lessThanFn) {
		t.Error("IsAssignableRuntimeType() got false, wanted true")
	}
}

func TestFunctionInvoke(t *testing.T) {
	if got := lessThanFn.Invoke(Int(1), Int(2)); got != True {
		t.Errorf("Invoke(1, 2) got %v, wanted true", got)
	}
	if got := lessThanFn.Invoke(Int(2), Int(1)); got != False {
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
	got := lessThanFn.Invoke(Int(1))
	if !IsError(got) || !strings.Contains(got.(*Err).String(), "expects 2 arguments, got 1") {
		t.Errorf("Invoke(1) got %v, wanted an arity error", got)
	}
}

func TestFunctionInvokeStrict(t *testing.T) {
	wantErr := NewErr("no such key")
	if got := lessThanFn.Invoke(wantErr, Int(1)); got != wantErr {
		t.Errorf("Invoke() got %v, wanted the error argument", got)
	}
	unk := NewUnknown(1, nil)
	if got := lessThanFn.Invoke(Int(1), unk); !IsUnknown(got) {
		t.Errorf("Invoke() got %v, wanted an unknown", got)
	}
	unbound := NewFunctionVal("unbound", NewFunctionType(BoolType), nil)
	if got := unbound.Invoke(); !IsError(got) {
		t.Errorf("Invoke() got %v, wanted an error for a function without an implementation", got)
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
	if got := invoker.(traits.Invoker).Invoke(Int(1), Int(2)); got != True {
		t.Errorf("Invoke() got %v, wanted true", got)
	}
	fn, err := lessThanFn.ConvertToNative(reflect.TypeOf(func(...ref.Val) ref.Val { return nil }))
	if err != nil {
		t.Fatalf("ConvertToNative() failed: %v", err)
	}
	if got := fn.(func(...ref.Val) ref.Val)(Int(1), Int(2)); got != True {
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
	other := NewFunctionVal("lessThan", lessThanType, lessThanFn.impl)
	if lessThanFn.Equal(other) != False {
		t.Error("Equal() got true for distinct function values, wanted false")
	}
	if lessThanFn.Equal(String("lessThan")) != False {
		t.Error("Equal() got true for a non-function value, wanted false")
	}
}
