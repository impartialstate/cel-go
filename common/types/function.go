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

	"github.com/google/cel-go/common/types/ref"
	"github.com/google/cel-go/common/types/traits"
)

// FunctionTypeName is the runtime type name shared by all function types and values.
const FunctionTypeName = "function"

var (
	// funcValueType is the reflected type of the *Function value.
	funcValueType = reflect.TypeOf(&Function{})

	// funcImplType is the reflected type of a function implementation.
	funcImplType = reflect.TypeOf(func(...ref.Val) ref.Val { return nil })
)

// NewFunctionType creates a function type whose first type parameter is the result type of the
// function and whose remaining type parameters describe the function's arguments, in order.
//
// Function types describe values which may be passed to, and returned from, other functions:
//
//	// A comparator over two values of type T which reports whether the first sorts first.
//	types.NewFunctionType(types.BoolType, types.NewTypeParamType("T"), types.NewTypeParamType("T"))
func NewFunctionType(resultType *Type, argTypes ...*Type) *Type {
	params := make([]*Type, 0, len(argTypes)+1)
	params = append(params, resultType)
	params = append(params, argTypes...)
	return NewOpaqueType(FunctionTypeName, params...)
}

// IsFunctionType returns whether the input type is a function type.
func IsFunctionType(t *Type) bool {
	return t != nil &&
		t.Kind() == OpaqueKind &&
		t.TypeName() == FunctionTypeName &&
		len(t.Parameters()) >= 1
}

// FunctionResultType returns the result type of a function type, or nil if the input is not a
// function type.
func FunctionResultType(t *Type) *Type {
	if !IsFunctionType(t) {
		return nil
	}
	return t.Parameters()[0]
}

// FunctionArgTypes returns the argument types of a function type in declaration order, or nil if
// the input is not a function type.
func FunctionArgTypes(t *Type) []*Type {
	if !IsFunctionType(t) {
		return nil
	}
	return t.Parameters()[1:]
}

// Function is a first-class function value which may be bound to a variable, passed as an
// argument to another function, and invoked either from CEL or from a function implementation.
//
// Function values are produced by referencing a declared function by name within an expression,
// or by binding a variable declared with a function type to a Function instance at evaluation
// time.
type Function struct {
	name   string
	fnType *Type
	impl   func(args ...ref.Val) ref.Val
}

var (
	_ ref.Val        = &Function{}
	_ traits.Invoker = &Function{}
	_ fmt.Stringer   = &Function{}
)

// NewFunctionVal creates a function value with the given name, function type, and implementation.
//
// The name is used for error reporting and has no bearing on how the value is invoked. The
// fnType must be a function type as produced by NewFunctionType, and its argument count
// determines the arity accepted by Invoke.
func NewFunctionVal(name string, fnType *Type, impl func(args ...ref.Val) ref.Val) *Function {
	return &Function{name: name, fnType: fnType, impl: impl}
}

// Name returns the name of the function, which is either the declared function name or the name
// of the variable which supplied the value.
func (f *Function) Name() string {
	return f.name
}

// Signature returns the function type which describes the result and argument types.
func (f *Function) Signature() *Type {
	return f.fnType
}

// Arity returns the number of arguments accepted by the function.
func (f *Function) Arity() int {
	return len(FunctionArgTypes(f.fnType))
}

// Invoke implements the traits.Invoker interface method.
//
// Invocation is strict: error and unknown arguments are returned to the caller without calling
// the underlying implementation.
func (f *Function) Invoke(args ...ref.Val) ref.Val {
	if len(args) != f.Arity() {
		return NewErr("no such overload: %s() expects %d arguments, got %d",
			f.name, f.Arity(), len(args))
	}
	var unk *Unknown
	for _, arg := range args {
		if IsError(arg) {
			return arg
		}
		if IsUnknown(arg) {
			unk = MergeUnknowns(arg.(*Unknown), unk)
		}
	}
	if unk != nil {
		return unk
	}
	if f.impl == nil {
		return NewErr("no such overload: %s()", f.name)
	}
	return f.impl(args...)
}

// ConvertToNative implements the ref.Val interface method.
func (f *Function) ConvertToNative(typeDesc reflect.Type) (any, error) {
	switch typeDesc {
	case funcValueType:
		return f, nil
	case funcImplType:
		return f.Invoke, nil
	}
	if typeDesc.Kind() == reflect.Interface {
		if reflect.TypeOf(f).Implements(typeDesc) {
			return f, nil
		}
	}
	return nil, fmt.Errorf("type conversion error from '%s' to '%v'", f.fnType, typeDesc)
}

// ConvertToType implements the ref.Val interface method.
func (f *Function) ConvertToType(typeVal ref.Type) ref.Val {
	switch typeVal {
	case TypeType:
		return f.fnType
	case StringType:
		return String(f.String())
	}
	return NewErr("type conversion error from '%s' to '%s'", f.fnType, typeVal)
}

// Equal implements the ref.Val interface method.
//
// Function values do not have a meaningful structural equality, so equality is reference
// equality: a function value is only equal to itself.
func (f *Function) Equal(other ref.Val) ref.Val {
	otherFn, ok := other.(*Function)
	if !ok {
		return False
	}
	return Bool(f == otherFn)
}

// Type implements the ref.Val interface method.
func (f *Function) Type() ref.Type {
	return f.fnType
}

// Value implements the ref.Val interface method.
func (f *Function) Value() any {
	return f.impl
}

// String implements the fmt.Stringer interface, rendering the function signature.
func (f *Function) String() string {
	argTypes := FunctionArgTypes(f.fnType)
	args := make([]string, len(argTypes))
	for i, at := range argTypes {
		args[i] = at.String()
	}
	return fmt.Sprintf("%s(%s) -> %s",
		f.name, strings.Join(args, ", "), FunctionResultType(f.fnType))
}
