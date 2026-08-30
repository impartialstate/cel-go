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

	"github.com/google/cel-go/common"
	"github.com/google/cel-go/common/functions"
	"github.com/google/cel-go/common/types/ref"
	"github.com/google/cel-go/common/types/traits"
)

// FunctionTypeName is the runtime type name shared by all function types and values.
const FunctionTypeName = "function"

// DefaultCallCost is the cost charged for a single invocation of a function value which does not
// declare its cost.
const DefaultCallCost = uint64(1)

var (
	// funcValueType is the reflected type of the *Function value.
	funcValueType = reflect.TypeOf(&Function{})

	// funcImplType is the reflected type of a function implementation.
	funcImplType = reflect.TypeOf(functions.FrameOp(nil))
)

// NewFunctionType creates a function type whose first type parameter is the result type of the
// function and whose remaining type parameters describe the function's arguments, in order.
//
// The estimate declares the cost of a single invocation of a value of this type and the size of
// its result, which is what allows calls made through a function value to be accounted for during
// cost estimation. Use common.UnknownCallEstimate for a type which describes a function whose cost
// is not known, e.g. the declared parameter type of a higher-order function.
//
//	// A comparator over two values of type T which costs one unit to invoke.
//	types.NewFunctionType(common.FixedCallEstimate(1),
//	    types.BoolType, types.NewTypeParamType("T"), types.NewTypeParamType("T"))
func NewFunctionType(estimate common.CallEstimate, resultType *Type, argTypes ...*Type) *Type {
	params := make([]*Type, 0, len(argTypes)+1)
	params = append(params, resultType)
	params = append(params, argTypes...)
	t := NewOpaqueType(FunctionTypeName, params...)
	t.callEstimate = &estimate
	return t
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

// FunctionCallEstimate returns the cost of invoking a value of the function type and the size of
// its result.
//
// An unknown estimate is returned for types which are not function types, and for function types
// which were created without one, e.g. by conversion from a serialized type.
func FunctionCallEstimate(t *Type) common.CallEstimate {
	if !IsFunctionType(t) || t.callEstimate == nil {
		return common.UnknownCallEstimate()
	}
	return *t.callEstimate
}

// Function is a first-class function value which may be bound to a variable, passed as an
// argument to another function, and invoked either from CEL or from a function implementation.
//
// Function values are produced by referencing a declared function by name within an expression,
// or by binding a variable declared with a function type to a Function instance at evaluation
// time.
type Function struct {
	name     string
	fnType   *Type
	estimate common.CallEstimate
	impl     functions.FrameOp
}

var (
	_ ref.Val        = &Function{}
	_ traits.Invoker = &Function{}
	_ fmt.Stringer   = &Function{}
)

// NewFunctionVal creates a function value with the given name, function type, call estimate, and
// implementation.
//
// The name is used for error reporting and has no bearing on how the value is invoked. The fnType
// must be a function type as produced by NewFunctionType, and its argument count determines the
// arity accepted by Invoke.
//
// The estimate declares the cost of a single invocation and the size of its result. The declared
// cost is charged against the evaluation's cost budget on each invocation, so a function whose
// work is proportional to its input should declare a cost which reflects that. Values which
// declare an unknown estimate are charged DefaultCallCost per invocation.
//
// The implementation receives the execution frame of the evaluation which invoked it as its first
// argument. The frame is nil when the value is invoked outside of an evaluation.
func NewFunctionVal(name string,
	fnType *Type, estimate common.CallEstimate, impl functions.FrameOp) *Function {
	return &Function{name: name, fnType: fnType, estimate: estimate, impl: impl}
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

// CallEstimate returns the declared cost of a single invocation and the size of its result.
func (f *Function) CallEstimate() common.CallEstimate {
	return f.estimate
}

// CallCost returns the cost charged for a single invocation of the function.
//
// The upper bound of the declared estimate is charged so that the cost accrued by an expression at
// runtime cannot exceed the cost estimated for it statically. Functions which do not declare an
// estimate are charged DefaultCallCost.
func (f *Function) CallCost() uint64 {
	if f.estimate.IsUnknown() {
		return DefaultCallCost
	}
	return f.estimate.Max
}

// Arity returns the number of arguments accepted by the function.
func (f *Function) Arity() int {
	return len(FunctionArgTypes(f.fnType))
}

// Invoke implements the traits.Invoker interface method.
//
// The frame is charged the function's declared cost before the implementation is called, and an
// error is returned if that exceeds the evaluation's cost limit. A nil frame skips cost
// accounting, which is appropriate when invoking a function value outside of an evaluation.
//
// Invocation is strict: error and unknown arguments are returned to the caller without calling
// the underlying implementation.
func (f *Function) Invoke(frame functions.ExecutionFrame, args ...ref.Val) ref.Val {
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
	if frame != nil {
		if err := frame.ChargeCost(f.CallCost()); err != nil {
			return WrapErr(err)
		}
	}
	return f.impl(frame, args...)
}

// ConvertToNative implements the ref.Val interface method.
func (f *Function) ConvertToNative(typeDesc reflect.Type) (any, error) {
	switch typeDesc {
	case funcValueType:
		return f, nil
	case funcImplType:
		return functions.FrameOp(f.Invoke), nil
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
