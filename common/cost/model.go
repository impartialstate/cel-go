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

package cost

import (
	"github.com/google/cel-go/common/ast"
	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
	"github.com/google/cel-go/common/types/traits"
)

// Operands is the receiver-first view of the values passed to a function call.
//
// For a member call such as `s.replace(old, new)` operand 0 is `s`. For a global call such as
// `regex.replace(s, old, new)` operand 0 is `s`. Overload ids are unique, so a [Model] always
// knows what each position holds without needing to distinguish the two call styles.
//
// The estimator implements Operands over the AST, where sizes are ranges and values are only
// available for literals. The tracker implements it over the evaluated values, where sizes are
// exact and every value is available.
type Operands interface {
	// Len returns the number of operands.
	Len() int

	// Size returns the size of the operand at the given index, or an unknown estimate when the
	// size cannot be determined.
	Size(i int) Estimate

	// Value returns the operand value when it is knowable, which is true of literals at
	// estimation time and of every operand at tracking time.
	Value(i int) (ref.Val, bool)

	// Type returns the type of the operand, or nil when the type is unknown.
	Type(i int) *types.Type

	// ElemType returns the element type of a list or map operand, or nil when it is unknown.
	ElemType(i int) *types.Type
}

// SizeFn computes a size from the operands of a call.
//
// SizeFn values compose, which is what allows the cost of most functions to be declared in a
// single expression, e.g. `Product(Operand(0), Operand(1)).Scale(StringTraversalCostFactor)`.
type SizeFn func(ops Operands) Estimate

// Operand returns the size of the operand at the given index.
func Operand(i int) SizeFn {
	return func(ops Operands) Estimate { return ops.Size(i) }
}

// Const returns a size which does not depend on the operands.
func Const(size uint64) SizeFn {
	return func(Operands) Estimate { return Fixed(size) }
}

// IntValue returns the value of an integer operand as a size, e.g. the depth argument to
// `flatten` or the count argument to `lists.range`.
//
// The default is used when the operand is not an integer literal, which is the common case at
// estimation time.
func IntValue(i int, defaultSize uint64) SizeFn {
	return func(ops Operands) Estimate {
		val, found := ops.Value(i)
		if !found {
			return Fixed(defaultSize)
		}
		iv, ok := val.(types.Int)
		if !ok {
			return Fixed(defaultSize)
		}
		if iv < 0 {
			return Fixed(0)
		}
		return Fixed(uint64(iv))
	}
}

// Span returns the size of the range described by a start and end operand, defaulting the end
// to the size of the target operand, e.g. `s.substring(start, end)` or `l.slice(start, end)`.
func Span(target, start, end int) SizeFn {
	return func(ops Operands) Estimate {
		lo := IntValue(start, 0)(ops)
		hi := IntValue(end, ops.Size(target).Max)(ops)
		return Ranged(sub(hi.Min, lo.Max), sub(hi.Max, lo.Min))
	}
}

// sub subtracts two sizes, clamping the result at zero.
func sub(x, y uint64) uint64 {
	if y > x {
		return 0
	}
	return x - y
}

// Sum returns the sum of the sizes computed by the input functions.
func Sum(sizes ...SizeFn) SizeFn {
	return func(ops Operands) Estimate {
		var total Estimate
		for _, sz := range sizes {
			total = total.Add(sz(ops))
		}
		return total
	}
}

// Product returns the product of the sizes computed by the input functions.
func Product(sizes ...SizeFn) SizeFn {
	return func(ops Operands) Estimate {
		total := Fixed(1)
		for _, sz := range sizes {
			total = total.Multiply(sz(ops))
		}
		return total
	}
}

// Square returns the product of a size with itself, which describes the work performed by
// operations that compare every element of an input against every other element.
func Square(size SizeFn) SizeFn {
	return Product(size, size)
}

// Between returns a size whose lower bound comes from one function and whose upper bound comes
// from another, which describes results whose size varies with how the input is encoded.
func Between(lo, hi SizeFn) SizeFn {
	return func(ops Operands) Estimate {
		return Ranged(lo(ops).Min, hi(ops).Max)
	}
}

// Smallest returns the element-wise minimum of the input sizes, which describes operations that
// halt as soon as the shortest of their inputs is exhausted.
func Smallest(sizes ...SizeFn) SizeFn {
	return func(ops Operands) Estimate {
		var result Estimate
		for i, sz := range sizes {
			if i == 0 {
				result = sz(ops)
				continue
			}
			result = result.Intersect(sz(ops))
		}
		return result
	}
}

// Plus returns the sum of this size and another.
func (f SizeFn) Plus(other SizeFn) SizeFn {
	return Sum(f, other)
}

// Times returns the product of this size and another.
func (f SizeFn) Times(other SizeFn) SizeFn {
	return Product(f, other)
}

// Scale multiplies the size by a per-element cost factor, rounding up.
func (f SizeFn) Scale(costPerUnit float64) SizeFn {
	return func(ops Operands) Estimate { return f(ops).Scale(costPerUnit) }
}

// ScaleBy multiplies the size by a cost factor derived from the operands, rounding up.
func (f SizeFn) ScaleBy(factor func(ops Operands) float64) SizeFn {
	return func(ops Operands) Estimate { return f(ops).Scale(factor(ops)) }
}

// Div divides the size by a fixed divisor, rounding down.
func (f SizeFn) Div(divisor uint64) SizeFn {
	return func(ops Operands) Estimate {
		e := f(ops)
		return Ranged(e.Min/divisor, e.Max/divisor)
	}
}

// Offset increases the size by a fixed amount, which keeps the cost of scanning an empty input
// from collapsing to zero.
func (f SizeFn) Offset(delta uint64) SizeFn {
	return func(ops Operands) Estimate { return f(ops).Offset(delta) }
}

// AtLeast raises the size to a floor.
func (f SizeFn) AtLeast(size uint64) SizeFn {
	return func(ops Operands) Estimate { return f(ops).AtLeast(size) }
}

// UpTo removes the lower bound from the size, describing a result which may be empty.
func (f SizeFn) UpTo() SizeFn {
	return func(ops Operands) Estimate { return f(ops).UpTo() }
}

// ElementFactor returns a cost factor for operations over the elements of a list operand which
// accounts for the added expense of comparing variable-length elements.
func ElementFactor(i int, base float64) func(ops Operands) float64 {
	return func(ops Operands) float64 {
		switch ops.ElemType(i) {
		case types.StringType, types.BytesType:
			return base + StringTraversalCostFactor
		}
		return base
	}
}

// Model declares how the cost of a single function overload relates to the sizes of its
// operands and of the value it produces.
//
// The cost of a call is:
//
//	Base + Alloc + Traversed(operands) [+ size of the result]
//
// A model is declared once and drives both halves of the cost system: the estimator evaluates
// it over size ranges derived from the AST, and the tracker evaluates it over the exact sizes
// of the values which were passed to the call.
type Model struct {
	// Base is the fixed cost of dispatching the call, typically CallCost.
	Base uint64

	// Alloc is the fixed cost of a container allocated by the call, e.g. ListCreateBaseCost.
	Alloc uint64

	// Traversed reports how much data the call scans, scaled by any per-element cost factor.
	// A nil Traversed describes a call whose cost is independent of the size of its inputs.
	Traversed SizeFn

	// Result reports the size of the value produced by the call. A nil Result leaves the size
	// of the result unknown, which is the right answer for calls that produce a scalar.
	Result SizeFn

	// ChargeResult adds the size of the result to the cost of the call, accounting for the
	// allocation performed by calls which build a new string, bytes, list, or map value.
	ChargeResult bool
}

// ResultSize returns the estimated size of the value produced by the call, and whether the
// model was able to determine it.
func (m Model) ResultSize(ops Operands) (Estimate, bool) {
	if m.Result == nil {
		return Unknown(), false
	}
	return m.Result(ops), true
}

// Cost returns the cost of a call given its operands and the size of its result.
func (m Model) Cost(ops Operands, resultSize Estimate) Estimate {
	total := Fixed(Add(m.Base, m.Alloc))
	if m.Traversed != nil {
		total = total.Add(m.Traversed(ops))
	}
	if m.ChargeResult {
		total = total.Add(resultSize)
	}
	return total
}

// Estimate computes the compile-time cost of a call over the estimated sizes of its operands.
func (m Model) Estimate(ctx EstimationContext, args []ast.Expr) *CallEstimate {
	ops := ExprOperands(ctx, args)
	size, known := m.ResultSize(ops)
	est := &CallEstimate{Cost: m.Cost(ops, size)}
	if known {
		est.ResultSize = &size
	}
	return est
}

// Track computes the runtime cost of a call over the actual sizes of its operands and result.
func (m Model) Track(args []ref.Val, result ref.Val) *uint64 {
	total := m.Cost(ValOperands(args), Fixed(AggregateSize(result))).Max
	return &total
}

// Overload binds a cost Model to a function overload id so that the same declaration configures
// both the estimator and the tracker.
type Overload struct {
	ID    string
	Model Model
}

// Function binds a cost Model to a function overload id.
func Function(overloadID string, model Model) Overload {
	return Overload{ID: overloadID, Model: model}
}

// Estimators converts a set of overload cost models into compile-time estimator options.
func Estimators(overloads ...Overload) []EstimatorOption {
	opts := make([]EstimatorOption, len(overloads))
	for i, o := range overloads {
		opts[i] = OverloadEstimator(o.ID, o.Model.Estimate)
	}
	return opts
}

// Trackers converts a set of overload cost models into runtime tracker options.
func Trackers(overloads ...Overload) []TrackerOption {
	opts := make([]TrackerOption, len(overloads))
	for i, o := range overloads {
		opts[i] = OverloadTracker(o.ID, o.Model.Track)
	}
	return opts
}

// ExprOperands adapts the expressions passed to a call, and the context which describes them, to
// the Operands interface.
func ExprOperands(ctx EstimationContext, args []ast.Expr) Operands {
	return &exprOperands{ctx: ctx, args: args}
}

type exprOperands struct {
	ctx  EstimationContext
	args []ast.Expr
}

func (ops *exprOperands) Len() int {
	return len(ops.args)
}

func (ops *exprOperands) expr(i int) ast.Expr {
	if i < 0 || i >= len(ops.args) {
		return nil
	}
	return ops.args[i]
}

func (ops *exprOperands) Size(i int) Estimate {
	expr := ops.expr(i)
	if expr == nil {
		return Unknown()
	}
	return ops.ctx.Size(expr)
}

func (ops *exprOperands) Value(i int) (ref.Val, bool) {
	expr := ops.expr(i)
	if expr == nil {
		return nil, false
	}
	return ops.ctx.Constant(expr)
}

func (ops *exprOperands) Type(i int) *types.Type {
	expr := ops.expr(i)
	if expr == nil {
		return nil
	}
	return ops.ctx.Type(expr)
}

func (ops *exprOperands) ElemType(i int) *types.Type {
	expr := ops.expr(i)
	if expr == nil {
		return nil
	}
	return ops.ctx.ElementType(expr)
}

// ValOperands adapts the runtime view of a call's arguments to the Operands interface.
func ValOperands(args []ref.Val) Operands {
	return &valOperands{args: args}
}

type valOperands struct {
	args []ref.Val
}

func (ops *valOperands) Len() int {
	return len(ops.args)
}

func (ops *valOperands) Size(i int) Estimate {
	val, found := ops.Value(i)
	if !found {
		return Unknown()
	}
	return Fixed(AggregateSize(val))
}

func (ops *valOperands) Value(i int) (ref.Val, bool) {
	if i < 0 || i >= len(ops.args) || ops.args[i] == nil {
		return nil, false
	}
	return ops.args[i], true
}

func (ops *valOperands) Type(i int) *types.Type {
	val, found := ops.Value(i)
	if !found {
		return nil
	}
	t, ok := val.Type().(*types.Type)
	if !ok {
		return nil
	}
	return t
}

func (ops *valOperands) ElemType(i int) *types.Type {
	val, found := ops.Value(i)
	if !found {
		return nil
	}
	if lister, ok := val.(traits.Lister); ok {
		if lister.Size() == types.IntZero {
			return nil
		}
		if t, ok := lister.Get(types.IntZero).Type().(*types.Type); ok {
			return t
		}
		return nil
	}
	return elemType(ops.Type(i))
}

// elemType returns the value type parameter of a list or map type.
func elemType(t *types.Type) *types.Type {
	if t == nil {
		return nil
	}
	switch t.Kind() {
	case types.ListKind:
		return t.Parameters()[0]
	case types.MapKind:
		return t.Parameters()[1]
	}
	return nil
}

// AggregateSize returns the size of a value as reported by the CEL `size()` function: the number
// of unicode code points in a string, bytes in a byte sequence, or entries in a list or map.
//
// Values which have no aggregate size, such as scalars and messages, have a size of 1 so that
// cost formulas expressed as a product of operand sizes remain well defined. An optional value
// reports the size of the value it holds.
func AggregateSize(val ref.Val) uint64 {
	switch v := val.(type) {
	case nil:
		return 1
	case traits.Sizer:
		return uint64(v.Size().(types.Int))
	case *types.Optional:
		if v.HasValue() {
			return AggregateSize(v.GetValue())
		}
	}
	return 1
}
