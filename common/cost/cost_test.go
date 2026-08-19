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
	"math"
	"testing"

	"github.com/google/cel-go/common/ast"
	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
)

// testContext is an EstimationContext over a fixed set of expressions, which lets a cost model
// be exercised without running the checker.
type testContext struct {
	sizes    map[int64]Estimate
	types    map[int64]*types.Type
	elemSize map[int64]Estimate
	elemType map[int64]*types.Type
	paths    map[int64][]string
}

func (c *testContext) Type(e ast.Expr) *types.Type { return c.types[e.ID()] }

func (c *testContext) Path(e ast.Expr) []string { return c.paths[e.ID()] }

func (c *testContext) Size(e ast.Expr) Estimate {
	if size, found := c.sizes[e.ID()]; found {
		return size
	}
	return Unknown()
}

func (c *testContext) SizeOf(n Node) Estimate {
	if n.Expr != nil {
		return c.Size(n.Expr)
	}
	return Unknown()
}

func (c *testContext) ElementType(e ast.Expr) *types.Type { return c.elemType[e.ID()] }

func (c *testContext) ElementSize(e ast.Expr) Estimate {
	if size, found := c.elemSize[e.ID()]; found {
		return size
	}
	return Unknown()
}

func (c *testContext) Constant(e ast.Expr) (ref.Val, bool) {
	if e == nil || e.Kind() != ast.LiteralKind {
		return nil, false
	}
	return e.AsLiteral(), true
}

func TestEstimateArithmetic(t *testing.T) {
	tests := []struct {
		name string
		got  Estimate
		want Estimate
	}{
		{"add", Fixed(2).Add(Ranged(1, 3)), Ranged(3, 5)},
		{"add saturates", Fixed(math.MaxUint64).Add(Fixed(1)), Fixed(math.MaxUint64)},
		{"multiply", Ranged(2, 3).Multiply(Fixed(4)), Ranged(8, 12)},
		{"multiply saturates", Fixed(math.MaxUint64).Multiply(Fixed(2)), Fixed(math.MaxUint64)},
		{"scale rounds up", Fixed(11).Scale(0.1), Fixed(2)},
		{"scale saturates", Fixed(math.MaxUint64).Scale(2), Fixed(math.MaxUint64)},
		{"union", Ranged(2, 4).Union(Ranged(3, 9)), Ranged(2, 9)},
		{"intersect", Ranged(2, 4).Intersect(Ranged(3, 9)), Ranged(2, 4)},
		{"at least", Ranged(0, 2).AtLeast(1), Ranged(1, 2)},
		{"up to", Ranged(2, 4).UpTo(), Ranged(0, 4)},
		{"offset", Ranged(2, 4).Offset(1), Ranged(3, 5)},
	}
	for _, tst := range tests {
		tc := tst
		t.Run(tc.name, func(t *testing.T) {
			if tc.got != tc.want {
				t.Errorf("got %v, wanted %v", tc.got, tc.want)
			}
		})
	}
}

func TestAggregateSize(t *testing.T) {
	tests := []struct {
		name string
		val  ref.Val
		want uint64
	}{
		{"string counts code points", types.String("héllo"), 5},
		{"bytes count octets", types.Bytes("hello"), 5},
		{"list counts entries", types.DefaultTypeAdapter.NativeToValue([]int64{1, 2, 3}), 3},
		{"scalars have unit size", types.Int(42), 1},
		{"optional reports its value", types.OptionalOf(types.String("hello")), 5},
		{"empty optional has unit size", types.OptionalNone, 1},
		{"missing value has unit size", nil, 1},
	}
	for _, tst := range tests {
		tc := tst
		t.Run(tc.name, func(t *testing.T) {
			if got := AggregateSize(tc.val); got != tc.want {
				t.Errorf("AggregateSize() got %d, wanted %d", got, tc.want)
			}
		})
	}
}

// scanModel describes an O(n) traversal of its target which allocates a result the same size as
// the value it started with.
var scanModel = Model{
	Base:         CallCost,
	Traversed:    Operand(0).Scale(StringTraversalCostFactor),
	Result:       Operand(0),
	ChargeResult: true,
}

func TestModelEstimateAndTrackAgree(t *testing.T) {
	// A model evaluated over exact sizes must produce the same cost as the same model evaluated
	// over an estimate which knows those exact sizes.
	target := types.String("hello world")
	fac := ast.NewExprFactory()
	expr := fac.NewIdent(1, "target")
	ctx := &testContext{
		sizes: map[int64]Estimate{1: Fixed(11)},
		types: map[int64]*types.Type{1: types.StringType},
	}
	est := scanModel.Estimate(ctx, []ast.Expr{expr})
	if est.Cost != Fixed(14) {
		t.Errorf("Model.Estimate() got cost %v, wanted %v", est.Cost, Fixed(14))
	}
	if est.ResultSize == nil || *est.ResultSize != Fixed(11) {
		t.Errorf("Model.Estimate() got result size %v, wanted %v", est.ResultSize, Fixed(11))
	}
	tracked := scanModel.Track([]ref.Val{target}, target)
	if *tracked != est.Cost.Max {
		t.Errorf("Model.Track() got %d, wanted %d", *tracked, est.Cost.Max)
	}
}

func TestModelUnknownOperandSize(t *testing.T) {
	fac := ast.NewExprFactory()
	ctx := &testContext{types: map[int64]*types.Type{1: types.StringType}}
	est := scanModel.Estimate(ctx, []ast.Expr{fac.NewIdent(1, "target")})
	if !est.Cost.IsUnknown() && est.Cost.Max != math.MaxUint64 {
		t.Errorf("Model.Estimate() got %v, wanted an unbounded cost", est.Cost)
	}
	if est.Cost.Min != 1 {
		t.Errorf("Model.Estimate() got min cost %d, wanted the base call cost", est.Cost.Min)
	}
}

func TestSizeFns(t *testing.T) {
	ops := ValOperands([]ref.Val{types.String("hello"), types.Int(1), types.Int(4)})
	tests := []struct {
		name string
		fn   SizeFn
		want Estimate
	}{
		{"operand size", Operand(0), Fixed(5)},
		{"integer operand value", IntValue(2, 0), Fixed(4)},
		{"missing integer operand default", IntValue(9, 7), Fixed(7)},
		{"span between two operands", Span(0, 1, 2), Fixed(3)},
		{"span clamps at zero", Span(0, 2, 1), Fixed(0)},
		{"product", Product(Operand(0), Operand(0)), Fixed(25)},
		{"sum", Sum(Operand(0), Const(3)), Fixed(8)},
		{"square", Square(Operand(0)), Fixed(25)},
		{"smallest", Smallest(Operand(0), Const(2)), Fixed(2)},
		{"between", Between(Const(1), Operand(0)), Ranged(1, 5)},
		{"divide rounds down", Operand(0).Div(4), Fixed(1)},
		{"scale rounds up", Operand(0).Scale(0.25), Fixed(2)},
	}
	for _, tst := range tests {
		tc := tst
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.fn(ops); got != tc.want {
				t.Errorf("SizeFn() got %v, wanted %v", got, tc.want)
			}
		})
	}
}

func TestOperandElementType(t *testing.T) {
	list := types.DefaultTypeAdapter.NativeToValue([]string{"a", "b"})
	if got := ValOperands([]ref.Val{list}).ElemType(0); got != types.StringType {
		t.Errorf("ElemType() got %v, wanted string", got)
	}
	empty := types.DefaultTypeAdapter.NativeToValue([]string{})
	if got := ValOperands([]ref.Val{empty}).ElemType(0); got != nil {
		t.Errorf("ElemType() got %v, wanted nil for an empty list", got)
	}
	fac := ast.NewExprFactory()
	expr := fac.NewIdent(1, "x")
	ctx := &testContext{elemType: map[int64]*types.Type{1: types.IntType}}
	if got := ExprOperands(ctx, []ast.Expr{expr}).ElemType(0); got != types.IntType {
		t.Errorf("ElemType() got %v, wanted int", got)
	}
}

func TestHints(t *testing.T) {
	hints := NewHints(
		SizeHint("user.emails", 10),
		ExactSizeHint("user.id", 36),
		TypeSizeHint(types.BytesType, 1024),
	)
	tests := []struct {
		name string
		node Node
		want *Estimate
	}{
		{"path hint", Node{Path: []string{"user", "emails"}, Type: types.NewListType(types.StringType)}, sizePtr(Ranged(0, 10))},
		{"exact path hint", Node{Path: []string{"user", "id"}, Type: types.StringType}, sizePtr(Fixed(36))},
		{"type hint", Node{Path: []string{"user", "avatar"}, Type: types.BytesType}, sizePtr(Ranged(0, 1024))},
		{"path wins over type", Node{Path: []string{"user", "id"}, Type: types.BytesType}, sizePtr(Fixed(36))},
		{"no hint", Node{Path: []string{"user", "name"}, Type: types.StringType}, nil},
	}
	for _, tst := range tests {
		tc := tst
		t.Run(tc.name, func(t *testing.T) {
			got := hints.EstimateSize(nil, tc.node)
			if (got == nil) != (tc.want == nil) || (got != nil && *got != *tc.want) {
				t.Errorf("EstimateSize() got %v, wanted %v", got, tc.want)
			}
		})
	}
}

func TestTrackerCallCostPrecedence(t *testing.T) {
	one := uint64(1)
	overloadCost := uint64(99)
	estimatorCost := uint64(50)
	args := []ref.Val{types.String("hello world")}

	tracker, err := NewTracker(callTracker(estimatorCost),
		OverloadTracker("custom_overload", func([]ref.Val, ref.Val) *uint64 { return &overloadCost }))
	if err != nil {
		t.Fatalf("NewTracker() failed: %v", err)
	}
	if got := tracker.CallCost("custom", "custom_overload", args, types.True); got != overloadCost {
		t.Errorf("CallCost() got %d, wanted the registered overload cost %d", got, overloadCost)
	}
	if got := tracker.CallCost("other", "other_overload", args, types.True); got != estimatorCost {
		t.Errorf("CallCost() got %d, wanted the estimator cost %d", got, estimatorCost)
	}

	// With no estimator, the standard model for the overload applies, and unknown overloads fall
	// back to the O(1) call cost.
	tracker, err = NewTracker(nil)
	if err != nil {
		t.Fatalf("NewTracker() failed: %v", err)
	}
	if got := tracker.CallCost("size", "size_string", args, types.Int(11)); got != one {
		t.Errorf("CallCost() got %d, wanted %d", got, one)
	}
	if got := tracker.CallCost("contains", "contains_string", []ref.Val{types.String("hello world"), types.String("lo w")}, types.True); got != 2 {
		t.Errorf("CallCost() got %d, wanted 2", got)
	}
}

func TestTrackerLimit(t *testing.T) {
	tracker, err := NewTracker(nil, Limit(10))
	if err != nil {
		t.Fatalf("NewTracker() failed: %v", err)
	}
	tracker.Add(10)
	if tracker.LimitExceeded() {
		t.Error("LimitExceeded() returned true at the limit, wanted false")
	}
	tracker.Add(1)
	if !tracker.LimitExceeded() {
		t.Error("LimitExceeded() returned false past the limit, wanted true")
	}
	if tracker.ActualCost() != 11 {
		t.Errorf("ActualCost() got %d, wanted 11", tracker.ActualCost())
	}
}

func TestTrackerPresenceTest(t *testing.T) {
	tracker, err := NewTracker(nil)
	if err != nil {
		t.Fatalf("NewTracker() failed: %v", err)
	}
	tracker.TrackPresenceTest()
	if tracker.ActualCost() != SelectAndIdentCost {
		t.Errorf("ActualCost() got %d, wanted %d", tracker.ActualCost(), SelectAndIdentCost)
	}
	free, err := NewTracker(nil, TrackPresenceTest(false))
	if err != nil {
		t.Fatalf("NewTracker() failed: %v", err)
	}
	free.TrackPresenceTest()
	if free.ActualCost() != 0 {
		t.Errorf("ActualCost() got %d, wanted 0", free.ActualCost())
	}
}

func sizePtr(e Estimate) *Estimate {
	return &e
}

type callTracker uint64

func (c callTracker) CallCost(function, overloadID string, args []ref.Val, result ref.Val) *uint64 {
	cost := uint64(c)
	return &cost
}
