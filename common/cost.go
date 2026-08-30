// Copyright 2022 Google LLC
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

package common

import "math"

const (
	// SelectAndIdentCost is the cost of an operation that accesses an identifier or performs a select.
	SelectAndIdentCost = 1

	// ConstCost is the cost of an operation that accesses a constant.
	ConstCost = 0

	// ListCreateBaseCost is the base cost of any operation that creates a new list.
	ListCreateBaseCost = 10

	// MapCreateBaseCost is the base cost of any operation that creates a new map.
	MapCreateBaseCost = 30

	// StructCreateBaseCost is the base cost of any operation that creates a new struct.
	StructCreateBaseCost = 40

	// StringTraversalCostFactor is multiplied to a length of a string when computing the cost of traversing the entire
	// string once.
	StringTraversalCostFactor = 0.1

	// RegexStringLengthCostFactor is multiplied ot the length of a regex string pattern when computing the cost of
	// applying the regex to a string of unit cost.
	RegexStringLengthCostFactor = 0.25
)

// CallEstimate includes a CostEstimate for the call, and an optional estimate of the result object size.
// The ResultSize should only be provided if the call results in a map, list, string or bytes.
type CallEstimate struct {
	CostEstimate

	ResultSize *SizeEstimate
}

// UnknownCallEstimate returns a call estimate with an unknown cost and an unknown result size.
func UnknownCallEstimate() CallEstimate {
	return CallEstimate{CostEstimate: UnknownCostEstimate()}
}

// FixedCallEstimate returns a call estimate with a fixed cost and no result size estimate.
//
// The cost excludes the cost of evaluating the arguments to the call.
func FixedCallEstimate(cost uint64) CallEstimate {
	return CallEstimate{CostEstimate: FixedCostEstimate(cost)}
}

// FixedCallEstimateWithSize returns a call estimate with a fixed cost and a fixed result size.
//
// The result size should only be declared for calls which produce a string, bytes, list, or map.
func FixedCallEstimateWithSize(cost, resultSize uint64) CallEstimate {
	size := FixedSizeEstimate(resultSize)
	return CallEstimate{CostEstimate: FixedCostEstimate(cost), ResultSize: &size}
}

// IsUnknown returns whether the cost of the call is unknown.
func (ce CallEstimate) IsUnknown() bool {
	return ce.CostEstimate == unknownCostEstimate
}

// SizeEstimate represents an estimated size of a variable length string, bytes, map or list.
type SizeEstimate struct {
	Min, Max uint64
}

// UnknownSizeEstimate returns a size between 0 and max uint
func UnknownSizeEstimate() SizeEstimate {
	return unknownSizeEstimate
}

// FixedSizeEstimate returns a size estimate with a fixed min and max range.
func FixedSizeEstimate(size uint64) SizeEstimate {
	return SizeEstimate{Min: size, Max: size}
}

// Add adds to another SizeEstimate and returns the sum.
// If add would result in an uint64 overflow, the result is math.MaxUint64.
func (se SizeEstimate) Add(sizeEstimate SizeEstimate) SizeEstimate {
	return SizeEstimate{
		addUint64NoOverflow(se.Min, sizeEstimate.Min),
		addUint64NoOverflow(se.Max, sizeEstimate.Max),
	}
}

// Multiply multiplies by another SizeEstimate and returns the product.
// If multiply would result in an uint64 overflow, the result is math.MaxUint64.
func (se SizeEstimate) Multiply(sizeEstimate SizeEstimate) SizeEstimate {
	return SizeEstimate{
		multiplyUint64NoOverflow(se.Min, sizeEstimate.Min),
		multiplyUint64NoOverflow(se.Max, sizeEstimate.Max),
	}
}

// MultiplyByCostFactor multiplies a SizeEstimate by a cost factor and returns the CostEstimate with the
// nearest integer of the result, rounded up.
func (se SizeEstimate) MultiplyByCostFactor(costPerUnit float64) CostEstimate {
	return CostEstimate{
		multiplyByCostFactor(se.Min, costPerUnit),
		multiplyByCostFactor(se.Max, costPerUnit),
	}
}

// MultiplyByCost multiplies by the cost and returns the product.
// If multiply would result in an uint64 overflow, the result is math.MaxUint64.
func (se SizeEstimate) MultiplyByCost(cost CostEstimate) CostEstimate {
	return CostEstimate{
		multiplyUint64NoOverflow(se.Min, cost.Min),
		multiplyUint64NoOverflow(se.Max, cost.Max),
	}
}

// Union returns a SizeEstimate that encompasses both input the SizeEstimate.
func (se SizeEstimate) Union(size SizeEstimate) SizeEstimate {
	result := se
	if size.Min < result.Min {
		result.Min = size.Min
	}
	if size.Max > result.Max {
		result.Max = size.Max
	}
	return result
}

// AsCost converts a size estimates to an equivalent cost estimate.
func (se SizeEstimate) AsCost() CostEstimate {
	return se.MultiplyByCostFactor(1)
}

// CostEstimate represents an estimated cost range and provides add and multiply operations
// that do not overflow.
type CostEstimate struct {
	Min, Max uint64
}

// UnknownCostEstimate returns a cost with an unknown impact.
func UnknownCostEstimate() CostEstimate {
	return unknownCostEstimate
}

// FixedCostEstimate returns a cost with a fixed min and max range.
func FixedCostEstimate(cost uint64) CostEstimate {
	return CostEstimate{Min: cost, Max: cost}
}

// Add adds the costs and returns the sum.
// If add would result in an uint64 overflow for the min or max, the value is set to math.MaxUint64.
func (ce CostEstimate) Add(cost CostEstimate) CostEstimate {
	return CostEstimate{
		Min: addUint64NoOverflow(ce.Min, cost.Min),
		Max: addUint64NoOverflow(ce.Max, cost.Max),
	}
}

// Multiply multiplies by the cost and returns the product.
// If multiply would result in an uint64 overflow, the result is math.MaxUint64.
func (ce CostEstimate) Multiply(cost CostEstimate) CostEstimate {
	return CostEstimate{
		Min: multiplyUint64NoOverflow(ce.Min, cost.Min),
		Max: multiplyUint64NoOverflow(ce.Max, cost.Max),
	}
}

// MultiplyByCostFactor multiplies a CostEstimate by a cost factor and returns the CostEstimate with the
// nearest integer of the result, rounded up.
func (ce CostEstimate) MultiplyByCostFactor(costPerUnit float64) CostEstimate {
	return CostEstimate{
		Min: multiplyByCostFactor(ce.Min, costPerUnit),
		Max: multiplyByCostFactor(ce.Max, costPerUnit),
	}
}

// Union returns a CostEstimate that encompasses both input the CostEstimates.
func (ce CostEstimate) Union(size CostEstimate) CostEstimate {
	result := ce
	if size.Min < result.Min {
		result.Min = size.Min
	}
	if size.Max > result.Max {
		result.Max = size.Max
	}
	return result
}

// addUint64NoOverflow adds non-negative ints. If the result is exceeds math.MaxUint64, math.MaxUint64
// is returned.
func addUint64NoOverflow(x, y uint64) uint64 {
	if y > 0 && x > math.MaxUint64-y {
		return math.MaxUint64
	}
	return x + y
}

// multiplyUint64NoOverflow multiplies non-negative ints. If the result is exceeds math.MaxUint64, math.MaxUint64
// is returned.
func multiplyUint64NoOverflow(x, y uint64) uint64 {
	if y != 0 && x > math.MaxUint64/y {
		return math.MaxUint64
	}
	return x * y
}

// multiplyByFactor multiplies an integer by a cost factor float and returns the nearest integer value, rounded up.
func multiplyByCostFactor(x uint64, y float64) uint64 {
	xFloat := float64(x)
	if xFloat > 0 && y > 0 && xFloat > math.MaxUint64/y {
		return math.MaxUint64
	}
	ceil := math.Ceil(xFloat * y)
	if ceil >= doubleTwoTo64 {
		return math.MaxUint64
	}
	return uint64(ceil)
}

var (
	doubleTwoTo64 = math.Ldexp(1.0, 64)

	unknownSizeEstimate = SizeEstimate{Min: 0, Max: math.MaxUint64}
	unknownCostEstimate = unknownSizeEstimate.MultiplyByCostFactor(1)
)
