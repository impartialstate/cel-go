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

// Package cost describes how CEL expressions are costed, both at compile time where costs are
// estimated from the AST and the types of its operands, and at runtime where costs are tracked
// from the values which flow through the evaluation.
//
// Both halves share a single vocabulary:
//
//   - [Estimate] is an inclusive range of sizes or costs. At runtime the range collapses to a
//     single value.
//   - [Model] declares how the cost of one function overload relates to the sizes of its
//     operands. A model is written once and used to both estimate and track the function.
//   - [Operands] is the receiver-first view of a call's arguments. The estimator populates it
//     from [ast.Expr] and [types.Type] values, the tracker from [ref.Val] values, which means a
//     [Model] never needs to know which half of the system is asking.
package cost

import (
	"math"
)

const (
	// SelectAndIdentCost is the cost of an operation that accesses an identifier or performs a
	// select.
	SelectAndIdentCost = 1

	// ConstCost is the cost of an operation that accesses a constant.
	ConstCost = 0

	// CallCost is the baseline cost of dispatching a function call.
	CallCost = 1

	// ListCreateBaseCost is the base cost of any operation that creates a new list.
	ListCreateBaseCost = 10

	// MapCreateBaseCost is the base cost of any operation that creates a new map.
	MapCreateBaseCost = 30

	// StructCreateBaseCost is the base cost of any operation that creates a new struct.
	StructCreateBaseCost = 40

	// StringTraversalCostFactor is multiplied to a length of a string when computing the cost of
	// traversing the entire string once.
	StringTraversalCostFactor = 0.1

	// RegexStringLengthCostFactor is multiplied to the length of a regex string pattern when
	// computing the cost of applying the regex to a string of unit cost.
	RegexStringLengthCostFactor = 0.25
)

// Estimate is an inclusive [Min, Max] range which describes either a size or a cost.
//
// Sizes and costs share a representation because they are freely converted into one another:
// the cost of allocating a value is a function of its size, and the size of a result is a
// function of the sizes of the inputs which produced it.
type Estimate struct {
	Min, Max uint64
}

// Fixed returns an Estimate whose minimum and maximum are the same value.
func Fixed(size uint64) Estimate {
	return Estimate{Min: size, Max: size}
}

// Ranged returns an Estimate bounded by the min and max values.
func Ranged(min, max uint64) Estimate {
	return Estimate{Min: min, Max: max}
}

// Unknown returns an Estimate which spans the entire range of possible values.
func Unknown() Estimate {
	return Estimate{Min: 0, Max: math.MaxUint64}
}

// IsUnknown returns true if the estimate spans the entire range of possible values.
func (e Estimate) IsUnknown() bool {
	return e.Min == 0 && e.Max == math.MaxUint64
}

// IsFixed returns true if the estimate describes exactly one value.
func (e Estimate) IsFixed() bool {
	return e.Min == e.Max
}

// Add returns the sum of two estimates, saturating at math.MaxUint64.
func (e Estimate) Add(other Estimate) Estimate {
	return Estimate{Min: Add(e.Min, other.Min), Max: Add(e.Max, other.Max)}
}

// Offset returns the estimate shifted by a fixed amount, saturating at math.MaxUint64.
func (e Estimate) Offset(delta uint64) Estimate {
	return Estimate{Min: Add(e.Min, delta), Max: Add(e.Max, delta)}
}

// Multiply returns the product of two estimates, saturating at math.MaxUint64.
func (e Estimate) Multiply(other Estimate) Estimate {
	return Estimate{Min: Multiply(e.Min, other.Min), Max: Multiply(e.Max, other.Max)}
}

// Scale multiplies the estimate by a cost factor, rounding each bound up to the nearest integer.
func (e Estimate) Scale(costPerUnit float64) Estimate {
	return Estimate{Min: Scale(e.Min, costPerUnit), Max: Scale(e.Max, costPerUnit)}
}

// Union returns the smallest Estimate which encompasses both inputs.
func (e Estimate) Union(other Estimate) Estimate {
	result := e
	if other.Min < result.Min {
		result.Min = other.Min
	}
	if other.Max > result.Max {
		result.Max = other.Max
	}
	return result
}

// Intersect returns the element-wise minimum of two estimates, which describes the amount of
// work performed by operations that halt as soon as the shorter of two inputs is exhausted.
func (e Estimate) Intersect(other Estimate) Estimate {
	result := e
	if other.Min < result.Min {
		result.Min = other.Min
	}
	if other.Max < result.Max {
		result.Max = other.Max
	}
	return result
}

// AtLeast raises both bounds of the estimate to the given floor.
func (e Estimate) AtLeast(min uint64) Estimate {
	if e.Min < min {
		e.Min = min
	}
	if e.Max < min {
		e.Max = min
	}
	return e
}

// UpTo returns an estimate with the same maximum, but no lower bound.
func (e Estimate) UpTo() Estimate {
	e.Min = 0
	return e
}

// Add returns the sum of two non-negative integers, saturating at math.MaxUint64.
func Add(x, y uint64) uint64 {
	if y > 0 && x > math.MaxUint64-y {
		return math.MaxUint64
	}
	return x + y
}

// Multiply returns the product of two non-negative integers, saturating at math.MaxUint64.
func Multiply(x, y uint64) uint64 {
	if y != 0 && x > math.MaxUint64/y {
		return math.MaxUint64
	}
	return x * y
}

// Scale multiplies an integer by a cost factor and rounds the result up, saturating at
// math.MaxUint64.
func Scale(x uint64, costPerUnit float64) uint64 {
	xFloat := float64(x)
	if xFloat > 0 && costPerUnit > 0 && xFloat > math.MaxUint64/costPerUnit {
		return math.MaxUint64
	}
	ceil := math.Ceil(xFloat * costPerUnit)
	if ceil >= doubleTwoTo64 {
		return math.MaxUint64
	}
	return uint64(ceil)
}

var doubleTwoTo64 = math.Ldexp(1.0, 64)
