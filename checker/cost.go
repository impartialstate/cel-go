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

package checker

import (
	"github.com/google/cel-go/common/ast"
	"github.com/google/cel-go/common/cost"
)

// Cost estimation is implemented by the common/cost package, which holds the runtime cost
// tracking as well. The declarations below keep the estimation entry points available from the
// package they have always been reached through.

// CostEstimator estimates the sizes of variable length input data and the costs of functions.
type CostEstimator = cost.CostEstimator

// AstNode represents an AST node for the purpose of cost estimations.
type AstNode = cost.AstNode

// CallEstimate includes a CostEstimate for the call, and an optional estimate of the result
// object size.
type CallEstimate = cost.CallEstimate

// SizeEstimate represents an estimated size of a variable length string, bytes, map or list.
type SizeEstimate = cost.SizeEstimate

// CostEstimate represents an estimated cost range and provides add and multiply operations that
// do not overflow.
type CostEstimate = cost.CostEstimate

// CostOption configures flags which affect cost computations.
type CostOption = cost.CostOption

// FunctionEstimator provides a CallEstimate given the target and arguments for a specific
// function, overload pair.
type FunctionEstimator = cost.FunctionEstimator

// UnknownSizeEstimate returns a size between 0 and max uint.
func UnknownSizeEstimate() SizeEstimate {
	return cost.UnknownSizeEstimate()
}

// UnknownCostEstimate returns a cost with an unknown impact.
func UnknownCostEstimate() CostEstimate {
	return cost.UnknownCostEstimate()
}

// FixedSizeEstimate returns a size estimate with a fixed min and max range.
func FixedSizeEstimate(size uint64) SizeEstimate {
	return cost.FixedSizeEstimate(size)
}

// FixedCostEstimate returns a cost with a fixed min and max range.
func FixedCostEstimate(c uint64) CostEstimate {
	return cost.FixedCostEstimate(c)
}

// PresenceTestHasCost determines whether presence testing has a cost of one or zero.
//
// Defaults to presence test has a cost of one.
func PresenceTestHasCost(hasCost bool) CostOption {
	return cost.PresenceTestHasCost(hasCost)
}

// OverloadCostEstimate binds a FunctionCoster to a specific function overload ID.
//
// When a OverloadCostEstimate is provided, it will override the cost calculation of the
// CostEstimator provided to the Cost() call.
func OverloadCostEstimate(overloadID string, functionCoster FunctionEstimator) CostOption {
	return cost.OverloadCostEstimate(overloadID, functionCoster)
}

// Cost estimates the cost of the parsed and type checked CEL expression.
func Cost(checked *ast.AST, estimator CostEstimator, opts ...CostOption) (CostEstimate, error) {
	return cost.Cost(checked, estimator, opts...)
}
