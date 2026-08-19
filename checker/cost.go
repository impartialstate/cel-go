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

// Cost estimation is implemented by the common/cost package, which is shared with the runtime
// cost tracking in the interpreter so that the two cannot drift apart. The aliases below keep
// the estimation entry points available from their historical home.

// CostEstimator estimates the sizes of variable length input data and the costs of functions.
type CostEstimator = cost.Estimator

// AstNode represents an AST node for the purpose of cost estimations.
type AstNode = cost.Node

// CallEstimate includes a cost estimate for a call, and an optional estimate of the result size.
type CallEstimate = cost.CallEstimate

// SizeEstimate represents an estimated size of a variable length string, bytes, map or list.
type SizeEstimate = cost.Estimate

// CostEstimate represents an estimated cost range.
type CostEstimate = cost.Estimate

// CostOption configures flags which affect cost computations.
type CostOption = cost.EstimatorOption

// FunctionEstimator provides a CallEstimate for the operands of a specific function overload.
type FunctionEstimator = cost.FunctionEstimator

// UnknownSizeEstimate returns a size between 0 and max uint.
func UnknownSizeEstimate() SizeEstimate {
	return cost.Unknown()
}

// UnknownCostEstimate returns a cost with an unknown impact.
func UnknownCostEstimate() CostEstimate {
	return cost.Unknown()
}

// FixedSizeEstimate returns a size estimate with a fixed min and max range.
func FixedSizeEstimate(size uint64) SizeEstimate {
	return cost.Fixed(size)
}

// FixedCostEstimate returns a cost with a fixed min and max range.
func FixedCostEstimate(c uint64) CostEstimate {
	return cost.Fixed(c)
}

// PresenceTestHasCost determines whether presence testing has a cost of one or zero.
//
// Defaults to presence test has a cost of one.
func PresenceTestHasCost(hasCost bool) CostOption {
	return cost.PresenceTestHasCost(hasCost)
}

// OverloadCostEstimate binds a FunctionEstimator to a specific function overload ID.
//
// When an OverloadCostEstimate is provided, it will override the cost calculation of the
// CostEstimator provided to the Cost() call.
func OverloadCostEstimate(overloadID string, functionCoster FunctionEstimator) CostOption {
	return cost.OverloadEstimator(overloadID, functionCoster)
}

// Cost estimates the cost of the parsed and type checked CEL expression.
func Cost(checked *ast.AST, estimator CostEstimator, opts ...CostOption) (CostEstimate, error) {
	return cost.EstimateCost(checked, estimator, opts...)
}
