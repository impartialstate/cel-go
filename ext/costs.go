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

package ext

import (
	"github.com/google/cel-go/common/cost"
)

// Cost models for the extension libraries.
//
// Every extension overload whose cost depends on the size of its inputs is described by a single
// cost.Model. The model is registered with both the compile-time estimator and the runtime
// tracker, which is what keeps an extension from reporting one cost when an expression is
// checked and a different cost when the same expression is evaluated.
//
// Operands are receiver-first, so `s.replace(old, new)` and `regex.replace(s, old, new)` address
// their target as operand 0 and their arguments from 1 onward.

// scanString describes a call which walks its target string once and allocates a result whose
// size is described by the result function.
func scanString(result cost.SizeFn) cost.Model {
	return cost.Model{
		Base:         cost.CallCost,
		Traversed:    cost.Operand(0).Scale(cost.StringTraversalCostFactor),
		Result:       result,
		ChargeResult: true,
	}
}

// searchString describes a call which scans its target once for every character of the value it
// is searching for, and which produces a scalar.
func searchString(target, needle int) cost.Model {
	return cost.Model{
		Base:      cost.CallCost,
		Traversed: cost.Product(cost.Operand(target), cost.Operand(needle)).Scale(cost.StringTraversalCostFactor),
	}
}

// matchRegex describes the search performed by applying a regex pattern to a target string.
//
// Both sizes are offset by one so that an empty target, or an empty pattern, cannot reduce the
// cost of the search to zero. The two cost factors are applied together rather than one at a
// time so that a short target and a short pattern are not each rounded up to a full unit.
func matchRegex(target, pattern int) cost.SizeFn {
	return cost.Product(
		cost.Operand(target).Offset(1),
		cost.Operand(pattern).Offset(1),
	).Scale(cost.StringTraversalCostFactor * cost.RegexStringLengthCostFactor)
}

// buildList describes a call which allocates a new list, where the cost of traversal and the
// size of the resulting list are one and the same.
func buildList(size cost.SizeFn) cost.Model {
	return cost.Model{
		Base:      cost.CallCost,
		Alloc:     cost.ListCreateBaseCost,
		Traversed: size,
		Result:    size,
	}
}

// compareElements describes a call which compares every element of a list operand against every
// other element, such as a sort or a duplicate check. Comparing variable-width elements costs
// more per comparison than comparing scalars.
func compareElements(operand int, result cost.SizeFn) cost.Model {
	return cost.Model{
		Base:      cost.CallCost,
		Alloc:     cost.ListCreateBaseCost,
		Traversed: cost.Square(cost.Operand(operand)).ScaleBy(cost.ElementFactor(operand, 2.0)),
		Result:    result,
	}
}
