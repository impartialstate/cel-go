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

package cost

import (
	"math"

	"github.com/google/cel-go/common"
	"github.com/google/cel-go/common/overloads"
	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
	"github.com/google/cel-go/common/types/traits"
)

// WARNING: Any changes to cost calculations in this file require a corresponding change to the
// estimated cost calculations in estimate.go.

// ActualCostEstimator provides function call cost estimations at runtime
// CallCost returns an estimated cost for the function overload invocation with the given args, or nil if it has no
// estimate to provide. CEL attempts to provide reasonable estimates for its standard function library, so CallCost
// should typically not need to provide an estimate for CELs standard function.
type ActualCostEstimator interface {
	CallCost(function, overloadID string, args []ref.Val, result ref.Val) *uint64
}

// CostTrackerOption configures the behavior of CostTracker objects.
type CostTrackerOption func(*CostTracker) error

// CostTrackerLimit sets the runtime limit on the evaluation cost during execution and will terminate the expression
// evaluation if the limit is exceeded.
func CostTrackerLimit(limit uint64) CostTrackerOption {
	return func(tracker *CostTracker) error {
		tracker.Limit = &limit
		return nil
	}
}

// TrackPresenceTest determines whether presence testing has a cost of one or zero.
// Defaults to presence test has a cost of one.
func TrackPresenceTest(hasCost bool) CostTrackerOption {
	return func(tracker *CostTracker) error {
		tracker.presenceTestHasCost = hasCost
		return nil
	}
}

// NewCostTracker creates a new CostTracker with a given estimator and a set of functional CostTrackerOption values.
func NewCostTracker(estimator ActualCostEstimator, opts ...CostTrackerOption) (*CostTracker, error) {
	tracker := &CostTracker{
		Estimator:           estimator,
		overloadTrackers:    map[string]FunctionTracker{},
		presenceTestHasCost: true,
	}
	for _, opt := range opts {
		err := opt(tracker)
		if err != nil {
			return nil, err
		}
	}
	return tracker, nil
}

// OverloadCostTracker binds an overload ID to a runtime FunctionTracker implementation.
//
// OverloadCostTracker instances augment or override ActualCostEstimator decisions, allowing for  versioned and/or
// optional cost tracking changes.
func OverloadCostTracker(overloadID string, fnTracker FunctionTracker) CostTrackerOption {
	return func(tracker *CostTracker) error {
		tracker.overloadTrackers[overloadID] = fnTracker
		return nil
	}
}

// FunctionTracker computes the actual cost of evaluating the functions with the given arguments and result.
type FunctionTracker func(args []ref.Val, result ref.Val) *uint64

// CostTracker represents the information needed for tracking runtime cost.
type CostTracker struct {
	Estimator           ActualCostEstimator
	overloadTrackers    map[string]FunctionTracker
	Limit               *uint64
	presenceTestHasCost bool

	cost  uint64
	stack refValStack
}

// ActualCost returns the runtime cost
func (c *CostTracker) ActualCost() uint64 {
	return c.cost
}

// Add charges the evaluation for work which was performed.
func (c *CostTracker) Add(delta uint64) {
	c.cost += delta
}

// PresenceTestHasCost returns whether a presence test is charged for.
func (c *CostTracker) PresenceTestHasCost() bool {
	return c.presenceTestHasCost
}

// LimitExceeded returns true when the accumulated cost has passed the configured limit.
func (c *CostTracker) LimitExceeded() bool {
	return c.Limit != nil && c.cost > *c.Limit
}

// Push records the value produced by an expression.
func (c *CostTracker) Push(val ref.Val, id int64) {
	c.stack.push(val, id)
}

// Drop removes the given expressions, and everything recorded after them, from the stack.
func (c *CostTracker) Drop(ids ...int64) {
	c.stack.drop(ids...)
}

// DropArgs removes the given expressions from the stack and returns the values they produced,
// reporting false if any of them are absent.
func (c *CostTracker) DropArgs(ids []int64) ([]ref.Val, bool) {
	return c.stack.dropArgs(ids)
}

// TrackCall charges the evaluation for a function call.
func (c *CostTracker) TrackCall(function, overloadID string, args []ref.Val, result ref.Val) {
	c.cost += c.costCall(function, overloadID, args, result)
}

func (c *CostTracker) costCall(function, overloadID string, args []ref.Val, result ref.Val) uint64 {
	var cost uint64
	if len(c.overloadTrackers) != 0 {
		if tracker, found := c.overloadTrackers[overloadID]; found {
			callCost := tracker(args, result)
			if callCost != nil {
				cost += *callCost
				return cost
			}
		}
	}
	if c.Estimator != nil {
		callCost := c.Estimator.CallCost(function, overloadID, args, result)
		if callCost != nil {
			cost += *callCost
			return cost
		}
	}
	// if user didn't specify, the default way of calculating runtime cost would be used.
	// if user has their own implementation of ActualCostEstimator, make sure to cover the mapping between overloadId and cost calculation
	switch overloadID {
	// O(n) functions
	case overloads.StartsWithString, overloads.EndsWithString, overloads.StringToBytes, overloads.BytesToString, overloads.ExtQuoteString, overloads.ExtFormatString:
		cost += uint64(math.Ceil(float64(actualSize(args[0])) * common.StringTraversalCostFactor))
	case overloads.InList:
		// If a list is composed entirely of constant values this is O(1), but we don't account for that here.
		// We just assume all list containment checks are O(n).
		cost += actualSize(args[1])
	// O(min(m, n)) functions
	case overloads.LessString, overloads.GreaterString, overloads.LessEqualsString, overloads.GreaterEqualsString,
		overloads.LessBytes, overloads.GreaterBytes, overloads.LessEqualsBytes, overloads.GreaterEqualsBytes,
		overloads.Equals, overloads.NotEquals:
		// When we check the equality of 2 scalar values (e.g. 2 integers, 2 floating-point numbers, 2 booleans etc.),
		// the CostTracker.ActualSize() function by definition returns 1 for each operand, resulting in an overall cost
		// of 1.
		lhsSize := actualSize(args[0])
		rhsSize := actualSize(args[1])
		minSize := lhsSize
		if rhsSize < minSize {
			minSize = rhsSize
		}
		cost += uint64(math.Ceil(float64(minSize) * common.StringTraversalCostFactor))
	// O(m+n) functions
	case overloads.AddString, overloads.AddBytes:
		// In the worst case scenario, we would need to reallocate a new backing store and copy both operands over.
		cost += uint64(math.Ceil(float64(actualSize(args[0])+actualSize(args[1])) * common.StringTraversalCostFactor))
	// O(nm) functions
	case overloads.MatchesString:
		// https://swtch.com/~rsc/regexp/regexp1.html applies to RE2 implementation supported by CEL
		// Add one to string length for purposes of cost calculation to prevent product of string and regex to be 0
		// in case where string is empty but regex is still expensive.
		strCost := uint64(math.Ceil((1.0 + float64(actualSize(args[0]))) * common.StringTraversalCostFactor))
		// We don't know how many expressions are in the regex, just the string length (a huge
		// improvement here would be to somehow get a count the number of expressions in the regex or
		// how many states are in the regex state machine and use that to measure regex cost).
		// For now, we're making a guess that each expression in a regex is typically at least 4 chars
		// in length.
		regexCost := uint64(math.Ceil(float64(actualSize(args[1])) * common.RegexStringLengthCostFactor))
		cost += strCost * regexCost
	case overloads.ContainsString:
		strCost := uint64(math.Ceil(float64(actualSize(args[0])) * common.StringTraversalCostFactor))
		substrCost := uint64(math.Ceil(float64(actualSize(args[1])) * common.StringTraversalCostFactor))
		cost += strCost * substrCost

	default:
		// The following operations are assumed to have O(1) complexity.
		// - AddList due to the implementation. Index lookup can be O(c) the
		//    number of concatenated lists, but we don't track that is cost calculations.
		// - Conversions, since none perform a traversal of a type of unbound length.
		// - Computing the size of strings, byte sequences, lists and maps.
		// - Logical operations and all operators on fixed width scalars (comparisons, equality)
		// - Any functions that don't have a declared cost either here or in provided ActualCostEstimator.
		cost++

	}
	return cost
}

// ActualSize returns the size of the value for all traits.Sizer values, a fixed size for all
// proto-based objects, and a size of 1 for all other value types.
func ActualSize(value ref.Val) uint64 {
	return actualSize(value)
}

// actualSize returns the size of the value for all traits.Sizer values, a fixed size for all proto-based
// objects, and a size of 1 for all other value types.
func actualSize(value ref.Val) uint64 {
	if sz, ok := value.(traits.Sizer); ok {
		return uint64(sz.Size().(types.Int))
	}
	if opt, ok := value.(*types.Optional); ok && opt.HasValue() {
		return actualSize(opt.GetValue())
	}
	return 1
}

type stackVal struct {
	Val ref.Val
	ID  int64
}

// refValStack keeps track of values of the stack for cost calculation purposes
type refValStack []stackVal

func (s *refValStack) push(val ref.Val, id int64) {
	value := stackVal{Val: val, ID: id}
	*s = append(*s, value)
}

// TODO: Allowing drop and dropArgs to remove stack items above the IDs they are provided is a workaround. drop and dropArgs
// should find and remove only the stack items matching the provided IDs once all attributes are properly pushed and popped from stack.

// drop searches the stack for each ID and removes the ID and all stack items above it.
// If none of the IDs are found, the stack is not modified.
// WARNING: It is possible for multiple expressions with the same ID to exist (due to how macros are implemented) so it's
// possible that a dropped ID will remain on the stack.  They should be removed when IDs on the stack are popped.
func (s *refValStack) drop(ids ...int64) {
	for _, id := range ids {
		for idx := len(*s) - 1; idx >= 0; idx-- {
			if (*s)[idx].ID == id {
				*s = (*s)[:idx]
				break
			}
		}
	}
}

// dropArgs searches the stack for all the args by their IDs, accumulates their associated ref.Vals and drops any
// stack items above any of the arg IDs. If any of the IDs are not found the stack, false is returned.
// Args are assumed to be found in the stack in reverse order, i.e. the last arg is expected to be found highest in
// the stack.
// WARNING: It is possible for multiple expressions with the same ID to exist (due to how macros are implemented) so it's
// possible that a dropped ID will remain on the stack.  They should be removed when IDs on the stack are popped.
func (s *refValStack) dropArgs(args []int64) ([]ref.Val, bool) {
	result := make([]ref.Val, len(args))
argloop:
	for nIdx := len(args) - 1; nIdx >= 0; nIdx-- {
		for idx := len(*s) - 1; idx >= 0; idx-- {
			if (*s)[idx].ID == args[nIdx] {
				el := (*s)[idx]
				*s = (*s)[:idx]
				result[nIdx] = el.Val
				continue argloop
			}
		}
		return nil, false
	}
	return result, true
}
