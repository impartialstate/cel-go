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
	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
)

// CallTracker computes the actual cost of a function call from the values which were passed to
// it and the value it produced.
//
// CEL costs its own standard library, so a CallTracker only needs to answer for functions the
// application has added itself. Returning nil defers to the cost CEL would otherwise compute.
type CallTracker interface {
	// CallCost returns the cost of the call, or nil if the tracker has no cost to report.
	// Arguments are receiver-first for member functions.
	CallCost(function, overloadID string, args []ref.Val, result ref.Val) *uint64
}

// FunctionTracker computes the actual cost of a single function overload. Arguments are
// receiver-first for member functions, mirroring the operand order used by [FunctionEstimator].
type FunctionTracker func(args []ref.Val, result ref.Val) *uint64

// Comprehension is the runtime view of a comprehension, which lets a cost tracker charge for
// iteration itself rather than only for the work performed by each step.
type Comprehension interface {
	// IterRange returns the value being iterated over.
	IterRange() ref.Val

	// Iterations returns the number of iterations performed so far.
	Iterations() uint64

	// Accu returns the current value of the accumulator.
	Accu() ref.Val
}

// ComprehensionTracker computes the cost of a comprehension, or nil if it has no cost to report.
type ComprehensionTracker func(comp Comprehension) *uint64

// TrackerOption configures the behavior of a Tracker.
type TrackerOption func(*Tracker) error

// Limit sets the maximum cost of an evaluation. Evaluation is terminated once the limit is
// exceeded.
func Limit(limit uint64) TrackerOption {
	return func(t *Tracker) error {
		t.Limit = &limit
		return nil
	}
}

// TrackPresenceTest determines whether presence testing has a cost of one or zero.
//
// Defaults to presence test has a cost of one.
func TrackPresenceTest(hasCost bool) TrackerOption {
	return func(t *Tracker) error {
		t.presenceTestHasCost = hasCost
		return nil
	}
}

// OverloadTracker binds a FunctionTracker to a function overload id, overriding the cost CEL
// would otherwise compute for the overload.
func OverloadTracker(overloadID string, tracker FunctionTracker) TrackerOption {
	return func(t *Tracker) error {
		t.overloadTrackers[overloadID] = tracker
		return nil
	}
}

// TrackComprehensions registers a tracker which charges for each comprehension evaluated.
func TrackComprehensions(tracker ComprehensionTracker) TrackerOption {
	return func(t *Tracker) error {
		t.comprehensionTracker = tracker
		return nil
	}
}

// NewTracker creates a Tracker for a single evaluation.
func NewTracker(estimator CallTracker, opts ...TrackerOption) (*Tracker, error) {
	t := &Tracker{
		Estimator:           estimator,
		overloadTrackers:    map[string]FunctionTracker{},
		presenceTestHasCost: true,
	}
	for _, opt := range opts {
		if err := opt(t); err != nil {
			return nil, err
		}
	}
	return t, nil
}

// Tracker accumulates the cost of a single evaluation.
//
// A Tracker is created per evaluation and is not safe for concurrent use.
type Tracker struct {
	// Estimator reports the cost of calls which CEL does not cost itself.
	Estimator CallTracker

	// Limit is the maximum cost of the evaluation, if one was configured.
	Limit *uint64

	overloadTrackers     map[string]FunctionTracker
	comprehensionTracker ComprehensionTracker
	presenceTestHasCost  bool

	cost uint64
	// ops is reused across calls so that costing a call does not allocate.
	ops valOperands
	// operands holds the most recent value produced by each expression, indexed by expression
	// id, which is how a call recovers the arguments it was given once they have been evaluated.
	operands []ref.Val
}

// ActualCost returns the cost accumulated so far.
func (t *Tracker) ActualCost() uint64 {
	return t.cost
}

// Add charges the evaluation for work performed.
func (t *Tracker) Add(delta uint64) {
	t.cost = Add(t.cost, delta)
}

// PresenceTestHasCost returns whether a presence test is charged for, which lets an estimate be
// configured to match how the expression will actually be tracked.
func (t *Tracker) PresenceTestHasCost() bool {
	return t.presenceTestHasCost
}

// LimitExceeded returns true when the accumulated cost has passed the configured limit.
func (t *Tracker) LimitExceeded() bool {
	return t.Limit != nil && t.cost > *t.Limit
}

// RecordOperand retains the value produced by an expression so that a call which consumes it can
// be costed once it completes.
//
// Expression ids are assigned sequentially, so the values are held in a slice indexed by id
// rather than in a map, which keeps the bookkeeping off the hot path of an evaluation.
func (t *Tracker) RecordOperand(id int64, val ref.Val) {
	if id < 0 {
		return
	}
	if int(id) >= len(t.operands) {
		grown := make([]ref.Val, roundUpPowerOfTwo(int(id)+1))
		copy(grown, t.operands)
		t.operands = grown
	}
	t.operands[id] = val
}

// Operand returns the most recent value produced by an expression, and whether one was found.
func (t *Tracker) Operand(id int64) (ref.Val, bool) {
	if id < 0 || int(id) >= len(t.operands) {
		return nil, false
	}
	val := t.operands[id]
	return val, val != nil
}

// roundUpPowerOfTwo returns the smallest power of two which is at least n, with a floor which
// covers the expression size of a typical program.
func roundUpPowerOfTwo(n int) int {
	size := 32
	for size < n {
		size *= 2
	}
	return size
}

// TrackIdent charges for resolving an identifier or a field selection.
func (t *Tracker) TrackIdent() {
	t.Add(SelectAndIdentCost)
}

// TrackQualifier charges for applying a qualifier to a value.
func (t *Tracker) TrackQualifier() {
	t.Add(1)
}

// TrackPresenceTest charges for a `has()` macro, which may be configured to be free.
func (t *Tracker) TrackPresenceTest() {
	if t.presenceTestHasCost {
		t.Add(SelectAndIdentCost)
	}
}

// TrackConstruct charges for allocating a list, map, or struct value.
func (t *Tracker) TrackConstruct(typ ref.Type) {
	switch typ {
	case types.ListType:
		t.Add(ListCreateBaseCost)
	case types.MapType:
		t.Add(MapCreateBaseCost)
	default:
		t.Add(StructCreateBaseCost)
	}
}

// TrackComprehension charges for a comprehension, which by default is free: the work a
// comprehension performs is charged to the steps evaluated within it.
func (t *Tracker) TrackComprehension(comp Comprehension) {
	if t.comprehensionTracker == nil {
		return
	}
	if c := t.comprehensionTracker(comp); c != nil {
		t.Add(*c)
	}
}

// TrackCall charges for a function call. Arguments are receiver-first for member functions.
func (t *Tracker) TrackCall(function, overloadID string, args []ref.Val, result ref.Val) {
	t.Add(t.CallCost(function, overloadID, args, result))
}

// CallCost returns the cost of a function call without charging for it.
//
// Costs are resolved in order of specificity: an overload tracker registered for the exact
// overload, then the tracker supplied by the application, then the standard cost model for the
// overload, and finally the O(1) default.
func (t *Tracker) CallCost(function, overloadID string, args []ref.Val, result ref.Val) uint64 {
	if tracker, found := t.overloadTrackers[overloadID]; found {
		if c := tracker(args, result); c != nil {
			return *c
		}
	}
	if t.Estimator != nil {
		if c := t.Estimator.CallCost(function, overloadID, args, result); c != nil {
			return *c
		}
	}
	if model, found := StandardModel(overloadID); found {
		t.ops.args = args
		return model.Cost(&t.ops, Fixed(AggregateSize(result))).Max
	}
	return CallCost
}
