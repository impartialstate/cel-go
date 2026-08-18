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

package interpreter

import (
	"errors"

	"github.com/google/cel-go/common/cost"
	"github.com/google/cel-go/common/types/ref"
)

// Runtime cost tracking is implemented by the common/cost package, which is shared with the
// compile-time estimation in the checker so that the two cannot drift apart. This file is the
// glue which drives a cost.Tracker from the steps of an evaluation.

// ActualCostEstimator provides function call cost estimations at runtime.
//
// CEL provides cost estimates for its own standard library, so an ActualCostEstimator typically
// only needs to answer for functions the application has added itself.
type ActualCostEstimator = cost.CallTracker

// CostTracker represents the information needed for tracking runtime cost.
type CostTracker = cost.Tracker

// CostTrackerOption configures the behavior of CostTracker objects.
type CostTrackerOption = cost.TrackerOption

// FunctionTracker computes the actual cost of evaluating a function with the given arguments and
// result. Arguments are receiver-first for member functions.
type FunctionTracker = cost.FunctionTracker

// NewCostTracker creates a new CostTracker with a given estimator and a set of functional
// CostTrackerOption values.
func NewCostTracker(estimator ActualCostEstimator, opts ...CostTrackerOption) (*CostTracker, error) {
	return cost.NewTracker(estimator, opts...)
}

// CostTrackerLimit sets the runtime limit on the evaluation cost during execution and will
// terminate the expression evaluation if the limit is exceeded.
func CostTrackerLimit(limit uint64) CostTrackerOption {
	return cost.Limit(limit)
}

// PresenceTestHasCost determines whether presence testing has a cost of one or zero.
// Defaults to presence test has a cost of one.
func PresenceTestHasCost(hasCost bool) CostTrackerOption {
	return cost.TrackPresenceTest(hasCost)
}

// OverloadCostTracker binds an overload ID to a runtime FunctionTracker implementation.
//
// OverloadCostTracker instances augment or override ActualCostEstimator decisions, allowing for
// versioned and/or optional cost tracking changes.
func OverloadCostTracker(overloadID string, fnTracker FunctionTracker) CostTrackerOption {
	return cost.OverloadTracker(overloadID, fnTracker)
}

// costTrackPlanOption modifies the cost tracking factory associated with the CostObserver
type costTrackPlanOption func(*costTrackerFactory) *costTrackerFactory

// CostTrackerFactory configures the factory method to generate a new cost-tracker per-evaluation.
func CostTrackerFactory(factory func() (*CostTracker, error)) costTrackPlanOption {
	return func(fac *costTrackerFactory) *costTrackerFactory {
		fac.factory = factory
		return fac
	}
}

// CostObserver provides an observer that tracks runtime cost.
func CostObserver(opts ...costTrackPlanOption) PlannerOption {
	ct := &costTrackerFactory{}
	for _, o := range opts {
		ct = o(ct)
	}
	return func(p *planner) (*planner, error) {
		if ct.factory == nil {
			return nil, errors.New("cost tracker factory not configured")
		}
		p.observers = append(p.observers, ct)
		p.decorators = append(p.decorators, decObserveEval(ct.Observe))
		return p, nil
	}
}

// costTrackerConverter identifies an object which is convertible to a CostTracker instance.
type costTrackerConverter interface {
	asCostTracker() *CostTracker
}

// costTrackActivation hides state in the Activation in a manner not accessible to expressions.
type costTrackActivation struct {
	vars        Activation
	costTracker *CostTracker
}

// ResolveName proxies variable lookups to the backing activation.
func (cta costTrackActivation) ResolveName(name string) (any, bool) {
	return cta.vars.ResolveName(name)
}

// Parent proxies parent lookups to the backing activation.
func (cta costTrackActivation) Parent() Activation {
	return cta.vars
}

// AsPartialActivation supports conversion to a partial activation in order to detect unknown
// attributes.
func (cta costTrackActivation) AsPartialActivation() (PartialActivation, bool) {
	return AsPartialActivation(cta.vars)
}

// asCostTracker implements the costTrackerConverter method.
func (cta costTrackActivation) asCostTracker() *CostTracker {
	return cta.costTracker
}

// asCostTracker walks the Activation hierarchy and returns the first cost tracker found, if
// present.
func asCostTracker(vars Activation) (*CostTracker, bool) {
	if conv, ok := vars.(costTrackerConverter); ok {
		return conv.asCostTracker(), true
	}
	if vars.Parent() != nil {
		return asCostTracker(vars.Parent())
	}
	return nil, false
}

// costTrackerFactory holds a factory for producing new CostTracker instances on each Eval call.
type costTrackerFactory struct {
	factory func() (*CostTracker, error)
}

// InitState produces a CostTracker and bundles it into an Activation in a way which is not
// visible to expression evaluation.
func (ct *costTrackerFactory) InitState(vars Activation) (Activation, error) {
	tracker, err := ct.factory()
	if err != nil {
		return nil, err
	}
	return costTrackActivation{vars: vars, costTracker: tracker}, nil
}

// GetState extracts the CostTracker from the Activation.
func (ct *costTrackerFactory) GetState(vars Activation) any {
	if tracker, found := asCostTracker(vars); found {
		return tracker
	}
	return nil
}

// Observe charges the cost tracker for each step of an evaluation as it completes.
//
// Steps are observed depth-first, so by the time a call is observed each of its arguments has
// already produced a value. Recording every step's value against its expression id is what lets
// a call recover the arguments it was given without maintaining a shadow stack of the
// evaluation.
func (ct *costTrackerFactory) Observe(vars Activation, id int64, programStep any, val ref.Val) {
	tracker, found := asCostTracker(vars)
	if !found {
		return
	}
	tracker.RecordOperand(id, val)
	switch t := programStep.(type) {
	case ConstantQualifier:
		// TODO: Push identifiers on to the stack before observing constant qualifiers that apply
		// to them so that this case can be collapsed into the Qualifier case.
		tracker.TrackQualifier()
	case InterpretableConst:
		// Constants are free.
	case *evalTestOnly:
		tracker.TrackPresenceTest()
	case InterpretableAttribute:
		if _, isConditional := t.Attr().(*conditionalAttribute); isConditional {
			// A ternary has no cost of its own. All cost comes from the condition and from
			// whichever branch was selected.
			break
		}
		tracker.TrackIdent()
	case Qualifier:
		tracker.TrackQualifier()
	case InterpretableCall:
		if args, ok := callArgs(tracker, t); ok {
			tracker.TrackCall(t.Function(), t.OverloadID(), args, val)
		}
	case InterpretableConstructor:
		tracker.TrackConstruct(t.Type())
	case *evalFold:
		tracker.TrackComprehension(&foldComprehension{fold: t, tracker: tracker, result: val})
	}
	if tracker.LimitExceeded() {
		panic(EvalCancelledError{Cause: CostLimitExceeded, Message: "operation cancelled: actual cost limit exceeded"})
	}
}

// callArgs resolves the values which were passed to a call, and reports whether all of them were
// observed. Arguments are receiver-first, matching the operand order used when the call cost was
// estimated.
func callArgs(tracker *CostTracker, call InterpretableCall) ([]ref.Val, bool) {
	argExprs := call.Args()
	args := make([]ref.Val, len(argExprs))
	for i, argExpr := range argExprs {
		val, found := tracker.Operand(argExpr.ID())
		if !found {
			return nil, false
		}
		args[i] = val
	}
	return args, true
}

// foldComprehension exposes an evaluated comprehension to cost trackers.
type foldComprehension struct {
	fold    *evalFold
	tracker *CostTracker
	result  ref.Val
}

// IterRange returns the value the comprehension iterated over.
func (fc *foldComprehension) IterRange() ref.Val {
	val, found := fc.tracker.Operand(fc.fold.iterRange.ID())
	if !found {
		return nil
	}
	return val
}

// Iterations returns the number of steps the comprehension performed.
func (fc *foldComprehension) Iterations() uint64 {
	iterRange := fc.IterRange()
	if iterRange == nil {
		return 0
	}
	return cost.AggregateSize(iterRange)
}

// Accu returns the value the comprehension accumulated.
func (fc *foldComprehension) Accu() ref.Val {
	return fc.result
}
