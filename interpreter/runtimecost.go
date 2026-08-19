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

	"github.com/google/cel-go/common"
	"github.com/google/cel-go/common/cost"
	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
)

// Runtime cost tracking is implemented by the common/cost package, which holds the compile time
// estimation as well. What remains here is the plumbing which drives a cost tracker from the
// steps of an evaluation, since that is the part which depends on the interpreter itself.

// ActualCostEstimator provides function call cost estimations at runtime.
//
// CallCost returns an estimated cost for the function overload invocation with the given args,
// or nil if it has no estimate to provide. CEL attempts to provide reasonable estimates for its
// standard function library, so CallCost should typically not need to provide an estimate for
// CELs standard function.
type ActualCostEstimator = cost.ActualCostEstimator

// CostTracker represents the information needed for tracking runtime cost.
type CostTracker = cost.CostTracker

// CostTrackerOption configures the behavior of CostTracker objects.
type CostTrackerOption = cost.CostTrackerOption

// FunctionTracker computes the actual cost of evaluating the functions with the given arguments
// and result.
type FunctionTracker = cost.FunctionTracker

// NewCostTracker creates a new CostTracker with a given estimator and a set of functional
// CostTrackerOption values.
func NewCostTracker(estimator ActualCostEstimator, opts ...CostTrackerOption) (*CostTracker, error) {
	return cost.NewCostTracker(estimator, opts...)
}

// CostTrackerLimit sets the runtime limit on the evaluation cost during execution and will
// terminate the expression evaluation if the limit is exceeded.
func CostTrackerLimit(limit uint64) CostTrackerOption {
	return cost.CostTrackerLimit(limit)
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
	return cost.OverloadCostTracker(overloadID, fnTracker)
}

// costTrackPlanOption modifies the cost tracking factory associatied with the CostObserver
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

// AsPartialActivation supports conversion to a partial activation in order to detect unknown attributes.
func (cta costTrackActivation) AsPartialActivation() (PartialActivation, bool) {
	return AsPartialActivation(cta.vars)
}

// asCostTracker implements the costTrackerConverter method.
func (cta costTrackActivation) asCostTracker() *CostTracker {
	return cta.costTracker
}

// asCostTracker walks the Activation hierarchy and returns the first cost tracker found, if present.
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

// InitState produces a CostTracker and bundles it into an Activation in a way which is not visible
// to expression evaluation.
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

// Observe computes the incremental cost of each step and records it into the CostTracker
// associated with the evaluation.
func (ct *costTrackerFactory) Observe(vars Activation, id int64, programStep any, val ref.Val) {
	tracker, found := asCostTracker(vars)
	if !found {
		return
	}
	switch t := programStep.(type) {
	case ConstantQualifier:
		// TODO: Push identifiers on to the stack before observing constant qualifiers that apply to them
		// and enable the below pop. Once enabled this can case can be collapsed into the Qualifier case.
		tracker.Add(1)
	case InterpretableConst:
		// zero cost
	case InterpretableAttribute:
		switch a := t.Attr().(type) {
		case *conditionalAttribute:
			// Ternary has no direct cost. All cost is from the conditional and the true/false branch expressions.
			tracker.Drop(a.falsy.ID(), a.truthy.ID(), a.expr.ID())
		default:
			tracker.Drop(t.Attr().ID())
			if _, isTestOnly := programStep.(*evalTestOnly); !isTestOnly || tracker.PresenceTestHasCost() {
				tracker.Add(common.SelectAndIdentCost)
			}
		}
	case *evalExhaustiveConditional:
		// Ternary has no direct cost. All cost is from the conditional and the true/false branch expressions.
		tracker.Drop(t.attr.falsy.ID(), t.attr.truthy.ID(), t.attr.expr.ID())

	// While the field names are identical, the boolean operation eval structs do not share an interface and so
	// must be handled individually.
	case *evalOr:
		tracker.Drop(termIDs(t.terms)...)
	case *evalAnd:
		tracker.Drop(termIDs(t.terms)...)
	case *evalExhaustiveOr:
		tracker.Drop(termIDs(t.terms)...)
	case *evalExhaustiveAnd:
		tracker.Drop(termIDs(t.terms)...)
	case *evalFold:
		tracker.Drop(t.iterRange.ID())
	case Qualifier:
		tracker.Add(1)
	case InterpretableCall:
		if argVals, ok := tracker.DropArgs(termIDs(t.Args())); ok {
			tracker.TrackCall(t.Function(), t.OverloadID(), argVals, val)
		}
	case InterpretableConstructor:
		tracker.DropArgs(termIDs(t.InitVals()))
		switch t.Type() {
		case types.ListType:
			tracker.Add(common.ListCreateBaseCost)
		case types.MapType:
			tracker.Add(common.MapCreateBaseCost)
		default:
			tracker.Add(common.StructCreateBaseCost)
		}
	}
	tracker.Push(val, id)

	if tracker.LimitExceeded() {
		panic(EvalCancelledError{Cause: CostLimitExceeded, Message: "operation cancelled: actual cost limit exceeded"})
	}
}

// termIDs returns the expression ids of a set of evaluation steps.
func termIDs(terms []Interpretable) []int64 {
	ids := make([]int64, len(terms))
	for i, term := range terms {
		ids[i] = term.ID()
	}
	return ids
}
