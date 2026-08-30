// Copyright 2025 Google LLC
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
	"github.com/google/cel-go/common/functions"
)

// execFrame exposes the state of an in-progress evaluation to function implementations which
// declare a frame binding.
type execFrame struct {
	vars    Activation
	tracker *CostTracker
}

var _ functions.ExecutionFrame = &execFrame{}

// newExecFrame creates an execution frame for the current evaluation state, binding the cost
// tracker for the evaluation when cost tracking is enabled.
func newExecFrame(vars Activation) *execFrame {
	tracker, _ := asCostTracker(vars)
	return &execFrame{vars: vars, tracker: tracker}
}

// ResolveName implements the functions.ExecutionFrame interface method.
func (f *execFrame) ResolveName(name string) (any, bool) {
	return f.vars.ResolveName(name)
}

// ChargeCost implements the functions.ExecutionFrame interface method.
//
// Charging is a no-op when cost tracking is not enabled for the evaluation.
func (f *execFrame) ChargeCost(cost uint64) error {
	if f.tracker == nil {
		return nil
	}
	return f.tracker.chargeCost(cost)
}

// Cost implements the functions.ExecutionFrame interface method.
func (f *execFrame) Cost() uint64 {
	if f.tracker == nil {
		return 0
	}
	return f.tracker.ActualCost()
}
