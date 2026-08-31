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

package functions

// ExecutionFrame accounts for work performed within a function implementation against the
// evaluation which invoked it.
//
// A frame is supplied to overloads which declare a FrameOp binding, and must be passed on to any
// function value the implementation invokes so that the work performed by the invoked function is
// charged against the evaluation's cost budget.
//
// A frame deliberately carries no access to the variable bindings of the evaluation. The inputs to
// a call are its arguments and nothing more, so a function value cannot read the state of the
// expression which called it.
type ExecutionFrame interface {
	// ChargeCost accounts for the cost of work performed within a function implementation.
	//
	// An error is returned when the evaluation's cost limit has been exceeded, at which point the
	// implementation must stop and surface the error to its caller.
	ChargeCost(cost uint64) error

	// Cost returns the cost accrued by the evaluation so far.
	//
	// The value is zero when cost tracking is not enabled for the evaluation.
	Cost() uint64
}
