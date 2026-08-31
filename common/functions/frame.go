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

// Bindings resolves variable names to values, and is the view of an evaluation's variables which
// an execution frame exposes.
//
// The Activation types used to evaluate an expression satisfy this interface, as do the argument
// bindings of a call.
type Bindings interface {
	// ResolveName returns the value bound to a variable name, if present.
	ResolveName(name string) (any, bool)
}

// ExecutionFrame provides a function implementation with the state of the evaluation which invoked
// it: the variables in scope for the call, and the cost budget the call is charged against.
//
// A frame is supplied to overloads which declare a FrameOp binding, and must be passed on to any
// function value the implementation invokes so that the work performed by the invoked function is
// charged against the evaluation's cost budget.
//
// The variables a call can reach are determined by the frame it is given rather than by the frame
// of its caller. A function value is invoked on a frame rebound away from the variables of the
// caller, so its arguments are its only inputs; an implementation which evaluates against named
// inputs of its own, such as one built from an expression, binds them onto that frame itself.
type ExecutionFrame interface {
	Bindings

	// WithBindings returns a copy of the frame which resolves names from the given bindings in
	// place of those of the evaluation which created it, and which continues to charge cost
	// against the same budget.
	//
	// Nil bindings resolve no names at all. Rebinding cannot recover the variables a frame was
	// created with, so a frame may be narrowed but never widened.
	WithBindings(bindings Bindings) ExecutionFrame

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
