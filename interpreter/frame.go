// Copyright 2024 Google LLC
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

// ExecutionFrame provides the context for a single evaluation of an expression.
type ExecutionFrame struct {
	Activation
	state EvalState
	costs *CostTracker

	Interrupt               <-chan struct{}
	InterruptCheckCount     uint
	InterruptCheckFrequency uint
}

// ResolveName implements the Activation interface by proxying to the internal activation.
//
// If the name is "#interrupted", the ExecutionFrame handles the interrupt check count and rate
// limit logic internally.
func (f *ExecutionFrame) ResolveName(name string) (any, bool) {
	if name == "#interrupted" {
		if f.CheckInterrupt() {
			return true, true
		}
		return nil, false
	}
	return f.Activation.ResolveName(name)
}

// Parent implements the Activation interface by proxying to the internal activation.
func (f *ExecutionFrame) Parent() Activation {
	return f.Activation.Parent()
}

// AsPartialActivation implements the PartialActivation interface by proxying to the internal activation.
func (f *ExecutionFrame) AsPartialActivation() (PartialActivation, bool) {
	return AsPartialActivation(f.Activation)
}

// Unwrap returns the internal activation.
func (f *ExecutionFrame) Unwrap() Activation {
	return f.Activation
}

// CheckInterrupt returns whether the evaluation has been interrupted.
func (f *ExecutionFrame) CheckInterrupt() bool {
	f.InterruptCheckCount++
	if f.InterruptCheckFrequency > 0 && f.InterruptCheckCount%f.InterruptCheckFrequency == 0 {
		select {
		case <-f.Interrupt:
			return true
		default:
			return false
		}
	}
	return false
}
