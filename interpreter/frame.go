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

import "sync"

// ExecutionFrame provides the context for a single evaluation of an expression.
type ExecutionFrame struct {
	Activation
	Interrupt               <-chan struct{}
	InterruptCheckCount     uint
	InterruptCheckFrequency uint

	state  EvalState
	costs  *CostTracker
	parent *ExecutionFrame
}

func (f *ExecutionFrame) Push(activation Activation) *ExecutionFrame {
	child := frameStack.create(f)
	child.Activation = activationStack.create(f.Activation, activation)
	return child
}

func (f *ExecutionFrame) Pop() *ExecutionFrame {
	return frameStack.release(f)
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

type framePool struct {
	sync.Pool
}

func (pool *framePool) create(parent *ExecutionFrame) *ExecutionFrame {
	child := pool.Get().(*ExecutionFrame)
	child.parent = parent
	child.costs = parent.costs
	child.state = parent.state
	child.Interrupt = parent.Interrupt
	child.InterruptCheckCount = parent.InterruptCheckCount
	child.InterruptCheckFrequency = parent.InterruptCheckFrequency
	return child
}

func (pool *framePool) release(frame *ExecutionFrame) *ExecutionFrame {
	parent := frame.parent
	if parent.InterruptCheckCount != frame.InterruptCheckCount {
		parent.InterruptCheckCount = frame.InterruptCheckCount
	}

	activationStack.release(frame.Activation)
	frame.Activation = nil
	frame.state = nil
	frame.costs = nil
	frame.parent = nil
	frame.Interrupt = nil
	frame.InterruptCheckCount = 0
	frame.InterruptCheckFrequency = 0
	pool.Pool.Put(frame)
	return parent
}

func newFramePool() *framePool {
	return &framePool{
		Pool: sync.Pool{
			New: func() any {
				return &ExecutionFrame{}
			},
		},
	}
}

type activationStackPool struct {
	sync.Pool
}

func (pool *activationStackPool) create(parent, child Activation) Activation {
	h := pool.Get().(*hierarchicalActivation)
	h.child = child
	h.parent = parent
	return h
}

func (pool *activationStackPool) release(activation Activation) {
	h, ok := activation.(*hierarchicalActivation)
	if !ok {
		return
	}
	h.parent = nil
	h.child = nil
	pool.Pool.Put(h)
}

func newActivationPool() *activationStackPool {
	return &activationStackPool{
		Pool: sync.Pool{
			New: func() any {
				return &hierarchicalActivation{}
			},
		},
	}
}

var (
	activationStack = newActivationPool()
	frameStack      = newFramePool()
)
