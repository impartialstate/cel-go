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

package traits

import (
	"github.com/google/cel-go/common/functions"
	"github.com/google/cel-go/common/types/ref"
)

// Invoker interface for values which may be called as functions.
//
// Function references, e.g. the name of a declared function used as a value, and variables
// declared with a function type both surface to CEL and to extension functions as Invoker
// instances.
type Invoker interface {
	// Invoke calls the function with the given arguments and returns its result.
	//
	// The frame is the execution frame of the evaluation which is making the call, and is what
	// accounts for the cost of the invocation. It may be nil when the value is invoked outside
	// of an evaluation.
	//
	// Implementations are strict: an error or unknown argument is returned to the caller
	// rather than being passed through to the underlying function implementation.
	Invoke(frame functions.ExecutionFrame, args ...ref.Val) ref.Val
}
