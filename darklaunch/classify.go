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

package darklaunch

import (
	"errors"
	"strings"

	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
	"github.com/google/cel-go/interpreter"
)

// Error classes reported as OpenTelemetry's error.type.
//
// This is the complete list. OpenTelemetry requires error.type to be
// predictable and low cardinality and defines _OTHER as the well-known
// fallback, so the goal is not an exhaustive mapping of every message cel-go
// can produce: it is a closed, documented set, with everything else collapsing
// to ErrOther rather than leaking a raw message into a metric dimension.
//
// cel-go's error sentinels are unexported and its messages are not part of its
// compatibility contract, so classification is by prefix and is expected to
// need maintenance. A miss costs an ErrOther, never a wrong class.
const (
	ErrNoSuchKey       = "no_such_key"
	ErrNoSuchField     = "no_such_field"
	ErrNoSuchOverload  = "no_such_overload"
	ErrNoSuchAttribute = "no_such_attribute"
	ErrDivisionByZero  = "division_by_zero"
	ErrModulusByZero   = "modulus_by_zero"
	ErrOverflow        = "overflow"
	ErrIndexOutOfRange = "index_out_of_range"
	ErrConversion      = "conversion_error"
	ErrInterrupted     = "interrupted"
	ErrCostLimit       = "cost_limit_exceeded"
	ErrOther           = "_OTHER"
)

// errPrefixes maps a message prefix to its class, longest match wins.
var errPrefixes = []struct {
	prefix string
	class  string
}{
	{"no such key", ErrNoSuchKey},
	{"no such field", ErrNoSuchField},
	{"no such overload", ErrNoSuchOverload},
	{"no such attribute", ErrNoSuchAttribute},
	{"division by zero", ErrDivisionByZero},
	{"modulus by zero", ErrModulusByZero},
	{"integer overflow", ErrOverflow},
	{"unsigned integer overflow", ErrOverflow},
	{"duration overflow", ErrOverflow},
	{"timestamp overflow", ErrOverflow},
	{"index '", ErrIndexOutOfRange},
	{"index out of bounds", ErrIndexOutOfRange},
	{"slice index out of bounds", ErrIndexOutOfRange},
	{"type conversion error", ErrConversion},
	{"operation interrupted", ErrInterrupted},
	{"operation cancelled: actual cost limit exceeded", ErrCostLimit},
}

// ErrorType returns the normalized error class for a failed evaluation.
//
// Either argument may be nil: val carries the error for an evaluation that
// produced types.Err, err carries it for one that could not run at all.
func ErrorType(val ref.Val, err error) string {
	if err == nil && val != nil {
		if e, ok := val.(*types.Err); ok {
			err = e
		}
	}
	if err == nil {
		return ""
	}
	if errors.Is(err, interpreter.InterruptError{}) {
		return ErrInterrupted
	}
	var cancelled interpreter.EvalCancelledError
	if errors.As(err, &cancelled) {
		if cancelled.Cause == interpreter.CostLimitExceeded {
			return ErrCostLimit
		}
		return ErrInterrupted
	}
	msg := err.Error()
	best := ErrOther
	bestLen := 0
	for _, p := range errPrefixes {
		if len(p.prefix) > bestLen && strings.HasPrefix(msg, p.prefix) {
			best, bestLen = p.class, len(p.prefix)
		}
	}
	return best
}
