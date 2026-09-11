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
	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
)

// ResultKind partitions a CEL result into the three outcomes the evaluator can
// produce. It is reported as the cel.result.kind attribute.
//
// The partition exists because OpenTelemetry's feature_flag.result.reason enum
// has no member for a partial evaluation: 'unknown' there means the reason is
// unknown, not that the result is.
type ResultKind string

// Result kinds.
const (
	KindValue   ResultKind = "value"
	KindError   ResultKind = "error"
	KindUnknown ResultKind = "unknown"
)

// KindOf classifies a CEL value.
func KindOf(val ref.Val) ResultKind {
	switch {
	case val == nil:
		return KindError
	case types.IsUnknown(val):
		return KindUnknown
	case types.IsError(val):
		return KindError
	default:
		return KindValue
	}
}

// Observation is one firing of one probe.
type Observation struct {
	// Name is the probe's literal name, the first argument to trace().
	Name string

	// Seq is the zero-based occurrence index of this name within a single
	// evaluation. A probe inside a comprehension fires once per iteration, and
	// each firing is recorded rather than collapsed.
	Seq int

	// Kind partitions Value into value, error or unknown.
	Kind ResultKind

	// Value is what the traced subexpression produced, unchanged.
	Value ref.Val
}

// Recorder accumulates probe observations for a single evaluation.
//
// A Recorder is not safe for concurrent use. It is owned by exactly one program
// and, through that program, by one goroutine at a time; Pool maintains that
// invariant.
type Recorder struct {
	obs    []Observation
	counts map[string]int
}

// NewRecorder returns an empty Recorder.
func NewRecorder() *Recorder {
	return &Recorder{counts: make(map[string]int)}
}

// observe is the late binding installed by RecordingBinding.
func (r *Recorder) observe(name, val ref.Val) ref.Val {
	// Under non-strict evaluation an error or unknown in the name argument is
	// handed here rather than short-circuited, so propagate it as a strict call
	// would have. The validator makes this unreachable for compiled
	// expressions, but the binding cannot assume it was used.
	if types.IsUnknownOrError(name) {
		return name
	}
	str, ok := name.(types.String)
	if !ok {
		return types.NewErr("trace() name must be a string, got %v", name.Type())
	}
	n := string(str)
	r.obs = append(r.obs, Observation{
		Name:  n,
		Seq:   r.counts[n],
		Kind:  KindOf(val),
		Value: val,
	})
	r.counts[n]++
	return val
}

// Observations returns the probes recorded so far, in evaluation order.
func (r *Recorder) Observations() []Observation {
	return r.obs
}

// Count returns how many times the named probe fired.
func (r *Recorder) Count(name string) int {
	return r.counts[name]
}

// Reset clears the recorder for reuse.
func (r *Recorder) Reset() {
	r.obs = r.obs[:0]
	for k := range r.counts {
		delete(r.counts, k)
	}
}

// snapshot copies the observations so they can outlive the recorder's next use.
func (r *Recorder) snapshot() []Observation {
	if len(r.obs) == 0 {
		return nil
	}
	out := make([]Observation, len(r.obs))
	copy(out, r.obs)
	return out
}
