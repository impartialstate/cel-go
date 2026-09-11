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
	"sort"
	"sync"
)

// OpenTelemetry attribute names.
//
// feature_flag.* and error.type are reused from the OpenTelemetry semantic
// conventions rather than reinvented. deployment.environment.name is Stable;
// feature_flag.* is Release Candidate. The cel.* names are not registered with
// OpenTelemetry and describe what no existing convention does.
const (
	AttrFlagKey     = "feature_flag.key"
	AttrErrorType   = "error.type"
	AttrEnvironment = "deployment.environment.name"

	// AttrRole distinguishes the program that served the request from the ones
	// evaluated for comparison.
	AttrRole = "cel.evaluation.role"

	// AttrApplied is false when the result was computed and discarded. It is
	// emitted unconditionally, because a shadow evaluation that looks like a
	// real decision in a backend is how discarded results end up in an alert or
	// an audit trail.
	AttrApplied = "cel.evaluation.applied"

	// AttrResultKind is value, error or unknown. OpenTelemetry's
	// feature_flag.result.reason has no member for a partial result.
	AttrResultKind = "cel.result.kind"

	// AttrProbeName is the literal name given to trace().
	AttrProbeName = "cel.probe.name"
)

// Instrument names and units. Naming follows the OpenTelemetry rules: no
// _total suffix, plural only for discrete countable quantities carrying a
// {unit} annotation, and .duration for elapsed time.
const (
	MetricEvalDuration      = "cel.evaluation.duration"
	MetricEvalCost          = "cel.evaluation.cost"
	MetricProbeObservations = "cel.probe.observations"

	UnitSeconds     = "s"
	UnitCost        = "{cost}"
	UnitObservation = "{observation}"
)

// Evaluation roles.
const (
	RoleControl       = "control"
	RoleCandidate     = "candidate"
	RoleShadowControl = "shadow_control"
)

// InstrumentKind distinguishes the two instrument shapes this package emits.
type InstrumentKind string

// Instrument kinds.
const (
	KindHistogram InstrumentKind = "histogram"
	KindCounter   InstrumentKind = "counter"
)

// Buckets for the duration histogram, in seconds. CEL evaluations are
// routinely sub-microsecond, so the default HTTP-shaped boundaries would put
// every observation in the first bucket.
var durationBuckets = []float64{
	1e-7, 2.5e-7, 5e-7, 1e-6, 2.5e-6, 5e-6, 1e-5, 2.5e-5, 5e-5, 1e-4, 5e-4, 1e-3, 1e-2,
}

// Buckets for the cost histogram, in cost units.
var costBuckets = []float64{1, 2, 5, 10, 25, 50, 100, 250, 500, 1000, 5000, 10000}

// Attrs identifies the expression and the role an evaluation played.
type Attrs struct {
	// FlagKey is the policy or expression key, emitted as feature_flag.key.
	FlagKey string

	// Role is one of RoleControl, RoleCandidate or RoleShadowControl.
	Role string

	// Applied reports whether the result was returned to the caller.
	Applied bool

	// Environment is emitted as deployment.environment.name when set.
	Environment string
}

type evalKey struct {
	flagKey, role, applied, env, kind, errType string
}

type probeKey struct {
	flagKey, role, name, kind string
}

// Aggregate accumulates instrumented evaluations into OpenTelemetry-shaped
// series.
//
// A mutex is used rather than sharded atomics: this is a proof of concept, and
// a lock that is provably correct is worth more here than one that is provably
// fast. Record is called once per evaluation, off the request path.
type Aggregate struct {
	mu     sync.Mutex
	dur    map[evalKey]*histogram
	cost   map[evalKey]*histogram
	probes map[probeKey]uint64
}

// NewAggregate returns an empty Aggregate.
func NewAggregate() *Aggregate {
	return &Aggregate{
		dur:    make(map[evalKey]*histogram),
		cost:   make(map[evalKey]*histogram),
		probes: make(map[probeKey]uint64),
	}
}

// Record folds one evaluation into the aggregate.
func (a *Aggregate) Record(attrs Attrs, r *Result) {
	if r == nil {
		return
	}
	applied := "false"
	if attrs.Applied {
		applied = "true"
	}
	ek := evalKey{
		flagKey: attrs.FlagKey,
		role:    attrs.Role,
		applied: applied,
		env:     attrs.Environment,
		kind:    string(r.Kind),
		errType: r.ErrorType,
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	hist(a.dur, ek, durationBuckets).observe(r.Duration.Seconds())
	if r.Cost != nil {
		hist(a.cost, ek, costBuckets).observe(float64(*r.Cost))
	}
	for _, o := range r.Probes {
		a.probes[probeKey{
			flagKey: attrs.FlagKey,
			role:    attrs.Role,
			name:    o.Name,
			kind:    string(o.Kind),
		}]++
	}
}

func hist(m map[evalKey]*histogram, k evalKey, bounds []float64) *histogram {
	h, ok := m[k]
	if !ok {
		h = newHistogram(bounds)
		m[k] = h
	}
	return h
}

// Bucket is one cumulative histogram bucket boundary and its count.
type Bucket struct {
	// LE is the inclusive upper bound. The final bucket is +Inf, reported as 0.
	LE       float64
	Infinite bool
	Count    uint64
}

// Series is one instrument at one attribute set.
type Series struct {
	Name       string
	Unit       string
	Kind       InstrumentKind
	Attributes map[string]string
	Count      uint64
	Sum        float64
	Buckets    []Bucket
}

// Snapshot renders the aggregate as a deterministically ordered set of series.
func (a *Aggregate) Snapshot() []Series {
	a.mu.Lock()
	defer a.mu.Unlock()

	out := make([]Series, 0, len(a.dur)+len(a.cost)+len(a.probes))
	for k, h := range a.dur {
		out = append(out, h.series(MetricEvalDuration, UnitSeconds, evalAttrs(k)))
	}
	for k, h := range a.cost {
		out = append(out, h.series(MetricEvalCost, UnitCost, evalAttrs(k)))
	}
	for k, n := range a.probes {
		attrs := map[string]string{
			AttrProbeName:  k.name,
			AttrResultKind: k.kind,
		}
		putIf(attrs, AttrFlagKey, k.flagKey)
		putIf(attrs, AttrRole, k.role)
		out = append(out, Series{
			Name:       MetricProbeObservations,
			Unit:       UnitObservation,
			Kind:       KindCounter,
			Attributes: attrs,
			Count:      n,
			Sum:        float64(n),
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Name != out[j].Name {
			return out[i].Name < out[j].Name
		}
		return attrString(out[i].Attributes) < attrString(out[j].Attributes)
	})
	return out
}

func evalAttrs(k evalKey) map[string]string {
	attrs := map[string]string{
		AttrResultKind: k.kind,
		AttrApplied:    k.applied,
	}
	putIf(attrs, AttrFlagKey, k.flagKey)
	putIf(attrs, AttrRole, k.role)
	putIf(attrs, AttrEnvironment, k.env)
	// error.type is omitted on success, as the convention requires.
	putIf(attrs, AttrErrorType, k.errType)
	return attrs
}

func putIf(m map[string]string, k, v string) {
	if v != "" {
		m[k] = v
	}
}

func attrString(m map[string]string) string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b []byte
	for _, k := range keys {
		b = append(b, k...)
		b = append(b, '=')
		b = append(b, m[k]...)
		b = append(b, ',')
	}
	return string(b)
}

type histogram struct {
	bounds []float64
	counts []uint64 // len(bounds)+1, last is +Inf
	count  uint64
	sum    float64
}

func newHistogram(bounds []float64) *histogram {
	return &histogram{bounds: bounds, counts: make([]uint64, len(bounds)+1)}
}

func (h *histogram) observe(v float64) {
	h.count++
	h.sum += v
	idx := sort.SearchFloat64s(h.bounds, v)
	if idx < len(h.bounds) && h.bounds[idx] == v {
		// SearchFloat64s returns the first index >= v; an exact hit belongs in
		// that bucket because boundaries are inclusive upper bounds.
		h.counts[idx]++
		return
	}
	h.counts[idx]++
}

func (h *histogram) series(name, unit string, attrs map[string]string) Series {
	buckets := make([]Bucket, 0, len(h.counts))
	var cumulative uint64
	for i, c := range h.counts {
		cumulative += c
		if i == len(h.bounds) {
			buckets = append(buckets, Bucket{Infinite: true, Count: cumulative})
			continue
		}
		buckets = append(buckets, Bucket{LE: h.bounds[i], Count: cumulative})
	}
	return Series{
		Name:       name,
		Unit:       unit,
		Kind:       KindHistogram,
		Attributes: attrs,
		Count:      h.count,
		Sum:        h.sum,
		Buckets:    buckets,
	}
}
