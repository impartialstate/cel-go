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
	"testing"

	"github.com/google/cel-go/cel"
)

// TestErrorTypeFromRealEvaluations classifies errors produced by the evaluator
// rather than synthesized ones, so the prefix table is checked against the
// messages cel-go actually emits.
func TestErrorTypeFromRealEvaluations(t *testing.T) {
	tests := []struct {
		name string
		expr string
		vars map[string]any
		want string
	}{
		{"missing key", `m['nope']`, map[string]any{"m": map[string]int64{"a": 1}}, ErrNoSuchKey},
		{"divide by zero", `x / 0`, map[string]any{"x": 1}, ErrDivisionByZero},
		{"modulus by zero", `x % 0`, map[string]any{"x": 1}, ErrModulusByZero},
		{"index out of range", `items[10]`, map[string]any{"items": []int64{1}}, ErrIndexOutOfRange},
		{"overflow", `x + 9223372036854775807`, map[string]any{"x": 1}, ErrOverflow},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			res := evalOnce(t, `trace('probe', `+tc.expr+`)`, tc.vars)
			if res.Kind != KindError {
				t.Fatalf("Kind got %v, wanted error (value %v)", res.Kind, res.Value)
			}
			if res.ErrorType != tc.want {
				t.Errorf("ErrorType got %q, wanted %q (err: %v)", res.ErrorType, tc.want, res.Err)
			}
			if len(res.Probes) != 1 || res.Probes[0].Kind != KindError {
				t.Errorf("Probes got %v, wanted one error observation", res.Probes)
			}
		})
	}
}

// TestErrorTypeUnrecognized checks that an unclassified error collapses to the
// well-known fallback instead of leaking a message into a metric dimension.
func TestErrorTypeUnrecognized(t *testing.T) {
	if got := ErrorType(nil, errString("a message no classifier knows")); got != ErrOther {
		t.Errorf("ErrorType got %q, wanted %q", got, ErrOther)
	}
	if got := ErrorType(nil, nil); got != "" {
		t.Errorf("ErrorType(nil, nil) got %q, wanted empty", got)
	}
}

type errString string

func (e errString) Error() string { return string(e) }

// TestErrorTypeCostLimit checks the cost-limit class, which is signalled by a
// typed error rather than a message prefix.
func TestErrorTypeCostLimit(t *testing.T) {
	env := testEnv(t)
	pool, err := NewPool(env, compile(t, env, `items.all(i, trace('i', i < 100))`),
		cel.CostLimit(1))
	if err != nil {
		t.Fatalf("NewPool() failed: %v", err)
	}
	res, err := pool.Eval(map[string]any{"items": []int64{1, 2, 3, 4, 5}})
	if err != nil {
		t.Fatalf("Eval() failed: %v", err)
	}
	if res.ErrorType != ErrCostLimit {
		t.Errorf("ErrorType got %q, wanted %q (err: %v, kind %v)",
			res.ErrorType, ErrCostLimit, res.Err, res.Kind)
	}
}

// TestAggregateSeries checks the emitted instrument names, units and attribute
// sets against the OpenTelemetry shape.
func TestAggregateSeries(t *testing.T) {
	agg := NewAggregate()
	attrs := Attrs{
		FlagKey:     "checkout.fraud_hold",
		Role:        RoleControl,
		Applied:     true,
		Environment: "production",
	}
	res := evalOnce(t, `trace('gt', x > 1) && trace('lt', x < 10)`, map[string]any{"x": 5})
	agg.Record(attrs, res)
	agg.Record(attrs, res)

	series := agg.Snapshot()
	byName := map[string][]Series{}
	for _, s := range series {
		byName[s.Name] = append(byName[s.Name], s)
	}

	dur := byName[MetricEvalDuration]
	if len(dur) != 1 {
		t.Fatalf("%s series got %d, wanted 1", MetricEvalDuration, len(dur))
	}
	if dur[0].Unit != UnitSeconds || dur[0].Kind != KindHistogram {
		t.Errorf("%s got unit %q kind %q, wanted %q/%q",
			MetricEvalDuration, dur[0].Unit, dur[0].Kind, UnitSeconds, KindHistogram)
	}
	if dur[0].Count != 2 {
		t.Errorf("%s Count got %d, wanted 2", MetricEvalDuration, dur[0].Count)
	}
	want := map[string]string{
		AttrFlagKey:     "checkout.fraud_hold",
		AttrRole:        RoleControl,
		AttrApplied:     "true",
		AttrResultKind:  string(KindValue),
		AttrEnvironment: "production",
	}
	for k, v := range want {
		if dur[0].Attributes[k] != v {
			t.Errorf("%s attribute %s got %q, wanted %q", MetricEvalDuration, k, dur[0].Attributes[k], v)
		}
	}
	// error.type must be absent on a successful evaluation.
	if _, found := dur[0].Attributes[AttrErrorType]; found {
		t.Errorf("%s carried %s on a successful evaluation", MetricEvalDuration, AttrErrorType)
	}
	// The final bucket is the +Inf bucket and must hold every observation.
	last := dur[0].Buckets[len(dur[0].Buckets)-1]
	if !last.Infinite || last.Count != 2 {
		t.Errorf("%s +Inf bucket got %+v, wanted all 2 observations", MetricEvalDuration, last)
	}

	cost := byName[MetricEvalCost]
	if len(cost) != 1 || cost[0].Unit != UnitCost {
		t.Fatalf("%s series got %v, wanted one series with unit %q", MetricEvalCost, cost, UnitCost)
	}
	if cost[0].Sum <= 0 {
		t.Errorf("%s Sum got %v, wanted a tracked cost above zero", MetricEvalCost, cost[0].Sum)
	}

	probes := byName[MetricProbeObservations]
	if len(probes) != 2 {
		t.Fatalf("%s series got %d, wanted one per probe name", MetricProbeObservations, len(probes))
	}
	for _, s := range probes {
		if s.Kind != KindCounter || s.Unit != UnitObservation {
			t.Errorf("%s got kind %q unit %q, wanted %q/%q",
				MetricProbeObservations, s.Kind, s.Unit, KindCounter, UnitObservation)
		}
		if s.Count != 2 {
			t.Errorf("%s[%s] Count got %d, wanted 2",
				MetricProbeObservations, s.Attributes[AttrProbeName], s.Count)
		}
	}
}

// TestAggregatePartitionsByResultKind checks that a failed evaluation lands in
// its own series carrying error.type, rather than being folded in with
// successes.
func TestAggregatePartitionsByResultKind(t *testing.T) {
	agg := NewAggregate()
	attrs := Attrs{FlagKey: "k", Role: RoleCandidate, Applied: false}
	agg.Record(attrs, evalOnce(t, `trace('p', x > 1)`, map[string]any{"x": 5}))
	agg.Record(attrs, evalOnce(t, `trace('p', m['nope'])`,
		map[string]any{"m": map[string]int64{}}))

	var okSeries, errSeries *Series
	for i, s := range agg.Snapshot() {
		if s.Name != MetricEvalDuration {
			continue
		}
		switch s.Attributes[AttrResultKind] {
		case string(KindValue):
			okSeries = &agg.Snapshot()[i]
		case string(KindError):
			errSeries = &agg.Snapshot()[i]
		}
	}
	if okSeries == nil || errSeries == nil {
		t.Fatalf("wanted separate value and error series, got value=%v error=%v", okSeries, errSeries)
	}
	if errSeries.Attributes[AttrErrorType] != ErrNoSuchKey {
		t.Errorf("error series %s got %q, wanted %q",
			AttrErrorType, errSeries.Attributes[AttrErrorType], ErrNoSuchKey)
	}
	if errSeries.Attributes[AttrApplied] != "false" {
		t.Errorf("error series %s got %q, wanted false",
			AttrApplied, errSeries.Attributes[AttrApplied])
	}
}
