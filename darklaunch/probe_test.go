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
	"strings"
	"sync"
	"testing"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/types"
)

func testEnv(t *testing.T, opts ...cel.EnvOption) *cel.Env {
	t.Helper()
	base := []cel.EnvOption{
		ProbeDecls(),
		cel.Variable("x", cel.IntType),
		cel.Variable("m", cel.MapType(cel.StringType, cel.IntType)),
		cel.Variable("items", cel.ListType(cel.IntType)),
	}
	env, err := cel.NewEnv(append(base, opts...)...)
	if err != nil {
		t.Fatalf("cel.NewEnv() failed: %v", err)
	}
	return env
}

func compile(t *testing.T, env *cel.Env, expr string) *cel.Ast {
	t.Helper()
	a, iss := env.Compile(expr)
	if iss.Err() != nil {
		t.Fatalf("Compile(%q) failed: %v", expr, iss.Err())
	}
	return a
}

func evalOnce(t *testing.T, expr string, vars any, opts ...cel.ProgramOption) *Result {
	t.Helper()
	env := testEnv(t)
	pool, err := NewPool(env, compile(t, env, expr), opts...)
	if err != nil {
		t.Fatalf("NewPool() failed: %v", err)
	}
	res, err := pool.Eval(vars)
	if err != nil {
		t.Fatalf("Eval() failed: %v", err)
	}
	return res
}

// TestTraceIsIdentity checks that the production binding changes nothing about
// the value of an expression.
func TestTraceIsIdentity(t *testing.T) {
	env := testEnv(t)
	tests := []struct {
		traced string
		plain  string
	}{
		{`trace('n', x + 1)`, `x + 1`},
		{`trace('a', x > 1) && trace('b', x < 10)`, `x > 1 && x < 10`},
		{`[1, 2, 3].all(i, trace('lt', i < 4))`, `[1, 2, 3].all(i, i < 4)`},
		{`trace('m', m)`, `m`},
	}
	vars := map[string]any{"x": 5, "m": map[string]int64{"a": 1}}
	for _, tc := range tests {
		t.Run(tc.traced, func(t *testing.T) {
			tracedPrg, err := NewProgram(env, compile(t, env, tc.traced))
			if err != nil {
				t.Fatalf("NewProgram() failed: %v", err)
			}
			plainPrg, err := NewProgram(env, compile(t, env, tc.plain))
			if err != nil {
				t.Fatalf("NewProgram() failed: %v", err)
			}
			got, _, err := tracedPrg.Eval(vars)
			if err != nil {
				t.Fatalf("Eval(%q) failed: %v", tc.traced, err)
			}
			want, _, err := plainPrg.Eval(vars)
			if err != nil {
				t.Fatalf("Eval(%q) failed: %v", tc.plain, err)
			}
			if got.Equal(want) != types.True {
				t.Errorf("traced %q got %v, plain %q got %v", tc.traced, got, tc.plain, want)
			}
		})
	}
}

// TestProbeCaptureOrder checks names, order and kinds for a simple expression.
func TestProbeCaptureOrder(t *testing.T) {
	res := evalOnce(t, `trace('gt', x > 1) && trace('lt', x < 10)`, map[string]any{"x": 5})
	if res.Kind != KindValue {
		t.Fatalf("Kind got %v, wanted %v", res.Kind, KindValue)
	}
	want := []string{"gt", "lt"}
	if len(res.Probes) != len(want) {
		t.Fatalf("Probes got %d, wanted %d: %v", len(res.Probes), len(want), res.Probes)
	}
	for i, n := range want {
		if res.Probes[i].Name != n {
			t.Errorf("Probes[%d].Name got %q, wanted %q", i, res.Probes[i].Name, n)
		}
		if res.Probes[i].Kind != KindValue {
			t.Errorf("Probes[%d].Kind got %v, wanted %v", i, res.Probes[i].Kind, KindValue)
		}
	}
}

// TestProbeShortCircuit checks that a probe in an unevaluated branch does not
// fire, which is what makes coverage divergence detectable between two
// expressions.
func TestProbeShortCircuit(t *testing.T) {
	res := evalOnce(t, `trace('gt', x > 100) && trace('lt', x < 10)`, map[string]any{"x": 5})
	if len(res.Probes) != 1 || res.Probes[0].Name != "gt" {
		t.Fatalf("Probes got %v, wanted only 'gt'", res.Probes)
	}
}

// TestProbeCapturesError is the non-strict payoff: a probe wrapping a
// subexpression that errors still fires, with the error as its value, and the
// error propagates unchanged.
func TestProbeCapturesError(t *testing.T) {
	res := evalOnce(t, `trace('lookup', m['missing'])`, map[string]any{"m": map[string]int64{"a": 1}})
	if res.Kind != KindError {
		t.Fatalf("Kind got %v, wanted %v (err: %v)", res.Kind, KindError, res.Err)
	}
	if res.ErrorType != ErrNoSuchKey {
		t.Errorf("ErrorType got %q, wanted %q (err: %v)", res.ErrorType, ErrNoSuchKey, res.Err)
	}
	if len(res.Probes) != 1 {
		t.Fatalf("Probes got %v, wanted one observation", res.Probes)
	}
	if res.Probes[0].Name != "lookup" || res.Probes[0].Kind != KindError {
		t.Errorf("Probes[0] got %+v, wanted lookup/error", res.Probes[0])
	}
	if !types.IsError(res.Probes[0].Value) {
		t.Errorf("Probes[0].Value got %v, wanted an error value", res.Probes[0].Value)
	}
}

// TestProbeCapturesUnknown checks the third result kind, which OpenTelemetry's
// feature_flag.result.reason enum cannot express.
func TestProbeCapturesUnknown(t *testing.T) {
	env := testEnv(t)
	pool, err := NewPool(env, compile(t, env, `trace('gt', x > 1)`),
		cel.EvalOptions(cel.OptPartialEval))
	if err != nil {
		t.Fatalf("NewPool() failed: %v", err)
	}
	vars, err := cel.PartialVars(map[string]any{}, cel.AttributePattern("x"))
	if err != nil {
		t.Fatalf("PartialVars() failed: %v", err)
	}
	res, err := pool.Eval(vars)
	if err != nil {
		t.Fatalf("Eval() failed: %v", err)
	}
	if res.Kind != KindUnknown {
		t.Fatalf("Kind got %v, wanted %v (value %v)", res.Kind, KindUnknown, res.Value)
	}
	if len(res.Probes) != 1 || res.Probes[0].Kind != KindUnknown {
		t.Fatalf("Probes got %v, wanted one unknown observation", res.Probes)
	}
}

// TestProbeInComprehension checks that a probe under a macro is recorded once
// per iteration rather than collapsed.
func TestProbeInComprehension(t *testing.T) {
	res := evalOnce(t, `items.all(i, trace('item', i < 100))`,
		map[string]any{"items": []int64{1, 2, 3}})
	if len(res.Probes) != 3 {
		t.Fatalf("Probes got %d, wanted 3: %v", len(res.Probes), res.Probes)
	}
	for i, o := range res.Probes {
		if o.Name != "item" {
			t.Errorf("Probes[%d].Name got %q, wanted %q", i, o.Name, "item")
		}
		if o.Seq != i {
			t.Errorf("Probes[%d].Seq got %d, wanted %d", i, o.Seq, i)
		}
	}
}

// TestTraceSurvivesConstantFolding is the late-binding payoff: the constant
// folder skips calls whose declaration reports a late binding, so a probe over
// constant operands is not optimized away.
func TestTraceSurvivesConstantFolding(t *testing.T) {
	env := testEnv(t)
	a := compile(t, env, `trace('sum', 1 + 2) == 3`)

	folder, err := cel.NewConstantFoldingOptimizer()
	if err != nil {
		t.Fatalf("NewConstantFoldingOptimizer() failed: %v", err)
	}
	opt, err := cel.NewStaticOptimizer(folder)
	if err != nil {
		t.Fatalf("NewStaticOptimizer() failed: %v", err)
	}
	folded, iss := opt.Optimize(env, a)
	if iss.Err() != nil {
		t.Fatalf("Optimize() failed: %v", iss.Err())
	}
	out, err := cel.AstToString(folded)
	if err != nil {
		t.Fatalf("AstToString() failed: %v", err)
	}
	if !strings.Contains(out, "trace(") {
		t.Fatalf("folding erased the probe: got %q", out)
	}

	// The probe must still fire after folding.
	pool, err := NewPool(env, folded)
	if err != nil {
		t.Fatalf("NewPool() failed: %v", err)
	}
	res, err := pool.Eval(map[string]any{})
	if err != nil {
		t.Fatalf("Eval() failed: %v", err)
	}
	if len(res.Probes) != 1 || res.Probes[0].Name != "sum" {
		t.Fatalf("Probes got %v, wanted one 'sum' observation", res.Probes)
	}
}

// TestMissingBindingFailsAtEvaluation documents the cost of late binding: a
// declaration with no implementation plans cleanly and fails at evaluation.
// This is why the production and instrumented paths both go through a
// constructor that installs one.
func TestMissingBindingFailsAtEvaluation(t *testing.T) {
	env := testEnv(t)
	prg, err := env.Program(compile(t, env, `trace('n', x)`))
	if err != nil {
		t.Fatalf("Program() planned with an error, wanted a clean plan: %v", err)
	}
	_, _, err = prg.Eval(map[string]any{"x": 1})
	if err == nil {
		t.Fatal("Eval() succeeded without a trace() binding, wanted no such overload")
	}
	if !strings.Contains(err.Error(), "no such overload") {
		t.Errorf("Eval() error got %q, wanted no such overload", err)
	}
}

// TestConstantProbeNames checks the compile-time guard on probe names.
func TestConstantProbeNames(t *testing.T) {
	env := testEnv(t, cel.Variable("label", cel.StringType))
	tests := []struct {
		expr    string
		wantErr bool
	}{
		{`trace('ok', x)`, false},
		{`trace(label, x)`, true},
		{`trace('a' + 'b', x)`, true},
	}
	for _, tc := range tests {
		t.Run(tc.expr, func(t *testing.T) {
			_, iss := env.Compile(tc.expr)
			if tc.wantErr {
				if iss.Err() == nil {
					t.Fatal("Compile() succeeded, wanted a validation error")
				}
				if !strings.Contains(iss.Err().Error(), "must be a string literal") {
					t.Errorf("Compile() error got %q, wanted a literal-name error", iss.Err())
				}
				return
			}
			if iss.Err() != nil {
				t.Fatalf("Compile() failed: %v", iss.Err())
			}
		})
	}
}

// TestPoolConcurrentEvaluation is the safety claim for recording in a late
// binding. The recorder is program state, so the only thing keeping it
// race-free is that a pooled program is never checked out twice at once. Run
// with -race.
func TestPoolConcurrentEvaluation(t *testing.T) {
	env := testEnv(t)
	pool, err := NewPool(env, compile(t, env, `trace('gt', x > 1) && trace('lt', x < 1000)`))
	if err != nil {
		t.Fatalf("NewPool() failed: %v", err)
	}
	const goroutines, iterations = 16, 100
	var wg sync.WaitGroup
	errs := make(chan error, goroutines*iterations)
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				res, err := pool.Eval(map[string]any{"x": int64(g + 2)})
				if err != nil {
					errs <- err
					return
				}
				if len(res.Probes) != 2 {
					errs <- errProbeCount(len(res.Probes))
					return
				}
			}
		}(g)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent Eval() failed: %v", err)
	}
}

type errProbeCount int

func (e errProbeCount) Error() string {
	return "expected 2 probes per evaluation, got " + string(rune('0'+int(e)))
}
