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

package ext

import (
	"math"
	"testing"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/checker"
	"github.com/google/cel-go/common/cost"
)

// TestCostShapes pins the estimates for expressions whose cost depends on the size of a value
// held inside another value.
//
// Every expression here reaches an element size through at least one operation - indexing,
// concatenation, a comprehension, a binding, or an extension function which rebuilds a container.
// Before element sizes were described recursively, most of these estimated an unbounded cost,
// which is useless to a cost limit: an unbounded estimate rejects the expression outright or
// forces the limit to be set so high that it stops protecting anything.
func TestCostShapes(t *testing.T) {
	tests := []struct {
		name string
		expr string
		want checker.CostEstimate
	}{
		// A hint for the elements of a variable reaches them through any number of steps.
		{name: "index", expr: `strs[0].contains("x")`, want: rng(2, 3)},
		{name: "map index", expr: `m["k"].contains("x")`, want: rng(2, 3)},
		{name: "nested index", expr: `nested[0][0].contains("x")`, want: rng(3, 4)},
		{name: "map of lists", expr: `mm["k"][0].contains("x")`, want: rng(3, 4)},
		{name: "dyn", expr: `dyn(strs)[0].contains("x")`, want: rng(3, 4)},

		// A value which came from one of two places is shaped like either.
		{name: "concat", expr: `(strs + strs2)[0].contains("x")`, want: rng(4, 5)},
		{name: "concat literal", expr: `(strs + ["lit"])[0].contains("x")`, want: rng(13, 14)},
		{name: "ternary", expr: `(flag ? strs : strs2)[0].contains("x")`, want: rng(3, 4)},

		// Literals describe what they hold, to any depth.
		{name: "list of variables", expr: `[strs, strs2][0][0].contains("x")`, want: rng(14, 15)},
		{name: "map of variables", expr: `{"a": strs}["a"][0].contains("x")`, want: rng(33, 34)},
		{name: "nested list literal", expr: `[["hello"], ["hi"]][0][0].contains("x")`, want: fixed(33)},

		// A comprehension holds whatever its loop step accumulated.
		{name: "map to scalar", expr: `strs.map(s, s + "!")[0].contains("x")`, want: rng(14, 70)},
		{name: "map to list", expr: `strs.map(s, [s])[0][0].contains("x")`, want: rng(14, 107)},
		{name: "map to map", expr: `strs.map(s, {"k": s})[0]["k"].contains("x")`, want: rng(14, 187)},
		{name: "filter", expr: `strs.filter(s, s != "")[0].contains("x")`, want: rng(13, 70)},
		{name: "map over nested", expr: `nested.map(l, l)[0][0].contains("x")`, want: rng(14, 54)},
		{name: "nested map", expr: `nested.map(l, l.map(s, s))[0][0].contains("x")`, want: rng(14, 243)},
		{name: "chained map", expr: `strs.map(s, s).map(s, s)[0].contains("x")`, want: rng(24, 129)},
		{name: "map over an element", expr: `nested[0].map(s, s)[0].contains("x")`, want: rng(14, 67)},
		{name: "two variable comprehension",
			expr: `m.transformMapEntry(k, v, {k: [v]})["a"][0].contains("x")`, want: rng(34, 255)},

		// A binding carries the shape of the expression it was bound to.
		{name: "bind", expr: `cel.bind(v, strs, v[0].contains("x"))`, want: rng(13, 14)},
		{name: "bind nested", expr: `cel.bind(v, nested, v[0][0].contains("x"))`, want: rng(14, 15)},

		// Extension functions which rebuild a container hold what it held.
		{name: "slice", expr: `strs.slice(0, 2)[0].contains("x")`, want: rng(15, 16)},
		{name: "reverse", expr: `strs.reverse()[0].contains("x")`, want: rng(13, 18)},
		{name: "distinct", expr: `strs.distinct()[0].contains("x")`, want: rng(13, 48)},
		{name: "sort", expr: `strs.sort()[0].contains("x")`, want: rng(13, 48)},
		{name: "sortBy", expr: `strs.sortBy(s, s)[0].contains("x")`, want: rng(36, 123)},
		{name: "flatten", expr: `nested.flatten()[0].contains("x")`, want: rng(13, 17)},
		{name: "split", expr: `strs.join(",").split(",")[0].contains("x")`, want: rng(16, 35)},
	}
	env := testShapeEnv(t)
	hints := testShapeHints()
	for _, tst := range tests {
		tc := tst
		t.Run(tc.name, func(t *testing.T) {
			ast, iss := env.Compile(tc.expr)
			if iss.Err() != nil {
				t.Fatalf("env.Compile(%s) failed: %v", tc.expr, iss.Err())
			}
			est, err := env.EstimateCost(ast, hints)
			if err != nil {
				t.Fatalf("env.EstimateCost() failed: %v", err)
			}
			if est.Max == math.MaxUint64 {
				t.Fatalf("env.EstimateCost() returned an unbounded cost for %s", tc.expr)
			}
			if est != tc.want {
				t.Errorf("env.EstimateCost() got %v, wanted %v", est, tc.want)
			}
		})
	}
}

// TestCostShapesBracketRuntime checks that the estimates derived from element shapes still
// contain the cost the expression actually incurs.
func TestCostShapesBracketRuntime(t *testing.T) {
	exprs := []string{
		`[{'x':4}, {'x':3}].sortBy(m, m['x']) == [{'x':3}, {'x':4}]`,
		`[{'x':4}, {'x':3}].map(m, m['x']).size() == 2`,
		`[[1, 1], [2, 2]].all(y, y.all(y, y == 1))`,
		`[1, 2].map(x, [x, x]).all(y, y.size() == 2)`,
		`["a,b", "c"].map(s, s.split(","))[0].size() == 2`,
		`cel.bind(v, [["hello"]], v[0].size() == 1)`,
	}
	env := testShapeEnv(t)
	hints := testShapeHints()
	for _, expr := range exprs {
		t.Run(expr, func(t *testing.T) {
			ast, iss := env.Compile(expr)
			if iss.Err() != nil {
				t.Fatalf("env.Compile() failed: %v", iss.Err())
			}
			est, err := env.EstimateCost(ast, hints)
			if err != nil {
				t.Fatalf("env.EstimateCost() failed: %v", err)
			}
			prg, err := env.Program(ast, cel.CostTracking(nil))
			if err != nil {
				t.Fatalf("env.Program() failed: %v", err)
			}
			_, det, err := prg.Eval(cel.NoVars())
			if err != nil {
				t.Fatalf("prg.Eval() failed: %v", err)
			}
			actual := *det.ActualCost()
			if actual < est.Min || actual > est.Max {
				t.Errorf("prg.Eval() cost %d outside the estimate [%d, %d]", actual, est.Min, est.Max)
			}
		})
	}
}

func testShapeEnv(t *testing.T) *cel.Env {
	t.Helper()
	env, err := cel.NewEnv(
		Bindings(), Strings(), Lists(), TwoVarComprehensions(),
		cel.Variable("strs", cel.ListType(cel.StringType)),
		cel.Variable("strs2", cel.ListType(cel.StringType)),
		cel.Variable("nested", cel.ListType(cel.ListType(cel.StringType))),
		cel.Variable("m", cel.MapType(cel.StringType, cel.StringType)),
		cel.Variable("mm", cel.MapType(cel.StringType, cel.ListType(cel.StringType))),
		cel.Variable("flag", cel.BoolType),
	)
	if err != nil {
		t.Fatalf("cel.NewEnv() failed: %v", err)
	}
	return env
}

func testShapeHints() *cost.Hints {
	return cost.NewHints(
		cost.SizeHint("strs", 4), cost.SizeHint("strs.@items", 6),
		cost.SizeHint("strs2", 4), cost.SizeHint("strs2.@items", 6),
		cost.SizeHint("nested", 3), cost.SizeHint("nested.@items", 4),
		cost.SizeHint("nested.@items.@items", 6),
		cost.SizeHint("m", 5), cost.SizeHint("m.@keys", 3), cost.SizeHint("m.@values", 6),
		cost.SizeHint("mm", 5), cost.SizeHint("mm.@values", 4),
		cost.SizeHint("mm.@values.@items", 6),
	)
}

func rng(min, max uint64) checker.CostEstimate {
	return checker.CostEstimate{Min: min, Max: max}
}

func fixed(size uint64) checker.CostEstimate {
	return checker.FixedCostEstimate(size)
}

// TestCostIndexIntoComputedValueUndercharged records a discrepancy which predates element
// shapes and is unrelated to them: indexing a value which was computed rather than named costs
// one unit less to estimate than it does to evaluate.
//
// At runtime, indexing a constructed list resolves a relative attribute, which is charged the
// cost of an identifier, and then applies a qualifier, which is charged again. Estimation charges
// only for the qualifier. Indexing a variable or a field agrees, because there the identifier is
// part of the expression and is charged by both halves.
//
// The expressions below could not be estimated at all before element shapes were recursive, so
// the gap had nowhere to show. This test pins it so that closing it is a deliberate change rather
// than a surprise.
func TestCostIndexIntoComputedValueUndercharged(t *testing.T) {
	exprs := []string{
		`["hello", "hi"][0].contains("x")`,
		`[["hello"], ["hi"]][0][0].contains("x")`,
		`(["a"] + ["bb"])[1].contains("b")`,
		`["a", "bb"].map(s, [s])[0][0].contains("a")`,
	}
	env := testShapeEnv(t)
	for _, expr := range exprs {
		t.Run(expr, func(t *testing.T) {
			ast, iss := env.Compile(expr)
			if iss.Err() != nil {
				t.Fatalf("env.Compile() failed: %v", iss.Err())
			}
			est, err := env.EstimateCost(ast, testShapeHints())
			if err != nil {
				t.Fatalf("env.EstimateCost() failed: %v", err)
			}
			prg, err := env.Program(ast, cel.CostTracking(nil))
			if err != nil {
				t.Fatalf("env.Program() failed: %v", err)
			}
			_, det, err := prg.Eval(cel.NoVars())
			if err != nil {
				t.Fatalf("prg.Eval() failed: %v", err)
			}
			if actual := *det.ActualCost(); actual != est.Max+1 {
				t.Errorf("prg.Eval() cost %d, wanted one more than the estimated maximum %d", actual, est.Max)
			}
		})
	}
}
