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

package cel

// Benchmarks for qualified attribute resolution.
//
// These measure the cost of resolving `a.b.c`-style attribute paths along the
// dimensions the evaluation plan can actually control:
//
//	Depth       how many qualifiers hang off the root identifier
//	Repr        how the caller represents the data (native Go, proto, ref.Val)
//	Result      whether the expression yields a scalar or a container
//	Qualifier   field select vs. string key vs. integer index
//	Activation  how variable names are resolved on each Eval
//
// Run:
//
//	go test ./cel/ -bench=BenchmarkAttr -benchmem -run=XXX
//
// Findings, measured on a 4-core Xeon @ 2.1GHz, go1.24.7. Treat the absolute
// numbers as machine-specific; the ratios are the durable part.
//
//  1. Resolution is dominated by string hashing and by wrapping results.
//     A bare identifier costs ~43ns, and each additional qualifier on a
//     map[string]any adds ~15ns, all of it map access.
//
//  2. Returning a container costs an allocation; returning a scalar does not.
//     `m.k0.k0` -> int64 is 71ns/0 allocs, `m.k0` -> map is 131ns/88B/2 allocs,
//     because the result map has to be wrapped in a ref.Val.
//
//  3. Handing over values that are already ref.Val is a pessimization
//     (188ns vs 72ns). Native Go types hit the specialized arms of
//     stringQualifier.qualifyInternal; a ref.Val falls through to refQualify,
//     which re-wraps every intermediate.
//
//  4. Only the arms that qualifyInternal enumerates are fast. map[string]any
//     is 72ns; map[string]map[string]int64 has no arm and costs 492ns/7 allocs
//     via the reflection fallback. Position within the switch does not matter
//     (Go searches type hashes) -- see BenchmarkAttrSwitchArm.
//
//  5. Proto field access is not free. It routes through protoreflect
//     (FieldDescription.GetFrom -> pbRef.Get), not a struct offset: 161ns for
//     two field selects against 72ns for two map keys.
//
//  6. The activation is the one place plan-time information pays off directly.
//     A checked AST names every variable the expression can reference, so the
//     plan can bind them to slots and skip both the name hash and the pooled
//     activation: 74ns -> 56ns for `m.k0.k0`, and 46ns -> 22ns for a bare
//     identifier. Crucially the win is *pruning*, not the data structure --
//     see BenchmarkAttrActivationScale, where a slot table built from the
//     declarations degrades to 107ns at 32 variables while one pruned by the
//     reference map stays flat at ~22ns.
//
//  7. Fusing an operator with its attribute removes a boxing step. The default
//     plan for `attr == const` resolves the attribute, wraps it in a ref.Val,
//     and immediately unwraps it in types.Equal. A fused node that compares
//     native Go values runs 105ns/16B/1alloc -> 73ns/0allocs for a string leaf
//     and 82ns -> 71ns for an int. The string allocation is the types.String
//     boxing; an int64 small enough for the runtime's cache never allocated.
//     Fused, `attr == const` costs about what resolving the attribute alone
//     costs -- the comparison becomes free. The price is observability: the
//     fused node absorbs an id, so state tracking sees one fewer node. See
//     TestFuseAttrEqualsLosesObservability.

import (
	"fmt"
	"reflect"
	"sort"
	"testing"

	"github.com/google/cel-go/common/operators"
	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
	"github.com/google/cel-go/interpreter"

	proto3pb "cel.dev/expr/conformance/proto3"
)

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

const fanout = 8

type namedMap map[string]int64

func benchKey(i int) string { return fmt.Sprintf("k%d", i) }

// nestedAny builds depth levels of map[string]any, with int64 leaves.
func nestedAny(depth int) map[string]any {
	if depth <= 1 {
		leaf := make(map[string]any, fanout)
		for i := 0; i < fanout; i++ {
			leaf[benchKey(i)] = int64(i)
		}
		return leaf
	}
	m := make(map[string]any, fanout)
	for i := 0; i < fanout; i++ {
		m[benchKey(i)] = nestedAny(depth - 1)
	}
	return m
}

// nestedTyped is the same shape with concrete value types all the way down, so
// qualification never passes through an `any`.
func nestedTyped() map[string]map[string]int64 {
	m := make(map[string]map[string]int64, fanout)
	for i := 0; i < fanout; i++ {
		leaf := make(map[string]int64, fanout)
		for j := 0; j < fanout; j++ {
			leaf[benchKey(j)] = int64(j)
		}
		m[benchKey(i)] = leaf
	}
	return m
}

func nestedProto(depth int) *proto3pb.NestedTestAllTypes {
	msg := &proto3pb.NestedTestAllTypes{
		Payload: &proto3pb.TestAllTypes{SingleInt64: 42},
	}
	for i := 0; i < depth; i++ {
		msg = &proto3pb.NestedTestAllTypes{Child: msg}
	}
	return msg
}

var (
	mapAnyType   = MapType(StringType, DynType)
	mapTypedType = MapType(StringType, MapType(StringType, IntType))
	listIntType  = ListType(IntType)
)

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

type benchCase struct {
	name  string
	expr  string
	decls []EnvOption
	vars  any // map[string]any or an Activation
}

// compile builds a program and asserts it evaluates cleanly before timing.
func compileBench(tb testing.TB, expr string, decls []EnvOption, vars any) Program {
	tb.Helper()
	env, err := NewEnv(decls...)
	if err != nil {
		tb.Fatalf("NewEnv() failed: %v", err)
	}
	ast, iss := env.Compile(expr)
	if iss.Err() != nil {
		tb.Fatalf("Compile(%q) failed: %v", expr, iss.Err())
	}
	if !ast.IsChecked() {
		tb.Fatalf("Compile(%q) produced an unchecked AST", expr)
	}
	prg, err := env.Program(ast)
	if err != nil {
		tb.Fatalf("Program() failed: %v", err)
	}
	if _, _, err := prg.Eval(vars); err != nil {
		tb.Fatalf("Eval(%q) failed: %v", expr, err)
	}
	return prg
}

func runCases(b *testing.B, cases []benchCase) {
	b.Helper()
	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			prg := compileBench(b, tc.expr, tc.decls, tc.vars)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, _, err := prg.Eval(tc.vars); err != nil {
					b.Fatalf("Eval() failed: %v", err)
				}
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Depth: cost per additional qualifier
// ---------------------------------------------------------------------------

// BenchmarkAttrDepth isolates the cost of one more qualifier. Every case
// resolves all the way to an int64 leaf, so the result is always a scalar and
// the difference between rows is qualification alone, not result wrapping.
func BenchmarkAttrDepth(b *testing.B) {
	decls := []EnvOption{Variable("m", mapAnyType)}
	runCases(b, []benchCase{
		{"0/ident", "x", []EnvOption{Variable("x", IntType)}, map[string]any{"x": int64(7)}},
		{"1/qualifier", "m.k0", decls, map[string]any{"m": nestedAny(1)}},
		{"2/qualifiers", "m.k0.k0", decls, map[string]any{"m": nestedAny(2)}},
		{"3/qualifiers", "m.k0.k0.k0", decls, map[string]any{"m": nestedAny(3)}},
		{"4/qualifiers", "m.k0.k0.k0.k0", decls, map[string]any{"m": nestedAny(4)}},
	})
}

// ---------------------------------------------------------------------------
// Representation: how the caller hands data to the evaluator
//
// The specialized arms of stringQualifier.qualifyInternal only fire for native
// Go types. A value that is already a ref.Val falls through to refQualify.
// ---------------------------------------------------------------------------

func BenchmarkAttrRepr(b *testing.B) {
	reg, err := types.NewRegistry()
	if err != nil {
		b.Fatalf("NewRegistry() failed: %v", err)
	}
	preconverted := reg.NativeToValue(nestedAny(2))

	runCases(b, []benchCase{
		{
			"native/map[string]any",
			"m.k0.k0",
			[]EnvOption{Variable("m", mapAnyType)},
			map[string]any{"m": nestedAny(2)},
		},
		{
			"native/map[string]map[string]int64 (no specialized arm)",
			"m.k0.k0",
			[]EnvOption{Variable("m", mapTypedType)},
			map[string]any{"m": nestedTyped()},
		},
		{
			"refval/preconverted",
			"m.k0.k0",
			[]EnvOption{Variable("m", mapAnyType)},
			map[string]any{"m": preconverted},
		},
		{
			"proto/field-select",
			"n.child.child.payload.single_int64",
			[]EnvOption{Variable("n", ObjectType("cel.expr.conformance.proto3.NestedTestAllTypes")),
				Types(&proto3pb.TestAllTypes{})},
			map[string]any{"n": nestedProto(2)},
		},
	})
}

// ---------------------------------------------------------------------------
// Result shape: a container result must be wrapped in a ref.Val, a scalar
// leaf usually need not be.
// ---------------------------------------------------------------------------

func BenchmarkAttrResultShape(b *testing.B) {
	vars := map[string]any{"m": nestedAny(2)}
	decls := []EnvOption{Variable("m", mapAnyType)}
	runCases(b, []benchCase{
		{"scalar-leaf", "m.k0.k0", decls, vars},
		{"container-leaf", "m.k0", decls, vars},
	})
}

// ---------------------------------------------------------------------------
// Qualifier kind
// ---------------------------------------------------------------------------

func BenchmarkAttrQualifierKind(b *testing.B) {
	runCases(b, []benchCase{
		{
			"string-key",
			"m.k0",
			[]EnvOption{Variable("m", MapType(StringType, IntType))},
			map[string]any{"m": map[string]int64{"k0": 1, "k1": 2}},
		},
		{
			"int-index",
			"l[3]",
			[]EnvOption{Variable("l", listIntType)},
			map[string]any{"l": []int64{0, 1, 2, 3, 4, 5, 6, 7}},
		},
		{
			"proto-field",
			"n.payload.single_int64",
			[]EnvOption{Variable("n", ObjectType("cel.expr.conformance.proto3.NestedTestAllTypes")),
				Types(&proto3pb.TestAllTypes{})},
			map[string]any{"n": nestedProto(0)},
		},
	})
}

// ---------------------------------------------------------------------------
// Type-switch arm position
//
// qualifyInternal dispatches over ~11 concrete map types. Go compiles a
// concrete-type switch to a search over type hashes rather than a linear scan,
// so an arm's position in the source should not matter. This benchmark exists
// to keep that assumption honest: if these diverge, per-arm ordering (or an
// inline type cache) becomes worth considering.
// ---------------------------------------------------------------------------

func BenchmarkAttrSwitchArm(b *testing.B) {
	decls := []EnvOption{Variable("m", MapType(StringType, DynType))}
	runCases(b, []benchCase{
		{"first-arm/map[string]any", "m.k0", decls,
			map[string]any{"m": map[string]any{"k0": int64(1)}}},
		{"last-arm/map[string]bool", "m.k0", decls,
			map[string]any{"m": map[string]bool{"k0": true}}},
		{"default-arm/reflection", "m.k0", decls,
			map[string]any{"m": namedMap{"k0": 1}}},
	})
}

// ---------------------------------------------------------------------------
// Activation strategy
//
// Every Eval resolves the root identifier of each attribute by name. The
// default path hashes the name into a map[string]any and rents an activation
// from a sync.Pool. A checked AST already names every variable the expression
// can reference, so the plan can bind them to fixed slots ahead of time and
// resolve by comparing interned names instead.
// ---------------------------------------------------------------------------

// referencedVars returns the variable names a checked AST resolves, in a
// stable order. Function references carry overload IDs and are skipped;
// constant-folded identifiers carry a Value and never reach an activation.
func referencedVars(a *Ast) []string {
	seen := map[string]bool{}
	names := []string{}
	for _, ref := range a.NativeRep().ReferenceMap() {
		if len(ref.OverloadIDs) != 0 || ref.Value != nil || ref.Name == "" {
			continue
		}
		if !seen[ref.Name] {
			seen[ref.Name] = true
			names = append(names, ref.Name)
		}
	}
	sort.Strings(names)
	return names
}

// slotActivation resolves names by scanning the slots the plan declared. For a
// handful of variables a few string compares beat one map hash, and it holds no
// pooled state, so Eval skips the sync.Pool round trip entirely.
type slotActivation struct {
	names []string
	vals  []any
}

func (a *slotActivation) Parent() Activation { return nil }

func (a *slotActivation) ResolveName(name string) (any, bool) {
	for i, n := range a.names {
		if n == name {
			return a.vals[i], true
		}
	}
	return nil, false
}

// singleSlotActivation is the degenerate case: one variable, one compare.
type singleSlotActivation struct {
	name string
	val  any
}

func (a *singleSlotActivation) Parent() Activation { return nil }

func (a *singleSlotActivation) ResolveName(name string) (any, bool) {
	if name == a.name {
		return a.val, true
	}
	return nil, false
}

// newSlotActivation binds vars to the slots a checked AST declares, dropping
// any binding the expression cannot reference.
func newSlotActivation(a *Ast, vars map[string]any) *slotActivation {
	names := referencedVars(a)
	act := &slotActivation{
		names: make([]string, 0, len(names)),
		vals:  make([]any, 0, len(names)),
	}
	for _, n := range names {
		if v, found := vars[n]; found {
			act.names = append(act.names, n)
			act.vals = append(act.vals, v)
		}
	}
	return act
}

func BenchmarkAttrActivation(b *testing.B) {
	expr := "m.k0.k0"
	decls := []EnvOption{Variable("m", mapAnyType)}
	vars := map[string]any{"m": nestedAny(2)}

	env, err := NewEnv(decls...)
	if err != nil {
		b.Fatalf("NewEnv() failed: %v", err)
	}
	ast, iss := env.Compile(expr)
	if iss.Err() != nil {
		b.Fatalf("Compile(%q) failed: %v", expr, iss.Err())
	}
	prg, err := env.Program(ast)
	if err != nil {
		b.Fatalf("Program() failed: %v", err)
	}

	if got := referencedVars(ast); len(got) != 1 || got[0] != "m" {
		b.Fatalf("referencedVars() got %v, wanted [m]", got)
	}
	mapAct, err := NewActivation(vars)
	if err != nil {
		b.Fatalf("NewActivation() failed: %v", err)
	}

	inputs := []struct {
		name  string
		input any
	}{
		{"map-input/pooled", vars},
		{"prebuilt-map-activation", mapAct},
		{"planned-slots", newSlotActivation(ast, vars)},
		{"planned-single-slot", &singleSlotActivation{name: "m", val: vars["m"]}},
	}
	for _, tc := range inputs {
		b.Run(tc.name, func(b *testing.B) {
			if _, _, err := prg.Eval(tc.input); err != nil {
				b.Fatalf("Eval() failed: %v", err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, _, err := prg.Eval(tc.input); err != nil {
					b.Fatalf("Eval() failed: %v", err)
				}
			}
		})
	}
}

// BenchmarkAttrActivationScale finds the point where hashing a name beats
// scanning the plan's slots.
func BenchmarkAttrActivationScale(b *testing.B) {
	for _, n := range []int{1, 2, 4, 8, 16, 32} {
		decls := make([]EnvOption, 0, n)
		vars := make(map[string]any, n)
		for i := 0; i < n; i++ {
			decls = append(decls, Variable(fmt.Sprintf("v%02d", i), IntType))
			vars[fmt.Sprintf("v%02d", i)] = int64(i)
		}
		// Reference the last-declared variable: the worst case for a scan.
		expr := fmt.Sprintf("v%02d", n-1)

		env, err := NewEnv(decls...)
		if err != nil {
			b.Fatalf("NewEnv() failed: %v", err)
		}
		ast, iss := env.Compile(expr)
		if iss.Err() != nil {
			b.Fatalf("Compile(%q) failed: %v", expr, iss.Err())
		}
		prg, err := env.Program(ast)
		if err != nil {
			b.Fatalf("Program() failed: %v", err)
		}

		// The plan only ever references one variable, so its slot table holds
		// one entry no matter how many bindings the caller supplies.
		planned := newSlotActivation(ast, vars)
		// A slot table built from the declarations rather than the plan shows
		// what the scan costs without the reference map to prune it.
		unpruned := &slotActivation{}
		for i := 0; i < n; i++ {
			unpruned.names = append(unpruned.names, fmt.Sprintf("v%02d", i))
			unpruned.vals = append(unpruned.vals, int64(i))
		}

		b.Run(fmt.Sprintf("vars=%02d/map-input", n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				prg.Eval(vars)
			}
		})
		b.Run(fmt.Sprintf("vars=%02d/slots-pruned-by-plan", n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				prg.Eval(planned)
			}
		})
		b.Run(fmt.Sprintf("vars=%02d/slots-unpruned", n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				prg.Eval(unpruned)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Correctness of the benchmark helpers
// ---------------------------------------------------------------------------

func TestReferencedVars(t *testing.T) {
	tests := []struct {
		expr  string
		decls []EnvOption
		want  []string
	}{
		{"x", []EnvOption{Variable("x", IntType)}, []string{"x"}},
		{"x + y", []EnvOption{Variable("x", IntType), Variable("y", IntType)}, []string{"x", "y"}},
		{"x + x", []EnvOption{Variable("x", IntType)}, []string{"x"}},
		{"m.k0.k0", []EnvOption{Variable("m", mapAnyType)}, []string{"m"}},
		// Functions carry overload IDs and must not be mistaken for variables.
		{"size(l) > 0", []EnvOption{Variable("l", listIntType)}, []string{"l"}},
		// A binding the expression never mentions is not a referenced variable.
		{"x", []EnvOption{Variable("x", IntType), Variable("unused", IntType)}, []string{"x"}},
	}
	for _, tc := range tests {
		t.Run(tc.expr, func(t *testing.T) {
			env, err := NewEnv(tc.decls...)
			if err != nil {
				t.Fatalf("NewEnv() failed: %v", err)
			}
			ast, iss := env.Compile(tc.expr)
			if iss.Err() != nil {
				t.Fatalf("Compile(%q) failed: %v", tc.expr, iss.Err())
			}
			got := referencedVars(ast)
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("referencedVars(%q) got %v, wanted %v", tc.expr, got, tc.want)
			}
		})
	}
}

// TestSlotActivationMatchesMap asserts the slot activations are drop-in
// equivalents: every expression must produce the same value through a slot
// activation as through the default map input.
func TestSlotActivationMatchesMap(t *testing.T) {
	tests := []struct {
		expr  string
		decls []EnvOption
		vars  map[string]any
	}{
		{"x", []EnvOption{Variable("x", IntType)}, map[string]any{"x": int64(7)}},
		{"x + y", []EnvOption{Variable("x", IntType), Variable("y", IntType)},
			map[string]any{"x": int64(7), "y": int64(5)}},
		{"m.k0.k0", []EnvOption{Variable("m", mapAnyType)}, map[string]any{"m": nestedAny(2)}},
		{"m.k0", []EnvOption{Variable("m", mapAnyType)}, map[string]any{"m": nestedAny(2)}},
		{"l[3]", []EnvOption{Variable("l", listIntType)}, map[string]any{"l": []int64{0, 1, 2, 3}}},
		{"has(m.k0)", []EnvOption{Variable("m", mapAnyType)}, map[string]any{"m": nestedAny(2)}},
	}
	for _, tc := range tests {
		t.Run(tc.expr, func(t *testing.T) {
			env, err := NewEnv(tc.decls...)
			if err != nil {
				t.Fatalf("NewEnv() failed: %v", err)
			}
			ast, iss := env.Compile(tc.expr)
			if iss.Err() != nil {
				t.Fatalf("Compile(%q) failed: %v", tc.expr, iss.Err())
			}
			prg, err := env.Program(ast)
			if err != nil {
				t.Fatalf("Program() failed: %v", err)
			}
			want, _, err := prg.Eval(tc.vars)
			if err != nil {
				t.Fatalf("Eval(map) failed: %v", err)
			}
			got, _, err := prg.Eval(newSlotActivation(ast, tc.vars))
			if err != nil {
				t.Fatalf("Eval(slots) failed: %v", err)
			}
			if got.Equal(want) != types.True {
				t.Errorf("Eval(slots) got %v, wanted %v", got, want)
			}
		})
	}
}

// TestSlotActivationMissingVar confirms an unbound name reports as missing
// rather than resolving to a neighbouring slot.
func TestSlotActivationMissingVar(t *testing.T) {
	env, err := NewEnv(Variable("x", IntType), Variable("y", IntType))
	if err != nil {
		t.Fatalf("NewEnv() failed: %v", err)
	}
	ast, iss := env.Compile("x + y")
	if iss.Err() != nil {
		t.Fatalf("Compile() failed: %v", iss.Err())
	}
	prg, err := env.Program(ast)
	if err != nil {
		t.Fatalf("Program() failed: %v", err)
	}
	// Bind only x; y has no slot.
	act := newSlotActivation(ast, map[string]any{"x": int64(1)})
	if _, _, err := prg.Eval(act); err == nil {
		t.Fatal("Eval() succeeded with an unbound variable, wanted an error")
	}
}

// ---------------------------------------------------------------------------
// Operator fusion
//
// The default plan for `attr == const` is three nodes: evalEq drives an
// evalAttr, which resolves the attribute and then wraps the native Go value in
// a ref.Val via NativeToValue, only for types.Equal to unwrap it again.
//
// InterpretableAttribute exposes Resolve, which hands back the native value
// before that wrapping. When the checker has already fixed the comparison type
// -- a primitive constant on one side pins it, since CEL equality is
// homogeneous -- the plan can fuse the three nodes into one that compares
// native Go values and never builds an intermediate ref.Val.
//
// This is a superinstruction: it removes a boxing step and two interface
// dispatches per evaluation. It is a benchmark fixture, not a proposed API.
// ---------------------------------------------------------------------------

// evalAttrEqString compares a resolved attribute against a string constant
// without boxing the attribute value.
type evalAttrEqString struct {
	id      int64
	attr    interpreter.InterpretableAttribute
	val     string
	generic interpreter.Interpretable
}

func (e *evalAttrEqString) ID() int64 { return e.id }

func (e *evalAttrEqString) Eval(ctx interpreter.Activation) ref.Val {
	v, err := e.attr.Resolve(ctx)
	if err != nil {
		return types.LabelErrNode(e.id, types.WrapErr(err))
	}
	if s, ok := v.(string); ok {
		if s == e.val {
			return types.True
		}
		return types.False
	}
	// Anything else -- an alias type, an unknown, a ref.Val supplied by the
	// activation -- keeps the semantics of the unfused plan.
	return e.generic.Eval(ctx)
}

// evalAttrEqInt is the same fusion for int64 constants.
type evalAttrEqInt struct {
	id      int64
	attr    interpreter.InterpretableAttribute
	val     int64
	generic interpreter.Interpretable
}

func (e *evalAttrEqInt) ID() int64 { return e.id }

func (e *evalAttrEqInt) Eval(ctx interpreter.Activation) ref.Val {
	v, err := e.attr.Resolve(ctx)
	if err != nil {
		return types.LabelErrNode(e.id, types.WrapErr(err))
	}
	if i, ok := v.(int64); ok {
		if i == e.val {
			return types.True
		}
		return types.False
	}
	return e.generic.Eval(ctx)
}

// fuseAttrEquals rewrites `attr == <primitive const>` into a fused node. The
// constant's type is what pins the comparison: CEL equality is homogeneous, so
// a string constant on one side means the checker proved both sides are
// strings.
func fuseAttrEquals(i interpreter.Interpretable) (interpreter.Interpretable, error) {
	call, ok := i.(interpreter.InterpretableCall)
	if !ok || call.Function() != operators.Equals {
		return i, nil
	}
	args := call.Args()
	if len(args) != 2 {
		return i, nil
	}
	// Accept the constant on either side; equality is commutative.
	attr, isAttr := args[0].(interpreter.InterpretableAttribute)
	konst, isConst := args[1].(interpreter.InterpretableConst)
	if !isAttr || !isConst {
		attr, isAttr = args[1].(interpreter.InterpretableAttribute)
		konst, isConst = args[0].(interpreter.InterpretableConst)
	}
	if !isAttr || !isConst {
		return i, nil
	}
	switch v := konst.Value().(type) {
	case types.String:
		return &evalAttrEqString{id: call.ID(), attr: attr, val: string(v), generic: i}, nil
	case types.Int:
		return &evalAttrEqInt{id: call.ID(), attr: attr, val: int64(v), generic: i}, nil
	}
	return i, nil
}

func BenchmarkAttrFusedEquals(b *testing.B) {
	strLeaf := map[string]any{"m": map[string]any{"k0": map[string]any{"k0": "admin"}}}
	intLeaf := map[string]any{"m": nestedAny(2)}

	cases := []struct {
		name string
		expr string
		vars map[string]any
		want bool
	}{
		{"string/match", `m.k0.k0 == "admin"`, strLeaf, true},
		{"string/no-match", `m.k0.k0 == "viewer"`, strLeaf, false},
		{"int/match", "m.k0.k0 == 0", intLeaf, true},
	}
	for _, tc := range cases {
		for _, fused := range []bool{false, true} {
			label := tc.name + "/unfused"
			if fused {
				label = tc.name + "/fused"
			}
			b.Run(label, func(b *testing.B) {
				env, err := NewEnv(Variable("m", mapAnyType))
				if err != nil {
					b.Fatalf("NewEnv() failed: %v", err)
				}
				ast, iss := env.Compile(tc.expr)
				if iss.Err() != nil {
					b.Fatalf("Compile(%q) failed: %v", tc.expr, iss.Err())
				}
				opts := []ProgramOption{}
				if fused {
					opts = append(opts, CustomDecorator(fuseAttrEquals))
				}
				prg, err := env.Program(ast, opts...)
				if err != nil {
					b.Fatalf("Program() failed: %v", err)
				}
				out, _, err := prg.Eval(tc.vars)
				if err != nil {
					b.Fatalf("Eval() failed: %v", err)
				}
				if out != types.Bool(tc.want) {
					b.Fatalf("Eval(%q) got %v, wanted %v", tc.expr, out, tc.want)
				}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					prg.Eval(tc.vars)
				}
			})
		}
	}
}

// TestFuseAttrEqualsMatchesUnfused is the differential test for the fusion
// above: for every input the fused plan must agree with the default plan, on
// values and on errors alike. A plan rewrite that is faster but disagrees is
// not an optimization.
func TestFuseAttrEqualsMatchesUnfused(t *testing.T) {
	type namedString string

	reg, err := types.NewRegistry()
	if err != nil {
		t.Fatalf("NewRegistry() failed: %v", err)
	}

	tests := []struct {
		name string
		expr string
		vars map[string]any
	}{
		{"string-match", `m.k0 == "admin"`, map[string]any{"m": map[string]any{"k0": "admin"}}},
		{"string-no-match", `m.k0 == "admin"`, map[string]any{"m": map[string]any{"k0": "viewer"}}},
		{"const-on-lhs", `"admin" == m.k0`, map[string]any{"m": map[string]any{"k0": "admin"}}},
		{"int-match", "m.k0 == 42", map[string]any{"m": map[string]any{"k0": int64(42)}}},
		{"int-no-match", "m.k0 == 42", map[string]any{"m": map[string]any{"k0": int64(7)}}},
		// Heterogeneous comparison: the leaf is not the constant's type.
		{"type-mismatch", `m.k0 == "admin"`, map[string]any{"m": map[string]any{"k0": int64(42)}}},
		// The native value is not a plain Go string, so the fused fast path
		// must decline and defer to the generic comparison.
		{"named-string-leaf", `m.k0 == "admin"`, map[string]any{"m": map[string]any{"k0": namedString("admin")}}},
		{"refval-leaf", `m.k0 == "admin"`, map[string]any{"m": map[string]any{"k0": reg.NativeToValue("admin")}}},
		// Error paths.
		{"missing-key", `m.absent == "admin"`, map[string]any{"m": map[string]any{"k0": "admin"}}},
		{"missing-root", `m.k0 == "admin"`, map[string]any{}},
		// Typed map rather than map[string]any.
		{"typed-map", `m.k0 == "admin"`, map[string]any{"m": map[string]string{"k0": "admin"}}},
		// A large int, which does not fit the runtime's small-value cache.
		{"large-int", "m.k0 == 1000000", map[string]any{"m": map[string]any{"k0": int64(1000000)}}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			env, err := NewEnv(Variable("m", mapAnyType))
			if err != nil {
				t.Fatalf("NewEnv() failed: %v", err)
			}
			ast, iss := env.Compile(tc.expr)
			if iss.Err() != nil {
				t.Fatalf("Compile(%q) failed: %v", tc.expr, iss.Err())
			}
			plain, err := env.Program(ast)
			if err != nil {
				t.Fatalf("Program() failed: %v", err)
			}
			fused, err := env.Program(ast, CustomDecorator(fuseAttrEquals))
			if err != nil {
				t.Fatalf("Program(fused) failed: %v", err)
			}

			wantVal, _, wantErr := plain.Eval(tc.vars)
			gotVal, _, gotErr := fused.Eval(tc.vars)

			if (wantErr == nil) != (gotErr == nil) {
				t.Fatalf("Eval() error mismatch: unfused %v, fused %v", wantErr, gotErr)
			}
			if wantErr != nil {
				if gotErr.Error() != wantErr.Error() {
					t.Errorf("Eval() error got %q, wanted %q", gotErr, wantErr)
				}
				return
			}
			if gotVal.Equal(wantVal) != types.True {
				t.Errorf("Eval() got %v (%T), wanted %v (%T)", gotVal, gotVal, wantVal, wantVal)
			}
		})
	}
}

// TestFuseAttrEqualsLosesObservability records the cost of collapsing nodes.
// Fusing `attr == const` removes the intermediate attribute node from the
// plan, so an observer -- state tracking, exhaustive eval, cost tracking --
// sees one fewer id. Any real fusion pass has to either decline to fuse when
// observers are installed, or report the ids it absorbed.
func TestFuseAttrEqualsLosesObservability(t *testing.T) {
	env, err := NewEnv(Variable("m", mapAnyType))
	if err != nil {
		t.Fatalf("NewEnv() failed: %v", err)
	}
	ast, iss := env.Compile(`m.k0 == "admin"`)
	if iss.Err() != nil {
		t.Fatalf("Compile() failed: %v", iss.Err())
	}
	vars := map[string]any{"m": map[string]any{"k0": "admin"}}

	trackedIDs := func(opts ...ProgramOption) int {
		t.Helper()
		prg, err := env.Program(ast, append([]ProgramOption{EvalOptions(OptTrackState)}, opts...)...)
		if err != nil {
			t.Fatalf("Program() failed: %v", err)
		}
		out, det, err := prg.Eval(vars)
		if err != nil {
			t.Fatalf("Eval() failed: %v", err)
		}
		if out != types.True {
			t.Fatalf("Eval() got %v, wanted true", out)
		}
		if det == nil || det.State() == nil {
			t.Fatal("Eval() returned no eval state")
		}
		return len(det.State().IDs())
	}

	plain := trackedIDs()
	fused := trackedIDs(CustomDecorator(fuseAttrEquals))
	if fused >= plain {
		t.Errorf("fused plan tracked %d ids, wanted fewer than the unfused %d; if fusion "+
			"has become observability-preserving, update this test and the note above", fused, plain)
	}
}
