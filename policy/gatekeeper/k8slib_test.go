// Copyright 2025 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package gatekeeper

import (
	"strings"
	"testing"

	"cel.dev/cel-go/cel"
)

// TestKubernetesLibraries checks the list and regex functions this package
// supplies against the behavior Kubernetes documents for them.
func TestKubernetesLibraries(t *testing.T) {
	env, err := cel.NewEnv(Libraries()...)
	if err != nil {
		t.Fatalf("cel.NewEnv() failed: %v", err)
	}
	tests := []struct {
		expr string
		want any
	}{
		{expr: `[1, 2, 3].isSorted()`, want: true},
		{expr: `[1, 3, 2].isSorted()`, want: false},
		{expr: `[].isSorted()`, want: true},
		{expr: `['a', 'b'].isSorted()`, want: true},
		{expr: `[duration('2s'), duration('1s')].isSorted()`, want: false},
		{expr: `[1, 2, 3].sum()`, want: int64(6)},
		{expr: `[].sum()`, want: int64(0)},
		{expr: `[1.5, 2.5].sum()`, want: 4.0},
		{expr: `[duration('1s'), duration('2s')].sum()`, want: "3s"},
		{expr: `[3, 1, 2].min()`, want: int64(1)},
		{expr: `[3, 1, 2].max()`, want: int64(3)},
		{expr: `['b', 'a'].min()`, want: "a"},
		{expr: `[1, 2, 3, 2].indexOf(2)`, want: int64(1)},
		{expr: `[1, 2, 3, 2].lastIndexOf(2)`, want: int64(3)},
		{expr: `[1, 2].indexOf(7)`, want: int64(-1)},
		{expr: `'abc 123'.find('[0-9]+')`, want: "123"},
		{expr: `'abc'.find('[0-9]+')`, want: ""},
		{expr: `'a1 b2 c3'.findAll('[a-z][0-9]')`, want: []any{"a1", "b2", "c3"}},
		{expr: `'a1 b2 c3'.findAll('[a-z][0-9]', 2)`, want: []any{"a1", "b2"}},
		{expr: `'abc'.findAll('[0-9]')`, want: []any{}},
		// The string extension Kubernetes enables remains available.
		{expr: `['a', 'b'].join('-')`, want: "a-b"},
	}
	for _, tc := range tests {
		t.Run(tc.expr, func(t *testing.T) {
			ast, iss := env.Compile(tc.expr)
			if iss.Err() != nil {
				t.Fatalf("Compile(%s) failed: %v", tc.expr, iss.Err())
			}
			prg, err := env.Program(ast)
			if err != nil {
				t.Fatalf("Program() failed: %v", err)
			}
			out, _, err := prg.Eval(cel.NoVars())
			if err != nil {
				t.Fatalf("Eval(%s) failed: %v", tc.expr, err)
			}
			if got := out.Value(); !equalValue(got, tc.want) {
				t.Errorf("Eval(%s) got %v (%T), wanted %v (%T)", tc.expr, got, got, tc.want, tc.want)
			}
		})
	}
}

// TestKubernetesListErrors checks the cases Kubernetes documents as errors.
func TestKubernetesListErrors(t *testing.T) {
	env, err := cel.NewEnv(Libraries()...)
	if err != nil {
		t.Fatalf("cel.NewEnv() failed: %v", err)
	}
	for _, tc := range []struct{ expr, want string }{
		{expr: `[].min()`, want: "min called on an empty list"},
		{expr: `[].max()`, want: "max called on an empty list"},
		{expr: `'a'.find('[')`, want: "invalid regular expression"},
	} {
		ast, iss := env.Compile(tc.expr)
		if iss.Err() != nil {
			t.Fatalf("Compile(%s) failed: %v", tc.expr, iss.Err())
		}
		prg, err := env.Program(ast)
		if err != nil {
			t.Fatalf("Program() failed: %v", err)
		}
		if _, _, err := prg.Eval(cel.NoVars()); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("Eval(%s) error is %v, wanted %q", tc.expr, err, tc.want)
		}
	}
}

// equalValue compares an evaluation result against the expected value, with
// durations compared by their string form.
func equalValue(got, want any) bool {
	if list, ok := want.([]any); ok {
		gotList, ok := got.([]string)
		if !ok || len(gotList) != len(list) {
			return false
		}
		for i, elem := range list {
			if gotList[i] != elem {
				return false
			}
		}
		return true
	}
	if str, ok := want.(string); ok {
		if duration, ok := got.(interface{ String() string }); ok {
			return duration.String() == str
		}
	}
	return got == want
}
