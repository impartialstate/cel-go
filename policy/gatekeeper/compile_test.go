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
	"context"
	"testing"
)

func TestCompileRequiredLabels(t *testing.T) {
	tmpl := compileTemplate(t, "k8srequiredlabels")
	if tmpl.Name() != "k8srequiredlabels" {
		t.Errorf("Name() got %q, wanted 'k8srequiredlabels'", tmpl.Name())
	}
	if tmpl.Description() == "" {
		t.Error("Description() got an empty string, wanted the description annotation")
	}
	if _, found := tmpl.ParamsSchema(); !found {
		t.Error("ParamsSchema() found no schema, wanted the openAPIV3Schema of the constraint")
	}
}

func TestReviewRequiredLabels(t *testing.T) {
	tmpl := compileTemplate(t, "k8srequiredlabels")
	params := map[string]any{
		"labels": []any{
			map[string]any{"key": "owner", "allowedRegex": "^[a-z]+$"},
		},
	}
	tests := []struct {
		name   string
		labels map[string]any
		want   []string
	}{
		{
			name:   "label present and well-formed",
			labels: map[string]any{"owner": "platform"},
		},
		{
			name:   "label missing",
			labels: map[string]any{"team": "platform"},
			want:   []string{"missing required label, requires all of: owner"},
		},
		{
			name:   "label value does not match",
			labels: map[string]any{"owner": "Platform-1"},
			want:   []string{"regex mismatch"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			violations, err := tmpl.Review(context.Background(), Review{
				Object: map[string]any{
					"apiVersion": "v1",
					"kind":       "Pod",
					"metadata":   map[string]any{"name": "nginx", "labels": tc.labels},
				},
				Parameters: params,
			})
			if err != nil {
				t.Fatalf("Review() failed: %v", err)
			}
			got := messages(violations)
			if len(got) != len(tc.want) {
				t.Fatalf("Review() got %v, wanted %v", got, tc.want)
			}
			for i, want := range tc.want {
				if got[i] != want {
					t.Errorf("Review() violation %d got %q, wanted %q", i, got[i], want)
				}
			}
		})
	}
}

// TestReviewDelete confirms that a DELETE request, where only oldObject is set,
// is reviewed with the same expressions as a create.
func TestReviewDelete(t *testing.T) {
	tmpl := compileTemplate(t, "k8srequiredlabels")
	violations, err := tmpl.Review(context.Background(), Review{
		OldObject: map[string]any{
			"metadata": map[string]any{"name": "nginx", "labels": map[string]any{"owner": "platform"}},
		},
		Parameters: map[string]any{
			"labels": []any{map[string]any{"key": "owner", "allowedRegex": "^[a-z]+$"}},
		},
	})
	if err != nil {
		t.Fatalf("Review() failed: %v", err)
	}
	if len(violations) != 0 {
		t.Errorf("Review() got %v, wanted no violations", messages(violations))
	}
}

func compileTemplate(t testing.TB, name string, opts ...Option) *Template {
	t.Helper()
	tmpl, err := CompileFile("testdata/"+name+"/template.yaml", opts...)
	if err != nil {
		t.Fatalf("CompileFile(%s) failed: %v", name, err)
	}
	return tmpl
}

func messages(violations []Violation) []string {
	out := make([]string, 0, len(violations))
	for _, v := range violations {
		out = append(out, v.Message)
	}
	return out
}

func TestCompileErrors(t *testing.T) {
	_, err := CompileFile("testdata/errors/template.yaml")
	if err == nil {
		t.Fatal("CompileFile() succeeded, wanted compilation errors")
	}
	want := `ERROR: testdata/errors/template.yaml:41:65: undeclared reference to 'isWellFormed' (in container '')
 |               - expression: "object.metadata.name.isWellFormed()"
 | ................................................................^
ERROR: testdata/errors/template.yaml:44:61: undeclared reference to 'variables' (in container '')
 |                 messageExpression: "'too many replicas: ' + variables.replica"
 | ............................................................^`
	if err.Error() != want {
		t.Errorf("CompileFile() got error:\n%s\nwanted:\n%s", err.Error(), want)
	}
}
