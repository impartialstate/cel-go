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
	"strings"
	"testing"

	"cel.dev/cel-go/policy"
)

// TestParamsFieldsAreChecked confirms that a policy which reads a parameter the
// constraint's schema does not declare fails to compile, reported against the
// expression which reads it.
func TestParamsFieldsAreChecked(t *testing.T) {
	tests := []struct {
		name       string
		expression string
		want       string
	}{
		{
			name:       "a misspelled parameter",
			expression: "variables.params.lables.size() > 0",
			want:       "undefined field 'lables'",
		},
		{
			name:       "a misspelled field of a parameter",
			expression: "variables.params.labels.all(entry, entry.kee != '')",
			want:       "undefined field 'kee'",
		},
		{
			name:       "a parameter compared against the wrong type",
			expression: "variables.params.message == 12",
			want:       "found no matching overload for '_==_' applied to '(string, int)'",
		},
		{
			name:       "a parameter of the wrong shape",
			expression: "variables.params.labels.startsWith('x')",
			want:       "found no matching overload for 'startsWith'",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Compile(templateWithValidation(t, tc.expression))
			if err == nil {
				t.Fatalf("Compile() succeeded, wanted %q", tc.want)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("Compile() error is:\n%s\nwanted it to report %q", err, tc.want)
			}
		})
	}
}

// TestParamsRemainNullable confirms that a policy can still compare its inputs
// against null, which is how Kubernetes reports an input a request does not
// carry, even though those inputs now carry the type of their schema.
func TestParamsRemainNullable(t *testing.T) {
	for _, expression := range []string{
		"variables.params == null || variables.params.labels.size() > 0",
		"object == null || has(object.metadata)",
		"variables.anyObject != null",
		"params.spec.parameters != null",
	} {
		if _, err := Compile(templateWithValidation(t, expression)); err != nil {
			t.Errorf("Compile(%s) failed: %v", expression, err)
		}
	}
}

// TestObjectSchemaFieldsAreChecked confirms that the schema of the reviewed
// object is checked the same way, and that a policy compiled without one keeps
// reading the object as an untyped value.
func TestObjectSchemaFieldsAreChecked(t *testing.T) {
	schema, err := ReadCRDSchema("testdata/crd/crd.yaml", "")
	if err != nil {
		t.Fatalf("ReadCRDSchema() failed: %v", err)
	}
	if _, err := CompileFile("testdata/crd/template.yaml", WithObjectSchema(schema)); err != nil {
		t.Fatalf("CompileFile() failed: %v", err)
	}
	misspelled := templateWithValidation(t, "variables.anyObject.spec.enigne == 'postgres'")
	if _, err := Compile(misspelled, WithObjectSchema(schema)); err == nil {
		t.Error("Compile() succeeded, wanted the misspelled field of the reviewed object to be reported")
	} else if !strings.Contains(err.Error(), "undefined field 'enigne'") {
		t.Errorf("Compile() error is %v, wanted it to report the undefined field", err)
	}
	// Without a schema the object is untyped, so the same expression compiles.
	if _, err := Compile(templateWithValidation(t, "variables.anyObject.spec.enigne == 'postgres'")); err != nil {
		t.Errorf("Compile() without a schema failed: %v", err)
	}
}

// TestReviewCustomResource reviews a custom resource against the policy written
// over its definition.
func TestReviewCustomResource(t *testing.T) {
	schema, err := ReadCRDSchema("testdata/crd/crd.yaml", "v1")
	if err != nil {
		t.Fatalf("ReadCRDSchema() failed: %v", err)
	}
	tmpl, err := CompileFile("testdata/crd/template.yaml", WithObjectSchema(schema))
	if err != nil {
		t.Fatalf("CompileFile() failed: %v", err)
	}
	database := func(engine string, replicas int, backups bool) map[string]any {
		return map[string]any{
			"apiVersion": "example.com/v1",
			"kind":       "Database",
			"metadata":   map[string]any{"name": "orders", "namespace": "prod"},
			"spec": map[string]any{
				"engine":   engine,
				"replicas": replicas,
				"backup":   map[string]any{"enabled": backups},
			},
		}
	}
	params := map[string]any{"minReplicas": 3, "exemptEngines": []any{"mysql"}}
	tests := []struct {
		name   string
		object map[string]any
		want   []string
	}{
		{
			name:   "backups enabled and enough replicas",
			object: database("postgres", 3, true),
		},
		{
			name:   "no backups",
			object: database("postgres", 3, false),
			want:   []string{"database orders runs postgres without backups enabled"},
		},
		{
			name:   "an exempt engine needs no backups",
			object: database("mysql", 3, false),
		},
		{
			name:   "too few replicas",
			object: database("postgres", 1, true),
			want:   []string{"database needs at least 3 replicas"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			violations, err := tmpl.Review(context.Background(), Review{Object: tc.object, Parameters: params})
			if err != nil {
				t.Fatalf("Review() failed: %v", err)
			}
			if got := messages(violations); strings.Join(got, "; ") != strings.Join(tc.want, "; ") {
				t.Errorf("Review() got %v, wanted %v", got, tc.want)
			}
		})
	}
}

func TestReadCRDSchema(t *testing.T) {
	storage, err := ReadCRDSchema("testdata/crd/crd.yaml", "")
	if err != nil {
		t.Fatalf("ReadCRDSchema() failed: %v", err)
	}
	properties, _ := storage["properties"].(map[string]any)
	spec, _ := properties["spec"].(map[string]any)
	specProperties, _ := spec["properties"].(map[string]any)
	if _, found := specProperties["replicas"]; !found {
		t.Errorf("ReadCRDSchema() read a version without replicas, wanted the stored version")
	}
	older, err := ReadCRDSchema("testdata/crd/crd.yaml", "v1alpha1")
	if err != nil {
		t.Fatalf("ReadCRDSchema(v1alpha1) failed: %v", err)
	}
	properties, _ = older["properties"].(map[string]any)
	spec, _ = properties["spec"].(map[string]any)
	specProperties, _ = spec["properties"].(map[string]any)
	if _, found := specProperties["replicas"]; found {
		t.Errorf("ReadCRDSchema(v1alpha1) read the wrong version")
	}
	if _, err := ReadCRDSchema("testdata/crd/crd.yaml", "v2"); err == nil ||
		!strings.Contains(err.Error(), "v1alpha1, v1") {
		t.Errorf("ReadCRDSchema(v2) error is %v, wanted it to name the versions declared", err)
	}
}

// templateWithValidation builds a ConstraintTemplate whose single validation is
// the given expression, over the parameters of the required-labels policy.
func templateWithValidation(t testing.TB, expression string) *policy.Source {
	t.Helper()
	return policy.ByteSource([]byte(`apiVersion: templates.gatekeeper.sh/v1
kind: ConstraintTemplate
metadata:
  name: expressiontest
spec:
  crd:
    spec:
      names:
        kind: ExpressionTest
      validation:
        openAPIV3Schema:
          type: object
          properties:
            message:
              type: string
            labels:
              type: array
              items:
                type: object
                properties:
                  key:
                    type: string
  targets:
    - target: admission.k8s.gatekeeper.sh
      code:
        - engine: K8sNativeValidation
          source:
            validations:
              - expression: |-
                  `+expression+`
                message: "violation"
`), "template.yaml")
}
