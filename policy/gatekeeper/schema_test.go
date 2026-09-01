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

	"github.com/google/cel-go/cel"

	"go.yaml.in/yaml/v3"
)

func schemaOf(t testing.TB, source string) map[string]any {
	t.Helper()
	schema := map[string]any{}
	if err := yaml.Unmarshal([]byte(source), &schema); err != nil {
		t.Fatalf("yaml.Unmarshal() failed: %v", err)
	}
	return schema
}

func TestSchemaTypes(t *testing.T) {
	tests := []struct {
		name   string
		schema string
		want   *cel.Type
	}{
		{
			name:   "a scalar property",
			schema: "type: string",
			want:   cel.StringType,
		},
		{
			name:   "an array of strings",
			schema: "{type: array, items: {type: string}}",
			want:   cel.ListType(cel.StringType),
		},
		{
			name: "an object with properties is a type whose fields are checked",
			schema: `
type: object
properties:
  key: {type: string}
  allowedRegex: {type: string}`,
			want: cel.ObjectType("test.Params"),
		},
		{
			name: "an object which keeps undeclared fields is a map",
			schema: `
type: object
x-kubernetes-preserve-unknown-fields: true
properties:
  key: {type: string}`,
			want: cel.MapType(cel.StringType, cel.DynType),
		},
		{
			name:   "an object with no properties is a map",
			schema: "type: object",
			want:   cel.MapType(cel.StringType, cel.DynType),
		},
		{
			name:   "additionalProperties gives the value type",
			schema: "{type: object, additionalProperties: {type: integer}}",
			want:   cel.MapType(cel.StringType, cel.IntType),
		},
		{
			name: "a list of objects",
			schema: `
type: array
items:
  type: object
  properties:
    min: {type: integer}
    max: {type: integer}`,
			want: cel.ListType(cel.ObjectType("test.Params.item")),
		},
		{
			name:   "a value which may be an integer or a string is untyped",
			schema: "{type: string, x-kubernetes-int-or-string: true}",
			want:   cel.DynType,
		},
		{
			name:   "a schema with no type is untyped",
			schema: "description: anything",
			want:   cel.DynType,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := NewSchemaTypes(nil).Declare("test.Params", schemaOf(t, tc.schema))
			if !got.IsExactType(tc.want) {
				t.Errorf("ParamsType() got %v, wanted %v", got, tc.want)
			}
		})
	}
}

func TestValidateParams(t *testing.T) {
	schema := schemaOf(t, `
type: object
required: ["labels"]
properties:
  message: {type: string}
  enforcement: {type: string, enum: ["warn", "deny"]}
  labels:
    type: array
    items:
      type: object
      properties:
        key: {type: string}
        allowedRegex: {type: string}`)
	tests := []struct {
		name   string
		params string
		want   []string
	}{
		{
			name:   "parameters which match the schema",
			params: `{labels: [{key: owner, allowedRegex: "^[a-z]+$"}]}`,
		},
		{
			name:   "a missing required property",
			params: `{message: "label required"}`,
			want:   []string{`parameters: missing required property "labels"`},
		},
		{
			name:   "a property of the wrong type",
			params: `{labels: {key: owner}}`,
			want:   []string{"parameters.labels: expected array, got map[string]interface {}"},
		},
		{
			name:   "a misspelled property",
			params: `{labels: [], lables: []}`,
			want:   []string{`parameters: unknown property "lables", the schema declares: enforcement, labels, message`},
		},
		{
			name:   "a value outside the enumeration",
			params: `{labels: [], enforcement: dryrun}`,
			want:   []string{`parameters.enforcement: value "dryrun" is not one of the allowed values: warn, deny`},
		},
		{
			name:   "a mistake inside a list item",
			params: `{labels: [{key: 12}]}`,
			want:   []string{"parameters.labels[0].key: expected string, got int"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var params any
			if err := yaml.Unmarshal([]byte(tc.params), &params); err != nil {
				t.Fatalf("yaml.Unmarshal() failed: %v", err)
			}
			errs := ValidateParams(schema, params)
			got := make([]string, 0, len(errs))
			for _, err := range errs {
				got = append(got, err.Error())
			}
			if strings.Join(got, "; ") != strings.Join(tc.want, "; ") {
				t.Errorf("ValidateParams() got %v, wanted %v", got, tc.want)
			}
		})
	}
}

// TestTemplateValidateParams checks the parameters of a constraint against the
// schema its template declares.
func TestTemplateValidateParams(t *testing.T) {
	tmpl := compileTemplate(t, "k8srequiredlabels")
	if errs := tmpl.ValidateParams(map[string]any{
		"labels": []any{map[string]any{"key": "owner"}},
	}); len(errs) != 0 {
		t.Errorf("ValidateParams() got %v, wanted no errors", errs)
	}
	errs := tmpl.ValidateParams(map[string]any{"labels": "owner"})
	if len(errs) != 1 || !strings.Contains(errs[0].Error(), "expected array") {
		t.Errorf("ValidateParams() got %v, wanted the type of labels to be reported", errs)
	}
}
