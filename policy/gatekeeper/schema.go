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
	"fmt"
	"strings"

	"cel.dev/cel-go/policy"
)

// ParamsSchema returns the openAPIV3Schema declared by a ConstraintTemplate,
// which describes the parameters accepted by constraints of that template.
//
// The second return value reports whether the template declared a schema.
func ParamsSchema(p *policy.Policy) (map[string]any, bool) {
	if p == nil {
		return nil, false
	}
	value, found := p.Metadata(MetadataParamsSchema)
	if !found {
		return nil, false
	}
	schema, ok := value.(map[string]any)
	return schema, ok
}

// ReadCRDSchema reads the schema a CustomResourceDefinition declares for one
// version of its resource, for typing the object a policy reviews.
//
// An empty version reads the version marked for storage, or the only version
// when the definition declares one.
func ReadCRDSchema(path, version string) (map[string]any, error) {
	objects, err := ReadObjects(path)
	if err != nil {
		return nil, err
	}
	for _, object := range objects {
		crd, ok := toStringMap(object)
		if !ok {
			continue
		}
		if kind, _ := crd["kind"].(string); kind != "CustomResourceDefinition" {
			continue
		}
		return crdVersionSchema(path, crd, version)
	}
	return nil, fmt.Errorf("%s: holds no CustomResourceDefinition", path)
}

// crdVersionSchema selects the version of a definition and returns its schema.
func crdVersionSchema(path string, crd map[string]any, version string) (map[string]any, error) {
	spec, _ := toStringMap(crd["spec"])
	versions, _ := spec["versions"].([]any)
	if len(versions) == 0 {
		return nil, fmt.Errorf("%s: the definition declares no versions", path)
	}
	var names []string
	for _, entry := range versions {
		declared, ok := toStringMap(entry)
		if !ok {
			continue
		}
		name, _ := declared["name"].(string)
		names = append(names, name)
		switch {
		case version != "" && name != version:
			continue
		case version == "" && len(versions) > 1 && declared["storage"] != true:
			continue
		}
		schema, _ := toStringMap(declared["schema"])
		openAPI, ok := toStringMap(schema["openAPIV3Schema"])
		if !ok {
			return nil, fmt.Errorf("%s: version %s declares no openAPIV3Schema", path, name)
		}
		return openAPI, nil
	}
	if version == "" {
		return nil, fmt.Errorf("%s: no version is marked for storage, name one of: %s",
			path, strings.Join(names, ", "))
	}
	return nil, fmt.Errorf("%s: no version named %s, the definition declares: %s",
		path, version, strings.Join(names, ", "))
}

// ValidateParams checks a set of constraint parameters against the
// openAPIV3Schema of the template they are used with, returning one error per
// mismatch. It is intended for test fixtures and constraint review, where a
// parameter which does not match the schema is a likely cause of a policy which
// does not behave as its author expects.
//
// Validation covers the parts of OpenAPI which the Gatekeeper constraint schema
// relies on: declared types, required properties, enumerated values, and the
// item type of arrays. Constraints which the schema does not declare are not
// checked.
func ValidateParams(schema map[string]any, params any) []error {
	if len(schema) == 0 {
		return nil
	}
	return validateValue(schema, params, "parameters")
}

func validateValue(schema map[string]any, value any, path string) []error {
	if value == nil {
		return nil
	}
	if intOrString, found := schema["x-kubernetes-int-or-string"]; found && intOrString == true {
		return nil
	}
	typeName, _ := schema["type"].(string)
	var errs []error
	switch typeName {
	case "string":
		str, ok := value.(string)
		if !ok {
			return []error{typeError(path, "string", value)}
		}
		if enum, found := schema["enum"].([]any); found && !containsValue(enum, str) {
			errs = append(errs, fmt.Errorf("%s: value %q is not one of the allowed values: %s", path, str, formatEnum(enum)))
		}
	case "integer":
		if !isInteger(value) {
			return []error{typeError(path, "integer", value)}
		}
	case "number":
		if !isNumber(value) {
			return []error{typeError(path, "number", value)}
		}
	case "boolean":
		if _, ok := value.(bool); !ok {
			return []error{typeError(path, "boolean", value)}
		}
	case "array":
		list, ok := value.([]any)
		if !ok {
			return []error{typeError(path, "array", value)}
		}
		items, found := schema["items"].(map[string]any)
		if !found {
			return nil
		}
		for i, elem := range list {
			errs = append(errs, validateValue(items, elem, fmt.Sprintf("%s[%d]", path, i))...)
		}
	case "object":
		obj, ok := toStringMap(value)
		if !ok {
			return []error{typeError(path, "object", value)}
		}
		errs = append(errs, validateObject(schema, obj, path)...)
	}
	return errs
}

func validateObject(schema map[string]any, obj map[string]any, path string) []error {
	var errs []error
	properties, _ := schema["properties"].(map[string]any)
	if required, found := schema["required"].([]any); found {
		for _, req := range required {
			name, ok := req.(string)
			if !ok {
				continue
			}
			if _, found := obj[name]; !found {
				errs = append(errs, fmt.Errorf("%s: missing required property %q", path, name))
			}
		}
	}
	additional, hasAdditional := schema["additionalProperties"].(map[string]any)
	for _, name := range sortedKeys(obj) {
		prop, found := properties[name].(map[string]any)
		if !found {
			if hasAdditional {
				errs = append(errs, validateValue(additional, obj[name], path+"."+name)...)
				continue
			}
			if len(properties) != 0 {
				errs = append(errs, fmt.Errorf("%s: unknown property %q, the schema declares: %s",
					path, name, strings.Join(sortedKeys(properties), ", ")))
			}
			continue
		}
		errs = append(errs, validateValue(prop, obj[name], path+"."+name)...)
	}
	return errs
}

func typeError(path, want string, value any) error {
	return fmt.Errorf("%s: expected %s, got %T", path, want, value)
}

func isInteger(value any) bool {
	switch v := value.(type) {
	case int, int32, int64, uint, uint32, uint64:
		return true
	case float64:
		// YAML and JSON decoders may present whole numbers as floats.
		return v == float64(int64(v))
	default:
		return false
	}
}

func isNumber(value any) bool {
	switch value.(type) {
	case float32, float64:
		return true
	default:
		return isInteger(value)
	}
}

func containsValue(values []any, want string) bool {
	for _, v := range values {
		if s, ok := v.(string); ok && s == want {
			return true
		}
	}
	return false
}

func formatEnum(values []any) string {
	out := make([]string, 0, len(values))
	for _, v := range values {
		out = append(out, fmt.Sprintf("%v", v))
	}
	return strings.Join(out, ", ")
}

// toStringMap normalizes the map representations produced by the YAML and JSON
// decoders.
func toStringMap(value any) (map[string]any, bool) {
	switch v := value.(type) {
	case map[string]any:
		return v, true
	case map[any]any:
		out := make(map[string]any, len(v))
		for key, val := range v {
			name, ok := key.(string)
			if !ok {
				return nil, false
			}
			out[name] = val
		}
		return out, true
	default:
		return nil, false
	}
}
