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
	"sort"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"

	"google.golang.org/protobuf/reflect/protoreflect"
)

// SchemaTypes turns the OpenAPI schemas of a policy's inputs into CEL types.
//
// A schema which declares properties becomes an object type whose fields are
// checked, so a policy which reads a field the schema does not declare fails to
// compile rather than reading an absent value in a cluster:
//
//	ERROR: template.yaml:31:24: undefined field 'lables'
//	 |               expression: "variables.params.lables"
//	 | .......................^
//
// Object types are nullable, so a policy may still compare an input against
// null, which is how Kubernetes reports an input the request does not carry.
//
// Values remain the unstructured maps the API server and Gatekeeper's cache
// hold; the types describe their shape without changing how they are read.
type SchemaTypes struct {
	types.Provider

	structs map[string]map[string]*cel.Type
}

// NewSchemaTypes creates a registry of schema types which falls back to the
// given provider for types it does not declare. A nil provider registers no
// other types.
func NewSchemaTypes(base types.Provider) *SchemaTypes {
	if base == nil {
		base = types.NewEmptyRegistry()
	}
	return &SchemaTypes{Provider: base, structs: map[string]map[string]*cel.Type{}}
}

// EnvOption supplies the declared types to a CEL environment.
func (s *SchemaTypes) EnvOption() cel.EnvOption {
	return cel.CustomTypeProvider(s)
}

// Declare converts an OpenAPI schema into a CEL type, registering an object
// type under typeName for the schema and one for each object nested within it.
//
// A schema which declares no properties, one which allows properties it does
// not declare, and one with no type at all describe values whose fields cannot
// be checked, and become maps or untyped values instead.
func (s *SchemaTypes) Declare(typeName string, schema map[string]any) *cel.Type {
	if len(schema) == 0 {
		return cel.DynType
	}
	// A field which may hold either an integer or a string has no single CEL
	// type, and neither does a field with no declared type.
	if boolField(schema, "x-kubernetes-int-or-string") {
		return cel.DynType
	}
	switch name, _ := schema["type"].(string); name {
	case "string":
		return cel.StringType
	case "integer":
		return cel.IntType
	case "number":
		return cel.DoubleType
	case "boolean":
		return cel.BoolType
	case "array":
		items, _ := schema["items"].(map[string]any)
		return cel.ListType(s.Declare(typeName+".item", items))
	case "object":
		return s.declareObject(typeName, schema)
	default:
		return cel.DynType
	}
}

// declareObject registers the object type for a schema, or describes it as a
// map when its fields are not known well enough to check them.
func (s *SchemaTypes) declareObject(typeName string, schema map[string]any) *cel.Type {
	if additional, found := schema["additionalProperties"].(map[string]any); found {
		return cel.MapType(cel.StringType, s.Declare(typeName+".value", additional))
	}
	properties, found := schema["properties"].(map[string]any)
	if !found || len(properties) == 0 {
		return cel.MapType(cel.StringType, cel.DynType)
	}
	// A schema which keeps the fields it does not declare cannot reject a field
	// as undefined, so its values are read as a map.
	if boolField(schema, "x-kubernetes-preserve-unknown-fields") {
		return cel.MapType(cel.StringType, cel.DynType)
	}
	fields := make(map[string]*cel.Type, len(properties))
	// The type is registered before its fields are declared, so that a schema
	// which refers back to itself terminates.
	s.structs[typeName] = fields
	for _, name := range sortedKeys(properties) {
		property, ok := properties[name].(map[string]any)
		if !ok {
			fields[name] = cel.DynType
			continue
		}
		fields[name] = s.Declare(typeName+"."+name, property)
	}
	return cel.ObjectType(typeName)
}

// FindStructType returns the type of a declared object type.
func (s *SchemaTypes) FindStructType(structType string) (*cel.Type, bool) {
	if _, found := s.structs[structType]; found {
		return types.NewTypeTypeWithParam(cel.ObjectType(structType)), true
	}
	return s.Provider.FindStructType(structType)
}

// FindStructFieldNames returns the fields a declared object type holds.
func (s *SchemaTypes) FindStructFieldNames(structType string) ([]string, bool) {
	fields, found := s.structs[structType]
	if !found {
		return s.Provider.FindStructFieldNames(structType)
	}
	return sortedKeys(fields), true
}

// FindStructFieldType returns the type of a field, and reports whether the
// object type declares it. A field which is not declared is what makes a
// misspelled field a compilation error.
func (s *SchemaTypes) FindStructFieldType(structType, fieldName string) (*types.FieldType, bool) {
	fields, found := s.structs[structType]
	if !found {
		return s.Provider.FindStructFieldType(structType, fieldName)
	}
	fieldType, found := fields[fieldName]
	if !found {
		return nil, false
	}
	return &types.FieldType{
		Type:        fieldType,
		IsSet:       fieldIsSet(fieldName),
		GetFrom:     fieldValue(fieldName),
		IsJSONField: true,
	}, true
}

// RegisterDescriptor and RegisterType pass type registrations through to the
// provider the schema types fall back to, so that an environment can register
// its own types alongside those derived from a schema.
func (s *SchemaTypes) RegisterDescriptor(fileDesc protoreflect.FileDescriptor) error {
	registry, ok := s.Provider.(interface {
		RegisterDescriptor(protoreflect.FileDescriptor) error
	})
	if !ok {
		return fmt.Errorf("provider does not support type registration: %T", s.Provider)
	}
	return registry.RegisterDescriptor(fileDesc)
}

// RegisterType registers a type with the underlying provider.
func (s *SchemaTypes) RegisterType(celTypes ...ref.Type) error {
	registry, ok := s.Provider.(interface {
		RegisterType(...ref.Type) error
	})
	if !ok {
		return fmt.Errorf("provider does not support type registration: %T", s.Provider)
	}
	return registry.RegisterType(celTypes...)
}

// NewValue reports that a schema type cannot be constructed, since the values
// it describes come from the cluster rather than from a policy.
func (s *SchemaTypes) NewValue(structType string, fields map[string]ref.Val) ref.Val {
	if _, found := s.structs[structType]; found {
		return types.NewErr("type '%s' cannot be constructed: it describes an object read from the cluster", structType)
	}
	return s.Provider.NewValue(structType, fields)
}

// fieldIsSet reports whether an unstructured object holds a field.
func fieldIsSet(fieldName string) ref.FieldTester {
	return func(target any) bool {
		fields, ok := toStringMap(unwrap(target))
		if !ok {
			return false
		}
		_, found := fields[fieldName]
		return found
	}
}

// fieldValue reads a field from an unstructured object.
func fieldValue(fieldName string) ref.FieldGetter {
	return func(target any) (any, error) {
		fields, ok := toStringMap(unwrap(target))
		if !ok {
			return nil, fmt.Errorf("no such field: %s", fieldName)
		}
		value, found := fields[fieldName]
		if !found {
			return nil, fmt.Errorf("no such field: %s", fieldName)
		}
		return value, nil
	}
}

// unwrap returns the native value behind a CEL value.
func unwrap(target any) any {
	if val, ok := target.(ref.Val); ok {
		return val.Value()
	}
	return target
}

func boolField(schema map[string]any, name string) bool {
	value, found := schema[name]
	return found && value == true
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
