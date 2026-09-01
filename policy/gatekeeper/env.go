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
	"cel.dev/cel-go/cel"
	"cel.dev/cel-go/common/types"
	"cel.dev/cel-go/ext"
)

// Variable names bound by the Gatekeeper admission engine. They mirror the
// variables which Kubernetes binds when evaluating a ValidatingAdmissionPolicy.
const (
	// ObjectVar is the object being admitted. It is null on DELETE.
	ObjectVar = "object"

	// OldObjectVar is the object as it exists in the cluster. It is null on
	// CREATE.
	OldObjectVar = "oldObject"

	// RequestVar is the attributes of the admission request.
	RequestVar = "request"

	// ParamsVar is the constraint resource which instantiated the template.
	ParamsVar = "params"

	// NamespaceObjectVar is the namespace of the object being admitted, and is
	// null for cluster-scoped objects.
	NamespaceObjectVar = "namespaceObject"
)

// Variables which Gatekeeper defines for every template, in terms of the
// variables above.
const (
	// ParamsVariable holds the parameters of the constraint under evaluation,
	// and is null for a constraint which declares none.
	ParamsVariable = "params"

	// AnyObjectVariable holds the object being admitted, falling back to the
	// object as it exists in the cluster so that DELETE requests can be
	// validated with the same expression as CREATE and UPDATE.
	AnyObjectVariable = "anyObject"
)

const (
	// paramsExpr extracts the parameters from the constraint resource. A
	// constraint which sets no parameters yields null, as it does in a cluster.
	paramsExpr = `has(params.spec) && has(params.spec.parameters) ? params.spec.parameters : null`

	// anyObjectExpr is the definition Gatekeeper uses for variables.anyObject.
	anyObjectExpr = `object == null ? oldObject : object`
)

// config carries the options which shape the CEL environment of a template and
// the reviews evaluated against it.
type config struct {
	objectSchema   map[string]any
	maxConcurrency int
	synchronous    bool
	programOpts    []cel.ProgramOption
	schemas        *SchemaTypes
	extraOptions   []cel.EnvOption
	provider       DataProvider
	maxRounds      int
}

// defaultMaxRounds bounds how many times a review re-evaluates a policy to
// resolve referential data. Each round resolves every lookup the policy can
// reach, so more than a couple of rounds means a policy whose lookups depend on
// the results of earlier ones.
const defaultMaxRounds = 10

// Option configures how a ConstraintTemplate is compiled.
type Option func(*config)

// WithObjectSchema types the object under review from an OpenAPI schema, such
// as the one a CustomResourceDefinition declares for its resource.
//
// A policy which reads a field the schema does not declare then fails to
// compile, rather than reading an absent value in a cluster. Without a schema
// the object is untyped, since a template may match kinds whose schemas are not
// known here.
//
// ReadCRDSchema reads the schema of a version from a CustomResourceDefinition:
//
//	schema, err := gatekeeper.ReadCRDSchema("crd.yaml", "v1")
//	tmpl, err := gatekeeper.CompileFile("template.yaml",
//	    gatekeeper.WithObjectSchema(schema))
func WithObjectSchema(schema map[string]any) Option {
	return func(c *config) {
		c.objectSchema = schema
	}
}

// WithSchemaTypes shares one registry of schema types across the environments a
// tool builds, so that the types a template declares are known to the
// environment its programs are planned against.
//
// It is needed when the environment and the template's own declarations are
// configured separately, as they are when compiling through the CEL compiler
// tool. Compiling through this package shares a registry already.
func WithSchemaTypes(schemas *SchemaTypes) Option {
	return func(c *config) {
		c.schemas = schemas
	}
}

// EnvOptions adds CEL environment options to those the template derives, for
// custom functions and types a host application exposes to its policies.
func EnvOptions(opts ...cel.EnvOption) Option {
	return func(c *config) {
		c.extraOptions = append(c.extraOptions, opts...)
	}
}

// WithDataProvider supplies the referential data a policy reads: the cluster
// inventory Rego policies read from `data.inventory`, and the providers Rego
// policies query with `external_data`.
//
// A policy which makes a lookup without a provider configured fails its review
// with an error naming the lookup, rather than silently deciding on absent
// data.
func WithDataProvider(provider DataProvider) Option {
	return func(c *config) {
		c.provider = provider
	}
}

// MaxConcurrentLookups bounds how many lookups a review may have in flight at
// once, so that a policy which reads the cluster from within a comprehension
// over a large list cannot launch a goroutine per element.
//
// The default is the evaluator's, which does not bound them.
func MaxConcurrentLookups(limit int) Option {
	return func(c *config) {
		c.maxConcurrency = limit
	}
}

// SynchronousLookups declares the inventory functions with blocking rather than
// asynchronous bindings.
//
// An environment which declares an asynchronous function yields programs that
// only ConcurrentEval can run, so a tool which evaluates a policy with Eval —
// the CEL test runner among them — needs this form. The lookups of a policy
// then run one after another on the evaluating goroutine, and are not
// cancellable.
func SynchronousLookups() Option {
	return func(c *config) {
		c.synchronous = true
	}
}

// ProgramOptions passes options to the programs a template compiles to, for
// evaluation behavior this package does not expose, such as the strategy which
// decides when the evaluator resumes after asynchronous calls complete.
func ProgramOptions(opts ...cel.ProgramOption) Option {
	return func(c *config) {
		c.programOpts = append(c.programOpts, opts...)
	}
}

// programOptions returns the options the programs of a template are planned
// with.
func (c *config) programOptions() []cel.ProgramOption {
	opts := c.programOpts
	if c.maxConcurrency > 0 {
		opts = append(append([]cel.ProgramOption{}, opts...), cel.AsyncMaxConcurrency(c.maxConcurrency))
	}
	return opts
}

// MaxDataRounds bounds how many passes a review makes to resolve the reads a
// policy performs through the data namespace, which cannot be asynchronous. The
// default is enough for policies whose reads depend on the results of earlier
// reads; a policy which exceeds it fails its review.
func MaxDataRounds(rounds int) Option {
	return func(c *config) {
		if rounds > 0 {
			c.maxRounds = rounds
		}
	}
}

func newConfig(opts ...Option) *config {
	c := &config{maxRounds: defaultMaxRounds}
	for _, opt := range opts {
		opt(c)
	}
	if c.schemas == nil {
		c.schemas = NewSchemaTypes(baseTypeProvider())
	}
	return c
}

// baseTypeProvider is the registry the schema types fall back to, which holds
// the standard types a CEL environment knows.
func baseTypeProvider() types.Provider {
	registry, err := types.NewRegistry()
	if err != nil {
		return types.NewEmptyRegistry()
	}
	return registry
}

// EnvironmentOptions returns the part of a template's CEL environment which
// does not depend on the template: the extension libraries a cluster enables,
// the functions which read referential data, the schema types, and the inputs
// whose type is the same for every template.
//
// The inputs a template's own schemas describe — the object under review and
// the constraint which parameterizes it — are declared by EnvOptionFromMetadata,
// so a tool which builds its own compiler uses both, sharing one registry of
// schema types between them:
//
//	schemas := gatekeeper.NewSchemaTypes(nil)
//	opts := []gatekeeper.Option{gatekeeper.WithSchemaTypes(schemas)}
//	compilerOpts := []any{
//	    gatekeeper.ParserOption(opts...),
//	    compiler.PolicyMetadataEnvOption(gatekeeper.EnvOptionFromMetadata(opts...)),
//	}
//	for _, opt := range gatekeeper.EnvironmentOptions(opts...) {
//	    compilerOpts = append(compilerOpts, opt)
//	}
func EnvironmentOptions(opts ...Option) []cel.EnvOption {
	return environmentOptions(newConfig(opts...))
}

// EnvOptionFromMetadata returns a function which declares the inputs whose
// types come from the template being compiled, using the metadata collected
// while parsing it.
//
// The signature matches the policy metadata hook of the CEL compiler tool,
// which supplies the metadata of the policy being compiled. It complements
// EnvironmentOptions rather than replacing it.
func EnvOptionFromMetadata(opts ...Option) func(map[string]any) cel.EnvOption {
	c := newConfig(opts...)
	return func(metadata map[string]any) cel.EnvOption {
		schema, _ := metadata[MetadataParamsSchema].(map[string]any)
		name, _ := metadata[MetadataTemplateName].(string)
		return envOption(inputOptions(c, name, schema))
	}
}

// inputOptions declares the object under review and the constraint which
// parameterizes it, typed from the schemas which describe them.
//
// Both are declared as object types where a schema is available. An object type
// is nullable, so a policy may still compare an input against null, which is
// how Kubernetes reports an input the request does not carry.
func inputOptions(c *config, templateName string, paramsSchema map[string]any) []cel.EnvOption {
	if templateName == "" {
		templateName = "template"
	}
	prefix := "gatekeeper." + templateName
	objectType := cel.DynType
	if len(c.objectSchema) != 0 {
		objectType = c.schemas.Declare(prefix+".Object", resourceSchema(c.objectSchema))
	}
	constraintType := c.schemas.Declare(prefix+".Constraint", constraintSchema(paramsSchema))
	return []cel.EnvOption{
		cel.Variable(ObjectVar, objectType),
		cel.Variable(OldObjectVar, objectType),
		cel.Variable(ParamsVar, constraintType),
	}
}

// constraintSchema describes the constraint resource bound to `params`, whose
// spec.parameters is what a template's own schema describes and what
// `variables.params` holds.
func constraintSchema(paramsSchema map[string]any) map[string]any {
	unstructured := map[string]any{"type": "object"}
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"apiVersion": map[string]any{"type": "string"},
			"kind":       map[string]any{"type": "string"},
			"metadata":   unstructured,
			"status":     unstructured,
			"spec": map[string]any{
				"type": "object",
				"properties": map[string]any{
					"match":                    unstructured,
					"enforcementAction":        map[string]any{"type": "string"},
					"scopedEnforcementActions": map[string]any{"type": "array", "items": unstructured},
					"parameters":               paramsSchema,
				},
			},
		},
	}
}

// resourceSchema adds the fields every Kubernetes object carries to the schema
// of a resource, which a CustomResourceDefinition leaves for the API server to
// supply.
func resourceSchema(schema map[string]any) map[string]any {
	properties, found := schema["properties"].(map[string]any)
	if !found {
		return schema
	}
	standard := map[string]any{
		"apiVersion": map[string]any{"type": "string"},
		"kind":       map[string]any{"type": "string"},
		"metadata":   map[string]any{"type": "object"},
	}
	merged := make(map[string]any, len(properties)+len(standard))
	for name, property := range properties {
		merged[name] = property
	}
	for name, property := range standard {
		if _, found := merged[name]; !found {
			merged[name] = property
		}
	}
	resource := make(map[string]any, len(schema))
	for key, value := range schema {
		resource[key] = value
	}
	resource["properties"] = merged
	return resource
}

// environmentOptions returns the libraries the admission engine enables, the
// functions which read referential data, and the inputs whose type is the same
// for every template.
func environmentOptions(c *config) []cel.EnvOption {
	// The admission inputs are declared as untyped values because Kubernetes
	// binds them from unstructured objects and leaves them null when they do
	// not apply to the request, and a null check does not type-check against a
	// map type.
	opts := []cel.EnvOption{
		// The CEL policy compiler composes the variables and validations of a
		// template into a single expression held together by cel.@block.
		ext.Bindings(),
		cel.EnableMacroCallTracking(),
		c.schemas.EnvOption(),
		cel.Variable(RequestVar, cel.DynType),
		cel.Variable(NamespaceObjectVar, cel.DynType),
	}
	opts = append(opts, Libraries()...)
	// Referential data has no equivalent in a ValidatingAdmissionPolicy, so the
	// functions which read it are declared alongside the admission variables,
	// as are the reads a policy ported from Rego makes.
	opts = append(opts, dataFunctions(c), RegoCompatibility())
	return append(opts, c.extraOptions...)
}

// variablePrefix is the namespace the CEL policy compiler gives to the
// variables declared by a policy, matching the `variables.name` form used by
// Kubernetes admission policies.
const variablePrefix = "variables."

// Libraries returns the CEL extension libraries available to policies
// evaluated by the Gatekeeper admission engine.
//
// The set is deliberately close to the environment Kubernetes configures for
// admission policies rather than to everything cel-go offers, so that a
// template which compiles here is one a cluster will also accept. The
// Kubernetes list and regex functions are supplied by this package, since
// cel-go does not provide them, and the string extension is pinned to the
// version Kubernetes enables.
//
// Libraries a cluster has and this environment does not are listed in the
// package documentation; a policy which needs one can declare it with the
// EnvOptions option.
func Libraries() []cel.EnvOption {
	return []cel.EnvOption{
		cel.OptionalTypes(),
		cel.CrossTypeNumericComparisons(true),
		cel.EagerlyValidateDeclarations(true),
		ext.Strings(ext.StringsVersion(2)),
		KubernetesLists(),
		KubernetesStrings(),
	}
}

// dataFunctions declares the referential lookups with the binding style the
// configuration asks for.
func dataFunctions(c *config) cel.EnvOption {
	if c.synchronous {
		return DataFunctions(SynchronousLookups())
	}
	return DataFunctions()
}

// envOption combines a list of environment options into one.
func envOption(opts []cel.EnvOption) cel.EnvOption {
	return func(e *cel.Env) (*cel.Env, error) {
		return e.Extend(opts...)
	}
}
