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
	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/ext"
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

// config carries the options which shape the CEL environment of a template.
type config struct {
	typedParams  bool
	extraOptions []cel.EnvOption
}

// Option configures how a ConstraintTemplate is compiled.
type Option func(*config)

// TypedParams declares `variables.params` with the type derived from the
// template's openAPIV3Schema, rather than as an untyped value.
//
// Typed parameters catch mistakes which are otherwise only visible at
// evaluation time, such as comparing a parameter to the wrong type. The cost is
// that a template may no longer compare `variables.params` against null, since
// CEL has no nullable map type; templates which do so should be compiled
// without this option.
func TypedParams() Option {
	return func(c *config) {
		c.typedParams = true
	}
}

// EnvOptions adds CEL environment options to those the template derives, for
// custom functions and types a host application exposes to its policies.
func EnvOptions(opts ...cel.EnvOption) Option {
	return func(c *config) {
		c.extraOptions = append(c.extraOptions, opts...)
	}
}

func newConfig(opts ...Option) *config {
	c := &config{}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// EnvOptionFromMetadata returns a function which builds the CEL environment for
// a ConstraintTemplate from the metadata collected while parsing it.
//
// The signature matches the policy metadata hook of the CEL compiler tool, so
// the environment of a template is derived from the template itself:
//
//	compiler.NewCompiler(
//	    compiler.PolicyMetadataEnvOption(gatekeeper.EnvOptionFromMetadata()),
//	    gatekeeper.ParserOption())
func EnvOptionFromMetadata(opts ...Option) func(map[string]any) cel.EnvOption {
	c := newConfig(opts...)
	return func(metadata map[string]any) cel.EnvOption {
		schema, _ := metadata[MetadataParamsSchema].(map[string]any)
		return envOption(environmentOptions(schema, c))
	}
}

// environmentOptions returns the environment of the Gatekeeper
// K8sNativeValidation engine: the variables it binds and the CEL extension
// libraries it enables.
func environmentOptions(schema map[string]any, c *config) []cel.EnvOption {
	// The admission inputs are declared as untyped values because Kubernetes
	// binds them from unstructured objects and leaves them null when they do
	// not apply to the request, and a null check does not type-check against a
	// map type.
	opts := []cel.EnvOption{
		// The CEL policy compiler composes the variables and validations of a
		// template into a single expression held together by cel.@block.
		ext.Bindings(),
		cel.EnableMacroCallTracking(),
		cel.Variable(ObjectVar, cel.DynType),
		cel.Variable(OldObjectVar, cel.DynType),
		cel.Variable(RequestVar, cel.DynType),
		cel.Variable(ParamsVar, cel.DynType),
		cel.Variable(NamespaceObjectVar, cel.DynType),
	}
	opts = append(opts, Libraries()...)
	if c.typedParams {
		opts = append(opts, cel.Variable(variablePrefix+ParamsVariable, ParamsType(schema)))
	}
	return append(opts, c.extraOptions...)
}

// variablePrefix is the namespace the CEL policy compiler gives to the
// variables declared by a policy, matching the `variables.name` form used by
// Kubernetes admission policies.
const variablePrefix = "variables."

// Libraries returns the CEL extension libraries available to policies
// evaluated by the Gatekeeper admission engine.
//
// The set approximates the environment Kubernetes configures for admission
// policies. See the package documentation for the differences which remain.
func Libraries() []cel.EnvOption {
	return []cel.EnvOption{
		cel.OptionalTypes(),
		cel.CrossTypeNumericComparisons(true),
		cel.EagerlyValidateDeclarations(true),
		ext.Strings(),
		ext.Sets(),
		ext.Encoders(),
		ext.Math(),
		KubernetesLists(),
		KubernetesStrings(),
	}
}

// envOption combines a list of environment options into one.
func envOption(opts []cel.EnvOption) cel.EnvOption {
	return func(e *cel.Env) (*cel.Env, error) {
		return e.Extend(opts...)
	}
}
