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

package gatekeeper_test

import (
	"testing"

	"cel.dev/cel-go/policy/gatekeeper"
	"cel.dev/cel-go/tools/celtest"
	"cel.dev/cel-go/tools/compiler"
)

// TestTemplateWithCELTestRunner runs a ConstraintTemplate through the CEL test
// runner, which reports how much of the policy the tests reached.
//
// It is the path a project already using the cel-go tooling would take, and
// checks that a Gatekeeper template compiles through the standard compiler as
// well as through this package's own entry point.
func TestTemplateWithCELTestRunner(t *testing.T) {
	// The environment and the template's own declarations are configured
	// separately here, so they share one registry of schema types: the types a
	// template derives from its schemas must be known to the environment its
	// programs are planned against.
	opts := []gatekeeper.Option{gatekeeper.WithSchemaTypes(gatekeeper.NewSchemaTypes(nil))}
	compilerOpts := []any{
		gatekeeper.ParserOption(opts...),
		compiler.PolicyMetadataEnvOption(gatekeeper.EnvOptionFromMetadata(opts...)),
	}
	for _, opt := range gatekeeper.EnvironmentOptions(opts...) {
		compilerOpts = append(compilerOpts, opt)
	}
	celtest.TriggerTests(t,
		celtest.TestCompiler(compilerOpts...),
		celtest.TestExpression("testdata/k8srequiredlabels/template.yaml"),
		celtest.TestSuite("testdata/k8srequiredlabels/tests.yaml"),
		celtest.EnableCoverage())
}
