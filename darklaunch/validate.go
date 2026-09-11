// Copyright 2026 Google LLC
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

package darklaunch

import (
	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/ast"
	"github.com/google/cel-go/common/types"
)

// constantProbeNames rejects trace() calls whose name argument is not a string
// literal.
//
// Probe names become metric attributes. OpenTelemetry requires metric
// dimensions to be low cardinality, so a computed name is a defect rather than
// an inconvenience: it makes the probe unalignable between a control and a
// candidate and gives the name unbounded cardinality. Validating at compile
// time makes that a build failure for everyone rather than a surprise on the
// day probes are switched on.
type constantProbeNames struct{}

// Name implements the cel.ASTValidator interface method.
func (constantProbeNames) Name() string { return "darklaunch.constant_probe_names" }

// Validate implements the cel.ASTValidator interface method.
func (constantProbeNames) Validate(_ *cel.Env, _ cel.ValidatorConfig, a *ast.AST, iss *cel.Issues) {
	ast.PostOrderVisit(a.Expr(), ast.NewExprVisitor(func(e ast.Expr) {
		if e.Kind() != ast.CallKind {
			return
		}
		call := e.AsCall()
		if call.FunctionName() != TraceFunction || call.IsMemberFunction() {
			return
		}
		args := call.Args()
		if len(args) != 2 {
			return
		}
		name := args[0]
		if name.Kind() == ast.LiteralKind && name.AsLiteral().Type() == types.StringType {
			return
		}
		iss.ReportErrorAtID(name.ID(),
			"%s() name must be a string literal so that probe names stay alignable and low cardinality",
			TraceFunction)
	}))
}
