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

// Package darklaunch is a proof of concept for capturing named probe traces and
// evaluation metrics from CEL programs without any change to the interpreter.
//
// The mechanism is a single CEL function:
//
//	trace('name', <expr>)
//
// declared non-strict and late-bound. Non-strict means the implementation is
// invoked for every value a subexpression can produce, including errors and
// unknowns, rather than being bypassed by the evaluator's early return.
// Late-bound means the declaration carries no implementation at all, so the
// implementation becomes a per-program input: production installs an identity
// binding, and an instrumented program installs a recording binding.
//
// Late binding also keeps the constant folder away from probes. The folder
// skips calls whose declaration reports HasLateBinding, so trace('n', 1 + 2)
// survives optimization instead of collapsing to 3.
//
// See DESIGN.md for the full design this proves out.
package darklaunch

import (
	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/functions"
	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
)

const (
	// TraceFunction is the CEL function name that marks a probe.
	TraceFunction = "trace"

	// TraceOverload is the overload id of the single trace() signature.
	TraceOverload = "trace_string_dyn"
)

// ProbeDecls declares trace() and installs the validator which requires probe
// names to be string literals.
//
// The declaration deliberately carries no implementation. A program built from
// an environment with this option MUST also be given exactly one of
// IdentityBinding or RecordingBinding, or evaluation fails with
// "no such overload: trace". NewProgram applies one for you.
func ProbeDecls() cel.EnvOption {
	paramT := cel.TypeParamType("T")
	return func(e *cel.Env) (*cel.Env, error) {
		return cel.Lib(&probeLib{paramT: paramT})(e)
	}
}

type probeLib struct {
	paramT *cel.Type
}

func (l *probeLib) LibraryName() string { return "cel.lib.ext.darklaunch.probe" }

func (l *probeLib) CompileOptions() []cel.EnvOption {
	return []cel.EnvOption{
		cel.Function(TraceFunction,
			cel.FunctionDocs(
				"records the value of an expression for dark-launch analysis and returns it unchanged"),
			cel.Overload(TraceOverload,
				[]*cel.Type{cel.StringType, l.paramT},
				l.paramT,
				cel.OverloadExamples(`trace('is_member', request.user in allowed_users) // -> true`),
				// Invoked for errors and unknowns rather than being bypassed.
				cel.OverloadIsNonStrict(),
				// No implementation here: it is supplied per-program.
				cel.LateFunctionBinding(),
			),
		),
		cel.ASTValidators(constantProbeNames{}),
	}
}

// ProgramOptions returns nothing on purpose.
//
// A library's ProgramOptions are static across every Env.Program() call, but
// the recording binding has to differ per program because it owns per-program
// state. The binding is therefore supplied at Program() time instead.
func (l *probeLib) ProgramOptions() []cel.ProgramOption { return nil }

// IdentityBinding installs the production implementation of trace(): the
// identity on its second argument.
func IdentityBinding() cel.ProgramOption {
	return bindTrace(identityProbe)
}

// RecordingBinding installs an implementation which appends every probe
// observation to rec before returning the traced value unchanged.
//
// The returned option binds rec to one program. Because a functions.BinaryOp
// receives no Activation, rec is shared by every concurrent evaluation of that
// program: a program built with this option must not be evaluated from more
// than one goroutine at a time. NewPool enforces that.
func RecordingBinding(rec *Recorder) cel.ProgramOption {
	if rec == nil {
		return IdentityBinding()
	}
	return bindTrace(rec.observe)
}

func bindTrace(op functions.BinaryOp) cel.ProgramOption {
	// Bound under both the overload id and the function name so that programs
	// built from parsed-only ASTs, where no overload id is resolved, dispatch
	// the same way as checked ones.
	return cel.Functions(
		&functions.Overload{Operator: TraceOverload, Binary: op, NonStrict: true},
		&functions.Overload{Operator: TraceFunction, Binary: op, NonStrict: true},
	)
}

// identityProbe is the production binding.
//
// It still has to propagate an error or unknown name argument, because
// non-strict evaluation hands those to the implementation rather than
// returning them first. Dropping that check would make trace() swallow an
// error that a strict call would have propagated.
func identityProbe(name, val ref.Val) ref.Val {
	if types.IsUnknownOrError(name) {
		return name
	}
	return val
}

// NewProgram builds a production program: probes are declared and evaluated,
// but nothing is recorded and the only cost is one call frame per probe.
//
// It exists so that the production path cannot be built without a binding. A
// late-bound declaration contributes no implementation, and a program missing
// one plans cleanly and then fails at evaluation with "no such overload:
// trace" — a runtime failure for a configuration mistake. Routing both paths
// through NewProgram and NewPool makes that unreachable.
func NewProgram(env *cel.Env, a *cel.Ast, opts ...cel.ProgramOption) (cel.Program, error) {
	return env.Program(a, append(append([]cel.ProgramOption{}, opts...), IdentityBinding())...)
}
