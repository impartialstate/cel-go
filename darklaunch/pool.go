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
	"context"
	"sync"
	"time"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/types/ref"
)

// defaultInterruptCheckFrequency is how often a traced program tests its
// context for cancellation. Without a non-zero frequency a ContextEval deadline
// is decorative, so a traced program always sets one.
const defaultInterruptCheckFrequency = 100

// Result is one instrumented evaluation.
type Result struct {
	// Value is the CEL result. It is nil only when evaluation could not run.
	Value ref.Val

	// Err is set when the evaluation produced a CEL error, matching the
	// cel.Program contract.
	Err error

	// Kind partitions Value into value, error or unknown.
	Kind ResultKind

	// ErrorType is the normalized, low-cardinality error class, empty when
	// Kind is not KindError. Reported as OpenTelemetry's error.type.
	ErrorType string

	// Duration is the wall time spent inside Eval.
	Duration time.Duration

	// Cost is the tracked evaluation cost.
	Cost *uint64

	// Probes are the trace() observations, in evaluation order.
	Probes []Observation
}

// Pool evaluates an expression with probe recording enabled, safely, from any
// number of goroutines.
//
// It exists because a late binding is a property of a program rather than of an
// evaluation: functions.BinaryOp receives no Activation, so a recording binding
// cannot reach per-evaluation state and is instead shared by every concurrent
// evaluation of the program it is bound to. Pool resolves that by never sharing
// one: each pooled entry owns a program and the recorder bound into it, an
// entry is checked out by one goroutine at a time, and the recorder's
// observations are copied out before the entry is returned. The recorder never
// escapes, so there is nothing for a second goroutine to race against.
//
// The cost is one planned program per concurrently-active caller rather than
// one per expression.
type Pool struct {
	pool sync.Pool
	env  *cel.Env
	ast  *cel.Ast
	opts []cel.ProgramOption
}

type entry struct {
	prg cel.Program
	rec *Recorder
	err error
}

// NewPool plans an instrumented program for the given AST.
//
// Cost tracking and interrupt checking are always enabled: an instrumented
// program exists to be measured and bounded, and leaving either optional makes
// the resulting metrics and deadlines silently unavailable.
func NewPool(env *cel.Env, a *cel.Ast, opts ...cel.ProgramOption) (*Pool, error) {
	p := &Pool{env: env, ast: a, opts: opts}
	p.pool.New = func() any { return p.plan() }
	// Plan once eagerly so a misconfigured program is an error here rather than
	// on the first evaluation, where sync.Pool.New cannot report it.
	e := p.plan()
	if e.err != nil {
		return nil, e.err
	}
	p.pool.Put(e)
	return p, nil
}

func (p *Pool) plan() *entry {
	rec := NewRecorder()
	opts := make([]cel.ProgramOption, 0, len(p.opts)+3)
	opts = append(opts, p.opts...)
	opts = append(opts,
		RecordingBinding(rec),
		cel.EvalOptions(cel.OptTrackCost),
		cel.InterruptCheckFrequency(defaultInterruptCheckFrequency),
	)
	prg, err := p.env.Program(p.ast, opts...)
	return &entry{prg: prg, rec: rec, err: err}
}

// Eval evaluates the expression and returns the result together with everything
// observed during it.
func (p *Pool) Eval(vars any) (*Result, error) {
	return p.eval(nil, vars)
}

// ContextEval evaluates under a context, so a deadline or cancellation
// interrupts the evaluation.
func (p *Pool) ContextEval(ctx context.Context, vars any) (*Result, error) {
	return p.eval(ctx, vars)
}

func (p *Pool) eval(ctx context.Context, vars any) (*Result, error) {
	e := p.pool.Get().(*entry)
	if e.err != nil {
		// A failed plan is not returned to the pool; the next Get re-plans.
		return nil, e.err
	}
	e.rec.Reset()

	start := time.Now()
	var (
		val ref.Val
		det *cel.EvalDetails
		err error
	)
	if ctx == nil {
		val, det, err = e.prg.Eval(vars)
	} else {
		val, det, err = e.prg.ContextEval(ctx, vars)
	}
	elapsed := time.Since(start)

	res := &Result{
		Value:    val,
		Err:      err,
		Kind:     KindOf(val),
		Duration: elapsed,
		Cost:     det.ActualCost(),
		Probes:   e.rec.snapshot(),
	}
	if res.Kind == KindError {
		res.ErrorType = ErrorType(val, err)
	}
	e.rec.Reset()
	p.pool.Put(e)
	return res, nil
}
