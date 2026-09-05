# Dark-launch evaluation for cel-go

Status: design proposal
Target: `cel/darklaunch.go` (root module) + `github.com/google/cel-go/darklaunch` (new module)

## 1. Problem

An operator has a CEL expression running in production and a replacement they
believe is equivalent — a rewritten policy, an expression migrated to a new
extension library, the output of a new optimizer pass. Today the only options
are offline replay against a captured corpus (which never matches the live
input distribution) or a staged rollout (which exposes real traffic to the
change).

Dark launching closes that gap: the candidate expression runs against real
production input, its result is never returned to the caller, and the
divergence between control and candidate is measured until the operator has
enough evidence to promote it.

The evidence has to answer three questions:

1. **Behaviour** — how often, and in what way, does the candidate disagree?
2. **Cost** — what does the candidate do to latency and evaluation cost?
3. **Locality** — *where inside the expression* does the divergence originate?

Question 3 is what distinguishes this from a generic A/B harness, and it is the
reason the design reaches into the evaluator rather than sitting entirely on
top of `cel.Program`.

## 2. Non-goals

- Not a traffic router. Nothing here decides to promote a candidate; it
  produces the metrics a human or a rollout system uses to decide.
- Not an offline replay tool. The corpus is live traffic. (An offline runner
  over recorded `Record` values is a natural follow-on, not part of this.)
- Not a correctness proof. Coverage is whatever production traffic exercises.
- No changes to the semantics of any existing program. A program that is not
  dark-launched must plan and evaluate byte-for-byte as it does today.

## 3. Settled constraints

| Constraint | Decision |
|---|---|
| Execution | Asynchronous, off the request path, each candidate under its own `context.Context` with an independent budget |
| Input safety | Caller guarantees the input is immutable and safe for concurrent read for the lifetime of the async work |
| Packaging | `cel/darklaunch.go` holds probes + evaluator hooks; `darklaunch/` is a **separate Go module** holding the runner, comparator, records and sinks |
| Node alignment | Author-written `trace('name', <expr>)` — a real function, declared **non-strict** and **late-bound**, with an identity binding installed by the library |
| Divergence | Layered: value equality / normalized error class / unknown attribute set |
| Output | Per-evaluation `Record` to a `Sink`, plus an in-process aggregate snapshot |

## 4. Architecture

```
 request goroutine                      background worker pool
 ─────────────────                      ──────────────────────
 Runner.Eval(vars)
   │
   ├─ control.Eval(vars)  ── monotonic timer
   │     │
   │     └─► return (val, details, err) to caller   ◄── request path ends here
   │
   └─ sampled? ──► non-blocking send ──► job{vars, prodVal, prodErr, prodDur}
                        │ (full → drop, counted)          │
                                                          ▼
                                            ctx, cancel := WithTimeout(base, budget)
                                            shuffle([controlInstrumented, cand₁..candₙ])
                                            for each: ContextEval(ctx, vars)
                                                          │
                                                          ▼
                                              compare(controlOutcome, candᵢOutcome)
                                                          │
                                            ┌─────────────┴─────────────┐
                                            ▼                           ▼
                                    Aggregate (every job)        Sink (divergent
                                    atomic counters +             jobs + baseline
                                    histograms                    sample)
```

### 4.1 The control is evaluated twice, on purpose

The program that serves the request is untouched: no state tracking, no cost
tracking, no probe recording. Instrumenting it would (a) put dark-launch
overhead on the request path and (b) mean production traffic is served by a
differently-planned program than the one that ran before the experiment.

Instead the background worker re-runs an **instrumented copy of the control**
alongside the candidates, in the same goroutine, back to back. Three
consequences, all of them wanted:

- Latency comparison is apples to apples. Comparing a request-path measurement
  against a background measurement would mostly measure cache warmth and
  scheduler noise.
- The instrumented control and the candidates pay the same instrumentation
  tax, so the cost delta reflects the expression, not the observers.
- Comparing the *request-path* control result against the *background* control
  result is a free nondeterminism detector. If they disagree, the expression is
  not a pure function of its input — a custom function with side effects, a
  clock-reading binding, a mutated activation — and every other number in the
  report for that job is meaningless. Records carry a `Nondeterministic` flag
  and the aggregate counts them separately; a nonzero count invalidates the
  experiment rather than merely annotating it.

Candidate evaluation order is shuffled per job so no candidate systematically
benefits from a warm cache line or a populated regex cache.

### 4.2 Module boundary

```
cel/darklaunch.go                 (root module — needs planner/prog internals)
  ProbeLibrary()                  cel.SingletonLibrary providing trace()
  TrackProbes()                   ProgramOption, sets OptTrackProbes
  OptTrackProbes                  new EvalOption bit
  (*EvalDetails).Probes()         *interpreter.ProbeTrace

interpreter/probes.go             (root module)
  ProbeObserver()                 PlannerOption, mirrors CostObserver
  ProbeTrace                      ordered probe observations

darklaunch/  (github.com/google/cel-go/darklaunch, own go.mod)
  Runner, Option, Candidate
  Comparator, Verdict, Outcome, ProbeDiff
  Record, Sink
  Aggregate, Snapshot
```

A separate module — following the `tools/` precedent with a `replace` directive
during development — keeps the runner's dependencies (anything a sink or
exporter drags in, plus the histogram code) out of the dependency graph of
every project that merely uses `cel`. The root-module half is stdlib-only and
is the part that genuinely cannot live outside `cel`/`interpreter`, because it
touches `planner.decorators` and `prog.observable`.

The dependency edge points one way: `darklaunch` imports `cel`. Nothing in
`cel` learns about runners, sinks or records.

## 5. `trace()` — named probes

### 5.1 Shape

`trace` is a real, declared, type-checked function whose value is the identity
on its second argument. It is declared **non-strict** and **late-bound**:

```go
// cel/darklaunch.go — CompileOptions()
paramT := TypeParamType("T")
Function("trace",
    FunctionDocs("records the value of an expression for dark-launch analysis; returns it unchanged"),
    Overload("trace_string_dyn", []*Type{StringType, paramT}, paramT,
        OverloadExamples(`trace('is_member', request.user in allowed_users) // -> true`),
        OverloadIsNonStrict(),
        LateFunctionBinding()))
```

```go
// cel/darklaunch.go — ProgramOptions()
Functions(&functions.Overload{
    Operator:  "trace_string_dyn",
    Binary:    func(_, val ref.Val) ref.Val { return val },
    NonStrict: true,
})
```

It appears in the source text of both the control and the candidate, so the two
expressions stay comparable and both unparse cleanly:

```
control:    trace('member', user in allowed) && trace('fresh', age < duration('5m'))
candidate:  trace('fresh', age < duration('5m')) && trace('member', user in allowed_v2)
```

The comparator aligns `member` with `member` and `fresh` with `fresh` even
though the operands were reordered and the expression IDs share nothing.

### 5.2 Non-strict: the probe must be transparent to errors and unknowns

`evalBinary.Eval` (interpreter/interpretable.go:507) evaluates both arguments
and then, **only when strict**, returns early if either is an error or an
unknown — the implementation is never reached. A strict `trace` would therefore
have two distinct evaluation paths: identity-via-binding for ordinary values,
and an early return that bypasses the binding entirely for errors and unknowns.

Non-strict collapses that to one path. Every value, including `*types.Err` and
`*types.Unknown`, flows through the same identity binding, which makes "the
probe observes exactly what the node returns" a structural property rather than
a coincidence of two paths happening to agree today. It is also the honest
declaration: `trace` is an observation point and must never be the reason an
error or an unknown stops propagating.

Two mechanical notes:

- `OverloadIsNonStrict()` on the *declaration* and `NonStrict: true` on the
  *installed* `functions.Overload` are different switches. The planner reads
  `impl.NonStrict` from the dispatcher entry (interpreter/planner.go:365), and a
  late-bound declaration contributes no dispatcher entry of its own — so the
  declaration flag alone would be inert. Both must be set: the declaration for
  checker and documentation consistency, the installed overload for actual
  runtime behaviour.
- On the non-strict path the result passes through
  `types.LabelErrNode(bin.id, ...)`, which stamps the trace node's ID onto an
  error only when the error carries no ID yet (common/types/err.go:81). Errors
  raised by ordinary nodes are already labelled, so attribution is unchanged;
  an unlabelled error surfacing through a probe gains the probe's ID, which is
  the more useful attribution anyway.

### 5.3 Late binding: fold exemption for free, and a per-program swap point

`cel.NewConstantFoldingOptimizer` would otherwise fold `trace('n', 1 + 2)` to
`3` and erase the probe. The folder already skips late-bound calls —
`isLateBoundFunctionCall` (cel/folding.go:171) consults
`FunctionDecl.HasLateBinding()`, and the check is applied both to direct folds
(cel/folding.go:100) and inside comprehensions (cel/folding.go:521). Declaring
`trace` late-bound therefore buys the fold exemption with **no change to
cel/folding.go at all**, using a mechanism whose documented purpose — functions
that are side-effecting or not deterministically computable — describes `trace`
exactly.

Late binding also moves the implementation from a compile-time property of the
declaration to a per-program input, which is what makes an identity binding in
production and a recording binding under dark launch expressible at all (§5.5).

Three constraints come with it, all load-bearing:

- **`LateFunctionBinding()` and `BinaryBinding()` are mutually exclusive**
  (common/decls/decls.go:878), as is `SingletonBinaryBinding`
  (common/decls/decls.go:357). The declaration carries no implementation; the
  implementation is installed separately.
- **A missing binding is a runtime failure, not a compile failure.**
  `FunctionDecl.Bindings()` emits nothing for a late-bound overload
  (common/decls/decls.go:339), and `planCallBinary` accepts a nil
  implementation without error (interpreter/planner.go:359) — the program plans
  cleanly and then fails at evaluation with `no such overload: trace`. This is
  why the identity binding is supplied by the library's own `ProgramOptions()`
  rather than left to the caller: `cel.Lib(ProbeLibrary())` installs the
  declaration and the binding together, and there is no configuration in which
  one arrives without the other.
- **A binding cannot be layered over an existing one.**
  `defaultDispatcher.Add` rejects a duplicate operator
  (interpreter/dispatcher.go:63), so a second `cel.Functions()` call cannot
  override the identity binding. Any binding swap has to be chosen when the
  library is constructed — `ProbeLibrary(WithRecorder(rec))` — not bolted on
  afterwards.

### 5.4 Recording is a planner decorator

Recording lives in a `StatefulObserver` + decorator pair modelled on
`CostObserver` (interpreter/runtimecost.go:50), not in the binding, for one
reason: **a `functions.BinaryOp` receives no `Activation`.** Per-evaluation
state has to be reachable from the activation — that is how `EvalState` and
`CostTracker` stay concurrency-safe when one `Program` serves many simultaneous
evaluations — and a binding has no way to reach it. A recorder closed over by
the binding would be shared mutable state across concurrent evaluations of the
same program.

The decorator differs from `decObserveEval` in one deliberate way: that one
wraps every planned node, whereas `decObserveProbes` returns the node unchanged
unless it is an `InterpretableCall` named `trace`:

```go
func decObserveProbes(observer EvalObserver) InterpretableDecorator {
    return func(i Interpretable) (Interpretable, error) {
        call, ok := i.(InterpretableCall)
        if !ok || call.Function() != "trace" {
            return i, nil
        }
        return &evalWatch{Interpretable: i, observer: observer}, nil
    }
}
```

Probe tracing is therefore O(number of probes), not O(number of nodes) — the
whole reason for named probes rather than `OptTrackState` plus offline
alignment. Because the decorator wraps the node rather than the implementation,
it observes the node's result and so captures errors and unknowns regardless of
strictness; non-strict guarantees that result and the binding's input are the
same value on every path.

### 5.5 Alternative unlocked by non-strict + late binding: record in the binding

The two changes together make binding-based recording viable, and it is worth
stating explicitly because it would remove nearly all of the core plumbing:
late binding supplies the swap point (identity in production, recorder under
dark launch), and non-strict guarantees the recorder is invoked for errors and
unknowns rather than being bypassed. `interpreter/probes.go`, `OptTrackProbes`
and `EvalDetails.Probes()` would all disappear, leaving the `trace` declaration
as the only addition to the root module.

The blocker is the missing `Activation`, and there is exactly one way around
it: give each background worker its own program instances, so a recorder is
owned by a single goroutine and needs no synchronisation. With a fixed worker
pool that is `workers × candidates` plans held in memory, and programs are
planned once at candidate registration.

Recommendation: keep the decorator as the default. It costs four additive items
in the root module and buys shareable programs, a probe trace available to
anyone through `EvalDetails` for ordinary debugging, and no coupling between
the runner's concurrency model and its correctness. The binding-based variant
is the right answer if the root-module footprint turns out to be the sticking
point in review, and the declaration above supports either without change.

### 5.6 Constant names are enforced at compile time

A dynamic probe name gives unbounded metric cardinality and unstable
alignment. A `cel.ASTValidator` shipped with the library rejects any `trace`
call whose first argument is not a string literal, so the failure is a compile
error for everyone rather than a surprise when probes are switched on.

### 5.7 Probes inside comprehensions

A probe under `.all()` fires once per iteration. `ProbeTrace` records
observations in evaluation order with an occurrence index rather than
collapsing them, and the comparator compares the *sequence* per name and
reports hit-count divergence as its own signal — a candidate that produces the
right answer while iterating a different number of times is a finding, not
noise.


## 6. Input handling

The caller guarantees the input is immutable and safe for concurrent read
until the async work completes. That contract is cheap and it is the right
default, but it is not sufficient on its own, because `cel` itself recycles
activations.

`prog.Eval` wraps a `map[string]any` input in a pooled activation and returns
it on defer (cel/program.go:321), and `ContextEval` does the same with
`ctxEvalActivation` (cel/program.go:361). Handing either to a background
goroutine hands it an object that will be handed to another request. So:

- `Runner.Eval` never forwards the activation the control evaluated. It builds
  its own non-pooled `interpreter.NewActivation(vars)` over the caller's input
  and enqueues that. Under the immutability contract the underlying map is not
  copied, so this is one small allocation per sampled request, not a deep copy.
- Lazy `func() any` bindings are incompatible with the contract by
  construction: `NewActivation` overwrites the binding with the resolved value
  on first read, which is a write from a background goroutine into shared
  state. The runner detects lazy bindings when a job is enqueued and drops the
  job with a distinct `DroppedUnsafeInput` counter rather than racing.
- `PartialActivation` inputs are forwarded as such so unknown propagation is
  preserved; the comparator has an unknown-aware layer for exactly this.
- The request's `context.Context` is dead by the time the worker runs. Each job
  gets `context.WithTimeout(runner.baseCtx, budget)`, and candidates are
  evaluated with `ContextEval`. This only interrupts if candidate programs are
  built with `cel.InterruptCheckFrequency`, which is why the runner compiles
  candidates itself (§8).

## 7. Comparison semantics

An `Outcome` is what one program produced: a value, an error, or an unknown,
plus duration, actual cost and probe trace.

Equality is layered and never itself errors:

```go
func equal(a, b ref.Val) bool {
    if types.IsUnknown(a) || types.IsUnknown(b) { ... }   // unknown layer
    if types.IsError(a) || types.IsError(b)     { ... }   // error layer
    eq, ok := a.Equal(b).(types.Bool)                      // value layer
    return ok && bool(eq)
}
```

- **Values.** `ref.Val.Equal` returns a `ref.Val`, and a cross-type comparison
  returns an error rather than `false`. Anything that is not `types.True` is a
  divergence — an `int` result where the control produced a `string` is
  reported as `ValueDiffer`, never as a comparator failure.
- **Errors.** Compared by normalized *class*, not message text: message strings
  are not part of the CEL contract and change between releases. The classifier
  buckets on `errors.Is` against exported sentinels plus prefix normalization
  for the common runtime errors (`no such key`, `no such overload`,
  `division by zero`, `index out of range`, integer overflow). This is
  best-effort, and honestly so: cel-go has no stable runtime error taxonomy
  today, which is why the classifier lives in the `darklaunch` module where it
  can evolve without an API commitment. A typed error kind on `types.Err` in
  core would make it exact and is worth proposing separately.
- **Unknowns.** Compared by attribute trail, never by expression ID —
  `*types.Unknown` is keyed by expr ID (common/types/unknown.go:148) and IDs
  are meaningless across two ASTs. `AttributeTrail.String()` gives a
  stable, AST-independent key.

Verdicts:

```
Match                  ValueDiffer         ErrorClassDiffer
ControlErrorOnly       CandidateErrorOnly  UnknownDiffer
CandidateTimeout       CandidateCostLimit  CandidatePanic
```

The split between `ControlErrorOnly` and `CandidateErrorOnly` is the point:
"the candidate now errors where the control returned false" and "the candidate
returns false where the control errored" are opposite risks and must never
share a bucket.

Probe comparison runs independently of the top-level verdict, because the
interesting failure is the candidate that agrees on the final answer for the
wrong reason. Per probe name the comparator emits: control-only, candidate-only
(coverage divergence — a branch one side never reached), hit-count mismatch, or
a value verdict using the same layered equality.

## 8. Candidates are compiled by the runner

The runner accepts an `*cel.Env` and a `*cel.Ast`, not a finished
`cel.Program`, and appends the options the harness depends on after the user's:

```go
func NewCandidate(name string, env *cel.Env, a *cel.Ast, opts ...cel.ProgramOption) (*Candidate, error)
// appends: cel.TrackProbes(), cel.EvalOptions(cel.OptTrackCost),
//          cel.InterruptCheckFrequency(n), cel.CostLimit(limit)
```

Without the cost limit a pathological candidate burns a worker indefinitely;
without the interrupt frequency the context deadline is decorative. Accepting a
pre-built `Program` would make both silently optional. The instrumented control
is compiled the same way from the control's own env and AST.

Candidates may come from a different `*cel.Env` than the control — dark
launching an expression against a new extension library is a primary use case —
so every `Record` carries the candidate name and the comparator makes no
assumption that the two programs share a type environment.

## 9. Output

### 9.1 Records and sinks

```go
type Sink interface {
    Emit(context.Context, *Record)
}
```

One `Record` per candidate per sampled job, carrying both outcomes, the
verdict, probe diffs, and the nondeterminism flag. Every sampled job updates
the aggregate; only divergent jobs plus a low-rate baseline sample carry the
full probe payload to the sink, so a candidate that agrees 99.99% of the time
costs almost nothing in sink volume while every disagreement is captured in
full.

Input capture is **opt-in and off by default**, and takes a
`func(any) any` redactor when enabled. The whole value of a divergence record
is being able to reproduce it, and the whole risk is that production CEL inputs
are exactly where the PII lives. Making the caller write the redactor is the
only defensible default.

Sink calls run in the worker with their own `recover`. A sink that panics
degrades the experiment, never the process.

### 9.2 Aggregate

`Runner.Snapshot()` returns a point-in-time copy: per candidate, counts by
verdict, agreement rate, latency and cost histograms (fixed buckets, atomic
counters, no locks on the hot path) for control and candidate, per-probe
agreement rates and hit counts, plus `Dropped`, `DroppedUnsafeInput` and
`Nondeterministic`. The snapshot is per-counter atomic and best-effort
consistent across counters; a report that needs an exactly consistent cut can
be built from the record stream instead.

No verdict, no threshold, no recommendation. The runner reports; the operator
decides. Turning a snapshot into a promote/hold decision is a policy question
that varies per deployment and belongs in whatever owns the rollout — a thin
`Report` helper over `Snapshot` can be added later without disturbing anything
here.

## 10. Safety

Overload never reaches the request path. `Runner.Eval` does a non-blocking send
to a bounded queue and drops on full, counting the drop; the request path is
one timer read, one sample check and one channel send in the worst case, and a
sample check alone in the common case. Sampling is a rate plus an optional
`func(any) bool` predicate for targeting specific traffic.

Every candidate evaluation is bounded three ways — context deadline, cost
limit, and its own `recover` — and worker count is fixed and configurable.
`Runner.Close(ctx)` stops accepting work and drains within the caller's
deadline. A runner with zero candidates, or with sampling at zero, is a
pass-through with no worker pool: the kill switch is the absence of work, not a
branch inside the evaluator.

## 11. Required changes in the root module

1. `interpreter/probes.go` — `ProbeObserver()` PlannerOption, `ProbeTrace`
   state, `decObserveProbes` decorator. Mirrors the `CostObserver` structure
   exactly; no change to `StatefulObserver` or `ObservableInterpretable`.
2. `cel/options.go` — `OptTrackProbes` EvalOption bit; wire it in
   `newProgram`'s observer block alongside `OptTrackState`/`OptTrackCost`.
3. `cel/program.go` — `probes` field on `EvalDetails`, populated from the
   existing `ObserveEval` callback switch; `Probes()` accessor.
4. `cel/darklaunch.go` — the `trace` library (declaration, identity binding,
   validator) and `TrackProbes()`.

All four are additive. No existing program plans differently unless it opts in.

Nothing in `cel/folding.go` changes: declaring `trace` late-bound reuses the
folder's existing late-binding exemption (§5.3). Adopting the binding-based
variant in §5.5 would reduce this list to item 4 alone.

## 12. Phasing

1. **Probes in core** (items 1–4 above) with unit tests covering: probes under
   comprehensions; probes wrapping subexpressions that produce errors and
   unknowns, asserting the value is unchanged and the probe still fires; a fold
   test proving `trace` survives `NewConstantFoldingOptimizer` with constant
   arguments; and a negative test that a program built without the library's
   `ProgramOptions()` fails at evaluation rather than silently dropping probes
   (§5.3).
2. **`darklaunch` module skeleton**: `Runner`, synchronous execution only,
   comparator, `Record`, a logging sink. Synchronous first keeps the comparator
   under test without the concurrency surface.
3. **Async execution**: worker pool, sampling, bounded queue, deadlines,
   nondeterminism detection.
4. **Aggregate and snapshot**, plus a worked example under `examples/`.
5. **Optional**: an `ASTOptimizer` that injects probes into an un-annotated AST
   so an existing production expression can be dark-launched without editing
   its source. Explicitly deferred — it only makes sense once the hand-annotated
   path has proven which probe placements are worth having.

## 13. Open questions

- Should the instrumented control's probe trace be compared against the
  *request-path* control at all, beyond the final value? Doing so would catch
  nondeterminism inside a subexpression whose effect cancels out at the top
  level, at the cost of instrumenting the request path.
- Is one `trace` overload enough, or is a zero-cost `trace(name, expr, tags)`
  form wanted for slicing metrics by request attributes? Deferred until the
  basic form is in use.
- Cost comparison assumes both programs share a cost estimator. Candidates from
  a different env may not. The record should probably carry the estimator
  identity so incomparable costs are dropped rather than charted.
