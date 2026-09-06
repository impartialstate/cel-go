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
  `division by zero`, `index out of range`, integer overflow). cel-go has no
  stable runtime error taxonomy today, but that is not the blocker it first
  appears: OTel's `error.type` requires a documented, closed, low-cardinality
  list with an `_OTHER` fallback rather than an exhaustive mapping, and the
  class list is fixed to that shape in §10.5. The classifier still lives in the
  `darklaunch` module so it can evolve without an API commitment; a typed error
  kind on `types.Err` in core would make it exact and is worth proposing
  separately.
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

## 10. OpenTelemetry signal mapping

Sources read on 2026-09-06 from `open-telemetry/semantic-conventions@main`
(opentelemetry.io is unreachable from this environment; paths below are
repository paths). Stability labels are the spec's own.

### 10.1 What OTel already defines, and what it does not

`feature_flag.*` (`docs/registry/attributes/feature-flag.md`, **Release
Candidate**) is a near-exact structural fit for a policy decision:

| Attribute | Notes |
|---|---|
| `feature_flag.key` | lookup key |
| `feature_flag.set.id` | flag set the key belongs to |
| `feature_flag.version` | version of the **ruleset** used in the evaluation |
| `feature_flag.context.id` | evaluation-context id, "for example, the targeting key" |
| `feature_flag.result.variant` | semantic identifier for the evaluated value |
| `feature_flag.result.value` | the evaluated value, any type |
| `feature_flag.result.reason` | closed enum: `cached`, `default`, `disabled`, `error`, `split`, `stale`, `static`, `targeting_match`, `unknown` |
| `feature_flag.error.message` | human-readable error detail |

Four older names are deprecated and must not be emitted: `feature_flag.variant`,
`feature_flag.evaluation.reason`, `feature_flag.provider_name`,
`feature_flag.evaluation.error.message`.

Three absences matter more than the presences:

- **There are no feature-flag metrics.** `docs/feature-flags/` contains only
  `README.md` and `feature-flags-events.md`; `model/feature-flags/` contains
  only `events.yaml`, `registry.yaml` and a `deprecated/` directory. Any
  aggregate is ours to name.
- **There is no span convention any more.** The evaluation is defined as an
  event: `feature_flag.evaluation` (RC), whose attributes "SHOULD be recorded as
  attributes on the Event passed to the Logger emit operations"
  (`docs/feature-flags/feature-flags-events.md`). A prior spans convention
  existed and is gone. High-volume sub-millisecond evaluations belong on the
  log/event path, not the span path, and OTel has already made that call.
- **There is no shadow, candidate or dark-launch concept anywhere.** Nothing in
  the registry expresses "this result was computed and discarded" (§10.4).

### 10.2 `result.reason` is about the provider, not about the policy

The enum answers *how the provider resolved a value* — cached, stale, static,
split, targeting matched. It does not answer *why the policy decided what it
decided*. Emitting a matched CEL rule name into `feature_flag.result.reason`
would be a misuse that also blows up its cardinality.

The mapping that is actually defensible is narrow:

| Evaluator state | `feature_flag.result.reason` |
|---|---|
| Result came from a constant-folded / fully static plan | `static` |
| Policy fell through to its default branch | `default` |
| Evaluation produced `*types.Err` | `error` |
| Ordinary evaluation over input | `targeting_match` |

And one state has no member at all: a **partial evaluation returning
`*types.Unknown`** is neither a value nor an error. `unknown` exists in the enum
but means "reason unknown", not "result unknown" — using it would be a second
misuse. Unknown-ness therefore needs its own attribute (§10.4), and this gap is
worth raising upstream: partial evaluation is not CEL-specific, and any flag
provider that supports deferred resolution has it.

*Which rule matched* has a better existing home: `security_rule.name` and
`security_rule.ruleset.name` (`docs/registry/attributes/security-rule.md`,
**Development**). The namespace is framed for "security monitoring and detection
systems", which fits authorization and admission policies and does not fit
routing or data-filtering policies. Recommendation: use it for the authz and
admission archetypes, where the framing is honest, and do not stretch it to the
others.

### 10.3 Signals a range of common policies should capture

Every archetype below shares the same core: policy identity and version, a
low-cardinality decision label, a normalized error class, duration, cost, and
whether the result was unknown. What differs is the decision label's domain and
the subject attributes worth attaching.

| Policy archetype | Decision label (`feature_flag.result.variant`) | Additional signals | Existing OTel home |
|---|---|---|---|
| Authorization (RBAC/ABAC) | `allow` / `deny` | matched rule, subject roles, resource, action | `security_rule.name`, `user.roles`, `enduser.pseudo.id` |
| Admission / validation | `valid` / `invalid` | violation count, first failing field path | `error.type`, custom `cel.violations` |
| Rate limit / quota | `permit` / `throttle` | limit, remaining | no OTel home; custom |
| Routing / traffic selection | route name | candidate set size | `feature_flag.result.variant` fits natively |
| Data filtering / redaction | `full` / `filtered` / `denied` | fields redacted count | custom; never the field values |
| Feature gating | variant name | — | `feature_flag.*` verbatim |

Subject identity: `user.*` and `enduser.*` are all **Development** stability.
Prefer `enduser.pseudo.id`, explicitly defined as a random value not linked to
the real identity, over `enduser.id`, which the registry marks as containing
PII. `user.roles` (`string[]`) is the replacement for the deprecated
`enduser.role`. Note that `enduser.scope` was deprecated with **no
replacement** — OAuth-scope-driven policies have no standard attribute today,
and inventing `cel.subject.scopes` is preferable to resurrecting a deprecated
name.

Stage: `deployment.environment.name` is **Stable** and is the right partition
for "is this staging or production". There are no canary or rollout attributes
in `deployment.*`, which is a second reason the dark-launch role attribute in
§10.4 has to be ours.

### 10.4 Dark-launch attributes: the ones OTel cannot supply

```
cel.evaluation.role      string   required   control | candidate | shadow_control
cel.evaluation.applied   boolean  required   false when the result was discarded
cel.result.kind          string   required   value | error | unknown
cel.divergence.verdict   string   cond. req. match | value_differ | error_class_differ |
                                             control_error_only | candidate_error_only |
                                             unknown_differ | candidate_timeout |
                                             candidate_cost_limit | candidate_panic
cel.evaluation.cost      int      recommended
cel.probe.name           string   cond. req.  on per-probe records only
cel.expression.id        string   recommended stable identity across control and candidates
```

`cel.evaluation.applied` is the safety-critical one and is why it is *required*
rather than recommended. Without it a shadow evaluation is indistinguishable in
the backend from a real decision, and someone will eventually build an alert, a
dashboard, or — worst — an audit trail on discarded results. A required boolean
that is `false` for every candidate makes the mistake impossible to make
silently.

`cel.result.kind` covers the `unknown` gap from §10.2 and makes
"value / error / unknown" a first-class partition, matching the layered
comparison in §7 exactly.

Namespace choice: `otel.*` is reserved to the specification
(`docs/general/naming.md`), and the guidance for organization-specific names is
reverse-domain or unique-application prefixing. `cel.*` is unregistered and
therefore a mild squat, justified by CEL being a named language rather than one
organization's product, and by the intent to propose it upstream. The risk is a
future OTel `cel.*` with different semantics; the mitigation is that everything
above is emitted through a `Sink` the user controls, so a rename is a
one-file change in the exporter rather than a change to the runner.

### 10.5 `error.type` resolves the error-taxonomy problem from §7

`error.type` (`docs/registry/attributes/error.md`) is **Stable** and its
guidance is directly usable: it "SHOULD be predictable, and SHOULD have low
cardinality", the well-known fallback is `_OTHER`, and instrumentation libraries
"SHOULD document their error reporting lists".

That is the shape the §7 classifier needed. cel-go's lack of a stable runtime
error taxonomy stops being a blocker, because the requirement is not an
exhaustive mapping — it is a **documented, closed, low-cardinality list plus
`_OTHER`**. The comparator's error classes become that list:
`no_such_key`, `no_such_overload`, `no_such_attribute`, `division_by_zero`,
`index_out_of_range`, `overflow`, `interrupt`, `cost_limit`, `_OTHER`. An
unrecognized message falls to `_OTHER` rather than to a raw string, which caps
cardinality by construction. Two candidates whose errors both classify as
`_OTHER` are reported as `error_class_differ` only if their raw messages
differ, and the raw message goes in `feature_flag.error.message`, which carries
no cardinality obligation because it is an event attribute rather than a metric
dimension.

### 10.6 Never emit the result value by default

The events convention warns that flag results "can be quite large or contain
private or sensitive details", tells instrumentation authors to "redact or
otherwise limit the size and scope" of `feature_flag.result.value`, and makes
`result.variant` the preferred attribute because it conveys meaning without the
value. `result.value` is Conditionally Required — required only when the
provider supplies no variant, and **opt-in** otherwise.

This is the same conclusion §9.1 reached independently, and the spec's framing
sharpens it: the default emission is the low-cardinality decision label in
`feature_flag.result.variant`, and the full `ref.Val` reaches
`feature_flag.result.value` only on divergence, only when a redactor is
installed, and never as a metric dimension. Policy inputs are exactly where
PII lives, so this is not a formality.

The probe-name validator in §5.6 gains a second justification here: probe names
become metric attributes, and OTel's cardinality rules make a dynamic name a
defect rather than merely inconvenient.

### 10.7 Instruments

No counter is proposed alongside the duration histogram. A histogram already
carries a count, and the attribute set below distinguishes success from error,
so a separate `cel.evaluations` counter would be redundant. Naming follows
`docs/general/naming.md`: no `_total` suffix, plural only for discrete countable
quantities with a `{unit}` annotation, and `.duration` for elapsed time.

```
cel.evaluation.duration            Histogram   s              per evaluation
  attrs: feature_flag.key, feature_flag.set.id, cel.evaluation.role,
         cel.result.kind, error.type, deployment.environment.name

cel.evaluation.cost                Histogram   {cost}         actual cost, when tracked
  attrs: as above

cel.darklaunch.comparisons         Counter     {comparison}   one per candidate per job
  attrs: feature_flag.key, cel.candidate.name, cel.divergence.verdict

cel.darklaunch.probe.comparisons   Counter     {comparison}   one per probe per candidate
  attrs: feature_flag.key, cel.candidate.name, cel.probe.name,
         cel.divergence.verdict

cel.darklaunch.dropped             Counter     {evaluation}   queue-full and unsafe-input drops
  attrs: feature_flag.key, cel.drop.reason
```

Agreement rate is `verdict=match` over the total of
`cel.darklaunch.comparisons`, so a single instrument answers the rollout
question without a derived metric. Probe comparisons are a separate instrument
because probe name multiplies the cardinality of every other dimension.

### 10.8 Trace context across the async boundary

The per-evaluation event belongs on the log path, but a background job may still
warrant a span — and it must not be a child of the request span, which has
already ended. The Tracing API defines Links as references to `SpanContext`s
"from the same or a different trace"
(`opentelemetry-specification/specification/trace/api.md`), which is the right
primitive for work that is causally related to a finished operation.

One constraint from that spec falls straight onto our design: adding links at
span creation "is preferred to calling `AddLink` later ... because head sampling
decisions can only consider information present during span creation". The
originating `SpanContext` therefore has to be captured at **enqueue** time, on
the request goroutine, and carried in the job struct — it cannot be recovered in
the worker, where the request context is already dead. That is one more field on
the job, and it has to be added when the job is built, not when it runs.

## 11. Safety

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

## 12. Required changes in the root module

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

## 13. Phasing

1. **Probes in core** (items 1–4 of §12) with unit tests covering: probes under
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

## 14. Open questions

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
- `feature_flag.*` is Release Candidate, not Stable, and four of its attribute
  names were renamed in the last revision (§10.1). Adopting it now means
  tracking one more rename before it settles. The alternative — an entirely
  private namespace — trades that churn for permanent incompatibility with every
  flag-aware backend, which seems the worse deal, but it is a call worth making
  explicitly rather than by default.
- Two gaps look worth proposing upstream rather than solving only here: no
  `result.reason` member for a partial or unknown result (§10.2), and no
  attribute anywhere for a shadow evaluation whose result was discarded
  (§10.4). Neither is CEL-specific.
