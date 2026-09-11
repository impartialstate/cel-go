# darklaunch (proof of concept)

Captures named probe traces and OpenTelemetry-shaped evaluation metrics from
CEL programs, using nothing but a non-strict, late-bound `trace()` function.
**No changes to `cel/`, `interpreter/` or any other package in the repository.**

See [DESIGN.md](DESIGN.md) for the design this proves out. This directory
implements §5.5 of that document — the variant where recording happens in the
late binding — rather than the planner-decorator default in §5.4.

## What it demonstrates

```go
env, _ := cel.NewEnv(darklaunch.ProbeDecls(), cel.Variable("role", cel.StringType))
a, _ := env.Compile(`trace('is_admin', role == 'admin')`)

pool, _ := darklaunch.NewPool(env, a)          // instrumented
res, _ := pool.Eval(map[string]any{"role": "owner"})

res.Probes   // [{Name: "is_admin", Seq: 0, Kind: value, Value: false}]
res.Kind     // value | error | unknown
res.Duration // wall time
res.Cost     // tracked evaluation cost

prg, _ := darklaunch.NewProgram(env, a)        // production: identity, no recording
```

Run `go test ./darklaunch/ -race` and `go test ./darklaunch/ -run Example -v`.

| Claim | Test |
|---|---|
| `trace()` does not change the value of an expression | `TestTraceIsIdentity` |
| Probes record name, order and result kind | `TestProbeCaptureOrder` |
| A probe in a short-circuited branch does not fire | `TestProbeShortCircuit` |
| Non-strict: a probe over an erroring subexpression still fires, and the error propagates | `TestProbeCapturesError` |
| Unknowns are captured as their own result kind | `TestProbeCapturesUnknown` |
| A probe under a comprehension fires once per iteration | `TestProbeInComprehension` |
| Late binding keeps the constant folder off probes | `TestTraceSurvivesConstantFolding` |
| A missing binding fails at evaluation, not at plan time | `TestMissingBindingFailsAtEvaluation` |
| Probe names must be string literals | `TestConstantProbeNames` |
| Concurrent evaluation through `Pool` is race-free | `TestPoolConcurrentEvaluation` (`-race`) |
| Error classes match the messages cel-go actually emits | `TestErrorTypeFromRealEvaluations` |
| Metrics carry the OTel names, units and attribute sets | `TestAggregateSeries` |

## Why `Pool` exists

A `functions.BinaryOp` receives no `Activation`, so a recording binding cannot
reach per-evaluation state: it is shared by every concurrent evaluation of the
program it is bound to. `Pool` never shares one — each pooled entry owns a
program and the recorder bound into it, an entry is checked out by one
goroutine at a time, and observations are copied out before it is returned.

That constraint is real, not theoretical. Sharing one program and recorder
across goroutines reproduces a data race under `-race`; `TestPoolConcurrentEvaluation`
does the same work through the pool and is clean.

## Known gaps

- **In the root module, not its own.** DESIGN.md §4.2 puts this in a separate
  Go module. The development environment has an empty module cache and the root
  module vendors its dependencies, so a separate module cannot resolve them
  offline. Splitting it is mechanical and should happen before this is real.
- **No comparator or runner.** This captures signals; it does not yet compare a
  control against candidates (DESIGN.md §7) or schedule the asynchronous work
  (§4). `Result` carries everything a comparator needs.
- **No OTel SDK export.** `Aggregate.Snapshot` returns instrument-shaped
  structs. Feeding them to a real meter is a small adapter, deliberately kept
  out so the package has no dependencies.
- **Error classification is by message prefix.** cel-go's error sentinels are
  unexported and its messages are not part of its compatibility contract.
  Unrecognized messages collapse to `_OTHER`, so a miss costs a bucket, never a
  wrong class.
