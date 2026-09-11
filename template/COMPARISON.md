# CEL templates compared with Go text/template, Jinja, and Helm + Sprig

This document compares the `template` package in this repository with the three template
systems it is most likely to be measured against:

* **Go `text/template`** (and its sibling `html/template`) — the Go standard library.
* **Jinja** (Jinja2/Jinja 3, Python) — and, by extension, its many ports.
* **Helm + Sprig** — Go `text/template` plus the Sprig function library and Helm's own
  additions, as used to render Kubernetes manifests.

The short version: all four share the same surface syntax, and the differences are almost
entirely about *what the system knows before it runs*. Go `text/template` and Jinja are
dynamically typed languages that discover problems while producing output. CEL templates push
the same work to compile time, because CEL is a checked expression language with a declared
type environment.

## At a glance

| | CEL templates | Go text/template | Jinja | Helm + Sprig |
| --- | --- | --- | --- | --- |
| Expression language | CEL | pipelines + funcs | Python-like sublanguage | pipelines + ~200 Sprig funcs |
| Types known before rendering | **yes, declared and checked** | no | no | no |
| Undeclared variable | compile error | `<no value>` / runtime error | empty (or error with `StrictUndefined`) | `<no value>` / nil deref |
| Misspelled field | compile error | `<no value>` / runtime error | empty or runtime error | runtime error, often far from the cause |
| Wrong argument type | compile error | runtime error | runtime error | runtime error |
| Truthiness | **none — conditions must be `bool`** | empty/zero is false | empty/zero is false | empty/zero is false |
| Arithmetic | built in | via functions | built in | via functions (`add`, `mul`, …) |
| Sub-template parameters | **typed, named, checked** | one untyped `.` | positional + keyword, untyped | one untyped `.` |
| Sub-template result usable in an expression | **yes, it is a function call** | no (`include` exists only in Helm) | yes (macros) | yes, via Helm's `include` |
| Sub-template scope | **only its parameters** | whatever `.` is | closure over globals | whatever `.` is, plus `$` |
| Recursion | rejected at compile time | allowed (depth-capped) | allowed | allowed |
| Guaranteed to terminate | **yes** | no | no | no |
| Deterministic output | **yes** | mostly (map keys sorted) | depends on functions | **no** (`randAlphaNum`, `uuidv4`, `now`, `lookup`) |
| Map iteration order | sorted | sorted | insertion order | sorted |
| Contextual auto-escaping | no (single pluggable escaper) | **yes, in `html/template`** | no (HTML autoescape only) | no |
| Template inheritance (`extends`/`block`) | no | partial (`block`) | **yes** | partial (`block`) |
| Sandbox story | **non-Turing-complete, declared functions only, cost-limited** | any registered Go func | opt-in sandbox over a Turing-complete host | any Sprig/Helm func, incl. cluster access |
| Host languages | Go (CEL itself: Go, Java, C++, Python, Rust) | Go | Python (+ports) | Go |
| Maturity / ecosystem | new | very mature | very mature | very mature |

## The same template, four ways

Render a list of order items with a running total, skipping anything out of stock.

**CEL templates**

```
{{ define line(name string, qty int, price double) }}
{{ name }} x{{ qty }} = {{ qty * int(price) }}
{{- end }}
{{ let inStock = order.items.filter(i, i.qty > 0) }}
{{ for i in inStock }}
{{ line(i.name, i.qty, i.price) }}
{{ else }}
Nothing in stock.
{{ end }}
Total: {{ sum(inStock.map(i, i.qty * int(i.price))) }}
```

**Go text/template**

```
{{ define "line" }}{{ .name }} x{{ .qty }} = {{ mul .qty .price }}{{ end }}
{{ $total := 0 }}
{{ range .Items }}
  {{ if gt .qty 0 }}
    {{ template "line" . }}
    {{ $total = add $total (mul .qty .price) }}
  {{ end }}
{{ else }}
Nothing in stock.
{{ end }}
Total: {{ $total }}
```

`mul` and `add` have to be supplied in a `FuncMap`; the standard library has no arithmetic.
`{{ template "line" . }}` passes the current dot, which the sub-template accesses by field name
with nothing checking that the fields exist.

**Jinja**

```
{% macro line(item) %}{{ item.name }} x{{ item.qty }} = {{ item.qty * item.price }}{% endmacro %}
{% set in_stock = order['items'] | selectattr('qty', '>', 0) | list %}
{% for i in in_stock %}
{{ line(i) }}
{% else %}
Nothing in stock.
{% endfor %}
Total: {{ in_stock | sum(attribute='qty') }}
```

Closest in spirit: macros are callable from expressions, and the expression language is
expressive. Everything is still discovered at render time.

**Helm + Sprig**

```
{{- define "chart.line" -}}
{{ .name }} x{{ .qty }} = {{ mul .qty .price }}
{{- end -}}
{{- $total := 0 -}}
{{- range .Values.order.items }}
{{- if gt (.qty | int) 0 }}
{{ include "chart.line" . }}
{{- $total = add $total (mul .qty .price) }}
{{- end }}
{{- end }}
Total: {{ $total }}
```

The `-}}` markers everywhere are not stylistic: YAML output is whitespace sensitive and every
control line would otherwise emit a blank line.

## Where errors surface

This is the difference that matters most in practice.

Given a typo in a field name, each system behaves differently:

| System | Result |
| --- | --- |
| CEL templates | `deployment.yaml:12:18: undeclared reference to 'imgae' (in container '')` — at compile time, before any rendering |
| Go text/template | `<no value>` for a map key; `can't evaluate field imgae` at execution for a struct |
| Jinja | empty string, unless the environment is configured with `StrictUndefined` |
| Helm | `<no value>`, or `nil pointer evaluating interface {}.repository` several lines later |

CEL templates report *every* problem in *every* source at once, ordered by position:

```
deployment.yaml:12:18: undeclared reference to 'imgae' (in container '')
deployment.yaml:19:7: condition must be a bool, found string; did you mean values.tag != ""
service.yaml:4:24: found no matching overload for 'port' applied to '(string)'
```

The cost is that you must declare what the inputs are:

```go
template.NewCompiler(
    template.Variable("values", cel.ObjectType("mychart.Values")),
)
```

CEL takes types from a `types.Provider`, so the declaration can come from a protobuf message,
a Go struct via `ext.NativeTypes`, or a hand-written `cel.MapType`. This is real up-front work
that Helm's "values are whatever the user's YAML contained" model does not ask for. It is also
precisely the work that makes a chart's interface checkable instead of folkloric.

## Composition and scope

Every one of these systems lets you factor out a reusable fragment. They differ in what that
fragment is allowed to see.

* **Go `text/template`** — `{{ template "name" pipeline }}` passes one value, the dot. There is
  no parameter list, so a sub-template's contract lives in a comment, if anywhere. The output
  cannot be captured into a variable, which is the entire reason Helm invented `include`.
* **Helm** — `{{ include "name" . }}` returns a string, so it composes with `nindent` and
  friends. Charts conventionally pass the whole root context (`.`) because a sub-template
  usually needs `.Release` *and* `.Values` *and* its own arguments, so sub-templates end up
  coupled to the entire chart. Passing something else is done with `dict "a" $x "b" $y`, which
  is an untyped bag of names.
* **Jinja** — macros have real parameters, including defaults and `varargs`, and are callable
  from expressions. They also close over the template's global namespace, so a macro can
  silently depend on a global that happens to exist.
* **CEL templates** — `{{ define row(name string, qty int) }}` is a typed signature. Calling it
  is a CEL function call, so the result is a string you can pass to `indent`, embed in a
  comprehension, or return from a conditional. The body sees its parameters and nothing else,
  so the signature is the whole contract.

```
{{ order.items.map(i, row(i.name, i.qty)).join("\n").nindent(4) }}
```

The tradeoff: CEL sub-templates have no default or variadic parameters, and **no template
inheritance**. If you are laying out HTML pages, Jinja's `{% extends %}` / `{% block %}` is a
better fit than anything offered here.

## Control flow

Broadly equivalent, with a few differences worth noting.

* **Conditions must be `bool`.** `{{ if user.name }}` is a compile error, not a condition that
  is quietly true for every non-empty name. The error suggests the comparison you meant. This
  removes a whole family of bugs that the other three share, at the cost of being wordier.
* **`{{ for }}` binds names, not a dot.** `{{ for i, item in items }}` keeps the enclosing
  scope visible, so there is no equivalent of Helm's `$` escape hatch for reaching outside a
  `range` block.
* **`{{ else }}` on an empty range** matches Go and Jinja.
* **`{{ break }}` / `{{ continue }}`** are built in. Go added them in 1.18; Jinja needs the
  `loopcontrols` extension.
* **No `loop.index`/`loop.first`.** Jinja's loop object has no equivalent; use the two-variable
  form and compare against `size(xs) - 1`.
* **`{{ let }}` is a binding, not a mutable variable.** Go's `$x = ...` reassignment inside a
  `range` (used above to accumulate a total) has no counterpart. Aggregate with an expression
  instead: `sum(items.map(i, i.qty))`. This is the single biggest adjustment for someone coming
  from Go templates, and it is a deliberate consequence of using an expression language rather
  than a statement language.

## Whitespace

Whitespace handling is where template languages are quietly judged, because YAML and code care.

* Go `text/template` offers only `{{-` and `-}}`. Every control action sits on a line that will
  be emitted unless trimmed, which is why Helm charts are dense with `{{- ... -}}`.
* Jinja offers `{%-`/`-%}` plus the `trim_blocks` and `lstrip_blocks` environment options,
  which are off by default.
* CEL templates remove standalone control lines **by default**: a line containing nothing but
  `{{ if }}`, `{{ end }}`, `{{ for }}`, `{{ let }}`, `{{ define }}` or a comment disappears
  along with its newline. `{{-` and `-}}` remain available for everything else, and
  `StandaloneLines(false)` restores `text/template` behavior.

```
items:
{{ for i in xs }}
  - {{ i }}
{{ end }}
```

renders as `items:\n  - 1\n  - 2\n`, with no markers and no blank lines.

## Missing values

| System | `{{ a.b }}` where `b` is absent |
| --- | --- |
| CEL templates | render fails: `no such key: b` |
| Go text/template | `<no value>` (map) or an execution error (struct) |
| Jinja | empty string by default |
| Helm | `<no value>`, or a nil dereference further down |

Silently rendering nothing is the worst of the options for generated configuration: a missing
image tag becomes a valid-looking manifest that deploys the wrong thing. CEL templates make the
absence explicit at the point you intend it:

```
{{ if has(cfg.replicas) }}replicas: {{ cfg.replicas }}{{ end }}
replicas: {{ cfg.?replicas.orValue(1) }}
```

Helm's `required "message" .Values.x` is the same instinct, applied one value at a time.

## Escaping and injection

Here Go's standard library is straightforwardly ahead.

`html/template` performs *contextual* auto-escaping: it parses the HTML being produced and
chooses HTML, attribute, URL, CSS, or JavaScript escaping per interpolation site. Jinja's
autoescape is HTML-only and not contextual. Helm does not escape at all; `quote` and
`toYaml` are applied by hand, and forgetting them is a routine source of broken manifests.

CEL templates take the Jinja-shaped position: a single pluggable `Escape` function applied to
every interpolated value, with `raw()` and sub-template results passing through. That is enough
for YAML, JSON, and shell output where the rule is uniform, and it is **not** a substitute for
`html/template` if you are generating HTML from untrusted input.

## Determinism, purity, and sandboxing

For configuration generation — the Helm use case — this is the second big differentiator.

A CEL template is a pure function of its inputs:

* There is no `now()`, no `uuidv4()`, no `randAlphaNum()`. If a template needs the time, it is
  declared as a `timestamp` variable and supplied by the caller.
* There is no file, network, or cluster access. Helm's `lookup` queries a live cluster during
  rendering; `Files.Get` reads the chart. Neither has an equivalent here.
* Map iteration is key-sorted, and `toJSON` / `toYAML` sort keys.

The practical consequence: `render(template, input)` produces the same bytes every time, so
rendered output can be committed, diffed in review, and compared in CI. Sprig's random and
time functions are why `helm template` output cannot be diffed reliably, and why chart authors
are advised to avoid them.

Termination is guaranteed rather than bounded by a depth cap:

* CEL has no unbounded loops. Comprehensions iterate over finite data.
* Sub-template calls must form an acyclic graph; recursion is rejected at compile time with the
  cycle spelled out. The price is that genuinely recursive output — an arbitrarily nested tree
  — cannot be rendered, which Jinja and Go both manage.
* `CostLimit` bounds the work each expression performs; `MaxOutputBytes` bounds the document.

Compare with the alternatives. Go templates can call any function in the `FuncMap`, which is
whatever the embedding program registered — arbitrary Go code, including I/O. Jinja's
`SandboxedEnvironment` restricts attribute access on a Turing-complete host language and has
historically been escaped through `str.format`, `__class__` chains, and similar. Helm charts
are effectively trusted code. CEL's safety comes from the other direction: the language cannot
express unbounded computation or I/O at all, and functions must be declared into the
environment before a template can name one.

## Function libraries

Sprig is the benchmark for breadth: roughly 200 functions covering strings, lists, dicts, math,
dates, encodings, regexes, semver, crypto, and UUIDs. Jinja ships a smaller set of filters and
tests but sits on Python, so a filter is a few lines away.

CEL templates inherit the CEL standard library plus the extension libraries in
[`ext`](../ext/README.md) — strings, lists, sets, math, encoders, regex, bindings,
two-variable comprehensions — and add only the functions that are specific to producing text:
`indent`, `nindent`, `quote`, `squote`, `toJSON`, `toYAML`, `repeat`, `sum`, `sha256`, `raw`.

What is deliberately absent: `now`, `uuidv4`, `randAlphaNum`, `env`, `lookup`, and Sprig's
`default`. The first five break purity. `default` exists in Sprig to paper over untyped
values; with declared types and optionals, `cfg.?tag.orValue("latest")` says the same thing
and is checked.

Comprehensions do a lot of what Sprig's list functions do, in one uniform way:

| Sprig / Helm | CEL |
| --- | --- |
| `pluck "name" $list` | `list.map(x, x.name)` |
| `compact` | `list.filter(x, x != "")` |
| `has $x $list` | `x in list` |
| `uniq` | `list.distinct()` |
| `sortAlpha` | `list.sort()` |
| `join "," $list` | `list.join(",")` |
| `empty $x` | `size(x) == 0` (or `!has(a.x)`) |

## Extending

| | How you add a function |
| --- | --- |
| CEL templates | `CELOptions(cel.Function("myFunc", cel.Overload(...)))` — typed, checked at compile time |
| Go text/template | `FuncMap{"myFunc": anyGoFunc}` — arity and types checked by reflection at execution |
| Jinja | `env.filters["myFilter"] = fn` — any Python callable |
| Helm | not extensible by chart authors; the function set is fixed by the Helm binary |

The CEL declaration carries a signature, so misuse is a compile error rather than a reflection
failure mid-render — and the same declaration is what tooling would read to offer completion.

## What the others do better

Being honest about it:

* **`html/template`** — contextual auto-escaping. Nothing here replaces it for HTML.
* **Jinja** — template inheritance (`extends`/`block`), macro defaults and varargs, the `loop`
  object, filter ergonomics, and an enormous ecosystem. For document and page layout it remains
  the more complete language.
* **Helm + Sprig** — breadth of functions, and a zero-ceremony model where values are whatever
  YAML the user supplied. For a quick chart, declaring a type schema is overhead. The ecosystem
  — thousands of published charts, and every Kubernetes engineer already knowing the syntax —
  is not something a new language displaces.
* **Go `text/template`** — it is in the standard library, it is everywhere, and mutable
  `$variables` with `range` accumulation express some loops more directly than a comprehension.

CEL templates are also new: no editor support, no formatter, no ecosystem, and only a Go
implementation, though CEL itself is specified across Go, Java, C++, Python, and Rust, so the
expression half of the language is portable in principle.

## Choosing

* Generating **HTML from untrusted input** → `html/template`. Contextual escaping is not
  optional and nothing else here provides it.
* Rendering **documents, emails, or pages** with layouts and inheritance → Jinja.
* Working **inside the Helm ecosystem**, or sharing charts publicly → Helm + Sprig. The
  ecosystem effect dominates.
* Generating **configuration that must be reviewed, diffed, and trusted** — Kubernetes
  manifests, Terraform, CI pipelines, policy bundles — where the inputs have a schema and you
  would rather fail in CI than in production → CEL templates.
* Rendering templates **authored by users you do not fully trust**, inside a service → CEL
  templates. Guaranteed termination, bounded cost, no I/O, and a function set you control are
  properties the others can only approximate.
