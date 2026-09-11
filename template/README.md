# CEL Templates

A text templating language whose expression language is CEL.

Templates are ordinary text with `{{ ... }}` actions embedded in them, in the tradition of Go's
`text/template`, Jinja, and Helm. The difference is that every expression inside an action is a
CEL expression which is **type-checked when the template is compiled**, against variables you
declare. A template that compiles has no undeclared variables, no misspelled functions, and no
type mismatches, for any input matching the declared types.

```go
tmpl, err := template.Compile("greeting.tmpl",
    `Hello {{ user.name }}{{ if user.admin }} (admin){{ end }}!`,
    template.Variable("user", cel.MapType(cel.StringType, cel.DynType)))
if err != nil {
    return err
}
out, err := tmpl.RenderString(map[string]any{
    "user": map[string]any{"name": "Ada", "admin": true},
})
```

See [COMPARISON.md](COMPARISON.md) for a detailed comparison with Go `text/template`, Jinja,
and Helm + Sprig.

## Actions

| Action | Meaning |
| --- | --- |
| `{{ expr }}` | Evaluate a CEL expression and write it to the output |
| `{{# comment }}` | Discarded |
| `{{ if expr }}` … `{{ else if expr }}` … `{{ else }}` … `{{ end }}` | Conditional |
| `{{ for x in xs }}` … `{{ else }}` … `{{ end }}` | Iteration, with an arm for an empty range |
| `{{ break }}`, `{{ continue }}` | Loop control |
| `{{ let name = expr }}` | Bind a value for the rest of the enclosing block |
| `{{ define name(p T, …) }}` … `{{ end }}` | Declare a sub-template |

## Sub-templates are functions

`{{ define }}` declares a named sub-template with typed parameters and registers it in the CEL
environment as a function returning the rendered string. Calling a sub-template is therefore an
ordinary expression, which means it composes with everything else in the language:

```
{{ define port(p int) }}
- containerPort: {{ p }}
  name: port-{{ p }}
{{- end }}

spec:
  ports:{{ ports.map(p, port(p)).join("\n").nindent(4) }}
```

Sub-templates are *hermetic*: they see their declared parameters and nothing else, so a
sub-template can never silently depend on a variable that happens to be in scope at one of its
call sites. Passing the wrong argument type is a compile error:

```
test.tmpl:1:37: found no matching overload for 'port' applied to '(string)'
```

The call graph must be acyclic. Combined with CEL's lack of unbounded loops, this means
rendering always terminates.

## Types, not truthiness

Conditions must be `bool`. There is no implicit truthiness, so an accidental
`{{ if user.name }}` is rejected rather than being silently true for every non-empty name:

```
page.tmpl:4:7: condition must be a bool, found string; did you mean user.name != ""
```

A missing key is an error at render time rather than an empty string in the output. Where a
value is genuinely optional, say so:

```
{{ if has(cfg.replicas) }}replicas: {{ cfg.replicas }}{{ end }}
replicas: {{ cfg.?replicas.orValue(1) }}
```

## Iteration

The one-variable form binds list elements or map keys; the two-variable form binds a list index
and element, or a map key and value.

```
{{ for name in names }}…{{ end }}
{{ for i, name in names }}…{{ end }}
{{ for key, value in labels }}…{{ end }}
```

Map iteration visits keys in sorted order, so the same data always renders to the same bytes.

## Whitespace

A line containing nothing but a single control action is removed from the output along with its
newline, which is what you almost always want when generating YAML or code:

```
items:
{{ for i in xs }}
  - {{ i }}
{{ end }}
```

renders as

```
items:
  - 1
  - 2
```

Disable this with `StandaloneLines(false)` to get `text/template`'s behavior. The explicit
`{{-` and `-}}` markers trim all adjacent whitespace and are always available.

## Functions

Templates are checked against the CEL standard library plus the
[extension libraries](../ext/README.md) for strings, lists, sets, math, encoders, regex,
bindings, and two-variable comprehensions, which together cover most of what template authors
reach for. `TemplateFuncs` adds the output-shaping functions which only make sense when
producing text:

| Function | Purpose |
| --- | --- |
| `raw(v)` | Mark a value as already escaped |
| `indent(s, n)`, `s.indent(n)` | Prefix every line with `n` spaces |
| `nindent(s, n)`, `s.nindent(n)` | As `indent`, with a leading newline |
| `quote(v)`, `squote(v)` | Render and quote |
| `toJSON(v)`, `toJSON(v, n)` | Encode as JSON with sorted keys |
| `toYAML(v)` | Encode as YAML with sorted keys |
| `s.repeat(n)` | Repeat a string |
| `sum(xs)`, `xs.sum()` | Add a list of numbers |
| `sha256(s)` | Hex digest, for config checksums |

Register your own with `CELOptions(cel.Function(...))`. Custom functions are the extension
point for anything domain specific, and are type-checked like everything else.

## Escaping

Escaping is off by default, since most templates produce text rather than markup. `Escape`
installs an escaper which is applied to every interpolated value; sub-template results and
values wrapped in `raw()` pass through unchanged.

```go
template.Escape(template.HTMLEscape)
```

`HTMLEscape` is not context aware: unlike Go's `html/template`, it applies the same
substitution everywhere, so it does not by itself make data safe to interpolate into script
blocks, style blocks, or unquoted attributes.

## Determinism and limits

A template is a pure function of its inputs. There is no `now()`, no `random()`, no file or
network access, and no way to reach out to a cluster mid-render, so a given template and input
always produce the same bytes — which is what makes rendered output worth committing, diffing,
and reviewing.

Evaluation is bounded:

| Option | Bound |
| --- | --- |
| `CostLimit(n)` | Work performed by each expression |
| `MaxOutputBytes(n)` | Size of the rendered document (8 MiB by default) |
| acyclic sub-template calls | Expansion depth |

## Multiple sources

`Compiler` compiles a group of templates which share a namespace of sub-templates. Every error
across every source is reported together, ordered by position.

```go
c, err := template.NewCompiler(template.Variable("release", cel.StringType))
c.AddSource("lib.tmpl", libSrc)
c.AddSource("deployment.yaml", deploymentSrc)
set, err := c.Compile()
err = set.Render(os.Stdout, "deployment.yaml", vars)
```
