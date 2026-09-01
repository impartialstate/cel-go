# Gatekeeper policies in CEL

Compile, test and evaluate the CEL inside OPA Gatekeeper ConstraintTemplates —
without a cluster, without Gatekeeper installed, and without writing Go.

Gatekeeper's `K8sNativeValidation` engine lets a ConstraintTemplate carry CEL
instead of Rego. Two things make that CEL harder to work with than the Rego
beside it:

* **Nothing checks it until it runs.** The Gatekeeper policy library tests its
  CEL implementations through gator rather than through unit tests, so a
  misspelled field or a type mistake is caught by a cluster rather than by a
  compiler.
* **It cannot read anything but the request.** Rego policies read the
  replicated cluster cache through `data.inventory` and call providers with
  `external_data`. CEL policies have neither, which is why every referential
  constraint in the policy library ships Rego only.

This package addresses both.

```
go run cel.dev/cel-go/policy/gatekeeper/cmd/gkcel check template.yaml
```

```
FAIL template.yaml
  ERROR: template.yaml:41:65: undeclared reference to 'isWellFormed' (in container '')
   |               - expression: "object.metadata.name.isWellFormed()"
   | ................................................................^
```

## The command

```
gkcel check <template.yaml>...   compile the templates and report mistakes
gkcel test <suite.yaml>...       run gator test suites against the templates
gkcel review [flags]             review one object and print the violations
```

`check` compiles every expression in a template — validations, variables and
match conditions — and reports mistakes against the line and column of the
template which holds them. `review` prints the violations a cluster would
report for one object, and exits non-zero when the object is rejected, so both
work as a step in continuous integration.

```
$ gkcel review -template template.yaml -constraint constraint.yaml \
      -object ingress.yaml -inventory cluster.yaml
1 violation(s) of k8suniqueingresshost
  - ingress host is already claimed by: prod/web
```

## Tests you already have

`gkcel test` reads the suites Gatekeeper's `gator` tool uses, unchanged:

```yaml
kind: Suite
apiVersion: test.gatekeeper.sh/v1alpha1
tests:
  - name: required-labels
    template: template.yaml
    constraint: constraint.yaml
    cases:
      - name: labelled
        object: samples/labelled.yaml
        assertions:
          - violations: no
      - name: unlabelled
        object: samples/unlabelled.yaml
        assertions:
          - violations: 1
            message: "missing required label"
```

A case may name `inventory` files, which become the cluster the policy reads,
and an object file may hold an `AdmissionReview` rather than a bare object, so
a recorded request — including a delete, where only the old object exists — is
replayed as it happened.

From Go, a suite is a test:

```go
func TestPolicies(t *testing.T) {
    gatekeeper.RunSuite(t, "suite.yaml")
}
```

### Coverage

A template can also be run through cel-go's own test runner, which reports how
much of the policy the tests reached — down to the branches no case exercised:

```
AST Node Coverage: 94.53% (121 out of 128 nodes covered)
AST Branch Coverage: 80.61% (79 out of 98 branch outcomes covered)
Interesting Unencountered Branch Paths:
    Expression ID 4 ('has(params.spec) && has(params.spec.parameters)'): lacks 'false' coverage
```

That first line says no case covers a constraint which sets no parameters. See
`celtest_test.go` for the wiring.

## From Go

```go
tmpl, err := gatekeeper.CompileFile("template.yaml")
if err != nil {
    // Compilation errors carry the position within the template.
    return err
}
violations, err := tmpl.Review(ctx, gatekeeper.Review{
    Object:     pod,
    Parameters: map[string]any{"labels": []any{map[string]any{"key": "owner"}}},
})
```

Every validation of the template is evaluated, so a review reports each
violation an object fails rather than stopping at the first, as Gatekeeper
does. A match condition which does not hold means no violation at all.

## The environment a policy sees

The variables are the ones Kubernetes binds for admission, plus the two
Gatekeeper defines for every template:

| Variable | Holds |
| --- | --- |
| `object` | the object being admitted; null on delete |
| `oldObject` | the object as it exists in the cluster; null on create |
| `request` | the attributes of the admission request |
| `params` | the constraint resource which instantiated the template |
| `namespaceObject` | the namespace of the object; null when cluster-scoped |
| `variables.params` | the constraint's `spec.parameters`; null when it sets none |
| `variables.anyObject` | `object`, falling back to `oldObject` |

The function set tracks what a cluster provides rather than everything cel-go
offers, so that a template which compiles here is one a cluster will accept:
the standard macros and functions, the string extension at the version
Kubernetes enables, optional types, cross-type numeric comparisons, and the
Kubernetes list (`isSorted`, `sum`, `min`, `max`, `indexOf`, `lastIndexOf`) and
regex (`find`, `findAll`) functions, which this package supplies because cel-go
does not.

Libraries a recent cluster has and this environment does not: quantity, URL, IP
and CIDR, format, semver, the authorizer, and two-variable comprehensions. A
policy which uses one can declare it with `gatekeeper.EnvOptions(...)`; a
policy which uses `quantity` or the authorizer needs an implementation first.

### Inputs are typed from their schemas

The `openAPIV3Schema` a template declares for its constraint becomes a CEL type,
so a policy which reads a parameter the schema does not declare does not
compile:

```
ERROR: template.yaml:29:48: undefined field 'lables'
 |               - expression: "variables.params.lables.all(e, e.key in object.metadata.labels)"
 | ...............................................^
ERROR: template.yaml:30:83: undefined field 'kee'
 |                 messageExpression: "'missing: ' + variables.params.labels.map(e, e.kee).join(', ')"
 | ..................................................................................^
```

The object under review is typed the same way when its definition is supplied,
which is how a policy over a custom resource is checked against the resource it
governs:

```
gkcel check -crd databases.crd.yaml template.yaml
```

```go
schema, err := gatekeeper.ReadCRDSchema("databases.crd.yaml", "v1")
tmpl, err := gatekeeper.CompileFile("template.yaml",
    gatekeeper.WithObjectSchema(schema))
```

Without a definition the object stays untyped, since a template may match kinds
whose schemas are not known here. Typing never costs a policy the null checks it
needs: a schema type is nullable, so `object == null` on a delete and
`variables.params == null` for a constraint which sets no parameters both still
compile. A schema which allows fields it does not declare — `additionalProperties`
or `x-kubernetes-preserve-unknown-fields` — is read as a map, since there is
nothing to check a field name against.

`Template.ValidateParams` checks a constraint's parameters against the same
schema, which catches the fixture that does not match the schema its constraint
declares — the usual reason a policy under test does not behave as its author
expects.

## Referential policies

`inventory` and `externalData` give CEL policies what Rego reads from
`data.inventory` and fetches with `external_data`:

```yaml
variables:
  - name: conflicting
    expression: >-
      inventory.list("networking.k8s.io/v1", "Ingress")
        .filter(other,
          other.metadata.namespace + "/" + other.metadata.name != variables.identity &&
          other.spec.rules.exists(rule, rule.host in variables.hosts))
        .map(other, other.metadata.namespace + "/" + other.metadata.name)
validations:
  - expression: "size(variables.conflicting) == 0"
    messageExpression: >-
      "ingress host is already claimed by: " + variables.conflicting.join(", ")
```

```
inventory.get(apiVersion, kind, name)              a cluster-scoped object, or null
inventory.get(apiVersion, kind, namespace, name)   a namespaced object, or null
inventory.list(apiVersion, kind)                   every object of a kind
inventory.list(apiVersion, kind, namespace)        every object in a namespace
externalData.get(provider, keys)                   a provider response
```

An `externalData` response has the shape Rego's `external_data` builtin
returns — `responses`, `errors`, `status_code`, `system_error` — so a policy
ported from Rego reads it the same way.

### Reading it the way Rego does

A constraint being ported from Rego can keep the paths its Rego implementation
used:

```yaml
variables:
  - name: services
    expression: 'data.inventory.namespace[variables.anyObject.metadata.namespace]["v1"]["Service"]'
  - name: namespaceObj
    expression: 'data.inventory.cluster["v1"]["Namespace"][variables.anyObject.metadata.namespace]'
  - name: signatures
    expression: 'external_data({"provider": variables.params.provider, "keys": variables.images})'
```

Indexing a kind yields every object of that kind keyed by name — the map a Rego
rule iterates — and `external_data` takes and returns exactly what the builtin
does. Both go to the same provider as the functions above.

Two differences from Rego are worth knowing. The path is read by key rather than
enumerated: the api versions and namespaces which exist cannot be listed, so a
rule which iterates them names them here instead, and one which tries is told
so. And reading a kind reads all of it, where `inventory.get` reads one object,
so a policy written for CEL rather than ported to it should prefer the
functions.

### Lookups are asynchronous

A lookup made through the inventory functions is an asynchronous CEL call: it
runs on its own goroutine with the review's context, and the evaluator carries
on with the rest of the expression while it is in flight. The lookups a policy
makes therefore overlap, across its validations as well as within one, and a
lookup the policy short-circuits past is never made.

```go
type DataProvider interface {
    Resolve(ctx context.Context, request Request) (any, error)
}

tmpl, err := gatekeeper.CompileFile("template.yaml",
    gatekeeper.WithDataProvider(provider),
    // A policy which reads the cluster from inside a comprehension could
    // otherwise launch a goroutine per element.
    gatekeeper.MaxConcurrentLookups(8))
```

A provider which answers a batch more cheaply than the requests one at a time
also implements `BatchDataProvider`. That is used for the reads a policy makes
through the data namespace, which are index operations rather than calls and so
cannot be asynchronous: they yield an unknown value, which CEL propagates
without failing the expression, so one pass gathers the reads the policy
reached and the next runs with the answers. Reads which depend on earlier
results resolve in later passes.

Whichever route a policy takes:

* An object read by two expressions is fetched once, and two reads of the same
  object in flight at the same time wait on one call rather than making two.
* A policy which makes a lookup with no provider configured fails its review
  naming the lookup, rather than quietly deciding on absent data.
* Cancelling the review's context abandons the lookups still running.

Since an environment which declares an asynchronous function yields programs
that only `ConcurrentEval` can run, a tool which evaluates a policy with `Eval`
— the CEL test runner among them — asks for `SynchronousLookups()`, which binds
the same functions to blocking implementations.

For tests, `StaticProvider` answers from a fixed set of objects, so a test
states the cluster it expects a decision under rather than arranging one.

## What is not modeled

* **The rest of the constraint.** `spec.match` — the kinds, namespaces and
  label selectors which decide whether a constraint applies — is not evaluated.
  A review evaluates the policy against the object it is given.
* **Enforcement.** `enforcementAction`, `failurePolicy` and the audit path
  decide what a cluster does with a violation, not whether there is one.
* **Cost limits.** A cluster bounds the runtime of an expression; this package
  does not.
* **Rego.** A template's Rego is ignored. A template with no CEL is reported as
  such rather than being silently skipped.
