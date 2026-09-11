// Copyright 2025 Google LLC
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

package template

import (
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/cel-go/cel"
)

func TestRender(t *testing.T) {
	tests := []struct {
		name string
		tmpl string
		vars map[string]any
		opts []Option
		out  string
	}{
		{
			name: "text only",
			tmpl: "hello, world\n",
			out:  "hello, world\n",
		},
		{
			name: "interpolation",
			tmpl: `hello, {{ name }}!`,
			opts: []Option{Variable("name", cel.StringType)},
			vars: map[string]any{"name": "Ada"},
			out:  "hello, Ada!",
		},
		{
			name: "expression",
			tmpl: `{{ name.upperAscii() + "!" }}`,
			opts: []Option{Variable("name", cel.StringType)},
			vars: map[string]any{"name": "ada"},
			out:  "ADA!",
		},
		{
			name: "comment removed",
			tmpl: "a\n{{# this is a comment }}\nb\n",
			out:  "a\nb\n",
		},
		{
			name: "inline comment",
			tmpl: "a{{# note }}b",
			out:  "ab",
		},
		{
			name: "string literal containing the closing delimiter",
			tmpl: `{{ "}}" }}`,
			out:  "}}",
		},
		{
			name: "if else chain",
			tmpl: `{{ if n > 10 }}big{{ else if n > 5 }}medium{{ else }}small{{ end }}`,
			opts: []Option{Variable("n", cel.IntType)},
			vars: map[string]any{"n": 7},
			out:  "medium",
		},
		{
			name: "if without else",
			tmpl: `[{{ if flag }}on{{ end }}]`,
			opts: []Option{Variable("flag", cel.BoolType)},
			vars: map[string]any{"flag": false},
			out:  "[]",
		},
		{
			name: "for over a list",
			tmpl: `{{ for x in xs }}<{{ x }}>{{ end }}`,
			opts: []Option{Variable("xs", cel.ListType(cel.StringType))},
			vars: map[string]any{"xs": []any{"a", "b"}},
			out:  "<a><b>",
		},
		{
			name: "for with an index",
			tmpl: `{{ for i, x in xs }}{{ i }}:{{ x }} {{ end }}`,
			opts: []Option{Variable("xs", cel.ListType(cel.StringType))},
			vars: map[string]any{"xs": []any{"a", "b"}},
			out:  "0:a 1:b ",
		},
		{
			name: "for over a map is key ordered",
			tmpl: `{{ for k, v in m }}{{ k }}={{ v }};{{ end }}`,
			opts: []Option{Variable("m", cel.MapType(cel.StringType, cel.IntType))},
			vars: map[string]any{"m": map[string]any{"c": 3, "a": 1, "b": 2}},
			out:  "a=1;b=2;c=3;",
		},
		{
			name: "for over map keys",
			tmpl: `{{ for k in m }}{{ k }},{{ end }}`,
			opts: []Option{Variable("m", cel.MapType(cel.StringType, cel.IntType))},
			vars: map[string]any{"m": map[string]any{"z": 1, "y": 2}},
			out:  "y,z,",
		},
		{
			name: "for else on an empty list",
			tmpl: `{{ for x in xs }}{{ x }}{{ else }}none{{ end }}`,
			opts: []Option{Variable("xs", cel.ListType(cel.StringType))},
			vars: map[string]any{"xs": []any{}},
			out:  "none",
		},
		{
			name: "break",
			tmpl: `{{ for x in xs }}{{ if x == "c" }}{{ break }}{{ end }}{{ x }}{{ end }}`,
			opts: []Option{Variable("xs", cel.ListType(cel.StringType))},
			vars: map[string]any{"xs": []any{"a", "b", "c", "d"}},
			out:  "ab",
		},
		{
			name: "continue",
			tmpl: `{{ for x in xs }}{{ if x == "b" }}{{ continue }}{{ end }}{{ x }}{{ end }}`,
			opts: []Option{Variable("xs", cel.ListType(cel.StringType))},
			vars: map[string]any{"xs": []any{"a", "b", "c"}},
			out:  "ac",
		},
		{
			name: "let binding",
			tmpl: `{{ let total = xs.sum() }}{{ total }} / {{ total * 2 }}`,
			opts: []Option{Variable("xs", cel.ListType(cel.IntType))},
			vars: map[string]any{"xs": []any{1, 2, 3}},
			out:  "6 / 12",
		},
		{
			name: "let inside a loop",
			tmpl: `{{ for x in xs }}{{ let doubled = x * 2 }}{{ doubled }},{{ end }}`,
			opts: []Option{Variable("xs", cel.ListType(cel.IntType))},
			vars: map[string]any{"xs": []any{1, 2}},
			out:  "2,4,",
		},
		{
			name: "sub-template call",
			tmpl: "{{ define greet(who string) }}hi {{ who }}{{ end }}{{ greet(\"ada\") }}",
			out:  "hi ada",
		},
		{
			name: "sub-template used inside a comprehension",
			tmpl: `{{ define item(s string) }}[{{ s }}]{{ end }}{{ xs.map(x, item(x)).join("") }}`,
			opts: []Option{Variable("xs", cel.ListType(cel.StringType))},
			vars: map[string]any{"xs": []any{"a", "b"}},
			out:  "[a][b]",
		},
		{
			name: "sub-template composed with nindent",
			tmpl: "{{ define body(k string) }}a: {{ k }}\nb: 2{{ end }}spec:{{ body(\"x\").nindent(2) }}\n",
			out:  "spec:\n  a: x\n  b: 2\n",
		},
		{
			name: "optional chaining",
			tmpl: `{{ m.?missing.orValue("fallback") }}`,
			opts: []Option{Variable("m", cel.MapType(cel.StringType, cel.StringType))},
			vars: map[string]any{"m": map[string]any{}},
			out:  "fallback",
		},
		{
			name: "has macro",
			tmpl: `{{ if has(m.a) }}yes{{ else }}no{{ end }}`,
			opts: []Option{Variable("m", cel.MapType(cel.StringType, cel.StringType))},
			vars: map[string]any{"m": map[string]any{}},
			out:  "no",
		},
		{
			name: "explicit whitespace trimming",
			tmpl: "a   {{- v -}}   b",
			opts: []Option{Variable("v", cel.StringType)},
			vars: map[string]any{"v": "X"},
			out:  "aXb",
		},
		{
			name: "negation is not a trim marker",
			tmpl: `{{-n }}`,
			opts: []Option{Variable("n", cel.IntType)},
			vars: map[string]any{"n": 3},
			out:  "-3",
		},
		{
			name: "standalone control lines are removed",
			tmpl: "start\n  {{ if flag }}\nbody\n  {{ end }}\nend\n",
			opts: []Option{Variable("flag", cel.BoolType)},
			vars: map[string]any{"flag": true},
			out:  "start\nbody\nend\n",
		},
		{
			name: "standalone trimming disabled",
			tmpl: "start\n{{ if flag }}\nbody\n{{ end }}\nend\n",
			opts: []Option{Variable("flag", cel.BoolType), StandaloneLines(false)},
			vars: map[string]any{"flag": true},
			out:  "start\n\nbody\n\nend\n",
		},
		{
			name: "interpolation lines are never standalone",
			tmpl: "a\n{{ v }}\nb\n",
			opts: []Option{Variable("v", cel.StringType)},
			vars: map[string]any{"v": "X"},
			out:  "a\nX\nb\n",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tmpl, err := Compile("test.tmpl", tc.tmpl, tc.opts...)
			if err != nil {
				t.Fatalf("Compile() failed: %v", err)
			}
			out, err := tmpl.RenderString(tc.vars)
			if err != nil {
				t.Fatalf("RenderString() failed: %v", err)
			}
			if out != tc.out {
				t.Errorf("RenderString() got %q, wanted %q", out, tc.out)
			}
		})
	}
}

func TestRenderValueFormatting(t *testing.T) {
	tests := []struct {
		name string
		expr string
		out  string
	}{
		{name: "int", expr: `1 + 2`, out: "3"},
		{name: "uint", expr: `2u`, out: "2"},
		{name: "double", expr: `1.5`, out: "1.5"},
		{name: "large double avoids exponents", expr: `1000000.0`, out: "1000000"},
		{name: "tiny double uses exponents", expr: `0.0000001`, out: "1e-07"},
		{name: "bool", expr: `true`, out: "true"},
		{name: "null", expr: `null`, out: "null"},
		{name: "bytes", expr: `b"abc"`, out: "abc"},
		{name: "duration", expr: `duration("1h")`, out: "3600s"},
		{name: "timestamp", expr: `timestamp("2024-01-02T03:04:05Z")`, out: "2024-01-02T03:04:05Z"},
		{name: "list as JSON", expr: `[1, 2]`, out: "[1,2]"},
		{name: "map as JSON with sorted keys", expr: `{"b": 1, "a": 2}`, out: `{"a":2,"b":1}`},
		{name: "string", expr: `"x"`, out: "x"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tmpl, err := Compile("test.tmpl", "{{ "+tc.expr+" }}")
			if err != nil {
				t.Fatalf("Compile() failed: %v", err)
			}
			out, err := tmpl.RenderString(nil)
			if err != nil {
				t.Fatalf("RenderString() failed: %v", err)
			}
			if out != tc.out {
				t.Errorf("RenderString() got %q, wanted %q", out, tc.out)
			}
		})
	}
}

func TestCompileErrors(t *testing.T) {
	tests := []struct {
		name string
		tmpl string
		opts []Option
		want string
	}{
		{
			name: "undeclared variable",
			tmpl: "hello {{ nobody }}",
			want: "test.tmpl:1:10: undeclared reference to 'nobody'",
		},
		{
			name: "unknown field on a typed map",
			tmpl: "{{ m.a }}{{ m.b }}",
			opts: []Option{Variable("m", cel.MapType(cel.StringType, cel.StringType))},
			want: "",
		},
		{
			name: "non-bool condition",
			tmpl: "{{ if name }}x{{ end }}",
			opts: []Option{Variable("name", cel.StringType)},
			want: `test.tmpl:1:7: condition must be a bool, found string; did you mean name != ""`,
		},
		{
			name: "non-bool condition on a list",
			tmpl: "{{ if xs }}x{{ end }}",
			opts: []Option{Variable("xs", cel.ListType(cel.IntType))},
			want: "did you mean size(xs) > 0",
		},
		{
			name: "range over a scalar",
			tmpl: "{{ for c in name }}{{ c }}{{ end }}",
			opts: []Option{Variable("name", cel.StringType)},
			want: "cannot range over string; {{ for }} requires a list or a map",
		},
		{
			name: "type mismatch in an expression",
			tmpl: `{{ n + "x" }}`,
			opts: []Option{Variable("n", cel.IntType)},
			want: "found no matching overload for '_+_'",
		},
		{
			name: "unclosed action",
			tmpl: "a {{ b ",
			want: `test.tmpl:1:3: unclosed action: missing "}}"`,
		},
		{
			name: "unclosed if",
			tmpl: "{{ if true }}x",
			want: "unclosed {{ if }}: missing {{ end }}",
		},
		{
			name: "unclosed for",
			tmpl: "{{ for x in [1] }}x",
			want: "unclosed {{ for }}: missing {{ end }}",
		},
		{
			name: "stray end",
			tmpl: "x{{ end }}",
			want: "unexpected {{ end }}",
		},
		{
			name: "break outside of a loop",
			tmpl: "{{ if true }}{{ break }}{{ end }}",
			want: "{{ break }} is only valid inside {{ for }}",
		},
		{
			name: "empty action",
			tmpl: "{{ }}",
			want: "empty action",
		},
		{
			name: "let without an expression",
			tmpl: "{{ let x = }}",
			want: "missing expression after '=' in {{ let x }}",
		},
		{
			name: "invalid loop variable",
			tmpl: "{{ for 1 in [1] }}{{ end }}",
			want: `invalid loop variable "1"`,
		},
		{
			name: "duplicate define",
			tmpl: "{{ define a() }}1{{ end }}{{ define a() }}2{{ end }}",
			want: `template "a" is already defined`,
		},
		{
			name: "unknown parameter type",
			tmpl: "{{ define a(x nope) }}{{ x }}{{ end }}",
			want: `parameter "x" of template "a"`,
		},
		{
			name: "parameter without a type",
			tmpl: "{{ define a(x) }}{{ x }}{{ end }}",
			want: `parameter "x" is missing a type`,
		},
		{
			name: "direct recursion",
			tmpl: `{{ define a(n int) }}{{ if n > 0 }}{{ a(n - 1) }}{{ end }}{{ end }}{{ a(3) }}`,
			want: `template "a" is recursive: a -> a`,
		},
		{
			name: "mutual recursion",
			tmpl: `{{ define a(n int) }}{{ b(n) }}{{ end }}{{ define b(n int) }}{{ a(n) }}{{ end }}{{ a(1) }}`,
			want: "is recursive",
		},
		{
			name: "sub-templates cannot see caller variables",
			tmpl: `{{ define a() }}{{ name }}{{ end }}{{ a() }}`,
			opts: []Option{Variable("name", cel.StringType)},
			want: "undeclared reference to 'name'",
		},
		{
			name: "wrong sub-template argument type",
			tmpl: `{{ define a(n int) }}{{ n }}{{ end }}{{ a("x") }}`,
			want: "found no matching overload for 'a'",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Compile("test.tmpl", tc.tmpl, tc.opts...)
			if tc.want == "" {
				if err != nil {
					t.Fatalf("Compile() got error %v, wanted none", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("Compile() succeeded, wanted error containing %q", tc.want)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("Compile() got error %v, wanted it to contain %q", err, tc.want)
			}
		})
	}
}

func TestCompileErrorsAreCollected(t *testing.T) {
	src := "{{ nope1 }}\n{{ nope2 }}\n"
	_, err := Compile("test.tmpl", src)
	if err == nil {
		t.Fatal("Compile() succeeded, wanted errors")
	}
	errs, ok := err.(*Errors)
	if !ok {
		t.Fatalf("Compile() returned %T, wanted *Errors", err)
	}
	if len(errs.List()) != 2 {
		t.Fatalf("Compile() reported %d errors, wanted 2: %v", len(errs.List()), err)
	}
	if got := errs.List()[0]; got.Line != 1 || got.Column != 4 {
		t.Errorf("first error at %d:%d, wanted 1:4", got.Line, got.Column)
	}
	if got := errs.List()[1]; got.Line != 2 || got.Column != 4 {
		t.Errorf("second error at %d:%d, wanted 2:4", got.Line, got.Column)
	}
}

func TestRenderErrors(t *testing.T) {
	tests := []struct {
		name string
		tmpl string
		opts []Option
		vars map[string]any
		want string
	}{
		{
			name: "missing key is an error rather than empty output",
			tmpl: "{{ m.missing }}",
			opts: []Option{Variable("m", cel.MapType(cel.StringType, cel.StringType))},
			vars: map[string]any{"m": map[string]any{}},
			want: "no such key: missing",
		},
		{
			name: "division by zero",
			tmpl: "{{ 1 / n }}",
			opts: []Option{Variable("n", cel.IntType)},
			vars: map[string]any{"n": 0},
			want: "division by zero",
		},
		{
			name: "absent optional",
			tmpl: "{{ m.?missing }}",
			opts: []Option{Variable("m", cel.MapType(cel.StringType, cel.StringType))},
			vars: map[string]any{"m": map[string]any{}},
			want: "optional value is absent",
		},
		{
			name: "error inside a sub-template names the sub-template",
			tmpl: `{{ define a(m map<string, string>) }}{{ m.missing }}{{ end }}{{ a({}) }}`,
			want: "in {{ define a }}: test.tmpl:1:41: no such key: missing",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tmpl, err := Compile("test.tmpl", tc.tmpl, tc.opts...)
			if err != nil {
				t.Fatalf("Compile() failed: %v", err)
			}
			_, err = tmpl.RenderString(tc.vars)
			if err == nil {
				t.Fatalf("RenderString() succeeded, wanted an error containing %q", tc.want)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("RenderString() got %v, wanted it to contain %q", err, tc.want)
			}
		})
	}
}

func TestRenderErrorLocation(t *testing.T) {
	tmpl, err := Compile("test.tmpl", "line one\nvalue: {{ m.missing }}\n",
		Variable("m", cel.MapType(cel.StringType, cel.StringType)))
	if err != nil {
		t.Fatalf("Compile() failed: %v", err)
	}
	_, err = tmpl.RenderString(map[string]any{"m": map[string]any{}})
	var rerr *RenderError
	if !errors.As(err, &rerr) {
		t.Fatalf("RenderString() returned %v, wanted a *RenderError", err)
	}
	if rerr.Line != 2 || rerr.Column != 11 {
		t.Errorf("error reported at %d:%d, wanted 2:11", rerr.Line, rerr.Column)
	}
}

func TestEscaping(t *testing.T) {
	src := `{{ define wrap(s string) }}<b>{{ s }}</b>{{ end }}` +
		`raw: {{ raw(v) }} escaped: {{ v }} nested: {{ wrap(v) }}`
	tmpl, err := Compile("test.tmpl", src, Variable("v", cel.StringType), Escape(HTMLEscape))
	if err != nil {
		t.Fatalf("Compile() failed: %v", err)
	}
	out, err := tmpl.RenderString(map[string]any{"v": "<script>"})
	if err != nil {
		t.Fatalf("RenderString() failed: %v", err)
	}
	want := "raw: <script> escaped: &lt;script&gt; nested: <b>&lt;script&gt;</b>"
	if out != want {
		t.Errorf("RenderString() got %q, wanted %q", out, want)
	}
}

func TestOutputLimit(t *testing.T) {
	tmpl, err := Compile("test.tmpl", `{{ for x in xs }}0123456789{{ end }}`,
		Variable("xs", cel.ListType(cel.IntType)), MaxOutputBytes(25))
	if err != nil {
		t.Fatalf("Compile() failed: %v", err)
	}
	_, err = tmpl.RenderString(map[string]any{"xs": []any{1, 2, 3, 4}})
	if !errors.Is(err, ErrOutputLimit) {
		t.Errorf("RenderString() got %v, wanted ErrOutputLimit", err)
	}
}

func TestCostLimit(t *testing.T) {
	tmpl, err := Compile("test.tmpl", `{{ xs.map(x, x * 2).size() }}`,
		Variable("xs", cel.ListType(cel.IntType)), CostLimit(10))
	if err != nil {
		t.Fatalf("Compile() failed: %v", err)
	}
	_, err = tmpl.RenderString(map[string]any{"xs": []any{1, 2, 3, 4, 5, 6, 7, 8}})
	if err == nil || !strings.Contains(err.Error(), "cost limit exceeded") {
		t.Errorf("RenderString() got %v, wanted a cost limit error", err)
	}
}

func TestSetAcrossSources(t *testing.T) {
	c, err := NewCompiler(Variable("name", cel.StringType))
	if err != nil {
		t.Fatalf("NewCompiler() failed: %v", err)
	}
	if err := c.AddSource("lib.tmpl", `{{ define bold(s string) }}**{{ s }}**{{ end }}`); err != nil {
		t.Fatalf("AddSource() failed: %v", err)
	}
	if err := c.AddSource("page.tmpl", `hello {{ bold(name) }}`); err != nil {
		t.Fatalf("AddSource() failed: %v", err)
	}
	set, err := c.Compile()
	if err != nil {
		t.Fatalf("Compile() failed: %v", err)
	}
	var sb strings.Builder
	if err := set.Render(&sb, "page.tmpl", map[string]any{"name": "ada"}); err != nil {
		t.Fatalf("Render() failed: %v", err)
	}
	if got := sb.String(); got != "hello **ada**" {
		t.Errorf("Render() got %q, wanted %q", got, "hello **ada**")
	}
	if _, found := set.Template("nope.tmpl"); found {
		t.Error("Template() found a template which was never added")
	}
	if got := set.Names(); len(got) != 2 {
		t.Errorf("Names() got %v, wanted two entries", got)
	}
}

func TestDuplicateSourceName(t *testing.T) {
	c, err := NewCompiler()
	if err != nil {
		t.Fatalf("NewCompiler() failed: %v", err)
	}
	if err := c.AddSource("a.tmpl", "x"); err != nil {
		t.Fatalf("AddSource() failed: %v", err)
	}
	if err := c.AddSource("a.tmpl", "y"); err == nil {
		t.Error("AddSource() accepted a duplicate name")
	}
}

func TestRenderIsConcurrencySafe(t *testing.T) {
	tmpl, err := Compile("test.tmpl",
		`{{ define row(s string) }}[{{ s }}]{{ end }}{{ for x in xs }}{{ row(x) }}{{ end }}`,
		Variable("xs", cel.ListType(cel.StringType)))
	if err != nil {
		t.Fatalf("Compile() failed: %v", err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			out, err := tmpl.RenderString(map[string]any{"xs": []any{"a", "b"}})
			if err != nil {
				t.Errorf("RenderString() failed: %v", err)
				return
			}
			if out != "[a][b]" {
				t.Errorf("RenderString() got %q, wanted %q", out, "[a][b]")
			}
		}()
	}
	wg.Wait()
}

func TestRenderIsDeterministic(t *testing.T) {
	tmpl, err := Compile("test.tmpl", `{{ for k, v in m }}{{ k }}={{ v }};{{ end }}{{ toJSON(m) }}`,
		Variable("m", cel.MapType(cel.StringType, cel.IntType)))
	if err != nil {
		t.Fatalf("Compile() failed: %v", err)
	}
	vars := map[string]any{"m": map[string]any{"a": 1, "b": 2, "c": 3, "d": 4, "e": 5}}
	first, err := tmpl.RenderString(vars)
	if err != nil {
		t.Fatalf("RenderString() failed: %v", err)
	}
	for i := 0; i < 50; i++ {
		next, err := tmpl.RenderString(vars)
		if err != nil {
			t.Fatalf("RenderString() failed: %v", err)
		}
		if next != first {
			t.Fatalf("RenderString() returned %q then %q for the same input", first, next)
		}
	}
}

func TestTimestampsComeFromInputs(t *testing.T) {
	// Templates are pure functions of their inputs: there is no now() to make output vary
	// between renders, so a timestamp has to be supplied by the caller.
	tmpl, err := Compile("test.tmpl", `{{ now }}`, Variable("now", cel.TimestampType))
	if err != nil {
		t.Fatalf("Compile() failed: %v", err)
	}
	out, err := tmpl.RenderString(map[string]any{"now": time.Date(2024, 5, 6, 7, 8, 9, 0, time.UTC)})
	if err != nil {
		t.Fatalf("RenderString() failed: %v", err)
	}
	if out != "2024-05-06T07:08:09Z" {
		t.Errorf("RenderString() got %q", out)
	}
}

func TestMustCompilePanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Error("MustCompile() did not panic on an invalid template")
		}
	}()
	MustCompile("bad.tmpl", "{{ nope }}")
}

func TestDefineMustBeTopLevel(t *testing.T) {
	_, err := Compile("test.tmpl", "{{ if true }}{{ define a() }}x{{ end }}{{ end }}")
	if err == nil || !strings.Contains(err.Error(), "only appear at the top level") {
		t.Errorf("Compile() got %v, wanted a top-level define error", err)
	}
}

// TestComparisonDocExample keeps the worked example in COMPARISON.md honest.
func TestComparisonDocExample(t *testing.T) {
	src := `{{ define line(name string, qty int, price double) }}
{{ name }} x{{ qty }} = {{ qty * int(price) }}
{{- end }}
{{ let inStock = order.items.filter(i, i.qty > 0) }}
{{ for i in inStock }}
{{ line(i.name, i.qty, i.price) }}
{{ else }}
Nothing in stock.
{{ end }}
Total: {{ sum(inStock.map(i, i.qty * int(i.price))) }}
`
	tmpl, err := Compile("order.tmpl", src,
		Variable("order", cel.MapType(cel.StringType,
			cel.ListType(cel.MapType(cel.StringType, cel.DynType)))))
	if err != nil {
		t.Fatalf("Compile() failed: %v", err)
	}
	out, err := tmpl.RenderString(map[string]any{"order": map[string]any{"items": []any{
		map[string]any{"name": "bolt", "qty": 2, "price": 3.0},
		map[string]any{"name": "nut", "qty": 0, "price": 1.0},
	}}})
	if err != nil {
		t.Fatalf("RenderString() failed: %v", err)
	}
	want := "\nbolt x2 = 6\nTotal: 6\n"
	if out != want {
		t.Errorf("RenderString() got %q, wanted %q", out, want)
	}
	empty, err := tmpl.RenderString(map[string]any{"order": map[string]any{"items": []any{}}})
	if err != nil {
		t.Fatalf("RenderString() failed: %v", err)
	}
	if !strings.Contains(empty, "Nothing in stock.") {
		t.Errorf("RenderString() got %q, wanted the empty-range arm", empty)
	}
}
