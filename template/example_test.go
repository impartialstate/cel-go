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

package template_test

import (
	"fmt"
	"os"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/template"
)

func ExampleCompile() {
	tmpl, err := template.Compile("greeting.tmpl",
		`Hello {{ user.name }}{{ if user.admin }} (admin){{ end }}!`,
		template.Variable("user", cel.MapType(cel.StringType, cel.DynType)))
	if err != nil {
		panic(err)
	}
	out, err := tmpl.RenderString(map[string]any{
		"user": map[string]any{"name": "Ada", "admin": true},
	})
	if err != nil {
		panic(err)
	}
	fmt.Println(out)
	// Output: Hello Ada (admin)!
}

// ExampleCompile_typeChecking shows that mistakes are reported when the template is compiled,
// with positions in the template source, rather than when it is rendered.
func ExampleCompile_typeChecking() {
	_, err := template.Compile("report.tmpl",
		"Totals\n{{ for item in order.items }}\n{{ if item.name }}{{ item.name }}{{ end }}\n{{ end }}\n",
		template.Variable("order", cel.MapType(cel.StringType,
			cel.ListType(cel.MapType(cel.StringType, cel.StringType)))))
	fmt.Println(err)
	// Output: report.tmpl:3:7: condition must be a bool, found string; did you mean item.name != ""
}

// ExampleCompiler shows a set of templates sharing a library of sub-templates. A sub-template
// is a CEL function, so it can be called from any expression, including from inside a
// comprehension.
func ExampleCompiler() {
	c, err := template.NewCompiler(
		template.Variable("release", cel.StringType),
		template.Variable("ports", cel.ListType(cel.IntType)),
	)
	if err != nil {
		panic(err)
	}
	err = c.AddSource("lib.tmpl", `
{{ define port(p int) }}
- containerPort: {{ p }}
  name: port-{{ p }}
{{- end }}
`)
	if err != nil {
		panic(err)
	}
	err = c.AddSource("deployment.yaml", `
metadata:
  name: {{ release }}
spec:
  ports:{{ ports.map(p, port(p)).join("\n").nindent(4) }}
`)
	if err != nil {
		panic(err)
	}
	set, err := c.Compile()
	if err != nil {
		panic(err)
	}
	err = set.Render(os.Stdout, "deployment.yaml", map[string]any{
		"release": "web",
		"ports":   []any{80, 443},
	})
	if err != nil {
		panic(err)
	}
	// Output:
	// metadata:
	//   name: web
	// spec:
	//   ports:
	//     - containerPort: 80
	//       name: port-80
	//     - containerPort: 443
	//       name: port-443
}

// ExampleEscape shows the configurable escaper. Values interpolated into the document are
// escaped; sub-template output and values wrapped in raw() are not.
func ExampleEscape() {
	tmpl, err := template.Compile("page.html",
		`<p>{{ comment }}</p>`,
		template.Variable("comment", cel.StringType),
		template.Escape(template.HTMLEscape))
	if err != nil {
		panic(err)
	}
	out, err := tmpl.RenderString(map[string]any{"comment": `<script>alert("hi")</script>`})
	if err != nil {
		panic(err)
	}
	fmt.Println(out)
	// Output: <p>&lt;script&gt;alert(&#34;hi&#34;)&lt;/script&gt;</p>
}
