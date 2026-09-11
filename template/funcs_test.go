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
	"strings"
	"testing"
)

func TestTemplateFuncs(t *testing.T) {
	tests := []struct {
		name string
		expr string
		out  string
	}{
		{name: "indent", expr: `"a\nb".indent(2)`, out: "  a\n  b"},
		{name: "indent skips blank lines", expr: `"a\n\nb".indent(2)`, out: "  a\n\n  b"},
		{name: "indent as a global", expr: `indent("a", 1)`, out: " a"},
		{name: "nindent", expr: `"a\nb".nindent(2)`, out: "\n  a\n  b"},
		{name: "quote", expr: `quote("a\"b")`, out: `"a\"b"`},
		{name: "quote a number", expr: `quote(12)`, out: `"12"`},
		{name: "squote", expr: `squote("it's")`, out: `'it'\''s'`},
		{name: "toJSON", expr: `toJSON({"b": 1, "a": [1, 2]})`, out: `{"a":[1,2],"b":1}`},
		{name: "toJSON indented", expr: `toJSON({"a": 1}, 2)`, out: "{\n  \"a\": 1\n}"},
		{name: "toJSON does not escape html", expr: `toJSON({"a": "<b>"})`, out: `{"a":"<b>"}`},
		{name: "toYAML", expr: `toYAML({"b": 1, "a": "x"})`, out: "a: x\nb: 1"},
		{name: "toYAML nested", expr: `toYAML({"a": [1, 2]})`, out: "a:\n    - 1\n    - 2"},
		{name: "repeat", expr: `"ab".repeat(3)`, out: "ababab"},
		{name: "sum ints", expr: `[1, 2, 3].sum()`, out: "6"},
		{name: "sum doubles", expr: `sum([1.5, 2.5])`, out: "4"},
		{name: "sum of an empty list", expr: `[].sum()`, out: "0"},
		{name: "sha256", expr: `sha256("abc")`,
			out: "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"},
		{name: "sha256 of bytes", expr: `sha256(b"abc")`,
			out: "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"},
		{name: "raw is an identity function", expr: `raw("x")`, out: "x"},
		{name: "strings extension", expr: `["a", "b"].join("-")`, out: "a-b"},
		{name: "lists extension", expr: `[3, 1, 2].sort()`, out: "[1,2,3]"},
		{name: "math extension", expr: `math.greatest([1, 5, 3])`, out: "5"},
		{name: "encoders extension", expr: `base64.encode(b"abc")`, out: "YWJj"},
		{name: "regex extension", expr: `regex.replace("a1b2", "[0-9]", "")`, out: "ab"},
		{name: "bindings extension", expr: `cel.bind(x, 2, x * x)`, out: "4"},
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

func TestTemplateFuncErrors(t *testing.T) {
	tests := []struct {
		name string
		expr string
		want string
	}{
		{name: "negative indent", expr: `"a".indent(-1)`, want: "out of range"},
		{name: "oversized indent", expr: `"a".indent(1000000)`, want: "out of range"},
		{name: "negative repeat", expr: `"a".repeat(-1)`, want: "negative"},
		{name: "oversized repeat", expr: `"ab".repeat(999999)`, want: "would produce more than"},
		{name: "mixed numeric sum", expr: `sum([1, 2.0])`, want: "a single numeric type"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tmpl, err := Compile("test.tmpl", "{{ "+tc.expr+" }}")
			if err != nil {
				if !strings.Contains(err.Error(), tc.want) {
					t.Fatalf("Compile() got %v, wanted it to contain %q", err, tc.want)
				}
				return
			}
			_, err = tmpl.RenderString(nil)
			if err == nil {
				t.Fatalf("RenderString() succeeded, wanted an error containing %q", tc.want)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("RenderString() got %v, wanted it to contain %q", err, tc.want)
			}
		})
	}
}

func TestStdLibDisabled(t *testing.T) {
	_, err := Compile("test.tmpl", `{{ "a".indent(2) }}`, StdLib(false))
	if err == nil {
		t.Fatal("Compile() succeeded with the standard library disabled")
	}
	if !strings.Contains(err.Error(), "indent") {
		t.Errorf("Compile() got %v, wanted an error naming indent", err)
	}
}
