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
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
	"github.com/google/cel-go/common/types/traits"

	"go.yaml.in/yaml/v3"
)

// rawFunc is recognized by the compiler as the opt-out from the configured escaper.
const rawFunc = "raw"

// maxIndent bounds the padding functions so that a mistaken width cannot be used to expand a
// small input into an enormous document.
const maxIndent = 1 << 16

// TemplateFuncs returns the output-shaping functions available to templates.
//
// These complement rather than replace the CEL extension libraries: string, list, set, math,
// encoding, and regex operations all come from the standard extensions, so only the functions
// which are specific to producing text live here.
//
// # raw
//
// Marks a value as already escaped, suppressing the escaper configured with Escape. It is a
// no-op when no escaper is configured.
//
//	raw(<dyn>) -> <dyn>
//
// # indent
//
// Prefixes every line of the input with the given number of spaces, including the first.
// Empty lines are left empty rather than padded with trailing whitespace.
//
//	indent(<string>, <int>) -> <string>
//	<string>.indent(<int>) -> <string>
//
// Examples:
//
//	"a\nb".indent(2)  // returns "  a\n  b"
//
// # nindent
//
// Like indent, but prefixes the result with a newline. This is the form needed when splicing a
// rendered block into a YAML or JSON document as the value of a key.
//
//	nindent(<string>, <int>) -> <string>
//	<string>.nindent(<int>) -> <string>
//
// # quote / squote
//
// Renders a value as text and wraps it in double or single quotes, escaping the contents.
//
//	quote(<dyn>) -> <string>
//	squote(<dyn>) -> <string>
//
// # toJSON
//
// Encodes a value as JSON with sorted object keys. The two argument form indents the output by
// the given number of spaces.
//
//	toJSON(<dyn>) -> <string>
//	toJSON(<dyn>, <int>) -> <string>
//
// # toYAML
//
// Encodes a value as YAML with sorted mapping keys. The output has no trailing newline, so it
// composes with nindent.
//
//	toYAML(<dyn>) -> <string>
//
// # repeat
//
// Concatenates a string with itself the given number of times.
//
//	<string>.repeat(<int>) -> <string>
//
// # sum
//
// Adds the elements of a list of numbers. The element type determines the result type, and an
// empty list sums to zero of that type.
//
//	sum(<list(int)>) -> <int>
//	sum(<list(uint)>) -> <uint>
//	sum(<list(double)>) -> <double>
//	<list(int)>.sum() -> <int>
//	<list(uint)>.sum() -> <uint>
//	<list(double)>.sum() -> <double>
//
// Examples:
//
//	sum(order.items.map(i, i.price))
//
// # sha256
//
// Returns the lowercase hexadecimal SHA-256 digest of a string or byte sequence, which is the
// usual way to derive a rollout-triggering checksum from rendered configuration.
//
//	sha256(<string>) -> <string>
//	sha256(<bytes>) -> <string>
func TemplateFuncs() cel.EnvOption {
	return cel.Lib(templateLib{})
}

type templateLib struct{}

// LibraryName implements cel.SingletonLibrary.
func (templateLib) LibraryName() string {
	return "cel.lib.ext.template"
}

func (templateLib) CompileOptions() []cel.EnvOption {
	return []cel.EnvOption{
		cel.Function(rawFunc,
			cel.Overload("template_raw", []*cel.Type{cel.DynType}, cel.DynType,
				cel.UnaryBinding(func(v ref.Val) ref.Val { return v }))),
		cel.Function("indent",
			cel.Overload("template_indent", []*cel.Type{cel.StringType, cel.IntType}, cel.StringType,
				cel.BinaryBinding(indentValue)),
			cel.MemberOverload("string_indent", []*cel.Type{cel.StringType, cel.IntType}, cel.StringType,
				cel.BinaryBinding(indentValue))),
		cel.Function("nindent",
			cel.Overload("template_nindent", []*cel.Type{cel.StringType, cel.IntType}, cel.StringType,
				cel.BinaryBinding(nindentValue)),
			cel.MemberOverload("string_nindent", []*cel.Type{cel.StringType, cel.IntType}, cel.StringType,
				cel.BinaryBinding(nindentValue))),
		cel.Function("quote",
			cel.Overload("template_quote", []*cel.Type{cel.DynType}, cel.StringType,
				cel.UnaryBinding(quoteValue))),
		cel.Function("squote",
			cel.Overload("template_squote", []*cel.Type{cel.DynType}, cel.StringType,
				cel.UnaryBinding(squoteValue))),
		cel.Function("toJSON",
			cel.Overload("template_to_json", []*cel.Type{cel.DynType}, cel.StringType,
				cel.UnaryBinding(func(v ref.Val) ref.Val { return jsonValue(v, "") })),
			cel.Overload("template_to_json_indent", []*cel.Type{cel.DynType, cel.IntType}, cel.StringType,
				cel.BinaryBinding(jsonIndentValue))),
		cel.Function("toYAML",
			cel.Overload("template_to_yaml", []*cel.Type{cel.DynType}, cel.StringType,
				cel.UnaryBinding(yamlValue))),
		cel.Function("repeat",
			cel.MemberOverload("string_repeat", []*cel.Type{cel.StringType, cel.IntType}, cel.StringType,
				cel.BinaryBinding(repeatValue))),
		cel.Function("sum",
			cel.Overload("template_sum_int", []*cel.Type{cel.ListType(cel.IntType)}, cel.IntType,
				cel.UnaryBinding(sumValue)),
			cel.Overload("template_sum_uint", []*cel.Type{cel.ListType(cel.UintType)}, cel.UintType,
				cel.UnaryBinding(sumValue)),
			cel.Overload("template_sum_double", []*cel.Type{cel.ListType(cel.DoubleType)}, cel.DoubleType,
				cel.UnaryBinding(sumValue)),
			cel.MemberOverload("list_int_sum", []*cel.Type{cel.ListType(cel.IntType)}, cel.IntType,
				cel.UnaryBinding(sumValue)),
			cel.MemberOverload("list_uint_sum", []*cel.Type{cel.ListType(cel.UintType)}, cel.UintType,
				cel.UnaryBinding(sumValue)),
			cel.MemberOverload("list_double_sum", []*cel.Type{cel.ListType(cel.DoubleType)}, cel.DoubleType,
				cel.UnaryBinding(sumValue))),
		cel.Function("sha256",
			cel.Overload("template_sha256_string", []*cel.Type{cel.StringType}, cel.StringType,
				cel.UnaryBinding(func(v ref.Val) ref.Val {
					s, ok := v.(types.String)
					if !ok {
						return types.MaybeNoSuchOverloadErr(v)
					}
					return hexDigest([]byte(s))
				})),
			cel.Overload("template_sha256_bytes", []*cel.Type{cel.BytesType}, cel.StringType,
				cel.UnaryBinding(func(v ref.Val) ref.Val {
					b, ok := v.(types.Bytes)
					if !ok {
						return types.MaybeNoSuchOverloadErr(v)
					}
					return hexDigest([]byte(b))
				}))),
	}
}

func (templateLib) ProgramOptions() []cel.ProgramOption {
	return []cel.ProgramOption{}
}

func indentValue(text, width ref.Val) ref.Val {
	s, n, err := stringAndWidth(text, width)
	if err != nil {
		return err
	}
	return types.String(indentString(s, n))
}

func nindentValue(text, width ref.Val) ref.Val {
	s, n, err := stringAndWidth(text, width)
	if err != nil {
		return err
	}
	return types.String("\n" + indentString(s, n))
}

// indentString pads every line of s, leaving blank lines free of trailing whitespace so that
// the result stays clean in whitespace-sensitive formats such as YAML.
func indentString(s string, width int) string {
	pad := strings.Repeat(" ", width)
	lines := strings.Split(s, "\n")
	for i, line := range lines {
		if line == "" {
			continue
		}
		lines[i] = pad + line
	}
	return strings.Join(lines, "\n")
}

func stringAndWidth(text, width ref.Val) (string, int, ref.Val) {
	s, ok := text.(types.String)
	if !ok {
		return "", 0, types.MaybeNoSuchOverloadErr(text)
	}
	n, ok := width.(types.Int)
	if !ok {
		return "", 0, types.MaybeNoSuchOverloadErr(width)
	}
	if n < 0 || n > maxIndent {
		return "", 0, types.NewErr("indent width %d is out of range [0, %d]", int64(n), maxIndent)
	}
	return string(s), int(n), nil
}

func repeatValue(text, count ref.Val) ref.Val {
	s, ok := text.(types.String)
	if !ok {
		return types.MaybeNoSuchOverloadErr(text)
	}
	n, ok := count.(types.Int)
	if !ok {
		return types.MaybeNoSuchOverloadErr(count)
	}
	if n < 0 {
		return types.NewErr("repeat count %d is negative", int64(n))
	}
	if int64(n)*int64(len(s)) > maxIndent*16 {
		return types.NewErr("repeat would produce more than %d bytes", maxIndent*16)
	}
	return types.String(strings.Repeat(string(s), int(n)))
}

func quoteValue(v ref.Val) ref.Val {
	s, err := formatValue(v)
	if err != nil {
		return types.WrapErr(err)
	}
	return types.String(strconv.Quote(s))
}

func squoteValue(v ref.Val) ref.Val {
	s, err := formatValue(v)
	if err != nil {
		return types.WrapErr(err)
	}
	return types.String("'" + strings.ReplaceAll(s, "'", `'\''`) + "'")
}

func jsonValue(v ref.Val, indent string) ref.Val {
	out, err := encodeJSON(v, indent)
	if err != nil {
		return types.WrapErr(err)
	}
	return types.String(out)
}

func jsonIndentValue(v, width ref.Val) ref.Val {
	n, ok := width.(types.Int)
	if !ok {
		return types.MaybeNoSuchOverloadErr(width)
	}
	if n < 0 || n > 16 {
		return types.NewErr("JSON indent width %d is out of range [0, 16]", int64(n))
	}
	return jsonValue(v, strings.Repeat(" ", int(n)))
}

func yamlValue(v ref.Val) ref.Val {
	native, err := toJSONValue(v)
	if err != nil {
		return types.WrapErr(err)
	}
	out, err := yaml.Marshal(native)
	if err != nil {
		return types.WrapErr(fmt.Errorf("cannot render a value of type %s as YAML: %w", v.Type(), err))
	}
	return types.String(strings.TrimSuffix(string(out), "\n"))
}

// sumValue adds the elements of a numeric list. Mixed numeric types are rejected rather than
// coerced, matching CEL's refusal to silently convert between int, uint, and double.
func sumValue(v ref.Val) ref.Val {
	list, ok := v.(traits.Lister)
	if !ok {
		return types.MaybeNoSuchOverloadErr(v)
	}
	size, ok := list.Size().(types.Int)
	if !ok {
		return types.NewErr("cannot determine the size of %s", v.Type())
	}
	if size == 0 {
		return types.Int(0)
	}
	total := list.Get(types.Int(0))
	switch total.(type) {
	case types.Int, types.Uint, types.Double:
	default:
		return types.NewErr("sum requires a list of numbers, found %s", total.Type())
	}
	for i := types.Int(1); i < size; i++ {
		adder, ok := total.(traits.Adder)
		if !ok {
			return types.NewErr("sum requires a list of numbers, found %s", total.Type())
		}
		elem := list.Get(i)
		if elem.Type() != total.Type() {
			return types.NewErr("sum requires a list of a single numeric type, found %s and %s",
				total.Type(), elem.Type())
		}
		total = adder.Add(elem)
		if types.IsError(total) {
			return total
		}
	}
	return total
}

func hexDigest(b []byte) ref.Val {
	sum := sha256.Sum256(b)
	return types.String(hex.EncodeToString(sum[:]))
}
