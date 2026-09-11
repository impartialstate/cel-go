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
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
	"github.com/google/cel-go/common/types/traits"

	structpb "google.golang.org/protobuf/types/known/structpb"
)

// Escaper rewrites an interpolated value before it reaches the output. It is applied to every
// `{{ expr }}` action except those wrapped in raw() or calling a sub-template.
type Escaper func(string) string

// HTMLEscape escapes the characters which are unsafe in HTML element and attribute contexts.
//
// Unlike Go's html/template it is not context aware: it performs the same substitution
// everywhere, which is predictable but does not by itself make arbitrary data safe to
// interpolate into script or style blocks, or into unquoted attributes.
func HTMLEscape(s string) string {
	return htmlEscaper.Replace(s)
}

var htmlEscaper = strings.NewReplacer(
	"&", "&amp;",
	"<", "&lt;",
	">", "&gt;",
	`"`, "&#34;",
	"'", "&#39;",
)

// formatValue renders a CEL value as template output.
//
// Scalars format as their natural textual form. Lists and maps format as canonical JSON with
// sorted keys, which keeps rendered output byte-for-byte reproducible. Values which have no
// meaningful text form, such as an absent optional, are reported as errors rather than being
// silently rendered as an empty string.
func formatValue(v ref.Val) (string, error) {
	switch v := v.(type) {
	case types.String:
		return string(v), nil
	case types.Bool:
		return strconv.FormatBool(bool(v)), nil
	case types.Int:
		return strconv.FormatInt(int64(v), 10), nil
	case types.Uint:
		return strconv.FormatUint(uint64(v), 10), nil
	case types.Double:
		return formatDouble(float64(v)), nil
	case types.Bytes:
		return string(v), nil
	case types.Null:
		return "null", nil
	case types.Duration:
		return v.ConvertToType(types.StringType).Value().(string), nil
	case types.Timestamp:
		return v.Time.UTC().Format(time.RFC3339Nano), nil
	case *types.Err:
		return "", v
	case *types.Optional:
		if !v.HasValue() {
			return "", fmt.Errorf("optional value is absent; supply a fallback with .orValue(...)")
		}
		return formatValue(v.GetValue())
	}
	if t, ok := v.Type().(*types.Type); ok {
		switch t.Kind() {
		case types.ListKind, types.MapKind, types.StructKind:
			return encodeJSON(v, "")
		}
	}
	return "", fmt.Errorf("cannot render a value of type %s", v.Type())
}

// formatDouble renders a float without the exponent notation that %g produces for ordinary
// magnitudes, so that 1000000.0 renders as "1000000" rather than "1e+06".
func formatDouble(f float64) string {
	switch {
	case math.IsNaN(f):
		return "NaN"
	case math.IsInf(f, 1):
		return "+Inf"
	case math.IsInf(f, -1):
		return "-Inf"
	}
	abs := math.Abs(f)
	if abs != 0 && (abs < 1e-6 || abs >= 1e21) {
		return strconv.FormatFloat(f, 'g', -1, 64)
	}
	return strconv.FormatFloat(f, 'f', -1, 64)
}

// encodeJSON renders a value as JSON. An empty indent produces the compact form.
func encodeJSON(v ref.Val, indent string) (string, error) {
	native, err := toJSONValue(v)
	if err != nil {
		return "", err
	}
	var buf strings.Builder
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if indent != "" {
		enc.SetIndent("", indent)
	}
	if err := enc.Encode(native); err != nil {
		return "", err
	}
	return strings.TrimSuffix(buf.String(), "\n"), nil
}

var jsonValueType = reflect.TypeOf(&structpb.Value{})

// toJSONValue converts a CEL value into the Go representation used by encoding/json. Map keys
// are emitted in sorted order by encoding/json, so equal inputs always produce equal output.
func toJSONValue(v ref.Val) (any, error) {
	switch v := v.(type) {
	case types.String:
		return string(v), nil
	case types.Bool:
		return bool(v), nil
	case types.Int:
		return int64(v), nil
	case types.Uint:
		return uint64(v), nil
	case types.Double:
		return float64(v), nil
	case types.Bytes:
		return []byte(v), nil
	case types.Null:
		return nil, nil
	case types.Duration:
		return v.ConvertToType(types.StringType).Value().(string), nil
	case types.Timestamp:
		return v.Time.UTC().Format(time.RFC3339Nano), nil
	case *types.Err:
		return nil, v
	case *types.Optional:
		if !v.HasValue() {
			return nil, nil
		}
		return toJSONValue(v.GetValue())
	}
	switch val := v.(type) {
	case traits.Lister:
		size, ok := val.Size().(types.Int)
		if !ok {
			return nil, fmt.Errorf("cannot determine the size of %s", v.Type())
		}
		out := make([]any, 0, int(size))
		for i := types.Int(0); i < size; i++ {
			elem, err := toJSONValue(val.Get(i))
			if err != nil {
				return nil, err
			}
			out = append(out, elem)
		}
		return out, nil
	case traits.Mapper:
		out := map[string]any{}
		it := val.Iterator()
		for it.HasNext() == types.True {
			k := it.Next()
			key, err := mapKeyString(k)
			if err != nil {
				return nil, err
			}
			elem, err := toJSONValue(val.Get(k))
			if err != nil {
				return nil, err
			}
			out[key] = elem
		}
		return out, nil
	}
	// Structured values, including protobuf messages, round-trip through the canonical JSON
	// representation the CEL type provider already knows how to produce.
	pb, err := v.ConvertToNative(jsonValueType)
	if err != nil {
		return nil, fmt.Errorf("cannot render a value of type %s as JSON: %w", v.Type(), err)
	}
	return pb.(*structpb.Value).AsInterface(), nil
}

// mapKeyString renders a CEL map key as a JSON object key.
func mapKeyString(k ref.Val) (string, error) {
	switch k := k.(type) {
	case types.String:
		return string(k), nil
	case types.Int:
		return strconv.FormatInt(int64(k), 10), nil
	case types.Uint:
		return strconv.FormatUint(uint64(k), 10), nil
	case types.Bool:
		return strconv.FormatBool(bool(k)), nil
	}
	return "", fmt.Errorf("unsupported map key type %s", k.Type())
}

// sortValues orders map keys so that iteration over a map is deterministic. Go maps and CEL
// maps have no intrinsic order, so rendering them unsorted would produce output that differs
// between runs of the same template over the same data.
func sortValues(vals []ref.Val) {
	sort.SliceStable(vals, func(i, j int) bool {
		return compareValues(vals[i], vals[j]) < 0
	})
}

func compareValues(a, b ref.Val) int {
	if ra, rb := keyRank(a), keyRank(b); ra != rb {
		return ra - rb
	}
	switch a := a.(type) {
	case types.String:
		return strings.Compare(string(a), string(b.(types.String)))
	case types.Int:
		return cmpInt(int64(a), int64(b.(types.Int)))
	case types.Uint:
		return cmpInt(int64(a), int64(b.(types.Uint)))
	case types.Bool:
		if bool(a) == bool(b.(types.Bool)) {
			return 0
		}
		if bool(a) {
			return 1
		}
		return -1
	}
	return 0
}

func cmpInt(a, b int64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}

// keyRank groups keys by type so that heterogeneous maps still sort deterministically.
func keyRank(v ref.Val) int {
	switch v.(type) {
	case types.Bool:
		return 0
	case types.Int:
		return 1
	case types.Uint:
		return 2
	case types.String:
		return 3
	default:
		return 4
	}
}
