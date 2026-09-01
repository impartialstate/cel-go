// Copyright 2025 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package gatekeeper

import (
	"fmt"
	"regexp"
	"sync"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
	"github.com/google/cel-go/common/types/traits"
)

// comparableTypes are the element types the Kubernetes list library orders.
var comparableTypes = []*cel.Type{
	cel.IntType, cel.UintType, cel.DoubleType, cel.StringType, cel.BoolType,
	cel.DurationType, cel.TimestampType,
}

// summableTypes are the element types the Kubernetes list library adds, paired
// with the value each yields for an empty list.
var summableTypes = []struct {
	celType *cel.Type
	zero    ref.Val
}{
	{cel.IntType, types.Int(0)},
	{cel.UintType, types.Uint(0)},
	{cel.DoubleType, types.Double(0.0)},
	{cel.DurationType, types.Duration{Duration: 0}},
}

// KubernetesLists returns the list functions which Kubernetes makes available
// to admission policies, and which Gatekeeper's CEL engine inherits:
//
//	<list<T>>.isSorted() <bool>
//	<list<T>>.sum() <T>
//	<list<T>>.min() <T>
//	<list<T>>.max() <T>
//	<list<T>>.indexOf(<T>) <int>
//	<list<T>>.lastIndexOf(<T>) <int>
//
// These differ from the cel-go list extension, which Kubernetes does not
// enable. `min` and `max` are errors on an empty list, `sum` yields the zero
// value of the element type, `isSorted` is true, and the index functions return
// -1 when the element is absent.
func KubernetesLists() cel.EnvOption {
	paramA := cel.TypeParamType("A")
	opts := []cel.EnvOption{
		cel.Function("indexOf",
			cel.MemberOverload("list_a_index_of_int",
				[]*cel.Type{cel.ListType(paramA), paramA}, cel.IntType,
				cel.BinaryBinding(indexOf))),
		cel.Function("lastIndexOf",
			cel.MemberOverload("list_a_last_index_of_int",
				[]*cel.Type{cel.ListType(paramA), paramA}, cel.IntType,
				cel.BinaryBinding(lastIndexOf))),
	}
	for _, elemType := range comparableTypes {
		name := elemType.TypeName()
		opts = append(opts,
			cel.Function("isSorted",
				cel.MemberOverload(fmt.Sprintf("list_%s_is_sorted_bool", name),
					[]*cel.Type{cel.ListType(elemType)}, cel.BoolType,
					cel.UnaryBinding(isSorted))),
			cel.Function("min",
				cel.MemberOverload(fmt.Sprintf("list_%s_min_%s", name, name),
					[]*cel.Type{cel.ListType(elemType)}, elemType,
					cel.UnaryBinding(minValue))),
			cel.Function("max",
				cel.MemberOverload(fmt.Sprintf("list_%s_max_%s", name, name),
					[]*cel.Type{cel.ListType(elemType)}, elemType,
					cel.UnaryBinding(maxValue))))
	}
	for _, summable := range summableTypes {
		name := summable.celType.TypeName()
		zero := summable.zero
		opts = append(opts,
			cel.Function("sum",
				cel.MemberOverload(fmt.Sprintf("list_%s_sum_%s", name, name),
					[]*cel.Type{cel.ListType(summable.celType)}, summable.celType,
					cel.UnaryBinding(func(list ref.Val) ref.Val {
						return sum(list, zero)
					}))))
	}
	return cel.Lib(&libraryOptions{name: "gatekeeper.k8s.lists", opts: opts})
}

// KubernetesStrings returns the regular expression functions which Kubernetes
// makes available to admission policies:
//
//	<string>.find(<string>) <string>
//	<string>.findAll(<string>) <list<string>>
//	<string>.findAll(<string>, <int>) <list<string>>
//
// `find` yields the empty string when the expression does not match, and
// `findAll` an empty list. A negative limit means no limit.
func KubernetesStrings() cel.EnvOption {
	return cel.Lib(&libraryOptions{
		name: "gatekeeper.k8s.strings",
		opts: []cel.EnvOption{
			cel.Function("find",
				cel.MemberOverload("string_find_string",
					[]*cel.Type{cel.StringType, cel.StringType}, cel.StringType,
					cel.BinaryBinding(find))),
			cel.Function("findAll",
				cel.MemberOverload("string_find_all_string",
					[]*cel.Type{cel.StringType, cel.StringType}, cel.ListType(cel.StringType),
					cel.BinaryBinding(func(str, pattern ref.Val) ref.Val {
						return findAll(str, pattern, types.Int(-1))
					})),
				cel.MemberOverload("string_find_all_string_int",
					[]*cel.Type{cel.StringType, cel.StringType, cel.IntType}, cel.ListType(cel.StringType),
					cel.FunctionBinding(func(args ...ref.Val) ref.Val {
						return findAll(args[0], args[1], args[2])
					}))),
		},
	})
}

// libraryOptions adapts a list of environment options to the cel.Library
// interface, so that a library is enabled at most once per environment.
type libraryOptions struct {
	name string
	opts []cel.EnvOption
}

func (l *libraryOptions) LibraryName() string {
	return "cel.lib." + l.name
}

func (l *libraryOptions) CompileOptions() []cel.EnvOption {
	return l.opts
}

func (l *libraryOptions) ProgramOptions() []cel.ProgramOption {
	return []cel.ProgramOption{}
}

func isSorted(val ref.Val) ref.Val {
	list, ok := val.(traits.Lister)
	if !ok {
		return types.MaybeNoSuchOverloadErr(val)
	}
	size := list.Size().(types.Int)
	for i := types.Int(1); i < size; i++ {
		cmp, err := compare(list.Get(i-1), list.Get(i))
		if err != nil {
			return err
		}
		if cmp > 0 {
			return types.False
		}
	}
	return types.True
}

func minValue(val ref.Val) ref.Val {
	return extreme(val, "min", func(cmp types.Int) bool { return cmp < 0 })
}

func maxValue(val ref.Val) ref.Val {
	return extreme(val, "max", func(cmp types.Int) bool { return cmp > 0 })
}

// extreme returns the element of a list for which replace reports that it
// should displace the running result.
func extreme(val ref.Val, name string, replace func(types.Int) bool) ref.Val {
	list, ok := val.(traits.Lister)
	if !ok {
		return types.MaybeNoSuchOverloadErr(val)
	}
	size := list.Size().(types.Int)
	if size == 0 {
		return types.NewErr("%s called on an empty list", name)
	}
	result := list.Get(types.Int(0))
	for i := types.Int(1); i < size; i++ {
		elem := list.Get(i)
		cmp, err := compare(elem, result)
		if err != nil {
			return err
		}
		if replace(cmp) {
			result = elem
		}
	}
	return result
}

func sum(val ref.Val, zero ref.Val) ref.Val {
	list, ok := val.(traits.Lister)
	if !ok {
		return types.MaybeNoSuchOverloadErr(val)
	}
	size := list.Size().(types.Int)
	total := zero
	for i := types.Int(0); i < size; i++ {
		adder, ok := total.(traits.Adder)
		if !ok {
			return types.MaybeNoSuchOverloadErr(total)
		}
		total = adder.Add(list.Get(i))
		if types.IsError(total) {
			return total
		}
	}
	return total
}

func indexOf(val, elem ref.Val) ref.Val {
	return findIndex(val, elem, false)
}

func lastIndexOf(val, elem ref.Val) ref.Val {
	return findIndex(val, elem, true)
}

func findIndex(val, elem ref.Val, last bool) ref.Val {
	list, ok := val.(traits.Lister)
	if !ok {
		return types.MaybeNoSuchOverloadErr(val)
	}
	size := list.Size().(types.Int)
	result := types.Int(-1)
	for i := types.Int(0); i < size; i++ {
		if list.Get(i).Equal(elem) != types.True {
			continue
		}
		if !last {
			return i
		}
		result = i
	}
	return result
}

// compare orders two values, reporting an error value for types which have no
// ordering.
func compare(lhs, rhs ref.Val) (types.Int, ref.Val) {
	comparer, ok := lhs.(traits.Comparer)
	if !ok {
		// Booleans are comparable in the Kubernetes list library, but carry no
		// ordering in CEL, so false is ordered before true.
		lhsBool, lhsOK := lhs.(types.Bool)
		rhsBool, rhsOK := rhs.(types.Bool)
		if lhsOK && rhsOK {
			switch {
			case lhsBool == rhsBool:
				return 0, nil
			case bool(rhsBool):
				return -1, nil
			default:
				return 1, nil
			}
		}
		return 0, types.MaybeNoSuchOverloadErr(lhs)
	}
	cmp := comparer.Compare(rhs)
	if types.IsError(cmp) || types.IsUnknown(cmp) {
		return 0, cmp
	}
	result, ok := cmp.(types.Int)
	if !ok {
		return 0, types.MaybeNoSuchOverloadErr(cmp)
	}
	return result, nil
}

func find(strVal, patternVal ref.Val) ref.Val {
	str, pattern, err := regexArgs(strVal, patternVal)
	if err != nil {
		return err
	}
	re, err := compileRegex(pattern)
	if err != nil {
		return err
	}
	return types.String(re.FindString(str))
}

func findAll(strVal, patternVal, limitVal ref.Val) ref.Val {
	str, pattern, err := regexArgs(strVal, patternVal)
	if err != nil {
		return err
	}
	limit, ok := limitVal.(types.Int)
	if !ok {
		return types.MaybeNoSuchOverloadErr(limitVal)
	}
	re, err := compileRegex(pattern)
	if err != nil {
		return err
	}
	matches := re.FindAllString(str, int(limit))
	if matches == nil {
		matches = []string{}
	}
	return types.DefaultTypeAdapter.NativeToValue(matches)
}

func regexArgs(strVal, patternVal ref.Val) (string, string, ref.Val) {
	str, ok := strVal.(types.String)
	if !ok {
		return "", "", types.MaybeNoSuchOverloadErr(strVal)
	}
	pattern, ok := patternVal.(types.String)
	if !ok {
		return "", "", types.MaybeNoSuchOverloadErr(patternVal)
	}
	return string(str), string(pattern), nil
}

var regexCache sync.Map

// compileRegex compiles and caches a regular expression, as policies evaluate
// the same patterns on every admission request.
func compileRegex(pattern string) (*regexp.Regexp, ref.Val) {
	if cached, found := regexCache.Load(pattern); found {
		return cached.(*regexp.Regexp), nil
	}
	re, err := regexp.Compile(pattern)
	if err != nil {
		return nil, types.NewErr("invalid regular expression %q: %v", pattern, err)
	}
	regexCache.Store(pattern, re)
	return re, nil
}
