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
	"reflect"
	"strings"

	"github.com/google/cel-go/cel"
	celast "github.com/google/cel-go/common/ast"
	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
	"github.com/google/cel-go/common/types/traits"
)

// DataVar is the root of the data a Rego policy reads, so that a policy ported
// from Rego reads the cluster inventory by the same path.
const DataVar = "data"

// ExternalDataFunc is the name of the Rego builtin which queries an external
// data provider.
const ExternalDataFunc = "external_data"

// Keys within the data namespace, which follow the shape Gatekeeper replicates
// the cluster into.
const (
	inventoryKey = "inventory"
	clusterKey   = "cluster"
	namespaceKey = "namespace"
)

// RegoCompatibility declares the reads a Rego policy makes, so that a
// constraint ported to CEL keeps the shape its Rego implementation had:
//
//	data.inventory.cluster[<apiVersion>][<kind>][<name>]
//	data.inventory.namespace[<namespace>][<apiVersion>][<kind>][<name>]
//	external_data({"provider": <provider>, "keys": <keys>})
//
// Indexing a kind yields every object of that kind, keyed by name, which is the
// map a Rego policy iterates. The path is read by key rather than enumerated:
// the api versions and namespaces which exist cannot be listed, so a Rego rule
// which iterates them names them here instead.
//
// The lookups are the same as the ones inventory.get and inventory.list make,
// and are answered by the same provider. The direct functions read a single
// object without reading the rest of its kind, so a policy written for CEL
// rather than ported to it should prefer them.
func RegoCompatibility() cel.EnvOption {
	return cel.Lib(&libraryOptions{
		name: "gatekeeper.rego",
		opts: []cel.EnvOption{
			cel.Variable(DataVar, cel.MapType(cel.StringType, cel.DynType)),
			cel.Macros(cel.GlobalMacro(ExternalDataFunc, 1, expandExternalData)),
		},
	})
}

// expandExternalData rewrites the Rego builtin into a call on the value which
// carries the lookups of the review being evaluated. The rewrite happens while
// parsing, so the policy keeps the form a Rego author wrote and the call is
// still type-checked.
func expandExternalData(eh cel.MacroExprFactory, target celast.Expr, args []celast.Expr) (celast.Expr, *cel.Error) {
	return eh.NewMemberCall("get", eh.NewIdent(ExternalDataVar), args[0]), nil
}

// dataValue is the value bound to `data`. Each level of the path narrows what
// is being read; the lookup is made when a kind is reached, since that is the
// point at which the objects to read are known.
type dataValue struct {
	resolver *resolver

	// path holds the keys read so far, below the inventory root.
	path []string

	// namespaced reports whether the path went through `namespace`, which
	// scopes the lookup and adds the namespace as its first key.
	namespaced bool
}

// newDataValue returns the root of the data namespace for one review.
func newDataValue(r *resolver) *dataValue {
	return &dataValue{resolver: r}
}

// Find returns the value at a key of the data namespace, resolving the lookup
// once enough of the path is known to make one.
func (v *dataValue) Find(key ref.Val) (ref.Val, bool) {
	name, ok := key.(types.String)
	if !ok {
		return types.MaybeNoSuchOverloadErr(key), true
	}
	return v.find(string(name))
}

func (v *dataValue) find(name string) (ref.Val, bool) {
	// The root holds only the inventory: the rest of the Rego document is the
	// policy's own rules, which have no equivalent here.
	if len(v.path) == 0 && !v.namespaced {
		if name != inventoryKey {
			return types.NewErr("no such key: data.%s: only data.%s is available to a policy", name, inventoryKey), true
		}
		return &dataValue{resolver: v.resolver, path: []string{inventoryKey}}, true
	}
	switch scope := v.scope(); scope {
	case inventoryKey:
		switch name {
		case clusterKey:
			return &dataValue{resolver: v.resolver, path: []string{clusterKey}}, true
		case namespaceKey:
			return &dataValue{resolver: v.resolver, path: []string{namespaceKey}, namespaced: true}, true
		default:
			return types.NewErr("no such key: data.inventory.%s: the inventory holds %s and %s",
				name, clusterKey, namespaceKey), true
		}
	default:
		return v.descend(name)
	}
}

// scope reports which part of the inventory the path is within.
func (v *dataValue) scope() string {
	if len(v.path) == 1 && v.path[0] == inventoryKey {
		return inventoryKey
	}
	return ""
}

// descend adds a key to the path, making the lookup once the path names a kind.
func (v *dataValue) descend(name string) (ref.Val, bool) {
	path := append(append([]string{}, v.path...), name)
	// The path below the scope is the namespace, if the scope is namespaced,
	// then the api version and the kind.
	keys := path[1:]
	want := 2
	if v.namespaced {
		want = 3
	}
	if len(keys) < want {
		return &dataValue{resolver: v.resolver, path: path, namespaced: v.namespaced}, true
	}
	req := Request{Kind: InventoryList, Namespaced: v.namespaced}
	if v.namespaced {
		req.Namespace, req.APIVersion, req.ResourceKind = keys[0], keys[1], keys[2]
	} else {
		req.APIVersion, req.ResourceKind = keys[0], keys[1]
	}
	return v.resolver.listByName(req), true
}

// Get returns the value at a key, reporting an error for a key the data
// namespace does not hold.
func (v *dataValue) Get(index ref.Val) ref.Val {
	value, _ := v.Find(index)
	return value
}

// Contains reports whether a key can be read. Every key of a kind or api
// version can be, since what exists is only known once it is read.
func (v *dataValue) Contains(key ref.Val) ref.Val {
	value, _ := v.Find(key)
	if types.IsError(value) {
		return types.False
	}
	return types.True
}

// Size reports that the number of keys is not known. The api versions and
// namespaces which exist are not something the inventory can be asked for.
func (v *dataValue) Size() ref.Val {
	return v.notEnumerable()
}

// Iterator reports that this level of the path cannot be enumerated. A kind
// resolves to a map, which can be.
//
// An iterator cannot refuse to be created, so it yields the error as its only
// element, which fails the expression which tried to walk it.
func (v *dataValue) Iterator() traits.Iterator {
	return &errorIterator{err: v.notEnumerable()}
}

func (v *dataValue) notEnumerable() ref.Val {
	return types.NewErr("data.%s cannot be listed: name the %s to read",
		strings.Join(append([]string{inventoryKey}, v.path[1:]...), "."), v.expects())
}

// expects names what the next key of the path is.
func (v *dataValue) expects() string {
	keys := len(v.path) - 1
	if v.namespaced {
		keys--
	}
	switch {
	case v.scope() == inventoryKey:
		return clusterKey + " or " + namespaceKey
	case v.namespaced && len(v.path) == 1:
		return "namespace"
	case keys <= 0:
		return "api version"
	default:
		return "kind"
	}
}

func (v *dataValue) ConvertToNative(typeDesc reflect.Type) (any, error) {
	return nil, fmt.Errorf("type conversion not supported for '%s'", v.Type())
}

func (v *dataValue) ConvertToType(typeVal ref.Type) ref.Val {
	if typeVal == types.TypeType {
		return types.MapType
	}
	return types.NewErr("type conversion not supported from '%s' to '%s'", v.Type(), typeVal)
}

func (v *dataValue) Equal(other ref.Val) ref.Val {
	return types.Bool(v == other)
}

func (v *dataValue) Type() ref.Type {
	return types.MapType
}

func (v *dataValue) Value() any {
	return v.resolver
}

// errorIterator reports the same error however it is used, for the levels of
// the data namespace which cannot be enumerated.
type errorIterator struct {
	err  ref.Val
	done bool
}

func (i *errorIterator) HasNext() ref.Val {
	return types.Bool(!i.done)
}

func (i *errorIterator) Next() ref.Val {
	i.done = true
	return i.err
}

func (i *errorIterator) ConvertToNative(typeDesc reflect.Type) (any, error) {
	return nil, fmt.Errorf("type conversion not supported for an iterator")
}

func (i *errorIterator) ConvertToType(typeVal ref.Type) ref.Val {
	return i.err
}

func (i *errorIterator) Equal(other ref.Val) ref.Val {
	return i.err
}

func (i *errorIterator) Type() ref.Type {
	return types.ErrType
}

func (i *errorIterator) Value() any {
	return i.err
}
