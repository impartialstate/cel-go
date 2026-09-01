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
	"context"
	"fmt"
	"reflect"
	"strings"
	"sync"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
)

// Variables which carry referential data into a policy. They have no equivalent
// in a ValidatingAdmissionPolicy, and correspond to what Rego policies read
// from `data.inventory` and fetch with `external_data`.
const (
	// InventoryVar is the replicated cache of cluster resources.
	InventoryVar = "inventory"

	// ExternalDataVar is the set of external data providers a policy may query.
	ExternalDataVar = "externalData"
)

// CEL types for the values bound to the referential data variables.
var (
	// InventoryType is the type of the `inventory` variable.
	InventoryType = cel.OpaqueType("gatekeeper.Inventory")

	// ExternalDataType is the type of the `externalData` variable.
	ExternalDataType = cel.OpaqueType("gatekeeper.ExternalData")
)

// RequestKind distinguishes the referential lookups a policy can make.
type RequestKind int

const (
	// InventoryGet reads a single object from the cluster inventory. A request
	// for an object which does not exist is answered with a nil value.
	InventoryGet RequestKind = iota + 1

	// InventoryList reads every object of a kind from the cluster inventory,
	// within a namespace when the request names one.
	InventoryList

	// ExternalData queries an external data provider, as the Rego
	// `external_data` builtin does.
	ExternalData
)

// String returns the name of the request kind.
func (k RequestKind) String() string {
	switch k {
	case InventoryGet:
		return "inventory.get"
	case InventoryList:
		return "inventory.list"
	case ExternalData:
		return "externalData.get"
	default:
		return fmt.Sprintf("unknown(%d)", int(k))
	}
}

// Request is a referential lookup made by a policy while it is evaluated.
type Request struct {
	// Kind is the lookup being made, and determines which of the remaining
	// fields are set.
	Kind RequestKind

	// APIVersion is the group and version of the resource being read, such as
	// "v1" or "apps/v1". Set for inventory lookups.
	APIVersion string

	// ResourceKind is the kind of the resource being read, such as "Service".
	// Set for inventory lookups.
	ResourceKind string

	// Namespace is the namespace being read from. It is empty for a
	// cluster-scoped object, and for a list across all namespaces.
	Namespace string

	// Namespaced reports whether the lookup named a namespace, which
	// distinguishes a cluster-scoped lookup from one across all namespaces.
	Namespaced bool

	// Name is the name of the object being read. Set for InventoryGet.
	Name string

	// Provider is the name of the external data provider being queried. Set for
	// ExternalData.
	Provider string

	// Keys are the keys being sent to the external data provider. Set for
	// ExternalData.
	Keys []string
}

// Response is the answer to a single Request.
type Response struct {
	// Value is the data the policy reads, and its shape follows the kind of the
	// request: the object, or nil, for InventoryGet; the list of objects for
	// InventoryList; and the provider response for ExternalData.
	//
	// A response is converted to a CEL value with the standard type adapter, so
	// Kubernetes objects are supplied as map[string]any, the form the API
	// server and Gatekeeper's cache hold them in.
	Value any

	// Err reports a lookup which could not be answered. It fails the review
	// rather than being visible to the policy, since a policy which cannot read
	// the data it depends on cannot decide.
	Err error
}

// DataProvider answers the referential lookups made by a policy.
//
// Lookups are answered in batches: a policy's expressions are evaluated to
// discover every lookup it can make, all of them are passed to the provider at
// once, and evaluation resumes with the answers. A provider is free to resolve a
// batch concurrently, and Parallel does so for providers which answer one
// request at a time.
//
// Responses must be returned in the order of the requests.
type DataProvider interface {
	Resolve(ctx context.Context, requests []Request) ([]Response, error)
}

// SingleDataProvider answers one referential lookup at a time. Parallel adapts
// it to the batched DataProvider interface.
type SingleDataProvider interface {
	Resolve(ctx context.Context, request Request) (any, error)
}

// Parallel adapts a provider which answers one request at a time into one which
// answers a batch, resolving up to limit requests concurrently. A limit of zero
// or less places no bound on concurrency.
func Parallel(provider SingleDataProvider, limit int) DataProvider {
	return &parallelProvider{provider: provider, limit: limit}
}

type parallelProvider struct {
	provider SingleDataProvider
	limit    int
}

func (p *parallelProvider) Resolve(ctx context.Context, requests []Request) ([]Response, error) {
	responses := make([]Response, len(requests))
	var tokens chan struct{}
	if p.limit > 0 {
		tokens = make(chan struct{}, p.limit)
	}
	var wg sync.WaitGroup
	for i, req := range requests {
		wg.Add(1)
		go func(i int, req Request) {
			defer wg.Done()
			if tokens != nil {
				tokens <- struct{}{}
				defer func() { <-tokens }()
			}
			if err := ctx.Err(); err != nil {
				responses[i] = Response{Err: err}
				return
			}
			value, err := p.provider.Resolve(ctx, req)
			responses[i] = Response{Value: value, Err: err}
		}(i, req)
	}
	wg.Wait()
	return responses, ctx.Err()
}

// DataProviderFunc adapts a function to the SingleDataProvider interface.
type DataProviderFunc func(ctx context.Context, request Request) (any, error)

// Resolve calls the function.
func (f DataProviderFunc) Resolve(ctx context.Context, request Request) (any, error) {
	return f(ctx, request)
}

// resolver holds the referential data read during a single review: the lookups
// a policy has asked for, and the answers gathered so far.
//
// A lookup which has not been answered evaluates to an unknown value, which CEL
// propagates through the expression without failing it. The review then fetches
// every lookup the expression reached and evaluates it again, so that
// independent lookups are fetched together rather than one at a time, and a
// lookup which the expression short-circuits past is never fetched at all.
type resolver struct {
	provider DataProvider
	answers  map[string]ref.Val
	pending  map[string]Request
	order    []string
}

func newResolver(provider DataProvider) *resolver {
	return &resolver{
		provider: provider,
		answers:  map[string]ref.Val{},
		pending:  map[string]Request{},
	}
}

// lookup returns the answer to a request, recording it as pending and returning
// an unknown value when it has not been fetched yet.
func (r *resolver) lookup(req Request) ref.Val {
	key := requestKey(req)
	if answer, found := r.answers[key]; found {
		return answer
	}
	if _, found := r.pending[key]; !found {
		r.pending[key] = req
		r.order = append(r.order, key)
	}
	return types.NewUnknown(0, types.NewAttributeTrail(req.Kind.String()))
}

// hasPending reports whether any lookup is waiting to be fetched.
func (r *resolver) hasPending() bool {
	return len(r.pending) != 0
}

// fetch resolves every pending lookup in one call to the provider.
func (r *resolver) fetch(ctx context.Context) error {
	if len(r.order) == 0 {
		return nil
	}
	if r.provider == nil {
		req := r.pending[r.order[0]]
		return fmt.Errorf("the policy reads %s but no data provider is configured: "+
			"pass gatekeeper.WithDataProvider to supply the cluster inventory and external data", req.Kind)
	}
	requests := make([]Request, 0, len(r.order))
	for _, key := range r.order {
		requests = append(requests, r.pending[key])
	}
	responses, err := r.provider.Resolve(ctx, requests)
	if err != nil {
		return err
	}
	if len(responses) != len(requests) {
		return fmt.Errorf("data provider returned %d responses for %d requests", len(responses), len(requests))
	}
	for i, response := range responses {
		if response.Err != nil {
			return fmt.Errorf("%s: %w", describeRequest(requests[i]), response.Err)
		}
		r.answers[r.order[i]] = types.DefaultTypeAdapter.NativeToValue(response.Value)
	}
	r.pending = map[string]Request{}
	r.order = nil
	return nil
}

// requestKey identifies a request, so that the same lookup made twice is
// fetched once.
func requestKey(req Request) string {
	return strings.Join([]string{
		req.Kind.String(), req.APIVersion, req.ResourceKind,
		fmt.Sprintf("%t", req.Namespaced), req.Namespace, req.Name,
		req.Provider, strings.Join(req.Keys, ","),
	}, "|")
}

// describeRequest renders a request for an error message.
func describeRequest(req Request) string {
	switch req.Kind {
	case InventoryGet, InventoryList:
		target := req.APIVersion + " " + req.ResourceKind
		if req.Namespaced {
			target += " in namespace " + req.Namespace
		}
		if req.Name != "" {
			target += " named " + req.Name
		}
		return fmt.Sprintf("%s(%s)", req.Kind, target)
	case ExternalData:
		return fmt.Sprintf("%s(provider %s, keys %s)", req.Kind, req.Provider, strings.Join(req.Keys, ", "))
	default:
		return req.Kind.String()
	}
}

// DataFunctions declares the functions which read referential data. The
// functions read from the values bound to the `inventory` and `externalData`
// variables, which carry the lookups of a single review.
//
//	inventory.get(<apiVersion>, <kind>, <name>) <dyn>
//	inventory.get(<apiVersion>, <kind>, <namespace>, <name>) <dyn>
//	inventory.list(<apiVersion>, <kind>) <list<dyn>>
//	inventory.list(<apiVersion>, <kind>, <namespace>) <list<dyn>>
//	externalData.get(<provider>, <list<string>>) <map<string, dyn>>
//
// A `get` of an object which is not in the cluster yields null, so a policy can
// test for absence. The three argument form of `get` reads a cluster-scoped
// object, and the two argument form of `list` reads every object of a kind
// across all namespaces.
//
// The response of externalData.get has the shape Rego's `external_data` builtin
// returns: `responses` holds the key and value pairs which resolved, `errors`
// the pairs which did not.
func DataFunctions() cel.EnvOption {
	return cel.Lib(&libraryOptions{
		name: "gatekeeper.data",
		opts: []cel.EnvOption{
			cel.Variable(InventoryVar, InventoryType),
			cel.Variable(ExternalDataVar, ExternalDataType),
			cel.Function("get",
				cel.MemberOverload("inventory_get_cluster",
					[]*cel.Type{InventoryType, cel.StringType, cel.StringType, cel.StringType}, cel.DynType,
					cel.FunctionBinding(inventoryGet)),
				cel.MemberOverload("inventory_get_namespaced",
					[]*cel.Type{InventoryType, cel.StringType, cel.StringType, cel.StringType, cel.StringType}, cel.DynType,
					cel.FunctionBinding(inventoryGet)),
				cel.MemberOverload("external_data_get",
					[]*cel.Type{ExternalDataType, cel.StringType, cel.ListType(cel.StringType)},
					cel.MapType(cel.StringType, cel.DynType),
					cel.FunctionBinding(externalDataGet))),
			cel.Function("list",
				cel.MemberOverload("inventory_list_cluster",
					[]*cel.Type{InventoryType, cel.StringType, cel.StringType}, cel.ListType(cel.DynType),
					cel.FunctionBinding(inventoryList)),
				cel.MemberOverload("inventory_list_namespaced",
					[]*cel.Type{InventoryType, cel.StringType, cel.StringType, cel.StringType}, cel.ListType(cel.DynType),
					cel.FunctionBinding(inventoryList))),
		},
	})
}

func inventoryGet(args ...ref.Val) ref.Val {
	r, ok := args[0].(*inventoryValue)
	if !ok {
		return types.MaybeNoSuchOverloadErr(args[0])
	}
	strs, err := stringArgs(args[1:])
	if err != nil {
		return err
	}
	req := Request{Kind: InventoryGet, APIVersion: strs[0], ResourceKind: strs[1]}
	if len(strs) == 4 {
		req.Namespaced, req.Namespace, req.Name = true, strs[2], strs[3]
	} else {
		req.Name = strs[2]
	}
	return r.resolver.lookup(req)
}

func inventoryList(args ...ref.Val) ref.Val {
	r, ok := args[0].(*inventoryValue)
	if !ok {
		return types.MaybeNoSuchOverloadErr(args[0])
	}
	strs, err := stringArgs(args[1:])
	if err != nil {
		return err
	}
	req := Request{Kind: InventoryList, APIVersion: strs[0], ResourceKind: strs[1]}
	if len(strs) == 3 {
		req.Namespaced, req.Namespace = true, strs[2]
	}
	return r.resolver.lookup(req)
}

func externalDataGet(args ...ref.Val) ref.Val {
	r, ok := args[0].(*externalDataValue)
	if !ok {
		return types.MaybeNoSuchOverloadErr(args[0])
	}
	provider, ok := args[1].(types.String)
	if !ok {
		return types.MaybeNoSuchOverloadErr(args[1])
	}
	keys, err := args[2].ConvertToNative(reflect.TypeOf([]string{}))
	if err != nil {
		return types.WrapErr(err)
	}
	return r.resolver.lookup(Request{
		Kind:     ExternalData,
		Provider: string(provider),
		Keys:     keys.([]string),
	})
}

func stringArgs(args []ref.Val) ([]string, ref.Val) {
	out := make([]string, 0, len(args))
	for _, arg := range args {
		str, ok := arg.(types.String)
		if !ok {
			return nil, types.MaybeNoSuchOverloadErr(arg)
		}
		out = append(out, string(str))
	}
	return out, nil
}

// inventoryValue is the value bound to the `inventory` variable, holding the
// lookups of the review being evaluated.
type inventoryValue struct {
	resolver *resolver
}

func (v *inventoryValue) ConvertToNative(typeDesc reflect.Type) (any, error) {
	return nil, fmt.Errorf("type conversion not supported for '%s'", InventoryType)
}

func (v *inventoryValue) ConvertToType(typeVal ref.Type) ref.Val {
	if typeVal == types.TypeType {
		return InventoryType
	}
	return types.NewErr("type conversion not supported from '%s' to '%s'", InventoryType, typeVal)
}

func (v *inventoryValue) Equal(other ref.Val) ref.Val {
	return types.Bool(v == other)
}

func (v *inventoryValue) Type() ref.Type {
	return InventoryType
}

func (v *inventoryValue) Value() any {
	return v.resolver
}

// externalDataValue is the value bound to the `externalData` variable.
type externalDataValue struct {
	resolver *resolver
}

func (v *externalDataValue) ConvertToNative(typeDesc reflect.Type) (any, error) {
	return nil, fmt.Errorf("type conversion not supported for '%s'", ExternalDataType)
}

func (v *externalDataValue) ConvertToType(typeVal ref.Type) ref.Val {
	if typeVal == types.TypeType {
		return ExternalDataType
	}
	return types.NewErr("type conversion not supported from '%s' to '%s'", ExternalDataType, typeVal)
}

func (v *externalDataValue) Equal(other ref.Val) ref.Val {
	return types.Bool(v == other)
}

func (v *externalDataValue) Type() ref.Type {
	return ExternalDataType
}

func (v *externalDataValue) Value() any {
	return v.resolver
}
