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
)

// StaticProvider answers referential lookups from a fixed set of objects,
// standing in for the cluster a policy would read in production.
//
// It is the provider to reach for when testing a referential policy: the
// objects are the cluster the policy sees, so a test states the cluster it
// expects a decision under rather than arranging one.
type StaticProvider struct {
	// Objects is the cluster inventory, as Kubernetes objects in the
	// unstructured form the API server returns: nested map[string]any values
	// with apiVersion, kind and metadata fields.
	Objects []any

	// ExternalData is the responses of external data providers, keyed by
	// provider name and then by the key a policy queries.
	ExternalData map[string]map[string]any

	// MissingProviderError, when set, is the error reported for a query to a
	// provider ExternalData does not name. By default an unknown provider
	// responds with an error for every key, as an external data provider which
	// is not installed does.
	MissingProviderError error
}

// Resolve answers a lookup from the objects the provider holds.
func (p *StaticProvider) Resolve(_ context.Context, request Request) (any, error) {
	response := p.resolve(request)
	return response.Value, response.Err
}

// ResolveBatch answers a set of lookups, which the provider holds already and
// so can answer without waiting.
func (p *StaticProvider) ResolveBatch(_ context.Context, requests []Request) ([]Response, error) {
	responses := make([]Response, 0, len(requests))
	for _, req := range requests {
		responses = append(responses, p.resolve(req))
	}
	return responses, nil
}

func (p *StaticProvider) resolve(req Request) Response {
	switch req.Kind {
	case InventoryGet:
		for _, obj := range p.Objects {
			if matchesRequest(obj, req) && objectField(obj, "metadata", "name") == req.Name {
				return Response{Value: obj}
			}
		}
		return Response{Value: nil}
	case InventoryList:
		matches := []any{}
		for _, obj := range p.Objects {
			if matchesRequest(obj, req) {
				matches = append(matches, obj)
			}
		}
		return Response{Value: matches}
	case ExternalData:
		return p.resolveExternalData(req)
	default:
		return Response{Err: fmt.Errorf("unsupported request kind: %v", req.Kind)}
	}
}

// resolveExternalData answers a provider query with the response shape of the
// Rego `external_data` builtin, so that a policy ported from Rego reads it the
// same way.
func (p *StaticProvider) resolveExternalData(req Request) Response {
	values, found := p.ExternalData[req.Provider]
	if !found {
		if p.MissingProviderError != nil {
			return Response{Err: p.MissingProviderError}
		}
		return Response{Value: map[string]any{
			"responses":    []any{},
			"errors":       keyErrors(req.Keys, "provider "+req.Provider+" is not installed"),
			"status_code":  int64(0),
			"system_error": "provider " + req.Provider + " is not installed",
		}}
	}
	resolved := []any{}
	var missing []string
	for _, key := range req.Keys {
		value, found := values[key]
		if !found {
			missing = append(missing, key)
			continue
		}
		resolved = append(resolved, []any{key, value})
	}
	return Response{Value: map[string]any{
		"responses":    resolved,
		"errors":       keyErrors(missing, "no value for key"),
		"status_code":  int64(200),
		"system_error": "",
	}}
}

func keyErrors(keys []string, message string) []any {
	errors := []any{}
	for _, key := range keys {
		errors = append(errors, []any{key, message})
	}
	return errors
}

// matchesRequest reports whether an object is of the kind, and in the scope, a
// request asks for.
func matchesRequest(obj any, req Request) bool {
	if objectField(obj, "apiVersion") != req.APIVersion || objectField(obj, "kind") != req.ResourceKind {
		return false
	}
	if !req.Namespaced {
		// A get without a namespace reads a cluster-scoped object, while a list
		// without one reads every namespace.
		return req.Kind == InventoryList || objectField(obj, "metadata", "namespace") == ""
	}
	return objectField(obj, "metadata", "namespace") == req.Namespace
}

// objectField reads a string field from an unstructured object, returning the
// empty string when the path is absent.
func objectField(obj any, path ...string) string {
	current := obj
	for _, field := range path {
		fields, ok := toStringMap(current)
		if !ok {
			return ""
		}
		current, ok = fields[field]
		if !ok {
			return ""
		}
	}
	value, _ := current.(string)
	return value
}
