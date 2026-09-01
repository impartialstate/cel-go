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
	"strings"
	"testing"
)

func regoCluster() *StaticProvider {
	return &StaticProvider{
		Objects: []any{
			map[string]any{
				"apiVersion": "v1",
				"kind":       "Namespace",
				"metadata": map[string]any{
					"name":   "prod",
					"labels": map[string]any{"tier": "critical"},
				},
			},
			map[string]any{
				"apiVersion": "v1",
				"kind":       "Namespace",
				"metadata":   map[string]any{"name": "dev"},
			},
			map[string]any{
				"apiVersion": "v1",
				"kind":       "Service",
				"metadata":   map[string]any{"name": "web", "namespace": "prod"},
				"spec":       map[string]any{"selector": map[string]any{"app": "web"}},
			},
		},
		ExternalData: map[string]map[string]any{
			"cosign": {"registry.example.com/web:1.0": "signed"},
		},
	}
}

func regoPod(namespace string, labels map[string]any, images ...string) map[string]any {
	containers := make([]any, 0, len(images))
	for _, image := range images {
		containers = append(containers, map[string]any{"image": image})
	}
	return map[string]any{
		"apiVersion": "v1",
		"kind":       "Pod",
		"metadata":   map[string]any{"name": "web-1", "namespace": namespace, "labels": labels},
		"spec":       map[string]any{"containers": containers},
	}
}

// TestRegoShapedReads reviews a policy which reads the inventory by the paths a
// Rego implementation uses, and queries a provider with the Rego builtin.
func TestRegoShapedReads(t *testing.T) {
	tmpl := compileTemplate(t, "k8sregoinventory", WithDataProvider(regoCluster()))
	params := map[string]any{"provider": "cosign"}
	tests := []struct {
		name   string
		object map[string]any
		want   []string
	}{
		{
			name:   "selected by a service, in a labelled namespace, with a signed image",
			object: regoPod("prod", map[string]any{"app": "web"}, "registry.example.com/web:1.0"),
		},
		{
			name:   "no service selects the pod",
			object: regoPod("prod", map[string]any{"app": "api"}, "registry.example.com/web:1.0"),
			want:   []string{"no service in prod selects this pod"},
		},
		{
			name:   "a namespace with no labels and an unsigned image",
			object: regoPod("dev", map[string]any{"app": "web"}, "docker.io/web:latest"),
			want: []string{
				"no service in dev selects this pod",
				"the namespace carries no labels",
				"unsigned images: docker.io/web:latest",
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			violations, err := tmpl.Review(context.Background(), Review{Object: tc.object, Parameters: params})
			if err != nil {
				t.Fatalf("Review() failed: %v", err)
			}
			if got := messages(violations); strings.Join(got, "; ") != strings.Join(tc.want, "; ") {
				t.Errorf("Review() got %v, wanted %v", got, tc.want)
			}
		})
	}
}

// TestRegoReadsAreBatched confirms that the inventory reads a policy makes
// through the data namespace are gathered into one call to the provider, the
// same as the reads it makes through the functions.
func TestRegoReadsAreBatched(t *testing.T) {
	var batches [][]Request
	provider := &recordingProvider{
		record:   func(reqs []Request) { batches = append(batches, reqs) },
		provider: regoCluster(),
	}
	tmpl := compileTemplate(t, "k8sregoinventory", WithDataProvider(provider))
	if _, err := tmpl.Review(context.Background(), Review{
		Object:     regoPod("prod", map[string]any{"app": "web"}, "registry.example.com/web:1.0"),
		Parameters: map[string]any{"provider": "cosign"},
	}); err != nil {
		t.Fatalf("Review() failed: %v", err)
	}
	if len(batches) != 1 {
		t.Fatalf("provider was called %d times with %v, wanted a single batch", len(batches), batches)
	}
	if len(batches[0]) != 3 {
		t.Errorf("batch holds %d requests, wanted the two inventory reads and the provider query", len(batches[0]))
	}
}

// TestRegoDataErrors checks the errors reported for paths the data namespace
// does not hold, which should say what can be read instead.
func TestRegoDataErrors(t *testing.T) {
	tests := []struct {
		name       string
		expression string
		want       string
	}{
		{
			name:       "a path outside the inventory",
			expression: "data.rules.size() == 0",
			want:       "only data.inventory is available",
		},
		{
			name:       "a scope the inventory does not hold",
			expression: "data.inventory.everything.size() == 0",
			want:       "the inventory holds cluster and namespace",
		},
		{
			name:       "listing the namespaces which exist",
			expression: "data.inventory.namespace.all(ns, ns != '')",
			want:       "cannot be listed",
		},
		{
			name:       "listing the api versions which exist",
			expression: `data.inventory.cluster.all(v, v != "")`,
			want:       "cannot be listed",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tmpl, err := Compile(templateWithValidation(t, tc.expression), WithDataProvider(regoCluster()))
			if err != nil {
				t.Fatalf("Compile() failed: %v", err)
			}
			_, err = tmpl.Review(context.Background(), Review{Object: regoPod("prod", nil)})
			if err == nil {
				t.Fatalf("Review() succeeded, wanted %q", tc.want)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("Review() error is %q, wanted it to report %q", err, tc.want)
			}
		})
	}
}
