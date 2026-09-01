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
	"errors"
	"strings"
	"sync"
	"testing"
)

func ingress(namespace, name string, hosts ...string) map[string]any {
	rules := make([]any, 0, len(hosts))
	for _, host := range hosts {
		rules = append(rules, map[string]any{"host": host})
	}
	return map[string]any{
		"apiVersion": "networking.k8s.io/v1",
		"kind":       "Ingress",
		"metadata":   map[string]any{"namespace": namespace, "name": name},
		"spec":       map[string]any{"rules": rules},
	}
}

func TestReviewReferential(t *testing.T) {
	cluster := &StaticProvider{
		Objects: []any{
			ingress("prod", "web", "example.com"),
			ingress("staging", "web", "staging.example.com"),
		},
	}
	tmpl := compileTemplate(t, "k8suniqueingresshost", WithDataProvider(cluster))
	tests := []struct {
		name   string
		object map[string]any
		params any
		want   []string
	}{
		{
			name:   "unclaimed host",
			object: ingress("dev", "web", "dev.example.com"),
		},
		{
			name:   "host claimed by another namespace",
			object: ingress("dev", "web", "example.com"),
			want:   []string{"ingress host is already claimed by: prod/web"},
		},
		{
			name:   "an ingress does not conflict with itself",
			object: ingress("prod", "web", "example.com"),
		},
		{
			name:   "conflict with an exempt namespace is allowed",
			object: ingress("dev", "web", "example.com"),
			params: map[string]any{"exemptNamespaces": []any{"prod"}},
		},
		{
			name: "an ingress with no rules is not matched",
			object: map[string]any{
				"apiVersion": "networking.k8s.io/v1",
				"kind":       "Ingress",
				"metadata":   map[string]any{"namespace": "dev", "name": "empty"},
				"spec":       map[string]any{},
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			violations, err := tmpl.Review(context.Background(), Review{Object: tc.object, Parameters: tc.params})
			if err != nil {
				t.Fatalf("Review() failed: %v", err)
			}
			got := messages(violations)
			if strings.Join(got, "; ") != strings.Join(tc.want, "; ") {
				t.Errorf("Review() got %v, wanted %v", got, tc.want)
			}
		})
	}
}

// TestReviewWithoutProvider confirms that a referential policy evaluated with
// no provider fails with an error which names the lookup it could not make,
// rather than deciding on absent data.
func TestReviewWithoutProvider(t *testing.T) {
	tmpl := compileTemplate(t, "k8suniqueingresshost")
	_, err := tmpl.Review(context.Background(), Review{Object: ingress("dev", "web", "dev.example.com")})
	if err == nil {
		t.Fatal("Review() succeeded, wanted an error naming the unresolved lookup")
	}
	if !strings.Contains(err.Error(), "inventory.list") || !strings.Contains(err.Error(), "no data provider") {
		t.Errorf("Review() error is %q, wanted it to name inventory.list and the missing provider", err)
	}
}

// TestReviewBatchesLookups confirms that the lookups a policy makes are
// gathered and resolved together, rather than one round trip at a time.
func TestReviewBatchesLookups(t *testing.T) {
	var mu sync.Mutex
	var batches [][]Request
	provider := &recordingProvider{
		record: func(reqs []Request) {
			mu.Lock()
			defer mu.Unlock()
			batches = append(batches, reqs)
		},
		provider: &StaticProvider{Objects: []any{ingress("prod", "web", "example.com")}},
	}
	tmpl := compileTemplate(t, "k8smultilookup", WithDataProvider(provider))
	if _, err := tmpl.Review(context.Background(), Review{
		Object: map[string]any{
			"apiVersion": "v1",
			"kind":       "Pod",
			"metadata":   map[string]any{"namespace": "dev", "name": "nginx"},
		},
	}); err != nil {
		t.Fatalf("Review() failed: %v", err)
	}
	if len(batches) != 1 {
		t.Fatalf("provider was called %d times with %v, wanted a single batch", len(batches), batches)
	}
	if len(batches[0]) != 2 {
		t.Errorf("batch holds %d requests, wanted the policy's two independent lookups", len(batches[0]))
	}
}

// TestReviewParallelProvider confirms the batch adapter resolves the requests
// of a batch concurrently: neither lookup returns until both have started.
func TestReviewParallelProvider(t *testing.T) {
	started := make(chan struct{}, 2)
	provider := Parallel(DataProviderFunc(func(ctx context.Context, req Request) (any, error) {
		started <- struct{}{}
		<-ctx.Done()
		return nil, ctx.Err()
	}), 0)
	tmpl := compileTemplate(t, "k8smultilookup", WithDataProvider(provider))
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		// Both lookups of the batch are in flight before either can return.
		<-started
		<-started
		cancel()
	}()
	_, err := tmpl.Review(ctx, Review{
		Object: map[string]any{
			"apiVersion": "v1",
			"kind":       "Pod",
			"metadata":   map[string]any{"namespace": "dev", "name": "nginx"},
		},
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Review() error is %v, wanted the cancelled context", err)
	}
}

// TestReviewChainedLookups confirms that a lookup which depends on the result
// of an earlier one is resolved in a later round.
func TestReviewChainedLookups(t *testing.T) {
	cluster := &StaticProvider{
		Objects: []any{
			map[string]any{
				"apiVersion": "v1",
				"kind":       "Namespace",
				"metadata": map[string]any{
					"name":   "dev",
					"labels": map[string]any{"owner": "platform"},
				},
			},
			map[string]any{
				"apiVersion": "v1",
				"kind":       "ConfigMap",
				"metadata":   map[string]any{"namespace": "config", "name": "platform"},
			},
		},
	}
	var rounds int
	provider := &recordingProvider{
		record:   func([]Request) { rounds++ },
		provider: cluster,
	}
	tmpl := compileTemplate(t, "k8schainedlookup", WithDataProvider(provider))
	pod := func(namespace string) map[string]any {
		return map[string]any{
			"apiVersion": "v1",
			"kind":       "Pod",
			"metadata":   map[string]any{"namespace": namespace, "name": "nginx"},
		}
	}
	violations, err := tmpl.Review(context.Background(), Review{Object: pod("dev")})
	if err != nil {
		t.Fatalf("Review() failed: %v", err)
	}
	if len(violations) != 0 {
		t.Errorf("Review() got %v, wanted no violations", messages(violations))
	}
	if rounds != 2 {
		t.Errorf("provider was called %d times, wanted one round per dependent lookup", rounds)
	}
	violations, err = tmpl.Review(context.Background(), Review{Object: pod("unlabelled")})
	if err != nil {
		t.Fatalf("Review() failed: %v", err)
	}
	want := "namespace unlabelled has no owner configuration in the config namespace"
	if len(violations) != 1 || violations[0].Message != want {
		t.Errorf("Review() got %v, wanted [%q]", messages(violations), want)
	}
}

// TestReviewExternalData confirms that a policy reads an external data response
// in the shape Rego's external_data builtin returns.
func TestReviewExternalData(t *testing.T) {
	provider := &StaticProvider{
		ExternalData: map[string]map[string]any{
			"image-validator": {"registry.example.com/nginx:1.2.3": "valid"},
		},
	}
	tmpl := compileTemplate(t, "k8sexternaldata", WithDataProvider(provider))
	pod := func(images ...string) map[string]any {
		containers := make([]any, 0, len(images))
		for _, image := range images {
			containers = append(containers, map[string]any{"image": image})
		}
		return map[string]any{
			"apiVersion": "v1",
			"kind":       "Pod",
			"metadata":   map[string]any{"namespace": "dev", "name": "nginx"},
			"spec":       map[string]any{"containers": containers},
		}
	}
	tests := []struct {
		name   string
		object map[string]any
		params any
		want   []string
	}{
		{
			name:   "every image resolves",
			object: pod("registry.example.com/nginx:1.2.3"),
			params: map[string]any{"provider": "image-validator"},
		},
		{
			name:   "an image the provider rejects",
			object: pod("registry.example.com/nginx:1.2.3", "docker.io/nginx:latest"),
			params: map[string]any{"provider": "image-validator"},
			want:   []string{"images rejected by the external data provider: docker.io/nginx:latest"},
		},
		{
			name:   "a provider which is not installed",
			object: pod("registry.example.com/nginx:1.2.3"),
			params: map[string]any{"provider": "missing"},
			want: []string{
				"the external data provider failed: provider missing is not installed",
				"images rejected by the external data provider: registry.example.com/nginx:1.2.3",
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			violations, err := tmpl.Review(context.Background(), Review{Object: tc.object, Parameters: tc.params})
			if err != nil {
				t.Fatalf("Review() failed: %v", err)
			}
			if got := messages(violations); strings.Join(got, "; ") != strings.Join(tc.want, "; ") {
				t.Errorf("Review() got %v, wanted %v", got, tc.want)
			}
		})
	}
}

type recordingProvider struct {
	record   func([]Request)
	provider DataProvider
}

func (p *recordingProvider) Resolve(ctx context.Context, requests []Request) ([]Response, error) {
	p.record(requests)
	return p.provider.Resolve(ctx, requests)
}
