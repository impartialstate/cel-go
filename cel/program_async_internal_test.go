// Copyright 2026 Google LLC
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

package cel

import "testing"

func TestResolveAsyncMaxConcurrency(t *testing.T) {
	cases := []struct {
		in   int
		want int
	}{
		{0, defaultAsyncMaxConcurrency}, // unset -> default bound
		{5, 5},                          // explicit
		{-1, -1},                        // unlimited preserved
	}
	for _, tc := range cases {
		if got := resolveAsyncMaxConcurrency(tc.in); got != tc.want {
			t.Errorf("resolveAsyncMaxConcurrency(%d) = %d, want %d", tc.in, got, tc.want)
		}
	}
}

func TestResolveCompletionBufferSize(t *testing.T) {
	cases := []struct {
		name       string
		bufferSize int
		maxConc    int
		want       int
	}{
		{"explicit buffer wins", 16, 4, 16},
		{"defaults to concurrency limit", 0, 4, 4},
		{"unset uses default concurrency", 0, 0, defaultAsyncMaxConcurrency},
		{"unlimited concurrency falls back to default", 0, -1, defaultAsyncMaxConcurrency},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := &prog{asyncCompletionBufferSize: tc.bufferSize, asyncMaxConcurrency: tc.maxConc}
			if got := p.resolveCompletionBufferSize(); got != tc.want {
				t.Errorf("resolveCompletionBufferSize() = %d, want %d", got, tc.want)
			}
		})
	}
}
