// Copyright 2024 Google LLC
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

package async

import (
	"testing"
	"time"

	"github.com/google/cel-go/common/types"
)

func TestDefaultCache_Internal(t *testing.T) {
	cache := newDefaultCache(10, 100*time.Millisecond)
	key := "test"
	val := types.Int(1)

	// Test Set
	cache.Set(key, val)

	// Test Get
	v, ok := cache.Get(key)
	if !ok || v != val {
		t.Errorf("Get failed")
	}

	// Test GetStale
	v, isStale, ok := cache.(StaleCache).GetStale(key)
	if !ok || isStale || v != val {
		t.Errorf("GetStale failed")
	}

	// Test TTL
	time.Sleep(150 * time.Millisecond)
	v, ok = cache.Get(key)
	if ok {
		t.Errorf("Get should have failed after TTL")
	}

	v, isStale, ok = cache.(StaleCache).GetStale(key)
	if !ok || !isStale || v != val {
		t.Errorf("GetStale should have returned stale value")
	}

	// Test Delete
	cache.Delete(key)
	_, ok = cache.Get(key)
	if ok {
		t.Errorf("Delete failed")
	}
}

func TestDefaultCache_Eviction(t *testing.T) {
	cache := newDefaultCache(2, 0)
	cache.Set("1", types.Int(1))
	cache.Set("2", types.Int(2))
	
	// This should trigger eviction (clearing the map in our simple implementation)
	cache.Set("3", types.Int(3))
	
	if _, ok := cache.Get("1"); ok {
		t.Errorf("expected 1 to be evicted")
	}
	if v, ok := cache.Get("3"); !ok || v != types.Int(3) {
		t.Errorf("expected 3 to be present")
	}
}
