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

package cel

import (
	"context"
	"fmt"
	"hash/fnv"
	"strconv"
	"sync"
	"time"

	"errors"

	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
	"github.com/google/cel-go/interpreter"
)

// AsyncCall describes a pending or completed asynchronous function call.
// This interface exposes a safe, read-only view of the internal interpreter state.
type AsyncCall = interpreter.AsyncCall

// AsyncObserver provides callbacks for monitoring the lifecycle of asynchronous function calls.
type AsyncObserver = interpreter.AsyncObserver

// DrainAction dictates what ConcurrentEval should do after inspecting completions.
type DrainAction struct {
	// Reevaluate indicates that the AST should be re-evaluated immediately.
	// If true, WaitDuration is ignored.
	Reevaluate bool
	// WaitDuration indicates how long the evaluator should wait for additional
	// completions before deciding to re-evaluate. A duration of 0 means wait
	// indefinitely (block on the next completion).
	WaitDuration time.Duration
}

// DrainStrategy controls when ConcurrentEval re-evaluates after async completions.
//
// The evaluator consults the strategy each time a completion is received.
type DrainStrategy interface {
	// NextAction evaluates the current state of asynchronous evaluation and
	// determines the next step.
	//
	// - completed: The set of completions accumulated in the current batch.
	// - pending: The number of async calls currently launched but unresolved.
	NextAction(completed []AsyncCall, pending int) DrainAction
}

// DrainNone returns a strategy that re-evaluates after every single completion.
// This is the default strategy.
func DrainNone() DrainStrategy {
	return drainNone{}
}

type drainNone struct{}

func (drainNone) NextAction(completed []AsyncCall, pending int) DrainAction {
	return DrainAction{Reevaluate: pending == 0 || len(completed) > 0}
}

// DrainReady returns a strategy that waits for a short duration after the first
// completion to batch any other functions that complete at roughly the same time.
func DrainReady(debounce time.Duration) DrainStrategy {
	return drainReady{debounce: debounce}
}

type drainReady struct {
	debounce time.Duration
}

func (d drainReady) NextAction(completed []AsyncCall, pending int) DrainAction {
	if pending == 0 {
		return DrainAction{Reevaluate: true} // Nothing left to wait for
	}
	if len(completed) == 0 {
		return DrainAction{Reevaluate: false, WaitDuration: 0} // Wait indefinitely for first
	}
	return DrainAction{Reevaluate: false, WaitDuration: d.debounce} // Wait for debounce period
}

// DrainAll returns a strategy that waits for all currently pending calls to
// complete before re-evaluating.
//
// Note: This strategy is optimal for independent async calls, but will over-wait
// if some calls depend on the results of others.
func DrainAll() DrainStrategy {
	return drainAll{}
}

type drainAll struct{}

func (drainAll) NextAction(completed []AsyncCall, pending int) DrainAction {
	return DrainAction{Reevaluate: pending == 0}
}

// AsyncCache is the interface for a user-provided or framework-provided cache backend.
type AsyncCache interface {
	// Get retrieves a value from the cache by its key.
	// Returns the value and true if found, nil and false otherwise.
	Get(key string) (ref.Val, bool)
	// Set stores a value in the cache with the given key.
	Set(key string, val ref.Val)
	// Delete removes a value from the cache by its key.
	Delete(key string)
}

// StaleAsyncCache extends AsyncCache to support retrieving expired entries for fallback or revalidation.
type StaleAsyncCache interface {
	AsyncCache
	// GetStale retrieves a value even if it has expired.
	// Returns (value, isStale, found).
	GetStale(key string) (val ref.Val, isStale bool, found bool)
}

// AsyncCacheOption configures the behavior of CachedAsyncBinding.
type AsyncCacheOption func(*asyncCacheConfig)

type asyncCacheConfig struct {
	cache   AsyncCache
	keyFunc              func(...ref.Val) string
	ttl                  time.Duration
	maxSize              int
	staleWhileError      bool
	staleWhileRevalidate bool
}

// CustomAsyncCache provides an externally managed cache instance.
// If not provided, a default concurrent map-based cache is created.
func CustomAsyncCache(cache AsyncCache) AsyncCacheOption {
	return func(c *asyncCacheConfig) {
		c.cache = cache
	}
}

// AsyncCacheKeyFunc provides a custom function for computing cache keys.
// The default computes a string representation of the arguments.
func AsyncCacheKeyFunc(fn func(...ref.Val) string) AsyncCacheOption {
	return func(c *asyncCacheConfig) {
		c.keyFunc = fn
	}
}

// AsyncCacheTTL sets the time-to-live for cache entries.
// Only applicable for the default cache implementation.
func AsyncCacheTTL(ttl time.Duration) AsyncCacheOption {
	return func(c *asyncCacheConfig) {
		c.ttl = ttl
	}
}

// AsyncCacheSize sets the maximum number of cache entries.
// Only applicable for the default cache implementation.
func AsyncCacheSize(maxSize int) AsyncCacheOption {
	return func(c *asyncCacheConfig) {
		c.maxSize = maxSize
	}
}

// AsyncCacheStaleWhileError enables serving stale cache entries if the underlying function fails.
func AsyncCacheStaleWhileError(enabled bool) AsyncCacheOption {
	return func(c *asyncCacheConfig) {
		c.staleWhileError = enabled
	}
}

// AsyncCacheStaleWhileRevalidate enables serving stale cache entries while refreshing them in the background.
func AsyncCacheStaleWhileRevalidate(enabled bool) AsyncCacheOption {
	return func(c *asyncCacheConfig) {
		c.staleWhileRevalidate = enabled
	}
}

// CachedAsyncBinding wraps a BlockingAsyncOp with a configurable cache.
// It returns an OverloadOpt that can be used with cel.Overload.
func CachedAsyncBinding(fn BlockingAsyncOp, opts ...AsyncCacheOption) OverloadOpt {
	config := &asyncCacheConfig{
		keyFunc: defaultKeyFunc,
	}
	for _, opt := range opts {
		opt(config)
	}

	if config.cache == nil {
		config.cache = newDefaultCache(config.maxSize, config.ttl)
	}

	return AsyncBinding(func(ctx context.Context, args ...ref.Val) ref.Val {
		key := config.keyFunc(args...)

		var staleVal ref.Val
		var isStale bool
		var found bool

		if sc, ok := config.cache.(StaleAsyncCache); ok {
			staleVal, isStale, found = sc.GetStale(key)
		} else {
			staleVal, found = config.cache.Get(key)
			isStale = false // Standard cache Get doesn't indicate staleness
		}

		if found && !isStale {
			return staleVal
		}

		if found && isStale && config.staleWhileRevalidate {
			// Serve stale immediately and refresh in background
			go func() {
				res := fn(ctx, args...)
				if !types.IsError(res) {
					config.cache.Set(key, res)
				}
			}()
			return staleVal
		}

		res := fn(ctx, args...)
		if !types.IsError(res) {
			config.cache.Set(key, res)
			return res
		}

		if found && isStale && config.staleWhileError {
			// Function failed, but we have a stale value to fall back to
			return staleVal
		}

		return res
	})
}

// TimeoutAsyncBinding wraps a BlockingAsyncOp with a per-call timeout.
func TimeoutAsyncBinding(fn BlockingAsyncOp, timeout time.Duration) OverloadOpt {
	return AsyncBinding(func(ctx context.Context, args ...ref.Val) ref.Val {
		tCtx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		return fn(tCtx, args...)
	})
}

// RetryOption configures the behavior of RetryAsyncBinding.
type RetryOption func(*retryConfig)

type retryConfig struct {
	maxAttempts int
	backoff     time.Duration
}

// RetryAttempts sets the maximum number of attempts (including the first one).
func RetryAttempts(attempts int) RetryOption {
	return func(c *retryConfig) {
		c.maxAttempts = attempts
	}
}

// RetryBackoff sets the fixed backoff duration between attempts.
func RetryBackoff(backoff time.Duration) RetryOption {
	return func(c *retryConfig) {
		c.backoff = backoff
	}
}

// RetryableError is an interface that errors can implement to signal whether they are retryable.
type RetryableError interface {
	error
	IsRetryable() bool
}

// RetryAsyncBinding wraps a BlockingAsyncOp with a retry policy.
// It will retry the operation if it returns a types.Err that wraps a RetryableError returning true for IsRetryable.
func RetryAsyncBinding(fn BlockingAsyncOp, opts ...RetryOption) OverloadOpt {
	config := &retryConfig{
		maxAttempts: 3,
		backoff:     100 * time.Millisecond,
	}
	for _, opt := range opts {
		opt(config)
	}

	return AsyncBinding(func(ctx context.Context, args ...ref.Val) ref.Val {
		var lastErr ref.Val
		for i := 0; i < config.maxAttempts; i++ {
			if i > 0 {
				select {
				case <-time.After(config.backoff):
				case <-ctx.Done():
					return types.NewErr("operation cancelled during retry: %v", ctx.Err())
				}
			}

			res := fn(ctx, args...)
			if !types.IsError(res) {
				return res
			}

			err := res.(*types.Err)
			lastErr = res

			if !isRetryable(err) {
				return res
			}
		}
		return lastErr
	})
}

func isRetryable(err *types.Err) bool {
	if err == nil {
		return false
	}
	var re RetryableError
	if errors.As(err, &re) {
		return re.IsRetryable()
	}
	return false
}

// AsyncLimiter provides concurrency control across one or more async functions.
type AsyncLimiter struct {
	sem chan struct{}
}

// NewAsyncLimiter creates a new limiter with the specified maximum concurrency.
func NewAsyncLimiter(maxConcurrency int) *AsyncLimiter {
	return &AsyncLimiter{sem: make(chan struct{}, maxConcurrency)}
}

// Limit wraps a BlockingAsyncOp to enforce the limiter's concurrency limit.
func (l *AsyncLimiter) Limit(fn BlockingAsyncOp) BlockingAsyncOp {
	return func(ctx context.Context, args ...ref.Val) ref.Val {
		select {
		case l.sem <- struct{}{}:
			defer func() { <-l.sem }()
			return fn(ctx, args...)
		case <-ctx.Done():
			return types.NewErr("concurrency limit wait cancelled: %v", ctx.Err())
		}
	}
}

func defaultKeyFunc(args ...ref.Val) string {
	h := fnv.New64a()
	for _, arg := range args {
		// Use the native Go value for hashing.
		// We use a separator to avoid collisions between e.g. ("a", "bc") and ("ab", "c").
		fmt.Fprintf(h, "%v\x00", arg.Value())
	}
	return strconv.FormatUint(h.Sum64(), 16)
}

type defaultCacheEntry struct {
	val       ref.Val
	expiresAt time.Time
}

type defaultCache struct {
	mu      sync.RWMutex
	entries map[string]defaultCacheEntry
	maxSize int
	ttl     time.Duration
}

func newDefaultCache(maxSize int, ttl time.Duration) AsyncCache {
	return &defaultCache{
		entries: make(map[string]defaultCacheEntry),
		maxSize: maxSize,
		ttl:     ttl,
	}
}

func (c *defaultCache) Get(key string) (ref.Val, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	entry, ok := c.entries[key]
	if !ok || (!entry.expiresAt.IsZero() && time.Now().After(entry.expiresAt)) {
		return nil, false
	}
	return entry.val, true
}

func (c *defaultCache) GetStale(key string) (ref.Val, bool, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	entry, ok := c.entries[key]
	if !ok {
		return nil, false, false
	}
	isStale := !entry.expiresAt.IsZero() && time.Now().After(entry.expiresAt)
	return entry.val, isStale, true
}

func (c *defaultCache) Set(key string, val ref.Val) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.maxSize > 0 && len(c.entries) >= c.maxSize {
		// Simple eviction: clear the map if it grows too large.
		// A real LRU would be better but this is a default.
		c.entries = make(map[string]defaultCacheEntry)
	}

	entry := defaultCacheEntry{val: val}
	if c.ttl > 0 {
		entry.expiresAt = time.Now().Add(c.ttl)
	}
	c.entries[key] = entry
}

func (c *defaultCache) Delete(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.entries, key)
}
