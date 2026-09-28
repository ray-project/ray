// Copyright 2025 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package runtime_env

import (
	"fmt"
	"sync"
)

// DefaultMaxURICacheSizeBytes is the default maximum URI cache size (10GB).
const DefaultMaxURICacheSizeBytes = (1024 * 1024 * 1024) * 10

// DeleteFunc deletes the URI and returns the number of bytes deleted.
type DeleteFunc func(uri string) int64

// URICache caches URIs up to a specified total size limit.
//
// URIs are represented by strings and each URI has an associated size on disk.
// When a URI is added to the URICache, it is marked as "in use". When a URI is
// no longer used, the user should call MarkUnused to notify the cache that the
// URI can be safely deleted.
//
// A URI in the cache can be marked as "in use" again by calling MarkUsed.
//
// URIs are only deleted from disk when the total size exceeds the limit. When
// this happens, unused URIs are randomly evicted until the size limit is
// satisfied or there are no more unused URIs.
//
// If all URIs are in use, the total size on disk may exceed the size limit.
type URICache struct {
	// mu protects all shared fields.
	mu sync.RWMutex
	// usedURIs holds the URIs that are currently in use.
	usedURIs map[string]struct{}
	// unusedURIs holds the URIs that are not in use and can be deleted.
	unusedURIs map[string]struct{}
	// deleteFn deletes a URI.
	deleteFn DeleteFunc
	// totalSizeBytes is the total size of all URIs in the cache.
	totalSizeBytes int64
	// maxTotalSizeBytes is the maximum cache size limit.
	maxTotalSizeBytes int64
	// debugMode enables debug checks; used for testing.
	debugMode bool
}

// URICacheOption configures a URICache.
type URICacheOption func(*URICache)

// WithDeleteFn sets the delete function.
func WithDeleteFn(deleteFn DeleteFunc) URICacheOption {
	return func(c *URICache) {
		c.deleteFn = deleteFn
	}
}

// WithMaxTotalSizeBytes sets the maximum cache size.
func WithMaxTotalSizeBytes(maxSize int64) URICacheOption {
	return func(c *URICache) {
		c.maxTotalSizeBytes = maxSize
	}
}

// WithDebugMode sets the debug mode.
func WithDebugMode(debugMode bool) URICacheOption {
	return func(c *URICache) {
		c.debugMode = debugMode
	}
}

// NewURICache creates a new URICache.
func NewURICache(opts ...URICacheOption) *URICache {
	cache := &URICache{
		usedURIs:          make(map[string]struct{}),
		unusedURIs:        make(map[string]struct{}),
		deleteFn:          func(uri string) int64 { return 0 },
		maxTotalSizeBytes: DefaultMaxURICacheSizeBytes,
		debugMode:         false,
	}

	for _, opt := range opts {
		opt(cache)
	}

	return cache
}

// MarkUnused marks a URI as unused, making it eligible for deletion.
func (c *URICache) MarkUnused(uri string) {
	c.mu.Lock()

	if _, exists := c.usedURIs[uri]; !exists {
		logger.Info("URI is already unused", "uri", uri)
	} else {
		c.unusedURIs[uri] = struct{}{}
		delete(c.usedURIs, uri)
	}
	logger.Info("Marked URI unused", "uri", uri)

	// Collect all URIs to evict, then release the lock before running the
	// delete callbacks to reduce lock contention.
	urisToEvict := c.collectURIsToEvictLocked()
	c.checkValidLocked()
	c.mu.Unlock()

	// Run the delete callbacks after releasing the lock to avoid deadlock.
	// Accumulate the number of bytes deleted and update totalSizeBytes once.
	var totalBytesDeleted int64 = 0
	for _, uri := range urisToEvict {
		numBytesDeleted := c.deleteFn(uri)
		totalBytesDeleted += numBytesDeleted
		logger.Info("Deleted URI", "uri", uri, "size", numBytesDeleted)
	}

	// Update the total size in one shot.
	if totalBytesDeleted > 0 {
		c.mu.Lock()
		c.totalSizeBytes -= int64(totalBytesDeleted)
		c.mu.Unlock()
	}
}

// MarkUsed marks a URI as in use; in-use URIs are never deleted.
func (c *URICache) MarkUsed(uri string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.usedURIs[uri]; exists {
		return nil
	}

	if _, exists := c.unusedURIs[uri]; exists {
		c.usedURIs[uri] = struct{}{}
		delete(c.unusedURIs, uri)
	} else {
		return fmt.Errorf("URI %s is not in the cache", uri)
	}

	logger.Info("Marked URI used", "uri", uri)
	c.checkValidLocked()
	return nil
}

// Add adds a URI to the cache and marks it as in use.
func (c *URICache) Add(uri string, sizeBytes int64) {
	c.mu.Lock()

	delete(c.unusedURIs, uri)

	c.usedURIs[uri] = struct{}{}
	c.totalSizeBytes += sizeBytes

	// Collect all URIs to evict, then release the lock before running the
	// delete callbacks to reduce lock contention.
	urisToEvict := c.collectURIsToEvictLocked()
	c.checkValidLocked()
	c.mu.Unlock()

	// Run the delete callbacks after releasing the lock to avoid deadlock.
	// Accumulate the number of bytes deleted and update totalSizeBytes once.
	var totalBytesDeleted int64 = 0
	for _, uri := range urisToEvict {
		numBytesDeleted := c.deleteFn(uri)
		totalBytesDeleted += numBytesDeleted
		logger.Info("Deleted URI", "uri", uri, "size", numBytesDeleted)
	}

	// Update the total size in one shot.
	if totalBytesDeleted > 0 {
		c.mu.Lock()
		c.totalSizeBytes -= int64(totalBytesDeleted)
		c.mu.Unlock()
	}
}

// GetTotalSizeBytes returns the total size of all URIs in the cache.
func (c *URICache) GetTotalSizeBytes() int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.totalSizeBytes
}

// Contains reports whether the URI is in the cache (used or unused).
func (c *URICache) Contains(uri string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	_, inUsed := c.usedURIs[uri]
	_, inUnused := c.unusedURIs[uri]
	return inUsed || inUnused
}

// collectURIsToEvictLocked collects the list of URIs to evict.
// The caller must already hold the lock.
func (c *URICache) collectURIsToEvictLocked() []string {
	var urisToEvict []string

	for len(c.unusedURIs) > 0 && c.totalSizeBytes > c.maxTotalSizeBytes {
		// Pick an arbitrary unused URI (Go map iteration order is random).
		var arbitraryUnusedURI string
		for uri := range c.unusedURIs {
			arbitraryUnusedURI = uri
			break
		}

		delete(c.unusedURIs, arbitraryUnusedURI)
		urisToEvict = append(urisToEvict, arbitraryUnusedURI)
	}

	return urisToEvict
}

// checkValidLocked verifies in debug mode that the used and unused sets are
// disjoint. The caller must already hold the lock.
func (c *URICache) checkValidLocked() {
	if !c.debugMode {
		return
	}

	// Check that the two sets do not intersect.
	for uri := range c.usedURIs {
		if _, exists := c.unusedURIs[uri]; exists {
			panic("URICache invariant violated: used and unused sets have intersection")
		}
	}
}
