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
	"testing"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
)

// mockLogger creates a simple logr.Logger for testing.
func mockLogger() logr.Logger {
	return logr.Discard()
}

func TestNewURICache(t *testing.T) {
	t.Run("create with default values", func(t *testing.T) {
		cache := NewURICache()
		assert.NotNil(t, cache)
		assert.Equal(t, int64(DefaultMaxURICacheSizeBytes), cache.maxTotalSizeBytes)
		assert.False(t, cache.debugMode)
		assert.Equal(t, int64(0), cache.GetTotalSizeBytes())
	})

	t.Run("create with custom max size", func(t *testing.T) {
		cache := NewURICache(WithMaxTotalSizeBytes(1024))
		assert.NotNil(t, cache)
		assert.Equal(t, int64(1024), cache.maxTotalSizeBytes)
	})

	t.Run("create with debug mode", func(t *testing.T) {
		cache := NewURICache(WithDebugMode(true))
		assert.NotNil(t, cache)
		assert.True(t, cache.debugMode)
	})

	t.Run("create with delete function", func(t *testing.T) {
		called := false
		deleteFn := func(uri string) int64 {
			called = true
			return 100
		}
		cache := NewURICache(WithDeleteFn(deleteFn))
		assert.NotNil(t, cache)

		// Add a URI and mark it unused; since the limit is not exceeded, no
		// deletion is triggered.
		cache.Add("test://uri1", 100)
		cache.MarkUnused("test://uri1")
		// deleteFn is not called because the cache limit is not exceeded.
		// To trigger deleteFn, set a smaller maxTotalSizeBytes.
		assert.False(t, called)
	})

	t.Run("create with multiple options", func(t *testing.T) {
		cache := NewURICache(
			WithMaxTotalSizeBytes(2048),
			WithDebugMode(true),
		)
		assert.NotNil(t, cache)
		assert.Equal(t, int64(2048), cache.maxTotalSizeBytes)
		assert.True(t, cache.debugMode)
	})
}

func TestURICache_Add(t *testing.T) {
	t.Run("add single URI", func(t *testing.T) {
		cache := NewURICache()
		cache.Add("test://uri1", 100)

		assert.True(t, cache.Contains("test://uri1"))
		assert.Equal(t, int64(100), cache.GetTotalSizeBytes())
	})

	t.Run("add multiple URIs", func(t *testing.T) {
		cache := NewURICache()
		cache.Add("test://uri1", 100)
		cache.Add("test://uri2", 200)
		cache.Add("test://uri3", 300)

		assert.True(t, cache.Contains("test://uri1"))
		assert.True(t, cache.Contains("test://uri2"))
		assert.True(t, cache.Contains("test://uri3"))
		assert.Equal(t, int64(600), cache.GetTotalSizeBytes())
	})

	t.Run("add same URI twice accumulates size", func(t *testing.T) {
		cache := NewURICache()
		cache.Add("test://uri1", 100)
		cache.Add("test://uri1", 100)

		assert.True(t, cache.Contains("test://uri1"))
		// Adding the same URI twice accumulates the size.
		assert.Equal(t, int64(200), cache.GetTotalSizeBytes())
	})

	t.Run("add URI exceeds max size triggers eviction", func(t *testing.T) {
		var deletedURIs []string
		deleteFn := func(uri string) int64 {
			deletedURIs = append(deletedURIs, uri)
			return 100
		}

		cache := NewURICache(
			WithMaxTotalSizeBytes(150),
			WithDeleteFn(deleteFn),
		)

		// Add two unused URIs.
		cache.Add("test://uri1", 100)
		cache.MarkUnused("test://uri1")

		cache.Add("test://uri2", 100)
		cache.MarkUnused("test://uri2")

		// The total size is now 200, which exceeds the limit of 150, so
		// eviction should be triggered.

		// Adding a third URI should trigger eviction.
		cache.Add("test://uri3", 100)

		// At least one URI is deleted.
		assert.GreaterOrEqual(t, len(deletedURIs), 1)
	})

	t.Run("add URI moves from unused to used", func(t *testing.T) {
		cache := NewURICache()
		cache.Add("test://uri1", 100)
		cache.MarkUnused("test://uri1")

		// Adding again moves the URI from unused to used.
		cache.Add("test://uri1", 100)

		assert.True(t, cache.Contains("test://uri1"))
	})
}

func TestURICache_MarkUnused(t *testing.T) {
	t.Run("mark used URI as unused", func(t *testing.T) {
		cache := NewURICache()
		cache.Add("test://uri1", 100)

		cache.MarkUnused("test://uri1")

		assert.True(t, cache.Contains("test://uri1"))
	})

	t.Run("mark already unused URI logs info", func(t *testing.T) {
		cache := NewURICache()
		cache.Add("test://uri1", 100)
		cache.MarkUnused("test://uri1")

		// Marking again as unused should not error.
		cache.MarkUnused("test://uri1")
	})

	t.Run("mark non-existent URI as unused logs info", func(t *testing.T) {
		cache := NewURICache()
		// Marking a non-existent URI should not error.
		cache.MarkUnused("test://nonexistent")
	})

	t.Run("mark unused triggers eviction when over limit", func(t *testing.T) {
		var deletedURIs []string
		var totalDeleted int64 = 0
		deleteFn := func(uri string) int64 {
			deletedURIs = append(deletedURIs, uri)
			size := int64(100)
			totalDeleted += size
			return size
		}

		cache := NewURICache(
			WithMaxTotalSizeBytes(150),
			WithDeleteFn(deleteFn),
		)

		// Add three URIs.
		cache.Add("test://uri1", 100)
		cache.Add("test://uri2", 100)
		cache.Add("test://uri3", 100)

		// The total size is 300 > 150; marking one as unused should trigger
		// eviction.
		cache.MarkUnused("test://uri1")

		// Some URI should have been deleted.
		assert.GreaterOrEqual(t, len(deletedURIs), 1)
		// Verify that totalSizeBytes is updated correctly.
		assert.Equal(t, int64(300-totalDeleted), cache.GetTotalSizeBytes())
	})
}

func TestURICache_MarkUsed(t *testing.T) {
	t.Run("mark unused URI as used", func(t *testing.T) {
		cache := NewURICache()
		cache.Add("test://uri1", 100)
		cache.MarkUnused("test://uri1")

		err := cache.MarkUsed("test://uri1")

		assert.NoError(t, err)
		assert.True(t, cache.Contains("test://uri1"))
	})

	t.Run("mark already used URI as used returns nil", func(t *testing.T) {
		cache := NewURICache()
		cache.Add("test://uri1", 100)

		err := cache.MarkUsed("test://uri1")

		assert.NoError(t, err)
	})

	t.Run("mark non-existent URI as used returns error", func(t *testing.T) {
		cache := NewURICache()

		err := cache.MarkUsed("test://nonexistent")

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "is not in the cache")
	})
}

func TestURICache_Contains(t *testing.T) {
	t.Run("contains returns true for used URI", func(t *testing.T) {
		cache := NewURICache()
		cache.Add("test://uri1", 100)

		assert.True(t, cache.Contains("test://uri1"))
	})

	t.Run("contains returns true for unused URI", func(t *testing.T) {
		cache := NewURICache()
		cache.Add("test://uri1", 100)
		cache.MarkUnused("test://uri1")

		assert.True(t, cache.Contains("test://uri1"))
	})

	t.Run("contains returns false for non-existent URI", func(t *testing.T) {
		cache := NewURICache()

		assert.False(t, cache.Contains("test://nonexistent"))
	})

	t.Run("contains returns false after eviction", func(t *testing.T) {
		var deletedURIs []string
		deleteFn := func(uri string) int64 {
			deletedURIs = append(deletedURIs, uri)
			return 100
		}

		cache := NewURICache(
			WithMaxTotalSizeBytes(50),
			WithDeleteFn(deleteFn),
		)

		cache.Add("test://uri1", 100)
		cache.MarkUnused("test://uri1")

		// Wait for MarkUnused to complete the eviction.
		// Since maxTotalSizeBytes=50 and the added URI size of 100 already
		// exceeds the limit, uri1 should be evicted.
		assert.False(t, cache.Contains("test://uri1"))
	})
}

func TestURICache_GetTotalSizeBytes(t *testing.T) {
	t.Run("get total size of empty cache", func(t *testing.T) {
		cache := NewURICache()
		assert.Equal(t, int64(0), cache.GetTotalSizeBytes())
	})

	t.Run("get total size after adding URIs", func(t *testing.T) {
		cache := NewURICache()
		cache.Add("test://uri1", 100)
		cache.Add("test://uri2", 200)

		assert.Equal(t, int64(300), cache.GetTotalSizeBytes())
	})

	t.Run("get total size is thread-safe", func(t *testing.T) {
		cache := NewURICache()

		done := make(chan bool)
		go func() {
			for i := 0; i < 100; i++ {
				cache.GetTotalSizeBytes()
			}
			done <- true
		}()

		go func() {
			for i := 0; i < 100; i++ {
				cache.Add("test://uri", 1)
			}
			done <- true
		}()

		<-done
		<-done
		// If there is no deadlock or race condition causing a panic, the test
		// passes.
	})
}

func TestURICache_Eviction(t *testing.T) {
	t.Run("evict unused URIs when over limit", func(t *testing.T) {
		var deletedURIs []string
		deleteFn := func(uri string) int64 {
			deletedURIs = append(deletedURIs, uri)
			return 100
		}

		cache := NewURICache(
			WithMaxTotalSizeBytes(150),
			WithDeleteFn(deleteFn),
		)

		// Add 3 URIs, each 100 bytes.
		cache.Add("test://uri1", 100)
		cache.MarkUnused("test://uri1")

		cache.Add("test://uri2", 100)
		cache.MarkUnused("test://uri2")

		cache.Add("test://uri3", 100)
		cache.MarkUnused("test://uri3")

		// Total size is 300 > 150, so at least 2 URIs should be evicted.
		assert.GreaterOrEqual(t, len(deletedURIs), 2)
	})

	t.Run("do not evict used URIs", func(t *testing.T) {
		var deletedURIs []string
		deleteFn := func(uri string) int64 {
			deletedURIs = append(deletedURIs, uri)
			return 100
		}

		cache := NewURICache(
			WithMaxTotalSizeBytes(150),
			WithDeleteFn(deleteFn),
		)

		// Add an in-use URI.
		cache.Add("test://uri1", 100)
		// Do not mark it unused; keep it in use.

		// Add another URI.
		cache.Add("test://uri2", 100)
		cache.MarkUnused("test://uri2")

		// Only uri2 may be evicted; uri1 is not evicted because it is in use.
		assert.Contains(t, deletedURIs, "test://uri2")
	})

	t.Run("stop eviction when under limit", func(t *testing.T) {
		var deletedURIs []string
		deleteFn := func(uri string) int64 {
			deletedURIs = append(deletedURIs, uri)
			return 50
		}

		cache := NewURICache(
			WithMaxTotalSizeBytes(100),
			WithDeleteFn(deleteFn),
		)

		// Add 3 URIs, each 50 bytes.
		cache.Add("test://uri1", 50)
		cache.MarkUnused("test://uri1")

		cache.Add("test://uri2", 50)
		cache.MarkUnused("test://uri2")

		// Total size is 100 = limit, so nothing should be evicted.
		assert.Equal(t, 0, len(deletedURIs))

		// Add another URI; total size becomes 150 > 100, so 1 should be
		// evicted.
		cache.Add("test://uri3", 50)
		cache.MarkUnused("test://uri3")

		assert.GreaterOrEqual(t, len(deletedURIs), 1)
	})
}

func TestURICache_DebugMode(t *testing.T) {
	t.Run("debug mode checks for intersection", func(t *testing.T) {
		cache := NewURICache(WithDebugMode(true))

		// Normal operations should not trigger a panic.
		cache.Add("test://uri1", 100)
		cache.MarkUnused("test://uri1")

		// Manually create an intersection (direct internal field access),
		// which should trigger a panic.
		cache.mu.Lock()
		cache.usedURIs["test://uri1"] = struct{}{}
		assert.Panics(t, func() {
			cache.checkValidLocked()
		})
		cache.mu.Unlock()
	})

	t.Run("non-debug mode does not check intersection", func(t *testing.T) {
		cache := NewURICache(WithDebugMode(false))

		// In non-debug mode, checkValidLocked should return immediately.
		cache.mu.Lock()
		cache.usedURIs["test://uri1"] = struct{}{}
		cache.unusedURIs["test://uri1"] = struct{}{}
		cache.checkValidLocked() // Should not panic.
		cache.mu.Unlock()
	})
}

func TestURICache_ConcurrentAccess(t *testing.T) {
	t.Run("concurrent add and mark unused", func(t *testing.T) {
		cache := NewURICache(WithMaxTotalSizeBytes(10000))

		done := make(chan bool)

		go func() {
			for i := 0; i < 50; i++ {
				cache.Add("test://uri", 100)
			}
			done <- true
		}()

		go func() {
			for i := 0; i < 50; i++ {
				cache.MarkUnused("test://uri")
			}
			done <- true
		}()

		<-done
		<-done
		// If there is no deadlock or race condition causing a panic, the test
		// passes.
	})

	t.Run("concurrent mark used and mark unused", func(t *testing.T) {
		cache := NewURICache(WithMaxTotalSizeBytes(10000))
		cache.Add("test://uri1", 100)
		cache.Add("test://uri2", 100)

		done := make(chan bool)

		go func() {
			for i := 0; i < 50; i++ {
				cache.MarkUsed("test://uri1")
				cache.MarkUnused("test://uri1")
			}
			done <- true
		}()

		go func() {
			for i := 0; i < 50; i++ {
				cache.MarkUsed("test://uri2")
				cache.MarkUnused("test://uri2")
			}
			done <- true
		}()

		<-done
		<-done
		// If there is no deadlock or race condition causing a panic, the test
		// passes.
	})
}

func TestURICache_DeleteFunction(t *testing.T) {
	t.Run("delete function is called with correct URI", func(t *testing.T) {
		var calledURIs []string
		deleteFn := func(uri string) int64 {
			calledURIs = append(calledURIs, uri)
			return 100
		}

		cache := NewURICache(
			WithMaxTotalSizeBytes(50),
			WithDeleteFn(deleteFn),
		)

		cache.Add("test://uri1", 100)
		cache.MarkUnused("test://uri1")

		assert.Contains(t, calledURIs, "test://uri1")
	})

	t.Run("delete function return value is used to update total size", func(t *testing.T) {
		deleteFn := func(uri string) int64 {
			return 200 // Return a delete size different from the actual one.
		}

		cache := NewURICache(
			WithMaxTotalSizeBytes(50),
			WithDeleteFn(deleteFn),
		)

		cache.Add("test://uri1", 100)
		cache.MarkUnused("test://uri1")

		// totalSizeBytes should be reduced by the value returned by deleteFn.
		// Initial 100 - deleted 200 = -100.
		assert.Equal(t, int64(-100), cache.GetTotalSizeBytes())
	})

	t.Run("default delete function returns 0", func(t *testing.T) {
		cache := NewURICache(WithMaxTotalSizeBytes(50))

		cache.Add("test://uri1", 100)
		cache.MarkUnused("test://uri1")

		// The default deleteFn returns 0, so totalSizeBytes is unchanged.
		assert.Equal(t, int64(100), cache.GetTotalSizeBytes())
	})
}

func TestDefaultMaxURICacheSizeBytes(t *testing.T) {
	t.Run("default max size is 10GB", func(t *testing.T) {
		expectedSize := int64((1024 * 1024 * 1024) * 10)
		assert.Equal(t, expectedSize, int64(DefaultMaxURICacheSizeBytes))
	})
}

func TestURICache_EdgeCases(t *testing.T) {
	t.Run("add URI with zero size", func(t *testing.T) {
		cache := NewURICache()
		cache.Add("test://uri1", 0)

		assert.True(t, cache.Contains("test://uri1"))
		assert.Equal(t, int64(0), cache.GetTotalSizeBytes())
	})

	t.Run("add URI with negative size", func(t *testing.T) {
		cache := NewURICache()
		cache.Add("test://uri1", -100)

		assert.True(t, cache.Contains("test://uri1"))
		assert.Equal(t, int64(-100), cache.GetTotalSizeBytes())
	})

	t.Run("max size zero allows no URIs", func(t *testing.T) {
		var deletedURIs []string
		deleteFn := func(uri string) int64 {
			deletedURIs = append(deletedURIs, uri)
			return 100
		}

		cache := NewURICache(
			WithMaxTotalSizeBytes(0),
			WithDeleteFn(deleteFn),
		)

		cache.Add("test://uri1", 100)
		cache.MarkUnused("test://uri1")

		// A URI of any size should be evicted immediately.
		assert.GreaterOrEqual(t, len(deletedURIs), 1)
	})

	t.Run("empty URI string", func(t *testing.T) {
		cache := NewURICache()
		cache.Add("", 100)

		assert.True(t, cache.Contains(""))
	})
}
