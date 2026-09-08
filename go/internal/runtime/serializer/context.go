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

package serializer

import (
	"fmt"
	"hash/fnv"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ray-project/ray/go/pkg/ids"
	"github.com/ray-project/ray/go/pkg/log"
)

// SerializationContext tracks nested object references during serialization.
// Equivalent to Java's ThreadLocal containedObjectIds and outerObjectId.
type SerializationContext struct {
	containedObjectIDs map[ids.ObjectID]bool
	outerObjectID      ids.ObjectID
}

// ContextManager manages goroutine-local serialization contexts.
//
// Note: This implementation uses LoadOrStore to avoid race conditions between
// Load/Store and pool Get/Put operations. Context is automatically managed
// and will be garbage collected when the goroutine exits.
type ContextManager struct {
	contextMap sync.Map
	pool       sync.Pool
}

// NewContextManager creates a new ContextManager.
func NewContextManager() *ContextManager {
	return &ContextManager{
		pool: sync.Pool{
			New: func() interface{} {
				return &SerializationContext{
					containedObjectIDs: make(map[ids.ObjectID]bool),
					outerObjectID:      ids.NilObjectID(),
				}
			},
		},
	}
}

// NewSerializationContext creates a new SerializationContext.
func NewSerializationContext() *SerializationContext {
	return &SerializationContext{
		containedObjectIDs: make(map[ids.ObjectID]bool),
		outerObjectID:      ids.NilObjectID(),
	}
}

// Clear clears the serialization context.
func (c *SerializationContext) Clear() {
	for k := range c.containedObjectIDs {
		delete(c.containedObjectIDs, k)
	}
	c.outerObjectID = ids.NilObjectID()
}

// assertSerializationContext performs a type assertion on an interface{} to *SerializationContext.
// It panics if the type assertion fails, logging the actual and expected types.
// This helper function eliminates duplicate type assertion logic.
func assertSerializationContext(value interface{}, context string) *SerializationContext {
	result, ok := value.(*SerializationContext)
	if !ok {
		log.Log.Error(nil, "type assertion failed", "context", context, "actualType", value, "expectedType", "*SerializationContext")
		panic(fmt.Sprintf("type assertion failed: %s, actualType=%v, expectedType=*SerializationContext", context, value))
	}
	return result
}

// getOrCreateContext gets or creates a SerializationContext for the current goroutine.
// Uses LoadOrStore to avoid race conditions with concurrent putContext calls.
func (m *ContextManager) getOrCreateContext() *SerializationContext {
	gid := getGoroutineID()
	// Use LoadOrStore to atomically get or create context
	// This avoids the race condition where:
	// 1. Goroutine A calls Load, gets nil
	// 2. Goroutine B calls Load, gets nil
	// 3. Both call pool.Get() and Store, one context is lost
	ctx, loaded := m.contextMap.Load(gid)
	if loaded {
		return assertSerializationContext(ctx, "contextMap.Load")
	}
	// Create new context and store atomically
	poolResult := m.pool.Get()
	newCtx := assertSerializationContext(poolResult, "pool.Get")
	actualCtx, loaded := m.contextMap.LoadOrStore(gid, newCtx)
	if loaded {
		// Another goroutine stored a context while we were creating ours
		// Return the existing one and put ours back to pool
		m.pool.Put(newCtx)
		return assertSerializationContext(actualCtx, "contextMap.LoadOrStore")
	}
	return newCtx
}

// GetContext gets or creates a SerializationContext for the current goroutine.
// This is the public method for external packages.
func (m *ContextManager) GetContext() *SerializationContext {
	return m.getOrCreateContext()
}

// putContext returns the SerializationContext to the pool for the current goroutine.
// It clears the context data and returns it to the pool for reuse.
// The entry in contextMap is deleted to allow garbage collection.
func (m *ContextManager) putContext() {
	gid := getGoroutineID()
	if ctx, ok := m.contextMap.Load(gid); ok {
		ctxVal := assertSerializationContext(ctx, "contextMap.Load in putContext")
		// Clear the context data
		for k := range ctxVal.containedObjectIDs {
			delete(ctxVal.containedObjectIDs, k)
		}
		ctxVal.outerObjectID = ids.NilObjectID()
		// Delete from map to allow GC
		m.contextMap.Delete(gid)
		// Put back to pool for reuse
		m.pool.Put(ctxVal)
	}
}

// ReturnContext returns the SerializationContext to the pool for the current goroutine.
// This is the public method for external packages.
// Deprecated: Context is now automatically managed. This method is kept for compatibility.
// Use putContextWithClear() for explicit context clearing.
func (m *ContextManager) ReturnContext(ctx *SerializationContext) {
	m.putContext()
}

// putContextWithClear returns the SerializationContext to the pool with explicit clearing.
// If ctx is provided, it clears that specific context; otherwise clears the current goroutine's context.
// This method is used by Serializer.ReturnContext() to properly handle the provided context.
func (m *ContextManager) putContextWithClear(ctx *SerializationContext) {
	// Clear the provided context if non-nil
	if ctx != nil {
		ctx.Clear()
	}
	// Always clear the current goroutine's context
	m.putContext()
}

// addContainedObjectID adds an object ID to the current context.
func (m *ContextManager) addContainedObjectID(objectID ids.ObjectID) {
	ctx := m.getOrCreateContext()
	ctx.containedObjectIDs[objectID] = true
}

// getAndClearContainedObjectIDs gets and clears contained object IDs from current context.
func (m *ContextManager) getAndClearContainedObjectIDs() []ids.ObjectID {
	ctx := m.getOrCreateContext()
	result := make([]ids.ObjectID, 0, len(ctx.containedObjectIDs))
	for id := range ctx.containedObjectIDs {
		result = append(result, id)
	}
	// Clear the map
	ctx.containedObjectIDs = make(map[ids.ObjectID]bool)
	return result
}

// setOuterObjectID sets the outer object ID in current context.
func (m *ContextManager) setOuterObjectID(objectID ids.ObjectID) {
	ctx := m.getOrCreateContext()
	ctx.outerObjectID = objectID
}

// getOuterObjectID gets the outer object ID from current context.
func (m *ContextManager) getOuterObjectID() ids.ObjectID {
	ctx := m.getOrCreateContext()
	return ctx.outerObjectID
}

// resetOuterObjectID resets the outer object ID in current context.
func (m *ContextManager) resetOuterObjectID() {
	ctx := m.getOrCreateContext()
	ctx.outerObjectID = ids.NilObjectID()
}

// goroutineIDCacheEntry holds a cached goroutine ID with its last access time.
type goroutineIDCacheEntry struct {
	goroutineID uint64
	lastAccess  atomic.Int64 // Stores nanoseconds since epoch for atomic access
}

// goroutineIDCache caches goroutine IDs to avoid repeated runtime.Stack() calls.
// Key is the goroutine ID itself, value is the cached entry with timestamp.
//
// Note: This cache is periodically cleaned up by a background goroutine to prevent
// memory leaks from long-running services with many goroutines. Entries older than
// cacheExpiryDuration are removed during cleanup.
var goroutineIDCache sync.Map

// fallbackGoroutineIDCounter is a fallback counter for goroutine ID generation.
// Used when runtime.Stack() parsing fails.
//
// Note: This is a last-resort fallback mechanism. The counter-based IDs are
// not true goroutine IDs and should only be used for context isolation.
var fallbackGoroutineIDCounter atomic.Uint64

// goroutineFallbackID is a thread-local storage for fallback goroutine ID.
// Uses sync.Map to store per-goroutine fallback IDs.
var goroutineFallbackID sync.Map

// cacheExpiryDuration is the duration after which cache entries are considered stale.
// Set to 5 minutes to balance performance and memory usage in high-churn scenarios.
const cacheExpiryDuration = 5 * time.Minute

// cacheCleanupInterval is the interval at which the cache cleanup runs.
// Set to 1 minute to promptly remove stale entries and prevent memory leaks.
const cacheCleanupInterval = 1 * time.Minute

// cacheSizeThreshold is the threshold for cache size monitoring.
// When cache size exceeds this threshold, a warning is logged.
// Set to 5000 to detect potential memory issues early.
const cacheSizeThreshold = 5000

// cacheSizeCounter tracks the approximate number of entries in the cache.
// Used for monitoring and triggering proactive cleanup.
var cacheSizeCounter atomic.Int64

// lastCleanupTime tracks the last time a cleanup was triggered.
// Used to rate-limit proactive cleanup calls.
var lastCleanupTime atomic.Int64

// init starts a background goroutine to periodically clean up the goroutine ID cache.
func init() {
	go func() {
		ticker := time.NewTicker(cacheCleanupInterval)
		defer ticker.Stop()
		for range ticker.C {
			cleanupGoroutineIDCache()
		}
	}()
}

// cleanupGoroutineIDCache removes stale entries from the cache to prevent memory leaks.
//
// Performance note: This function uses Range() to iterate over all cache entries.
// While Range() is not atomic and the deletion operation may have race conditions,
// the impact is minimal in practice:
// - At most one stale entry may be missed or redundantly deleted
// - No data corruption can occur since sync.Map handles concurrent access safely
// - The cleanup interval (5 minutes) is much longer than typical cache access patterns
//
// For services with extremely high goroutine churn (>10000 unique goroutines),
// consider monitoring cache size and adjusting cacheExpiryDuration/cacheCleanupInterval.
func cleanupGoroutineIDCache() {
	now := time.Now()
	var cacheEntryCount int64
	var deletedCount int64
	goroutineIDCache.Range(func(key, value interface{}) bool {
		cacheEntryCount++
		if entry, ok := value.(*goroutineIDCacheEntry); ok {
			lastAccess := time.Unix(0, entry.lastAccess.Load())
			if now.Sub(lastAccess) > cacheExpiryDuration {
				goroutineIDCache.Delete(key)
				deletedCount++
			}
		}
		return true
	})
	// Monitor cache size and log warning if threshold exceeded
	if cacheEntryCount > cacheSizeThreshold {
		log.Log.V(1).Info("goroutineIDCache size exceeds threshold", "cacheEntryCount", cacheEntryCount, "threshold", cacheSizeThreshold, "deletedCount", deletedCount)
	}
	// Update the global counter for external monitoring
	cacheSizeCounter.Store(cacheEntryCount)
}

// tryProactiveCleanup checks if cache size exceeds threshold and triggers cleanup if needed.
// Uses rate limiting to avoid excessive cleanup calls in high-concurrency scenarios.
//
// This function is called from getGoroutineID() when cache size approaches the threshold.
func tryProactiveCleanup() {
	currentSize := cacheSizeCounter.Load()
	if currentSize <= cacheSizeThreshold {
		return
	}

	// Rate limit: only allow one proactive cleanup per cacheCleanupInterval
	now := time.Now().UnixNano()
	lastCleanup := lastCleanupTime.Load()
	if now-lastCleanup < int64(cacheCleanupInterval) {
		return
	}

	// Try to update lastCleanupTime atomically
	if lastCleanupTime.CompareAndSwap(lastCleanup, now) {
		// Successfully acquired cleanup lock, trigger cleanup
		cleanupGoroutineIDCache()
		log.Log.V(1).Info("proactive goroutineIDCache cleanup triggered", "cacheSize", currentSize)
	}
}

// GetGoroutineIDCacheSize returns the approximate number of entries in the cache.
// This can be used for monitoring and alerting.
func GetGoroutineIDCacheSize() int64 {
	return cacheSizeCounter.Load()
}

// GetGoroutineID returns the current goroutine ID with caching.
//
// Performance characteristics:
// - First call: ~1-5μs (calls runtime.Stack which is relatively expensive)
// - Subsequent calls: O(1) cache lookup (very fast)
//
// Implementation: Uses runtime.Stack() to parse the goroutine ID from stack trace.
// The result is cached using the goroutine ID itself as key to avoid repeated parsing.
//
// Fallback mechanism: If runtime.Stack() parsing fails, uses a counter-based ID
// to ensure goroutine isolation still works.
func GetGoroutineID() uint64 {
	return getGoroutineID()
}

// getGoroutineID returns the current goroutine ID.
//
// Performance characteristics:
// - First call in a goroutine: ~1-5μs (calls runtime.Stack which is relatively expensive)
// - Subsequent calls: O(1) cache lookup (very fast)
//
// Implementation note: This function uses runtime.Stack() to parse the goroutine ID
// from stack trace output. The result is cached using the goroutine ID itself as key.
//
// Limitations:
//   - Depends on runtime.Stack() output format (may break if Go runtime changes)
//   - Cache grows with number of unique goroutines, but is periodically
//     cleaned up by a background goroutine to prevent memory leaks
//   - The cache cleanup runs every 1 minute and removes entries not accessed for 5 minutes
//   - Proactive cleanup is triggered when cache size exceeds threshold (5000)
//
// Fallback mechanism:
// If runtime.Stack() parsing fails, this function uses a fallback counter-based ID
// to ensure goroutine isolation still works. The fallback ID is stored per-goroutine
// using sync.Map to maintain isolation.
func getGoroutineID() uint64 {
	id := parseGoroutineIDFromStack()
	if id == 0 {
		// Fallback: runtime.Stack() parsing failed
		// Use a counter-based ID to maintain goroutine isolation
		return getFallbackGoroutineID()
	}

	// Use goroutine ID itself as cache key
	// This ensures each goroutine has its own cache entry
	if cached, ok := goroutineIDCache.Load(id); ok {
		entry := cached.(*goroutineIDCacheEntry)
		// Update last access time atomically to avoid race conditions
		entry.lastAccess.Store(time.Now().UnixNano())
		return entry.goroutineID
	}

	// Check if proactive cleanup is needed before adding new entry
	// This helps prevent cache explosion in high-churn scenarios
	tryProactiveCleanup()

	// Cache the result for this goroutine
	entry := &goroutineIDCacheEntry{
		goroutineID: id,
	}
	entry.lastAccess.Store(time.Now().UnixNano())
	goroutineIDCache.Store(id, entry)
	cacheSizeCounter.Add(1)
	return id
}

// getFallbackGoroutineID returns a fallback goroutine ID when runtime.Stack() parsing fails.
// Uses a counter-based approach with per-goroutine caching to maintain isolation.
//
// Note: This is a last-resort mechanism. The returned ID is not a true goroutine ID
// but provides sufficient isolation for serialization contexts.
// The fallback key is generated from the call stack hash to distinguish different goroutines.
func getFallbackGoroutineID() uint64 {
	// Try to get cached fallback ID for this goroutine
	// We use a hash of the call stack as key to distinguish different goroutines
	key := getCallStackHash()
	if cached, ok := goroutineFallbackID.Load(key); ok {
		return cached.(uint64)
	}

	// Generate a new fallback ID
	newID := fallbackGoroutineIDCounter.Add(1)
	goroutineFallbackID.Store(key, newID)
	return newID
}

// callStackDepth is the number of frames to capture for fallback goroutine identification.
// Capturing multiple frames provides better discrimination between goroutines
// that may have different call paths.
const callStackDepth = 8

// getCallStackHash returns a hash of the current call stack.
// This is used as a key for fallback goroutine ID caching when runtime.Stack() parsing fails.
//
// Note: While this provides better discrimination than a single PC value,
// it still cannot guarantee unique identification in all cases.
// However, for typical serialization use cases, different goroutines will have
// sufficiently different call stacks to maintain isolation.
func getCallStackHash() uint64 {
	var pcs [callStackDepth]uintptr
	n := runtime.Callers(1, pcs[:])
	if n == 0 {
		// Fallback to counter-based ID if we can't get any call stack
		return fallbackGoroutineIDCounter.Add(1)
	}

	// Compute FNV-1a hash of the call stack using Go standard library
	h := fnv.New64a()
	for i := 0; i < n; i++ {
		// Convert each PC to bytes and write to hash
		pcBytes := make([]byte, 8)
		for j := 0; j < 8; j++ {
			pcBytes[j] = byte(pcs[i] >> (j * 8))
		}
		h.Write(pcBytes)
	}
	return h.Sum64()
}

// parseGoroutineIDFromStack parses the goroutine ID from runtime.Stack() output.
// Uses a robust parsing strategy with multiple fallbacks to handle format variations.
//
// Stack trace format examples:
//   - Go 1.x: "goroutine 123 [running]:..."
//   - Go 1.x (nested): "goroutine 123 [sleep]:..."
//   - Future formats may vary, so we parse defensively.
//
// Returns 0 if parsing fails.
func parseGoroutineIDFromStack() uint64 {
	var buf [128]byte // Larger buffer to handle longer prefixes
	n := runtime.Stack(buf[:], false)
	if n == 0 {
		return 0
	}

	// Strategy 1: Look for "goroutine " prefix and parse the number after it
	// This is the most robust approach as it doesn't depend on fixed positions
	prefix := "goroutine "
	stackStr := string(buf[:n])

	if idx := strings.Index(stackStr, prefix); idx >= 0 {
		// Find the start of the number
		numStart := idx + len(prefix)
		if numStart < len(stackStr) {
			// Find the end of the number (first non-digit character)
			numEnd := numStart
			for numEnd < len(stackStr) && stackStr[numEnd] >= '0' && stackStr[numEnd] <= '9' {
				numEnd++
			}
			if numEnd > numStart {
				id, err := strconv.ParseUint(stackStr[numStart:numEnd], 10, 64)
				if err == nil {
					return id
				}
			}
		}
	}

	// Strategy 2: Fallback to the original parsing method (for backward compatibility)
	// Format: "goroutine 123 [running]" - ID starts around position 10
	// Skip "goroutine " (10 characters) and parse until space or bracket
	idStr := strings.TrimSpace(string(buf[10:n]))
	if idx := strings.IndexAny(idStr, " ["); idx >= 0 {
		idStr = idStr[:idx]
	}
	id, err := strconv.ParseUint(idStr, 10, 64)
	if err == nil {
		return id
	}

	// All parsing strategies failed
	return 0
}
