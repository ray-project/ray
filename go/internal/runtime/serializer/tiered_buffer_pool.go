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
	"math/bits"
	"reflect"
	"sync"
	"sync/atomic"
	"unsafe"
)

// Buffer alignment for cache efficiency (matches C++ implementation).
// 64-byte alignment ensures buffer starts at cache line boundary.
const BufferAlignment = 64

// Size class constants for tiered buffer pool.
// Fine-grained size classes from 64 bytes to 1MB (powers of 2).
const (
	MinBufferSize     = 64          // 64B - minimum buffer size
	MaxBufferSize     = 1024 * 1024 // 1MB - maximum pooled buffer size
	SizeClassCount    = 15          // Number of size classes
	MaxSizeClassIndex = SizeClassCount - 1
)

// Default buffer configuration.
const (
	DefaultInitialBufferSize = 4096 // 4KB - default initial buffer size for most objects
)

// sizeClasses defines buffer size classes (powers of 2).
var sizeClasses = [SizeClassCount]int{
	64,      // 0: 64B
	128,     // 1: 128B
	256,     // 2: 256B
	512,     // 3: 512B
	1024,    // 4: 1KB
	2048,    // 5: 2KB
	4096,    // 6: 4KB
	8192,    // 7: 8KB
	16384,   // 8: 16KB
	32768,   // 9: 32KB
	65536,   // 10: 64KB
	131072,  // 11: 128KB
	262144,  // 12: 256KB
	524288,  // 13: 512KB
	1048576, // 14: 1MB
}

// minCapacityPercent is the minimum capacity percentage (80%) for buffer recycling.
const minCapacityPercent = 80

// Default configuration values
const (
	DefaultPreAllocCount = 2                // Reduced from 4 for better memory efficiency
	DefaultMaxBuffers    = 10000            // Maximum number of active buffers (backpressure)
	DefaultMaxMemory     = 64 * 1024 * 1024 // 64MB - maximum memory usage
)

// PoolStats contains comprehensive statistics about buffer pool usage.
type PoolStats struct {
	// Operation counts per size class
	Gets []uint64 // Number of Get operations per size class
	Puts []uint64 // Number of Put operations per size class

	// Current state
	ActiveCount int64 // Current number of active (checked out) buffers
	ActiveBytes int64 // Current memory usage of active buffers

	// Performance metrics
	AllocFailures uint64 // Number of allocation failures (backpressure triggered)
	Hits          uint64 // Number of successful pool hits (approximate)
	Misses        uint64 // Number of pool misses (new allocations, approximate)
}

// allocateAligned allocates a buffer with specified size, aligned to BufferAlignment.
func allocateAligned(size int) []byte {
	// Allocate extra space for alignment
	extra := BufferAlignment + size
	buf := make([]byte, extra)
	// Align the buffer
	aligned := alignBuffer(buf)
	// Return slice with exact size
	return aligned[:size:size]
}

// alignBuffer aligns a byte slice to BufferAlignment boundary.
// The input buffer must have length >= BufferAlignment to guarantee success.
func alignBuffer(buf []byte) []byte {
	if len(buf) == 0 {
		return buf
	}
	// Get the address of the first element
	ptr := uintptr(unsafe.Pointer(&buf[0]))
	// Calculate alignment offset
	offset := (BufferAlignment - (ptr % BufferAlignment)) % BufferAlignment
	// Safety check: ensure we have enough space
	if int(offset) >= len(buf) {
		// This should not happen if allocateAligned is used correctly
		// Return original buffer rather than panicking
		return buf
	}
	// Return aligned slice
	return buf[offset:]
}

// globalPool is the global buffer pool with tiered size classes.
type globalPool struct {
	pools [SizeClassCount]*sync.Pool

	// Statistics (atomic counters) - tracks Get/Put operations per size class
	gets [SizeClassCount]uint64
	puts [SizeClassCount]uint64

	// Pre-allocated buffers per size class
	preAllocCount int
}

// newGlobalPool creates a new global pool with pre-allocation.
func newGlobalPool(preAllocCount int) *globalPool {
	gp := &globalPool{
		preAllocCount: preAllocCount,
	}

	for i := range gp.pools {
		size := sizeClasses[i]
		gp.pools[i] = &sync.Pool{
			New: func() interface{} {
				return allocateAligned(size)
			},
		}

		// Pre-allocate buffers (reduced count for better memory efficiency)
		for j := 0; j < preAllocCount; j++ {
			buf := allocateAligned(size)
			gp.pools[i].Put(buf)
		}
	}

	return gp
}

// get gets a buffer from global pool.
func (gp *globalPool) get(sizeClass int) []byte {
	atomic.AddUint64(&gp.gets[sizeClass], 1)
	buf := gp.pools[sizeClass].Get().([]byte)
	// Reset length but retain capacity
	return buf[:0]
}

// put returns a buffer to global pool.
func (gp *globalPool) put(sizeClass int, buf []byte) {
	// Reset length but retain capacity
	buf = buf[:0]
	atomic.AddUint64(&gp.puts[sizeClass], 1)
	gp.pools[sizeClass].Put(buf)
}

// TieredBufferPool implements a high-performance buffer pool with:
// - Tiered size classes (64B to 1MB, powers of 2)
// - 64-byte cache-line alignment for better memory access
// - Pre-allocation for hot paths
// - Backpressure mechanism to prevent memory explosion
// - Comprehensive statistics tracking
//
// Design principles from Java and C++ implementations:
// - Java: ThreadLocal-like per-goroutine caching (via sync.Pool)
// - C++: Fine-grained size classes, memory alignment, pre-allocation
type TieredBufferPool struct {
	global *globalPool

	// Backpressure configuration
	maxBuffers int64 // Maximum number of active buffers
	maxMemory  int64 // Maximum memory usage in bytes

	// Current state tracking (atomic for thread safety)
	currentCount  atomic.Int64 // Current number of active buffers
	currentMemory atomic.Int64 // Current memory usage of active buffers

	// Statistics
	allocFailures atomic.Uint64 // Allocation failures due to backpressure
	hits          atomic.Uint64 // Approximate pool hits
	misses        atomic.Uint64 // Approximate pool misses
}

// TieredBufferPoolConfig holds configuration for creating a TieredBufferPool.
type TieredBufferPoolConfig struct {
	// PreAllocCount specifies the number of pre-allocated buffers per size class.
	// Default: DefaultPreAllocCount (2)
	PreAllocCount int

	// MaxBuffers specifies the maximum number of active buffers before backpressure kicks in.
	// When reached or exceeded, Get() will fall back to direct allocation.
	// Default: DefaultMaxBuffers (10000)
	MaxBuffers int64

	// MaxMemory specifies the maximum memory usage in bytes before backpressure kicks in.
	// When reached or exceeded, Get() will fall back to direct allocation.
	// Default: DefaultMaxMemory (64MB)
	MaxMemory int64
}

// DefaultConfig returns the default configuration.
func DefaultConfig() TieredBufferPoolConfig {
	return TieredBufferPoolConfig{
		PreAllocCount: DefaultPreAllocCount,
		MaxBuffers:    DefaultMaxBuffers,
		MaxMemory:     DefaultMaxMemory,
	}
}

// NewTieredBufferPool creates a new tiered buffer pool with default configuration.
// For custom configuration, use NewTieredBufferPoolWithConfig.
func NewTieredBufferPool(preAllocCount int) *TieredBufferPool {
	config := DefaultConfig()
	if preAllocCount > 0 {
		config.PreAllocCount = preAllocCount
	}
	return NewTieredBufferPoolWithConfig(config)
}

// NewTieredBufferPoolWithConfig creates a new tiered buffer pool with custom configuration.
func NewTieredBufferPoolWithConfig(config TieredBufferPoolConfig) *TieredBufferPool {
	// Apply defaults for zero values
	if config.PreAllocCount <= 0 {
		config.PreAllocCount = DefaultPreAllocCount
	}
	if config.MaxBuffers <= 0 {
		config.MaxBuffers = DefaultMaxBuffers
	}
	if config.MaxMemory <= 0 {
		config.MaxMemory = DefaultMaxMemory
	}

	pool := &TieredBufferPool{
		global:     newGlobalPool(config.PreAllocCount),
		maxBuffers: config.MaxBuffers,
		maxMemory:  config.MaxMemory,
	}

	return pool
}

// sizeClassIndex returns the index of the size class for a given size.
// Returns the smallest size class that can hold the requested size.
// Size classes: 64B, 128B, 256B, 512B, 1KB, 2KB, 4KB, 8KB, 16KB, 32KB, 64KB, 128KB, 256KB, 512KB, 1MB
func sizeClassIndex(size int) int {
	// Handle edge cases
	if size <= 0 {
		return 0 // Return minimum size class for invalid sizes
	}
	if size <= MinBufferSize {
		return 0 // 64B
	}
	if size > MaxBufferSize {
		return MaxSizeClassIndex // 1MB
	}

	// Find the smallest power of 2 that can hold the size
	// bits.Len32(n) returns the position of the most significant bit
	// For size=65 (needs 128B): bits.Len32(64) = 7, index = 7-6 = 1 (128B)
	// For size=128 (needs 128B): bits.Len32(127) = 7, index = 7-6 = 1 (128B)
	// For size=129 (needs 256B): bits.Len32(128) = 8, index = 8-6 = 2 (256B)
	idx := bits.Len32(uint32(size - 1))

	// Adjust for our size class array (starts at 64 = 2^6)
	// idx ranges from 6 (for size 64) to 20 (for size 1MB)
	return idx - 6
}

// checkBackpressure checks if backpressure should be applied.
// Returns true if backpressure is triggered (pool limits reached or exceeded).
func (p *TieredBufferPool) checkBackpressure(size int) bool {
	currentCount := p.currentCount.Load()
	currentMemory := p.currentMemory.Load()

	// Check buffer count limit
	if currentCount >= p.maxBuffers {
		return true
	}

	// Check memory limit
	if currentMemory >= p.maxMemory {
		return true
	}

	// Check if this allocation would exceed memory limit
	size64 := int64(size)
	if currentMemory+size64 > p.maxMemory {
		return true
	}

	return false
}

// Get gets a buffer with at least the requested capacity.
// The returned buffer has length 0 and capacity >= requested size.
// Buffer is aligned to 64-byte boundary for optimal memory access.
//
// Backpressure: When the pool reaches or exceeds maxBuffers or maxMemory limits,
// a new buffer is allocated directly (not from the pool) to prevent
// memory explosion. The caller is still responsible for calling Put().
//
// IMPORTANT: Call Put() when done to return the buffer to the pool.
func (p *TieredBufferPool) Get(size int) []byte {
	idx := sizeClassIndex(size)
	expectedSize := sizeClasses[idx]

	// Check backpressure before getting from pool
	if p.checkBackpressure(expectedSize) {
		// Backpressure triggered - allocate directly
		p.allocFailures.Add(1)
		p.misses.Add(1)
		buf := allocateAligned(expectedSize)
		// Track active buffers even for direct allocations
		p.currentCount.Add(1)
		p.currentMemory.Add(int64(cap(buf)))
		return buf[:0]
	}

	// Get from pool
	p.currentCount.Add(1)
	p.currentMemory.Add(int64(expectedSize))
	buf := p.global.get(idx)

	// Track as miss if capacity doesn't match (sync.Pool gave us a new allocation)
	// This is approximate since sync.Pool doesn't expose hit/miss info
	p.misses.Add(1)

	return buf
}

// Put returns a buffer to the pool.
//
// IMPORTANT: After calling Put(), the buffer must NOT be used anymore.
// Using a buffer after calling Put() can lead to data corruption and race conditions.
//
// The buffer is returned to the appropriate size class based on its capacity.
// Buffers with capacity < 80% of target capacity are not recycled to avoid memory waste.
func (p *TieredBufferPool) Put(buf []byte) {
	if buf == nil {
		return
	}

	cap := cap(buf)
	idx := sizeClassIndex(cap)
	expectedCap := sizeClasses[idx]

	// Only recycle if capacity is at least 80% of expected
	if cap < expectedCap*minCapacityPercent/100 {
		// Don't recycle - just update stats
		p.currentCount.Add(-1)
		p.currentMemory.Add(-int64(cap))
		return
	}

	// Update active count before returning to pool
	p.currentCount.Add(-1)
	p.currentMemory.Add(-int64(expectedCap))

	p.global.put(idx, buf)
}

// GetStats returns comprehensive pool statistics.
// This is a snapshot of the current state.
func (p *TieredBufferPool) GetStats() PoolStats {
	stats := PoolStats{
		Gets:          make([]uint64, SizeClassCount),
		Puts:          make([]uint64, SizeClassCount),
		ActiveCount:   p.currentCount.Load(),
		ActiveBytes:   p.currentMemory.Load(),
		AllocFailures: p.allocFailures.Load(),
		Hits:          p.hits.Load(),
		Misses:        p.misses.Load(),
	}

	for i := range p.global.gets {
		stats.Gets[i] = atomic.LoadUint64(&p.global.gets[i])
		stats.Puts[i] = atomic.LoadUint64(&p.global.puts[i])
	}

	return stats
}

// GetMemoryUsage returns current memory usage statistics.
// Returns (activeBuffers, activeBytes, maxBuffers, maxBytes).
func (p *TieredBufferPool) GetMemoryUsage() (activeBuffers, activeBytes, maxBuffers, maxBytes int64) {
	return p.currentCount.Load(), p.currentMemory.Load(), p.maxBuffers, p.maxMemory
}

// ResetStats resets all statistics counters.
func (p *TieredBufferPool) ResetStats() {
	for i := range p.global.gets {
		atomic.StoreUint64(&p.global.gets[i], 0)
		atomic.StoreUint64(&p.global.puts[i], 0)
	}
	p.allocFailures.Store(0)
	p.hits.Store(0)
	p.misses.Store(0)
}

// EstimateBufferSize estimates the buffer size needed to serialize an object.
// This is a heuristic-based estimation to optimize initial buffer allocation.
//
// The estimation is based on:
// - Object type (primitive, slice, map, struct)
// - Object size (length of slices/maps)
// - MsgPack encoding overhead (approximately 20-30% for complex objects)
//
// Returns DefaultInitialBufferSize (4KB) for most objects, which provides
// a good balance between memory efficiency and avoiding frequent reallocations.
func EstimateBufferSize(obj interface{}) int {
	if obj == nil {
		return 64 // Minimal size for nil
	}

	switch v := obj.(type) {
	case []byte:
		// Add 10% overhead for msgpack encoding
		return len(v) * 11 / 10
	case string:
		// Add 20% overhead for string encoding
		return len(v) * 12 / 10
	case []interface{}:
		// Estimate: 8 bytes per element + 20% overhead
		estimated := len(v) * 8 * 12 / 10
		if estimated < DefaultInitialBufferSize {
			return DefaultInitialBufferSize
		}
		return estimated
	case map[string]interface{}:
		// Estimate: 16 bytes per entry + 20% overhead
		estimated := len(v) * 16 * 12 / 10
		if estimated < DefaultInitialBufferSize {
			return DefaultInitialBufferSize
		}
		return estimated
	case map[interface{}]interface{}:
		// Estimate: 20 bytes per entry + 20% overhead
		estimated := len(v) * 20 * 12 / 10
		if estimated < DefaultInitialBufferSize {
			return DefaultInitialBufferSize
		}
		return estimated
	default:
		// For structs and other types, use reflection for rough estimation
		val := reflect.ValueOf(obj)
		switch val.Kind() {
		case reflect.Slice, reflect.Array:
			// Estimate based on element count and type size
			elemSize := val.Type().Elem().Size()
			estimated := int(val.Len()) * int(elemSize) * 12 / 10
			if estimated < DefaultInitialBufferSize {
				return DefaultInitialBufferSize
			}
			return estimated
		case reflect.Map:
			// Estimate based on map length
			estimated := int(val.Len()) * 20 * 12 / 10
			if estimated < DefaultInitialBufferSize {
				return DefaultInitialBufferSize
			}
			return estimated
		default:
			// Default size for primitives and small structs
			return DefaultInitialBufferSize
		}
	}
}

// Default global tiered buffer pool instance.
var defaultPool = NewTieredBufferPool(4)

// GetBuffer gets a buffer from the default pool based on estimated size.
// This is the main API for obtaining buffers from the pool.
//
// Parameters:
//   - estimatedSize: The estimated size of the buffer needed.
//
// Returns:
//   - A byte slice with length 0 and pre-allocated capacity.
//     The buffer is aligned to 64-byte boundary.
//
// IMPORTANT: Call PutBuffer() when done to return the buffer to the pool.
func GetBuffer(estimatedSize int) []byte {
	return defaultPool.Get(estimatedSize)
}

// PutBuffer returns a buffer to the default pool.
//
// Parameters:
//   - buf: The buffer to return. Must have been obtained from GetBuffer().
//     If nil, this method is a no-op.
//
// IMPORTANT: After calling PutBuffer(), the buffer must NOT be used anymore.
func PutBuffer(buf []byte) {
	defaultPool.Put(buf)
}

// GetPoolStats returns statistics from the default pool.
// Deprecated: Use defaultPool.GetStats() for more comprehensive stats.
func GetPoolStats() (gets, puts []uint64) {
	stats := defaultPool.GetStats()
	return stats.Gets, stats.Puts
}

// GetDefaultPoolStats returns comprehensive statistics from the default pool.
func GetDefaultPoolStats() PoolStats {
	return defaultPool.GetStats()
}

// ============================================================================
// Backward Compatibility Layer
// ============================================================================
// The following types and functions are provided for backward compatibility
// with existing code that uses the old BufferPool API. New code should use
// TieredBufferPool directly.

// BufferPool is a legacy buffer pool interface for backward compatibility.
// It wraps TieredBufferPool to provide the old API.
// Deprecated: Use TieredBufferPool for better performance.
type BufferPool struct {
	tiered   *TieredBufferPool
	capacity int
}

// NewBufferPool creates a legacy buffer pool with specified initial capacity.
// For better performance, use NewTieredBufferPool instead.
// Deprecated: Use NewTieredBufferPool for better performance.
func NewBufferPool(initialCapacity int) *BufferPool {
	// Create a tiered pool and use it as backing store
	return &BufferPool{
		tiered:   defaultPool,
		capacity: initialCapacity,
	}
}

// Get gets a buffer from the pool.
// The returned buffer has length 0 but with pre-allocated capacity.
// Deprecated: Use TieredBufferPool.Get() instead.
func (p *BufferPool) Get() []byte {
	return p.tiered.Get(p.capacity)
}

// Put returns a buffer to the pool.
// Deprecated: Use TieredBufferPool.Put() instead.
func (p *BufferPool) Put(buf []byte) {
	p.tiered.Put(buf)
}
