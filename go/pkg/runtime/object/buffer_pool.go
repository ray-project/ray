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

package object

// BufferPool defines the interface for buffer pool management.
// This interface allows different buffer pool implementations to be used
// interchangeably, enabling future performance optimizations.
//
// The interface is designed to support dependency injection, allowing
// the object package to use buffer pools without depending on specific
// implementations.
type BufferPool interface {
	// Get retrieves a buffer from the pool with at least the requested capacity.
	// The returned buffer has length 0 and capacity >= size.
	// The caller is responsible for calling Put() when done.
	//
	// Parameters:
	//   - size: the minimum capacity required for the buffer
	//
	// Returns:
	//   - []byte: a buffer with length 0 and capacity >= size
	//
	// IMPORTANT: The returned buffer must be returned to the pool via Put()
	// when no longer needed. Failure to do so will cause memory leaks.
	Get(size int) []byte

	// Put returns a buffer to the pool.
	// After calling Put(), the buffer must NOT be used anymore.
	//
	// Parameters:
	//   - buf: the buffer to return to the pool
	//
	// IMPORTANT: Using a buffer after calling Put() can lead to data
	// corruption and race conditions, as the buffer may be reused by
	// other goroutines.
	Put(buf []byte)
}

// defaultBufferPool is the global buffer pool instance.
// It is set during runtime initialization via SetDefaultBufferPool().
// If not set, GetBuffer() and PutBuffer() will use fallback behavior.
var defaultBufferPool BufferPool

// SetDefaultBufferPool sets the global buffer pool implementation.
// This function should be called during runtime initialization.
// It allows injecting different buffer pool implementations (e.g., for testing
// or performance optimization).
//
// Parameters:
//   - pool: the buffer pool implementation to use globally
//
// Thread Safety: This function is not thread-safe and should only be called
// once during initialization before any concurrent access to GetBuffer/PutBuffer.
func SetDefaultBufferPool(pool BufferPool) {
	defaultBufferPool = pool
}

// GetDefaultBufferPool returns the current global buffer pool.
// Returns nil if no buffer pool has been set.
//
// Returns:
//   - BufferPool: the current global buffer pool, or nil if not set
func GetDefaultBufferPool() BufferPool {
	return defaultBufferPool
}

// GetBuffer retrieves a buffer from the global pool.
// If no pool is set, falls back to direct allocation (make([]byte, 0, size)).
//
// Parameters:
//   - size: the minimum capacity required for the buffer
//
// Returns:
//   - []byte: a buffer with length 0 and capacity >= size
//
// IMPORTANT: The returned buffer must be returned to the pool via PutBuffer()
// when no longer needed. Failure to do so will cause memory leaks.
//
// Thread Safety: This function is thread-safe and can be called concurrently.
func GetBuffer(size int) []byte {
	if defaultBufferPool != nil {
		return defaultBufferPool.Get(size)
	}
	// Fallback: direct allocation (not pooled)
	// This ensures the function works even if SetDefaultBufferPool() was not called.
	return make([]byte, 0, size)
}

// PutBuffer returns a buffer to the global pool.
// If no pool is set, does nothing (GC will handle the buffer).
//
// Parameters:
//   - buf: the buffer to return to the pool
//
// IMPORTANT: After calling PutBuffer(), the buffer must NOT be used anymore.
// Using a buffer after calling PutBuffer() can lead to data corruption and
// race conditions.
//
// Thread Safety: This function is thread-safe and can be called concurrently.
func PutBuffer(buf []byte) {
	if defaultBufferPool != nil {
		defaultBufferPool.Put(buf)
	}
	// If no pool is set, just let GC handle the buffer
	// This ensures the function works even if SetDefaultBufferPool() was not called.
}
