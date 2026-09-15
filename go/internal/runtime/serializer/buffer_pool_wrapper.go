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
	"github.com/ray-project/ray/go/pkg/runtime/object"
)

// BufferPoolWrapper wraps the default TieredBufferPool to implement the
// object.BufferPool interface.
//
// This adapter allows the serializer package to provide buffer pool services
// to the object package while maintaining the correct dependency direction:
// serializer -> object (not the other way around).
//
// The wrapper delegates all operations to the defaultPool (TieredBufferPool),
// which is the high-performance tiered buffer pool implementation in this package.
type BufferPoolWrapper struct{}

// Compile-time interface compliance check to ensure BufferPoolWrapper
// implements the object.BufferPool interface.
var _ object.BufferPool = (*BufferPoolWrapper)(nil)

// Get implements object.BufferPool.Get by delegating to the default tiered buffer pool.
//
// Parameters:
//   - size: the minimum capacity required for the buffer
//
// Returns:
//   - []byte: a buffer with length 0 and capacity >= size
//
// The returned buffer must be returned to the pool via Put() when no longer needed.
func (w *BufferPoolWrapper) Get(size int) []byte {
	return defaultPool.Get(size)
}

// Put implements object.BufferPool.Put by delegating to the default tiered buffer pool.
//
// Parameters:
//   - buf: the buffer to return to the pool
//
// IMPORTANT: After calling Put(), the buffer must NOT be used anymore.
// Using a buffer after calling Put() can lead to data corruption and race conditions.
func (w *BufferPoolWrapper) Put(buf []byte) {
	defaultPool.Put(buf)
}
