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
	"testing"
)

// TestBufferPool_Creation tests buffer pool creation.
func TestBufferPool_Creation(t *testing.T) {
	pool := NewBufferPool(1024)
	if pool == nil {
		t.Fatal("NewBufferPool() returned nil")
	}
	if pool.capacity != 1024 {
		t.Errorf("NewBufferPool() capacity = %d, want 1024", pool.capacity)
	}
}

// TestBufferPool_GetPut tests getting and putting buffers.
func TestBufferPool_GetPut(t *testing.T) {
	pool := NewBufferPool(1024)

	// Get a buffer
	buf := pool.Get()
	if buf == nil {
		t.Fatal("Get() returned nil")
	}
	if cap(buf) != 1024 {
		t.Errorf("Get() buffer capacity = %d, want 1024", cap(buf))
	}
	if len(buf) != 0 {
		t.Errorf("Get() buffer length = %d, want 0", len(buf))
	}

	// Use the buffer
	buf = append(buf, []byte("test data")...)

	// Return the buffer
	pool.Put(buf)
}

// TestBufferPool_PutNil tests returning nil buffer.
func TestBufferPool_PutNil(t *testing.T) {
	pool := NewBufferPool(1024)
	// Should not panic
	pool.Put(nil)
}

// TestBufferPool_ConcurrentAccess tests concurrent access to buffer pool.
func TestBufferPool_ConcurrentAccess(t *testing.T) {
	pool := NewBufferPool(1024)
	done := make(chan bool, 10)

	for i := 0; i < 10; i++ {
		go func() {
			defer func() { done <- true }()
			for j := 0; j < 100; j++ {
				buf := pool.Get()
				buf = append(buf, []byte("test")...)
				pool.Put(buf)
			}
		}()
	}

	for i := 0; i < 10; i++ {
		<-done
	}
}

// TestSelectBufferPool tests selecting appropriate pool based on size.
// This test verifies the internal getPool method works correctly.
// Note: The new TieredBufferPool uses 2^n size classes (64B, 128B, 256B, 512B, 1KB, ... 1MB).
func TestSelectBufferPool(t *testing.T) {
	tests := []struct {
		size           int
		expectedMinCap int
	}{
		{500, 512},                 // 500 -> 512B (2^9)
		{1024, 1024},               // 1024 -> 1KB (2^10)
		{50 * 1024, 65536},         // 51200 -> 64KB (2^16)
		{100 * 1024, 131072},       // 102400 -> 128KB (2^17)
		{300 * 1024, 524288},       // 307200 -> 512KB (2^19)
		{512 * 1024, 524288},       // 524288 -> 512KB (2^19)
		{2 * 1024 * 1024, 1048576}, // 2MB -> 1MB (max size class)
	}

	for _, tt := range tests {
		buf := GetBuffer(tt.size)
		if cap(buf) < tt.expectedMinCap {
			t.Errorf("GetBuffer(%d) should return buffer with cap >= %d, got %d", tt.size, tt.expectedMinCap, cap(buf))
		}
		PutBuffer(buf)
	}
}

// TestGlobalPoolFunctions tests global pool functions.
func TestGlobalPoolFunctions(t *testing.T) {
	// Test GetBuffer/PutBuffer with different sizes
	// Small size (500 bytes) - should get 512B buffer (2^9)
	buf := GetBuffer(500)
	if cap(buf) < 512 {
		t.Errorf("GetBuffer(500) should return buffer with cap >= 512, got %d", cap(buf))
	}
	PutBuffer(buf)

	// Medium size (50KB) - should get 64KB buffer (2^16)
	buf = GetBuffer(50 * 1024)
	if cap(buf) < 65536 {
		t.Errorf("GetBuffer(50KB) should return buffer with cap >= 65536, got %d", cap(buf))
	}
	PutBuffer(buf)

	// Large size (2MB) - should get 1MB buffer (max size class)
	buf = GetBuffer(2 * 1024 * 1024)
	if cap(buf) < 1048576 {
		t.Errorf("GetBuffer(2MB) should return buffer with cap >= 1048576, got %d", cap(buf))
	}
	PutBuffer(buf)
}

// BenchmarkBufferPool_GetPut benchmarks buffer pool get/put operations.
func BenchmarkBufferPool_GetPut(b *testing.B) {
	pool := NewBufferPool(1024)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf := pool.Get()
		buf = append(buf, []byte("test data")...)
		pool.Put(buf)
	}
}

// BenchmarkBufferPool_Concurrent benchmarks concurrent buffer pool access.
func BenchmarkBufferPool_Concurrent(b *testing.B) {
	pool := NewBufferPool(1024)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			buf := pool.Get()
			buf = append(buf, []byte("test")...)
			pool.Put(buf)
		}
	})
}
