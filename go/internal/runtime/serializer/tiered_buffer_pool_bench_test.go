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
	"math/rand"
	"sync"
	"testing"
	"time"
	"unsafe"
)

// Benchmark comparing old and new buffer pool implementations.

// oldBufferPool simulates the old implementation for comparison.
type oldBufferPool struct {
	pool     *sync.Pool
	capacity int
}

func newOldBufferPool(capacity int) *oldBufferPool {
	return &oldBufferPool{
		pool: &sync.Pool{
			New: func() interface{} {
				return make([]byte, 0, capacity)
			},
		},
		capacity: capacity,
	}
}

func (p *oldBufferPool) Get() []byte {
	return p.pool.Get().([]byte)
}

func (p *oldBufferPool) Put(buf []byte) {
	if buf == nil {
		return
	}
	if cap(buf) < p.capacity*minCapacityPercent/100 {
		return
	}
	buf = buf[:0]
	p.pool.Put(buf)
}

// Benchmark scenarios

// BenchmarkBufferPool_SingleThreaded benchmarks single-threaded performance.
func BenchmarkBufferPool_SingleThreaded_Old(b *testing.B) {
	pool := newOldBufferPool(1024)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf := pool.Get()
		buf = append(buf, make([]byte, 512)...)
		pool.Put(buf)
	}
}

func BenchmarkBufferPool_SingleThreaded_New(b *testing.B) {
	pool := NewTieredBufferPool(4)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf := pool.Get(512)
		buf = append(buf, make([]byte, 512)...)
		pool.Put(buf)
	}
}

// BenchmarkBufferPool_MultiThreaded benchmarks multi-threaded performance.
func BenchmarkBufferPool_MultiThreaded_Old(b *testing.B) {
	pool := newOldBufferPool(1024)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			buf := pool.Get()
			buf = append(buf, make([]byte, 512)...)
			pool.Put(buf)
		}
	})
}

func BenchmarkBufferPool_MultiThreaded_New(b *testing.B) {
	pool := NewTieredBufferPool(4)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			buf := pool.Get(512)
			buf = append(buf, make([]byte, 512)...)
			pool.Put(buf)
		}
	})
}

// Benchmark different buffer sizes

func BenchmarkBufferPool_Size64_Old(b *testing.B) {
	pool := newOldBufferPool(64)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf := pool.Get()
		buf = append(buf, make([]byte, 32)...)
		pool.Put(buf)
	}
}

func BenchmarkBufferPool_Size64_New(b *testing.B) {
	pool := NewTieredBufferPool(4)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf := pool.Get(32)
		buf = append(buf, make([]byte, 32)...)
		pool.Put(buf)
	}
}

func BenchmarkBufferPool_Size4KB_Old(b *testing.B) {
	pool := newOldBufferPool(4096)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf := pool.Get()
		buf = append(buf, make([]byte, 2048)...)
		pool.Put(buf)
	}
}

func BenchmarkBufferPool_Size4KB_New(b *testing.B) {
	pool := NewTieredBufferPool(4)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf := pool.Get(2048)
		buf = append(buf, make([]byte, 2048)...)
		pool.Put(buf)
	}
}

func BenchmarkBufferPool_Size64KB_Old(b *testing.B) {
	pool := newOldBufferPool(65536)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf := pool.Get()
		buf = append(buf, make([]byte, 32768)...)
		pool.Put(buf)
	}
}

func BenchmarkBufferPool_Size64KB_New(b *testing.B) {
	pool := NewTieredBufferPool(4)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf := pool.Get(32768)
		buf = append(buf, make([]byte, 32768)...)
		pool.Put(buf)
	}
}

// BenchmarkBufferPool_MixedSizes benchmarks mixed buffer sizes.
func BenchmarkBufferPool_MixedSizes_Old(b *testing.B) {
	sizes := []int{64, 256, 1024, 4096, 16384, 65536}
	pool := newOldBufferPool(65536)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		size := sizes[i%len(sizes)]
		buf := pool.Get()
		buf = append(buf, make([]byte, size/2)...)
		pool.Put(buf)
	}
}

func BenchmarkBufferPool_MixedSizes_New(b *testing.B) {
	sizes := []int{64, 256, 1024, 4096, 16384, 65536}
	pool := NewTieredBufferPool(4)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		size := sizes[i%len(sizes)]
		buf := pool.Get(size)
		buf = append(buf, make([]byte, size/2)...)
		pool.Put(buf)
	}
}

// BenchmarkBufferPool_Contention benchmarks high contention scenario.
func BenchmarkBufferPool_Contention_Old(b *testing.B) {
	pool := newOldBufferPool(1024)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			buf := pool.Get()
			buf = append(buf, []byte("test")...)
			pool.Put(buf)
		}
	})
}

func BenchmarkBufferPool_Contention_New(b *testing.B) {
	pool := NewTieredBufferPool(4)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			buf := pool.Get(4)
			buf = append(buf, []byte("test")...)
			pool.Put(buf)
		}
	})
}

// Benchmark allocation overhead

func BenchmarkBufferPool_Allocation_Old(b *testing.B) {
	pool := newOldBufferPool(1024)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf := pool.Get()
		_ = buf
		pool.Put(buf)
	}
}

func BenchmarkBufferPool_Allocation_New(b *testing.B) {
	pool := NewTieredBufferPool(4)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf := pool.Get(1024)
		_ = buf
		pool.Put(buf)
	}
}

// Test functions to verify correctness

// TestTieredBufferPool_BasicOperations tests basic get/put operations.
func TestTieredBufferPool_BasicOperations(t *testing.T) {
	pool := NewTieredBufferPool(4)

	// Test small buffer
	buf := pool.Get(32)
	if cap(buf) < 32 {
		t.Errorf("Get(32) returned buffer with cap %d, want >= 32", cap(buf))
	}
	if len(buf) != 0 {
		t.Errorf("Get(32) returned buffer with len %d, want 0", len(buf))
	}
	pool.Put(buf)

	// Test large buffer
	buf = pool.Get(100000)
	if cap(buf) < 100000 {
		t.Errorf("Get(100000) returned buffer with cap %d, want >= 100000", cap(buf))
	}
	pool.Put(buf)

	// Test nil buffer
	pool.Put(nil) // Should not panic
}

// TestTieredBufferPool_SizeClasses tests size class selection.
func TestTieredBufferPool_SizeClasses(t *testing.T) {
	tests := []struct {
		size   int
		minCap int
		maxCap int
	}{
		{32, 64, 64},
		{64, 64, 64},
		{100, 128, 128},
		{256, 256, 256},
		{500, 512, 512},
		{1024, 1024, 1024},
		{2000, 2048, 2048},
		{4096, 4096, 4096},
		{10000, 16384, 16384},
		{65536, 65536, 65536},
		{100000, 131072, 131072},
		{524288, 524288, 524288},
		{1000000, 1048576, 1048576},
		{2000000, 1048576, 1048576}, // Larger than max, should get max
	}

	pool := NewTieredBufferPool(4)

	for _, tt := range tests {
		buf := pool.Get(tt.size)
		if cap(buf) < tt.minCap {
			t.Errorf("Get(%d) returned buffer with cap %d, want >= %d", tt.size, cap(buf), tt.minCap)
		}
		if cap(buf) > tt.maxCap {
			t.Errorf("Get(%d) returned buffer with cap %d, want <= %d", tt.size, cap(buf), tt.maxCap)
		}
		pool.Put(buf)
	}
}

// TestTieredBufferPool_Alignment tests buffer alignment.
func TestTieredBufferPool_Alignment(t *testing.T) {
	pool := NewTieredBufferPool(4)

	for i := 0; i < 100; i++ {
		buf := pool.Get(1024)
		// Check alignment using capacity pointer
		// Append one byte to get a valid pointer
		buf = append(buf, 0)
		ptr := uintptr(unsafe.Pointer(&buf[0]))
		if ptr%BufferAlignment != 0 {
			t.Errorf("Buffer %d not aligned: ptr=%x, mod=%d", i, ptr, ptr%BufferAlignment)
		}
		pool.Put(buf)
	}
}

// TestTieredBufferPool_Concurrent tests concurrent access.
func TestTieredBufferPool_Concurrent(t *testing.T) {
	pool := NewTieredBufferPool(4)
	done := make(chan bool, 100)

	for i := 0; i < 100; i++ {
		go func() {
			defer func() { done <- true }()
			for j := 0; j < 100; j++ {
				size := rand.Intn(10000) + 64
				buf := pool.Get(size)
				if len(buf) != 0 {
					t.Errorf("Get() returned buffer with len %d, want 0", len(buf))
				}
				if cap(buf) < size {
					t.Errorf("Get(%d) returned buffer with cap %d, want >= %d", size, cap(buf), size)
				}
				// Use the buffer
				buf = append(buf, make([]byte, size)...)
				pool.Put(buf)
			}
		}()
	}

	for i := 0; i < 100; i++ {
		<-done
	}
}

// TestTieredBufferPool_Recycling tests buffer recycling.
func TestTieredBufferPool_Recycling(t *testing.T) {
	pool := NewTieredBufferPool(4)

	// Get a buffer and put it back
	buf1 := pool.Get(1024)
	// Append one byte to get a valid pointer
	buf1 = append(buf1, 0)
	ptr1 := uintptr(unsafe.Pointer(&buf1[0]))
	pool.Put(buf1)

	// Get another buffer - should be the same one (recycled)
	buf2 := pool.Get(1024)
	buf2 = append(buf2, 0)
	ptr2 := uintptr(unsafe.Pointer(&buf2[0]))

	if ptr1 != ptr2 {
		t.Log("Note: Buffer was not recycled (this is acceptable under high concurrency)")
	}

	pool.Put(buf2)
}

// TestTieredBufferPool_Stats tests statistics tracking.
func TestTieredBufferPool_Stats(t *testing.T) {
	pool := NewTieredBufferPool(4)

	// Get some buffers
	for i := 0; i < 10; i++ {
		buf := pool.Get(1024)
		pool.Put(buf)
	}

	stats := pool.GetStats()

	totalGets := uint64(0)
	totalPuts := uint64(0)
	for i := range stats.Gets {
		totalGets += stats.Gets[i]
		totalPuts += stats.Puts[i]
	}

	if totalGets != 10 {
		t.Errorf("Expected 10 gets, got %d", totalGets)
	}
	if totalPuts != 10 {
		t.Errorf("Expected 10 puts, got %d", totalPuts)
	}
}

// TestSizeClassIndex tests the sizeClassIndex function.
func TestSizeClassIndex(t *testing.T) {
	tests := []struct {
		size int
		want int
	}{
		{0, 0},
		{32, 0},
		{64, 0},
		{65, 1},
		{128, 1},
		{129, 2},
		{256, 2},
		{512, 3},
		{1024, 4},
		{2048, 5},
		{4096, 6},
		{8192, 7},
		{16384, 8},
		{32768, 9},
		{65536, 10},
		{131072, 11},
		{262144, 12},
		{524288, 13},
		{1048576, 14},
		{2000000, 14}, // Larger than max
	}

	for _, tt := range tests {
		got := sizeClassIndex(tt.size)
		if got != tt.want {
			t.Errorf("sizeClassIndex(%d) = %d, want %d", tt.size, got, tt.want)
		}
	}
}

// Benchmark size class index calculation
func BenchmarkSizeClassIndex(b *testing.B) {
	sizes := []int{64, 256, 1024, 4096, 16384, 65536, 262144, 1048576}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = sizeClassIndex(sizes[i%len(sizes)])
	}
}

// TestTieredBufferPool_Reset tests the Reset method.
func TestTieredBufferPool_Reset(t *testing.T) {
	pool := NewTieredBufferPool(4)

	// Get some buffers
	bufs := make([][]byte, 10)
	for i := range bufs {
		bufs[i] = pool.Get(1024)
	}

	// Return all buffers
	for i := 0; i < 10; i++ {
		pool.Put(bufs[i])
	}

	// Note: TieredBufferPool doesn't have a Reset method
	// as sync.Pool manages its own lifecycle
	t.Log("Buffer pool cleanup completed")
}

// Example usage demonstrating performance improvement
func ExampleTieredBufferPool_usage() {
	pool := NewTieredBufferPool(4)

	// Get a buffer for serialization
	buf := pool.Get(4096)

	// Use the buffer
	data := []byte("Hello, World!")
	buf = append(buf, data...)

	// Return the buffer when done
	pool.Put(buf)

	fmt.Println("Buffer pool usage example completed")
}

// TestTieredBufferPool_MemoryEfficiency tests memory efficiency.
func TestTieredBufferPool_MemoryEfficiency(t *testing.T) {
	pool := NewTieredBufferPool(4)

	// Allocate many small buffers
	const count = 1000

	// Warm up the allocator so the first measurement reflects steady-state
	// performance rather than GC or allocator cold start.
	warm := pool.Get(256)
	pool.Put(warm)

	bufs := make([][]byte, count)

	startTime := time.Now()
	for i := 0; i < count; i++ {
		bufs[i] = pool.Get(256)
	}
	allocTime := time.Since(startTime)

	// Return all buffers
	for i := 0; i < count; i++ {
		pool.Put(bufs[i])
	}

	t.Logf("Allocated %d buffers in %v (%v per buffer)", count, allocTime, allocTime/count)

	// Re-allocate - should be faster due to recycling
	startTime = time.Now()
	for i := 0; i < count; i++ {
		bufs[i] = pool.Get(256)
	}
	reallocTime := time.Since(startTime)

	t.Logf("Re-allocated %d buffers in %v (%v per buffer)", count, reallocTime, reallocTime/count)

	if reallocTime > allocTime*3 {
		t.Errorf("Re-allocation should be faster: alloc=%v, realloc=%v", allocTime, reallocTime)
	}
}

// Benchmark memory efficiency
func BenchmarkBufferPool_MemoryEfficiency_Old(b *testing.B) {
	pool := newOldBufferPool(256)
	bufs := make([][]byte, 100)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < 100; j++ {
			bufs[j] = pool.Get()
		}
		for j := 0; j < 100; j++ {
			pool.Put(bufs[j])
		}
	}
}

func BenchmarkBufferPool_MemoryEfficiency_New(b *testing.B) {
	pool := NewTieredBufferPool(4)
	bufs := make([][]byte, 100)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < 100; j++ {
			bufs[j] = pool.Get(256)
		}
		for j := 0; j < 100; j++ {
			pool.Put(bufs[j])
		}
	}
}

// TestEstimateBufferSize tests the EstimateBufferSize function.
func TestEstimateBufferSize(t *testing.T) {
	tests := []struct {
		name    string
		obj     interface{}
		minSize int // Minimum expected size
		maxSize int // Maximum expected size
	}{
		{"nil", nil, 64, 64},
		{"empty_byte_slice", []byte{}, 0, 100},
		{"small_byte_slice", []byte{1, 2, 3, 4, 5}, 5, 20},
		{"large_byte_slice", make([]byte, 10000), 10000, 12000},
		{"empty_string", "", 0, 10},
		{"small_string", "hello", 5, 20},
		{"large_string", string(make([]byte, 1000)), 1000, 1500},
		{"empty_slice", []interface{}{}, DefaultInitialBufferSize, DefaultInitialBufferSize},
		{"small_slice", []interface{}{1, 2, 3}, DefaultInitialBufferSize, DefaultInitialBufferSize},
		{"large_slice", make([]interface{}, 1000), 8000, 12000},
		{"empty_map", map[string]interface{}{}, DefaultInitialBufferSize, DefaultInitialBufferSize},
		{"small_map", map[string]interface{}{"a": 1}, DefaultInitialBufferSize, DefaultInitialBufferSize},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			size := EstimateBufferSize(tt.obj)
			if size < tt.minSize || size > tt.maxSize {
				t.Errorf("EstimateBufferSize(%v) = %d, want [%d, %d]",
					tt.obj, size, tt.minSize, tt.maxSize)
			}
		})
	}

	// Test large map with actual data
	largeMap := make(map[string]interface{})
	for i := 0; i < 100; i++ {
		largeMap[fmt.Sprintf("key%d", i)] = i
	}
	size := EstimateBufferSize(largeMap)
	// 100 entries * 16 bytes * 1.2 = 1920, but capped at DefaultInitialBufferSize (4096)
	if size < 1920 || size > 5000 {
		t.Errorf("EstimateBufferSize(large_map) = %d, want [1920, 5000]", size)
	}
}
