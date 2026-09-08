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

// Package cgo provides CGO bindings for Ray runtime.
// This package is organized into subdirectories by function:
//   - boundary/: CGO boundary handling (CoreWorker lifecycle)
//   - interfaces/: Interface implementations (WorkerContext, TaskExecutor, TaskSubmitter)
//   - memory/: Memory management (object allocation)
//   - callback/: Callback functions (called from C++)
//   - utils/: Shared utilities (type conversion)
package cgo

/*
#include <stdint.h>
#include <stdlib.h>

// GoObjectRefHandle - C-side handle structure for Go object references
// This struct is used to pass object information between C++ and Go.
typedef struct {
    void* data_ptr;      // Pointer to Go-managed data
    size_t size;         // Size of data in bytes
    void* ref_handle;    // Go-side reference handle for GC tracking
} GoObjectRefHandle;
*/
import "C"

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/ray-project/ray/go/pkg/ids"
	"github.com/ray-project/ray/go/pkg/log"
)

var logger = log.WithName("native_allocator")

// ObjectRefImpl is the Go-side object reference implementation.
// It stores object data, metadata, and tracking information.
//
// This is similar to Java's ObjectRefImpl class in the Java runtime.
type ObjectRefImpl struct {
	// objectID is the unique identifier for the object
	objectID ids.ObjectID

	// data is the object data byte slice (Go heap managed)
	data []byte

	// metadata is the object metadata byte slice
	metadata []byte

	// isSealed indicates whether the object has been sealed
	isSealed bool

	// pinCount is the reference count to prevent premature GC
	// Uses atomic operations for thread-safe access
	pinCount int32

	// mu is the mutex for protecting concurrent access
	mu sync.RWMutex
}

// AllocatorCallbackRegistry manages the registry of allocated objects.
// It tracks the mapping between ObjectIDs and ObjectRefImpl instances.
//
// Thread safety:
// - Uses sync.Map for concurrent read access
// - Uses mutex for write operations
//
// Lifecycle management:
// - Uses context.Context for graceful shutdown of finalizer workers
// - Uses atomic shutdown flag for finalizer functions (to avoid deadlocks)
type AllocatorCallbackRegistry struct {
	// registry maps objectID string to ObjectRefImpl
	// Using map + RWMutex for better memory management
	registry map[string]*ObjectRefImpl

	// mu protects the registry during concurrent access
	mu sync.RWMutex

	// finalizerQueue is used for delayed cleanup of GC'd objects.
	//
	// Dynamic capacity adjustment:
	// - Initial capacity: 50000 (minimum capacity)
	// - Monitored every 10 seconds by monitorQueueSaturation()
	// - If saturation > 90%, double the capacity (up to reasonable limits)
	// - If saturation < 10% and capacity > 50000, halve the capacity
	// - Non-blocking copy when resizing to avoid blocking finalizer operations
	//
	// Multi-worker design: numFinalizerWorkers (4) workers process the queue in parallel,
	// providing 4x cleanup throughput to handle high GC rates and prevent queue saturation.
	//
	// Saturation monitoring: queueSaturationThreshold defines when to log warnings
	// (90% capacity). Logs are rate-limited to avoid spam during high GC periods.
	finalizerQueue chan string

	// queueSaturationThreshold is the percentage (0.0-1.0) at which to log warnings
	// when finalizerQueue is approaching capacity.
	queueSaturationThreshold float64

	// lastSaturationLog tracks the last time a saturation warning was logged
	// to implement rate limiting (1 minute between warnings)
	lastSaturationLog int64

	// ctx is the context for managing finalizer workers lifecycle
	ctx context.Context

	// cancel is the cancel function for ctx
	cancel context.CancelFunc

	// shutdown is the atomic flag for shutdown signal
	// Used by finalizers to check shutdown status without locks (to avoid deadlocks)
	shutdown int32

	// activeCount tracks the current number of active objects.
	// Using atomic counter instead of len(registry) for lock-free reads.
	activeCount int64

	// totalAllocations tracks total number of allocations
	totalAllocations int64

	// totalBytesAllocated tracks total bytes allocated
	totalBytesAllocated int64

	// gcCount tracks number of objects collected by GC
	gcCount int64

	// workerWg tracks all finalizer worker goroutines
	workerWg sync.WaitGroup
}

// globalRegistry is the global registry instance for all allocated objects.
var globalRegistry = NewAllocatorCallbackRegistry()

// globalRegistryMu protects access to globalRegistry during reset
var globalRegistryMu sync.Mutex

// numFinalizerWorkers is the number of worker goroutines processing the finalizer queue.
// This multi-worker design increases cleanup throughput and prevents memory leaks
// when GC rate exceeds single worker processing capacity.
const numFinalizerWorkers = 4

// NewAllocatorCallbackRegistry creates a new AllocatorCallbackRegistry.
func NewAllocatorCallbackRegistry() *AllocatorCallbackRegistry {
	ctx, cancel := context.WithCancel(context.Background())

	// Set initial capacity to 50000 (minimum capacity).
	// This value balances memory footprint vs resize frequency:
	// - Large enough to handle typical GC bursts without immediate resizing
	// - Small enough to avoid excessive memory waste in steady state
	// Capacity will be dynamically adjusted by monitorQueueSaturation()
	initialCapacity := 50000
	registry := &AllocatorCallbackRegistry{
		registry:                 make(map[string]*ObjectRefImpl),
		finalizerQueue:           make(chan string, initialCapacity),
		ctx:                      ctx,
		cancel:                   cancel,
		queueSaturationThreshold: 0.9, // 90% capacity triggers warning
		lastSaturationLog:        0,
	}

	// Start multiple finalizer worker goroutines for parallel cleanup
	for i := 0; i < numFinalizerWorkers; i++ {
		registry.workerWg.Add(1)
		go registry.finalizerWorker(i)
	}

	// Start queue saturation monitoring goroutine
	registry.workerWg.Add(1)
	go registry.monitorQueueSaturation()

	return registry
}

// ResetGlobalRegistry resets the global registry.
// This function is intended for testing purposes only.
// WARNING: This should only be called when no other goroutines are using the registry.
func ResetGlobalRegistry() {
	globalRegistryMu.Lock()
	defer globalRegistryMu.Unlock()

	oldRegistry := globalRegistry
	globalRegistry = NewAllocatorCallbackRegistry()

	// Shutdown old registry
	if oldRegistry != nil {
		oldRegistry.Shutdown()
	}
}

// finalizerWorker is a worker goroutine that processes cleanup requests from the finalizer queue.
// Multiple workers run in parallel to handle high GC throughput and prevent memory leaks.
func (r *AllocatorCallbackRegistry) finalizerWorker(id int) {
	defer r.workerWg.Done()

	for {
		select {
		case objectID := <-r.finalizerQueue:
			r.cleanupObject(objectID)
		case <-r.ctx.Done():
			// Context cancelled, drain remaining queue items and exit
			for {
				select {
				case objectID := <-r.finalizerQueue:
					r.cleanupObject(objectID)
				default:
					return
				}
			}
		}
	}
}

// resizeQueue resizes the finalizerQueue to newCapacity using non-blocking copy.
// The action parameter ("expanding" or "shrinking") is used for logging.
// Returns true if resize succeeded, false if retry needed (queue too active).
//
// Retry limit: After 100 failed non-blocking attempts, falls back to blocking copy
// to prevent infinite spinning when the queue is highly active.
func (r *AllocatorCallbackRegistry) resizeQueue(newCapacity int, action string) bool {
	logger.Info("finalizerQueue "+action,
		"currentCapacity", cap(r.finalizerQueue),
		"newCapacity", newCapacity,
		"queueLength", len(r.finalizerQueue))

	newQueue := make(chan string, newCapacity)

	// Non-blocking copy with retry limit
	const maxRetries = 100
	retries := 0

	for {
		select {
		case item := <-r.finalizerQueue:
			select {
			case newQueue <- item:
				// Successfully copied
			default:
				// New queue is full, this shouldn't happen with doubled capacity
				// Fall back to blocking send
				newQueue <- item
			}
		default:
			// Old queue is empty, replacement complete
			r.mu.Lock()
			r.finalizerQueue = newQueue
			r.mu.Unlock()
			logger.Info("finalizerQueue capacity "+action+" successfully",
				"newCapacity", newCapacity)
			return true
		}

		retries++
		if retries > maxRetries {
			// Too many retries, the queue is too active
			// Drain remaining items with blocking sends
			for {
				select {
				case item := <-r.finalizerQueue:
					newQueue <- item // Blocking send
				default:
					r.mu.Lock()
					r.finalizerQueue = newQueue
					r.mu.Unlock()
					logger.Info("finalizerQueue resize completed with blocking fallback",
						"newCapacity", newCapacity, "retries", retries)
					return true
				}
			}
		}
	}
}

// monitorQueueSaturation dynamically adjusts finalizerQueue capacity based on saturation.
// Expands when >90% full (to prevent overflow) and shrinks when <10% full (to save memory).
// Uses non-blocking copy to avoid interfering with finalizer operations.
// Minimum capacity is always 50000.
func (r *AllocatorCallbackRegistry) monitorQueueSaturation() {
	defer r.workerWg.Done()

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			queueLen := len(r.finalizerQueue)
			queueCap := cap(r.finalizerQueue)

			if queueCap == 0 {
				continue // Avoid division by zero
			}

			saturation := float64(queueLen) / float64(queueCap)

			if saturation > 0.9 {
				// Expand: double the capacity
				// Use exponential backoff to avoid CPU spinning during retries
				newCapacity := queueCap * 2
				backoff := 10 * time.Millisecond
				const maxBackoff = time.Second

				for !r.resizeQueue(newCapacity, "expanding") {
					time.Sleep(backoff)
					backoff *= 2
					if backoff > maxBackoff {
						backoff = maxBackoff
					}
				}

			} else if saturation < 0.1 && queueCap > 50000 {
				// Shrink: halve the capacity (with minimum check)
				// Use exponential backoff to avoid CPU spinning during retries
				newCapacity := queueCap / 2
				if newCapacity < 50000 {
					newCapacity = 50000
				}
				backoff := 10 * time.Millisecond
				const maxBackoff = time.Second

				for !r.resizeQueue(newCapacity, "shrinking") {
					time.Sleep(backoff)
					backoff *= 2
					if backoff > maxBackoff {
						backoff = maxBackoff
					}
				}
			}

		case <-r.ctx.Done():
			return
		}
	}
}

// cleanupObject removes an object from the registry.
func (r *AllocatorCallbackRegistry) cleanupObject(objectID string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.registry[objectID]; exists {
		delete(r.registry, objectID)
		atomic.AddInt64(&r.activeCount, -1)
		atomic.AddInt64(&r.gcCount, 1)
	}
}

// requestCleanup requests cleanup of an object from the registry.
// This method encapsulates the finalizer cleanup logic to avoid exposing
// internal registry structure.
//
// The method checks the shutdown flag atomically (to avoid deadlocks in finalizers)
// and attempts to queue the objectID for cleanup. If the queue is full, cleanup
// is skipped and the object remains in the registry.
//
// Rate-limited saturation logging: When queue usage exceeds 90%, logs a warning
// at most once per minute to avoid log spam during high GC periods.
func (r *AllocatorCallbackRegistry) requestCleanup(objectID string) {
	// Check shutdown flag using atomic operation only
	// Avoiding mutex locks in finalizer to prevent deadlocks
	if atomic.LoadInt32(&r.shutdown) != 0 {
		return
	}

	// Check queue saturation and log warning if needed (rate-limited)
	queueLen := len(r.finalizerQueue)
	queueCap := cap(r.finalizerQueue)
	if queueCap > 0 && float64(queueLen)/float64(queueCap) >= r.queueSaturationThreshold {
		now := time.Now().Unix()
		lastLog := atomic.LoadInt64(&r.lastSaturationLog)
		// Rate limit: at most one warning per minute
		if now-lastLog >= 60 {
			if atomic.CompareAndSwapInt64(&r.lastSaturationLog, lastLog, now) {
				logger.Error(
					nil,
					"finalizerQueue saturation detected",
					"queueLength", queueLen,
					"queueCapacity", queueCap,
					"usagePercent", int(float64(queueLen)/float64(queueCap)*100),
				)
			}
		}
	}

	// Non-blocking send to finalizer queue
	select {
	case r.finalizerQueue <- objectID:
		// Successfully queued for cleanup
	default:
		// Queue is full, skip cleanup
		// The object will remain in registry but this is acceptable
		// as it will be cleaned up during next shutdown
	}
}

// RegisterObject registers an object reference in the registry.
func (r *AllocatorCallbackRegistry) RegisterObject(ref *ObjectRefImpl) {
	objectIDStr := string(ref.objectID.Binary())
	r.mu.Lock()
	defer r.mu.Unlock()
	r.registry[objectIDStr] = ref
	atomic.AddInt64(&r.activeCount, 1)
}

// GetObject retrieves an object reference from the registry.
func (r *AllocatorCallbackRegistry) GetObject(objectID string) *ObjectRefImpl {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.registry[objectID]
}

// GetObjectCount returns the current number of registered objects.
// This is useful for debugging and testing.
//
// Performance note: This method returns an approximate count using atomic operations
// for high-performance lock-free reads. The count may briefly differ from the actual
// len(registry) during concurrent RegisterObject/cleanupObject operations.
// For exact count, use GetObjectCountExact() which acquires a read lock.
func (r *AllocatorCallbackRegistry) GetObjectCount() int {
	return int(atomic.LoadInt64(&r.activeCount))
}

// GetObjectCountExact returns the exact number of registered objects.
// This method acquires a read lock and is slower than GetObjectCount().
// Use this only when exact count is required (e.g., in tests).
func (r *AllocatorCallbackRegistry) GetObjectCountExact() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.registry)
}

// Shutdown gracefully shuts down the registry.
func (r *AllocatorCallbackRegistry) Shutdown() {
	atomic.StoreInt32(&r.shutdown, 1)

	// Cancel context to signal all finalizer workers to stop
	r.cancel()

	// Wait for all workers to complete and drain their queues
	r.workerWg.Wait()
}

// GoAllocateObject allocates object memory in Go heap.
//
// This function is called from C++ via CGO when the ObjectAllocatorCallback
// needs to allocate memory for a new Ray object.
//
// Parameters:
//
//	objectIDData - Binary data of ObjectID
//	objectIDSize - Size of ObjectID binary data
//	data - Object data pointer (C memory, will be copied)
//	dataSize - Object data size in bytes
//	metadata - Object metadata pointer (C memory, will be copied)
//	metadataSize - Object metadata size in bytes
//
// Returns:
//
//	Opaque handle (GoObjectRefHandle*) on success
//	NULL on failure
//
// Memory management:
//   - Data is copied from C to Go heap
//   - Go GC manages the lifetime of the copied data
//   - C++ side holds a reference via GoObjectRefHandle
//
//export GoAllocateObject
func GoAllocateObject(
	objectIDData *C.char,
	objectIDSize C.int,
	data *C.char,
	dataSize C.int,
	metadata *C.char,
	metadataSize C.int,
) unsafe.Pointer {

	// Parse ObjectID from binary data
	objectIDBytes := C.GoBytes(unsafe.Pointer(objectIDData), objectIDSize)
	if len(objectIDBytes) != ids.ObjectIDSize {
		logger.Error(
			fmt.Errorf("invalid ObjectID size"),
			"GoAllocateObject: ObjectID size mismatch",
			"got", len(objectIDBytes),
			"expected", ids.ObjectIDSize,
		)
		return nil
	}

	objectID, err := ids.ObjectIDFromBinary(objectIDBytes)
	if err != nil {
		return nil
	}

	// Create ObjectRefImpl instance
	objRef := &ObjectRefImpl{
		objectID: objectID,
		data:     nil,
		metadata: nil,
		isSealed: false,
		pinCount: 1, // Initial reference count is 1
	}

	// Allocate Go memory and copy data
	dataSizeInt := int(dataSize)
	if dataSizeInt > 0 && data != nil {
		objRef.data = make([]byte, dataSizeInt)
		copy(objRef.data, C.GoBytes(unsafe.Pointer(data), dataSize))
	}

	// Allocate Go memory and copy metadata
	metadataSizeInt := int(metadataSize)
	if metadataSizeInt > 0 && metadata != nil {
		objRef.metadata = make([]byte, metadataSizeInt)
		copy(objRef.metadata, C.GoBytes(unsafe.Pointer(metadata), metadataSize))
	}

	// Create C-side handle BEFORE registering in registry
	handle := (*C.GoObjectRefHandle)(C.malloc(C.sizeof_GoObjectRefHandle))
	if handle == nil {
		logger.Error(
			fmt.Errorf("C.malloc failed"),
			"GoAllocateObject: failed to allocate object handle",
		)
		return nil
	}

	// Set handle fields BEFORE registering in registry
	// This ensures that if registration succeeds, the handle is fully initialized
	if len(objRef.data) > 0 {
		handle.data_ptr = unsafe.Pointer(&objRef.data[0])
	} else {
		handle.data_ptr = nil
	}
	handle.size = C.size_t(len(objRef.data))
	handle.ref_handle = unsafe.Pointer(objRef)

	// Register object in global registry AFTER successful malloc and handle initialization
	globalRegistry.RegisterObject(objRef)
	globalRegistryMu.Lock()
	atomic.AddInt64(&globalRegistry.totalAllocations, 1)
	atomic.AddInt64(&globalRegistry.totalBytesAllocated, int64(dataSizeInt+metadataSizeInt))
	globalRegistryMu.Unlock()

	// Set finalizer for GC coordination
	// When Go GC collects this object, the finalizer will be called
	// Note: We use only atomic operations here to avoid deadlocks
	runtime.SetFinalizer(objRef, func(o *ObjectRefImpl) {
		globalRegistry.requestCleanup(string(o.objectID.Binary()))
	})

	return unsafe.Pointer(handle)
}

// GoReleaseObjectRef releases a Go object reference.
//
// This function is called from C++ when a GoManagedBuffer is destroyed.
// It decrements the reference count, allowing Go GC to reclaim memory
// when no more references exist.
//
// Parameters:
//
//	handle - Opaque handle returned by GoAllocateObject
//
//export GoReleaseObjectRef
func GoReleaseObjectRef(handle unsafe.Pointer) {
	if handle == nil {
		return
	}

	// Decrement reference count
	goHandle := (*ObjectRefImpl)(handle)
	newCount := atomic.AddInt32(&goHandle.pinCount, -1)

	if newCount < 0 {
		// Reference count underflow - this indicates a bug
		// Log warning but don't panic to avoid crashing the runtime
		return
	}

	// Note: Actual memory release is handled by Go GC
	// We just decrement the reference count here
}

// GoGetObjectData returns the data pointer of a Go object.
//
// This function is called from C++ to access the object data
// without copying. The returned pointer is valid as long as
// the Go object is not garbage collected.
//
// Parameters:
//
//	handle - Opaque handle returned by GoAllocateObject
//
// Returns:
//
//	Pointer to object data, or NULL if object has no data
//
//export GoGetObjectData
func GoGetObjectData(handle unsafe.Pointer) unsafe.Pointer {
	if handle == nil {
		return nil
	}

	goHandle := (*ObjectRefImpl)(handle)
	if len(goHandle.data) == 0 {
		return nil
	}

	return unsafe.Pointer(&goHandle.data[0])
}

// GoGetObjectSize returns the size of a Go object.
//
// Parameters:
//
//	handle - Opaque handle returned by GoAllocateObject
//
// Returns:
//
//	Size of object data in bytes
//
//export GoGetObjectSize
func GoGetObjectSize(handle unsafe.Pointer) C.size_t {
	if handle == nil {
		return 0
	}

	goHandle := (*ObjectRefImpl)(handle)
	return C.size_t(len(goHandle.data))
}

// AllocatorStats contains statistics about the object allocator.
type AllocatorStats struct {
	// TotalAllocations is the total number of allocations
	TotalAllocations int64

	// TotalBytesAllocated is the total bytes allocated
	TotalBytesAllocated int64

	// ActiveObjects is the current number of active objects
	ActiveObjects int64

	// GCCount is the number of objects collected by GC
	GCCount int64
}

// GetAllocatorStats returns statistics about the object allocator.
// This is useful for debugging and monitoring.
func GetAllocatorStats() AllocatorStats {
	return AllocatorStats{
		TotalAllocations:    atomic.LoadInt64(&globalRegistry.totalAllocations),
		TotalBytesAllocated: atomic.LoadInt64(&globalRegistry.totalBytesAllocated),
		ActiveObjects:       int64(globalRegistry.GetObjectCount()),
		GCCount:             atomic.LoadInt64(&globalRegistry.gcCount),
	}
}

// ShutdownAllocator gracefully shuts down the allocator.
// This should be called when the runtime is shutting down.
func ShutdownAllocator() {
	globalRegistry.Shutdown()
}
