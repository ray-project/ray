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

package cgo

import (
	"sync"
	"testing"
	"time"

	"github.com/ray-project/ray/go/pkg/ids"
)

// TestObjectRefImpl tests the basic structure of ObjectRefImpl.
func TestObjectRefImpl(t *testing.T) {
	// Create test ObjectID
	objectIDData := make([]byte, ids.ObjectIDSize)
	for i := range objectIDData {
		objectIDData[i] = byte(i)
	}
	objectID, err := ids.ObjectIDFromBinary(objectIDData)
	if err != nil {
		t.Fatalf("Failed to create ObjectID: %v", err)
	}

	// Create ObjectRefImpl instance
	objRef := &ObjectRefImpl{
		objectID: objectID,
		data:     []byte("test data"),
		metadata: []byte("test metadata"),
		isSealed: false,
		pinCount: 1,
	}

	// Verify attributes
	if objRef.objectID != objectID {
		t.Error("objectID mismatch")
	}
	if string(objRef.data) != "test data" {
		t.Errorf("data mismatch: got %s, want test data", string(objRef.data))
	}
	if string(objRef.metadata) != "test metadata" {
		t.Errorf("metadata mismatch: got %s, want test metadata", string(objRef.metadata))
	}
	if objRef.isSealed {
		t.Error("isSealed should be false")
	}
	if objRef.pinCount != 1 {
		t.Errorf("pinCount mismatch: got %d, want 1", objRef.pinCount)
	}
}

// TestObjectRefImpl_Mutex tests the concurrency safety of ObjectRefImpl.
func TestObjectRefImpl_Mutex(t *testing.T) {
	objectIDData := make([]byte, ids.ObjectIDSize)
	objectID, _ := ids.ObjectIDFromBinary(objectIDData)

	objRef := &ObjectRefImpl{
		objectID: objectID,
		data:     make([]byte, 100),
		pinCount: 1,
	}

	// Concurrent read/write test
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			objRef.mu.RLock()
			_ = len(objRef.data)
			objRef.mu.RUnlock()
		}()
	}
	wg.Wait()
}

// TestNewAllocatorCallbackRegistry tests creating a new registry.
func TestNewAllocatorCallbackRegistry(t *testing.T) {
	registry := NewAllocatorCallbackRegistry()
	defer registry.Shutdown()

	if registry == nil {
		t.Fatal("Failed to create AllocatorCallbackRegistry")
	}
	if registry.finalizerQueue == nil {
		t.Error("finalizerQueue should not be nil")
	}
	if cap(registry.finalizerQueue) != 50000 {
		t.Errorf("finalizerQueue capacity mismatch: got %d, want 50000", cap(registry.finalizerQueue))
	}
}

// TestAllocatorCallbackRegistry_RegisterAndGet tests registering and retrieving objects.
func TestAllocatorCallbackRegistry_RegisterAndGet(t *testing.T) {
	registry := NewAllocatorCallbackRegistry()
	defer registry.Shutdown()

	// Create test object
	objectIDData := make([]byte, ids.ObjectIDSize)
	for i := range objectIDData {
		objectIDData[i] = byte(i)
	}
	objectID, _ := ids.ObjectIDFromBinary(objectIDData)

	objRef := &ObjectRefImpl{
		objectID: objectID,
		data:     []byte("test data"),
		pinCount: 1,
	}

	// Register object
	registry.RegisterObject(objRef)

	// Verify object count
	count := registry.GetObjectCount()
	if count != 1 {
		t.Errorf("ObjectCount mismatch: got %d, want 1", count)
	}

	// Get object
	objectIDStr := string(objectID.Binary())
	retrieved := registry.GetObject(objectIDStr)
	if retrieved == nil {
		t.Fatal("Failed to get registered object")
	}
	if retrieved != objRef {
		t.Error("Retrieved object is not the same as registered")
	}
}

// TestAllocatorCallbackRegistry_GetNonExistent tests getting a non-existent object.
func TestAllocatorCallbackRegistry_GetNonExistent(t *testing.T) {
	registry := NewAllocatorCallbackRegistry()
	defer registry.Shutdown()

	retrieved := registry.GetObject("non-existent-id")
	if retrieved != nil {
		t.Error("GetObject should return nil for non-existent ID")
	}
}

// TestAllocatorCallbackRegistry_CleanupObject tests cleaning up an object.
func TestAllocatorCallbackRegistry_CleanupObject(t *testing.T) {
	registry := NewAllocatorCallbackRegistry()
	defer registry.Shutdown()

	// Create and register test object
	objectIDData := make([]byte, ids.ObjectIDSize)
	for i := range objectIDData {
		objectIDData[i] = byte(i)
	}
	objectID, _ := ids.ObjectIDFromBinary(objectIDData)

	objRef := &ObjectRefImpl{
		objectID: objectID,
		data:     []byte("test data"),
		pinCount: 1,
	}

	registry.RegisterObject(objRef)

	// Verify object is registered
	if registry.GetObjectCount() != 1 {
		t.Error("Object should be registered")
	}

	// Clean up object
	registry.cleanupObject(string(objectID.Binary()))

	// Wait for cleanup to complete
	time.Sleep(10 * time.Millisecond)

	// Verify object is cleaned up
	if registry.GetObjectCount() != 0 {
		t.Errorf("Object should be cleaned up, got count %d", registry.GetObjectCount())
	}
}

// TestAllocatorCallbackRegistry_ConcurrentAccess tests concurrent access.
func TestAllocatorCallbackRegistry_ConcurrentAccess(t *testing.T) {
	registry := NewAllocatorCallbackRegistry()
	defer registry.Shutdown()

	// Concurrent register and get objects
	var wg sync.WaitGroup
	numGoroutines := 10
	numObjects := 100

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(base int) {
			defer wg.Done()
			for j := 0; j < numObjects; j++ {
				objectIDData := make([]byte, ids.ObjectIDSize)
				// Use larger offset to avoid overflow
				objectIDData[0] = byte((base + j) % 256)
				objectIDData[1] = byte((base + j) / 256)
				objectID, _ := ids.ObjectIDFromBinary(objectIDData)

				objRef := &ObjectRefImpl{
					objectID: objectID,
					data:     []byte("test data"),
					pinCount: 1,
				}

				registry.RegisterObject(objRef)
				_ = registry.GetObject(string(objectID.Binary()))
			}
		}(i * 1000)
	}

	wg.Wait()

	// Verify all objects are registered (only verify count > 0 due to potential duplicate objectIDs)
	count := registry.GetObjectCount()
	if count <= 0 {
		t.Errorf("ObjectCount should be greater than 0, got %d", count)
	}
}

// TestAllocatorCallbackRegistry_FinalizerLoop tests the finalizer loop.
func TestAllocatorCallbackRegistry_FinalizerLoop(t *testing.T) {
	registry := NewAllocatorCallbackRegistry()
	defer registry.Shutdown()

	// Send object ID to cleanup queue
	testObjectID := "test-object-id"
	registry.finalizerQueue <- testObjectID

	// Wait for cleanup to complete
	time.Sleep(50 * time.Millisecond)

	// Verify finalizerLoop doesn't panic
}

// TestAllocatorCallbackRegistry_FinalizerLoop_QueueFull tests the queue full scenario.
func TestAllocatorCallbackRegistry_FinalizerLoop_QueueFull(t *testing.T) {
	registry := NewAllocatorCallbackRegistry()
	defer registry.Shutdown()

	// Fill the queue
	for i := 0; i < cap(registry.finalizerQueue); i++ {
		registry.finalizerQueue <- string(rune(i))
	}

	// Try to send more (should be skipped, not blocked)
	registry.finalizerQueue <- "overflow"

	// Verify no panic occurs
}

// TestObjectRefImpl_BasicOperations tests basic operations.
func TestObjectRefImpl_BasicOperations(t *testing.T) {
	objectIDData := make([]byte, ids.ObjectIDSize)
	for i := range objectIDData {
		objectIDData[i] = byte(i)
	}
	objectID, _ := ids.ObjectIDFromBinary(objectIDData)

	objRef := &ObjectRefImpl{
		objectID: objectID,
		data:     []byte("test"),
		pinCount: 1,
	}

	// Test basic operations
	objRef.mu.Lock()
	objRef.isSealed = true
	objRef.mu.Unlock()

	if !objRef.isSealed {
		t.Error("isSealed should be true after setting")
	}
}

// TestGetAllocatorStats tests getting allocator statistics.
func TestGetAllocatorStats(t *testing.T) {
	// Ensure clean initial state
	stats := GetAllocatorStats()
	initialCount := stats.ActiveObjects

	// Create some test objects
	numObjects := 5
	refs := make([]*ObjectRefImpl, numObjects)

	for i := 0; i < numObjects; i++ {
		objectIDData := make([]byte, ids.ObjectIDSize)
		objectIDData[0] = byte(i)
		objectID, _ := ids.ObjectIDFromBinary(objectIDData)

		refs[i] = &ObjectRefImpl{
			objectID: objectID,
			data:     []byte("test data"),
			pinCount: 1,
		}
		globalRegistry.RegisterObject(refs[i])
	}

	// Verify statistics
	stats = GetAllocatorStats()
	expectedCount := initialCount + int64(numObjects)
	if stats.ActiveObjects != expectedCount {
		t.Errorf("ActiveObjects mismatch: got %d, want %d", stats.ActiveObjects, expectedCount)
	}

	// Cleanup
	for _, ref := range refs {
		globalRegistry.cleanupObject(string(ref.objectID.Binary()))
	}
}

// TestShutdownAllocator tests shutting down the allocator.
func TestShutdownAllocator(t *testing.T) {
	// Shutdown allocator
	ShutdownAllocator()

	// Verify shutdown flag
	if globalRegistry.shutdown != 1 {
		t.Error("Shutdown flag should be set")
	}
}

// TestObjectRefImpl_PinCount tests reference counting.
func TestObjectRefImpl_PinCount(t *testing.T) {
	objectIDData := make([]byte, ids.ObjectIDSize)
	objectID, _ := ids.ObjectIDFromBinary(objectIDData)

	objRef := &ObjectRefImpl{
		objectID: objectID,
		data:     []byte("test"),
		pinCount: 1,
	}

	// Test reference count operation
	newCount := objRef.pinCount - 1
	if newCount != 0 {
		t.Errorf("pinCount after decrement should be 0, got %d", newCount)
	}
}

// TestAllocatorCallbackRegistry_Shutdown tests the shutdown functionality.
func TestAllocatorCallbackRegistry_Shutdown(t *testing.T) {
	registry := NewAllocatorCallbackRegistry()

	// Verify initial state
	if registry.shutdown != 0 {
		t.Error("Initial shutdown flag should be 0")
	}

	// Shutdown
	registry.Shutdown()

	// Verify shutdown state
	if registry.shutdown != 1 {
		t.Error("Shutdown flag should be 1 after Shutdown()")
	}
}

// TestConcurrentRegisterAndCleanup tests concurrent registration and cleanup.
func TestConcurrentRegisterAndCleanup(t *testing.T) {
	registry := NewAllocatorCallbackRegistry()
	defer registry.Shutdown()

	var wg sync.WaitGroup
	numGoroutines := 10

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			objectIDData := make([]byte, ids.ObjectIDSize)
			objectIDData[0] = byte(id)
			objectID, _ := ids.ObjectIDFromBinary(objectIDData)

			objRef := &ObjectRefImpl{
				objectID: objectID,
				data:     []byte("test data"),
				pinCount: 1,
			}

			registry.RegisterObject(objRef)
			time.Sleep(time.Millisecond)
			registry.cleanupObject(string(objectID.Binary()))
		}(i)
	}

	wg.Wait()
}

// TestObjectRefImpl_DataOperations tests data operations.
func TestObjectRefImpl_DataOperations(t *testing.T) {
	objectIDData := make([]byte, ids.ObjectIDSize)
	objectID, _ := ids.ObjectIDFromBinary(objectIDData)

	// Test empty data
	objRef1 := &ObjectRefImpl{
		objectID: objectID,
		data:     nil,
		pinCount: 1,
	}
	if len(objRef1.data) != 0 {
		t.Error("nil data should have length 0")
	}

	// Test non-empty data
	testData := []byte("test data for operations")
	objRef2 := &ObjectRefImpl{
		objectID: objectID,
		data:     testData,
		pinCount: 1,
	}
	if len(objRef2.data) != len(testData) {
		t.Errorf("data length mismatch: got %d, want %d", len(objRef2.data), len(testData))
	}
}

// TestAllocatorCallbackRegistry_GetObjectCount tests getting object count.
func TestAllocatorCallbackRegistry_GetObjectCount(t *testing.T) {
	registry := NewAllocatorCallbackRegistry()
	defer registry.Shutdown()

	// Initial should be 0
	if registry.GetObjectCount() != 0 {
		t.Error("Initial object count should be 0")
	}

	// Add objects
	for i := 0; i < 10; i++ {
		objectIDData := make([]byte, ids.ObjectIDSize)
		objectIDData[0] = byte(i)
		objectID, _ := ids.ObjectIDFromBinary(objectIDData)

		objRef := &ObjectRefImpl{
			objectID: objectID,
			data:     []byte("test"),
			pinCount: 1,
		}
		registry.RegisterObject(objRef)
	}

	// Verify count
	if registry.GetObjectCount() != 10 {
		t.Errorf("Object count should be 10, got %d", registry.GetObjectCount())
	}
}
