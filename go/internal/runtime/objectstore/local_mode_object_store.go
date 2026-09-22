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

package objectstore

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/ray-project/ray/go/pkg/ids"
	"github.com/ray-project/ray/go/pkg/runtime/object"
)

// LocalModeObjectStore is the local mode implementation of ObjectStore.
// It provides in-memory storage for development and testing without requiring a cluster.
//
// Designed to be compatible with Java's io.ray.runtime.object.LocalModeObjectStore.
type LocalModeObjectStore struct {
	mu                 sync.RWMutex
	cond               *sync.Cond
	store              map[ids.ObjectID]*object.NativeRayObject
	objectPutCallbacks []func(ids.ObjectID)
	checkIntervalMs    int64
}

// LocalModeOption is a function type for configuring LocalModeObjectStore.
type LocalModeOption func(*LocalModeObjectStore)

// WithCheckIntervalMs sets the polling interval for checking object readiness.
// Default is 1ms. Only effective when using polling-based waiting (deprecated Wait method).
func WithCheckIntervalMs(interval int64) LocalModeOption {
	return func(s *LocalModeObjectStore) {
		if interval > 0 {
			s.checkIntervalMs = interval
		}
	}
}

// LocalModeObjectStoreOption is a function type for configuring object store behavior.
// Deprecated: Use LocalModeOption instead.
type LocalModeObjectStoreOption = LocalModeOption

// WithDeepCopy is retained for API compatibility but has no effect: local mode
// always uses reference semantics (shared underlying arrays) aligned with
// Java's LocalModeObjectStore. Callers must not mutate the Data/Metadata/
// ContainedObjectIds slices after Put.
func WithDeepCopy(enabled bool) LocalModeOption {
	return func(s *LocalModeObjectStore) {
		// No-op: reference semantics are always used in local mode.
	}
}

// Interface compliance check: LocalModeObjectStore implements object.ObjectStore.
var _ object.ObjectStore = (*LocalModeObjectStore)(nil)

// NewLocalModeObjectStore creates a new LocalModeObjectStore instance.
func NewLocalModeObjectStore(opts ...LocalModeOption) *LocalModeObjectStore {
	store := &LocalModeObjectStore{
		store:              make(map[ids.ObjectID]*object.NativeRayObject),
		objectPutCallbacks: make([]func(ids.ObjectID), 0),
		checkIntervalMs:    1, // default 1ms
	}
	store.cond = sync.NewCond(&store.mu)
	for _, opt := range opts {
		opt(store)
	}
	return store
}

// AddObjectPutCallback registers a callback to be invoked when an object is put.
func (l *LocalModeObjectStore) AddObjectPutCallback(callback func(ids.ObjectID)) {
	if callback == nil {
		panic("callback cannot be nil")
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.objectPutCallbacks = append(l.objectPutCallbacks, callback)
}

// IsObjectReady checks if an object is present in the store.
func (l *LocalModeObjectStore) IsObjectReady(objectID ids.ObjectID) bool {
	l.mu.RLock()
	defer l.mu.RUnlock()
	_, exists := l.store[objectID]
	return exists
}

// PutRaw stores the object and returns the generated ObjectID.
func (l *LocalModeObjectStore) PutRaw(obj *object.NativeRayObject) (*ids.ObjectID, error) {
	// Generate a random ObjectID like Java's ObjectId.fromRandom()
	objectID := ids.NewObjectID()
	err := l.PutRawWithID(obj, &objectID)
	if err != nil {
		return nil, err
	}
	return &objectID, nil
}

// PutRawWithOwner stores the object with the specified owner actor ID.
// LocalMode does not support owner assignment, so this throws an error.
func (l *LocalModeObjectStore) PutRawWithOwner(obj *object.NativeRayObject, ownerActorID *ids.ActorID) (*ids.ObjectID, error) {
	return nil, fmt.Errorf("assigning owner in Ray.put() is not implemented in local mode")
}

// PutRawWithID stores the object with the specified ObjectID.
//
// Uses reference semantics aligned with Java's LocalModeObjectStore: the stored
// object shares the caller's underlying arrays (O(1) slice-header copy) and
// Get returns the same reference with zero copy. Ownership of pool-allocated
// buffers is transferred to the store via ReleasePoolOwnership on both the
// stored object and the caller's object, so neither Close() returns the shared
// buffer to the pool (the caller's defer Close() would otherwise undo the
// transfer if local mode ever enables the buffer pool).
//
// Callers must not mutate the Data/Metadata/ContainedObjectIds slices after Put.
func (l *LocalModeObjectStore) PutRawWithID(obj *object.NativeRayObject, objectID *ids.ObjectID) error {
	if obj == nil {
		return fmt.Errorf("object cannot be nil")
	}
	if objectID == nil {
		return fmt.Errorf("objectID cannot be nil")
	}

	l.mu.Lock()
	// Store the object (only if not already present, like Java's putIfAbsent)
	var needCallback bool
	if _, exists := l.store[*objectID]; !exists {
		// Reference semantics aligned with Java's LocalModeObjectStore: store a
		// distinct struct sharing the caller's underlying arrays (O(1) slice-header
		// copy). The caller shares those arrays via stored, so both the stored
		// object and the caller's object must give up pool ownership; otherwise the
		// caller's Close() would return the shared buffer to the pool and pool reuse
		// could invalidate the stored reference (relevant if local mode ever
		// enables the buffer pool). ReleasePoolOwnership is a no-op for
		// non-pool-allocated buffers, so ordinary objects are unaffected.
		stored := &object.NativeRayObject{
			Data:               obj.Data,
			Metadata:           obj.Metadata,
			ContainedObjectIds: obj.ContainedObjectIds,
		}
		stored.ReleasePoolOwnership()
		obj.ReleasePoolOwnership()
		l.store[*objectID] = stored
		needCallback = true
		// Notify waiting goroutines that object is ready
		l.cond.Broadcast()
	}
	// Copy callback list to avoid concurrent modification while holding the lock
	callbacks := make([]func(ids.ObjectID), len(l.objectPutCallbacks))
	copy(callbacks, l.objectPutCallbacks)
	l.mu.Unlock()

	// Invoke callbacks outside the lock to prevent blocking other goroutines
	// This avoids potential deadlocks if callbacks trigger operations that need the lock
	if needCallback {
		for _, callback := range callbacks {
			callback(*objectID)
		}
	}
	return nil
}

// CreateOwned allocates a writable buffer in the object store for a new object.
// Not supported in local mode (no plasma store).
func (l *LocalModeObjectStore) CreateOwned(metadata *object.NativeRayObject, dataSize int) (*ids.ObjectID, uintptr, uint64, error) {
	return nil, 0, 0, fmt.Errorf("CreateOwned is not implemented in local mode")
}

// SealOwned finalizes an object created by CreateOwned.
// Not supported in local mode (no plasma store).
func (l *LocalModeObjectStore) SealOwned(objectID *ids.ObjectID, handle uint64) error {
	return fmt.Errorf("SealOwned is not implemented in local mode")
}

// CreateExisting allocates a writable buffer for a caller-supplied ObjectID.
// Returns handle==0 in local mode so callers fall back to the existing
// in-memory copy path.
func (l *LocalModeObjectStore) CreateExisting(metadata *object.NativeRayObject, dataSize int, objectID *ids.ObjectID) (uintptr, uint64, error) {
	return 0, 0, nil
}

// SealExisting finalizes an object created by CreateExisting.
// Not supported in local mode (no plasma store).
func (l *LocalModeObjectStore) SealExisting(objectID *ids.ObjectID, handle uint64) error {
	return fmt.Errorf("SealExisting is not implemented in local mode")
}

// getRawInternal is the internal implementation for retrieving objects.
// If checkCtx is provided, it is called at key points to check for context cancellation.
func (l *LocalModeObjectStore) getRawInternal(
	objectIDs []*ids.ObjectID,
	timeoutMs int64,
	objectType string,
	checkCtx func() error,
) ([]*object.NativeRayObject, error) {
	// Check context if provided
	if checkCtx != nil {
		if err := checkCtx(); err != nil {
			return nil, err
		}
	}

	// Wait for objects to be ready
	l.waitForObjects(objectIDs, len(objectIDs), timeoutMs)

	// Check context again if provided
	if checkCtx != nil {
		if err := checkCtx(); err != nil {
			return nil, err
		}
	}

	// Check if all objects are ready
	if timeoutMs >= 0 {
		readyCount := 0
		for _, oid := range objectIDs {
			if l.IsObjectReady(*oid) {
				readyCount++
			}
		}
		if readyCount < len(objectIDs) {
			return nil, fmt.Errorf("get timed out: some object(s) not ready")
		}
	}

	// Retrieve objects with reference semantics (aligned with Java's getRaw):
	// return a distinct struct sharing the stored object's underlying arrays so
	// callers get zero-copy access while Close() on the returned object does not
	// nil out the stored object's Data.
	result := make([]*object.NativeRayObject, 0, len(objectIDs))
	for _, oid := range objectIDs {
		l.mu.RLock()
		obj, exists := l.store[*oid]
		l.mu.RUnlock()

		if exists {
			result = append(result, &object.NativeRayObject{
				Data:               obj.Data,
				Metadata:           obj.Metadata,
				ContainedObjectIds: obj.ContainedObjectIds,
			})
		}
	}
	return result, nil
}

// GetRaw retrieves objects by their IDs with reference semantics: the returned
// objects share the same underlying arrays as the stored objects.
//
// WARNING: Callers MUST NOT mutate the returned Data, Metadata, or
// ContainedObjectIds slices. Violating this can cause data corruption and race
// conditions. The returned struct is a shallow copy distinct from the stored
// object, so calling Close() on it is safe and does not affect the stored
// object.
func (l *LocalModeObjectStore) GetRaw(objectIDs []*ids.ObjectID, timeoutMs int64, objectType string) ([]*object.NativeRayObject, error) {
	return l.getRawInternal(objectIDs, timeoutMs, objectType, nil)
}

// GetRawWithContext retrieves objects by their IDs with context support.
func (l *LocalModeObjectStore) GetRawWithContext(ctx context.Context, objectIDs []*ids.ObjectID, timeoutMs int64, objectType string) ([]*object.NativeRayObject, error) {
	return l.getRawInternal(objectIDs, timeoutMs, objectType, func() error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			return nil
		}
	})
}

// GetRawReadOnly retrieves objects with shallow copying for read-only access.
// This method returns references to internal data to avoid allocation overhead.
//
// WARNING: The returned slices share the same underlying arrays as the stored objects.
// Callers MUST NOT modify the returned Data, Metadata, or ContainedObjectIds slices.
// Violating this can cause data corruption and race conditions.
//
// Use this method only when:
// 1. You only need to read the data
// 2. Performance is critical and GC pressure from deep copying is a concern
// 3. You can guarantee no mutation of returned slices
//
// GetRaw and GetRawReadOnly share the same reference semantics; GetRawReadOnly
// exists for callers that want to be explicit about read-only access.
func (l *LocalModeObjectStore) GetRawReadOnly(objectIDs []*ids.ObjectID, timeoutMs int64, objectType string) ([]*object.NativeRayObject, error) {
	// Wait for objects to be ready
	l.waitForObjects(objectIDs, len(objectIDs), timeoutMs)

	// Check if all objects are ready
	if timeoutMs >= 0 {
		readyCount := 0
		for _, oid := range objectIDs {
			if l.IsObjectReady(*oid) {
				readyCount++
			}
		}
		if readyCount < len(objectIDs) {
			return nil, fmt.Errorf("get timed out: some object(s) not ready")
		}
	}

	// Retrieve objects with shallow copy (reference semantics)
	result := make([]*object.NativeRayObject, 0, len(objectIDs))
	for _, oid := range objectIDs {
		l.mu.RLock()
		obj, exists := l.store[*oid]
		l.mu.RUnlock()

		if exists {
			// Shallow copy - shares underlying arrays
			result = append(result, &object.NativeRayObject{
				Data:               obj.Data,
				Metadata:           obj.Metadata,
				ContainedObjectIds: obj.ContainedObjectIds,
			})
		}
	}
	return result, nil
}

// Wait waits for objects to become ready.
//
// Deprecated: Use WaitWithOptions instead for better parameter organization.
func (l *LocalModeObjectStore) Wait(objectIDs []*ids.ObjectID, numObjects int, timeoutMs int64, fetchLocal bool) ([]bool, error) {
	l.waitForObjects(objectIDs, numObjects, timeoutMs)

	// Return readiness status for each object
	result := make([]bool, len(objectIDs))
	for i, oid := range objectIDs {
		result[i] = l.IsObjectReady(*oid)
	}
	return result, nil
}

// WaitWithOptions waits for objects to become ready using the provided options.
func (l *LocalModeObjectStore) WaitWithOptions(opts object.WaitOptions) ([]bool, error) {
	return l.Wait(opts.ObjectIDs, opts.NumObjects, opts.TimeoutMs, opts.FetchLocal)
}

// waitForObjects waits for the specified number of objects to become ready.
func (l *LocalModeObjectStore) waitForObjects(objectIDs []*ids.ObjectID, numObjects int, timeoutMs int64) {
	if timeoutMs == 0 {
		return
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	if timeoutMs < 0 {
		// Infinite wait - use condition variable
		for l.countReady(objectIDs) < numObjects {
			l.cond.Wait()
		}
	} else {
		// Wait with timeout using time.AfterFunc to avoid goroutine leaks
		deadline := time.Now().Add(time.Duration(timeoutMs) * time.Millisecond)
		timeoutFired := false

		for l.countReady(objectIDs) < numObjects {
			remaining := time.Until(deadline)
			if remaining <= 0 {
				break
			}

			// Use a channel to coordinate timeout and broadcast
			done := make(chan struct{})

			// Schedule timeout using AfterFunc - more efficient than goroutine+Sleep
			stopFunc := time.AfterFunc(remaining, func() {
				l.mu.Lock()
				defer l.mu.Unlock()
				timeoutFired = true
				close(done)
				l.cond.Broadcast()
			})

			// Wait on condition variable
			l.cond.Wait()

			// Stop the timer if we woke up before timeout
			if !stopFunc.Stop() {
				// Timer already fired, wait for done channel to ensure cleanup
				<-done
			}

			// Check if timeout fired and condition not met
			if timeoutFired && l.countReady(objectIDs) < numObjects {
				break
			}
		}
	}
}

// countReady returns the number of objects that are present in the store.
// Must be called with l.mu held.
func (l *LocalModeObjectStore) countReady(objectIDs []*ids.ObjectID) int {
	count := 0
	for _, oid := range objectIDs {
		if _, exists := l.store[*oid]; exists {
			count++
		}
	}
	return count
}

// Delete deletes objects from the object store.
func (l *LocalModeObjectStore) Delete(objectIDs []*ids.ObjectID, localOnly bool) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	for _, oid := range objectIDs {
		delete(l.store, *oid)
	}
	return nil
}

// AddLocalReference adds a local reference to the object.
// No-op in local mode (like Java LocalModeObjectStore).
func (l *LocalModeObjectStore) AddLocalReference(objectID *ids.ObjectID) error {
	// No-op in local mode
	return nil
}

// RemoveLocalReference removes a local reference from the object.
// No-op in local mode (like Java LocalModeObjectStore).
func (l *LocalModeObjectStore) RemoveLocalReference(objectID *ids.ObjectID) error {
	// No-op in local mode
	return nil
}

// GetOwnershipInfo returns the ownership info for the object.
// Returns empty bytes in local mode.
func (l *LocalModeObjectStore) GetOwnershipInfo(objectID *ids.ObjectID) ([]byte, error) {
	return []byte{}, nil
}

// RegisterOwnershipInfoAndResolveFuture registers ownership info and resolves future.
// No-op in local mode.
func (l *LocalModeObjectStore) RegisterOwnershipInfoAndResolveFuture(
	objectID *ids.ObjectID,
	outerObjectID *ids.ObjectID,
	ownerAddress []byte,
) error {
	// No-op in local mode
	return nil
}

// GetOwnerAddress returns the owner address of the object.
// Returns empty/default address in local mode.
func (l *LocalModeObjectStore) GetOwnerAddress(objectID *ids.ObjectID) ([]byte, error) {
	return []byte{}, nil
}

// GetAllReferenceCounts returns all reference counts.
// Returns empty map in local mode.
func (l *LocalModeObjectStore) GetAllReferenceCounts() (map[ids.ObjectID][2]int64, error) {
	return make(map[ids.ObjectID][2]int64), nil
}
