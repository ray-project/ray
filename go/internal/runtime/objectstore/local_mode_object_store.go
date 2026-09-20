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
	"bytes"
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

// WithDeepCopy controls whether PutRawWithID and GetRaw perform deep copying.
// Default is true for safety. Set to false only if caller guarantees no mutation.
//
// WARNING: Disabling deep copy can cause data races if the caller modifies
// the original slice after Put or before Get returns.
func WithDeepCopy(enabled bool) LocalModeOption {
	return func(s *LocalModeObjectStore) {
		// This option is kept for API compatibility but deep copy is always
		// performed in local mode for safety. Use GetRawReadOnly for read-only access.
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
// This method performs deep copying to prevent:
// 1. External modifications to stored data
// 2. Buffer reuse issues when caller invokes Close()
// 3. Data races in concurrent scenarios
//
// For performance-critical scenarios with large objects, consider using
// NativeObjectStore which uses zero-copy strategies with runtime.Pinner.
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
		// Create a deep copy to prevent external modifications and
		// buffer reuse issues when the caller invokes Close().
		// Deep copy ensures each caller has independent data.
		containedIDs := make([][]byte, len(obj.ContainedObjectIds))
		copy(containedIDs, obj.ContainedObjectIds)
		l.store[*objectID] = &object.NativeRayObject{
			Data:               bytes.Clone(obj.Data),
			Metadata:           bytes.Clone(obj.Metadata),
			ContainedObjectIds: containedIDs,
		}
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

	// Retrieve objects with deep copy
	result := make([]*object.NativeRayObject, 0, len(objectIDs))
	for _, oid := range objectIDs {
		l.mu.RLock()
		obj, exists := l.store[*oid]
		l.mu.RUnlock()

		if exists {
			containedIDs := make([][]byte, len(obj.ContainedObjectIds))
			copy(containedIDs, obj.ContainedObjectIds)
			result = append(result, &object.NativeRayObject{
				Data:               bytes.Clone(obj.Data),
				Metadata:           bytes.Clone(obj.Metadata),
				ContainedObjectIds: containedIDs,
			})
		}
	}
	return result, nil
}

// GetRaw retrieves objects by their IDs.
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
// For general use, prefer GetRaw which provides safe deep copying.
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
