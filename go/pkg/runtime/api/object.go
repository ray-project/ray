// Copyright 2025 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package api

import (
	"fmt"
	"os"
	"reflect"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/ray-project/ray/go/pkg/errors"
	"github.com/ray-project/ray/go/pkg/ids"
	"github.com/ray-project/ray/go/pkg/log"
	"github.com/ray-project/ray/go/pkg/runtime/object"
)

// ============================================================================
// Object Operations
// ============================================================================

// Put puts an object into the object store.
//
// Parameters:
//   - value: the value to put
//   - owner: the owner of the object (optional, can be nil)
//
// Returns:
//   - *ObjectRef[T]: a reference to the put object
//   - error: any error encountered during the put operation
func Put[T any](value T, owner *ActorHandleImpl[T]) (*ObjectRef[T], error) {
	handle, ok := tryGetHandle()
	if !ok || handle == nil {
		return nil, errors.ErrRuntimeNotInitialized
	}

	rt := handle.Runtime()
	if rt == nil {
		return nil, fmt.Errorf("runtime instance not available")
	}

	objectStore := rt.GetObjectStore()
	if objectStore == nil {
		return nil, fmt.Errorf("object store not available")
	}

	// Serialize the value using the global serializer
	ser := object.GetSerializer()
	nativeObj, err := ser.Serialize(value)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize value: %w", err)
	}

	// Generate ObjectID
	objectID, err := generateObjectID()
	if err != nil {
		nativeObj.Close() // Return buffer to pool on error
		return nil, fmt.Errorf("failed to generate object ID: %w", err)
	}

	// Put the serialized object into object store with the generated ID
	err = objectStore.PutRawWithID(nativeObj, objectID)
	if err != nil {
		nativeObj.Close() // Return buffer to pool on error
		return nil, fmt.Errorf("failed to put object: %w", err)
	}

	// Close the native object to return buffer to pool
	// Note: ObjectStore should have copied the data internally
	nativeObj.Close()

	// Create ObjectRef
	objectType := ""
	if t := reflect.TypeOf(value); t != nil {
		objectType = t.String()
	}
	ref := &ObjectRef[T]{
		objectID:           *objectID,
		objectType:         objectType,
		skipAddingLocalRef: false,
		released:           atomic.Bool{},
	}

	// The single local reference for this ObjectRef is registered by PutRawWithID
	// (the C++ PutWithID bridge registers ownership with add_local_ref=true) and
	// removed by the finalizer below when the ObjectRef is GCed. Adding another
	// reference here would pin the object in plasma forever, because the finalizer
	// only removes one.

	// Capture objectID in the finalizer closure to avoid accessing the ObjectRef
	// after it has been garbage collected. This is critical because the finalizer
	// runs when the ObjectRef is being GCed, and accessing r.objectID at that point
	// may read corrupted memory.
	objectIDForFinalizer := *objectID

	// Use shared helper to set finalizer
	setupObjectRefFinalizer(ref, objectIDForFinalizer, "FINALIZER")

	return ref, nil
}

// PutWithOwner puts an object into the object store with a specific owner.
//
// Parameters:
//   - value: the value to put
//   - owner: the owner of the object
//
// Returns:
//   - *ObjectRef[T]: a reference to the put object
//   - error: any error encountered during the put operation
func PutWithOwner[T any](value T, owner *ActorHandleImpl[T]) (*ObjectRef[T], error) {
	return Put(value, owner)
}

// Get fetches the object from the object store.
// This method blocks until the object is locally available.
//
// Parameters:
//   - ref: the object reference
//
// Returns:
//   - T: the object value
//   - error: any error encountered during the get operation
func Get[T any](ref *ObjectRef[T]) (T, error) {
	if ref == nil {
		var zero T
		return zero, fmt.Errorf("object reference is nil")
	}
	return ref.Get()
}

// GetWithTimeout fetches the object from the object store with a timeout.
//
// Parameters:
//   - ref: the object reference
//   - timeoutMs: the maximum time in milliseconds to wait (use -1 for infinite timeout)
//
// Returns:
//   - T: the object value
//   - error: any error encountered during the get operation, including timeout
func GetWithTimeout[T any](ref *ObjectRef[T], timeoutMs int64) (T, error) {
	if ref == nil {
		var zero T
		return zero, fmt.Errorf("object reference is nil")
	}
	return ref.GetWithTimeout(timeoutMs)
}

// getObjectsInParallel is an internal helper function that fetches objects in parallel.
// It accepts a getter function to handle both Get and GetWithTimeout cases.
func getObjectsInParallel[T any](
	refs []*ObjectRef[T],
	getter func(*ObjectRef[T]) (T, error),
) ([]T, error) {
	if len(refs) == 0 {
		return []T{}, nil
	}

	// For single object, use direct getter for simplicity
	if len(refs) == 1 {
		value, err := getter(refs[0])
		if err != nil {
			return nil, err
		}
		return []T{value}, nil
	}

	// For multiple objects, fetch in parallel using goroutines
	results := make([]T, len(refs))
	errs := make([]error, len(refs))

	var wg sync.WaitGroup
	wg.Add(len(refs))

	for i, ref := range refs {
		go func(index int, r *ObjectRef[T]) {
			defer wg.Done()
			value, err := getter(r)
			results[index] = value
			errs[index] = err
		}(i, ref)
	}

	wg.Wait()

	// Check for errors
	for i, err := range errs {
		if err != nil {
			return nil, fmt.Errorf("failed to get object %s: %w", refs[i].ObjectID().Hex(), err)
		}
	}

	return results, nil
}

// GetList fetches a list of objects from the object store.
// This method blocks until all objects are locally available.
//
// Parameters:
//   - refs: the list of object references
//
// Returns:
//   - []T: the list of object values
//   - error: any error encountered during the get operation
//
// Performance note: This method fetches objects in parallel to minimize
// total wait time. The total latency is bounded by the slowest object,
// not the sum of all object latencies.
func GetList[T any](refs []*ObjectRef[T]) ([]T, error) {
	return getObjectsInParallel(refs, Get)
}

// GetListWithTimeout fetches a list of objects from the object store with a timeout.
//
// Parameters:
//   - refs: the list of object references
//   - timeoutMs: the maximum time in milliseconds to wait per object (use -1 for infinite timeout)
//
// Returns:
//   - []T: the list of object values
//   - error: any error encountered during the get operation, including timeout
//
// Performance note: This method fetches objects in parallel to minimize
// total wait time. The total latency is bounded by the slowest object,
// not the sum of all object latencies.
func GetListWithTimeout[T any](refs []*ObjectRef[T], timeoutMs int64) ([]T, error) {
	return getObjectsInParallel(refs, func(ref *ObjectRef[T]) (T, error) {
		return GetWithTimeout(ref, timeoutMs)
	})
}

// Wait waits for a list of objects to be locally available.
//
// Parameters:
//   - refs: the list of object references to wait for
//   - numReturns: the number of objects that need to be available
//   - timeoutMs: the maximum time in milliseconds to wait (use -1 for infinite timeout)
//   - fetchLocal: whether to fetch the objects locally
//
// Returns:
//   - *WaitResult[T]: the wait result containing ready and unready objects
//   - error: any error encountered during the wait operation
func Wait[T any](refs []*ObjectRef[T], numReturns int, timeoutMs int64, fetchLocal bool) (*WaitResult[T], error) {
	handle, ok := tryGetHandle()
	if !ok || handle == nil {
		return nil, errors.ErrRuntimeNotInitialized
	}

	runtime := handle.Runtime()
	if runtime == nil {
		return nil, fmt.Errorf("runtime instance not available")
	}

	objectStore := runtime.GetObjectStore()
	if objectStore == nil {
		return nil, fmt.Errorf("object store not available")
	}

	// Extract object IDs
	objectIDs := make([]*ids.ObjectID, 0, len(refs))
	for _, ref := range refs {
		if ref != nil {
			oid := ref.ObjectID()
			objectIDs = append(objectIDs, &oid)
		}
	}

	// Call objectStore.WaitWithOptions
	opts := object.WaitOptions{
		ObjectIDs:  objectIDs,
		NumObjects: numReturns,
		TimeoutMs:  timeoutMs,
		FetchLocal: fetchLocal,
	}

	readyFlags, err := objectStore.WaitWithOptions(opts)
	if err != nil {
		return nil, err
	}

	// Build result
	ready := make([]*ObjectRef[T], 0)
	unready := make([]*ObjectRef[T], 0)

	for i, ref := range refs {
		if ref != nil && i < len(readyFlags) && readyFlags[i] {
			ready = append(ready, ref)
		} else {
			unready = append(unready, ref)
		}
	}

	return NewWaitResult(ready, unready), nil
}

// WaitWithNumReturns waits for a specific number of objects to be locally available.
//
// Parameters:
//   - refs: the list of object references to wait for
//   - numReturns: the number of objects that need to be available
//   - timeoutMs: the maximum time in milliseconds to wait (use -1 for infinite timeout)
//
// Returns:
//   - *WaitResult[T]: the wait result containing ready and unready objects
//   - error: any error encountered during the wait operation
func WaitWithNumReturns[T any](refs []*ObjectRef[T], numReturns int, timeoutMs int64) (*WaitResult[T], error) {
	return Wait(refs, numReturns, timeoutMs, false)
}

// WaitWithTimeout waits for objects with a timeout.
//
// Parameters:
//   - refs: the list of object references to wait for
//   - timeoutMs: the maximum time in milliseconds to wait
//
// Returns:
//   - *WaitResult[T]: the wait result containing ready and unready objects
//   - error: any error encountered during the wait operation
func WaitWithTimeout[T any](refs []*ObjectRef[T], timeoutMs int64) (*WaitResult[T], error) {
	return Wait(refs, 1, timeoutMs, false)
}

// WaitWithFetchLocal waits for objects and fetches them locally.
//
// Parameters:
//   - refs: the list of object references to wait for
//   - numReturns: the number of objects that need to be available
//   - timeoutMs: the maximum time in milliseconds to wait (use -1 for infinite timeout)
//
// Returns:
//   - *WaitResult[T]: the wait result containing ready and unready objects
//   - error: any error encountered during the wait operation
func WaitWithFetchLocal[T any](refs []*ObjectRef[T], numReturns int, timeoutMs int64) (*WaitResult[T], error) {
	return Wait(refs, numReturns, timeoutMs, true)
}

// ============================================================================
// Helper Functions for ObjectRef Lifecycle Management
// ============================================================================

// setupObjectRefFinalizer sets up a finalizer for an ObjectRef to automatically
// release local references when GC runs.
//
// This is a shared helper to eliminate code duplication between call.go and object.go.
// The finalizer ensures that local references are released when the ObjectRef is GCed,
// preventing memory leaks in the object store.
//
// Parameters:
//   - ref: The ObjectRef to set finalizer on
//   - objectIDForFinalizer: The ObjectID to use for cleanup (captured to avoid corruption)
//   - logPrefix: Prefix for debug logging (e.g., "FINALIZER-call" or "FINALIZER")
func setupObjectRefFinalizer[T any](ref *ObjectRef[T], objectIDForFinalizer ids.ObjectID, logPrefix string) {
	runtime.SetFinalizer(ref, func(r *ObjectRef[T]) {
		fmt.Fprintf(os.Stderr, "[%s] ObjectRef finalizer called for objectID=%s\n", logPrefix, objectIDForFinalizer.Hex())

		// Acquire read lock on finalizerMu - this blocks if shutdown is running.
		// This guarantees shutdown is either not started or complete, never
		// in-progress, matching the previous finalizer ordering.
		finalizerMu.RLock()
		defer finalizerMu.RUnlock()

		if !r.released.CompareAndSwap(false, true) {
			return
		}
		// Double-check if shutdown has completed - if so, skip the release.
		// This check is safe because we hold the read lock.
		if shutdownComplete.Load() {
			return
		}
		// Do NOT call CGO here: Go runs finalizers on a GC-specialized goroutine
		// where a CGO call into the C++ CoreWorker object store can segfault. The
		// object ID is enqueued instead; a background worker performs the CGO
		// RemoveLocalReference outside the GC context.
		//
		// The enqueue is non-blocking: if the worker queue is full, the object ID
		// is dropped. Dropping only delays the reference release (the C++ object
		// store keeps the object alive via its reference count); it never blocks
		// the GC goroutine. A nil queue means the release worker is not running
		// (runtime not initialized, or already shut down), in which case there is
		// no local reference to remove.
		if releaseQueue != nil {
			select {
			case releaseQueue <- objectIDForFinalizer:
			default:
				// Queue full or worker unavailable; drop the release request.
			}
		}
	})
}

// releaseObjectRef releases the local reference held by an ObjectRef.
//
// This is a shared helper to eliminate code duplication between api.go.Release()
// and other release scenarios.
//
// Parameters:
//   - o: The ObjectRef to release
//
// Note: This function is idempotent - calling it multiple times has no additional effect.
func releaseObjectRef[T any](o *ObjectRef[T]) {
	logger := log.WithName("object-ref-release").V(1)

	if o.skipAddingLocalRef {
		logger.Info("skipping release due to skipAddingLocalRef", "objectID", o.objectID.Hex())
		return
	}

	if !o.released.CompareAndSwap(false, true) {
		logger.Info("release called on already-released ObjectRef", "objectID", o.objectID.Hex())
		return
	}

	logger.Info("releasing ObjectRef", "objectID", o.objectID.Hex())

	// Acquire read lock on finalizerMu - this blocks if shutdown is running
	// After acquiring the lock, we are guaranteed that shutdown is either:
	// 1. Not yet started (shutdownComplete=false)
	// 2. Already completed (shutdownComplete=true)
	// But never in-progress, which prevents the SIGSEGV race condition
	finalizerMu.RLock()
	defer finalizerMu.RUnlock()

	// Double-check if shutdown has completed - if so, skip RemoveLocalReference
	if shutdownComplete.Load() {
		return
	}

	// Copy objectID to a local variable to avoid potential memory corruption
	objectIDCopy := o.objectID

	handle, ok := tryGetHandle()
	if !ok || handle == nil {
		return // Silently ignore if runtime is not initialized
	}

	runtime := handle.Runtime()
	if runtime != nil && runtime.GetObjectStore() != nil {
		objectStore := runtime.GetObjectStore()
		_ = objectStore.RemoveLocalReference(&objectIDCopy)
	}
}

// createObjectRefWithFinalizer creates an ObjectRef for a return value and registers
// a finalizer for automatic cleanup of local references.
//
// **Important**: If the runtime is not available (e.g., during testing or in edge cases),
// this function returns an ObjectRef WITHOUT a finalizer. In such cases, the caller
// MUST explicitly call Release() to avoid memory leaks.
//
// This is a shared helper used by both Call() and Remote() to eliminate code duplication.
// The finalizer ensures that local references are released when the ObjectRef is GCed,
// preventing memory leaks in the object store.
//
// Parameters:
//   - returnID: The ObjectID of the return value
//   - objectType: The object type for deserialization (empty string if unknown)
//
// Returns:
//   - *ObjectRef[T]: The created ObjectRef (may not have finalizer if runtime unavailable)
//   - error: Any error encountered during setup
func createObjectRefWithFinalizer[T any](returnID ids.ObjectID, objectType string) (*ObjectRef[T], error) {
	ref := &ObjectRef[T]{
		objectID:           returnID,
		objectType:         objectType,
		skipAddingLocalRef: false,
		released:           atomic.Bool{},
	}

	// Register local reference and set finalizer
	handle, ok := tryGetHandle()
	if !ok || handle == nil {
		// Runtime not available - return ObjectRef without finalizer
		// The caller can still use Get() but may need to handle errors
		return ref, nil
	}

	rt := handle.Runtime()
	if rt == nil || rt.GetObjectStore() == nil {
		return ref, nil
	}

	objectStore := rt.GetObjectStore()
	_ = objectStore.AddLocalReference(&returnID)

	// Capture objectID in the finalizer closure to avoid accessing the ObjectRef
	// after it has been garbage collected.
	objectIDForFinalizer := returnID

	// Use shared helper to set finalizer
	setupObjectRefFinalizer(ref, objectIDForFinalizer, "FINALIZER-call")

	return ref, nil
}
