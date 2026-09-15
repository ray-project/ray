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

// Package api provides the public API for Ray Go Runtime.
// This package is designed to be consistent with the Java Ray API.
package api

import (
	"fmt"
	"sync/atomic"

	"github.com/ray-project/ray/go/pkg/errors"
	"github.com/ray-project/ray/go/pkg/ids"
	"github.com/ray-project/ray/go/pkg/log"
	"github.com/ray-project/ray/go/pkg/runtime/function"
	"github.com/ray-project/ray/go/pkg/runtime/object"
)

var registeredLogger = log.WithName("function_registry")

// ============================================================================
// Re-export function registry types for public API
// ============================================================================

// FunctionEntry is re-exported from function package for public API.
// It represents a registered function with its descriptor.
type FunctionEntry = function.FunctionEntry

// ============================================================================
// Core API Types
// ============================================================================

// ObjectRef represents a reference to an object in the object store.
// Consistent with Java's io.ray.api.ObjectRef and C++ ObjectRef<T>.
//
// Type parameter T is the object type.
//
// Memory management:
// ObjectRef uses a finalizer to automatically release local references when GC runs.
// However, for high-throughput scenarios with many ObjectRefs, it's recommended to
// explicitly call Release() when the reference is no longer needed to avoid potential
// memory pressure from delayed finalizer execution.
type ObjectRef[T any] struct {
	// objectID is the unique identifier for the object.
	objectID ids.ObjectID
	// objectType is the type information for deserialization.
	objectType string
	// skipAddingLocalRef indicates whether to skip adding local reference.
	skipAddingLocalRef bool
	// rawData is the raw data of the object (used for memory store objects).
	// This is currently used by only the memory store objects.
	rawData []byte
	// released indicates whether Release() has been called.
	// Used to prevent double-release.
	released atomic.Bool
}

// ObjectID returns the object ID.
func (o *ObjectRef[T]) ObjectID() ids.ObjectID {
	return o.objectID
}

// ObjectType returns the object type.
func (o *ObjectRef[T]) ObjectType() string {
	return o.objectType
}

// String returns the string representation of this ObjectRef.
// Consistent with Java's ObjectRefImpl.toString().
func (o *ObjectRef[T]) String() string {
	return fmt.Sprintf("ObjectRef(%s)", o.objectID.Hex())
}

// Get fetches the object from the object store.
// This method blocks until the object is locally available.
//
// Returns:
//   - T: the object value
//   - error: any error encountered during the get operation
func (o *ObjectRef[T]) Get() (T, error) {
	return o.GetWithTimeout(-1)
}

// GetWithTimeout fetches the object from the object store with a timeout.
// This method blocks until the object is locally available or timeout occurs.
// Consistent with Java's ObjectRefImpl.get() which does NOT call removeLocalReference.
// The local reference is released only when the ObjectRef is GCed (via finalizer)
// or when Release() is explicitly called.
//
// Parameters:
//   - timeoutMs: the maximum time in milliseconds to wait (use -1 for infinite timeout)
//
// Returns:
//   - T: the object value
//   - error: any error encountered during the get operation, including timeout
func (o *ObjectRef[T]) GetWithTimeout(timeoutMs int64) (T, error) {
	handle, ok := tryGetHandle()
	if !ok || handle == nil {
		var zero T
		return zero, errors.ErrRuntimeNotInitialized
	}

	runtime := handle.Runtime()
	if runtime == nil {
		var zero T
		return zero, fmt.Errorf("runtime instance not available")
	}

	objectStore := runtime.GetObjectStore()
	if objectStore == nil {
		var zero T
		return zero, fmt.Errorf("object store not available")
	}

	// Copy objectID to a local variable to avoid potential memory corruption
	// from CGO calls or concurrent access. This ensures we use a stable copy
	// of the objectID even if the original ObjectRef memory is compromised.
	objectIDCopy := o.objectID

	// Get the object from object store with timeout and type information
	// This implements type safety closure similar to Java's Ray.get() -> ObjectStore.get(objectType)
	objectIDs := []*ids.ObjectID{&objectIDCopy}
	nativeObjects, err := objectStore.GetRaw(objectIDs, timeoutMs, o.objectType)
	if err != nil {
		var zero T
		return zero, fmt.Errorf("failed to get object: %w", err)
	}

	if len(nativeObjects) == 0 {
		var zero T
		return zero, fmt.Errorf("object not found")
	}

	// Note: Unlike Java, Go does NOT call RemoveLocalReference here.
	// The local reference is released only when:
	// 1. The ObjectRef is GCed (via finalizer), or
	// 2. Release() is explicitly called by the user
	// This is consistent with Java's ObjectRefImpl.get() behavior.

	// If the object's metadata encodes a Ray error type (e.g. WORKER_STARTUP_FAILED, WORKER_DIED,
	// TASK_EXECUTION_EXCEPTION), surface it as a readable exception instead of a generic
	// "failed to deserialize object" error. This runs before deserialization because an error
	// object does not carry a T-typed payload.
	if nativeObjects[0] == nil {
		var zero T
		return zero, fmt.Errorf("native object for object %s is nil", objectIDCopy.Hex())
	}
	if exc, ok := object.ErrorObjectFromNative(nativeObjects[0]); ok {
		var zero T
		return zero, fmt.Errorf("failed to get object %s: %w", objectIDCopy.Hex(), exc)
	}

	// Deserialize the object using the global serializer with type information
	// This ensures type safety throughout the deserialization chain
	ser := object.GetSerializer()

	// Create a variable of type T to receive the deserialized value
	var result T

	// Deserialize directly to the target type T
	// This avoids the issue of msgpack decoding small integers as int8/uint8
	if err := ser.DeserializeTo(nativeObjects[0], &result); err != nil {
		var zero T
		return zero, fmt.Errorf("failed to deserialize object: %w", err)
	}

	return result, nil
}

// NewObjectRef creates a new ObjectRef instance.
// This is a pure data constructor that does not interact with the runtime.
// The caller is responsible for registering local references when needed.
func NewObjectRef[T any](objectID ids.ObjectID, objectType string, skipAddingLocalRef bool) *ObjectRef[T] {
	return &ObjectRef[T]{
		objectID:           objectID,
		objectType:         objectType,
		skipAddingLocalRef: skipAddingLocalRef,
		released:           atomic.Bool{},
	}
}

// Release explicitly releases the local reference held by this ObjectRef.
// This method is idempotent - calling it multiple times has no additional effect.
//
// Memory management best practices:
//   - For short-lived ObjectRefs, relying on the finalizer is acceptable.
//   - For high-throughput scenarios (e.g., creating thousands of ObjectRefs per second),
//     explicitly call Release() when the reference is no longer needed to avoid
//     memory pressure from delayed finalizer execution.
//   - After calling Release(), the ObjectRef should not be used for Get() operations,
//     as the local reference count may be incorrect.
//
// Example usage:
//
//	ref, err := api.Put(myData)
//	if err != nil {
//	    // handle error
//	}
//	defer ref.Release() // Ensure reference is released
//	result, err := ref.Get()
func (o *ObjectRef[T]) Release() {
	// Use shared helper to release object reference
	releaseObjectRef(o)
}

// WaitResult represents the result of a Ray.wait call.
// It contains two lists: one containing the locally available objects,
// and one containing the rest.
// Consistent with Java's io.ray.api.WaitResult.
//
// Type parameter T is the object type.
type WaitResult[T any] struct {
	// ready is the list of locally available objects.
	ready []*ObjectRef[T]
	// unready is the list of objects that are not yet available.
	unready []*ObjectRef[T]
}

// Ready returns the list of locally available objects.
func (w *WaitResult[T]) Ready() []*ObjectRef[T] {
	return w.ready
}

// Unready returns the list of objects that are not yet available.
func (w *WaitResult[T]) Unready() []*ObjectRef[T] {
	return w.unready
}

// NewWaitResult creates a new WaitResult instance.
func NewWaitResult[T any](ready, unready []*ObjectRef[T]) *WaitResult[T] {
	return &WaitResult[T]{
		ready:   ready,
		unready: unready,
	}
}

// ActorHandle represents a handle to an actor.
// Consistent with Java's io.ray.api.BaseActorHandle.
type ActorHandle interface {
	// ID returns the actor ID.
	ID() ids.ActorID
}

// ============================================================================
// Runtime Context
// ============================================================================

// RuntimeContext provides access to runtime context information.
// Consistent with Java's io.ray.api.runtimecontext.RuntimeContext.
type RuntimeContext struct {
	// jobID is the current job ID.
	jobID ids.JobID
	// taskID is the current task ID.
	taskID ids.TaskID
	// actorID is the current actor ID (if running in an actor).
	actorID ids.ActorID
	// namespace is the current namespace.
	namespace string
	// runtimeEnv is the runtime environment.
	runtimeEnv string
	// nodeID is the current node ID.
	nodeID ids.NodeID
	// localMode indicates whether running in local mode.
	localMode bool
}

// JobID returns the current job ID.
func (r *RuntimeContext) JobID() ids.JobID {
	return r.jobID
}

// TaskID returns the current task ID.
func (r *RuntimeContext) TaskID() ids.TaskID {
	return r.taskID
}

// ActorID returns the current actor ID.
func (r *RuntimeContext) ActorID() ids.ActorID {
	return r.actorID
}

// Namespace returns the current namespace.
func (r *RuntimeContext) Namespace() string {
	return r.namespace
}

// RuntimeEnv returns the runtime environment.
func (r *RuntimeContext) RuntimeEnv() string {
	return r.runtimeEnv
}

// NodeID returns the current node ID.
func (r *RuntimeContext) NodeID() ids.NodeID {
	return r.nodeID
}

// IsLocalMode returns whether running in local mode.
func (r *RuntimeContext) IsLocalMode() bool {
	return r.localMode
}

// WasCurrentActorRestarted returns whether the current actor was restarted.
// Consistent with Java's RuntimeContext.wasCurrentActorRestarted()
//
// Returns:
//   - bool: true if the actor was restarted, false otherwise
func (r *RuntimeContext) WasCurrentActorRestarted() bool {
	// This requires support from the underlying runtime
	// For now, return false as a placeholder
	// TODO: Implement when runtime supports actor restart detection
	return false
}

// GetAllNodeInfo returns information about all nodes in the cluster.
// Consistent with Java's RuntimeContext.getAllNodeInfo()
//
// Returns:
//   - []NodeInfo: list of node information
func (r *RuntimeContext) GetAllNodeInfo() []NodeInfo {
	// This requires support from the underlying runtime
	// For now, return empty slice as a placeholder
	// TODO: Implement when runtime supports node info retrieval
	return []NodeInfo{}
}

// GetAllActorInfo returns information about all actors in the cluster.
// Consistent with Java's RuntimeContext.getAllActorInfo()
//
// Returns:
//   - []ActorInfo: list of actor information
func (r *RuntimeContext) GetAllActorInfo() []ActorInfo {
	// This requires support from the underlying runtime
	// For now, return empty slice as a placeholder
	// TODO: Implement when runtime supports actor info retrieval
	return []ActorInfo{}
}

// GetCurrentActorHandle returns the handle of the current actor.
// Consistent with Java's RuntimeContext.getCurrentActorHandle()
//
// Returns:
//   - ActorHandle: the current actor handle, or nil if not in an actor
func (r *RuntimeContext) GetCurrentActorHandle() ActorHandle {
	// This requires support from the underlying runtime
	// For now, return nil as a placeholder
	if r.actorID.IsNil() {
		return nil
	}
	// TODO: Implement when runtime supports actor handle retrieval
	return &actorHandleWrapper{actorID: r.actorID}
}

// GetGpuIds returns the GPU IDs available to the current worker.
// Consistent with Java's RuntimeContext.getGpuIds()
//
// Returns:
//   - []int64: list of GPU IDs
func (r *RuntimeContext) GetGpuIds() []int64 {
	// This requires support from the underlying runtime
	// For now, return empty slice as a placeholder
	// TODO: Implement when runtime supports GPU ID retrieval
	return []int64{}
}

// NewRuntimeContext creates a new RuntimeContext instance.
func NewRuntimeContext(jobID ids.JobID, taskID ids.TaskID, actorID ids.ActorID, namespace string, runtimeEnv string, nodeID ids.NodeID, localMode bool) *RuntimeContext {
	return &RuntimeContext{
		jobID:      jobID,
		taskID:     taskID,
		actorID:    actorID,
		namespace:  namespace,
		runtimeEnv: runtimeEnv,
		nodeID:     nodeID,
		localMode:  localMode,
	}
}

// NodeInfo represents node information.
// Consistent with Java's io.ray.api.runtimecontext.NodeInfo
type NodeInfo struct {
	NodeID                ids.NodeID
	NodeAddress           string
	NodeHostname          string
	NodeManagerPort       int
	ObjectStoreSocketName string
	RayletSocketName      string
	IsAlive               bool
	Resources             map[string]float64
	Labels                map[string]string
}

// ActorInfo represents actor information.
// Consistent with Java's io.ray.api.runtimecontext.ActorInfo
type ActorInfo struct {
	ActorID     ids.ActorID
	State       ActorState
	NumRestarts int64
	Address     Address
	Name        string
}

// ActorState represents actor state.
// Consistent with Java's io.ray.api.runtimecontext.ActorState
type ActorState int

const (
	ActorStateDependenciesUnready ActorState = iota
	ActorStatePendingCreation
	ActorStateAlive
	ActorStateRestarting
	ActorStateDead
)

// Address represents actor address.
// Consistent with Java's io.ray.api.runtimecontext.Address
type Address struct {
	NodeID ids.NodeID
	IP     string
	Port   int
}

// actorHandleWrapper is a simple wrapper for ActorHandle.
type actorHandleWrapper struct {
	actorID ids.ActorID
}

func (w *actorHandleWrapper) ID() ids.ActorID {
	return w.actorID
}

// String returns the string representation of ActorInfo.
func (a ActorInfo) String() string {
	return fmt.Sprintf("ActorInfo{ActorID=%s, State=%v, Name=%s, Address=%s}",
		a.ActorID.Hex(), a.State, a.Name, a.Address.String())
}

// String returns the string representation of Address.
func (a Address) String() string {
	return fmt.Sprintf("%s:%d", a.IP, a.Port)
}

// String returns the string representation of NodeInfo.
func (n NodeInfo) String() string {
	return fmt.Sprintf("NodeInfo{NodeID=%s, Address=%s, IsAlive=%v}",
		n.NodeID.Hex(), n.NodeAddress, n.IsAlive)
}

// ============================================================================
// Global Function Registry
// ============================================================================

// RegisterFunction registers a function for remote execution.
// This function should be called before starting the worker (before RunWorker()).
//
// Design notes:
//  1. This is the Go equivalent of Java's automatic function registration via
//     lambda serialization, but Go requires explicit registration.
//  2. The function descriptor is extracted from the function using reflection.
//  3. Functions registered here will be available for task execution in the worker.
//
// Parameters:
//   - fn: the function to register (must be a regular function, not a method)
//
// Returns:
//   - error: if the function is invalid or descriptor extraction fails
//
// Example usage:
//
//	func goAdd(x int, y int) int { return x + y }
//	if err := api.RegisterFunction(goAdd); err != nil {
//	    log.Fatal(err)
//	}
//	api.RunWorker() // Start worker with registered functions
func RegisterFunction(fn interface{}) error {
	if fn == nil {
		return fmt.Errorf("function cannot be nil")
	}
	// Delegate to function.Registry.Register
	return function.Registry.Register(fn)
}

// GetRegisteredFunctions returns all registered functions.
// This is called by go/internal/worker during worker startup to populate
// the FunctionManager.
//
// Returns:
//   - []FunctionEntry: list of registered function entries
//   - bool: true if any functions were registered
func GetRegisteredFunctions() ([]FunctionEntry, bool) {
	entries, hasFuncs := function.Registry.ListEntries()
	return entries, hasFuncs
}

// MarkRegistryReadonly marks the registry as readonly after worker startup.
// This prevents new functions from being registered after the worker has started.
func MarkRegistryReadonly() {
	function.Registry.MarkReadonly()
}

// IsRegistryReadonly returns true if the registry has been marked as read-only.
func IsRegistryReadonly() bool {
	return function.Registry.IsReadonly()
}

// RegisteredFunctionsCount returns the number of registered functions.
// This is useful for debugging and verification.
//
// Returns:
//   - int: number of functions registered via RegisterFunction()
func RegisteredFunctionsCount() int {
	return len(function.Registry.List())
}
