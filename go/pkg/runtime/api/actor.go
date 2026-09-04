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

	"github.com/ray-project/ray/go/pkg/errors"
)

// ============================================================================
// Actor Operations
// ============================================================================

// GetActor retrieves a named actor by its name.
//
// Parameters:
//   - name: the name of the actor
//
// Returns:
//   - *ActorHandleImpl[T]: a handle to the actor
//   - error: any error encountered during retrieval
func GetActor[T any](name string) (*ActorHandleImpl[T], error) {
	return GetActorWithNamespace[T](name, "")
}

// GetActorWithNamespace retrieves a named actor by its name and namespace.
//
// Parameters:
//   - name: the name of the actor
//   - namespace: the namespace of the actor (empty string for default namespace)
//
// Returns:
//   - *ActorHandleImpl[T]: a handle to the actor
//   - error: any error encountered during retrieval
func GetActorWithNamespace[T any](name string, namespace string) (*ActorHandleImpl[T], error) {
	handle, ok := tryGetHandle()
	if !ok || handle == nil {
		return nil, errors.ErrRuntimeNotInitialized
	}

	// Get the task submitter to retrieve the actor
	submitter, ok := tryGetTaskSubmitter()
	if !ok {
		// tryGetTaskSubmitter() returned false: runtime or handle is nil
		return nil, errors.ErrRuntimeNotInitialized
	}
	if submitter == nil {
		// Submitter itself is nil even though runtime is available.
		// This indicates an internal inconsistency.
		return nil, errors.NewRuntimeError("get_actor", "submitter_not_available")
	}

	// Validate input
	if name == "" {
		return nil, errors.NewRayInvalidArgumentException("actor name cannot be empty")
	}

	// Call submitter.GetActor to retrieve the actor from GCS
	actorHandle, err := submitter.GetActor(name, namespace)
	if err != nil {
		return nil, fmt.Errorf("failed to get actor '%s' in namespace '%s': %w", name, namespace, err)
	}
	
	if actorHandle == nil {
		return nil, fmt.Errorf("actor '%s' not found in namespace '%s'", name, namespace)
	}

	// Convert to typed ActorHandleImpl[T]
	// The submitter returns a NativeActorHandle, which we wrap in ActorHandleImpl
	actorID := actorHandle.ID()
	return NewActorHandleImpl[T](actorID), nil
}

// ExitActor exits the current actor.
// This function should only be called from within an actor.
// It works by throwing a special exception that signals the task executor to stop.
//
// Returns:
//   - error: always returns a RayIntentionalSystemExitException
//
// Usage example:
//
//	func (a *MyActor) ProcessAndExit(data []Data) {
//	    for _, d := range data {
//	        a.process(d)
//	    }
//	    // Exit after processing all data
//	    api.ExitActor()
//	}
func ExitActor() error {
	handle, ok := tryGetHandle()
	if !ok || handle == nil {
		return errors.ErrRuntimeNotInitialized
	}

	// Get the current actor ID to verify we're in an actor
	runtime := handle.Runtime()
	if runtime == nil {
		return fmt.Errorf("runtime instance not available")
	}

	workerCtx := runtime.WorkerContext()
	if workerCtx == nil {
		return fmt.Errorf("worker context not available")
	}

	actorID := workerCtx.GetCurrentActorID()
	if actorID.IsNil() {
		return fmt.Errorf("ExitActor can only be called from within an actor")
	}

	// Throw a special exception that signals intentional exit
	// This is consistent with Java's implementation
	// The task executor will catch this exception and stop the actor
	return errors.NewRayIntentionalSystemExitException(
		fmt.Sprintf("Actor %s is exiting.", actorID.Hex()),
	)
}

// GetRuntimeContext returns the current runtime context.
//
// Returns:
//   - *RuntimeContext: the current runtime context
//   - error: any error encountered during retrieval
func GetRuntimeContext() (*RuntimeContext, error) {
	handle, ok := tryGetHandle()
	if !ok || handle == nil {
		return nil, errors.ErrRuntimeNotInitialized
	}

	runtime := handle.Runtime()
	if runtime == nil {
		return nil, fmt.Errorf("runtime instance not available")
	}

	// Get worker context for context information
	workerCtx := runtime.WorkerContext()
	if workerCtx == nil {
		return nil, fmt.Errorf("worker context not available")
	}

	return NewRuntimeContext(
		workerCtx.GetCurrentJobID(),
		workerCtx.GetCurrentTaskID(),
		workerCtx.GetCurrentActorID(),
		workerCtx.GetNamespace(),
		workerCtx.GetSerializedRuntimeEnv(),
		workerCtx.GetCurrentNodeID(),
		runtime.IsLocalMode(),
	), nil
}
