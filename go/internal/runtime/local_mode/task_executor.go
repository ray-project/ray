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

package local_mode

import (
	"fmt"
	"sync"

	"github.com/ray-project/ray/go/internal/runtime/objectstore"
	"github.com/ray-project/ray/go/pkg/ids"
	"github.com/ray-project/ray/go/pkg/runtime/function"
)

// LocalModeTaskExecutor implements task execution for local mode.
// Inspired by Java's LocalModeTaskExecutor.
//
// Design notes:
// 1. Uses ActorConcurrencyGroupManager to manage actor task execution
// 2. Handles both normal tasks and actor tasks
// 3. Integrates with LocalModeObjectStore for object storage
type LocalModeTaskExecutor struct {
	functionMgr              *function.FunctionManager
	actorConcurrencyGroupMgr *ActorConcurrencyGroupManager
	objectStore              *objectstore.LocalModeObjectStore
	actorContexts            sync.Map // map[ids.ActorID]*LocalActorContext
	currentActorContext      *LocalActorContext
	currentActorContextMu    sync.RWMutex
}

// LocalActorContext holds context information for an actor.
// Similar to Java's LocalModeTaskExecutor.LocalActorContext.
type LocalActorContext struct {
	workerID ids.UniqueID
}

// NewLocalActorContext creates a new LocalActorContext.
func NewLocalActorContext(workerID ids.UniqueID) *LocalActorContext {
	return &LocalActorContext{
		workerID: workerID,
	}
}

// GetWorkerID returns the worker ID of the actor.
func (c *LocalActorContext) GetWorkerID() ids.UniqueID {
	return c.workerID
}

// NewLocalModeTaskExecutor creates a new LocalModeTaskExecutor.
func NewLocalModeTaskExecutor(
	functionMgr *function.FunctionManager,
	actorConcurrencyGroupMgr *ActorConcurrencyGroupManager,
	objectStore *objectstore.LocalModeObjectStore,
) *LocalModeTaskExecutor {
	return &LocalModeTaskExecutor{
		functionMgr:              functionMgr,
		actorConcurrencyGroupMgr: actorConcurrencyGroupMgr,
		objectStore:              objectStore,
	}
}

// Execute executes a normal task and returns the results.
func (e *LocalModeTaskExecutor) Execute(
	functionDescriptor function.FunctionDescriptor,
	args []function.FunctionArg,
	numReturns int,
) ([]function.SerializedObject, error) {
	// Execute the function
	results, err := e.executeFunction(functionDescriptor, args, numReturns)
	if err != nil {
		return nil, err
	}
	return results, nil
}

// ExecuteActorTask executes an actor task and returns the results.
func (e *LocalModeTaskExecutor) ExecuteActorTask(
	actorID ids.ActorID,
	functionDescriptor function.FunctionDescriptor,
	args []function.FunctionArg,
	numReturns int,
) ([]function.SerializedObject, error) {
	// Get or create actor concurrency group
	group := e.actorConcurrencyGroupMgr.GetGroup(actorID)
	if group == nil {
		return nil, fmt.Errorf("actor not found: %s", actorID)
	}

	// Execute the task through the concurrency group
	var results []function.SerializedObject
	var execErr error

	done := make(chan struct{})
	group.Submit(func() {
		results, execErr = e.executeFunction(functionDescriptor, args, numReturns)
		close(done)
	})

	// Wait for execution to complete
	<-done

	return results, execErr
}

// resolveByRefArgs materializes pass-by-reference arguments from the in-memory
// object store into pass-by-value arguments. This mirrors the C++ local
// dependency resolution (LocalDependencyResolver in
// src/ray/core_worker/task_submission/dependency_resolver.cc) that inlines
// by-ref arguments before task execution: cluster mode never hands a by-ref
// FunctionArg to the Go worker, so function.DeserializeArgs must not see one in
// local mode either. Without this, large arguments (>100KB, which
// convertArgToFunctionArg passes by reference) fail deserialization and the
// task's return object is never produced.
//
// The returned slices alias the object store's stored arrays (GetRaw uses
// reference semantics) and stay owned by the store, so they must not be mutated
// or released here.
func (e *LocalModeTaskExecutor) resolveByRefArgs(args []function.FunctionArg) ([]function.FunctionArg, error) {
	resolved := make([]function.FunctionArg, len(args))
	for i, arg := range args {
		if arg.ObjectRef == nil {
			resolved[i] = arg
			continue
		}
		nativeObjects, err := e.objectStore.GetRaw(
			[]*ids.ObjectID{&arg.ObjectRef.ObjectID}, -1, "")
		if err != nil {
			return nil, fmt.Errorf("failed to resolve pass-by-reference argument %d: %w", i, err)
		}
		if len(nativeObjects) == 0 {
			return nil, fmt.Errorf("pass-by-reference argument %d object %s not found",
				i, arg.ObjectRef.ObjectID.Hex())
		}
		resolved[i] = function.NewFunctionArgByValue(nativeObjects[0].Data, nativeObjects[0].Metadata)
	}
	return resolved, nil
}

// executeFunction executes a function with the given arguments.
func (e *LocalModeTaskExecutor) executeFunction(
	functionDescriptor function.FunctionDescriptor,
	args []function.FunctionArg,
	numReturns int,
) ([]function.SerializedObject, error) {
	// Get the function from the manager
	goDesc, err := function.FromBaseFunctionDescriptor(functionDescriptor)
	if err != nil {
		return nil, fmt.Errorf("invalid function descriptor: %w", err)
	}

	rayFunc, err := e.functionMgr.GetFunction(goDesc)
	if err != nil {
		return nil, fmt.Errorf("failed to get function: %w", err)
	}

	// Materialize pass-by-reference arguments before execution (mirrors the
	// C++ LocalDependencyResolver) so DeserializeArgs only ever sees values.
	resolvedArgs, err := e.resolveByRefArgs(args)
	if err != nil {
		return nil, err
	}

	// Execute the function - it will handle its own serialization/deserialization
	results, err := rayFunc(resolvedArgs)
	if err != nil {
		return nil, fmt.Errorf("function execution failed: %w", err)
	}

	// Convert results to SerializedObject
	serializedResults := make([]function.SerializedObject, len(results))
	for i, result := range results {
		serializedResults[i] = function.SerializedObject{
			Data:     result.Data,
			Metadata: result.Metadata,
		}
	}

	return serializedResults, nil
}

// SetActorContext sets the current actor context.
func (e *LocalModeTaskExecutor) SetActorContext(workerID ids.UniqueID, actorContext *LocalActorContext) {
	e.currentActorContextMu.Lock()
	defer e.currentActorContextMu.Unlock()
	e.currentActorContext = actorContext
}

// GetActorContext returns the current actor context.
func (e *LocalModeTaskExecutor) GetActorContext() *LocalActorContext {
	e.currentActorContextMu.RLock()
	defer e.currentActorContextMu.RUnlock()
	return e.currentActorContext
}

// RegisterActorContext registers an actor context for the given actor ID.
func (e *LocalModeTaskExecutor) RegisterActorContext(actorID ids.ActorID, ctx *LocalActorContext) {
	e.actorContexts.Store(actorID, ctx)
}

// GetActorContextByID gets an actor context by actor ID.
func (e *LocalModeTaskExecutor) GetActorContextByID(actorID ids.ActorID) (*LocalActorContext, bool) {
	if ctx, ok := e.actorContexts.Load(actorID); ok {
		return ctx.(*LocalActorContext), true
	}
	return nil, false
}

// RemoveActorContext removes the actor context for the given actor ID.
// Called when an actor is killed or exits intentionally in local mode.
func (e *LocalModeTaskExecutor) RemoveActorContext(actorID ids.ActorID) {
	e.actorContexts.Delete(actorID)
}

// Compile-time check to ensure LocalModeTaskExecutor implements the expected interface
var _ interface {
	Execute(function.FunctionDescriptor, []function.FunctionArg, int) ([]function.SerializedObject, error)
	ExecuteActorTask(ids.ActorID, function.FunctionDescriptor, []function.FunctionArg, int) ([]function.SerializedObject, error)
} = (*LocalModeTaskExecutor)(nil)
