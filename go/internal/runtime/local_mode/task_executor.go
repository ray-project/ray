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

	"github.com/ray-project/ray/go/pkg/ids"
	"github.com/ray-project/ray/go/pkg/runtime/function"
	"github.com/ray-project/ray/go/internal/runtime/objectstore"
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

	// Execute the function - it will handle its own serialization/deserialization
	results, err := rayFunc(args)
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

// Compile-time check to ensure LocalModeTaskExecutor implements the expected interface
var _ interface {
	Execute(function.FunctionDescriptor, []function.FunctionArg, int) ([]function.SerializedObject, error)
	ExecuteActorTask(ids.ActorID, function.FunctionDescriptor, []function.FunctionArg, int) ([]function.SerializedObject, error)
} = (*LocalModeTaskExecutor)(nil)
