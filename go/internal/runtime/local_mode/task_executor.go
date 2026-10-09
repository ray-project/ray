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

	"github.com/ray-project/ray/go/internal/runtime/actor"
	"github.com/ray-project/ray/go/internal/runtime/localstore"
	"github.com/ray-project/ray/go/pkg/ids"
	"github.com/ray-project/ray/go/pkg/log"
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
	objectStore              *localstore.LocalModeObjectStore
	actorManager             *actor.ActorManager
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
	objectStore *localstore.LocalModeObjectStore,
) *LocalModeTaskExecutor {
	return &LocalModeTaskExecutor{
		functionMgr:              functionMgr,
		actorConcurrencyGroupMgr: actorConcurrencyGroupMgr,
		objectStore:              objectStore,
		actorManager:             actor.NewActorManager(),
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

// RegisterActorConstructor registers the constructor for an actor type, keyed
// by its "<init>" descriptor key. Local mode mirrors actor.ActorManager here:
// constructors live outside the FunctionManager (they return an actor instance,
// not serialized data), so they are stored separately and looked up by the
// actor creation task's descriptor.
func (e *LocalModeTaskExecutor) RegisterActorConstructor(
	desc *function.GoFunctionDescriptor,
	ctor func(args []function.FunctionArg) (interface{}, error),
) {
	e.actorManager.RegisterConstructor(desc.String(), ctor)
}

// ExecuteActorCreation constructs an actor instance by invoking the registered
// constructor and stores it under the actor ID. The actor instance is NOT
// serialized (it stays in-process), mirroring Java's LocalModeTaskExecutor
// where the constructor runs and the instance is kept in the actor context.
func (e *LocalModeTaskExecutor) ExecuteActorCreation(
	actorID ids.ActorID,
	desc *function.GoFunctionDescriptor,
	args []function.FunctionArg,
) error {
	instance, err := e.actorManager.ConstructActor(desc, args)
	if err != nil {
		// ConstructActor already wraps the error with the actor descriptor, so
		// returning it as-is avoids a duplicated "failed to construct actor"
		// prefix in the final message.
		return err
	}
	e.actorManager.SetInstance(actorID, instance)
	return nil
}

// ExecuteActorMethodByID invokes an actor method on the live instance for
// actorID using reflection, mirroring the shared actor.ActorManager.
// The method is resolved dynamically, so actor methods do not need to be
// registered in the FunctionManager (matching Java/CPP actor dispatch).
func (e *LocalModeTaskExecutor) ExecuteActorMethodByID(
	actorID ids.ActorID,
	functionDescriptor function.FunctionDescriptor,
	args []function.FunctionArg,
) ([]function.SerializedObject, error) {
	goDesc, err := function.FromBaseFunctionDescriptor(functionDescriptor)
	if err != nil {
		return nil, fmt.Errorf("invalid function descriptor: %w", err)
	}

	// Materialize pass-by-reference arguments (same resolution as normal tasks,
	// see resolveByRefArgs) so DeserializeArgs only ever sees values.
	resolvedArgs, err := e.resolveByRefArgs(args)
	if err != nil {
		return nil, err
	}

	// ExecuteActorMethod resolves the method from the live instance and removes
	// the instance on an intentional exit (mirroring the native path).
	return e.actorManager.ExecuteActorMethod(actorID, goDesc, resolvedArgs)
}

// GetActorInstance returns the live actor instance for actorID, if present.
// It lets the submitter check actor availability without reaching into the
// executor's internals.
func (e *LocalModeTaskExecutor) GetActorInstance(actorID ids.ActorID) (interface{}, bool) {
	return e.actorManager.GetInstance(actorID)
}

// RemoveActorInstance drops the live actor instance for actorID so a
// killed/exited actor is garbage collected and no longer resolvable.
func (e *LocalModeTaskExecutor) RemoveActorInstance(actorID ids.ActorID) {
	e.actorManager.RemoveInstance(actorID)
}

// systemConcurrencyGroupName is the system concurrency group that C++
// auto-creates on first use with concurrency 1 (see
// ConcurrencyGroupManager::GetExecutor). Local mode mirrors that behavior so
// WithConcurrencyGroup("_ray_system") routes to a dedicated executor.
//
// This value must stay in sync with the C++ default
// RayConfig::system_concurrency_group_name (src/ray/common/ray_config_def.h).
// The C++ config is the authority for the native path; change both together.
const systemConcurrencyGroupName = "_ray_system"

// ExecuteActorTaskInGroup executes an actor task and returns the results. The
// task is routed to the named concurrency group when groupName is non-empty.
// Semantics align with C++ ConcurrencyGroupManager::GetExecutor: the system
// group "_ray_system" is auto-created with concurrency 1 when not declared,
// while any other undeclared group fails fast instead of silently falling back
// to the default group (which would mask misconfiguration).
func (e *LocalModeTaskExecutor) ExecuteActorTaskInGroup(
	actorID ids.ActorID,
	groupName string,
	functionDescriptor function.FunctionDescriptor,
	args []function.FunctionArg,
) ([]function.SerializedObject, error) {
	var group *ActorConcurrencyGroup
	if groupName == "" {
		group = e.actorConcurrencyGroupMgr.GetGroup(actorID)
	} else {
		group = e.actorConcurrencyGroupMgr.GetNamedGroup(actorID, groupName)
		if group == nil {
			if groupName == systemConcurrencyGroupName {
				group = e.actorConcurrencyGroupMgr.GetOrCreateNamedGroup(actorID, groupName, 1)
			} else {
				return nil, fmt.Errorf("concurrency group %q not declared for actor %s",
					groupName, actorID.Hex())
			}
		}
	}
	if group == nil {
		log.Log.Info("actor concurrency group not found", "actorID", actorID.Hex())
		return nil, fmt.Errorf("actor not found: %s", actorID.Hex())
	}

	return e.executeActorMethodInGroup(group, actorID, functionDescriptor, args)
}

// executeActorMethodInGroup executes an actor method through the given
// concurrency group, submitting it to the group's queue and waiting for
// completion. The method is resolved dynamically from the live actor instance
// (mirroring actor.ActorManager), so it does not need to be registered in the
// FunctionManager.
func (e *LocalModeTaskExecutor) executeActorMethodInGroup(
	group *ActorConcurrencyGroup,
	actorID ids.ActorID,
	functionDescriptor function.FunctionDescriptor,
	args []function.FunctionArg,
) ([]function.SerializedObject, error) {
	// Execute the task through the concurrency group
	var results []function.SerializedObject
	var execErr error

	done := make(chan struct{})
	if !group.Submit(func() {
		results, execErr = e.ExecuteActorMethodByID(actorID, functionDescriptor, args)
		close(done)
	}) {
		// The group is shutting down (or was shut down concurrently); do not
		// wait on done, which would never close and would hang the caller.
		return nil, fmt.Errorf("concurrency group is shutting down for actor %s", group.actorID.Hex())
	}

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
func (e *LocalModeTaskExecutor) resolveByRefArgs(args []function.FunctionArg) ([]function.FunctionArg, error) {
	resolved := make([]function.FunctionArg, len(args))
	var byRefIDs []*ids.ObjectID
	var byRefArgIdx []int
	for i, arg := range args {
		if arg.ObjectRef == nil {
			resolved[i] = arg
			continue
		}
		byRefArgIdx = append(byRefArgIdx, i)
		byRefIDs = append(byRefIDs, &arg.ObjectRef.ObjectID)
	}
	if len(byRefIDs) == 0 {
		return resolved, nil
	}
	// Fetch all pass-by-reference arguments in one GetRaw call: the store's
	// waitForObjects waits for the whole batch under a single lock acquisition,
	// so per-argument calls would multiply that wait by the argument count.
	// GetRaw returns results in input order, so index j corresponds to
	// byRefArgIdx[j].
	nativeObjects, err := e.objectStore.GetRaw(byRefIDs, -1, "")
	if err != nil {
		return nil, fmt.Errorf("failed to resolve pass-by-reference arguments: %w", err)
	}
	if len(nativeObjects) != len(byRefIDs) {
		// getRawInternal skips objects that are no longer in the store and
		// returns no IDs, so the missing entries cannot be derived from the
		// returned slice directly. Check each by-ref ID individually to report
		// exactly which object(s) are missing (this path only triggers when an
		// object was deleted concurrently after waitForObjects returned).
		missing := make([]string, 0, len(byRefIDs)-len(nativeObjects))
		for _, id := range byRefIDs {
			if !e.objectStore.IsObjectReady(*id) {
				missing = append(missing, id.Hex())
			}
		}
		for _, obj := range nativeObjects {
			obj.Close()
		}
		return nil, fmt.Errorf("pass-by-reference resolution returned %d objects for %d IDs, missing: %v",
			len(nativeObjects), len(byRefIDs), missing)
	}
	for j, i := range byRefArgIdx {
		obj := nativeObjects[j]
		resolved[i] = function.NewFunctionArgByValue(obj.DataBytes(), obj.Metadata)
		obj.Close()
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
// Called when an actor exits intentionally in local mode.
func (e *LocalModeTaskExecutor) RemoveActorContext(actorID ids.ActorID) {
	e.actorContexts.Delete(actorID)
}

// Compile-time check to ensure LocalModeTaskExecutor implements the expected interface
var _ interface {
	Execute(function.FunctionDescriptor, []function.FunctionArg, int) ([]function.SerializedObject, error)
} = (*LocalModeTaskExecutor)(nil)
