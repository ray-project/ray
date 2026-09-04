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

// Package local_mode provides local mode runtime implementation for Ray Go.
// Local mode allows running Ray programs without a cluster, useful for development and testing.
package local_mode

import (
	"sync"

	"github.com/ray-project/ray/go/internal/runtime/base"
	"github.com/ray-project/ray/go/internal/runtime/serializer"
	"github.com/ray-project/ray/go/pkg/ids"
)

// LocalModeWorkerContext implements WorkerContext for local mode.
// It uses goroutine-local storage to track current task and worker information.
//
// Design notes:
// 1. Uses sync.Map for goroutine-local storage (keyed by goroutine ID)
// 2. Tracks current task, worker ID, actor ID, job ID, etc.
// 3. Compatible with Java's LocalModeWorkerContext behavior
type LocalModeWorkerContext struct {
	// goroutineLocalData maps goroutine IDs to their context data
	goroutineLocalData sync.Map
	// defaultWorkerID is used for non-actor tasks
	defaultWorkerID ids.UniqueID
	// defaultJobID is the default job ID for local mode
	defaultJobID ids.JobID
	// defaultNodeID is the default node ID for local mode
	defaultNodeID ids.NodeID
	// rpcAddress is the RPC address (empty in local mode)
	rpcAddress []byte
	// namespace is the current namespace
	namespace string
	// serializedRuntimeEnv is the serialized runtime environment
	serializedRuntimeEnv string
}

// goroutineContext holds all context data for a single goroutine
type goroutineContext struct {
	mu            sync.RWMutex
	currentTaskID ids.TaskID
	currentTask   interface{} // Can be TaskSpec or similar
	currentWorker ids.UniqueID
	currentActor  ids.ActorID
	taskType      base.TaskType
}

// NewLocalModeWorkerContext creates a new LocalModeWorkerContext instance.
func NewLocalModeWorkerContext() *LocalModeWorkerContext {
	return &LocalModeWorkerContext{
		defaultWorkerID:      ids.NewUniqueID(),
		defaultJobID:         ids.NewJobID(),
		defaultNodeID:        ids.NewNodeID(),
		rpcAddress:           []byte{},
		namespace:            "",
		serializedRuntimeEnv: "",
	}
}

// getGoroutineContext returns the context for the current goroutine.
// Creates a new context if none exists.
func (c *LocalModeWorkerContext) getGoroutineContext() *goroutineContext {
	goroutineID := serializer.GetGoroutineID()
	if ctx, ok := c.goroutineLocalData.Load(goroutineID); ok {
		return ctx.(*goroutineContext)
	}
	
	// Create new context for this goroutine
	newCtx := &goroutineContext{
		currentWorker: c.defaultWorkerID,
		currentActor:  ids.NilActorID(),
		currentTaskID: ids.NilTaskID(),
		taskType:      base.TaskTypeNormal,
	}
	c.goroutineLocalData.Store(goroutineID, newCtx)
	return newCtx
}

// SetCurrentTask sets the current task for the calling goroutine.
// task can be nil to clear the current task.
func (c *LocalModeWorkerContext) SetCurrentTask(task interface{}) {
	ctx := c.getGoroutineContext()
	ctx.mu.Lock()
	defer ctx.mu.Unlock()
	ctx.currentTask = task
}

// SetCurrentWorkerId sets the current worker ID for the calling goroutine.
func (c *LocalModeWorkerContext) SetCurrentWorkerId(workerID ids.UniqueID) {
	ctx := c.getGoroutineContext()
	ctx.mu.Lock()
	defer ctx.mu.Unlock()
	ctx.currentWorker = workerID
}

// SetCurrentActorId sets the current actor ID for the calling goroutine.
func (c *LocalModeWorkerContext) SetCurrentActorId(actorID ids.ActorID) {
	ctx := c.getGoroutineContext()
	ctx.mu.Lock()
	defer ctx.mu.Unlock()
	ctx.currentActor = actorID
}

// SetCurrentTaskId sets the current task ID for the calling goroutine.
func (c *LocalModeWorkerContext) SetCurrentTaskId(taskID ids.TaskID) {
	ctx := c.getGoroutineContext()
	ctx.mu.Lock()
	defer ctx.mu.Unlock()
	ctx.currentTaskID = taskID
}

// SetCurrentTaskType sets the current task type for the calling goroutine.
func (c *LocalModeWorkerContext) SetCurrentTaskType(taskType base.TaskType) {
	ctx := c.getGoroutineContext()
	ctx.mu.Lock()
	defer ctx.mu.Unlock()
	ctx.taskType = taskType
}

// GetCurrentWorkerId returns the ID of the current worker.
func (c *LocalModeWorkerContext) GetCurrentWorkerId() ids.UniqueID {
	ctx := c.getGoroutineContext()
	ctx.mu.RLock()
	defer ctx.mu.RUnlock()
	return ctx.currentWorker
}

// GetCurrentJobID returns the ID of the current job.
func (c *LocalModeWorkerContext) GetCurrentJobID() ids.JobID {
	return c.defaultJobID
}

// GetCurrentActorID returns the ID of the current actor.
// Returns empty ActorID if not executing in an actor.
func (c *LocalModeWorkerContext) GetCurrentActorID() ids.ActorID {
	ctx := c.getGoroutineContext()
	ctx.mu.RLock()
	defer ctx.mu.RUnlock()
	return ctx.currentActor
}

// GetCurrentTaskType returns the type of the current task.
func (c *LocalModeWorkerContext) GetCurrentTaskType() base.TaskType {
	ctx := c.getGoroutineContext()
	ctx.mu.RLock()
	defer ctx.mu.RUnlock()
	return ctx.taskType
}

// GetCurrentTaskID returns the ID of the current task.
func (c *LocalModeWorkerContext) GetCurrentTaskID() ids.TaskID {
	ctx := c.getGoroutineContext()
	ctx.mu.RLock()
	defer ctx.mu.RUnlock()
	return ctx.currentTaskID
}

// GetRpcAddress returns the RPC address bytes of the current worker.
// In local mode, this returns an empty slice.
func (c *LocalModeWorkerContext) GetRpcAddress() []byte {
	return c.rpcAddress
}

// GetSerializedRuntimeEnv returns the serialized runtime environment.
func (c *LocalModeWorkerContext) GetSerializedRuntimeEnv() string {
	return c.serializedRuntimeEnv
}

// GetNamespace returns the current namespace.
func (c *LocalModeWorkerContext) GetNamespace() string {
	return c.namespace
}

// GetCurrentNodeID returns the ID of the current node.
// In local mode, this returns a default node ID.
func (c *LocalModeWorkerContext) GetCurrentNodeID() ids.NodeID {
	return c.defaultNodeID
}

// Compile-time check to ensure LocalModeWorkerContext implements WorkerContext
var _ base.WorkerContext = (*LocalModeWorkerContext)(nil)
