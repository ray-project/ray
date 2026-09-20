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
	"sync"
	"sync/atomic"

	rayerrors "github.com/ray-project/ray/go/internal/errors"
	"github.com/ray-project/ray/go/internal/runtime/base"
	"github.com/ray-project/ray/go/internal/runtime/objectstore"
	"github.com/ray-project/ray/go/pkg/ids"
	"github.com/ray-project/ray/go/pkg/runtime/contract"
	"github.com/ray-project/ray/go/pkg/runtime/function"
	"github.com/ray-project/ray/go/pkg/runtime/object"
	"github.com/ray-project/ray/go/pkg/runtime/submitter"
)

// LocalModeRuntime implements Runtime for local mode execution.
// Inspired by Java's RayDevRuntime.
//
// Design notes:
// 1. Implements contract.Runtime interface
// 2. Creates LocalModeObjectStore, LocalModeTaskSubmitter, LocalModeWorkerContext
// 3. Run() is a no-op (local mode doesn't have blocking task loop)
// 4. Thread-safe shutdown with proper resource cleanup
type LocalModeRuntime struct {
	opts          base.InitializeOptions
	initialized   atomic.Bool
	objectStore   *objectstore.LocalModeObjectStore
	taskSubmitter *LocalModeTaskSubmitter
	workerContext *LocalModeWorkerContext
	functionMgr   *function.FunctionManager

	shutdownLock sync.RWMutex
	taskExecutor *LocalModeTaskExecutor

	// Cached values
	runMode contract.RunMode
}

// NewLocalModeRuntime creates a new LocalModeRuntime instance.
func NewLocalModeRuntime(opts base.InitializeOptions) (*LocalModeRuntime, error) {
	store := objectstore.NewLocalModeObjectStore()
	return &LocalModeRuntime{
		opts:          opts,
		objectStore:   store,
		functionMgr:   function.NewFunctionManager(nil),
		workerContext: NewLocalModeWorkerContext(),
		runMode:       contract.RunModeLocal,
	}, nil
}

// Start starts the local mode runtime.
func (lr *LocalModeRuntime) Start() error {
	if !lr.initialized.CompareAndSwap(false, true) {
		return rayerrors.ErrRuntimeAlreadyInitialized
	}

	lr.shutdownLock.Lock()
	defer lr.shutdownLock.Unlock()

	// Create task executor
	lr.taskExecutor = NewLocalModeTaskExecutor(
		lr.functionMgr,
		NewActorConcurrencyGroupManager(),
		lr.objectStore,
	)

	// Create task submitter with executor
	lr.taskSubmitter = NewLocalModeTaskSubmitter(
		lr.objectStore,
		lr.workerContext,
		lr.taskExecutor,
		lr.functionMgr,
	)

	// Register object put callback to trigger waiting tasks
	lr.objectStore.AddObjectPutCallback(func(oid ids.ObjectID) {
		if lr.taskSubmitter != nil {
			lr.taskSubmitter.onObjectPut(oid)
		}
	})

	return nil
}

// Shutdown shuts down the local mode runtime and releases resources.
func (lr *LocalModeRuntime) Shutdown() error {
	lr.shutdownLock.Lock()
	defer lr.shutdownLock.Unlock()

	if lr.taskSubmitter != nil {
		lr.taskSubmitter.Shutdown()
	}

	if lr.taskExecutor != nil {
		// Clear actor contexts
	}

	// Clear object store
	if lr.objectStore != nil {
		lr.objectStore = nil
	}

	lr.initialized.Store(false)
	return nil
}

// Run is a no-op in local mode (tasks are executed asynchronously).
// Local mode doesn't have a blocking task execution loop.
func (lr *LocalModeRuntime) Run() error {
	// Local mode doesn't have a blocking task execution loop
	// Tasks are executed asynchronously when submitted
	return nil
}

// IsInitialized returns whether the runtime has been initialized.
func (lr *LocalModeRuntime) IsInitialized() bool {
	return lr.initialized.Load()
}

// WorkerContext returns the worker context accessor.
func (lr *LocalModeRuntime) WorkerContext() contract.WorkerContext {
	return lr.workerContext
}

// GetRunMode returns the current run mode (local mode).
func (lr *LocalModeRuntime) GetRunMode() contract.RunMode {
	return lr.runMode
}

// IsLocalMode returns true if running in local mode.
func (lr *LocalModeRuntime) IsLocalMode() bool {
	return true
}

// WasCurrentActorRestarted returns true if the current actor was restarted.
// In local mode, this always returns false as actors don't restart.
func (lr *LocalModeRuntime) WasCurrentActorRestarted() bool {
	return false
}

// GetAllNodeInfo returns information about all nodes in the cluster.
// In local mode, returns a single dummy node.
func (lr *LocalModeRuntime) GetAllNodeInfo() []contract.NodeInfo {
	// Return a single dummy node for local mode
	return []contract.NodeInfo{
		{
			NodeID:             lr.workerContext.GetCurrentNodeID(),
			NodeManagerAddress: "127.0.0.1",
			NodeManagerPort:    0,
			ObjectManagerPort:  0,
			State:              contract.NodeStateAlive,
		},
	}
}

// GetAllActorInfo returns information about all actors in the cluster.
// In local mode, returns empty slice (no cluster-wide actor tracking).
func (lr *LocalModeRuntime) GetAllActorInfo() []contract.ActorInfo {
	return []contract.ActorInfo{}
}

// GetGpuIds returns the IDs of GPUs allocated to the current worker.
// In local mode, returns empty slice (no GPU allocation).
func (lr *LocalModeRuntime) GetGpuIds() []string {
	return []string{}
}

// GetCurrentActorHandle returns the handle of the current actor.
// In local mode, returns nil (not implemented).
func (lr *LocalModeRuntime) GetCurrentActorHandle() submitter.ActorHandle {
	return nil
}

// GetObjectStore returns the internal ObjectStore instance.
func (lr *LocalModeRuntime) GetObjectStore() object.ObjectStore {
	return lr.objectStore
}

// GetTaskSubmitter returns the task submitter for submitting tasks.
func (lr *LocalModeRuntime) GetTaskSubmitter() submitter.TaskSubmitter {
	return lr.taskSubmitter
}

// GetFunctionManager returns the function manager for registering user functions.
// This method is intended for internal use during worker initialization.
func (lr *LocalModeRuntime) GetFunctionManager() function.Manager {
	return lr.functionMgr
}

// Compile-time check to ensure LocalModeRuntime implements Runtime
var _ contract.Runtime = (*LocalModeRuntime)(nil)
