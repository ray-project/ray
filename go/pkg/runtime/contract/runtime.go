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

// Package contract defines the core interfaces and types for Ray Go Runtime.
// This package serves as the abstraction layer between public API and internal implementations.
package contract

import (
	"github.com/ray-project/ray/go/pkg/ids"
	"github.com/ray-project/ray/go/pkg/runtime/function"
	"github.com/ray-project/ray/go/pkg/runtime/object"
	"github.com/ray-project/ray/go/pkg/runtime/submitter"
)

// Runtime defines the core interface for Ray runtime.
//
// Design notes:
// 1. The interface is kept minimal, containing only lifecycle management methods.
// 2. Business methods like Put/Get/Call are not included; they are handled by the API layer.
// 3. Corresponds to the core methods of Java's AbstractRayRuntime.
type Runtime interface {
	// Start starts the runtime.
	// Corresponds to Java: RayNativeRuntime.start()
	Start() error

	// Shutdown shuts down the runtime and releases resources.
	// Corresponds to Java: RayNativeRuntime.shutdown()
	Shutdown() error

	// Run runs the task execution loop (only in Worker mode).
	// Corresponds to Java: RayNativeRuntime.run()
	Run() error

	// IsInitialized returns whether the runtime has been initialized.
	IsInitialized() bool

	// WorkerContext returns the worker context accessor.
	// Corresponds to Java: RayNativeRuntime.getWorkerContext()
	WorkerContext() WorkerContext

	// GetRunMode returns the current run mode (cluster or local).
	// Corresponds to Java: RayNativeRuntime.isLocalMode()
	GetRunMode() RunMode

	// IsLocalMode returns true if running in local mode.
	// Note: This method is not part of WorkerContext interface.
	IsLocalMode() bool

	// RuntimeContext methods (Phase 2 & 3)
	// WasCurrentActorRestarted returns true if the current actor was restarted.
	WasCurrentActorRestarted() bool
	// GetAllNodeInfo returns information about all nodes in the cluster.
	GetAllNodeInfo() []NodeInfo
	// GetAllActorInfo returns information about all actors in the cluster.
	GetAllActorInfo() []ActorInfo
	// GetGpuIds returns the IDs of GPUs allocated to the current worker.
	GetGpuIds() []string
	// GetCurrentActorHandle returns the handle of the current actor.
	GetCurrentActorHandle() submitter.ActorHandle

	// GetObjectStore returns the internal ObjectStore instance.
	// This method is intended for internal use by RuntimeHandle only.
	// External code should access ObjectStore through RuntimeHandle.ObjectStore().
	// The ObjectStore is created during Start() and cleared during Shutdown().
	GetObjectStore() object.ObjectStore

	// GetTaskSubmitter returns the task submitter for submitting tasks.
	// This method is intended for internal use by the API layer.
	GetTaskSubmitter() submitter.TaskSubmitter

	// GetFunctionManager returns the function manager for registering user functions.
	// This method is intended for internal use during worker initialization.
	GetFunctionManager() function.Manager
}

// WorkerContext defines the interface for accessing worker context information.
// This interface is used internally to access current worker state.
type WorkerContext interface {
	// GetCurrentWorkerId returns the current worker ID.
	GetCurrentWorkerId() ids.UniqueID
	// GetCurrentJobID returns the current job ID.
	GetCurrentJobID() ids.JobID
	// GetCurrentActorID returns the current actor ID (empty if not in an actor).
	GetCurrentActorID() ids.ActorID
	// GetCurrentTaskType returns the current task type.
	GetCurrentTaskType() TaskType
	// GetCurrentTaskID returns the current task ID.
	GetCurrentTaskID() ids.TaskID
	// GetRpcAddress returns the RPC address as bytes.
	GetRpcAddress() []byte
	// GetSerializedRuntimeEnv returns the serialized runtime environment.
	GetSerializedRuntimeEnv() string
	// GetNamespace returns the current namespace.
	GetNamespace() string
	// GetCurrentNodeID returns the current node ID.
	GetCurrentNodeID() ids.NodeID
}

// TaskType represents the type of task.
// Consistent with Java's io.ray.runtime.generated.Common.TaskType.
type TaskType int32

const (
	// TaskTypeNormal represents a normal task.
	TaskTypeNormal TaskType = 0
	// TaskTypeActorCreation represents an actor creation task.
	TaskTypeActorCreation TaskType = 1
	// TaskTypeActorTask represents an actor task.
	TaskTypeActorTask TaskType = 2
)

// RuntimeHandle provides access to the runtime instance and its services.
// This interface should be used by the API layer instead of directly
// depending on internal implementations.
type RuntimeHandle interface {
	IsRuntimeHandle() // marker method for type safety

	// Runtime returns the underlying runtime implementation.
	// Use this to access worker context, object store, and other runtime features.
	Runtime() Runtime
}
