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


// Package gcs provides the Go client for Ray Global Control Store (GCS).
package gcs

import (
	"sync"

	"github.com/ray-project/ray/go/pkg/ids"
	"github.com/ray-project/ray/go/proto"
)

// Global singleton GlobalStateAccessor.
var (
	stateAccessorInstance GlobalStateAccessor
	stateAccessorOnce     sync.Once
)

// GlobalStateAccessor is the synchronous GCS global-state accessor,
// corresponding to C++ ray::gcs::GlobalStateAccessor.
type GlobalStateAccessor interface {
	Close() error
	// Connect establishes the connection to GCS.
	Connect() (bool, error)

	// --- Job ---
	// GetAllJobInfo returns all job info. If skipSubmissionJobInfoField is set
	// the JobSubmissionInfo field is skipped; if skipIsRunningTasksField is set
	// the IsRunningTasks field is skipped.
	GetAllJobInfo(skipSubmissionJobInfoField, skipIsRunningTasksField bool) ([]*proto.JobTableData, error)
	// GetNextJobID returns the next job ID.
	GetNextJobID() (ids.JobID, error)

	// --- Node ---
	GetAllNodeInfo() (map[ids.NodeID]*proto.GcsNodeInfo, error)
	GetNode(nodeID ids.NodeID) (*proto.GcsNodeInfo, error)
	// GetDrainingNodes returns nodes being drained together with their drain deadline.
	GetDrainingNodes() (map[ids.NodeID]int64, error)
	// GetNodeToConnectForDriver returns the node a driver should connect to for
	// the given node IP address.
	GetNodeToConnectForDriver(nodeIPAddress string) (*proto.GcsNodeInfo, error)

	// --- Internal KV ---
	GetInternalKV(ns string, key string) ([]byte, error)

	// --- Resource ---
	GetAllAvailableResources() ([]*proto.AvailableResources, error)
	GetAllTotalResources() ([]*proto.TotalResources, error)
	GetAllResourceUsage() (*proto.ResourceUsageBatchData, error)

	// --- Actor ---
	// GetAllActorInfo returns all actor info. jobID filters by job ID
	// (nil means no filter); actorStateName filters by state (nil means no filter).
	GetAllActorInfo(jobID *ids.JobID, actorStateName *string) ([]*proto.ActorTableData, error)
	GetActorInfo(actorID ids.ActorID) (*proto.ActorTableData, error)

	// --- Worker ---
	GetAllWorkerInfo() ([]*proto.WorkerTableData, error)
	GetWorkerInfo(workerID ids.WorkerID) (*proto.WorkerTableData, error)
	AddWorkerInfo(data *proto.WorkerTableData) (bool, error)
	GetWorkerDebuggerPort(workerID ids.WorkerID) (uint32, error)
	UpdateWorkerDebuggerPort(workerID ids.WorkerID, debuggerPort uint32) (bool, error)
	// UpdateWorkerNumPausedThreads adjusts the number of paused threads by
	// numPausedThreadsDelta.
	UpdateWorkerNumPausedThreads(workerID ids.WorkerID, numPausedThreadsDelta int32) (bool, error)

	// --- Task ---
	GetAllTaskEvents() ([]*proto.TaskEvents, error)

	// --- Placement Group ---
	GetAllPlacementGroupInfo() ([]*proto.PlacementGroupTableData, error)
	GetPlacementGroupInfo(pgID ids.PlacementGroupID) (*proto.PlacementGroupTableData, error)
	GetPlacementGroupByName(name, rayNamespace string) (*proto.PlacementGroupTableData, error)

	// --- System ---
	GetSystemConfig() (string, error)
}

// SetGlobalStateAccessor sets the global GlobalStateAccessor instance. It is
// called by implementation packages (e.g., go/internal/gcs/native) during
// initialization. sync.Once ensures it is set only once.
func SetGlobalStateAccessor(accessor GlobalStateAccessor) {
	stateAccessorOnce.Do(func() {
		stateAccessorInstance = accessor
	})
}

// GetGlobalStateAccessor returns the global GlobalStateAccessor instance, or
// (nil, ErrNotImplemented) if it has not been initialized yet.
func GetGlobalStateAccessor() (GlobalStateAccessor, error) {
	if stateAccessorInstance == nil {
		return nil, ErrNotImplemented
	}
	return stateAccessorInstance, nil
}
