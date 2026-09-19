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

package native

import (
	"context"

	"github.com/ray-project/ray/go/internal/runtime/base"
	"github.com/ray-project/ray/go/pkg/ids"
	contract "github.com/ray-project/ray/go/pkg/runtime/contract"
	"github.com/ray-project/ray/go/pkg/runtime/object"
	"github.com/ray-project/ray/go/pkg/runtime/submitter"
	"github.com/ray-project/ray/go/proto"
)

// runtimeContextProvider implements RuntimeContext by delegating to Runtime interface.
//
// This struct provides access to runtime context information such as current job ID,
// task ID, actor ID, namespace, and node ID. Phase 1 implementation delegates to
// the Runtime interface. Phase 2 methods (GCS-related) are placeholders.
//
// Thread safety:
// - workerCtx is set once during initialization and read many times thereafter.
// - All fields are safe for concurrent use after initialization.
type runtimeContextProvider struct {
	runtime   base.Runtime
	workerCtx base.WorkerContext
}

// newRuntimeContextProvider creates a new runtimeContextProvider instance.
//
// Parameters:
//   - runtime: The Runtime interface to delegate to
//
// Returns:
//   - A pointer to the newly created runtimeContextProvider
func newRuntimeContextProvider(runtime base.Runtime) *runtimeContextProvider {
	return &runtimeContextProvider{
		runtime:   runtime,
		workerCtx: runtime.WorkerContext(), // Cache WorkerContext to avoid repeated calls
	}
}

// GetCurrentJobID returns the current job ID.
//
// This method delegates to WorkerContext to retrieve the ID of the
// current job.
//
// Returns:
//   - ids.JobID: The current job ID
func (p *runtimeContextProvider) GetCurrentJobID() ids.JobID {
	if p.workerCtx == nil {
		return ids.NilJobID()
	}
	return p.workerCtx.GetCurrentJobID()
}

// GetCurrentTaskID returns the current task ID.
//
// This method delegates to WorkerContext to retrieve the ID of the
// currently executing task.
//
// Returns:
//   - ids.TaskID: The current task ID
func (p *runtimeContextProvider) GetCurrentTaskID() ids.TaskID {
	if p.workerCtx == nil {
		return ids.NilTaskID()
	}
	return p.workerCtx.GetCurrentTaskID()
}

// GetCurrentActorID returns the current actor ID.
//
// This method delegates to WorkerContext to retrieve the ID of the
// current actor if running within an actor, otherwise returns a nil actor ID.
//
// Returns:
//   - ids.ActorID: The current actor ID, or nil if not in an actor
func (p *runtimeContextProvider) GetCurrentActorID() ids.ActorID {
	if p.workerCtx == nil {
		return ids.NilActorID()
	}
	return p.workerCtx.GetCurrentActorID()
}

// GetNamespace returns the current namespace.
//
// This method delegates to WorkerContext to retrieve the namespace
// that the current job is running in.
//
// Returns:
//   - string: The current namespace
func (p *runtimeContextProvider) GetNamespace() string {
	if p.workerCtx == nil {
		return ""
	}
	return p.workerCtx.GetNamespace()
}

// GetCurrentNodeID returns the current node ID.
//
// This method delegates to WorkerContext to retrieve the ID of the
// current node.
//
// Returns:
//   - ids.NodeID: The current node ID, or nil if not available
func (p *runtimeContextProvider) GetCurrentNodeID() ids.NodeID {
	if p.workerCtx == nil {
		return ids.NilNodeID()
	}
	return p.workerCtx.GetCurrentNodeID()
}

// GetSerializedRuntimeEnv returns the serialized runtime environment.
//
// This method delegates to WorkerContext to retrieve the JSON-serialized
// runtime environment configuration for the current job or task.
//
// Returns:
//   - string: The serialized runtime environment as a JSON string
func (p *runtimeContextProvider) GetSerializedRuntimeEnv() string {
	if p.workerCtx == nil {
		return ""
	}
	return p.workerCtx.GetSerializedRuntimeEnv()
}

// IsLocalMode returns true if running in local mode.
//
// This method delegates to the Runtime interface to check if the runtime
// is operating in local mode (single process) rather than cluster mode.
//
// Returns:
//   - bool: True if running in local mode, false otherwise
func (p *runtimeContextProvider) IsLocalMode() bool {
	return p.runtime.IsLocalMode()
}

// WasCurrentActorRestarted returns true if the current actor was restarted
// due to failure or other reasons.
//
// This method delegates to NativeRuntime.WasCurrentActorRestarted() to avoid
// duplicating the actor restart check logic.
//
// Returns:
//   - bool: True if the actor was restarted, false otherwise
func (p *runtimeContextProvider) WasCurrentActorRestarted() bool {
	// Delegate to NativeRuntime instead of duplicating logic
	nativeRuntime, ok := p.runtime.(*NativeRuntime)
	if !ok {
		logger.Info("Failed to cast runtime to NativeRuntime")
		return false
	}
	return nativeRuntime.WasCurrentActorRestarted()
}

// GetAllNodeInfo returns information about all nodes in the cluster.
//
// Phase 2: This method will query the GCS client to retrieve comprehensive
// information about all nodes currently in the Ray cluster.
//
// Returns:
//   - []contract.NodeInfo: Slice of NodeInfo for all nodes, or empty slice on error
func (p *runtimeContextProvider) GetAllNodeInfo() []contract.NodeInfo {
	// Cast to NativeRuntime to access GetGcsClient
	nativeRuntime, ok := p.runtime.(*NativeRuntime)
	if !ok {
		return []contract.NodeInfo{}
	}

	// Get GCS client
	gcsClient, err := nativeRuntime.GetGcsClient()
	if err != nil || gcsClient == nil {
		logger.Info("GCS client not initialized")
		return []contract.NodeInfo{}
	}

	// Get all nodes from GCS
	ctx := context.Background()
	nodesMap, err := gcsClient.GetAll(ctx, nil)
	if err != nil {
		logger.Error(err, "Failed to get all nodes from GCS")
		return []contract.NodeInfo{}
	}

	// Convert protobuf to NodeInfo slice
	result := make([]contract.NodeInfo, 0, len(nodesMap))
	for _, protoNode := range nodesMap {
		result = append(result, convertNodeInfo(protoNode))
	}

	return result
}

// convertNodeState converts protobuf GcsNodeInfo_GcsNodeState to contract.NodeState
func convertNodeState(state proto.GcsNodeInfo_GcsNodeState) contract.NodeState {
	switch state {
	case proto.GcsNodeInfo_ALIVE:
		return contract.NodeStateAlive
	case proto.GcsNodeInfo_DEAD:
		return contract.NodeStateDead
	default:
		return contract.NodeStateDead
	}
}

// convertNodeInfo converts protobuf GcsNodeInfo to contract.NodeInfo
func convertNodeInfo(protoNode *proto.GcsNodeInfo) contract.NodeInfo {
	// Convert NodeID from protobuf bytes
	var nodeID ids.NodeID
	if len(protoNode.NodeId) > 0 {
		// Use NodeIDFromBinary to convert from protobuf bytes
		id, err := ids.NodeIDFromBinary(protoNode.NodeId)
		if err == nil {
			nodeID = id
		}
	}

	return contract.NodeInfo{
		NodeID:                nodeID,
		NodeManagerAddress:    protoNode.NodeManagerAddress,
		NodeManagerPort:       int(protoNode.NodeManagerPort),
		ObjectManagerPort:     int(protoNode.ObjectManagerPort),
		ObjectStoreSocketName: protoNode.ObjectStoreSocketName,
		RayletSocketName:      protoNode.RayletSocketName,
		Resources:             protoNode.GetResourcesTotal(),
		State:                 convertNodeState(protoNode.State),
		ClusterName:           "", // Not available in protobuf
		Version:               "", // Not available in protobuf
	}
}

// convertActorState converts protobuf ActorTableData_ActorState to contract.ActorState
func convertActorState(state proto.ActorTableData_ActorState) contract.ActorState {
	switch state {
	case proto.ActorTableData_PENDING_CREATION:
		return contract.ActorStatePending
	case proto.ActorTableData_ALIVE:
		return contract.ActorStateAlive
	case proto.ActorTableData_DEAD:
		return contract.ActorStateDead
	case proto.ActorTableData_RESTARTING:
		return contract.ActorStateRestarting
	default:
		return contract.ActorStateDead
	}
}

// convertActorInfo converts protobuf ActorTableData to contract.ActorInfo
func convertActorInfo(protoActor *proto.ActorTableData) contract.ActorInfo {
	// Convert ActorID from protobuf bytes
	var actorID ids.ActorID
	if len(protoActor.ActorId) > 0 {
		id, err := ids.ActorIDFromBinary(protoActor.ActorId)
		if err == nil {
			actorID = id
		}
	}

	// Convert JobID from protobuf bytes
	var jobID ids.JobID
	if len(protoActor.JobId) > 0 {
		id, err := ids.JobIDFromBinary(protoActor.JobId)
		if err == nil {
			jobID = id
		}
	}

	// Convert OwnerWorkerID from protobuf bytes (using UniqueID)
	var ownerWorkerID ids.UniqueID
	if len(protoActor.ParentId) > 0 {
		id, err := ids.UniqueIDFromBinary(protoActor.ParentId)
		if err == nil {
			ownerWorkerID = id
		}
	}

	// Extract owner IP and port from OwnerAddress
	var ownerIPAddress string
	var ownerPort int
	if protoActor.OwnerAddress != nil {
		ownerIPAddress = protoActor.OwnerAddress.IpAddress
		ownerPort = int(protoActor.OwnerAddress.Port)
	}

	// Convert ActorHandleID - not directly available in protobuf, use empty
	var actorHandleID ids.UniqueID

	// Convert resources
	resources := protoActor.RequiredResources
	if resources == nil {
		resources = make(map[string]float64)
	}

	// Extract address from Address struct
	var address string
	if protoActor.Address != nil {
		address = protoActor.Address.IpAddress
	}

	return contract.ActorInfo{
		ActorID:        actorID,
		JobID:          jobID,
		OwnerWorkerID:  ownerWorkerID,
		OwnerIPAddress: ownerIPAddress,
		OwnerPort:      ownerPort,
		ActorHandleID:  actorHandleID,
		Name:           protoActor.Name,
		Namespace:      protoActor.RayNamespace,
		MaxRestarts:    int(protoActor.MaxRestarts),
		NumRestarts:    int(protoActor.NumRestarts),
		State:          convertActorState(protoActor.State),
		Address:        address,
		IsDetached:     protoActor.IsDetached,
		StartTime:      int64(protoActor.StartTime),
		EndTime:        int64(protoActor.EndTime),
		Resources:      resources,
	}
}

// GetAllActorInfo returns information about all actors in the cluster.
//
// Phase 2: This method will query the GCS client to retrieve comprehensive
// information about all actors currently registered in the Ray cluster.
//
// Returns:
//   - []contract.ActorInfo: Slice of ActorInfo for all actors, or empty slice on error
func (p *runtimeContextProvider) GetAllActorInfo() []contract.ActorInfo {
	// Cast to NativeRuntime to access GetGcsClient
	nativeRuntime, ok := p.runtime.(*NativeRuntime)
	if !ok {
		return []contract.ActorInfo{}
	}

	// Get GCS client
	gcsClient, err := nativeRuntime.GetGcsClient()
	if err != nil || gcsClient == nil {
		logger.Info("GCS client not initialized")
		return []contract.ActorInfo{}
	}

	// Get all actors from GCS
	ctx := context.Background()
	actors, err := gcsClient.ListActors(ctx, nil)
	if err != nil {
		logger.Error(err, "Failed to get all actors from GCS")
		return []contract.ActorInfo{}
	}

	// Convert protobuf to ActorInfo slice
	result := make([]contract.ActorInfo, 0, len(actors))
	for _, protoActor := range actors {
		result = append(result, convertActorInfo(protoActor))
	}

	return result
}

// GetCurrentActorHandle returns the handle of the current actor
//
// This method retrieves the actor handle for the current actor
// by creating a NativeActorHandle with the current actor ID.
//
// Returns:
//   - submitter.ActorHandle: Actor handle if in actor context, nil otherwise
func (p *runtimeContextProvider) GetCurrentActorHandle() submitter.ActorHandle {
	// Get current actor ID from cached WorkerContext
	if p.workerCtx == nil {
		return nil
	}
	actorID := p.workerCtx.GetCurrentActorID()
	if actorID.IsNil() {
		// Not in actor context
		return nil
	}

	// Create NativeActorHandle with the current actor ID
	// We don't need to query GCS for this - just need the actor ID
	return &object.NativeActorHandle{
		ActorID:  actorID,
		Language: object.LanguageGo,
	}
}

// GetGpuIds returns the IDs of GPUs allocated to the current worker
//
// This method delegates to NativeRuntime.GetGpuIds() to retrieve the IDs
// of GPU resources allocated to the current execution context.
//
// Returns:
//   - []string: List of GPU device IDs (e.g., ["0", "2", "4"])
func (p *runtimeContextProvider) GetGpuIds() []string {
	// Delegate to NativeRuntime.GetGpuIds()
	nativeRuntime, ok := p.runtime.(*NativeRuntime)
	if !ok {
		logger.Info("Failed to cast runtime to NativeRuntime")
		return []string{}
	}

	return nativeRuntime.GetGpuIds()
}
