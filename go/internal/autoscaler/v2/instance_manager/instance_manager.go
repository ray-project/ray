// Copyright 2026 The Ray Authors.
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

package instance_manager

import (
	"fmt"

	"github.com/ray-project/ray/go/pkg/log"
	"github.com/ray-project/ray/go/proto"
)

// InstanceUpdatedSubscriber is the instance status update subscriber
// interface.
// Corresponds to Python instance_manager.InstanceUpdatedSubscriber:
// subscribers are called back when instance statuses change and may perform
// side effects (such as syncing the cloud provider, stopping Ray, installing
// Ray, monitoring resources).
type InstanceUpdatedSubscriber interface {
	// Notify is the instance status update event callback.
	// events is the list of events produced in this round and may be empty
	// (a no-change sync only).
	Notify(events []*proto.InstanceUpdateEvent)
}

// InstanceManager is the instance manager.
// Corresponds to Python instance_manager.InstanceManager (see
// InstanceManagerService in instance_manager.proto):
// it processes instance status updates, or creates new instances on upsert
// updates.
// Only the following statuses allow inserting a new instance:
//  1. ALLOCATED: an unmanaged instance not initialized by this manager, such
//     as the head node
//  2. QUEUED: a new instance queued waiting to be launched
//  3. TERMINATING: a leaked cloud instance that needs termination
//
// Difference from Python: Python asserts on invalid transitions/inserts
// (crashing the process); Go instead returns a reply with the
// UNKNOWN_ERRORS status code and skips persisting the whole update batch.
//
// Not thread safe; use it as a singleton.
type InstanceManager struct {
	instanceStorage *InstanceStorage
	subscribers     []InstanceUpdatedSubscriber
}

// NewInstanceManager creates an instance manager.
func NewInstanceManager(instanceStorage *InstanceStorage, instanceStatusUpdateSubscribers []InstanceUpdatedSubscriber) *InstanceManager {
	return &InstanceManager{
		instanceStorage: instanceStorage,
		subscribers:     instanceStatusUpdateSubscribers,
	}
}

// UpdateInstanceManagerState updates the instance manager state.
// Any failure (version mismatch/validation failure/storage write failure)
// leaves the storage untouched; the reply carries the latest storage version
// and the error message.
func (im *InstanceManager) UpdateInstanceManagerState(request *proto.UpdateInstanceManagerStateRequest) *proto.UpdateInstanceManagerStateReply {
	// Multiple updates of the same instance: the later one overrides the
	// earlier one (matching the Python dict building semantics).
	idsToUpdates := make(map[string]*proto.InstanceUpdateEvent, len(request.Updates))
	instanceIDs := make([]string, 0, len(request.Updates))
	for _, update := range request.Updates {
		if _, ok := idsToUpdates[update.InstanceId]; !ok {
			instanceIDs = append(instanceIDs, update.InstanceId)
		}
		idsToUpdates[update.InstanceId] = update
	}

	toUpdateInstances, version := im.instanceStorage.GetInstances(instanceIDs, nil)

	if request.ExpectedVersion >= 0 && request.ExpectedVersion != version {
		errStr := fmt.Sprintf("Version mismatch: expected: %d, actual: %d",
			request.ExpectedVersion, version)
		log.Log.Info(errStr)
		return getUpdateIMStateReply(proto.StatusCode_VERSION_MISMATCH, version, errStr)
	}

	// Process the instance status updates: update existing instances, insert
	// new ones.
	toUpsertInstances := make([]*proto.Instance, 0, len(instanceIDs))
	for _, instanceID := range instanceIDs {
		update := idsToUpdates[instanceID]
		var instance *proto.Instance
		var err error
		if existing, ok := toUpdateInstances[instanceID]; ok {
			instance, err = im.updateInstance(existing, update)
		} else {
			instance, err = im.createInstance(update)
		}
		if err != nil {
			// Validation failed; roll back the whole update batch (nothing has
			// been written to storage yet).
			log.Log.Error(err, "Failed to update instance manager state.")
			return getUpdateIMStateReply(proto.StatusCode_UNKNOWN_ERRORS, version, err.Error())
		}
		toUpsertInstances = append(toUpsertInstances, instance)
	}

	// Batch write to the instance storage.
	result := im.instanceStorage.BatchUpsertInstances(toUpsertInstances, &version)
	if !result.Success {
		if result.Version != version {
			errStr := fmt.Sprintf("Version mismatch: expected: %d, actual: %d",
				version, result.Version)
			log.Log.Info(errStr)
			return getUpdateIMStateReply(proto.StatusCode_VERSION_MISMATCH, result.Version, errStr)
		}
		errStr := "Failed to update instance storage."
		log.Log.Error(nil, errStr)
		return getUpdateIMStateReply(proto.StatusCode_UNKNOWN_ERRORS, result.Version, errStr)
	}

	// After a successful update, broadcast this round's events to all
	// subscribers.
	for _, subscriber := range im.subscribers {
		subscriber.Notify(request.Updates)
	}

	return getUpdateIMStateReply(proto.StatusCode_OK, result.Version, "")
}

// GetInstanceManagerState gets the instance manager state (all instances and
// the storage version).
func (im *InstanceManager) GetInstanceManagerState(request *proto.GetInstanceManagerStateRequest) *proto.GetInstanceManagerStateReply {
	instances, version := im.instanceStorage.GetInstances(nil, nil)
	state := &proto.InstanceManagerState{
		Version:   version,
		Instances: make([]*proto.Instance, 0, len(instances)),
	}
	for _, instance := range instances {
		state.Instances = append(state.Instances, instance)
	}
	return &proto.GetInstanceManagerStateReply{
		Status: &proto.Status{Code: proto.StatusCode_OK},
		State:  state,
	}
}

// getUpdateIMStateReply builds an update reply.
func getUpdateIMStateReply(statusCode proto.StatusCode, version int64, errorMessage string) *proto.UpdateInstanceManagerStateReply {
	reply := &proto.UpdateInstanceManagerStateReply{
		Status:  &proto.Status{Code: statusCode},
		Version: version,
	}
	if errorMessage != "" {
		reply.Status.Message = errorMessage
	}
	return reply
}

// applyUpdate applies the status specific field updates to the instance.
// Corresponds to Python InstanceManager._apply_update.
func applyUpdate(instance *proto.Instance, update *proto.InstanceUpdateEvent) error {
	switch update.NewInstanceStatus {
	case proto.Instance_ALLOCATED:
		if update.CloudInstanceId == nil {
			return fmt.Errorf("ALLOCATED update must have cloud_instance_id")
		}
		if update.NodeKind == nil ||
			(update.GetNodeKind() != proto.NodeKind_WORKER && update.GetNodeKind() != proto.NodeKind_HEAD) {
			return fmt.Errorf("ALLOCATED update must have node_kind as WORKER or HEAD")
		}
		if update.InstanceType == nil {
			return fmt.Errorf("ALLOCATED update must have instance_type")
		}
		instance.CloudInstanceId = update.CloudInstanceId
		instance.NodeKind = update.GetNodeKind()
		instance.InstanceType = update.GetInstanceType()
		instance.NodeId = update.RayNodeId
	case proto.Instance_RAY_RUNNING:
		if update.RayNodeId == nil {
			return fmt.Errorf("RAY_RUNNING update must have ray_node_id")
		}
		instance.NodeId = update.RayNodeId
	case proto.Instance_REQUESTED:
		if update.LaunchRequestId == nil {
			return fmt.Errorf("REQUESTED update must have launch_request_id")
		}
		if update.InstanceType == nil {
			return fmt.Errorf("REQUESTED update must have instance_type")
		}
		instance.LaunchRequestId = update.GetLaunchRequestId()
		instance.InstanceType = update.GetInstanceType()
	case proto.Instance_TERMINATING:
		if update.CloudInstanceId == nil {
			return fmt.Errorf("TERMINATING update must have cloud instance id")
		}
	}
	return nil
}

// createInstance creates a new instance from an update event.
// Corresponds to Python InstanceManager._create_instance.
func (im *InstanceManager) createInstance(update *proto.InstanceUpdateEvent) (*proto.Instance, error) {
	if !update.Upsert {
		return nil, fmt.Errorf("upsert must be true for creating new instance")
	}

	if update.NewInstanceStatus != proto.Instance_ALLOCATED &&
		update.NewInstanceStatus != proto.Instance_QUEUED &&
		update.NewInstanceStatus != proto.Instance_TERMINATING {
		return nil, fmt.Errorf("invalid status %s for new instance, must be one of [ALLOCATED, QUEUED, TERMINATING]",
			update.NewInstanceStatus.String())
	}

	instance := InstanceUtil.NewInstance(
		update.InstanceId,
		update.GetInstanceType(),
		update.NewInstanceStatus,
		update.Details,
	)

	log.Log.Info(InstanceUtil.GetLogStrForUpdate(instance, update))
	if err := applyUpdate(instance, update); err != nil {
		return nil, err
	}
	return instance, nil
}

// updateInstance updates an existing instance with an update event.
// Corresponds to Python InstanceManager._update_instance.
func (im *InstanceManager) updateInstance(instance *proto.Instance, update *proto.InstanceUpdateEvent) (*proto.Instance, error) {
	log.Log.Info(InstanceUtil.GetLogStrForUpdate(instance, update))
	if !InstanceUtil.SetStatus(instance, update.NewInstanceStatus, update.Details) {
		return nil, fmt.Errorf("invalid status transition from %s to %s",
			instance.Status.String(), update.NewInstanceStatus.String())
	}
	if err := applyUpdate(instance, update); err != nil {
		return nil, err
	}
	return instance, nil
}
