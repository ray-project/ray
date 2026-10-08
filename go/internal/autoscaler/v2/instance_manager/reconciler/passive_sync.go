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

// Passive sync subtasks: align the instance statuses with the external state
// such as the cloud provider/Ray cluster.
// Corresponds to the _handle_cloud_instance_allocation through
// _handle_ray_stop_failed section of Python reconciler.py.
package reconciler

import (
	"encoding/hex"
	"fmt"
	"sort"

	"github.com/ray-project/ray/go/internal/autoscaler"
	"github.com/ray-project/ray/go/internal/autoscaler/v2/instance_manager"
	"github.com/ray-project/ray/go/internal/autoscaler/v2/instance_manager/subscribers"
	"github.com/ray-project/ray/go/pkg/log"
	"github.com/ray-project/ray/go/proto"
)

// handleCloudInstanceAllocation handles cloud instance allocation.
// Corresponds to Python Reconciler._handle_cloud_instance_allocation:
// REQUESTED -> ALLOCATED when an unassigned cloud instance of the same type
// exists; REQUESTED -> ALLOCATION_FAILED when the cloud provider reported a
// launch failure.
func (r *Reconciler) handleCloudInstanceAllocation(
	nonTerminatedCloudInstances map[string]autoscaler.CloudInstance,
	cloudProviderErrors []error,
) {
	imInstances, version, err := r.getImInstances()
	if err != nil {
		return
	}
	updates := make([]*proto.InstanceUpdateEvent, 0)

	// Collect the REQUESTED instances carrying a launch request.
	instancesWithLaunchRequests := make([]*proto.Instance, 0)
	for _, instance := range imInstances {
		if instance.Status != proto.Instance_REQUESTED {
			continue
		}
		if instance.LaunchRequestId == "" {
			// Python asserts here; Go defensively logs and skips.
			log.Log.Error(fmt.Errorf("instance %s in REQUESTED status should have launch_request_id set",
				instance.InstanceId), "")
			continue
		}
		instancesWithLaunchRequests = append(instancesWithLaunchRequests, instance)
	}

	// Instances with an assigned cloud instance (excluding
	// TERMINATED/ALLOCATION_FAILED).
	assignedCloudInstanceIds := make(map[string]bool)
	for _, instance := range imInstances {
		if id := instance.GetCloudInstanceId(); id != "" &&
			instance.Status != proto.Instance_TERMINATED &&
			instance.Status != proto.Instance_ALLOCATION_FAILED {
			assignedCloudInstanceIds[id] = true
		}
	}

	// Launch failure errors, indexed by request ID.
	launchErrors := make(map[string]*autoscaler.LaunchNodeError)
	for _, e := range cloudProviderErrors {
		if launchError, ok := e.(*autoscaler.LaunchNodeError); ok {
			launchErrors[launchError.RequestId] = launchError
		}
	}

	// Unassigned cloud instances, grouped by node type.
	unassignedCloudInstancesByType := make(map[string][]autoscaler.CloudInstance)
	for cloudInstanceId, cloudInstance := range nonTerminatedCloudInstances {
		if !assignedCloudInstanceIds[cloudInstanceId] {
			unassignedCloudInstancesByType[cloudInstance.NodeType] =
				append(unassignedCloudInstancesByType[cloudInstance.NodeType], cloudInstance)
		}
	}

	// Sort from the earliest to the latest request time (matching Python
	// sorting by the REQUESTED status time list).
	sort.SliceStable(instancesWithLaunchRequests, func(i, j int) bool {
		return lessStatusTransitionTimes(
			instance_manager.InstanceUtil.GetStatusTransitionTimesNs(instancesWithLaunchRequests[i], proto.Instance_REQUESTED),
			instance_manager.InstanceUtil.GetStatusTransitionTimesNs(instancesWithLaunchRequests[j], proto.Instance_REQUESTED),
		)
	})

	// Try to allocate or mark allocation failed, instance by instance.
	for _, instance := range instancesWithLaunchRequests {
		updateEvent := tryResolvePendingAllocation(instance, unassignedCloudInstancesByType, launchErrors)
		if updateEvent == nil {
			continue
		}
		updates = append(updates, updateEvent)
	}

	r.updateInstanceManager(version, updates)
}

// tryResolvePendingAllocation assigns a cloud instance to a single instance or
// marks the allocation failed.
// Corresponds to Python Reconciler._try_resolve_pending_allocation.
func tryResolvePendingAllocation(
	imInstance *proto.Instance,
	unassignedCloudInstancesByType map[string][]autoscaler.CloudInstance,
	launchErrors map[string]*autoscaler.LaunchNodeError,
) *proto.InstanceUpdateEvent {
	// Try to assign an unassigned cloud instance of the same type (taken from
	// the end of the list, matching Python's pop).
	var unassignedCloudInstance *autoscaler.CloudInstance
	if unassigned := unassignedCloudInstancesByType[imInstance.InstanceType]; len(unassigned) > 0 {
		unassignedCloudInstance = &unassigned[len(unassigned)-1]
		unassignedCloudInstancesByType[imInstance.InstanceType] = unassigned[:len(unassigned)-1]
	}

	if unassignedCloudInstance != nil {
		cloudInstanceId := unassignedCloudInstance.CloudInstanceId
		nodeKind := unassignedCloudInstance.NodeKind
		instanceType := unassignedCloudInstance.NodeType
		return &proto.InstanceUpdateEvent{
			InstanceId:        imInstance.InstanceId,
			NewInstanceStatus: proto.Instance_ALLOCATED,
			CloudInstanceId:   &cloudInstanceId,
			NodeKind:          &nodeKind,
			InstanceType:      &instanceType,
			Details: fmt.Sprintf("allocated unassigned cloud instance %s",
				unassignedCloudInstance.CloudInstanceId),
		}
	}

	// Mark the allocation failed when there is a launch failure error.
	if launchError := launchErrors[imInstance.LaunchRequestId]; launchError != nil &&
		launchError.NodeType == imInstance.InstanceType {
		return &proto.InstanceUpdateEvent{
			InstanceId:        imInstance.InstanceId,
			NewInstanceStatus: proto.Instance_ALLOCATION_FAILED,
			Details:           fmt.Sprintf("launch failed with %s", launchError.Error()),
		}
	}
	// No update.
	return nil
}

// handleCloudInstanceTerminated handles terminated cloud instances.
// Corresponds to Python Reconciler._handle_cloud_instance_terminated:
// when a cloud instance disappears from the non-terminated list,
// * -> TERMINATED.
func (r *Reconciler) handleCloudInstanceTerminated(
	nonTerminatedCloudInstances map[string]autoscaler.CloudInstance,
) {
	updates := make([]*proto.InstanceUpdateEvent, 0)
	instances, version, err := r.getImInstances()
	if err != nil {
		return
	}

	// Instances with an assigned, non-terminated cloud instance.
	nonTerminatedInstancesWithCloudInstance := make(map[string]*proto.Instance)
	for _, instance := range instances {
		if cloudInstanceId := instance.GetCloudInstanceId(); cloudInstanceId != "" &&
			instance.Status != proto.Instance_TERMINATED {
			nonTerminatedInstancesWithCloudInstance[cloudInstanceId] = instance
		}
	}

	for cloudInstanceId, instance := range nonTerminatedInstancesWithCloudInstance {
		if _, running := nonTerminatedCloudInstances[cloudInstanceId]; running {
			// The cloud instance is still running.
			continue
		}

		// The cloud instance has terminated.
		updates = append(updates, &proto.InstanceUpdateEvent{
			InstanceId:        instance.InstanceId,
			NewInstanceStatus: proto.Instance_TERMINATED,
			Details:           fmt.Sprintf("cloud instance %s no longer found", cloudInstanceId),
		})
	}

	r.updateInstanceManager(version, updates)
}

// handleCloudInstanceTerminationErrors handles cloud instance termination
// failures.
// Corresponds to Python Reconciler._handle_cloud_instance_termination_errors:
// TERMINATING -> TERMINATION_FAILED when the instance's cloud instance fails
// to terminate; the next reconcile round retries the termination.
func (r *Reconciler) handleCloudInstanceTerminationErrors(cloudProviderErrors []error) {
	instances, version, err := r.getImInstances()
	if err != nil {
		return
	}
	updates := make([]*proto.InstanceUpdateEvent, 0)

	// Termination failure errors, indexed by cloud instance ID.
	terminationErrors := make(map[string]*autoscaler.TerminateNodeError)
	for _, e := range cloudProviderErrors {
		if terminateError, ok := e.(*autoscaler.TerminateNodeError); ok {
			terminationErrors[terminateError.CloudInstanceId] = terminateError
		}
	}

	// TERMINATING instances, indexed by cloud instance ID.
	terminatingInstancesByCloudInstanceId := make(map[string]*proto.Instance)
	for _, instance := range instances {
		if instance.Status == proto.Instance_TERMINATING {
			terminatingInstancesByCloudInstanceId[instance.GetCloudInstanceId()] = instance
		}
	}

	// Sort by cloud instance ID to keep the update order deterministic.
	failedCloudInstanceIds := make([]string, 0, len(terminationErrors))
	for cloudInstanceId := range terminationErrors {
		failedCloudInstanceIds = append(failedCloudInstanceIds, cloudInstanceId)
	}
	sort.Strings(failedCloudInstanceIds)

	for _, cloudInstanceId := range failedCloudInstanceIds {
		instance := terminatingInstancesByCloudInstanceId[cloudInstanceId]
		if instance == nil {
			// The instance is no longer in TERMINATING.
			continue
		}

		updates = append(updates, &proto.InstanceUpdateEvent{
			InstanceId:        instance.InstanceId,
			NewInstanceStatus: proto.Instance_TERMINATION_FAILED,
			Details:           fmt.Sprintf("termination failed: %s", terminationErrors[cloudInstanceId].Error()),
		})
	}

	r.updateInstanceManager(version, updates)
}

// handleRayStatusTransition handles Ray node status changes.
// Corresponds to Python Reconciler._handle_ray_status_transition:
// a Ray node joins the cluster -> RAY_RUNNING; stopped -> RAY_STOPPED;
// draining -> RAY_STOPPING.
func (r *Reconciler) handleRayStatusTransition(
	rayNodes []*proto.NodeState,
	asConfig *instance_manager.AutoscalingConfig,
) {
	instances, version, err := r.getImInstances()
	if err != nil {
		return
	}
	updates := make([]*proto.InstanceUpdateEvent, 0)

	imInstancesByCloudInstanceId := make(map[string]*proto.Instance)
	imInstancesByRayNodeId := make(map[string]*proto.Instance)
	for _, instance := range instances {
		if cloudInstanceId := instance.GetCloudInstanceId(); cloudInstanceId != "" &&
			instance.Status != proto.Instance_TERMINATED &&
			instance.Status != proto.Instance_ALLOCATION_FAILED {
			imInstancesByCloudInstanceId[cloudInstanceId] = instance
		}
		if nodeId := instance.GetNodeId(); nodeId != "" {
			imInstancesByRayNodeId[nodeId] = instance
		}
	}

	for _, rayNode := range rayNodes {
		rayNodeId := hex.EncodeToString(rayNode.NodeId)
		var imInstance *proto.Instance
		if instance, ok := imInstancesByRayNodeId[rayNodeId]; ok {
			imInstance = instance
		} else if asConfig.GetProvider() == instance_manager.ProviderReadOnly {
			// The read-only provider uses the node ID as the cloud instance ID.
			imInstance = imInstancesByCloudInstanceId[rayNodeId]
		} else if rayNode.InstanceId != "" {
			imInstance = imInstancesByCloudInstanceId[rayNode.InstanceId]
		} else {
			// Only happens to a Ray node not managed by the autoscaler.
			log.Log.Info("Ray node has no instance id. This only happens to a ray node not managed by autoscaler.",
				"ray_node_id", rayNodeId)
			continue
		}

		if imInstance == nil {
			// Python asserts here; by this point all cloud instances and Ray
			// nodes have been synced, so a Ray node with a cloud instance ID
			// should not fail to find a matching instance in the IM.
			log.Log.Error(fmt.Errorf("ray node %s has no matching instance with cloud instance id=%s",
				rayNodeId, rayNode.InstanceId), "")
			continue
		}

		reconciledImStatus, err := reconciledIMStatusFromRayStatus(rayNode.Status, imInstance.Status)
		if err != nil {
			log.Log.Error(err, "")
			continue
		}

		if reconciledImStatus != imInstance.Status {
			rayNodeIdCopy := rayNodeId
			instanceType := imInstance.InstanceType
			updates = append(updates, &proto.InstanceUpdateEvent{
				InstanceId:        imInstance.InstanceId,
				NewInstanceStatus: reconciledImStatus,
				Details:           fmt.Sprintf("ray node %s is %s", rayNodeId, rayNode.Status.String()),
				RayNodeId:         &rayNodeIdCopy,
				InstanceType:      &instanceType,
			})
		}
	}

	r.updateInstanceManager(version, updates)
}

// reconciledIMStatusFromRayStatus computes the status an instance should be in
// from the Ray node status.
// Corresponds to Python Reconciler._reconciled_im_status_from_ray_status:
// keep the status unchanged when the instance already is at, or has transitioned
// beyond, the target status.
func reconciledIMStatusFromRayStatus(
	rayStatus proto.NodeStatus,
	curImStatus proto.Instance_InstanceStatus,
) (proto.Instance_InstanceStatus, error) {
	var reconciledImStatus proto.Instance_InstanceStatus
	switch rayStatus {
	case proto.NodeStatus_RUNNING, proto.NodeStatus_IDLE:
		reconciledImStatus = proto.Instance_RAY_RUNNING
	case proto.NodeStatus_DEAD:
		reconciledImStatus = proto.Instance_RAY_STOPPED
	case proto.NodeStatus_DRAINING:
		reconciledImStatus = proto.Instance_RAY_STOPPING
	default:
		return proto.Instance_UNKNOWN, fmt.Errorf("unknown ray status: %s", rayStatus.String())
	}

	if curImStatus == reconciledImStatus ||
		instance_manager.InstanceUtil.CanReach(reconciledImStatus, curImStatus) {
		// Already at, or transitioned beyond, the target status; nothing to
		// reconcile.
		return curImStatus, nil
	}

	return reconciledImStatus, nil
}

// handleRayInstallFailed handles Ray installation failures.
// Corresponds to Python Reconciler._handle_ray_install_failed:
// RAY_INSTALLING -> RAY_INSTALL_FAILED when the instance has an install error.
func (r *Reconciler) handleRayInstallFailed(rayInstallErrors []error) {
	instances, version, err := r.getImInstances()
	if err != nil {
		return
	}
	updates := make([]*proto.InstanceUpdateEvent, 0)

	// RAY_INSTALLING instances.
	instancesWithRayInstalling := make(map[string]*proto.Instance)
	for _, instance := range instances {
		if instance.Status == proto.Instance_RAY_INSTALLING {
			instancesWithRayInstalling[instance.InstanceId] = instance
		}
	}

	// Install errors, indexed by instance ID.
	installErrors := make(map[string]subscribers.RayInstallError)
	for _, e := range rayInstallErrors {
		if installError, ok := e.(subscribers.RayInstallError); ok {
			installErrors[installError.ImInstanceId] = installError
		}
	}

	// Sort by instance ID to keep the update order deterministic.
	instanceIds := make([]string, 0, len(instancesWithRayInstalling))
	for instanceId := range instancesWithRayInstalling {
		instanceIds = append(instanceIds, instanceId)
	}
	sort.Strings(instanceIds)

	for _, instanceId := range instanceIds {
		if installError, ok := installErrors[instanceId]; ok {
			updates = append(updates, &proto.InstanceUpdateEvent{
				InstanceId:        instanceId,
				NewInstanceStatus: proto.Instance_RAY_INSTALL_FAILED,
				Details:           fmt.Sprintf("failed to install ray with errors: %s", installError.Details),
			})
		}
	}

	r.updateInstanceManager(version, updates)
}

// handleRayStopFailed handles Ray stop failures.
// Corresponds to Python Reconciler._handle_ray_stop_failed:
// RAY_STOP_REQUESTED -> RAY_RUNNING when stopping Ray failed.
func (r *Reconciler) handleRayStopFailed(rayStopErrors []error, rayNodes []*proto.NodeState) {
	instances, version, err := r.getImInstances()
	if err != nil {
		return
	}
	updates := make([]*proto.InstanceUpdateEvent, 0)

	// Stop failure errors, indexed by instance ID.
	rayStopErrorsByInstanceId := make(map[string]subscribers.RayStopError)
	for _, e := range rayStopErrors {
		if stopError, ok := e.(subscribers.RayStopError); ok {
			rayStopErrorsByInstanceId[stopError.ImInstanceId] = stopError
		}
	}

	rayNodesByRayNodeId := make(map[string]*proto.NodeState)
	for _, node := range rayNodes {
		rayNodesByRayNodeId[hex.EncodeToString(node.NodeId)] = node
	}

	for _, instance := range instances {
		if instance.Status != proto.Instance_RAY_STOP_REQUESTED {
			continue
		}
		if _, ok := rayStopErrorsByInstanceId[instance.InstanceId]; !ok {
			continue
		}

		// Python asserts a RUNNING/IDLE Ray node exists; Go validates
		// defensively.
		rayNode := rayNodesByRayNodeId[instance.GetNodeId()]
		if rayNode == nil ||
			(rayNode.Status != proto.NodeStatus_RUNNING && rayNode.Status != proto.NodeStatus_IDLE) {
			log.Log.Error(fmt.Errorf("there should be a running ray node for instance %s with ray stop requested failed",
				instance.InstanceId), "")
			continue
		}

		rayNodeId := instance.GetNodeId()
		updates = append(updates, &proto.InstanceUpdateEvent{
			InstanceId:        instance.InstanceId,
			NewInstanceStatus: proto.Instance_RAY_RUNNING,
			Details:           "failed to stop/drain ray",
			RayNodeId:         &rayNodeId,
		})
	}

	r.updateInstanceManager(version, updates)
}

// handleExtraCloudInstances handles unmanaged extra cloud instances.
// Corresponds to Python Reconciler._handle_extra_cloud_instances:
// creates new ALLOCATED instances for the instances present in the cloud
// provider/Ray cluster but not managed by the IM.
func (r *Reconciler) handleExtraCloudInstances(
	nonTerminatedCloudInstances map[string]autoscaler.CloudInstance,
	rayNodes []*proto.NodeState,
) {
	r.handleExtraCloudInstancesFromRayNodes(rayNodes)
	r.handleExtraCloudInstancesFromCloudProvider(nonTerminatedCloudInstances)
}

// handleExtraCloudInstancesFromCloudProvider creates IM instances for the
// unmanaged cloud instances reported by the cloud provider.
// Corresponds to Python
// Reconciler._handle_extra_cloud_instances_from_cloud_provider.
func (r *Reconciler) handleExtraCloudInstancesFromCloudProvider(
	nonTerminatedCloudInstances map[string]autoscaler.CloudInstance,
) {
	updates := make([]*proto.InstanceUpdateEvent, 0)

	instances, version, err := r.getImInstances()
	if err != nil {
		return
	}
	cloudInstanceIdsManagedByIm := make(map[string]bool)
	for _, instance := range instances {
		if cloudInstanceId := instance.GetCloudInstanceId(); cloudInstanceId != "" &&
			instance.Status != proto.Instance_TERMINATED &&
			instance.Status != proto.Instance_ALLOCATION_FAILED {
			cloudInstanceIdsManagedByIm[cloudInstanceId] = true
		}
	}

	// Sort by cloud instance ID to keep the update order deterministic.
	cloudInstanceIds := make([]string, 0, len(nonTerminatedCloudInstances))
	for cloudInstanceId := range nonTerminatedCloudInstances {
		cloudInstanceIds = append(cloudInstanceIds, cloudInstanceId)
	}
	sort.Strings(cloudInstanceIds)

	for _, cloudInstanceId := range cloudInstanceIds {
		if cloudInstanceIdsManagedByIm[cloudInstanceId] {
			continue
		}
		cloudInstance := nonTerminatedCloudInstances[cloudInstanceId]
		nodeKind := cloudInstance.NodeKind
		instanceType := cloudInstance.NodeType
		cloudInstanceIdCopy := cloudInstanceId
		updates = append(updates, &proto.InstanceUpdateEvent{
			InstanceId:        instance_manager.InstanceUtil.RandomInstanceID(),
			CloudInstanceId:   &cloudInstanceIdCopy,
			NewInstanceStatus: proto.Instance_ALLOCATED,
			NodeKind:          &nodeKind,
			InstanceType:      &instanceType,
			Details: fmt.Sprintf("allocated unmanaged cloud instance :%s (%s) from cloud provider",
				cloudInstanceId, cloudInstance.NodeKind.String()),
			Upsert: true,
		})
	}
	r.updateInstanceManager(version, updates)
}

// handleExtraCloudInstancesFromRayNodes creates IM instances for the unmanaged
// Ray nodes reported by Ray.
// Corresponds to Python
// Reconciler._handle_extra_cloud_instances_from_ray_nodes.
func (r *Reconciler) handleExtraCloudInstancesFromRayNodes(rayNodes []*proto.NodeState) {
	updates := make([]*proto.InstanceUpdateEvent, 0)

	instances, version, err := r.getImInstances()
	if err != nil {
		return
	}
	cloudInstanceIdsManagedByIm := make(map[string]bool)
	for _, instance := range instances {
		if cloudInstanceId := instance.GetCloudInstanceId(); cloudInstanceId != "" &&
			instance.GetNodeId() == "" &&
			instance.Status != proto.Instance_TERMINATED &&
			instance.Status != proto.Instance_ALLOCATION_FAILED {
			cloudInstanceIdsManagedByIm[cloudInstanceId] = true
		}
	}
	rayNodeIdsManagedByIm := make(map[string]bool)
	for _, instance := range instances {
		if nodeId := instance.GetNodeId(); nodeId != "" {
			rayNodeIdsManagedByIm[nodeId] = true
		}
	}

	for _, rayNode := range rayNodes {
		if rayNode.InstanceId == "" {
			continue
		}

		rayNodeId := hex.EncodeToString(rayNode.NodeId)
		if rayNodeIdsManagedByIm[rayNodeId] {
			continue
		}

		cloudInstanceId := rayNode.InstanceId
		if cloudInstanceIdsManagedByIm[cloudInstanceId] {
			continue
		}

		nodeKind := proto.NodeKind_WORKER
		if autoscaler.IsHeadNode(rayNode) {
			nodeKind = proto.NodeKind_HEAD
		}
		rayNodeIdCopy := rayNodeId
		cloudInstanceIdCopy := cloudInstanceId
		instanceType := rayNode.RayNodeTypeName
		updates = append(updates, &proto.InstanceUpdateEvent{
			InstanceId:        instance_manager.InstanceUtil.RandomInstanceID(),
			CloudInstanceId:   &cloudInstanceIdCopy,
			NewInstanceStatus: proto.Instance_ALLOCATED,
			NodeKind:          &nodeKind,
			RayNodeId:         &rayNodeIdCopy,
			InstanceType:      &instanceType,
			Details:           fmt.Sprintf("allocated unmanaged worker cloud instance from ray node: %s", rayNodeId),
			Upsert:            true,
		})
	}

	r.updateInstanceManager(version, updates)
}

// lessStatusTransitionTimes compares two status transition timestamp lists
// (lexicographically).
// Corresponds to Python's list comparison semantics with the time list as the
// key.
func lessStatusTransitionTimes(a, b []int64) bool {
	for i := 0; i < len(a) && i < len(b); i++ {
		if a[i] != b[i] {
			return a[i] < b[i]
		}
	}
	return len(a) < len(b)
}
