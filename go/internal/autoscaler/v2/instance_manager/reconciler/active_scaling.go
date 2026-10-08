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

// Proactive status stepping subtasks: stuck instance handling, scale up/down,
// launch requests, instance termination, Ray installation and the autoscaling
// state summary.
// Corresponds to the _handle_stuck_instances through _fill_autoscaling_state
// section of Python reconciler.py.
package reconciler

import (
	"encoding/hex"
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/ray-project/ray/go/internal/autoscaler"
	"github.com/ray-project/ray/go/internal/autoscaler/v2/instance_manager"
	"github.com/ray-project/ray/go/internal/autoscaler/v2/scheduler"
	"github.com/ray-project/ray/go/pkg/log"
	"github.com/ray-project/ray/go/proto"
)

// handleStuckInstances handles the stuck instances.
// Corresponds to Python Reconciler._handle_stuck_instances:
// a REQUESTED instance that timed out is re-queued or marked failed;
// ALLOCATED/RAY_INSTALLING/TERMINATING/RAY_STOP_REQUESTED transition to a
// failure status on timeout; the other transient statuses only warn.
func (r *Reconciler) handleStuckInstances(reconcileConfig *instance_manager.InstanceReconcileConfig) {
	instances, version, err := r.getImInstances()
	if err != nil {
		return
	}
	updates := make([]*proto.InstanceUpdateEvent, 0)

	// Group by status.
	instancesByStatus := make(map[proto.Instance_InstanceStatus][]*proto.Instance)
	for _, instance := range instances {
		instancesByStatus[instance.Status] = append(instancesByStatus[instance.Status], instance)
	}

	// Allocation stuck: re-queue or mark the allocation failed.
	for _, instance := range instancesByStatus[proto.Instance_REQUESTED] {
		update := handleStuckRequestedInstance(
			instance,
			reconcileConfig.RequestStatusTimeoutS,
			reconcileConfig.MaxNumRetryRequestToAllocate,
		)
		if update != nil {
			updates = append(updates, update)
		}
	}

	// A leaked ALLOCATED instance should be terminated:
	// usually because Ray failed to start on the instance and it cannot reach
	// RAY_RUNNING for a long time.
	for _, instance := range instancesByStatus[proto.Instance_ALLOCATED] {
		if instance.GetCloudInstanceId() == "" {
			// Python asserts here; Go defensively logs and skips.
			log.Log.Error(fmt.Errorf("cloud instance id should be set on ALLOCATED instance %s",
				instance.InstanceId), "")
			continue
		}
		cloudInstanceId := instance.GetCloudInstanceId()
		instanceType := instance.InstanceType
		update := handleStuckInstance(
			instance,
			reconcileConfig.AllocateStatusTimeoutS,
			proto.Instance_ALLOCATION_TIMEOUT,
			&cloudInstanceId,
			&instanceType,
			nil,
		)
		if update != nil {
			updates = append(updates, update)
		}
	}

	// Install stuck: RAY_INSTALLING that does not finish for a long time is
	// treated as an install failure.
	for _, instance := range instancesByStatus[proto.Instance_RAY_INSTALLING] {
		update := handleStuckInstance(
			instance,
			reconcileConfig.RayInstallStatusTimeoutS,
			proto.Instance_RAY_INSTALL_FAILED,
			nil, nil, nil,
		)
		if update != nil {
			updates = append(updates, update)
		}
	}

	// Termination stuck: TERMINATING that does not disappear from the cloud
	// provider for a long time is marked failed to trigger a retry.
	for _, instance := range instancesByStatus[proto.Instance_TERMINATING] {
		update := handleStuckInstance(
			instance,
			reconcileConfig.TerminatingStatusTimeoutS,
			proto.Instance_TERMINATION_FAILED,
			nil, nil, nil,
		)
		if update != nil {
			updates = append(updates, update)
		}
	}

	// Stop stuck: RAY_STOP_REQUESTED that does not stop for a long time falls
	// back to RAY_RUNNING.
	for _, instance := range instancesByStatus[proto.Instance_RAY_STOP_REQUESTED] {
		rayNodeId := instance.GetNodeId()
		if rayNodeId == "" {
			// A RAY_RUNNING update requires a non-empty ray_node_id; skip on
			// miss so the whole update batch does not fail.
			log.Log.Error(fmt.Errorf("instance %s in RAY_STOP_REQUESTED status should have node_id set",
				instance.InstanceId), "")
			continue
		}
		update := handleStuckInstance(
			instance,
			reconcileConfig.RayStopRequestedStatusTimeoutS,
			proto.Instance_RAY_RUNNING,
			nil, nil, &rayNodeId,
		)
		if update != nil {
			updates = append(updates, update)
		}
	}

	// Transient/unbounded statuses only warn, no timeout transitions.
	for _, status := range []proto.Instance_InstanceStatus{
		proto.Instance_RAY_STOPPING,
		proto.Instance_RAY_INSTALL_FAILED,
		proto.Instance_RAY_STOPPED,
		proto.Instance_TERMINATION_FAILED,
		proto.Instance_QUEUED,
	} {
		warnStuckInstances(
			instancesByStatus[status],
			status,
			reconcileConfig.TransientStatusWarnIntervalS,
		)
	}

	r.updateInstanceManager(version, updates)
}

// handleStuckRequestedInstance handles an instance stuck in REQUESTED.
// Corresponds to Python Reconciler._handle_stuck_requested_instance:
// on timeout with the retries exceeded -> ALLOCATION_FAILED, otherwise
// re-queue -> QUEUED.
func handleStuckRequestedInstance(
	instance *proto.Instance,
	timeoutS int,
	maxNumRetryRequestToAllocate int,
) *proto.InstanceUpdateEvent {
	if !instance_manager.InstanceUtil.HasTimeout(instance, timeoutS) {
		// Not timed out yet; stay patient.
		return nil
	}

	allRequestTimesNs := instance_manager.InstanceUtil.GetStatusTransitionTimesNs(instance, proto.Instance_REQUESTED)
	sort.Slice(allRequestTimesNs, func(i, j int) bool {
		return allRequestTimesNs[i] < allRequestTimesNs[j]
	})
	// Retries exceeded; mark the allocation failed.
	if len(allRequestTimesNs) > maxNumRetryRequestToAllocate {
		return &proto.InstanceUpdateEvent{
			InstanceId:        instance.InstanceId,
			NewInstanceStatus: proto.Instance_ALLOCATION_FAILED,
			Details: fmt.Sprintf("failed to allocate cloud instance after %d attempts > max_num_retry_request_to_allocate=%d",
				len(allRequestTimesNs), maxNumRetryRequestToAllocate),
		}
	}

	// Re-queue to retry the allocation.
	return &proto.InstanceUpdateEvent{
		InstanceId:        instance.InstanceId,
		NewInstanceStatus: proto.Instance_QUEUED,
		Details:           fmt.Sprintf("queue again to launch after timeout=%ds", timeoutS),
	}
}

// handleStuckInstance handles an instance stuck in the given status.
// Corresponds to Python Reconciler._handle_stuck_instance:
// on timeout transition to newStatus; cloudInstanceId/instanceType/rayNodeId
// correspond to Python's mutable update_kwargs.
func handleStuckInstance(
	instance *proto.Instance,
	timeoutS int,
	newStatus proto.Instance_InstanceStatus,
	cloudInstanceId *string,
	instanceType *string,
	rayNodeId *string,
) *proto.InstanceUpdateEvent {
	if !instance_manager.InstanceUtil.HasTimeout(instance, timeoutS) {
		// Not timed out yet; stay patient.
		return nil
	}

	return &proto.InstanceUpdateEvent{
		InstanceId:        instance.InstanceId,
		NewInstanceStatus: newStatus,
		CloudInstanceId:   cloudInstanceId,
		InstanceType:      instanceType,
		RayNodeId:         rayNodeId,
		Details: fmt.Sprintf("timeout=%ds at status %s",
			timeoutS, instance.Status.String()),
	}
}

// warnStuckInstances warns about the instances stuck in a transient status for
// too long.
// Corresponds to Python Reconciler._warn_stuck_instances.
// Returns the number of warned instances (handy for test assertions).
func warnStuckInstances(
	instances []*proto.Instance,
	status proto.Instance_InstanceStatus,
	warnIntervalS int,
) int {
	warned := 0
	for _, instance := range instances {
		statusTimesNs := instance_manager.InstanceUtil.GetStatusTransitionTimesNs(instance, status)
		if len(statusTimesNs) == 0 {
			// Python asserts here; Go defensively logs.
			log.Log.Error(fmt.Errorf("instance %s has no %s status in history",
				instance.InstanceId, status.String()), "")
			continue
		}
		sort.Slice(statusTimesNs, func(i, j int) bool {
			return statusTimesNs[i] < statusTimesNs[j]
		})
		statusTimeNs := statusTimesNs[len(statusTimesNs)-1]

		nowNs := instance_manager.NowUnixNano()
		if nowNs-statusTimeNs > int64(warnIntervalS)*int64(time.Second) {
			log.Log.Info("Instance is stuck in status for too long.",
				"instance_id", instance.InstanceId,
				"instance_status", instance.Status.String(),
				"stuck_status", status.String(),
				"stuck_seconds", (nowNs-statusTimeNs)/int64(time.Second))
			warned++
		}
	}
	return warned
}

// scaleCluster scales the cluster per the scheduler decisions.
// Corresponds to Python Reconciler._scale_cluster:
// the scheduler gives the instances to launch/terminate, which are translated
// into instance status updates; no scaling happens before the head node is
// ready or with the read-only provider.
func (r *Reconciler) scaleCluster(
	autoscalingState *proto.AutoscalingState,
	rayState *proto.ClusterResourceState,
	asConfig *instance_manager.AutoscalingConfig,
) {
	// Get the current instance state.
	imInstances, version, err := r.getImInstances()
	if err != nil {
		return
	}

	imInstancesByInstanceId := make(map[string]*proto.Instance, len(imInstances))
	for _, instance := range imInstances {
		if instance.InstanceId != "" {
			imInstancesByInstanceId[instance.InstanceId] = instance
		}
	}

	// Build the instance snapshot the scheduler needs.
	rayNodesById := make(map[string]*proto.NodeState, len(rayState.NodeStates))
	for _, node := range rayState.NodeStates {
		rayNodesById[hex.EncodeToString(node.NodeId)] = node
	}
	// 1. Build the AutoscalerInstance list covering all instance states of the
	// current cluster.
	autoscalerInstances := make([]*scheduler.AutoscalerInstance, 0, len(imInstances))
	for _, imInstance := range imInstances {
		var rayNode *proto.NodeState
		if nodeId := imInstance.GetNodeId(); nodeId != "" {
			rayNode = rayNodesById[nodeId]
		}
		autoscalerInstances = append(autoscalerInstances, &scheduler.AutoscalerInstance{
			RayNode:         rayNode,
			IMInstance:      imInstance,
			CloudInstanceID: imInstance.GetCloudInstanceId(),
		})
	}

	var cloudResourceAvailabilities map[string]float64
	if r.cloudResourceMonitor != nil {
		cloudResourceAvailabilities = r.cloudResourceMonitor.GetResourceAvailabilities()
	}
	// 2. Create the scheduling request for the resource scheduler to decide.
	schedRequest := &scheduler.SchedulingRequest{
		NodeTypeConfigs:             asConfig.GetNodeTypeConfigs(),
		MaxNumNodes:                 asConfig.GetMaxNumNodes(),
		ResourceRequests:            rayState.PendingResourceRequests,
		GangResourceRequests:        rayState.PendingGangResourceRequests,
		ClusterResourceConstraints:  rayState.ClusterResourceConstraints,
		CurrentInstances:            autoscalerInstances,
		IdleTimeoutS:                asConfig.GetIdleTimeoutS(),
		DisableLaunchConfigCheck:    asConfig.DisableLaunchConfigCheck(),
		CloudResourceAvailabilities: cloudResourceAvailabilities,
	}

	// 3. Ask the scheduler whether to scale.
	reply := r.scheduler.Schedule(schedRequest)

	// Fill the autoscaling state.
	autoscalingState.InfeasibleResourceRequests = append(
		autoscalingState.InfeasibleResourceRequests, reply.InfeasibleResourceRequests...)
	autoscalingState.InfeasibleGangResourceRequests = append(
		autoscalingState.InfeasibleGangResourceRequests, reply.InfeasibleGangResourceRequests...)
	autoscalingState.InfeasibleClusterResourceConstraints = append(
		autoscalingState.InfeasibleClusterResourceConstraints, reply.InfeasibleClusterResourceConstraints...)

	if !r.isHeadNodeRunning() {
		// The cluster must not scale before the head node is ready:
		// the head node (i.e. the raylet) may still be registering (although
		// GCS is already available); wait for it to run to avoid scaling up by
		// min worker counts prematurely.
		return
	}

	if asConfig.GetProvider() == instance_manager.ProviderReadOnly {
		// The read-only provider must not scale the cluster.
		return
	}

	// Scale the cluster as needed.
	updates := make([]*proto.InstanceUpdateEvent, 0)
	for _, terminateRequest := range reply.ToTerminate {
		instanceId := terminateRequest.InstanceId
		if terminateRequest.InstanceStatus == proto.Instance_ALLOCATED ||
			terminateRequest.InstanceStatus == proto.Instance_RAY_INSTALLING {
			// The instance is not running Ray yet and cannot be asked to
			// stop/drain; skip the RAY_STOP_REQUESTED status and terminate the
			// node directly.
			imInstanceToTerminate := imInstancesByInstanceId[instanceId]
			if imInstanceToTerminate == nil {
				log.Log.Error(fmt.Errorf("instance %s to terminate not found in instance manager", instanceId), "")
				continue
			}
			cloudInstanceId := imInstanceToTerminate.GetCloudInstanceId()
			if cloudInstanceId == "" {
				// A TERMINATING update requires a non-empty cloud instance ID;
				// skip on miss so the whole update batch does not fail.
				log.Log.Error(fmt.Errorf("instance %s to terminate has no cloud_instance_id", instanceId), "")
				continue
			}
			updates = append(updates, &proto.InstanceUpdateEvent{
				InstanceId:         instanceId,
				NewInstanceStatus:  proto.Instance_TERMINATING,
				CloudInstanceId:    &cloudInstanceId,
				TerminationRequest: terminateRequest,
				Details:            fmt.Sprintf("terminating ray: %s", terminateRequest.Details),
			})
		} else {
			updates = append(updates, &proto.InstanceUpdateEvent{
				InstanceId:         instanceId,
				NewInstanceStatus:  proto.Instance_RAY_STOP_REQUESTED,
				TerminationRequest: terminateRequest,
				Details:            fmt.Sprintf("draining ray: %s", terminateRequest.Details),
			})
		}
	}

	// New instances.
	for _, launchRequest := range reply.ToLaunch {
		for i := 0; i < int(launchRequest.Count); i++ {
			instanceType := launchRequest.InstanceType
			updates = append(updates, &proto.InstanceUpdateEvent{
				InstanceId:        instance_manager.InstanceUtil.RandomInstanceID(),
				NewInstanceStatus: proto.Instance_QUEUED,
				InstanceType:      &instanceType,
				Upsert:            true,
				Details: fmt.Sprintf("queuing new instance of %s from scheduler",
					launchRequest.InstanceType),
			})
		}
	}

	r.updateInstanceManager(version, updates)
}

// handleInstancesLaunch handles the launch requests of the queued instances.
// Corresponds to Python Reconciler._handle_instances_launch:
// moves QUEUED instances to REQUESTED, bounded by the upscaling speed and the
// concurrent launch cap.
func (r *Reconciler) handleInstancesLaunch(asConfig *instance_manager.AutoscalingConfig) {
	instances, version, err := r.getImInstances()
	if err != nil {
		return
	}

	queuedInstances := make([]*proto.Instance, 0)
	requestedInstances := make([]*proto.Instance, 0)
	runningInstances := make([]*proto.Instance, 0)
	for _, instance := range instances {
		switch instance.Status {
		case proto.Instance_QUEUED:
			queuedInstances = append(queuedInstances, instance)
		case proto.Instance_REQUESTED:
			requestedInstances = append(requestedInstances, instance)
		case proto.Instance_RAY_RUNNING:
			runningInstances = append(runningInstances, instance)
		}
	}

	if len(queuedInstances) == 0 {
		// No queued instances.
		return
	}

	toLaunch := computeToLaunch(
		queuedInstances,
		requestedInstances,
		runningInstances,
		asConfig.GetUpscalingSpeed(),
		asConfig.GetMaxConcurrentLaunches(),
	)

	// Move the instances to REQUESTED so the instance launcher picks them up.
	updates := make([]*proto.InstanceUpdateEvent, 0)
	newLaunchRequestId := instance_manager.InstanceUtil.RandomInstanceID()
	// Sort by node type to keep the update order deterministic.
	instanceTypes := make([]string, 0, len(toLaunch))
	for instanceType := range toLaunch {
		instanceTypes = append(instanceTypes, instanceType)
	}
	sort.Strings(instanceTypes)
	for _, instanceType := range instanceTypes {
		for _, instance := range toLaunch[instanceType] {
			// A re-queued instance reuses its original launch request ID.
			launchRequestId := newLaunchRequestId
			if instance.LaunchRequestId != "" {
				launchRequestId = instance.LaunchRequestId
			}
			instanceTypeCopy := instanceType
			updates = append(updates, &proto.InstanceUpdateEvent{
				InstanceId:        instance.InstanceId,
				NewInstanceStatus: proto.Instance_REQUESTED,
				LaunchRequestId:   &launchRequestId,
				InstanceType:      &instanceTypeCopy,
				Details: fmt.Sprintf("requested to launch %s with request id %s",
					instanceType, launchRequestId),
			})
		}
	}

	r.updateInstanceManager(version, updates)
}

// computeToLaunch computes the queued instances to launch this round.
// Corresponds to Python Reconciler._compute_to_launch:
// each node type is bounded by the upscaling speed (based on the running
// instance count) and the global concurrent launch cap; the queued instances
// are taken earliest-queued first.
func computeToLaunch(
	queuedInstances []*proto.Instance,
	requestedInstances []*proto.Instance,
	runningInstances []*proto.Instance,
	upscalingSpeed float64,
	maxConcurrentLaunches int,
) map[string][]*proto.Instance {
	// Group by type, keeping the order of the first appearance of each type
	// (matching the Python dict insertion order; the global concurrency cap
	// accumulates in that order, so the order affects the result).
	groupByType := func(instances []*proto.Instance) (map[string][]*proto.Instance, []string) {
		instancesByType := make(map[string][]*proto.Instance)
		typeOrder := make([]string, 0)
		for _, instance := range instances {
			if _, ok := instancesByType[instance.InstanceType]; !ok {
				typeOrder = append(typeOrder, instance.InstanceType)
			}
			instancesByType[instance.InstanceType] = append(instancesByType[instance.InstanceType], instance)
		}
		return instancesByType, typeOrder
	}

	queuedInstancesByType, typeOrder := groupByType(queuedInstances)
	runningInstancesByType, _ := groupByType(runningInstances)

	totalNumRequestedToLaunch := len(requestedInstances)
	allToLaunch := make(map[string][]*proto.Instance)

	for _, instanceType := range typeOrder {
		queuedInstancesForType := queuedInstancesByType[instanceType]
		runningInstancesForType := runningInstancesByType[instanceType]

		// Bound the number of nodes allowed to launch by the current running
		// instance count.
		numDesiredToUpscale := int(math.Ceil(upscalingSpeed * math.Max(float64(len(runningInstancesForType)), 1)))
		if numDesiredToUpscale < 1 {
			numDesiredToUpscale = 1
		}

		// Global bound: the total concurrent launches do not exceed
		// maxConcurrentLaunches.
		numToLaunch := maxConcurrentLaunches - totalNumRequestedToLaunch
		if numDesiredToUpscale < numToLaunch {
			numToLaunch = numDesiredToUpscale
		}

		// Clamp to [0, len(queued)].
		if numToLaunch < 0 {
			numToLaunch = 0
		}
		if numToLaunch > len(queuedInstancesForType) {
			numToLaunch = len(queuedInstancesForType)
		}

		// Sort by the earliest queued time and take the first numToLaunch.
		sorted := make([]*proto.Instance, len(queuedInstancesForType))
		copy(sorted, queuedInstancesForType)
		sort.SliceStable(sorted, func(i, j int) bool {
			return lessStatusTransitionTimes(
				instance_manager.InstanceUtil.GetStatusTransitionTimesNs(sorted[i], proto.Instance_QUEUED),
				instance_manager.InstanceUtil.GetStatusTransitionTimesNs(sorted[j], proto.Instance_QUEUED),
			)
		})

		allToLaunch[instanceType] = sorted[:numToLaunch]
		totalNumRequestedToLaunch += numToLaunch
	}

	return allToLaunch
}

// terminateInstances terminates the instances in the pre-termination statuses.
// Corresponds to Python Reconciler._terminate_instances:
// RAY_STOPPED/ALLOCATION_TIMEOUT/RAY_INSTALL_FAILED/TERMINATION_FAILED ->
// TERMINATING.
func (r *Reconciler) terminateInstances() {
	imInstances, version, err := r.getImInstances()
	if err != nil {
		return
	}
	updates := make([]*proto.InstanceUpdateEvent, 0)
	for _, instance := range imInstances {
		switch instance.Status {
		case proto.Instance_RAY_STOPPED,
			proto.Instance_ALLOCATION_TIMEOUT,
			proto.Instance_RAY_INSTALL_FAILED,
			proto.Instance_TERMINATION_FAILED:
		default:
			continue
		}

		// Terminate the instance (a TERMINATING update requires a non-empty
		// cloud instance ID; skip that instance on miss so the whole update
		// batch does not fail).
		cloudInstanceId := instance.GetCloudInstanceId()
		if cloudInstanceId == "" {
			log.Log.Error(fmt.Errorf("instance %s in %s status should have cloud_instance_id set",
				instance.InstanceId, instance.Status.String()), "")
			continue
		}
		updates = append(updates, &proto.InstanceUpdateEvent{
			InstanceId:        instance.InstanceId,
			NewInstanceStatus: proto.Instance_TERMINATING,
			CloudInstanceId:   &cloudInstanceId,
			Details: fmt.Sprintf("terminating instance from %s",
				instance.Status.String()),
		})
	}

	r.updateInstanceManager(version, updates)
}

// installRay installs Ray on the ready ALLOCATED instances.
// Corresponds to Python Reconciler._install_ray:
// when the cloud instance is running, ALLOCATED -> RAY_INSTALLING; the actual
// installation is done asynchronously by the subscriber (the
// ThreadedRayInstaller, Phase 8 pending port).
func (r *Reconciler) installRay(nonTerminatedCloudInstances map[string]autoscaler.CloudInstance) {
	imInstances, version, err := r.getImInstances()
	if err != nil {
		return
	}
	updates := make([]*proto.InstanceUpdateEvent, 0)
	for _, instance := range imInstances {
		if instance.Status != proto.Instance_ALLOCATED {
			continue
		}

		if instance.NodeKind == proto.NodeKind_HEAD {
			// Skip the head node.
			continue
		}

		cloudInstance := nonTerminatedCloudInstances[instance.GetCloudInstanceId()]
		if cloudInstance.CloudInstanceId == "" {
			// Python asserts here; the instance was not found in the
			// non-terminated cloud instance list.
			log.Log.Error(fmt.Errorf("cloud instance %s is not found in non_terminated_cloud_instances",
				instance.GetCloudInstanceId()), "")
			continue
		}

		if !cloudInstance.IsRunning {
			// The instance may still be booting (e.g. configuring ssh).
			continue
		}

		// Install Ray on the running cloud instance.
		updates = append(updates, &proto.InstanceUpdateEvent{
			InstanceId:        instance.InstanceId,
			NewInstanceStatus: proto.Instance_RAY_INSTALLING,
			Details:           "installing ray",
		})
	}

	r.updateInstanceManager(version, updates)
}

// fillAutoscalingState summarizes the instance statuses into the autoscaling
// state.
// Corresponds to Python Reconciler._fill_autoscaling_state:
// REQUESTED/QUEUED are aggregated by launch_request_id into pending launch
// requests, ALLOCATED/RAY_INSTALLING are the pending instances, and
// ALLOCATION_FAILED are the failed launch requests.
func (r *Reconciler) fillAutoscalingState(autoscalingState *proto.AutoscalingState) {
	// The autoscaling state version uses the IM's storage version.
	instances, version, err := r.getImInstances()
	if err != nil {
		return
	}
	autoscalingState.AutoscalerStateVersion = version

	// Group by status.
	instancesByStatus := make(map[proto.Instance_InstanceStatus][]*proto.Instance)
	for _, instance := range instances {
		instancesByStatus[instance.Status] = append(instancesByStatus[instance.Status], instance)
	}

	// Pending launch requests: instances in REQUESTED/QUEUED with a launch
	// request ID, aggregated by request.
	instancesByLaunchRequest := make(map[string][]*proto.Instance)
	launchRequestOrder := make([]string, 0)
	for _, status := range []proto.Instance_InstanceStatus{proto.Instance_REQUESTED, proto.Instance_QUEUED} {
		for _, instance := range instancesByStatus[status] {
			if instance.LaunchRequestId != "" {
				if _, ok := instancesByLaunchRequest[instance.LaunchRequestId]; !ok {
					launchRequestOrder = append(launchRequestOrder, instance.LaunchRequestId)
				}
				instancesByLaunchRequest[instance.LaunchRequestId] =
					append(instancesByLaunchRequest[instance.LaunchRequestId], instance)
			}
		}
	}

	for _, launchRequestId := range launchRequestOrder {
		launchRequestInstances := instancesByLaunchRequest[launchRequestId]
		numInstancesByType := make(map[string]int32)
		for _, instance := range launchRequestInstances {
			numInstancesByType[instance.InstanceType]++
		}

		// All instances of the same request ID should share the same request
		// time.
		requestUpdate := instance_manager.InstanceUtil.GetLastStatusTransition(
			launchRequestInstances[0], proto.Instance_REQUESTED)
		requestTimeNs := int64(0)
		if requestUpdate != nil {
			requestTimeNs = requestUpdate.TimestampNs
		}

		for instanceType, count := range numInstancesByType {
			autoscalingState.PendingInstanceRequests = append(autoscalingState.PendingInstanceRequests,
				&proto.PendingInstanceRequest{
					RayNodeTypeName: instanceType,
					Count:           count,
					RequestTs:       requestTimeNs / int64(time.Second),
				})
		}
	}

	// Pending instances.
	for _, status := range []proto.Instance_InstanceStatus{proto.Instance_ALLOCATED, proto.Instance_RAY_INSTALLING} {
		for _, instance := range instancesByStatus[status] {
			// Take the details of the most recent status update.
			statusHistory := make([]*proto.Instance_StatusHistory, len(instance.StatusHistory))
			copy(statusHistory, instance.StatusHistory)
			sort.SliceStable(statusHistory, func(i, j int) bool {
				return statusHistory[i].TimestampNs > statusHistory[j].TimestampNs
			})
			var details string
			if len(statusHistory) > 0 {
				details = statusHistory[0].Details
			}
			autoscalingState.PendingInstances = append(autoscalingState.PendingInstances,
				&proto.PendingInstance{
					InstanceId:      instance.InstanceId,
					RayNodeTypeName: instance.InstanceType,
					Details:         details,
				})
		}
	}

	// Failed launch requests.
	for _, instance := range instancesByStatus[proto.Instance_ALLOCATION_FAILED] {
		requestStatusUpdate := instance_manager.InstanceUtil.GetLastStatusTransition(
			instance, proto.Instance_REQUESTED)
		failedStatusUpdate := instance_manager.InstanceUtil.GetLastStatusTransition(
			instance, proto.Instance_ALLOCATION_FAILED)
		failedTimeNs := int64(0)
		if failedStatusUpdate != nil {
			failedTimeNs = failedStatusUpdate.TimestampNs
		}
		requestTimeNs := int64(0)
		if requestStatusUpdate != nil {
			requestTimeNs = requestStatusUpdate.TimestampNs
		}
		var reason string
		if failedStatusUpdate != nil {
			reason = failedStatusUpdate.Details
		}
		autoscalingState.FailedInstanceRequests = append(autoscalingState.FailedInstanceRequests,
			&proto.FailedInstanceRequest{
				RayNodeTypeName: instance.InstanceType,
				StartTs:         requestTimeNs / int64(time.Second),
				FailedTs:        failedTimeNs / int64(time.Second),
				Reason:          reason,
				Count:           1,
			})
	}
}
