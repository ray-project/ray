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
	"sort"
	"time"

	"github.com/google/uuid"
	"github.com/ray-project/ray/go/pkg/log"
	"github.com/ray-project/ray/go/proto"
)

var InstanceUtil = &instanceUtil{}

// NowUnixNano returns the current time in nanoseconds.
// It is a variable so tests can inject a fixed time (matching Python tests
// mocking time.time_ns).
var NowUnixNano = func() int64 { return time.Now().UnixNano() }

// InstanceUtil provides instance state judgment methods.
type instanceUtil struct {
	// reachableFrom is the memoized set of reachable statuses, keyed by the
	// instance status, valued by the set of statuses reachable from it.
	reachableFrom map[proto.Instance_InstanceStatus]map[proto.Instance_InstanceStatus]bool
}

// NewInstance creates a new instance.
// version is set to 0 and backfilled by the underlying storage on write.
func (iu *instanceUtil) NewInstance(instanceID, instanceType string, status proto.Instance_InstanceStatus, details string) *proto.Instance {
	instance := &proto.Instance{
		InstanceId:   instanceID,
		InstanceType: instanceType,
		Status:       status,
	}
	iu.recordStatusTransition(instance, status, details)
	return instance
}

// SetStatus transitions the instance status to the new status.
// Returning false means the status transition is invalid (not in the valid
// transition table).
func (iu *instanceUtil) SetStatus(instance *proto.Instance, newStatus proto.Instance_InstanceStatus, details string) bool {
	for _, s := range iu.getValidTransitions()[instance.Status] {
		if s == newStatus {
			instance.Status = newStatus
			iu.recordStatusTransition(instance, newStatus, details)
			return true
		}
	}
	return false
}

// recordStatusTransition records a status transition into the instance's
// status history.
func (iu *instanceUtil) recordStatusTransition(instance *proto.Instance, status proto.Instance_InstanceStatus, details string) {
	instance.StatusHistory = append(instance.StatusHistory, &proto.Instance_StatusHistory{
		InstanceStatus: status,
		TimestampNs:    time.Now().UnixNano(),
		Details:        details,
	})
}

// GetLogStrForUpdate returns the log string for an instance update.
func (iu *instanceUtil) GetLogStrForUpdate(instance *proto.Instance, update *proto.InstanceUpdateEvent) string {
	if update.Upsert {
		return fmt.Sprintf("New instance %s (id=%s, type=%s, cloud_instance_id=%s, ray_id=%s): %s",
			update.NewInstanceStatus.String(), instance.InstanceId, instance.InstanceType,
			instance.GetCloudInstanceId(), instance.GetNodeId(), update.Details)
	}
	return fmt.Sprintf("Update instance %s->%s (id=%s, type=%s, cloud_instance_id=%s, ray_id=%s): %s",
		instance.Status.String(), update.NewInstanceStatus.String(), instance.InstanceId,
		instance.InstanceType, instance.GetCloudInstanceId(), instance.GetNodeId(), update.Details)
}

// IsRayPending reports whether the instance is in a pending state.
// Returns true when the instance status is QUEUED, REQUESTED, ALLOCATED or
// RAY_INSTALLING, i.e. all the pre-states before the Ray process has started
// running.
func (iu *instanceUtil) IsRayPending(status proto.Instance_InstanceStatus) bool {
	if status == proto.Instance_UNKNOWN {
		return false
	}

	if !iu.getReachableStatuses(status)[proto.Instance_RAY_RUNNING] {
		return false
	}

	if iu.getReachableStatuses(proto.Instance_RAY_RUNNING)[status] {
		return false
	}

	return true
}

// IsRayRunningReachable reports whether the instance status can transition to
// RAY_RUNNING.
// Corresponds to Python InstanceUtil.is_ray_running_reachable:
// returns whether the reachable status set of the given status contains
// RAY_RUNNING.
// Note this differs from IsRayRunning: IsRayRunning checks an already running
// state, while this checks "may run" — including the pre-states
// QUEUED/REQUESTED/ALLOCATED/RAY_INSTALLING.
func (iu *instanceUtil) IsRayRunningReachable(status proto.Instance_InstanceStatus) bool {
	return iu.getReachableStatuses(status)[proto.Instance_RAY_RUNNING]
}

// IsRayRunning reports whether the instance is in a running state.
// Returns true when the instance status is RAY_RUNNING, RAY_STOP_REQUESTED or
// RAY_STOPPING, i.e. all the states where the Ray process has started
// (including an in-progress graceful stop).
func (iu *instanceUtil) IsRayRunning(status proto.Instance_InstanceStatus) bool {
	if status == proto.Instance_UNKNOWN {
		return false
	}

	if iu.getReachableStatuses(proto.Instance_RAY_STOPPING)[status] {
		return false
	}

	if iu.getReachableStatuses(proto.Instance_RAY_RUNNING)[status] {
		return true
	}

	return false
}

// RandomInstanceID generates a random instance ID.
// Corresponds to Python InstanceUtil.random_instance_id (uuid4).
func (iu *instanceUtil) RandomInstanceID() string {
	return uuid.New().String()
}

// GetStatusTransitions returns the instance's status transition history.
// Corresponds to Python InstanceUtil.get_status_transitions:
// when selectInstanceStatus is non-empty, only the transitions to that status
// are returned, otherwise all records are returned.
func (iu *instanceUtil) GetStatusTransitions(instance *proto.Instance, selectInstanceStatus ...proto.Instance_InstanceStatus) []*proto.Instance_StatusHistory {
	var selectStatus *proto.Instance_InstanceStatus
	if len(selectInstanceStatus) > 0 {
		selectStatus = &selectInstanceStatus[0]
	}
	history := make([]*proto.Instance_StatusHistory, 0, len(instance.StatusHistory))
	for _, statusUpdate := range instance.StatusHistory {
		if selectStatus != nil && statusUpdate.InstanceStatus != *selectStatus {
			continue
		}
		history = append(history, statusUpdate)
	}
	return history
}

// GetLastStatusTransition returns the instance's most recent status transition.
// Corresponds to Python InstanceUtil.get_last_status_transition:
// when selectInstanceStatus is non-empty, the most recent transition to that
// status is returned, otherwise the most recent status update.
func (iu *instanceUtil) GetLastStatusTransition(instance *proto.Instance, selectInstanceStatus ...proto.Instance_InstanceStatus) *proto.Instance_StatusHistory {
	history := iu.GetStatusTransitions(instance, selectInstanceStatus...)
	sort.SliceStable(history, func(i, j int) bool {
		return history[i].TimestampNs < history[j].TimestampNs
	})
	if len(history) == 0 {
		return nil
	}
	return history[len(history)-1]
}

// GetStatusTransitionTimesNs returns the instance status update timestamps in
// nanoseconds.
// Corresponds to Python InstanceUtil.get_status_transition_times_ns.
func (iu *instanceUtil) GetStatusTransitionTimesNs(instance *proto.Instance, selectInstanceStatus ...proto.Instance_InstanceStatus) []int64 {
	history := iu.GetStatusTransitions(instance, selectInstanceStatus...)
	times := make([]int64, 0, len(history))
	for _, e := range history {
		times = append(times, e.TimestampNs)
	}
	return times
}

// HasTimeout reports whether the instance has stayed in the current status
// longer than timeoutS seconds.
// Corresponds to Python InstanceUtil.has_timeout:
// it compares the timestamp of the most recent entry into the current status
// with the current time.
// A missing status history is treated as not timed out and logs an error
// (Python asserts there).
func (iu *instanceUtil) HasTimeout(instance *proto.Instance, timeoutS int) bool {
	curStatus := instance.Status

	statusTimesNs := iu.GetStatusTransitionTimesNs(instance, curStatus)
	if len(statusTimesNs) == 0 {
		log.Log.Error(fmt.Errorf("instance %s has no %s status in history",
			instance.InstanceId, curStatus.String()), "")
		return false
	}
	sort.Slice(statusTimesNs, func(i, j int) bool {
		return statusTimesNs[i] < statusTimesNs[j]
	})
	statusTimeNs := statusTimesNs[len(statusTimesNs)-1]

	return NowUnixNano()-statusTimeNs > int64(timeoutS)*int64(time.Second)
}

// CanReach reports whether the to status is reachable from the from status.
// Corresponds to Python `to in InstanceUtil.get_reachable_statuses(from)`.
func (iu *instanceUtil) CanReach(from, to proto.Instance_InstanceStatus) bool {
	return iu.getReachableStatuses(from)[to]
}

// getReachableStatuses returns the set of all instance statuses reachable from
// the given instance status according to the transition rules.
func (iu *instanceUtil) getReachableStatuses(instanceStatus proto.Instance_InstanceStatus) map[proto.Instance_InstanceStatus]bool {
	// Run the pre-computation if the memoized cache is not initialized yet.
	if iu.reachableFrom == nil {
		iu.computeReachable()
	}
	// Return the reachable status set of the given status straight from the
	// cache.
	return iu.reachableFrom[instanceStatus]
}

// computeReachable pre-computes the reachability between all statuses via a
// depth-first search (DFS) and caches it in reachableFrom.
func (iu *instanceUtil) computeReachable() {
	validTransitions := iu.getValidTransitions()

	// dfs is the depth-first search finding all nodes reachable from the given
	// start node.
	var dfs func(graph map[proto.Instance_InstanceStatus][]proto.Instance_InstanceStatus,
		start proto.Instance_InstanceStatus,
		visited map[proto.Instance_InstanceStatus]bool)
	dfs = func(graph map[proto.Instance_InstanceStatus][]proto.Instance_InstanceStatus,
		start proto.Instance_InstanceStatus,
		visited map[proto.Instance_InstanceStatus]bool) {
		for _, nextNode := range graph[start] {
			if !visited[nextNode] {
				// Add to the visited set lazily so self-loops are captured.
				visited[nextNode] = true
				dfs(graph, nextNode, visited)
			}
		}
	}

	// Initialize the graph structure.
	iu.reachableFrom = make(map[proto.Instance_InstanceStatus]map[proto.Instance_InstanceStatus]bool)
	for status := range validTransitions {
		// All nodes reachable from 'start'.
		visited := make(map[proto.Instance_InstanceStatus]bool)
		dfs(validTransitions, status, visited)
		iu.reachableFrom[status] = visited
	}
}

// map[proto.Instance_InstanceStatus]map[proto.Instance_InstanceStatus]bool
// getValidTransitions returns the valid status transition map.
func (iu *instanceUtil) getValidTransitions() map[proto.Instance_InstanceStatus][]proto.Instance_InstanceStatus {
	return map[proto.Instance_InstanceStatus][]proto.Instance_InstanceStatus{
		// This is the initial status of a new instance.
		proto.Instance_QUEUED: {
			// The cloud provider is asked to launch a node for the instance.
			proto.Instance_REQUESTED,
		},
		// While in this status, a launch request has been made to the cloud
		// provider.
		proto.Instance_REQUESTED: {
			// The cloud provider allocated a cloud instance for the instance.
			proto.Instance_ALLOCATED,
			// Retry the allocation and go back to queued.
			proto.Instance_QUEUED,
			// The cloud provider failed to allocate one immediately.
			proto.Instance_ALLOCATION_FAILED,
		},
		// While in this status, the cloud instance has been allocated and is
		// running.
		proto.Instance_ALLOCATED: {
			// Ray needs to be installed and started on the configured cloud
			// instance.
			proto.Instance_RAY_INSTALLING,
			// Ray has been installed on the configured cloud instance.
			proto.Instance_RAY_RUNNING,
			// The cloud provider timed out allocating the running cloud
			// instance.
			proto.Instance_ALLOCATION_TIMEOUT,
			proto.Instance_RAY_STOPPING,
			proto.Instance_RAY_STOPPED,
			// The instance was asked to stop, e.g. a leaked instance: no
			// matching instance of the same type is found in the autoscaler's
			// state.
			proto.Instance_TERMINATING,
			// The cloud instance failed in some way.
			proto.Instance_TERMINATED,
		},
		// The Ray process is being installed and started on the cloud
		// instance.
		proto.Instance_RAY_INSTALLING: {
			// Ray was installed and started successfully, as reported by the
			// Ray cluster.
			proto.Instance_RAY_RUNNING,
			// The Ray installation failed.
			proto.Instance_RAY_INSTALL_FAILED,
			// When the Ray node was reported as stopped by the Ray cluster.
			proto.Instance_RAY_STOPPED,
			// The cloud instance is terminating (when the instance itself is
			// no longer needed, e.g. the instance is outdated and the
			// autoscaler is scaling down).
			proto.Instance_TERMINATING,
			// The cloud instance failed in some way during installation.
			proto.Instance_TERMINATED,
		},
		// The Ray process has been installed on the cloud instance and is
		// running.
		proto.Instance_RAY_RUNNING: {
			// Ray was asked to stop.
			proto.Instance_RAY_STOP_REQUESTED,
			// Ray is stopping (currently draining), e.g. idle termination.
			proto.Instance_RAY_STOPPING,
			// Ray has stopped, as reported by the Ray cluster.
			proto.Instance_RAY_STOPPED,
			// The cloud instance is terminating (when the instance itself is
			// no longer needed, e.g. the instance is outdated and the
			// autoscaler is scaling down).
			proto.Instance_TERMINATING,
			// The cloud instance failed in some way.
			proto.Instance_TERMINATED,
		},
		// The Ray process on the cloud instance was asked to stop.
		proto.Instance_RAY_STOP_REQUESTED: {
			// Ray is stopping on the cloud instance.
			proto.Instance_RAY_STOPPING,
			// Ray has stopped.
			proto.Instance_RAY_STOPPED,
			// The Ray stop request failed (e.g. the idle node is no longer
			// idle) and Ray is still running.
			proto.Instance_RAY_RUNNING,
			// The cloud instance failed in some way.
			proto.Instance_TERMINATED,
		},
		// The instance was allocated to a cloud instance, but the cloud
		// provider timed out allocating the running cloud instance.
		proto.Instance_ALLOCATION_TIMEOUT: {
			// The instance was asked to stop.
			proto.Instance_TERMINATING,
		},
		// While in this status, the Ray process was asked to stop at the Ray
		// cluster but has not yet shown up in the dead Ray node list reported
		// by the Ray cluster.
		proto.Instance_RAY_STOPPING: {
			// Ray has stopped and the Ray node exists in the dead Ray node
			// list reported by the Ray cluster.
			proto.Instance_RAY_STOPPED,
			// The cloud instance is terminating (when the instance itself is
			// no longer needed, e.g. the instance is outdated and the
			// autoscaler is scaling down).
			proto.Instance_TERMINATING,
			// The cloud instance failed in some way.
			proto.Instance_TERMINATED,
		},
		// While in this status, the Ray process has stopped and the Ray node
		// exists in the dead Ray node list reported by the Ray cluster.
		proto.Instance_RAY_STOPPED: {
			// The cloud instance is terminating (when the instance itself is
			// no longer needed, e.g. the instance is outdated and the
			// autoscaler is scaling down).
			proto.Instance_TERMINATING,
			// The cloud instance failed in some way.
			proto.Instance_TERMINATED,
		},
		// While in this status, the cloud instance was asked to stop at the
		// node provider.
		proto.Instance_TERMINATING: {
			// When the cloud instance no longer appears in the node provider's
			// list of running cloud instances.
			proto.Instance_TERMINATED,
			// When the cloud instance failed to be terminated.
			proto.Instance_TERMINATION_FAILED,
		},
		// While in this status, the cloud instance failed to be terminated by
		// the node provider.
		proto.Instance_TERMINATION_FAILED: {
			// Retry the termination and go back to terminating.
			proto.Instance_TERMINATING,
		},
		// Whenever the cloud instance disappears from the node provider's list
		// of running cloud instances, the instance is marked stopped.
		proto.Instance_TERMINATED: {}, // terminal status
		// While in this status, the cloud instance failed to be allocated by
		// the node provider.
		proto.Instance_ALLOCATION_FAILED: {}, // terminal status
		proto.Instance_RAY_INSTALL_FAILED: {
			// When the Ray installation failed, the autoscaler asked to shut
			// the instance down.
			proto.Instance_TERMINATING,
			// The cloud instance failed in some way.
			proto.Instance_TERMINATED,
		},
		// The initial status before the instance is created; should not be
		// used.
		proto.Instance_UNKNOWN: {},
	}
}
