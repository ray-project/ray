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

package subscribers

import (
	"time"

	"github.com/ray-project/ray/go/internal/autoscaler/v2/instance_manager"
	"github.com/ray-project/ray/go/pkg/log"
	"github.com/ray-project/ray/go/proto"
)

// Compile-time assertion: CloudResourceMonitor implements the instance update
// subscriber interface.
var _ instance_manager.InstanceUpdatedSubscriber = (*CloudResourceMonitor)(nil)

// CloudResourceMonitor is the subscriber recording the cloud resource
// availability per node type.
// Corresponds to Python subscribers/cloud_resource_monitor.py:
// with Spot and similar setups cloud resources change dynamically; if a type
// times out on allocation it will most likely fail again in the short term.
// This subscriber records the most recent unavailability timestamp per node
// type and computes availability scores from it, so the reconciler's scale-up
// decision (_compute_to_launch) can down-prioritize recently failed types.
type CloudResourceMonitor struct {
	lastUnavailableTimestamp map[string]float64
}

// NewCloudResourceMonitor creates the cloud resource monitor subscriber.
func NewCloudResourceMonitor() *CloudResourceMonitor {
	return &CloudResourceMonitor{
		lastUnavailableTimestamp: make(map[string]float64),
	}
}

// allocationTimeout records the unavailability timestamp when a node type
// times out allocating a cloud instance.
func (m *CloudResourceMonitor) allocationTimeout(failedEvent *proto.InstanceUpdateEvent) {
	unavailableTimestamp := float64(time.Now().UnixNano()) / 1e9
	m.lastUnavailableTimestamp[failedEvent.GetInstanceType()] = unavailableTimestamp
	log.Log.Info("Cloud resource type is unavailable, lowering its priority in future schedules.",
		"instanceType", failedEvent.GetInstanceType(),
		"unavailableTimestamp", unavailableTimestamp)
}

// allocationSucceeded clears the unavailability record when an instance of a
// node type runs successfully.
func (m *CloudResourceMonitor) allocationSucceeded(succeededEvent *proto.InstanceUpdateEvent) {
	if _, exists := m.lastUnavailableTimestamp[succeededEvent.GetInstanceType()]; exists {
		delete(m.lastUnavailableTimestamp, succeededEvent.GetInstanceType())
		log.Log.Info("Cloud resource type is available again, raising its priority in future schedules.",
			"instanceType", succeededEvent.GetInstanceType())
	}
}

// Notify is the instance status update callback:
// ALLOCATION_TIMEOUT -> record unavailable; RAY_RUNNING -> clear the
// unavailability record.
func (m *CloudResourceMonitor) Notify(events []*proto.InstanceUpdateEvent) {
	for _, event := range events {
		if event.NewInstanceStatus == proto.Instance_ALLOCATION_TIMEOUT {
			m.allocationTimeout(event)
		} else if event.NewInstanceStatus == proto.Instance_RAY_RUNNING && event.GetInstanceType() != "" {
			m.allocationSucceeded(event)
		}
	}
}

// GetResourceAvailabilities computes the resource availability score per node
// type.
// A higher score means the type is more likely to allocate successfully:
// score = 1 - the most recent unavailability timestamp / the max unavailability
// timestamp, i.e. types that have not failed for longer score higher, and a
// type that just failed scores close to 0.
// Returns an empty map when there is no unavailability record at all (all
// types are considered available).
func (m *CloudResourceMonitor) GetResourceAvailabilities() map[string]float64 {
	scores := make(map[string]float64)
	if len(m.lastUnavailableTimestamp) == 0 {
		return scores
	}

	maxTimestamp := float64(0)
	for _, timestamp := range m.lastUnavailableTimestamp {
		if timestamp > maxTimestamp {
			maxTimestamp = timestamp
		}
	}
	for nodeType, timestamp := range m.lastUnavailableTimestamp {
		scores[nodeType] = 1 - timestamp/maxTimestamp
	}
	return scores
}
