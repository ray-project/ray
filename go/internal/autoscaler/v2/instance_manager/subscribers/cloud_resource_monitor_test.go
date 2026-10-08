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
	"testing"

	"github.com/ray-project/ray/go/proto"
	"github.com/stretchr/testify/assert"
)

// TestCloudResourceMonitor_AllocationTimeout ALLOCATION_TIMEOUT records the
// unavailability timestamp.
func TestCloudResourceMonitor_AllocationTimeout(t *testing.T) {
	monitor := NewCloudResourceMonitor()

	monitor.Notify([]*proto.InstanceUpdateEvent{
		newEvent(proto.Instance_ALLOCATION_TIMEOUT, "type-a", "", ""),
	})

	assert.Contains(t, monitor.lastUnavailableTimestamp, "type-a")
	assert.Greater(t, monitor.lastUnavailableTimestamp["type-a"], float64(0))
}

// TestCloudResourceMonitor_AllocationSucceeded RAY_RUNNING clears the
// unavailability record.
func TestCloudResourceMonitor_AllocationSucceeded(t *testing.T) {
	monitor := NewCloudResourceMonitor()

	// Mark unavailable first, then run successfully.
	monitor.Notify([]*proto.InstanceUpdateEvent{
		newEvent(proto.Instance_ALLOCATION_TIMEOUT, "type-a", "", ""),
	})
	assert.Contains(t, monitor.lastUnavailableTimestamp, "type-a")

	monitor.Notify([]*proto.InstanceUpdateEvent{
		newEvent(proto.Instance_RAY_RUNNING, "type-a", "", "cloud-1"),
	})
	assert.NotContains(t, monitor.lastUnavailableTimestamp, "type-a")

	// For a type that was never unavailable, RAY_RUNNING produces no record.
	monitor.Notify([]*proto.InstanceUpdateEvent{
		newEvent(proto.Instance_RAY_RUNNING, "type-b", "", "cloud-2"),
	})
	assert.NotContains(t, monitor.lastUnavailableTimestamp, "type-b")
}

// TestCloudResourceMonitor_IgnoresOtherStatus other status events do not touch
// the records.
func TestCloudResourceMonitor_IgnoresOtherStatus(t *testing.T) {
	monitor := NewCloudResourceMonitor()

	monitor.Notify([]*proto.InstanceUpdateEvent{
		newEvent(proto.Instance_ALLOCATION_FAILED, "type-a", "", ""),
		newEvent(proto.Instance_TERMINATED, "type-a", "", "cloud-1"),
	})

	assert.Empty(t, monitor.lastUnavailableTimestamp)
}

// TestCloudResourceMonitor_GetResourceAvailabilities availability scores:
// the most recently failed type scores 0, types that failed earlier score
// higher, and no records returns an empty map.
func TestCloudResourceMonitor_GetResourceAvailabilities(t *testing.T) {
	monitor := NewCloudResourceMonitor()

	// No records at all: every type is considered available; returns an empty
	// map.
	assert.Empty(t, monitor.GetResourceAvailabilities())

	// One unavailable type: score = 1 - ts/ts = 0.
	monitor.lastUnavailableTimestamp["type-a"] = 100.0
	scores := monitor.GetResourceAvailabilities()
	assert.Len(t, scores, 1)
	assert.EqualValues(t, 0, scores["type-a"])

	// Two unavailable types: type-a failed earlier (t=100), type-b failed most
	// recently (t=200).
	monitor.lastUnavailableTimestamp["type-b"] = 200.0
	scores = monitor.GetResourceAvailabilities()
	assert.Len(t, scores, 2)
	assert.EqualValues(t, 0, scores["type-b"])
	assert.EqualValues(t, 0.5, scores["type-a"])
}
