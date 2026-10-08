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
	"testing"

	"github.com/ray-project/ray/go/proto"
	"github.com/stretchr/testify/assert"
)

// mockSubscriber records the received events, corresponding to Python
// tests/util.py's MockSubscriber.
type mockSubscriber struct {
	events []*proto.InstanceUpdateEvent
}

func (m *mockSubscriber) Notify(events []*proto.InstanceUpdateEvent) {
	m.events = append(m.events, events...)
}

func (m *mockSubscriber) clear() {
	m.events = nil
}

// fakeIMStorage is a controllable storage fake used to simulate version
// mismatch and similar scenarios.
type fakeIMStorage struct {
	getVersion  int64                     // the version returned by Get/GetAll/GetVersion
	batchStatus StoreStatus               // the result returned by BatchUpdate/Update
	entries     map[string]VersionedValue // the entries returned by Get (may be nil)
}

func (f *fakeIMStorage) BatchUpdate(table string, mutation map[string][]byte, deletion []string, expectedStorageVersion *int64) StoreStatus {
	return f.batchStatus
}

func (f *fakeIMStorage) Update(table string, key string, value []byte, expectedEntryVersion *int64, expectedStorageVersion *int64, insertOnly bool) StoreStatus {
	return f.batchStatus
}

func (f *fakeIMStorage) GetAll(table string) (map[string]VersionedValue, int64) {
	return f.entries, f.getVersion
}

func (f *fakeIMStorage) Get(table string, keys []string) (map[string]VersionedValue, int64) {
	if len(keys) == 0 {
		return f.entries, f.getVersion
	}
	result := make(map[string]VersionedValue, len(keys))
	for _, key := range keys {
		if v, ok := f.entries[key]; ok {
			result[key] = v
		}
	}
	return result, f.getVersion
}

func (f *fakeIMStorage) GetVersion() int64 {
	return f.getVersion
}

func strPtr(s string) *string { return &s }

func nodeKindPtr(k proto.NodeKind) *proto.NodeKind { return &k }

// newTestIM builds an InstanceManager on the in-memory storage.
func newTestIM(subscribers ...InstanceUpdatedSubscriber) (*InstanceManager, *InstanceStorage) {
	storage := NewInstanceStorage("cluster-id", NewInMemoryStorage())
	return NewInstanceManager(storage, subscribers), storage
}

// TestInstanceManager_VersionMismatch covers the version mismatch scenarios.
// Corresponds to Python test_instances_version_mismatch.
func TestInstanceManager_VersionMismatch(t *testing.T) {
	storage := &fakeIMStorage{
		getVersion:  1,
		batchStatus: StoreStatus{Success: true, Version: 1},
		entries:     map[string]VersionedValue{},
	}
	subscriber := &mockSubscriber{}
	im := NewInstanceManager(NewInstanceStorage("cluster-id", storage), []InstanceUpdatedSubscriber{subscriber})

	update := &proto.InstanceUpdateEvent{
		InstanceId:        "id-1",
		NewInstanceStatus: proto.Instance_QUEUED,
		InstanceType:      strPtr("type-1"),
		Upsert:            true,
	}

	// Version mismatch while reading the storage.
	reply := im.UpdateInstanceManagerState(&proto.UpdateInstanceManagerStateRequest{
		ExpectedVersion: 0,
		Updates:         []*proto.InstanceUpdateEvent{update},
	})
	assert.Equal(t, proto.StatusCode_VERSION_MISMATCH, reply.Status.Code)
	assert.Empty(t, subscriber.events)

	// Version matches; the update succeeds.
	reply = im.UpdateInstanceManagerState(&proto.UpdateInstanceManagerStateRequest{
		ExpectedVersion: 1,
		Updates:         []*proto.InstanceUpdateEvent{update},
	})
	assert.Equal(t, proto.StatusCode_OK, reply.Status.Code)
	assert.Len(t, subscriber.events, 1)
	assert.Equal(t, proto.Instance_QUEUED, subscriber.events[0].NewInstanceStatus)

	// Version mismatch while writing the storage (concurrent race).
	storage.batchStatus = StoreStatus{Success: false, Version: 2}
	subscriber.clear()
	reply = im.UpdateInstanceManagerState(&proto.UpdateInstanceManagerStateRequest{
		ExpectedVersion: 1,
		Updates:         []*proto.InstanceUpdateEvent{update},
	})
	assert.Equal(t, proto.StatusCode_VERSION_MISMATCH, reply.Status.Code)
	assert.Empty(t, subscriber.events)

	// A storage error other than a version mismatch.
	storage.batchStatus = StoreStatus{Success: false, Version: 1}
	reply = im.UpdateInstanceManagerState(&proto.UpdateInstanceManagerStateRequest{
		ExpectedVersion: 1,
		Updates:         []*proto.InstanceUpdateEvent{update},
	})
	assert.Equal(t, proto.StatusCode_UNKNOWN_ERRORS, reply.Status.Code)
	assert.Empty(t, subscriber.events)
}

// TestInstanceManager_GetAndUpdate covers insert/update/query and invalid
// transitions.
// Corresponds to Python test_get_and_updates.
func TestInstanceManager_GetAndUpdate(t *testing.T) {
	subscriber := &mockSubscriber{}
	im, _ := newTestIM(subscriber)

	// Empty storage.
	getReply := im.GetInstanceManagerState(&proto.GetInstanceManagerStateRequest{})
	assert.Equal(t, proto.StatusCode_OK, getReply.Status.Code)
	assert.Empty(t, getReply.State.Instances)

	// Queue 3 nodes for launch.
	reply := im.UpdateInstanceManagerState(&proto.UpdateInstanceManagerStateRequest{
		ExpectedVersion: 0,
		Updates: []*proto.InstanceUpdateEvent{
			{InstanceId: "id-1", NewInstanceStatus: proto.Instance_QUEUED, InstanceType: strPtr("type-1"), Upsert: true},
			{InstanceId: "id-2", NewInstanceStatus: proto.Instance_QUEUED, InstanceType: strPtr("type-2"), Upsert: true},
			{InstanceId: "id-3", NewInstanceStatus: proto.Instance_QUEUED, InstanceType: strPtr("type-2"), Upsert: true},
		},
	})
	assert.Equal(t, proto.StatusCode_OK, reply.Status.Code)
	assert.Len(t, subscriber.events, 3)
	for _, e := range subscriber.events {
		assert.Equal(t, proto.Instance_QUEUED, e.NewInstanceStatus)
	}

	// Query the launched nodes.
	getReply = im.GetInstanceManagerState(&proto.GetInstanceManagerStateRequest{})
	assert.Equal(t, proto.StatusCode_OK, getReply.Status.Code)
	assert.Len(t, getReply.State.Instances, 3)

	typesCount := map[string]int{}
	statusByID := map[string]proto.Instance_InstanceStatus{}
	for _, ins := range getReply.State.Instances {
		typesCount[ins.InstanceType]++
		statusByID[ins.InstanceId] = ins.Status
		assert.Equal(t, proto.Instance_QUEUED, ins.Status)
	}
	assert.Equal(t, 1, typesCount["type-1"])
	assert.Equal(t, 2, typesCount["type-2"])

	// Update the node statuses.
	subscriber.clear()
	reply = im.UpdateInstanceManagerState(&proto.UpdateInstanceManagerStateRequest{
		ExpectedVersion: 1,
		Updates: []*proto.InstanceUpdateEvent{
			{InstanceId: "id-1", NewInstanceStatus: proto.Instance_REQUESTED, InstanceType: strPtr("type-1"), LaunchRequestId: strPtr("l1")},
			{InstanceId: "id-2", NewInstanceStatus: proto.Instance_REQUESTED, InstanceType: strPtr("type-1"), LaunchRequestId: strPtr("l1")},
		},
	})
	assert.Equal(t, proto.StatusCode_OK, reply.Status.Code)
	assert.Len(t, subscriber.events, 2)
	for _, e := range subscriber.events {
		assert.Equal(t, proto.Instance_REQUESTED, e.NewInstanceStatus)
	}

	// Query the updated nodes.
	getReply = im.GetInstanceManagerState(&proto.GetInstanceManagerStateRequest{})
	assert.Equal(t, proto.StatusCode_OK, getReply.Status.Code)
	assert.Len(t, getReply.State.Instances, 3)
	for _, ins := range getReply.State.Instances {
		if ins.InstanceId == "id-1" || ins.InstanceId == "id-2" {
			assert.Equal(t, proto.Instance_REQUESTED, ins.Status)
		} else {
			assert.Equal(t, proto.Instance_QUEUED, ins.Status)
		}
	}

	// Invalid status transition (QUEUED cannot go to RAY_RUNNING directly);
	// the whole batch is rolled back.
	subscriber.clear()
	reply = im.UpdateInstanceManagerState(&proto.UpdateInstanceManagerStateRequest{
		ExpectedVersion: 2,
		Updates: []*proto.InstanceUpdateEvent{
			{InstanceId: "id-3", NewInstanceStatus: proto.Instance_RAY_RUNNING},
		},
	})
	assert.Equal(t, proto.StatusCode_UNKNOWN_ERRORS, reply.Status.Code)
	assert.NotEmpty(t, reply.Status.Message)
	assert.Empty(t, subscriber.events)

	// A stale version.
	reply = im.UpdateInstanceManagerState(&proto.UpdateInstanceManagerStateRequest{
		ExpectedVersion: 0,
		Updates: []*proto.InstanceUpdateEvent{
			{InstanceId: "id-3", NewInstanceStatus: proto.Instance_REQUESTED, InstanceType: strPtr("type-2")},
		},
	})
	assert.Equal(t, proto.StatusCode_VERSION_MISMATCH, reply.Status.Code)
	assert.Empty(t, subscriber.events)
}

// TestInstanceManager_Insert covers the validation of new instance inserts.
// Corresponds to Python test_insert.
func TestInstanceManager_Insert(t *testing.T) {
	subscriber := &mockSubscriber{}
	im, _ := newTestIM(subscriber)

	reply := im.UpdateInstanceManagerState(&proto.UpdateInstanceManagerStateRequest{
		ExpectedVersion: 0,
		Updates: []*proto.InstanceUpdateEvent{
			{InstanceId: "id-1", NewInstanceStatus: proto.Instance_QUEUED, InstanceType: strPtr("type-1"), Upsert: true},
			{InstanceId: "id-2", NewInstanceStatus: proto.Instance_TERMINATING, CloudInstanceId: strPtr("cloud-id-2"), Upsert: true},
			{InstanceId: "id-3", NewInstanceStatus: proto.Instance_ALLOCATED, CloudInstanceId: strPtr("cloud-id-3"), NodeKind: nodeKindPtr(proto.NodeKind_WORKER), InstanceType: strPtr("type-3"), Upsert: true},
		},
	})
	assert.Equal(t, proto.StatusCode_OK, reply.Status.Code)

	getReply := im.GetInstanceManagerState(&proto.GetInstanceManagerStateRequest{})
	assert.Len(t, getReply.State.Instances, 3)
	instanceByIDs := map[string]*proto.Instance{}
	for _, ins := range getReply.State.Instances {
		instanceByIDs[ins.InstanceId] = ins
	}
	assert.Equal(t, proto.Instance_QUEUED, instanceByIDs["id-1"].Status)
	assert.Equal(t, "type-1", instanceByIDs["id-1"].InstanceType)
	assert.Equal(t, proto.Instance_TERMINATING, instanceByIDs["id-2"].Status)
	assert.Equal(t, proto.Instance_ALLOCATED, instanceByIDs["id-3"].Status)
	assert.Equal(t, "cloud-id-3", instanceByIDs["id-3"].GetCloudInstanceId())
	version := getReply.State.Version

	// A new instance must carry the upsert flag.
	subscriber.clear()
	reply = im.UpdateInstanceManagerState(&proto.UpdateInstanceManagerStateRequest{
		ExpectedVersion: version,
		Updates: []*proto.InstanceUpdateEvent{
			{InstanceId: "id-999", NewInstanceStatus: proto.Instance_QUEUED, InstanceType: strPtr("type-1")},
		},
	})
	assert.Equal(t, proto.StatusCode_UNKNOWN_ERRORS, reply.Status.Code)
	assert.Empty(t, subscriber.events)

	// Invalid new instance statuses.
	invalidStatuses := []proto.Instance_InstanceStatus{
		proto.Instance_UNKNOWN,
		proto.Instance_REQUESTED,
		proto.Instance_RAY_INSTALLING,
		proto.Instance_RAY_RUNNING,
		proto.Instance_RAY_STOP_REQUESTED,
		proto.Instance_RAY_STOPPING,
		proto.Instance_RAY_STOPPED,
		proto.Instance_ALLOCATION_FAILED,
		proto.Instance_ALLOCATION_TIMEOUT,
		proto.Instance_TERMINATED,
		proto.Instance_TERMINATION_FAILED,
		proto.Instance_RAY_INSTALL_FAILED,
	}
	for _, status := range invalidStatuses {
		subscriber.clear()
		reply = im.UpdateInstanceManagerState(&proto.UpdateInstanceManagerStateRequest{
			ExpectedVersion: version,
			Updates: []*proto.InstanceUpdateEvent{
				{InstanceId: "id-999", NewInstanceStatus: status, Upsert: true},
			},
		})
		assert.Equal(t, proto.StatusCode_UNKNOWN_ERRORS, reply.Status.Code, "status: %s", status.String())
		assert.Empty(t, subscriber.events, "status: %s", status.String())
	}
}

// TestInstanceManager_ApplyUpdate covers the field backfill per status update.
// Corresponds to Python test_apply_update.
func TestInstanceManager_ApplyUpdate(t *testing.T) {
	subscriber := &mockSubscriber{}
	im, _ := newTestIM(subscriber)

	getInstances := func() []*proto.Instance {
		reply := im.GetInstanceManagerState(&proto.GetInstanceManagerStateRequest{})
		return reply.State.Instances
	}

	// Insert a new instance.
	reply := im.UpdateInstanceManagerState(&proto.UpdateInstanceManagerStateRequest{
		ExpectedVersion: 0,
		Updates: []*proto.InstanceUpdateEvent{
			{InstanceId: "id-1", NewInstanceStatus: proto.Instance_QUEUED, InstanceType: strPtr("type-1"), Upsert: true},
		},
	})
	assert.Equal(t, proto.StatusCode_OK, reply.Status.Code)
	instances := getInstances()
	assert.Len(t, instances, 1)
	assert.Equal(t, proto.Instance_QUEUED, instances[0].Status)
	assert.Equal(t, "type-1", instances[0].InstanceType)

	// REQUESTED.
	reply = im.UpdateInstanceManagerState(&proto.UpdateInstanceManagerStateRequest{
		ExpectedVersion: 1,
		Updates: []*proto.InstanceUpdateEvent{
			{InstanceId: "id-1", NewInstanceStatus: proto.Instance_REQUESTED, LaunchRequestId: strPtr("l1"), InstanceType: strPtr("type-1")},
		},
	})
	assert.Equal(t, proto.StatusCode_OK, reply.Status.Code)
	instances = getInstances()
	assert.Len(t, instances, 1)
	assert.Equal(t, proto.Instance_REQUESTED, instances[0].Status)
	assert.Equal(t, "l1", instances[0].LaunchRequestId)

	// ALLOCATED.
	reply = im.UpdateInstanceManagerState(&proto.UpdateInstanceManagerStateRequest{
		ExpectedVersion: 2,
		Updates: []*proto.InstanceUpdateEvent{
			{InstanceId: "id-1", NewInstanceStatus: proto.Instance_ALLOCATED, CloudInstanceId: strPtr("cloud-id-1"), NodeKind: nodeKindPtr(proto.NodeKind_WORKER), InstanceType: strPtr("type-1")},
		},
	})
	assert.Equal(t, proto.StatusCode_OK, reply.Status.Code)
	instances = getInstances()
	assert.Len(t, instances, 1)
	assert.Equal(t, proto.Instance_ALLOCATED, instances[0].Status)
	assert.Equal(t, "cloud-id-1", instances[0].GetCloudInstanceId())

	// RAY_RUNNING.
	reply = im.UpdateInstanceManagerState(&proto.UpdateInstanceManagerStateRequest{
		ExpectedVersion: 3,
		Updates: []*proto.InstanceUpdateEvent{
			{InstanceId: "id-1", NewInstanceStatus: proto.Instance_RAY_RUNNING, RayNodeId: strPtr("ray-node-1")},
		},
	})
	assert.Equal(t, proto.StatusCode_OK, reply.Status.Code)
	instances = getInstances()
	assert.Len(t, instances, 1)
	assert.Equal(t, proto.Instance_RAY_RUNNING, instances[0].Status)
	assert.Equal(t, "ray-node-1", instances[0].GetNodeId())

	// TERMINATED.
	reply = im.UpdateInstanceManagerState(&proto.UpdateInstanceManagerStateRequest{
		ExpectedVersion: 4,
		Updates: []*proto.InstanceUpdateEvent{
			{InstanceId: "id-1", NewInstanceStatus: proto.Instance_TERMINATED},
		},
	})
	assert.Equal(t, proto.StatusCode_OK, reply.Status.Code)
	instances = getInstances()
	assert.Len(t, instances, 1)
	assert.Equal(t, proto.Instance_TERMINATED, instances[0].Status)

	// The status history should record every transition.
	history := instances[0].StatusHistory
	assert.GreaterOrEqual(t, len(history), 5)
	expected := []proto.Instance_InstanceStatus{
		proto.Instance_QUEUED,
		proto.Instance_REQUESTED,
		proto.Instance_ALLOCATED,
		proto.Instance_RAY_RUNNING,
		proto.Instance_TERMINATED,
	}
	for i, status := range expected {
		assert.Equal(t, status, history[i].InstanceStatus)
	}
}

// TestInstanceManager_BatchRollbackOnInvalidUpdate verifies the whole batch
// rolls back when a single update fails validation.
func TestInstanceManager_BatchRollbackOnInvalidUpdate(t *testing.T) {
	subscriber := &mockSubscriber{}
	im, storage := newTestIM(subscriber)

	// The first update is valid, the second is invalid (REQUESTED without a
	// launch_request_id).
	reply := im.UpdateInstanceManagerState(&proto.UpdateInstanceManagerStateRequest{
		ExpectedVersion: 0,
		Updates: []*proto.InstanceUpdateEvent{
			{InstanceId: "id-1", NewInstanceStatus: proto.Instance_QUEUED, InstanceType: strPtr("type-1"), Upsert: true},
			{InstanceId: "id-2", NewInstanceStatus: proto.Instance_QUEUED, InstanceType: strPtr("type-1"), Upsert: true},
		},
	})
	assert.Equal(t, proto.StatusCode_OK, reply.Status.Code)
	subscriber.clear()

	reply = im.UpdateInstanceManagerState(&proto.UpdateInstanceManagerStateRequest{
		ExpectedVersion: 1,
		Updates: []*proto.InstanceUpdateEvent{
			// Valid: QUEUED -> REQUESTED.
			{InstanceId: "id-1", NewInstanceStatus: proto.Instance_REQUESTED, LaunchRequestId: strPtr("l1"), InstanceType: strPtr("type-1")},
			// Invalid: QUEUED -> RAY_RUNNING.
			{InstanceId: "id-2", NewInstanceStatus: proto.Instance_RAY_RUNNING, RayNodeId: strPtr("ray-node-2")},
		},
	})
	assert.Equal(t, proto.StatusCode_UNKNOWN_ERRORS, reply.Status.Code)

	// The whole batch is rolled back: id-1 should still be QUEUED, id-2 should
	// still be QUEUED, and the subscriber is not notified.
	instances, _ := storage.GetInstances(nil, nil)
	assert.Equal(t, proto.Instance_QUEUED, instances["id-1"].Status)
	assert.Equal(t, proto.Instance_QUEUED, instances["id-2"].Status)
	assert.Empty(t, subscriber.events)
}
