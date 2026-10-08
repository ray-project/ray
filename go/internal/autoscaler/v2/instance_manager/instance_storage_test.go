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

// newTestInstance builds an instance for tests.
func newTestInstance(id string, status proto.Instance_InstanceStatus) *proto.Instance {
	return &proto.Instance{
		InstanceId: id,
		Status:     status,
		NodeKind:   proto.NodeKind_WORKER,
	}
}

// TestInstanceStorage_UpsertAndGet is the write/read round trip:
// the version is zeroed on write and backfilled from the storage entry version
// on read.
func TestInstanceStorage_UpsertAndGet(t *testing.T) {
	storage := NewInstanceStorage("cluster-1", NewInMemoryStorage())

	status := storage.UpsertInstance(newTestInstance("i-1", proto.Instance_QUEUED), nil, nil)
	assert.True(t, status.Success)
	assert.EqualValues(t, 1, status.Version)

	instances, version := storage.GetInstances(nil, nil)
	assert.EqualValues(t, 1, version)
	assert.Len(t, instances, 1)

	instance := instances["i-1"]
	// The instance fields are read back correctly.
	assert.Equal(t, proto.Instance_QUEUED, instance.Status)
	// The version is backfilled from the storage entry version (not the value
	// at write time).
	assert.EqualValues(t, 1, instance.Version)

	// The version changes after an update.
	updated := newTestInstance("i-1", proto.Instance_REQUESTED)
	updated.Version = 999
	status = storage.UpsertInstance(updated, nil, nil)
	assert.True(t, status.Success)
	assert.EqualValues(t, 2, status.Version)

	instances, _ = storage.GetInstances(nil, nil)
	assert.EqualValues(t, 2, instances["i-1"].Version)
	assert.Equal(t, proto.Instance_REQUESTED, instances["i-1"].Status)

	// The original instance is not mutated by the write (deep copy semantics).
	assert.EqualValues(t, 999, updated.Version)
}

// TestInstanceStorage_UpsertVersionChecks covers the instance and storage
// version checks of upsert.
func TestInstanceStorage_UpsertVersionChecks(t *testing.T) {
	storage := NewInstanceStorage("cluster-1", NewInMemoryStorage())
	storage.UpsertInstance(newTestInstance("i-1", proto.Instance_QUEUED), nil, nil)

	// Instance version mismatch: fails.
	stale := int64(0)
	status := storage.UpsertInstance(newTestInstance("i-1", proto.Instance_REQUESTED), &stale, nil)
	assert.False(t, status.Success)

	// Instance version matches: succeeds.
	current := int64(1)
	status = storage.UpsertInstance(newTestInstance("i-1", proto.Instance_REQUESTED), &current, nil)
	assert.True(t, status.Success)

	// Storage version mismatch: fails.
	storageVersion := int64(1)
	status = storage.UpsertInstance(newTestInstance("i-1", proto.Instance_ALLOCATED), nil, &storageVersion)
	assert.False(t, status.Success)
}

// TestInstanceStorage_BatchUpsert covers batch inserts and filtered reads.
func TestInstanceStorage_BatchUpsert(t *testing.T) {
	storage := NewInstanceStorage("cluster-1", NewInMemoryStorage())

	status := storage.BatchUpsertInstances([]*proto.Instance{
		newTestInstance("i-1", proto.Instance_QUEUED),
		newTestInstance("i-2", proto.Instance_REQUESTED),
		newTestInstance("i-3", proto.Instance_RAY_RUNNING),
	}, nil)
	assert.True(t, status.Success)
	assert.EqualValues(t, 1, status.Version)

	// Filter by status.
	instances, _ := storage.GetInstances(nil, map[proto.Instance_InstanceStatus]bool{
		proto.Instance_QUEUED:    true,
		proto.Instance_REQUESTED: true,
	})
	assert.Len(t, instances, 2)
	assert.Contains(t, instances, "i-1")
	assert.Contains(t, instances, "i-2")
	assert.NotContains(t, instances, "i-3")

	// Read by instance ID.
	instances, _ = storage.GetInstances([]string{"i-1", "missing"}, nil)
	assert.Len(t, instances, 1)
	assert.Contains(t, instances, "i-1")

	// Expected storage version mismatch: fails.
	stale := int64(0)
	status = storage.BatchUpsertInstances([]*proto.Instance{newTestInstance("i-4", proto.Instance_QUEUED)}, &stale)
	assert.False(t, status.Success)
}

// TestInstanceStorage_BatchDelete covers batch deletions.
func TestInstanceStorage_BatchDelete(t *testing.T) {
	storage := NewInstanceStorage("cluster-1", NewInMemoryStorage())
	storage.BatchUpsertInstances([]*proto.Instance{
		newTestInstance("i-1", proto.Instance_QUEUED),
		newTestInstance("i-2", proto.Instance_QUEUED),
	}, nil)

	status := storage.BatchDeleteInstances([]string{"i-1"}, nil)
	assert.True(t, status.Success)

	instances, _ := storage.GetInstances(nil, nil)
	assert.NotContains(t, instances, "i-1")
	assert.Contains(t, instances, "i-2")

	// Expected storage version mismatch: fails.
	stale := int64(0)
	status = storage.BatchDeleteInstances([]string{"i-2"}, &stale)
	assert.False(t, status.Success)
	instances, _ = storage.GetInstances(nil, nil)
	assert.Contains(t, instances, "i-2")
}

// TestInstanceStorage_TablePerCluster verifies different cluster IDs are
// isolated into different tables.
func TestInstanceStorage_TablePerCluster(t *testing.T) {
	backend := NewInMemoryStorage()
	storageA := NewInstanceStorage("cluster-a", backend)
	storageB := NewInstanceStorage("cluster-b", backend)

	storageA.UpsertInstance(newTestInstance("i-1", proto.Instance_QUEUED), nil, nil)
	storageB.UpsertInstance(newTestInstance("i-2", proto.Instance_QUEUED), nil, nil)

	instancesA, _ := storageA.GetInstances(nil, nil)
	instancesB, _ := storageB.GetInstances(nil, nil)
	assert.Len(t, instancesA, 1)
	assert.Contains(t, instancesA, "i-1")
	assert.Len(t, instancesB, 1)
	assert.Contains(t, instancesB, "i-2")
}
