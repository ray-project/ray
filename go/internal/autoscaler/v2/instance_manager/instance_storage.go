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

	"github.com/ray-project/ray/go/proto"
	protolib "google.golang.org/protobuf/proto"
)

// InstanceStorage is the instance storage, wrapping Instance persistence on
// top of IStorage.
// Corresponds to Python instance_manager.instance_storage.InstanceStorage:
// the serialized Instance is the stored value; reads and writes maintain the
// mapping between the instance version and the storage version.
type InstanceStorage struct {
	storage   IStorage
	clusterID string
	tableName string
}

// NewInstanceStorage creates the instance storage.
// The table name has the format instance_table@{clusterID}, matching Python.
func NewInstanceStorage(clusterID string, storage IStorage) *InstanceStorage {
	return &InstanceStorage{
		storage:   storage,
		clusterID: clusterID,
		tableName: fmt.Sprintf("instance_table@%s", clusterID),
	}
}

// BatchUpsertInstances batch inserts or updates instances.
// When expectedStorageVersion is non-nil and does not match the current
// storage version, the call fails.
// Note: the version of written instances is set to 0 and backfilled from the
// storage entry version on read.
func (s *InstanceStorage) BatchUpsertInstances(updates []*proto.Instance, expectedStorageVersion *int64) StoreStatus {
	// Version precheck (matching Python; BatchUpdate validates again
	// internally).
	if expectedStorageVersion != nil && *expectedStorageVersion != s.storage.GetVersion() {
		return StoreStatus{Success: false, Version: s.storage.GetVersion()}
	}

	mutations := make(map[string][]byte, len(updates))
	for _, instance := range updates {
		// Deep copy and zero the version, so the caller-held instance is not
		// mutated.
		copied := protolib.Clone(instance).(*proto.Instance)
		copied.Version = 0
		data, err := protolib.Marshal(copied)
		if err != nil {
			return StoreStatus{Success: false, Version: s.storage.GetVersion()}
		}
		mutations[copied.InstanceId] = data
	}

	return s.storage.BatchUpdate(s.tableName, mutations, nil, expectedStorageVersion)
}

// UpsertInstance inserts or updates a single instance.
// When expectedInstanceVersion/expectedStorageVersion are non-nil, the
// corresponding version checks run.
// Note: the version of the written instance is set to 0 and backfilled from
// the storage entry version on read.
func (s *InstanceStorage) UpsertInstance(instance *proto.Instance, expectedInstanceVersion *int64, expectedStorageVersion *int64) StoreStatus {
	copied := protolib.Clone(instance).(*proto.Instance)
	copied.Version = 0
	data, err := protolib.Marshal(copied)
	if err != nil {
		return StoreStatus{Success: false, Version: s.storage.GetVersion()}
	}

	return s.storage.Update(s.tableName, copied.InstanceId, data,
		expectedInstanceVersion, expectedStorageVersion, false)
}

// GetInstances reads instances.
// When instanceIDs is empty, all instances are returned; when statusFilter is
// non-nil, only instances in the given statuses are returned.
// Returns the instance table (keyed by instance ID) and the current storage
// version; the instance version has been backfilled from the entry version.
func (s *InstanceStorage) GetInstances(instanceIDs []string, statusFilter map[proto.Instance_InstanceStatus]bool) (map[string]*proto.Instance, int64) {
	pairs, version := s.storage.Get(s.tableName, instanceIDs)
	instances := make(map[string]*proto.Instance, len(pairs))
	for instanceID, entry := range pairs {
		instance := &proto.Instance{}
		if err := protolib.Unmarshal(entry.Value, instance); err != nil {
			continue
		}
		// Backfill the instance version from the storage entry version.
		instance.Version = entry.Version
		if statusFilter != nil && !statusFilter[instance.Status] {
			continue
		}
		instances[instanceID] = instance
	}
	return instances, version
}

// BatchDeleteInstances batch deletes instances.
// When expectedStorageVersion is non-nil and does not match the current
// storage version, the call fails.
func (s *InstanceStorage) BatchDeleteInstances(instanceIDs []string, expectedStorageVersion *int64) StoreStatus {
	if expectedStorageVersion != nil && *expectedStorageVersion != s.storage.GetVersion() {
		return StoreStatus{Success: false, Version: s.storage.GetVersion()}
	}

	return s.storage.BatchUpdate(s.tableName, nil, instanceIDs, expectedStorageVersion)
}
