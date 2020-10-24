// Copyright 2017 The Ray Authors.
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

#pragma once

#include <utility>

#include "ray/common/asio/instrumented_io_context.h"
#include "ray/gcs/store_client/in_memory_store_client.h"
#include "ray/gcs/store_client/redis_store_client.h"
#include "src/ray/protobuf/gcs.pb.h"

namespace ray {
namespace gcs {

using rpc::ActorTableData;
using rpc::ErrorTableData;
using rpc::GcsNodeInfo;
using rpc::HeartbeatTableData;
using rpc::JobTableData;
using rpc::ObjectLocationInfo;
using rpc::ObjectTableData;
using rpc::PlacementGroupTableData;
using rpc::ProfileTableData;
using rpc::ResourceMap;
using rpc::ResourceTableData;
using rpc::ResourceUsageBatchData;
using rpc::ScheduleData;
using rpc::StoredConfig;
using rpc::TaskLeaseData;
using rpc::TaskReconstructionData;
using rpc::TaskSpec;
using rpc::TaskTableData;
using rpc::WorkerTableData;

/// \class GcsTable
///
/// GcsTable is the storage interface for all GCS tables whose data do not belong to
/// specific jobs. This class is not meant to be used directly. All gcs table classes
/// without job id should derive from this class and override the table_name_ member with
/// a unique value for that table.
template <typename Key, typename Data>
class GcsTable {
 public:
  explicit GcsTable(std::shared_ptr<StoreClient> &store_client)
      : store_client_(store_client) {}

  virtual ~GcsTable() = default;

  /// Write data to the table asynchronously.
  ///
  /// \param key The key that will be written to the table.
  /// \param value The value of the key that will be written to the table.
  /// \param callback Callback that will be called after write finishes.
  /// \return Status
  virtual Status Put(const Key &key, const Data &value, const StatusCallback &callback);

  /// Get data from the table asynchronously.
  ///
  /// \param key The key to lookup from the table.
  /// \param callback Callback that will be called after read finishes.
  /// \return Status
  Status Get(const Key &key, const OptionalItemCallback<Data> &callback);

  /// Get all data from the table asynchronously.
  ///
  /// \param callback Callback that will be called after data has been received.
  /// \return Status
  Status GetAll(const MapCallback<Key, Data> &callback);

  /// Delete data from the table asynchronously.
  ///
  /// \param key The key that will be deleted from the table.
  /// \param callback Callback that will be called after delete finishes.
  /// \return Status
  virtual Status Delete(const Key &key, const StatusCallback &callback);

  /// Delete a batch of data from the table asynchronously.
  ///
  /// \param keys The batch key that will be deleted from the table.
  /// \param callback Callback that will be called after delete finishes.
  /// \return Status
  virtual Status BatchDelete(const std::vector<Key> &keys,
                             const StatusCallback &callback);

 protected:
  std::string table_name_;
  std::shared_ptr<StoreClient> store_client_;
};

/// \class GcsTableWithJobId
///
/// GcsTableWithJobId is the storage interface for all GCS tables whose data belongs to
/// specific jobs. This class is not meant to be used directly. All gcs table classes with
/// job id should derive from this class and override the table_name_ member with a unique
/// value for that table.
template <typename Key, typename Data>
class GcsTableWithJobId : public GcsTable<Key, Data> {
 public:
  explicit GcsTableWithJobId(std::shared_ptr<StoreClient> &store_client)
      : GcsTable<Key, Data>(store_client) {}

  /// Write data to the table asynchronously.
  ///
  /// \param key The key that will be written to the table. The job id can be obtained
  /// from the key. \param value The value of the key that will be written to the table.
  /// \param callback Callback that will be called after write finishes.
  /// \return Status
  Status Put(const Key &key, const Data &value, const StatusCallback &callback) override;

  /// Get all the data of the specified job id from the table asynchronously.
  ///
  /// \param job_id The key to lookup from the table.
  /// \param callback Callback that will be called after read finishes.
  /// \return Status
  Status GetByJobId(const JobID &job_id, const MapCallback<Key, Data> &callback);

  /// Delete all the data of the specified job id from the table asynchronously.
  ///
  /// \param job_id The key that will be deleted from the table.
  /// \param callback Callback that will be called after delete finishes.
  /// \return Status
  Status DeleteByJobId(const JobID &job_id, const StatusCallback &callback);

  /// Delete data and index from the table asynchronously.
  ///
  /// \param key The key that will be deleted from the table.
  /// \param callback Callback that will be called after delete finishes.
  /// \return Status
  Status Delete(const Key &key, const StatusCallback &callback) override;

  /// Delete a batch of data and index from the table asynchronously.
  ///
  /// \param keys The batch key that will be deleted from the table.
  /// \param callback Callback that will be called after delete finishes.
  /// \return Status
  Status BatchDelete(const std::vector<Key> &keys,
                     const StatusCallback &callback) override;

 protected:
  virtual JobID GetJobIdFromKey(const Key &key) = 0;
};

class GcsJobTable : public GcsTable<JobID, JobTableData> {
 public:
  explicit GcsJobTable(std::shared_ptr<StoreClient> &store_client)
      : GcsTable(store_client) {
    table_name_ = TablePrefix_Name(TablePrefix::JOB);
  }
};

class GcsActorTable : public GcsTableWithJobId<ActorID, ActorTableData> {
 public:
  explicit GcsActorTable(std::shared_ptr<StoreClient> &store_client)
      : GcsTableWithJobId(store_client) {
    table_name_ = TablePrefix_Name(TablePrefix::ACTOR);
  }

 private:
  JobID GetJobIdFromKey(const ActorID &key) override { return key.JobId(); }
};

class GcsActorTaskSpecTable : public GcsTableWithJobId<ActorID, TaskSpec> {
 public:
  explicit GcsActorTaskSpecTable(std::shared_ptr<StoreClient> &store_client)
      : GcsTableWithJobId(store_client) {
    table_name_ = TablePrefix_Name(TablePrefix::ACTOR_TASK_SPEC);
  }

 private:
  JobID GetJobIdFromKey(const ActorID &key) override { return key.JobId(); }
};

class GcsPlacementGroupTable
    : public GcsTable<PlacementGroupID, PlacementGroupTableData> {
 public:
  explicit GcsPlacementGroupTable(std::shared_ptr<StoreClient> &store_client)
      : GcsTable(store_client) {
    table_name_ = TablePrefix_Name(TablePrefix::PLACEMENT_GROUP);
  }
};

class GcsTaskTable : public GcsTableWithJobId<TaskID, TaskTableData> {
 public:
  explicit GcsTaskTable(std::shared_ptr<StoreClient> &store_client)
      : GcsTableWithJobId(store_client) {
    table_name_ = TablePrefix_Name(TablePrefix::TASK);
  }

 private:
  JobID GetJobIdFromKey(const TaskID &key) override { return key.ActorId().JobId(); }
};

class GcsTaskLeaseTable : public GcsTableWithJobId<TaskID, TaskLeaseData> {
 public:
  explicit GcsTaskLeaseTable(std::shared_ptr<StoreClient> &store_client)
      : GcsTableWithJobId(store_client) {
    table_name_ = TablePrefix_Name(TablePrefix::TASK_LEASE);
  }

 private:
  JobID GetJobIdFromKey(const TaskID &key) override { return key.ActorId().JobId(); }
};

class GcsTaskReconstructionTable
    : public GcsTableWithJobId<TaskID, TaskReconstructionData> {
 public:
  explicit GcsTaskReconstructionTable(std::shared_ptr<StoreClient> &store_client)
      : GcsTableWithJobId(store_client) {
    table_name_ = TablePrefix_Name(TablePrefix::TASK_RECONSTRUCTION);
  }

 private:
  JobID GetJobIdFromKey(const TaskID &key) override { return key.ActorId().JobId(); }
};

class GcsObjectTable : public GcsTableWithJobId<ObjectID, ObjectLocationInfo> {
 public:
  explicit GcsObjectTable(std::shared_ptr<StoreClient> &store_client)
      : GcsTableWithJobId(store_client) {
    table_name_ = TablePrefix_Name(TablePrefix::OBJECT);
  }

 private:
  JobID GetJobIdFromKey(const ObjectID &key) override { return key.TaskId().JobId(); }
};

class GcsNodeTable : public GcsTable<NodeID, GcsNodeInfo> {
 public:
  explicit GcsNodeTable(std::shared_ptr<StoreClient> &store_client)
      : GcsTable(store_client) {
    table_name_ = TablePrefix_Name(TablePrefix::NODE);
  }
};

class GcsNodeResourceTable : public GcsTable<NodeID, ResourceMap> {
 public:
  explicit GcsNodeResourceTable(std::shared_ptr<StoreClient> &store_client)
      : GcsTable(store_client) {
    table_name_ = TablePrefix_Name(TablePrefix::NODE_RESOURCE);
  }
};

class GcsHeartbeatTable : public GcsTable<NodeID, HeartbeatTableData> {
 public:
  explicit GcsHeartbeatTable(std::shared_ptr<StoreClient> &store_client)
      : GcsTable(store_client) {
    table_name_ = TablePrefix_Name(TablePrefix::HEARTBEAT);
  }
};

class GcsPlacementGroupScheduleTable : public GcsTable<PlacementGroupID, ScheduleData> {
 public:
  explicit GcsPlacementGroupScheduleTable(std::shared_ptr<StoreClient> &store_client)
      : GcsTable(store_client) {
    table_name_ = TablePrefix_Name(TablePrefix::PLACEMENT_GROUP_SCHEDULE);
  }
};

class GcsResourceUsageBatchTable : public GcsTable<NodeID, ResourceUsageBatchData> {
 public:
  explicit GcsResourceUsageBatchTable(std::shared_ptr<StoreClient> &store_client)
      : GcsTable(store_client) {
    table_name_ = TablePrefix_Name(TablePrefix::RESOURCE_USAGE_BATCH);
  }
};

class GcsProfileTable : public GcsTable<UniqueID, ProfileTableData> {
 public:
  explicit GcsProfileTable(std::shared_ptr<StoreClient> &store_client)
      : GcsTable(store_client) {
    table_name_ = TablePrefix_Name(TablePrefix::PROFILE);
  }
};

class GcsWorkerTable : public GcsTable<WorkerID, WorkerTableData> {
 public:
  explicit GcsWorkerTable(std::shared_ptr<StoreClient> &store_client)
      : GcsTable(store_client) {
    table_name_ = TablePrefix_Name(TablePrefix::WORKERS);
  }
};

class GcsInternalConfigTable : public GcsTable<UniqueID, StoredConfig> {
 public:
  explicit GcsInternalConfigTable(std::shared_ptr<StoreClient> &store_client)
      : GcsTable(store_client) {
    table_name_ = TablePrefix_Name(TablePrefix::INTERNAL_CONFIG);
  }
};

/// \class GcsTableStorage
///
/// This class is not meant to be used directly. All gcs table storage classes should
/// derive from this class and override class member variables.
class GcsTableStorage {
 public:
  GcsJobTable &JobTable() {
    RAY_CHECK(job_table_ != nullptr);
    return *job_table_;
  }

  GcsActorTable &ActorTable() {
    RAY_CHECK(actor_table_ != nullptr);
    return *actor_table_;
  }

  GcsActorTaskSpecTable &ActorTaskSpecTable() {
    RAY_CHECK(actor_task_spec_table_ != nullptr);
    return *actor_task_spec_table_;
  }

  GcsPlacementGroupTable &PlacementGroupTable() {
    RAY_CHECK(placement_group_table_ != nullptr);
    return *placement_group_table_;
  }

  GcsTaskTable &TaskTable() {
    RAY_CHECK(task_table_ != nullptr);
    return *task_table_;
  }

  GcsTaskLeaseTable &TaskLeaseTable() {
    RAY_CHECK(task_lease_table_ != nullptr);
    return *task_lease_table_;
  }

  GcsTaskReconstructionTable &TaskReconstructionTable() {
    RAY_CHECK(task_reconstruction_table_ != nullptr);
    return *task_reconstruction_table_;
  }

  GcsObjectTable &ObjectTable() {
    RAY_CHECK(object_table_ != nullptr);
    return *object_table_;
  }

  GcsNodeTable &NodeTable() {
    RAY_CHECK(node_table_ != nullptr);
    return *node_table_;
  }

  GcsNodeResourceTable &NodeResourceTable() {
    RAY_CHECK(node_resource_table_ != nullptr);
    return *node_resource_table_;
  }

  GcsPlacementGroupScheduleTable &PlacementGroupScheduleTable() {
    RAY_CHECK(placement_group_schedule_table_ != nullptr);
    return *placement_group_schedule_table_;
  }

  GcsHeartbeatTable &HeartbeatTable() {
    RAY_CHECK(heartbeat_table_ != nullptr);
    return *heartbeat_table_;
  }

  GcsResourceUsageBatchTable &HeartbeatBatchTable() {
    RAY_CHECK(resource_usage_batch_table_ != nullptr);
    return *resource_usage_batch_table_;
  }

  GcsProfileTable &ProfileTable() {
    RAY_CHECK(profile_table_ != nullptr);
    return *profile_table_;
  }

  GcsWorkerTable &WorkerTable() {
    RAY_CHECK(worker_table_ != nullptr);
    return *worker_table_;
  }

  GcsInternalConfigTable &InternalConfigTable() {
    RAY_CHECK(system_config_table_ != nullptr);
    return *system_config_table_;
  }

 protected:
  std::shared_ptr<StoreClient> store_client_;
  std::unique_ptr<GcsJobTable> job_table_;
  std::unique_ptr<GcsActorTable> actor_table_;
  std::unique_ptr<GcsActorTaskSpecTable> actor_task_spec_table_;
  std::unique_ptr<GcsPlacementGroupTable> placement_group_table_;
  std::unique_ptr<GcsTaskTable> task_table_;
  std::unique_ptr<GcsTaskLeaseTable> task_lease_table_;
  std::unique_ptr<GcsTaskReconstructionTable> task_reconstruction_table_;
  std::unique_ptr<GcsObjectTable> object_table_;
  std::unique_ptr<GcsNodeTable> node_table_;
  std::unique_ptr<GcsNodeResourceTable> node_resource_table_;
  std::unique_ptr<GcsPlacementGroupScheduleTable> placement_group_schedule_table_;
  std::unique_ptr<GcsHeartbeatTable> heartbeat_table_;
  std::unique_ptr<GcsResourceUsageBatchTable> resource_usage_batch_table_;
  std::unique_ptr<GcsProfileTable> profile_table_;
  std::unique_ptr<GcsWorkerTable> worker_table_;
  std::unique_ptr<GcsInternalConfigTable> system_config_table_;
};

/// \class RedisGcsTableStorage
/// RedisGcsTableStorage is an implementation of `GcsTableStorage`
/// that uses redis as storage.
class RedisGcsTableStorage : public GcsTableStorage {
 public:
  explicit RedisGcsTableStorage(std::shared_ptr<RedisClient> redis_client) {
    store_client_ = std::make_shared<RedisStoreClient>(redis_client);
    job_table_.reset(new GcsJobTable(store_client_));
    actor_table_.reset(new GcsActorTable(store_client_));
    actor_task_spec_table_.reset(new GcsActorTaskSpecTable(store_client_));
    placement_group_table_.reset(new GcsPlacementGroupTable(store_client_));
    task_table_.reset(new GcsTaskTable(store_client_));
    task_lease_table_.reset(new GcsTaskLeaseTable(store_client_));
    task_reconstruction_table_.reset(new GcsTaskReconstructionTable(store_client_));
    object_table_.reset(new GcsObjectTable(store_client_));
    node_table_.reset(new GcsNodeTable(store_client_));
    node_resource_table_.reset(new GcsNodeResourceTable(store_client_));
    placement_group_schedule_table_.reset(
        new GcsPlacementGroupScheduleTable(store_client_));
    heartbeat_table_.reset(new GcsHeartbeatTable(store_client_));
    placement_group_schedule_table_.reset(
        new GcsPlacementGroupScheduleTable(store_client_));
    resource_usage_batch_table_.reset(new GcsResourceUsageBatchTable(store_client_));
    profile_table_.reset(new GcsProfileTable(store_client_));
    worker_table_.reset(new GcsWorkerTable(store_client_));
    system_config_table_.reset(new GcsInternalConfigTable(store_client_));
  }
};

/// \class InMemoryGcsTableStorage
/// InMemoryGcsTableStorage is an implementation of `GcsTableStorage`
/// that uses memory as storage.
class InMemoryGcsTableStorage : public GcsTableStorage {
 public:
  explicit InMemoryGcsTableStorage(instrumented_io_context &main_io_service) {
    store_client_ = std::make_shared<InMemoryStoreClient>(main_io_service);
    job_table_.reset(new GcsJobTable(store_client_));
    actor_table_.reset(new GcsActorTable(store_client_));
    actor_task_spec_table_.reset(new GcsActorTaskSpecTable(store_client_));
    placement_group_table_.reset(new GcsPlacementGroupTable(store_client_));
    task_table_.reset(new GcsTaskTable(store_client_));
    task_lease_table_.reset(new GcsTaskLeaseTable(store_client_));
    task_reconstruction_table_.reset(new GcsTaskReconstructionTable(store_client_));
    object_table_.reset(new GcsObjectTable(store_client_));
    node_table_.reset(new GcsNodeTable(store_client_));
    node_resource_table_.reset(new GcsNodeResourceTable(store_client_));
    placement_group_schedule_table_.reset(
        new GcsPlacementGroupScheduleTable(store_client_));
    heartbeat_table_.reset(new GcsHeartbeatTable(store_client_));
    resource_usage_batch_table_.reset(new GcsResourceUsageBatchTable(store_client_));
    profile_table_.reset(new GcsProfileTable(store_client_));
    worker_table_.reset(new GcsWorkerTable(store_client_));
    system_config_table_.reset(new GcsInternalConfigTable(store_client_));
  }
};

}  // namespace gcs
}  // namespace ray
