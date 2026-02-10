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

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "ray/gcs/store_client/store_client.h"
#include "src/ray/protobuf/gcs.pb.h"

namespace ray {
namespace gcs {

/// \class GcsTable
///
/// GcsTable is the storage interface for all GCS tables whose data do not belong to
/// specific jobs. This class is not meant to be used directly. All gcs table classes
/// without job id should derive from this class and override the table_name_ member with
/// a unique value for that table.
template <typename Key, typename Data>
class GcsTable {
 public:
  explicit GcsTable(std::shared_ptr<StoreClient> store_client)
      : store_client_(std::move(store_client)) {}

  virtual ~GcsTable() = default;

  /// Write data to the table asynchronously.
  ///
  /// \param key The key that will be written to the table.
  /// \param value The value of the key that will be written to the table.
  /// \param callback Callback that will be called after write finishes.
  virtual void Put(const Key &key,
                   const Data &value,
                   Postable<void(ray::Status)> callback);

  /// Get data from the table asynchronously.
  ///
  /// \param key The key to lookup from the table.
  /// \param callback Callback that will be called after read finishes.
  void Get(const Key &key, Postable<void(Status, std::optional<Data>)> callback);

  /// Get all data from the table asynchronously.
  ///
  /// \param callback Callback that will be called after data has been received.
  void GetAll(Postable<void(absl::flat_hash_map<Key, Data>)> callback);

  /// Delete data from the table asynchronously.
  ///
  /// \param key The key that will be deleted from the table.
  /// \param callback Callback that will be called after delete finishes.
  virtual void Delete(const Key &key, Postable<void(ray::Status)> callback);

  /// Delete a batch of data from the table asynchronously.
  ///
  /// \param keys The batch key that will be deleted from the table.
  /// \param callback Callback that will be called after delete finishes.
  virtual void BatchDelete(const std::vector<Key> &keys,
                           Postable<void(ray::Status)> callback);

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
///
/// GcsTableWithJobId build index in memory. There is a known race condition
/// that index could be stale if multiple writer change the same index at the same time.
template <typename Key, typename Data>
class GcsTableWithJobId : public GcsTable<Key, Data> {
 public:
  explicit GcsTableWithJobId(std::shared_ptr<StoreClient> store_client)
      : GcsTable<Key, Data>(std::move(store_client)) {}

  /// Write data to the table asynchronously.
  ///
  /// \param key The key that will be written to the table. The job id can be obtained
  /// from the key.
  /// \param value The value of the key that will be written to the table.
  /// \param callback Callback that will be called after write finishes, whether it
  /// succeeds or not.
  void Put(const Key &key,
           const Data &value,
           Postable<void(ray::Status)> callback) override;

  /// Get all the data of the specified job id from the table asynchronously.
  ///
  /// \param job_id The key to lookup from the table.
  /// \param callback Callback that will be called after read finishes.
  void GetByJobId(const JobID &job_id,
                  Postable<void(absl::flat_hash_map<Key, Data>)> callback);

  /// Delete all the data of the specified job id from the table asynchronously.
  ///
  /// \param job_id The key that will be deleted from the table.
  /// \param callback Callback that will be called after delete finishes.
  void DeleteByJobId(const JobID &job_id, Postable<void(ray::Status)> callback);

  /// Delete data and index from the table asynchronously.
  ///
  /// \param key The key that will be deleted from the table.
  /// \param callback Callback that will be called after delete finishes.
  void Delete(const Key &key, Postable<void(ray::Status)> callback) override;

  /// Delete a batch of data and index from the table asynchronously.
  ///
  /// \param keys The batch key that will be deleted from the table.
  /// \param callback Callback that will be called after delete finishes.
  void BatchDelete(const std::vector<Key> &keys,
                   Postable<void(ray::Status)> callback) override;

  /// Rebuild the index during startup.
  void AsyncRebuildIndexAndGetAll(
      Postable<void(absl::flat_hash_map<Key, Data>)> callback);

 protected:
  virtual JobID GetJobIdFromKey(const Key &key) = 0;

  absl::Mutex mutex_;
  absl::flat_hash_map<JobID, absl::flat_hash_set<Key>> index_ ABSL_GUARDED_BY(mutex_);
};

class GcsJobTable : public GcsTable<JobID, rpc::JobTableData> {
 public:
  explicit GcsJobTable(std::shared_ptr<StoreClient> store_client)
      : GcsTable(std::move(store_client)) {
    table_name_ = rpc::TablePrefix_Name(rpc::TablePrefix::JOB);
  }
};

class GcsActorTable : public GcsTableWithJobId<ActorID, rpc::ActorTableData> {
 public:
  explicit GcsActorTable(std::shared_ptr<StoreClient> store_client)
      : GcsTableWithJobId(std::move(store_client)) {
    table_name_ = rpc::TablePrefix_Name(rpc::TablePrefix::ACTOR);
  }

 private:
  JobID GetJobIdFromKey(const ActorID &key) override { return key.JobId(); }
};

class GcsActorTaskSpecTable : public GcsTableWithJobId<ActorID, rpc::TaskSpec> {
 public:
  explicit GcsActorTaskSpecTable(std::shared_ptr<StoreClient> store_client)
      : GcsTableWithJobId(std::move(store_client)) {
    table_name_ = rpc::TablePrefix_Name(rpc::TablePrefix::ACTOR_TASK_SPEC);
  }

 private:
  JobID GetJobIdFromKey(const ActorID &key) override { return key.JobId(); }
};

class GcsPlacementGroupTable
    : public GcsTable<PlacementGroupID, rpc::PlacementGroupTableData> {
 public:
  explicit GcsPlacementGroupTable(std::shared_ptr<StoreClient> store_client)
      : GcsTable(std::move(store_client)) {
    table_name_ = rpc::TablePrefix_Name(rpc::TablePrefix::PLACEMENT_GROUP);
  }
};

class GcsNodeTable : public GcsTable<NodeID, rpc::GcsNodeInfo> {
 public:
  explicit GcsNodeTable(std::shared_ptr<StoreClient> store_client)
      : GcsTable(std::move(store_client)) {
    table_name_ = rpc::TablePrefix_Name(rpc::TablePrefix::NODE);
  }
};

class GcsWorkerTable : public GcsTable<WorkerID, rpc::WorkerTableData> {
 public:
  explicit GcsWorkerTable(std::shared_ptr<StoreClient> store_client)
      : GcsTable(std::move(store_client)) {
    table_name_ = rpc::TablePrefix_Name(rpc::TablePrefix::WORKERS);
  }
};

class GcsTableStorage {
 public:
  explicit GcsTableStorage(std::shared_ptr<StoreClient> store_client)
      : store_client_(std::move(store_client)) {
    job_table_ = std::make_unique<GcsJobTable>(store_client_);
    actor_table_ = std::make_unique<GcsActorTable>(store_client_);
    actor_task_spec_table_ = std::make_unique<GcsActorTaskSpecTable>(store_client_);
    placement_group_table_ = std::make_unique<GcsPlacementGroupTable>(store_client_);
    node_table_ = std::make_unique<GcsNodeTable>(store_client_);
    worker_table_ = std::make_unique<GcsWorkerTable>(store_client_);
  }

  virtual ~GcsTableStorage() = default;

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

  virtual GcsNodeTable &NodeTable() {
    RAY_CHECK(node_table_ != nullptr);
    return *node_table_;
  }

  GcsWorkerTable &WorkerTable() {
    RAY_CHECK(worker_table_ != nullptr);
    return *worker_table_;
  }

  void AsyncGetNextJobID(Postable<void(int)> callback) {
    RAY_CHECK(store_client_);
    store_client_->AsyncGetNextJobID(std::move(callback));
  }

 protected:
  std::shared_ptr<StoreClient> store_client_;
  std::unique_ptr<GcsJobTable> job_table_;
  std::unique_ptr<GcsActorTable> actor_table_;
  std::unique_ptr<GcsActorTaskSpecTable> actor_task_spec_table_;
  std::unique_ptr<GcsPlacementGroupTable> placement_group_table_;
  std::unique_ptr<GcsNodeTable> node_table_;
  std::unique_ptr<GcsWorkerTable> worker_table_;
};

}  // namespace gcs
}  // namespace ray
