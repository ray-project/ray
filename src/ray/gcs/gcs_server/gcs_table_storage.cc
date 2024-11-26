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

#include "ray/gcs/gcs_server/gcs_table_storage.h"

#include "ray/common/id.h"
#include "ray/common/status.h"
#include "ray/gcs/callback.h"

namespace ray {
namespace gcs {

template <typename Key, typename Data>
Status GcsTable<Key, Data>::Put(const Key &key,
                                const Data &value,
                                Postable<void(Status)> callback) {
  return store_client_->AsyncPut(
      table_name_,
      key.Binary(),
      value.SerializeAsString(),
      /*overwrite*/ true,
      std::move(callback).TransformArg([](bool) { return Status::OK(); }));
}

template <typename Key, typename Data>
Status GcsTable<Key, Data>::Get(const Key &key,
                                ToPostable<OptionalItemCallback<Data>> callback) {
  return store_client_->AsyncGet(
      table_name_, key.Binary(), std::move(callback).Rebind([](auto cb) {
        return [cb = std::move(cb)](Status status, std::optional<std::string> &&result) {
          std::optional<Data> value;
          if (result) {
            Data data;
            data.ParseFromString(*result);
            value = std::move(data);
          }
          cb(status, std::move(value));
        };
      }));
}

template <typename Key, typename Data>
Status GcsTable<Key, Data>::GetAll(
    Postable<void(absl::flat_hash_map<Key, Data>)> callback) {
  return store_client_->AsyncGetAll(
      table_name_,
      std::move(callback).TransformArg(
          [](absl::flat_hash_map<std::string, std::string> result) {
            absl::flat_hash_map<Key, Data> values;
            values.reserve(result.size());
            for (auto &item : result) {
              if (!item.second.empty()) {
                values[Key::FromBinary(item.first)].ParseFromString(item.second);
              }
            }
            return values;
          }));
}

template <typename Key, typename Data>
Status GcsTable<Key, Data>::Delete(const Key &key, Postable<void(Status)> callback) {
  return store_client_->AsyncDelete(
      table_name_, key.Binary(), std::move(callback).TransformArg([](bool) {
        return Status::OK();
      }));
}

template <typename Key, typename Data>
Status GcsTable<Key, Data>::BatchDelete(const std::vector<Key> &keys,
                                        Postable<void(Status)> callback) {
  std::vector<std::string> keys_to_delete;
  keys_to_delete.reserve(keys.size());
  for (auto &key : keys) {
    keys_to_delete.emplace_back(std::move(key.Binary()));
  }
  return this->store_client_->AsyncBatchDelete(
      this->table_name_, keys_to_delete, std::move(callback).TransformArg([](int64_t) {
        return Status::OK();
      }));
}

template <typename Key, typename Data>
Status GcsTableWithJobId<Key, Data>::Put(const Key &key,
                                         const Data &value,
                                         Postable<void(Status)> callback) {
  {
    absl::MutexLock lock(&mutex_);
    index_[GetJobIdFromKey(key)].insert(key);
  }
  return this->store_client_->AsyncPut(
      this->table_name_,
      key.Binary(),
      value.SerializeAsString(),
      /*overwrite*/ true,
      std::move(callback).TransformArg([](bool) { return Status::OK(); }));
}

template <typename Key, typename Data>
Status GcsTableWithJobId<Key, Data>::GetByJobId(
    const JobID &job_id, Postable<void(absl::flat_hash_map<Key, Data>)> callback) {
  std::vector<std::string> keys;
  {
    absl::MutexLock lock(&mutex_);
    auto &key_set = index_[job_id];
    for (auto &key : key_set) {
      keys.push_back(key.Binary());
    }
  }

  return this->store_client_->AsyncMultiGet(
      this->table_name_,
      keys,
      std::move(callback).TransformArg(
          [](absl::flat_hash_map<std::string, std::string> result) {
            absl::flat_hash_map<Key, Data> values;
            for (auto &item : result) {
              if (!item.second.empty()) {
                values[Key::FromBinary(item.first)].ParseFromString(item.second);
              }
            }
            return std::move(values);
          }));
}

template <typename Key, typename Data>
Status GcsTableWithJobId<Key, Data>::DeleteByJobId(const JobID &job_id,
                                                   Postable<void(Status)> callback) {
  std::vector<Key> keys;
  {
    absl::MutexLock lock(&mutex_);
    auto &key_set = index_[job_id];
    for (auto &key : key_set) {
      keys.push_back(key);
    }
  }
  return BatchDelete(keys, callback);
}

template <typename Key, typename Data>
Status GcsTableWithJobId<Key, Data>::Delete(const Key &key,
                                            Postable<void(Status)> callback) {
  return BatchDelete({key}, callback);
}

template <typename Key, typename Data>
Status GcsTableWithJobId<Key, Data>::BatchDelete(const std::vector<Key> &keys,
                                                 Postable<void(Status)> callback) {
  std::vector<std::string> keys_to_delete;
  keys_to_delete.reserve(keys.size());
  for (auto &key : keys) {
    keys_to_delete.push_back(key.Binary());
  }
  return this->store_client_->AsyncBatchDelete(
      this->table_name_,
      keys_to_delete,
      std::move(callback).TransformArg([this, keys](int64_t) {
        {
          absl::MutexLock lock(&mutex_);
          for (auto &key : keys) {
            index_[GetJobIdFromKey(key)].erase(key);
          }
        }
        return Status::OK();
      }));
}

template <typename Key, typename Data>
Status GcsTableWithJobId<Key, Data>::AsyncRebuildIndexAndGetAll(
    Postable<void(absl::flat_hash_map<Key, Data>)> callback) {
  return this->GetAll(
      std::move(callback).TransformArg([this](absl::flat_hash_map<Key, Data> result) {
        absl::MutexLock lock(&mutex_);
        index_.clear();
        for (auto &item : result) {
          auto key = item.first;
          index_[GetJobIdFromKey(key)].insert(key);
        }
        return result;
      }));
}

template class GcsTable<JobID, JobTableData>;
template class GcsTable<NodeID, GcsNodeInfo>;
template class GcsTable<NodeID, ResourceUsageBatchData>;
template class GcsTable<JobID, ErrorTableData>;
template class GcsTable<WorkerID, WorkerTableData>;
template class GcsTable<ActorID, ActorTableData>;
template class GcsTable<ActorID, TaskSpec>;
template class GcsTable<UniqueID, StoredConfig>;
template class GcsTableWithJobId<ActorID, ActorTableData>;
template class GcsTableWithJobId<ActorID, TaskSpec>;
template class GcsTable<PlacementGroupID, PlacementGroupTableData>;

}  // namespace gcs
}  // namespace ray
