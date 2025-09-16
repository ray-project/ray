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

#include "ray/gcs/gcs_table_storage.h"

#include <string>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "ray/common/asio/postable.h"
#include "ray/common/id.h"
#include "ray/common/status.h"

namespace ray {
namespace gcs {

namespace {
// Transforms the callback that, regardless of the underlying return T value, we always
// return OK.
template <typename T>
Postable<void(T)> JustOk(Postable<void(Status)> callback) {
  return std::move(callback).TransformArg([](T) { return Status::OK(); });
}
}  // namespace

template <typename Key, typename Data>
void GcsTable<Key, Data>::Put(const Key &key,
                              const Data &value,
                              Postable<void(ray::Status)> callback) {
  store_client_->AsyncPut(table_name_,
                          key.Binary(),
                          value.SerializeAsString(),
                          /*overwrite*/ true,
                          JustOk<bool>(std::move(callback)));
}

template <typename Key, typename Data>
void GcsTable<Key, Data>::Get(const Key &key,
                              Postable<void(Status, std::optional<Data>)> callback) {
  // We can't use TransformArg here because we need to return 2 arguments.
  store_client_->AsyncGet(
      table_name_, key.Binary(), std::move(callback).Rebind([](auto cb) {
        return [cb = std::move(cb)](Status status, std::optional<std::string> result) {
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
void GcsTable<Key, Data>::GetAll(
    Postable<void(absl::flat_hash_map<Key, Data>)> callback) {
  store_client_->AsyncGetAll(
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
void GcsTable<Key, Data>::Delete(const Key &key, Postable<void(ray::Status)> callback) {
  store_client_->AsyncDelete(
      table_name_, key.Binary(), JustOk<bool>(std::move(callback)));
}

template <typename Key, typename Data>
void GcsTable<Key, Data>::BatchDelete(const std::vector<Key> &keys,
                                      Postable<void(ray::Status)> callback) {
  std::vector<std::string> keys_to_delete;
  keys_to_delete.reserve(keys.size());
  for (auto &key : keys) {
    keys_to_delete.emplace_back(std::move(key.Binary()));
  }
  this->store_client_->AsyncBatchDelete(
      this->table_name_, keys_to_delete, JustOk<int64_t>(std::move(callback)));
}

template <typename Key, typename Data>
void GcsTableWithJobId<Key, Data>::Put(const Key &key,
                                       const Data &value,
                                       Postable<void(ray::Status)> callback) {
  {
    absl::MutexLock lock(&mutex_);
    index_[GetJobIdFromKey(key)].insert(key);
  }
  this->store_client_->AsyncPut(this->table_name_,
                                key.Binary(),
                                value.SerializeAsString(),
                                /*overwrite*/ true,
                                JustOk<bool>(std::move(callback)));
}

template <typename Key, typename Data>
void GcsTableWithJobId<Key, Data>::GetByJobId(
    const JobID &job_id, Postable<void(absl::flat_hash_map<Key, Data>)> callback) {
  std::vector<std::string> keys;
  {
    absl::MutexLock lock(&mutex_);
    auto &key_set = index_[job_id];
    for (auto &key : key_set) {
      keys.push_back(key.Binary());
    }
  }
  this->store_client_->AsyncMultiGet(
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
            return values;
          }));
}

template <typename Key, typename Data>
void GcsTableWithJobId<Key, Data>::DeleteByJobId(const JobID &job_id,
                                                 Postable<void(ray::Status)> callback) {
  std::vector<Key> keys;
  {
    absl::MutexLock lock(&mutex_);
    auto &key_set = index_[job_id];
    for (auto &key : key_set) {
      keys.push_back(key);
    }
  }
  BatchDelete(keys, std::move(callback));
}

template <typename Key, typename Data>
void GcsTableWithJobId<Key, Data>::Delete(const Key &key,
                                          Postable<void(ray::Status)> callback) {
  BatchDelete({key}, std::move(callback));
}

template <typename Key, typename Data>
void GcsTableWithJobId<Key, Data>::BatchDelete(const std::vector<Key> &keys,
                                               Postable<void(ray::Status)> callback) {
  std::vector<std::string> keys_to_delete;
  keys_to_delete.reserve(keys.size());
  for (auto &key : keys) {
    keys_to_delete.push_back(key.Binary());
  }
  this->store_client_->AsyncBatchDelete(
      this->table_name_,
      keys_to_delete,
      std::move(callback).TransformArg([this, callback, keys](int64_t) {
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
void GcsTableWithJobId<Key, Data>::AsyncRebuildIndexAndGetAll(
    Postable<void(absl::flat_hash_map<Key, Data>)> callback) {
  this->GetAll(std::move(callback).TransformArg(
      [this](absl::flat_hash_map<Key, Data> result) mutable {
        absl::MutexLock lock(&this->mutex_);
        this->index_.clear();
        for (auto &item : result) {
          auto key = item.first;
          this->index_[GetJobIdFromKey(key)].insert(key);
        }
        return result;
      }));
}

template class GcsTable<JobID, rpc::JobTableData>;
template class GcsTable<NodeID, rpc::GcsNodeInfo>;
template class GcsTable<NodeID, rpc::ResourceUsageBatchData>;
template class GcsTable<JobID, rpc::ErrorTableData>;
template class GcsTable<WorkerID, rpc::WorkerTableData>;
template class GcsTable<ActorID, rpc::ActorTableData>;
template class GcsTable<ActorID, rpc::TaskSpec>;
template class GcsTableWithJobId<ActorID, rpc::ActorTableData>;
template class GcsTableWithJobId<ActorID, rpc::TaskSpec>;
template class GcsTable<PlacementGroupID, rpc::PlacementGroupTableData>;

}  // namespace gcs
}  // namespace ray
