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
                                const StatusCallback &callback) {
  return store_client_->AsyncPut(table_name_,
                                 key.Binary(),
                                 value.SerializeAsString(),
                                 /*overwrite*/ true,
                                 [callback](auto) {
                                   if (callback) {
                                     callback(Status::OK());
                                   }
                                 });
}

template <typename Key, typename Data>
Status GcsTable<Key, Data>::Get(const Key &key,
                                const OptionalItemCallback<Data> &callback) {
  auto on_done = [callback](const Status &status,
                            const boost::optional<std::string> &result) {
    if (!callback) {
      return;
    }
    boost::optional<Data> value;
    if (result) {
      Data data;
      data.ParseFromString(*result);
      value = std::move(data);
    }
    callback(status, value);
  };
  return store_client_->AsyncGet(table_name_, key.Binary(), on_done);
}

template <typename Key, typename Data>
Status GcsTable<Key, Data>::GetAll(const MapCallback<Key, Data> &callback) {
  auto on_done = [callback](absl::flat_hash_map<std::string, std::string> &&result) {
    if (!callback) {
      return;
    }
    absl::flat_hash_map<Key, Data> values;
    for (auto &item : result) {
      if (!item.second.empty()) {
        values[Key::FromBinary(item.first)].ParseFromString(item.second);
      }
    }
    callback(std::move(values));
  };
  return store_client_->AsyncGetAll(table_name_, on_done);
}

template <typename Key, typename Data>
Status GcsTable<Key, Data>::Delete(const Key &key, const StatusCallback &callback) {
  return store_client_->AsyncDelete(table_name_, key.Binary(), [callback](auto) {
    if (callback) {
      callback(Status::OK());
    }
  });
}

template <typename Key, typename Data>
Status GcsTable<Key, Data>::BatchDelete(const std::vector<Key> &keys,
                                        const StatusCallback &callback) {
  std::vector<std::string> keys_to_delete;
  keys_to_delete.reserve(keys.size());
  for (auto &key : keys) {
    keys_to_delete.emplace_back(std::move(key.Binary()));
  }
  return this->store_client_->AsyncBatchDelete(
      this->table_name_, keys_to_delete, [callback](auto) {
        if (callback) {
          callback(Status::OK());
        }
      });
}

template <typename Key, typename Data>
Status GcsTableWithJobId<Key, Data>::Put(const Key &key,
                                         const Data &value,
                                         const StatusCallback &callback) {
  {
    absl::MutexLock lock(&mutex_);
    index_[GetJobIdFromKey(key)].insert(key);
  }
  return this->store_client_->AsyncPut(this->table_name_,
                                       key.Binary(),
                                       value.SerializeAsString(),
                                       /*overwrite*/ true,
                                       [callback](auto) {
                                         if (!callback) {
                                           return;
                                         }
                                         callback(Status::OK());
                                       });
}

template <typename Key, typename Data>
Status GcsTableWithJobId<Key, Data>::GetByJobId(const JobID &job_id,
                                                const MapCallback<Key, Data> &callback) {
  std::vector<std::string> keys;
  {
    absl::MutexLock lock(&mutex_);
    auto &key_set = index_[job_id];
    for (auto &key : key_set) {
      keys.push_back(key.Binary());
    }
  }
  auto on_done = [callback](absl::flat_hash_map<std::string, std::string> &&result) {
    if (!callback) {
      return;
    }
    absl::flat_hash_map<Key, Data> values;
    for (auto &item : result) {
      if (!item.second.empty()) {
        values[Key::FromBinary(item.first)].ParseFromString(item.second);
      }
    }
    callback(std::move(values));
  };
  return this->store_client_->AsyncMultiGet(this->table_name_, keys, on_done);
}

template <typename Key, typename Data>
Status GcsTableWithJobId<Key, Data>::DeleteByJobId(const JobID &job_id,
                                                   const StatusCallback &callback) {
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
                                            const StatusCallback &callback) {
  return BatchDelete({key}, callback);
}

template <typename Key, typename Data>
Status GcsTableWithJobId<Key, Data>::BatchDelete(const std::vector<Key> &keys,
                                                 const StatusCallback &callback) {
  std::vector<std::string> keys_to_delete;
  for (auto key : keys) {
    keys_to_delete.push_back(key.Binary());
  }
  return this->store_client_->AsyncBatchDelete(
      this->table_name_, keys_to_delete, [this, callback, keys](auto) {
        {
          absl::MutexLock lock(&mutex_);
          for (auto &key : keys) {
            index_[GetJobIdFromKey(key)].erase(key);
          }
        }
        if (callback) {
          callback(Status::OK());
        }
      });
}

template <typename Key, typename Data>
Status GcsTableWithJobId<Key, Data>::AsyncRebuildIndexAndGetAll(
    const MapCallback<Key, Data> &callback) {
  return this->GetAll([this, callback](absl::flat_hash_map<Key, Data> &&result) mutable {
    absl::MutexLock lock(&mutex_);
    index_.clear();
    for (auto &item : result) {
      auto key = item.first;
      index_[GetJobIdFromKey(key)].insert(key);
    }
    if (!callback) {
      return;
    }
    callback(std::move(result));
  });
}

template class GcsTable<JobID, JobTableData>;
template class GcsTable<NodeID, GcsNodeInfo>;
template class GcsTable<NodeID, ResourceMap>;
template class GcsTable<NodeID, ResourceUsageBatchData>;
template class GcsTable<JobID, ErrorTableData>;
template class GcsTable<WorkerID, WorkerTableData>;
template class GcsTable<ActorID, ActorTableData>;
template class GcsTable<ActorID, TaskSpec>;
template class GcsTable<UniqueID, StoredConfig>;
template class GcsTableWithJobId<ActorID, ActorTableData>;
template class GcsTableWithJobId<ActorID, TaskSpec>;
template class GcsTable<PlacementGroupID, PlacementGroupTableData>;
template class GcsTable<PlacementGroupID, ScheduleData>;

}  // namespace gcs
}  // namespace ray
