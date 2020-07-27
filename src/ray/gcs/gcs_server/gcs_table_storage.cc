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
Status GcsTable<Key, Data>::Put(const Key &key, const Data &value,
                                const StatusCallback &callback) {
  return store_client_->AsyncPut(table_name_, key.Binary(), value.SerializeAsString(),
                                 callback);
}

template <typename Key, typename Data>
Status GcsTable<Key, Data>::Get(const Key &key,
                                const OptionalItemCallback<Data> &callback) {
  auto on_done = [callback](const Status &status,
                            const boost::optional<std::string> &result) {
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
  auto on_done = [callback](const std::unordered_map<std::string, std::string> &result) {
    std::unordered_map<Key, Data> values;
    for (auto &item : result) {
      Data data;
      data.ParseFromString(item.second);
      values[Key::FromBinary(item.first)] = data;
    }
    callback(values);
  };
  return store_client_->AsyncGetAll(table_name_, on_done);
}

template <typename Key, typename Data>
Status GcsTable<Key, Data>::Delete(const Key &key, const StatusCallback &callback) {
  return store_client_->AsyncDelete(table_name_, key.Binary(), callback);
}

template <typename Key, typename Data>
Status GcsTable<Key, Data>::BatchDelete(const std::vector<Key> &keys,
                                        const StatusCallback &callback) {
  std::vector<std::string> keys_to_delete;
  keys_to_delete.reserve(keys.size());
  for (auto &key : keys) {
    keys_to_delete.emplace_back(std::move(key.Binary()));
  }
  return this->store_client_->AsyncBatchDelete(this->table_name_, keys_to_delete,
                                               callback);
}

template <typename Key, typename Data>
Status GcsTableWithJobId<Key, Data>::Put(const Key &key, const Data &value,
                                         const StatusCallback &callback) {
  return this->store_client_->AsyncPutWithIndex(this->table_name_, key.Binary(),
                                                GetJobIdFromKey(key).Binary(),
                                                value.SerializeAsString(), callback);
}

template <typename Key, typename Data>
Status GcsTableWithJobId<Key, Data>::GetByJobId(const JobID &job_id,
                                                const MapCallback<Key, Data> &callback) {
  auto on_done = [callback](const std::unordered_map<std::string, std::string> &result) {
    std::unordered_map<Key, Data> values;
    for (auto &item : result) {
      Data data;
      data.ParseFromString(item.second);
      values[Key::FromBinary(item.first)] = std::move(data);
    }
    callback(values);
  };
  return this->store_client_->AsyncGetByIndex(this->table_name_, job_id.Binary(),
                                              on_done);
}

template <typename Key, typename Data>
Status GcsTableWithJobId<Key, Data>::DeleteByJobId(const JobID &job_id,
                                                   const StatusCallback &callback) {
  return this->store_client_->AsyncDeleteByIndex(this->table_name_, job_id.Binary(),
                                                 callback);
}

template class GcsTable<JobID, JobTableData>;
template class GcsTable<ClientID, GcsNodeInfo>;
template class GcsTable<ClientID, ResourceMap>;
template class GcsTable<ClientID, HeartbeatTableData>;
template class GcsTable<ClientID, HeartbeatBatchTableData>;
template class GcsTable<JobID, ErrorTableData>;
template class GcsTable<UniqueID, ProfileTableData>;
template class GcsTable<WorkerID, WorkerTableData>;
template class GcsTable<ActorID, ActorTableData>;
template class GcsTable<ActorCheckpointID, ActorCheckpointData>;
template class GcsTable<ActorID, ActorCheckpointIdData>;
template class GcsTable<TaskID, TaskTableData>;
template class GcsTable<TaskID, TaskLeaseData>;
template class GcsTable<TaskID, TaskReconstructionData>;
template class GcsTable<ObjectID, ObjectTableDataList>;
template class GcsTable<UniqueID, StoredConfig>;
template class GcsTableWithJobId<ActorID, ActorTableData>;
template class GcsTableWithJobId<ActorID, ActorCheckpointIdData>;
template class GcsTableWithJobId<TaskID, TaskTableData>;
template class GcsTableWithJobId<TaskID, TaskLeaseData>;
template class GcsTableWithJobId<TaskID, TaskReconstructionData>;
template class GcsTableWithJobId<ObjectID, ObjectTableDataList>;
template class GcsTable<PlacementGroupID, PlacementGroupTableData>;
template class GcsTable<PlacementGroupID, ScheduleData>;

}  // namespace gcs
}  // namespace ray
