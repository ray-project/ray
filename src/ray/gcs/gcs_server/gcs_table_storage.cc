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

#include "gcs_table_storage.h"
#include "ray/common/id.h"
#include "ray/common/status.h"
#include "ray/gcs/callback.h"

namespace ray {
namespace gcs {

template <typename Key, typename Data>
Status GcsTable<Key, Data>::Put(const JobID &job_id, const Key &key, const Data &value,
                                const StatusCallback &callback) {
  return store_client_->AsyncPutWithIndex(table_name_, key, job_id, value, callback);
}

template <typename Key, typename Data>
Status GcsTable<Key, Data>::Get(const JobID &job_id, const Key &key,
                                const OptionalItemCallback<Data> &callback) {
  return store_client_->AsyncGet(table_name_, key, callback);
}

template <typename Key, typename Data>
Status GcsTable<Key, Data>::GetAll(
    const JobID &job_id, const SegmentedCallback<std::pair<Key, Data>> &callback) {
  return store_client_->AsyncGetAll(table_name_, callback);
}

template <typename Key, typename Data>
Status GcsTable<Key, Data>::Delete(const JobID &job_id, const Key &key,
                                   const StatusCallback &callback) {
  return store_client_->AsyncDelete(table_name_, key, callback);
}

template <typename Key, typename Data>
Status GcsTable<Key, Data>::Delete(const JobID &job_id, const std::vector<Key> &keys,
                                   const StatusCallback &callback) {
  auto finished_count = std::make_shared<int>(0);
  int size = keys.size();
  for (Key key : keys) {
    auto done = [finished_count, size, callback](Status status) {
      ++(*finished_count);
      if (*finished_count == size) {
        callback(Status::OK());
      }
    };
    RAY_CHECK_OK(store_client_->AsyncDelete(table_name_, key, done));
  }
  return Status::OK();
}

template <typename Key, typename Data>
Status GcsTable<Key, Data>::Delete(const JobID &job_id, const StatusCallback &callback) {
  return store_client_->AsyncDeleteByIndex(table_name_, job_id, callback);
}

template class GcsTable<JobID, JobTableData>;
template class GcsTable<ActorID, ActorTableData>;
template class GcsTable<ActorCheckpointID, ActorCheckpointData>;
template class GcsTable<ActorID, ActorCheckpointIdData>;
template class GcsTable<TaskID, TaskTableData>;
template class GcsTable<TaskID, TaskLeaseData>;
template class GcsTable<TaskID, TaskReconstructionData>;
template class GcsTable<ObjectID, ObjectTableDataList>;
template class GcsTable<ClientID, GcsNodeInfo>;
template class GcsTable<ClientID, ResourceMap>;
template class GcsTable<ClientID, HeartbeatTableData>;
template class GcsTable<ClientID, HeartbeatBatchTableData>;
template class GcsTable<JobID, ErrorTableData>;
template class GcsTable<UniqueID, ProfileTableData>;
template class GcsTable<WorkerID, WorkerFailureData>;

}  // namespace gcs
}  // namespace ray
