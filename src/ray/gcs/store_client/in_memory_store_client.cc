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

#include <functional>
#include "ray/util/logging.h"
#include "ray/gcs/store_client/in_memory_store_client.h"

namespace ray {

namespace gcs {

template <typename Key, typename Data, typename IndexKey>
Status InMemoryStoreClient<Key, Data, IndexKey>::AsyncPut(const std::string &table_name,
                                                       const Key &key, const Data &data,
                                                       const StatusCallback &callback) {
  mutex_.lock();
  std::shared_ptr<InMemoryTable> table;
  if (tables_.count(table_name)) {
    table.reset();
  } else {
    table = std::make_shared<InMemoryTable>();
    tables_[table_name] = table;
  }


  main_service_.post([]() {});
  return Status::OK();
}

template <typename Key, typename Data, typename IndexKey>
Status InMemoryStoreClient<Key, Data, IndexKey>::AsyncPutWithIndex(
    const std::string &table_name, const Key &key, const IndexKey &index_key,
    const Data &data, const StatusCallback &callback) {
  return Status::OK();
}

template <typename Key, typename Data, typename IndexKey>
Status InMemoryStoreClient<Key, Data, IndexKey>::AsyncGet(
    const std::string &table_name, const Key &key,
    const OptionalItemCallback<Data> &callback) {
  return Status::OK();
}

template <typename Key, typename Data, typename IndexKey>
Status InMemoryStoreClient<Key, Data, IndexKey>::AsyncGetAll(
    const std::string &table_name,
    const SegmentedCallback<std::pair<Key, Data>> &callback) {
  return Status::OK();
}

template <typename Key, typename Data, typename IndexKey>
Status InMemoryStoreClient<Key, Data, IndexKey>::AsyncDelete(
    const std::string &table_name, const Key &key, const StatusCallback &callback) {
  return Status::OK();
}

template <typename Key, typename Data, typename IndexKey>
Status InMemoryStoreClient<Key, Data, IndexKey>::AsyncDeleteByIndex(
    const std::string &table_name, const IndexKey &index_key,
    const StatusCallback &callback) {
  RAY_CHECK(0) << "Not implemented! Will implement this function in next PR.";
  return Status::OK();
}

//template class InMemoryStoreClient<ActorID, rpc::ActorTableData, JobID>;
//template class InMemoryStoreClient<JobID, rpc::JobTableData, JobID>;
//template class InMemoryStoreClient<ActorCheckpointID, rpc::ActorCheckpointData, JobID>;
//template class InMemoryStoreClient<ActorID, rpc::ActorCheckpointIdData, JobID>;
//template class InMemoryStoreClient<TaskID, rpc::TaskTableData, JobID>;
//template class InMemoryStoreClient<TaskID, rpc::TaskLeaseData, JobID>;
//template class InMemoryStoreClient<TaskID, rpc::TaskReconstructionData, JobID>;
//template class InMemoryStoreClient<ObjectID, rpc::ObjectTableDataList, JobID>;
//template class InMemoryStoreClient<ClientID, rpc::GcsNodeInfo, JobID>;
//template class InMemoryStoreClient<ClientID, rpc::ResourceMap, JobID>;
//template class InMemoryStoreClient<ClientID, rpc::HeartbeatTableData, JobID>;
//template class InMemoryStoreClient<ClientID, rpc::HeartbeatBatchTableData, JobID>;
//template class InMemoryStoreClient<JobID, rpc::ErrorTableData, JobID>;
//template class InMemoryStoreClient<UniqueID, rpc::ProfileTableData, JobID>;
//template class InMemoryStoreClient<WorkerID, rpc::WorkerFailureData, JobID>;

}  // namespace gcs

}  // namespace ray
