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

#ifndef RAY_GCS_STORE_CLIENT_REDIS_STORE_CLIENT_H
#define RAY_GCS_STORE_CLIENT_REDIS_STORE_CLIENT_H

#include <memory>
#include <unordered_set>
#include "ray/gcs/redis_client.h"
#include "ray/gcs/redis_context.h"
#include "ray/gcs/store_client/store_client.h"
#include "ray/protobuf/gcs.pb.h"

namespace ray {

namespace gcs {

template <typename Key, typename Data, typename IndexKey>
class RedisStoreClient : public StoreClient<Key, Data, IndexKey> {
 public:
  RedisStoreClient(std::shared_ptr<RedisClient> redis_client)
      : redis_client_(std::move(redis_client)) {}

  virtual ~RedisStoreClient() {}

  Status AsyncPut(const std::string &table_name, const Key &key, const Data &data,
                  const StatusCallback &callback) override;

  Status AsyncPutWithIndex(const std::string &table_name, const Key &key,
                           const IndexKey &index_key, const Data &data,
                           const StatusCallback &callback) override;

  Status AsyncGet(const std::string &table_name, const Key &key,
                  const OptionalItemCallback<Data> &callback) override;

  Status AsyncGetAll(const std::string &table_name,
                     const SegmentedCallback<std::pair<Key, Data>> &callback) override;

  Status AsyncDelete(const std::string &table_name, const Key &key,
                     const StatusCallback &callback) override;

  Status AsyncDeleteByIndex(const std::string &table_name, const IndexKey &index_key,
                            const StatusCallback &callback) override;

 private:
  Status DoPut(const std::string &key, const std::string &data,
               const StatusCallback &callback);

  std::shared_ptr<RedisClient> redis_client_;
};

typedef RedisStoreClient<ActorID, rpc::ActorTableData, JobID> RedisActorStoreTable;

}  // namespace gcs

}  // namespace ray

#endif  // RAY_GCS_STORE_CLIENT_REDIS_STORE_CLIENT_H
