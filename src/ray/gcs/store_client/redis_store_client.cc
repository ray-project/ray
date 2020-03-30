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

#include "ray/gcs/store_client/redis_store_client.h"

#include <functional>
#include "ray/common/ray_config.h"
#include "ray/gcs/redis_context.h"
#include "ray/protobuf/gcs.pb.h"
#include "ray/util/logging.h"

namespace ray {

namespace gcs {

template <typename Key, typename Data, typename SecondaryKey>
Status RedisStoreClient<Key, Data, SecondaryKey>::AsyncPut(
    const std::string &table_name, const Key &key, const Data &data,
    const StatusCallback &callback) {
  std::string full_key = table_name + key.Binary();
  std::string data_str = data.SerializeAsString();
  RAY_CHECK(!data_str.empty());
  return DoPut(full_key, data_str, callback);
}

template <typename Key, typename Data, typename SecondaryKey>
Status RedisStoreClient<Key, Data, SecondaryKey>::AsyncPutWithIndex(
    const std::string &table_name, const Key &key, const SecondaryKey &index_key,
    const Data &data, const StatusCallback &callback) {
  std::string data_str = data.SerializeAsString();
  RAY_CHECK(!data_str.empty());

  auto write_callback = [this, table_name, key, data_str, callback](Status status) {
    if (!status.ok()) {
      // Run callback if failed.
      if (callback != nullptr) {
        callback(status);
      }
      return;
    }

    // Write data to Redis.
    std::string full_key = table_name + key.Binary();
    status = DoPut(full_key, data_str, callback);

    if (!status.ok()) {
      // Run callback if failed.
      if (callback != nullptr) {
        callback(status);
      }
    }
  };

  // Write index to Redis.
  std::string index_table_key = index_key.Binary() + table_name + key.Binary();
  return DoPut(index_table_key, key.Binary(), write_callback);
}

template <typename Key, typename Data, typename SecondaryKey>
Status RedisStoreClient<Key, Data, SecondaryKey>::DoPut(const std::string &key,
                                                        const std::string &data,
                                                        const StatusCallback &callback) {
  std::vector<std::string> args = {"SET", key, data};
  RedisCallback write_callback = nullptr;
  if (callback) {
    write_callback = [callback](std::shared_ptr<CallbackReply> reply) {
      auto status = reply->ReadAsStatus();
      callback(status);
    };
  }

  auto shard_context = redis_client_->GetShardContext(key);
  return shard_context->RunArgvAsync(args, write_callback);
}

template <typename Key, typename Data, typename SecondaryKey>
Status RedisStoreClient<Key, Data, SecondaryKey>::AsyncGet(
    const std::string &table_name, const Key &key,
    const OptionalItemCallback<Data> &callback) {
  RAY_CHECK(callback != nullptr);

  auto redis_callback = [callback](std::shared_ptr<CallbackReply> reply) {
    boost::optional<Data> result;
    if (!reply->IsNil()) {
      std::string data_str = reply->ReadAsString();
      if (!data_str.empty()) {
        Data data;
        RAY_CHECK(data.ParseFromString(data_str));
        result = std::move(data);
      }
    }
    callback(Status::OK(), result);
  };

  std::string full_key = table_name + key.Binary();
  std::vector<std::string> args = {"GET", full_key};

  auto shard_context = redis_client_->GetShardContext(full_key);
  return shard_context->RunArgvAsync(args, redis_callback);
}

template <typename Key, typename Data, typename SecondaryKey>
Status RedisStoreClient<Key, Data, SecondaryKey>::AsyncGetAll(
    const std::string &table_name,
    const SegmentedCallback<std::pair<Key, Data>> &callback) {
  RAY_CHECK(0) << "Not implemented! Will implement this function in next PR.";
  return Status::OK();
}

template <typename Key, typename Data, typename SecondaryKey>
Status RedisStoreClient<Key, Data, SecondaryKey>::AsyncDelete(
    const std::string &table_name, const Key &key, const StatusCallback &callback) {
  RedisCallback delete_callback = nullptr;
  if (callback) {
    delete_callback = [callback](std::shared_ptr<CallbackReply> reply) {
      int64_t deleted_count = reply->ReadAsInteger();
      RAY_LOG(DEBUG) << "Delete done, total delete count " << deleted_count;
      callback(Status::OK());
    };
  }

  std::string full_key = table_name + key.Binary();
  std::vector<std::string> args = {"DEL", full_key};

  auto shard_context = redis_client_->GetShardContext(full_key);
  return shard_context->RunArgvAsync(args, delete_callback);
}

template <typename Key, typename Data, typename SecondaryKey>
Status RedisStoreClient<Key, Data, SecondaryKey>::AsyncDeleteByIndex(
    const std::string &table_name, const SecondaryKey &index_key,
    const StatusCallback &callback) {
  RAY_CHECK(0) << "Not implemented! Will implement this function in next PR.";
  return Status::OK();
}

template class RedisStoreClient<ActorID, rpc::ActorTableData, JobID>;

}  // namespace gcs

}  // namespace ray
