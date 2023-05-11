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

#include <queue>

#include "absl/container/flat_hash_set.h"
#include "absl/synchronization/mutex.h"
#include "ray/common/ray_config.h"
#include "ray/gcs/redis_client.h"
#include "ray/gcs/redis_context.h"
#include "ray/gcs/store_client/store_client.h"
#include "src/ray/protobuf/gcs.pb.h"

namespace ray {

namespace gcs {

class RedisStoreClient : public StoreClient {
 public:
  explicit RedisStoreClient(std::shared_ptr<RedisClient> redis_client);

  Status AsyncPut(const std::string &table_name,
                  const std::string &key,
                  const std::string &data,
                  bool overwrite,
                  std::function<void(bool)> callback) override;

  Status AsyncGet(const std::string &table_name,
                  const std::string &key,
                  const OptionalItemCallback<std::string> &callback) override;

  Status AsyncGetAll(const std::string &table_name,
                     const MapCallback<std::string, std::string> &callback) override;

  Status AsyncMultiGet(const std::string &table_name,
                       const std::vector<std::string> &keys,
                       const MapCallback<std::string, std::string> &callback) override;

  Status AsyncDelete(const std::string &table_name,
                     const std::string &key,
                     std::function<void(bool)> callback) override;

  Status AsyncBatchDelete(const std::string &table_name,
                          const std::vector<std::string> &keys,
                          std::function<void(int64_t)> callback) override;

  int GetNextJobID() override;

  Status AsyncGetKeys(const std::string &table_name,
                      const std::string &prefix,
                      std::function<void(std::vector<std::string>)> callback) override;

  Status AsyncExists(const std::string &table_name,
                     const std::string &key,
                     std::function<void(bool)> callback) override;

 private:
  /// \class RedisScanner
  /// This class is used to scan data from Redis.
  ///
  /// If you called one method, should never call the other methods.
  /// Otherwise it will disturb the status of the RedisScanner.
  class RedisScanner {
   public:
    explicit RedisScanner(std::shared_ptr<RedisClient> redis_client,
                          const std::string &external_storage_namespace,
                          const std::string &table_name);

    Status ScanKeysAndValues(const std::string &match_pattern,
                             const MapCallback<std::string, std::string> &callback);

   private:
    void Scan(const std::string &match_pattern, const StatusCallback &callback);

    void OnScanCallback(const std::string &match_pattern,
                        size_t shard_index,
                        const std::shared_ptr<CallbackReply> &reply,
                        const StatusCallback &callback);
    /// The table name that the scanner will scan.
    std::string table_name_;

    // The namespace of the external storage. Used for isolation.
    std::string external_storage_namespace_;

    /// Mutex to protect the shard_to_cursor_ field and the keys_ field and the
    /// key_value_map_ field.
    absl::Mutex mutex_;

    /// All keys that scanned from redis.
    absl::flat_hash_map<std::string, std::string> results_;

    /// The scan cursor for each shard.
    absl::flat_hash_map<size_t, size_t> shard_to_cursor_;

    /// The pending shard scan count.
    std::atomic<size_t> pending_request_count_{0};

    std::shared_ptr<RedisClient> redis_client_;
  };

  // Iterate the pending queue and call the progress_fn for each key.
  //
  // \param keys The keys to scan
  // \param progress_fn The function to call for the queue of each key.
  // \return The number of queues that have been added or deleted.
  template <typename F>
  std::pair<size_t, size_t> Progress(const std::vector<std::string> &keys,
                                     F progress_fn) {
    size_t added_cnt = 0;
    size_t deleted_cnt = 0;
    {
      absl::MutexLock lock(&mu_);
      for (const auto &key : keys) {
        auto [op_iter, added] = pending_redis_request_by_key_.emplace(
            key, std::queue<std::function<void()>>());
        if (added) {
          added_cnt++;
        }
        progress_fn(op_iter->second);
        if (op_iter->second.empty()) {
          deleted_cnt++;
          pending_redis_request_by_key_.erase(op_iter);
        }
      }
    }
    return {added_cnt, deleted_cnt};
  }

  Status DoPut(const std::string &key,
               const std::string &data,
               bool overwrite,
               std::function<void(bool)> callback);

  Status DeleteByKeys(const std::vector<std::string> &keys,
                      std::function<void(int64_t)> callback);

  // Send the redis command to the server. This method will make request to be
  // serialized for each key in keys. At a given time, only one request for a key
  // will be in flight.
  //
  // \param keys The keys in the request.
  // \param args The redis commands
  // \param redis_callback The callback to call when the reply is received.
  void SendRedisCmd(std::vector<std::string> keys,
                    std::vector<std::string> args,
                    RedisCallback redis_callback);

  void MGetValues(const std::string &table_name,
                  const std::vector<std::string> &keys,
                  const MapCallback<std::string, std::string> &callback);

  std::string external_storage_namespace_;
  std::shared_ptr<RedisClient> redis_client_;
  absl::Mutex mu_;

  // The pending redis requests queue for each key.
  // The queue will be poped when the request is processed.
  absl::flat_hash_map<std::string, std::queue<std::function<void()>>>
      pending_redis_request_by_key_ GUARDED_BY(mu_);
};

}  // namespace gcs

}  // namespace ray
