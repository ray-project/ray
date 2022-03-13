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

#include "absl/container/flat_hash_set.h"
#include "ray/gcs/redis_client.h"
#include "ray/gcs/redis_context.h"
#include "ray/gcs/store_client/store_client.h"
#include "src/ray/protobuf/gcs.pb.h"

namespace ray {

namespace gcs {

class RedisStoreClient : public StoreClient {
 public:
  explicit RedisStoreClient(std::shared_ptr<RedisClient> redis_client)
      : redis_client_(std::move(redis_client)) {}

  Status AsyncPut(const std::string &table_name,
                  const std::string &key,
                  const std::string &data,
                  const StatusCallback &callback) override;

  Status AsyncPutWithIndex(const std::string &table_name,
                           const std::string &key,
                           const std::string &index_key,
                           const std::string &data,
                           const StatusCallback &callback) override;

  Status AsyncGet(const std::string &table_name,
                  const std::string &key,
                  const OptionalItemCallback<std::string> &callback) override;

  Status AsyncGetByIndex(const std::string &table_name,
                         const std::string &index_key,
                         const MapCallback<std::string, std::string> &callback) override;

  Status AsyncGetAll(const std::string &table_name,
                     const MapCallback<std::string, std::string> &callback) override;

  Status AsyncDelete(const std::string &table_name,
                     const std::string &key,
                     const StatusCallback &callback) override;

  Status AsyncDeleteWithIndex(const std::string &table_name,
                              const std::string &key,
                              const std::string &index_key,
                              const StatusCallback &callback) override;

  Status AsyncBatchDelete(const std::string &table_name,
                          const std::vector<std::string> &keys,
                          const StatusCallback &callback) override;

  Status AsyncBatchDeleteWithIndex(const std::string &table_name,
                                   const std::vector<std::string> &keys,
                                   const std::vector<std::string> &index_keys,
                                   const StatusCallback &callback) override;

  Status AsyncDeleteByIndex(const std::string &table_name,
                            const std::string &index_key,
                            const StatusCallback &callback) override;

  int GetNextJobID() override;

 private:
  /// \class RedisScanner
  /// This class is used to scan data from Redis.
  ///
  /// If you called one method, should never call the other methods.
  /// Otherwise it will disturb the status of the RedisScanner.
  class RedisScanner {
   public:
    explicit RedisScanner(std::shared_ptr<RedisClient> redis_client,
                          const std::string &table_name);

    Status ScanKeysAndValues(const std::string &match_pattern,
                             const MapCallback<std::string, std::string> &callback);

    Status ScanKeys(const std::string &match_pattern,
                    const MultiItemCallback<std::string> &callback);

   private:
    void Scan(const std::string &match_pattern, const StatusCallback &callback);

    void OnScanCallback(const std::string &match_pattern,
                        size_t shard_index,
                        const std::shared_ptr<CallbackReply> &reply,
                        const StatusCallback &callback);

    std::string table_name_;

    /// Mutex to protect the shard_to_cursor_ field and the keys_ field and the
    /// key_value_map_ field.
    absl::Mutex mutex_;

    /// All keys that scanned from redis.
    absl::flat_hash_set<std::string> keys_;

    /// The scan cursor for each shard.
    absl::flat_hash_map<size_t, size_t> shard_to_cursor_;

    /// The pending shard scan count.
    std::atomic<size_t> pending_request_count_{0};

    std::shared_ptr<RedisClient> redis_client_;
  };

  Status DoPut(const std::string &key,
               const std::string &data,
               const StatusCallback &callback);

  Status DeleteByKeys(const std::vector<std::string> &keys,
                      const StatusCallback &callback);

  /// The return value is a map, whose key is the shard and the value is a list of batch
  /// operations.
  static absl::flat_hash_map<RedisContext *, std::list<std::vector<std::string>>>
  GenCommandsByShards(const std::shared_ptr<RedisClient> &redis_client,
                      const std::string &command,
                      const std::vector<std::string> &keys,
                      int *count);

  /// The separator is used when building redis key.
  static std::string table_separator_;
  static std::string index_table_separator_;

  static std::string GenRedisKey(const std::string &table_name, const std::string &key);

  static std::string GenRedisKey(const std::string &table_name,
                                 const std::string &key,
                                 const std::string &index_key);

  static std::string GenRedisMatchPattern(const std::string &table_name);

  static std::string GenRedisMatchPattern(const std::string &table_name,
                                          const std::string &index_key);

  static std::string GetKeyFromRedisKey(const std::string &redis_key,
                                        const std::string &table_name);

  static std::string GetKeyFromRedisKey(const std::string &redis_key,
                                        const std::string &table_name,
                                        const std::string &index_key);

  static Status MGetValues(std::shared_ptr<RedisClient> redis_client,
                           const std::string &table_name,
                           const std::vector<std::string> &keys,
                           const MapCallback<std::string, std::string> &callback);

  std::shared_ptr<RedisClient> redis_client_;
};

}  // namespace gcs

}  // namespace ray
