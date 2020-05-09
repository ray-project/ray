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

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "ray/gcs/redis_client.h"
#include "ray/gcs/redis_context.h"
#include "ray/gcs/store_client/store_client.h"
#include "ray/protobuf/gcs.pb.h"

namespace ray {

namespace gcs {

class RedisStoreClient : public StoreClient {
 public:
  explicit RedisStoreClient(std::shared_ptr<RedisClient> redis_client)
      : redis_client_(std::move(redis_client)) {}

  Status AsyncPut(const std::string &table_name, const std::string &key,
                  const std::string &data, const StatusCallback &callback) override;

  Status AsyncPutWithIndex(const std::string &table_name, const std::string &key,
                           const std::string &index_key, const std::string &data,
                           const StatusCallback &callback) override;

  Status AsyncGet(const std::string &table_name, const std::string &key,
                  const OptionalItemCallback<std::string> &callback) override;

  Status AsyncGetAll(
      const std::string &table_name,
      const MultiItemCallback<std::pair<std::string, std::string>> &callback) override;

  Status AsyncDelete(const std::string &table_name, const std::string &key,
                     const StatusCallback &callback) override;

  Status AsyncBatchDelete(const std::string &table_name,
                          const std::vector<std::string> &keys,
                          const StatusCallback &callback) override;

  Status AsyncDeleteByIndex(const std::string &table_name, const std::string &index_key,
                            const StatusCallback &callback) override;

 private:
  /// \class RedisScanner
  /// This class is used to scan data from Redis.
  ///
  /// If you called one method, should never call the other methods.
  /// Otherwise it will disturb the status of the RedisScanner.
  class RedisScanner {
   public:
    explicit RedisScanner(std::shared_ptr<RedisClient> redis_client,
                          std::string table_name, std::string match_pattern);

    Status ScanRows(
        const MultiItemCallback<std::pair<std::string, std::string>> &callback);

    Status ScanKeys(const MultiItemCallback<std::string> &callback);

   private:
    void Scan();

    void OnScanDone();

    void OnScanCallback(size_t shard_index, const std::shared_ptr<CallbackReply> &reply);

    void ReadRows(const absl::flat_hash_set<std::string> &keys);

    std::string table_name_;

    /// The scan match pattern.
    std::string match_pattern_;

    /// The callback that will be called when scan finished.
    StatusCallback callback_{nullptr};

    /// The scan result in rows.
    /// If the scan type is kScanPartialRows, partial scan result will be saved in this
    /// variable. If the scan type is kScanAllRows, all scan result will be saved in this
    /// variable.
    std::vector<std::pair<std::string, std::string>> rows_;

    absl::Mutex mutex_;

    /// The scan cursor for each shard.
    absl::flat_hash_map<size_t, size_t> shard_to_cursor_;

    /// All keys that received from redis.
    absl::flat_hash_set<std::string> keys_;

    /// Whether the scan is failed.
    std::atomic<bool> is_failed_{false};

    /// The pending shard scan count.
    std::atomic<size_t> pending_request_count_{0};

    /// Total pending read request count.
    std::atomic<size_t> pending_read_count_{0};

    std::shared_ptr<RedisClient> redis_client_;
  };

  Status DoPut(const std::string &key, const std::string &data,
               const StatusCallback &callback);

  std::shared_ptr<RedisClient> redis_client_;

  std::shared_ptr<RedisScanner> redis_scanner_;
};

}  // namespace gcs

}  // namespace ray

#endif  // RAY_GCS_STORE_CLIENT_REDIS_STORE_CLIENT_H
