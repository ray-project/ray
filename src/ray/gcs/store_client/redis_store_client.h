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

#include <gtest/gtest_prod.h>

#include <memory>
#include <queue>
#include <string>
#include <utility>
#include <vector>

#include "absl/synchronization/mutex.h"
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/asio/postable.h"
#include "ray/gcs/store_client/redis_context.h"
#include "ray/gcs/store_client/store_client.h"

namespace ray {

namespace gcs {

// Typed key to avoid forgetting to prepend external_storage_namespace.
struct RedisKey {
  const std::string external_storage_namespace;
  const std::string table_name;
  std::string ToString() const;
};

struct RedisMatchPattern {
  static RedisMatchPattern Prefix(const std::string &prefix);
  static RedisMatchPattern Any() {
    static const RedisMatchPattern kAny("*");
    return kAny;
  }
  const std::string escaped_;

 private:
  explicit RedisMatchPattern(std::string escaped) : escaped_(std::move(escaped)) {}
};

struct RedisCommand {
  std::string command;
  // Redis "key" referring to a HASH.
  RedisKey redis_key;
  std::vector<std::string> args;

  std::vector<std::string> ToRedisArgs() const {
    std::vector<std::string> redis_args;
    redis_args.reserve(2 + args.size());
    redis_args.push_back(command);
    redis_args.push_back(redis_key.ToString());
    for (const auto &arg : args) {
      redis_args.push_back(arg);
    }
    return redis_args;
  }
};

struct RedisConcurrencyKey {
  std::string table_name;
  std::string key;

  template <typename H>
  friend H AbslHashValue(H h, const RedisConcurrencyKey &k) {
    return H::combine(std::move(h), k.table_name, k.key);
  }
  bool operator==(const RedisConcurrencyKey &other) const {
    return table_name == other.table_name && key == other.key;
  }
};

inline std::ostream &operator<<(std::ostream &os, const RedisConcurrencyKey &key) {
  os << "{" << key.table_name << ", " << key.key << "}";
  return os;
}

struct RedisClientOptions {
  // Redis server address.
  std::string ip;
  int port;

  // Redis username and password.
  std::string username;
  std::string password;

  // Whether to use TLS/SSL for the connection.
  bool enable_ssl = false;
};

// StoreClient using Redis as persistence backend.
//
// The StoreClient does not currently handle any failures (transient or otherwise) of
// the Redis server. A periodic health check runs in the background and it will crash
// the process if the Redis server cannot be reached.
//
// Note in redis term a "key" points to a hash table and a "field" is a key, a "value"
// is just a value. We double quote "key" and "field" to avoid confusion.
//
// In variable namings, we stick to the table - key - value terminology.
//
// Schema:
// - Each table is a Redis HASH. The HASH "key" is
//    "RAY" + `external_storage_namespace` + "@" + `table_name`.
// - Each key-value pair in the hash is a row in the table. The "field" is the key.
//
// Consistency:
// - All Put/Get/Delete operations to a same (table, key) pair are serialized, see #35123.
// - For MultiGet/BatchDelete operations, they are subject to *all* keys in the operation,
//      i.e. only after it's at the queue front of all keys, it will be processed.
// - A big loophole is GetAll and AsyncGetKeys. They're not serialized with other
// operations, since "since it's either RPC call or used during initializing GCS". [1]
// [1] https://github.com/ray-project/ray/pull/35123#issuecomment-1546549046
class RedisStoreClient : public StoreClient {
 public:
  /// Connect to Redis. Not thread safe.
  ///
  /// \param io_service The event loop for this client. Must be single threaded.
  /// \param options The options for connecting to Redis.
  explicit RedisStoreClient(instrumented_io_context &io_service,
                            const RedisClientOptions &options);

  void AsyncPut(const std::string &table_name,
                const std::string &key,
                std::string data,
                bool overwrite,
                Postable<void(bool)> callback) override;

  void AsyncGet(const std::string &table_name,
                const std::string &key,
                ToPostable<OptionalItemCallback<std::string>> callback) override;

  void AsyncGetAll(
      const std::string &table_name,
      Postable<void(absl::flat_hash_map<std::string, std::string>)> callback) override;

  void AsyncMultiGet(
      const std::string &table_name,
      const std::vector<std::string> &keys,
      Postable<void(absl::flat_hash_map<std::string, std::string>)> callback) override;

  void AsyncDelete(const std::string &table_name,
                   const std::string &key,
                   Postable<void(bool)> callback) override;

  void AsyncBatchDelete(const std::string &table_name,
                        const std::vector<std::string> &keys,
                        Postable<void(int64_t)> callback) override;

  void AsyncGetNextJobID(Postable<void(int)> callback) override;

  void AsyncGetKeys(const std::string &table_name,
                    const std::string &prefix,
                    Postable<void(std::vector<std::string>)> callback) override;

  void AsyncExists(const std::string &table_name,
                   const std::string &key,
                   Postable<void(bool)> callback) override;

  // Check if Redis is available.
  //
  // \param callback The callback that will be called with a Status. OK means healthy.
  void AsyncCheckHealth(Postable<void(Status)> callback);

 private:
  /// \class RedisScanner
  ///
  /// This class is used to HSCAN data from a Redis table.
  ///
  /// The scan is not locked with other operations. It's not guaranteed to be consistent
  /// with other operations. It's batched by
  /// RAY_maximum_gcs_storage_operation_batch_size.
  class RedisScanner {
   private:
    // We want a private ctor but can use make_shared.
    // See https://en.cppreference.com/w/cpp/memory/enable_shared_from_this
    struct PrivateCtorTag {};

   public:
    // Don't call this. Use ScanKeysAndValues instead.
    explicit RedisScanner(
        PrivateCtorTag tag,
        std::shared_ptr<RedisContext> primary_context,
        RedisKey redis_key,
        RedisMatchPattern match_pattern,
        Postable<void(absl::flat_hash_map<std::string, std::string>)> callback);

    static void ScanKeysAndValues(
        std::shared_ptr<RedisContext> primary_context,
        RedisKey redis_key,
        RedisMatchPattern match_pattern,
        Postable<void(absl::flat_hash_map<std::string, std::string>)> callback);

   private:
    // Scans the keys and values, one batch a time. Once all keys are scanned, the
    // callback will be called. When the calls are in progress, the scanner temporarily
    // holds its own reference so users don't need to keep it alive.
    void Scan();

    void OnScanCallback(const std::shared_ptr<CallbackReply> &reply);

    /// The table name that the scanner will scan.
    RedisKey redis_key_;

    /// The pattern to match the keys.
    RedisMatchPattern match_pattern_;

    /// Mutex to protect the cursor_ field and the keys_ field and the
    /// key_value_map_ field.
    absl::Mutex mutex_;

    /// All keys that scanned from redis.
    absl::flat_hash_map<std::string, std::string> results_;

    /// The scan cursor.
    std::optional<size_t> cursor_;

    /// The pending shard scan count.
    std::atomic<size_t> pending_request_count_{0};

    std::shared_ptr<RedisContext> primary_context_;

    Postable<void(absl::flat_hash_map<std::string, std::string>)> callback_;

    // Holds a self-ref until the scan is done.
    std::shared_ptr<RedisScanner> self_ref_;
  };

  // Push a request to the sending queue.
  //
  // \param keys The keys impacted by the request.
  // \param send_request The request to send.
  //
  // \return The number of queues newly added. A queue will be added
  // only when there is no in-flight request for the key.
  size_t PushToSendingQueue(const std::vector<RedisConcurrencyKey> &keys,
                            const std::function<void()> &send_request)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);

  // Take requests from the sending queue and erase the queue if it's
  // empty.
  //
  // \param keys The keys to check for next request
  //
  // \return The requests to send.
  std::vector<std::function<void()>> TakeRequestsFromSendingQueue(
      const std::vector<RedisConcurrencyKey> &keys) ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);

  void DeleteByKeys(const std::string &table_name,
                    const std::vector<std::string> &keys,
                    Postable<void(int64_t)> callback);

  // Send the redis command to the server. This method will make request to be
  // serialized for each key in keys. At a given time, only one request for a {table_name,
  // key} will be in flight.
  //
  // \param keys Used as concurrency key.
  // \param args The redis commands
  // \param redis_callback The callback to call when the reply is received.
  void SendRedisCmdWithKeys(std::vector<std::string> keys,
                            RedisCommand command,
                            RedisCallback redis_callback);

  // Conveinence method for SendRedisCmdWithKeys with keys = command.args.
  // Reason for this method: if you call SendRedisCmdWithKeys(command.args,
  // std::move(command)), it's UB because C++ don't have arg evaluation order guarantee,
  // hence command.args may become empty.
  void SendRedisCmdArgsAsKeys(RedisCommand command, RedisCallback redis_callback);

  // HMGET external_storage_namespace@table_name key1 key2 ...
  // `keys` are chunked to multiple HMGET commands by
  // RAY_maximum_gcs_storage_operation_batch_size.
  void MGetValues(const std::string &table_name,
                  const std::vector<std::string> &keys,
                  Postable<void(absl::flat_hash_map<std::string, std::string>)> callback);

  instrumented_io_context &io_service_;

  RedisClientOptions options_;

  std::string external_storage_namespace_;

  // The following context writes everything to the primary shard.
  std::shared_ptr<RedisContext> primary_context_;

  absl::Mutex mu_;

  // The pending redis requests queue for each key.
  // The queue will be poped when the request is processed.
  absl::flat_hash_map<RedisConcurrencyKey, std::queue<std::function<void()>>>
      pending_redis_request_by_key_ ABSL_GUARDED_BY(mu_);
  FRIEND_TEST(RedisStoreClientTest, Random);
};

// Helper function used by Python to delete all redis HASHes with a given prefix.
bool RedisDelKeyPrefixSync(const std::string &host,
                           int32_t port,
                           const std::string &username,
                           const std::string &password,
                           bool use_ssl,
                           const std::string &external_storage_namespace);

}  // namespace gcs

}  // namespace ray
