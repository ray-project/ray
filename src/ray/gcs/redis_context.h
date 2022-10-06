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

#include <boost/asio.hpp>
#include <boost/bind/bind.hpp>
#include <functional>
#include <memory>
#include <mutex>
#include <unordered_map>

#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/id.h"
#include "ray/common/status.h"
#include "ray/gcs/redis_async_context.h"
#include "ray/util/logging.h"
#include "src/ray/protobuf/gcs.pb.h"

struct redisContext;
struct redisAsyncContext;

namespace ray {

namespace gcs {

using rpc::TablePrefix;
using rpc::TablePubsub;

/// A simple reply wrapper for redis reply.
class CallbackReply {
 public:
  explicit CallbackReply(redisReply *redis_reply);

  /// Whether this reply is `nil` type reply.
  bool IsNil() const;

  /// Read this reply data as an integer.
  int64_t ReadAsInteger() const;

  /// Read this reply data as a status.
  Status ReadAsStatus() const;

  /// Read this reply data as a string.
  ///
  /// Note that this will return an empty string if
  /// the type of this reply is `nil` or `status`.
  const std::string &ReadAsString() const;

  /// Read this reply data as pub-sub data.
  const std::string &ReadAsPubsubData() const;

  /// Read this reply data as a string array.
  [[nodiscard]] const std::vector<std::optional<std::string>> &ReadAsStringArray() const;

  /// Read this reply data as a scan array.
  ///
  /// \param array The result array of scan.
  /// \return size_t The next cursor for scan.
  size_t ReadAsScanArray(std::vector<std::string> *array) const;

  bool IsSubscribeCallback() const { return is_subscribe_callback_; }

  bool IsUnsubscribeCallback() const { return is_unsubscribe_callback_; }

 private:
  /// Parse redis reply as string array or scan array.
  void ParseAsStringArrayOrScanArray(redisReply *redis_reply);

  /// Parse redis reply as string array.
  void ParseAsStringArray(redisReply *redis_reply);

  /// Flag indicating the type of reply this represents.
  int reply_type_;

  /// Reply data if reply_type_ is REDIS_REPLY_INTEGER.
  int64_t int_reply_;

  /// Reply data if reply_type_ is REDIS_REPLY_STATUS.
  Status status_reply_;

  /// Reply data if reply_type_ is REDIS_REPLY_STRING.
  std::string string_reply_;

  /// Reply data if reply_type_ is REDIS_REPLY_ARRAY.
  /// Represent the reply of StringArray or ScanArray.
  std::vector<std::optional<std::string>> string_array_reply_;

  bool is_subscribe_callback_ = false;
  bool is_unsubscribe_callback_ = false;

  /// Represent the reply of SCanArray, means the next scan cursor for scan request.
  size_t next_scan_cursor_reply_{0};
};

/// Every callback should take in a vector of the results from the Redis
/// operation.
using RedisCallback = std::function<void(std::shared_ptr<CallbackReply>)>;

void GlobalRedisCallback(void *c, void *r, void *privdata);

class RedisCallbackManager {
 public:
  static RedisCallbackManager &instance() {
    static RedisCallbackManager instance;
    return instance;
  }

  struct CallbackItem : public std::enable_shared_from_this<CallbackItem> {
    CallbackItem() = default;

    CallbackItem(const RedisCallback &callback,
                 bool is_subscription,
                 int64_t start_time,
                 instrumented_io_context &io_service)
        : callback_(callback),
          is_subscription_(is_subscription),
          start_time_(start_time),
          io_service_(&io_service) {}

    void Dispatch(std::shared_ptr<CallbackReply> &reply) {
      std::shared_ptr<CallbackItem> self = shared_from_this();
      if (callback_ != nullptr) {
        io_service_->post([self, reply]() { self->callback_(std::move(reply)); },
                          "RedisCallbackManager.DispatchCallback");
      }
    }

    RedisCallback callback_;
    bool is_subscription_;
    int64_t start_time_;
    instrumented_io_context *io_service_;
  };

  /// Allocate an index at which we can add a callback later on.
  int64_t AllocateCallbackIndex();

  /// Add a callback at an optionally specified index.
  int64_t AddCallback(const RedisCallback &function,
                      bool is_subscription,
                      instrumented_io_context &io_service,
                      int64_t callback_index = -1);

  /// Remove a callback.
  void RemoveCallback(int64_t callback_index);

  /// Get a callback.
  std::shared_ptr<CallbackItem> GetCallback(int64_t callback_index) const;

 private:
  RedisCallbackManager() : num_callbacks_(0){};

  ~RedisCallbackManager() {}

  mutable std::mutex mutex_;

  int64_t num_callbacks_ = 0;
  absl::flat_hash_map<int64_t, std::shared_ptr<CallbackItem>> callback_items_;
};

class RedisContext {
 public:
  RedisContext(instrumented_io_context &io_service)
      : io_service_(io_service), context_(nullptr) {}

  ~RedisContext();

  /// Test whether the address and port has a reachable Redis service.
  ///
  /// \param address IP address to test.
  /// \param port port number to test.
  /// \return The Status that we would get if we Connected.
  Status PingPort(const std::string &address, int port);

  Status Connect(const std::string &address,
                 int port,
                 bool sharding,
                 const std::string &password);

  /// Run an arbitrary Redis command synchronously.
  ///
  /// \param args The vector of command args to pass to Redis.
  /// \return CallbackReply(The reply from redis).
  std::unique_ptr<CallbackReply> RunArgvSync(const std::vector<std::string> &args);

  /// Run an arbitrary Redis command without a callback.
  ///
  /// \param args The vector of command args to pass to Redis.
  /// \param redis_callback The Redis callback function.
  /// \return Status.
  Status RunArgvAsync(const std::vector<std::string> &args,
                      const RedisCallback &redis_callback = nullptr);

  /// Subscribe to a specific Pub-Sub channel.
  ///
  /// \param node_id The node ID that subscribe this message.
  /// \param pubsub_channel The Pub-Sub channel to subscribe to.
  /// \param redisCallback The callback function that the notification calls.
  /// \param out_callback_index The output pointer to callback index.
  /// \return Status.
  Status SubscribeAsync(const NodeID &node_id,
                        const TablePubsub pubsub_channel,
                        const RedisCallback &redisCallback,
                        int64_t *out_callback_index);

  /// Subscribes the client to the given channel.
  ///
  /// \param channel The subscription channel.
  /// \param redisCallback The callback function that the notification calls.
  /// \param callback_index The index at which to add the callback. This index
  /// must already be allocated in the callback manager via
  /// RedisCallbackManager::AllocateCallbackIndex.
  /// \return Status.
  Status SubscribeAsync(const std::string &channel,
                        const RedisCallback &redisCallback,
                        int64_t callback_index);

  /// Unsubscribes the client from the given channel.
  ///
  /// \param channel The unsubscription channel.
  /// \return Status.
  Status UnsubscribeAsync(const std::string &channel);

  /// Subscribes the client to the given pattern.
  ///
  /// \param pattern The pattern of subscription channel.
  /// \param redisCallback The callback function that the notification calls.
  /// \param callback_index The index at which to add the callback. This index
  /// must already be allocated in the callback manager via
  /// RedisCallbackManager::AllocateCallbackIndex.
  /// \return Status.
  Status PSubscribeAsync(const std::string &pattern,
                         const RedisCallback &redisCallback,
                         int64_t callback_index);

  /// Unsubscribes the client from the given pattern.
  ///
  /// \param pattern The pattern of unsubscription channel.
  /// \return Status.
  Status PUnsubscribeAsync(const std::string &pattern);

  /// Posts a message to the given channel.
  ///
  /// \param channel The channel for message publishing to redis.
  /// \param message The message to be published to redis.
  /// \param redisCallback The callback will be called when the message is published to
  /// redis. \return Status.
  Status PublishAsync(const std::string &channel,
                      const std::string &message,
                      const RedisCallback &redisCallback);

  redisContext *sync_context() {
    RAY_CHECK(context_);
    return context_;
  }

  RedisAsyncContext &async_context() {
    RAY_CHECK(redis_async_context_);
    return *redis_async_context_;
  }

  RedisAsyncContext &subscribe_context() {
    RAY_CHECK(async_redis_subscribe_context_);
    return *async_redis_subscribe_context_;
  }

  instrumented_io_context &io_service() { return io_service_; }

 private:
  // These functions avoid problems with dependence on hiredis headers with clang-cl.
  static int GetRedisError(redisContext *context);
  static void FreeRedisReply(void *reply);

  instrumented_io_context &io_service_;
  redisContext *context_;
  std::unique_ptr<RedisAsyncContext> redis_async_context_;
  std::unique_ptr<RedisAsyncContext> async_redis_subscribe_context_;
};

}  // namespace gcs

}  // namespace ray
