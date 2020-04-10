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

#ifndef RAY_GCS_REDIS_CONTEXT_H
#define RAY_GCS_REDIS_CONTEXT_H

#include <boost/asio.hpp>
#include <boost/bind.hpp>
#include <functional>
#include <memory>
#include <mutex>
#include <unordered_map>

#include "ray/common/id.h"
#include "ray/common/status.h"
#include "ray/util/logging.h"

#include "ray/gcs/redis_async_context.h"
#include "ray/protobuf/gcs.pb.h"

extern "C" {
#include "hiredis/async.h"
#include "hiredis/hiredis.h"
}

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
  const std::vector<std::string> &ReadAsStringArray() const;

  /// Read this reply data as a scan array.
  ///
  /// \param array The result array of scan.
  /// \return size_t The next cursor for scan.
  size_t ReadAsScanArray(std::vector<std::string> *array) const;

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
  std::vector<std::string> string_array_reply_;

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

    CallbackItem(const RedisCallback &callback, bool is_subscription, int64_t start_time,
                 boost::asio::io_service &io_service)
        : callback_(callback),
          is_subscription_(is_subscription),
          start_time_(start_time),
          io_service_(&io_service) {}

    void Dispatch(std::shared_ptr<CallbackReply> &reply) {
      std::shared_ptr<CallbackItem> self = shared_from_this();
      if (callback_ != nullptr) {
        io_service_->post([self, reply]() { self->callback_(std::move(reply)); });
      }
    }

    RedisCallback callback_;
    bool is_subscription_;
    int64_t start_time_;
    boost::asio::io_service *io_service_;
  };

  int64_t add(const RedisCallback &function, bool is_subscription,
              boost::asio::io_service &io_service);

  std::shared_ptr<CallbackItem> get(int64_t callback_index);

  /// Remove a callback.
  void remove(int64_t callback_index);

 private:
  RedisCallbackManager() : num_callbacks_(0){};

  ~RedisCallbackManager() {}

  std::mutex mutex_;

  int64_t num_callbacks_ = 0;
  std::unordered_map<int64_t, std::shared_ptr<CallbackItem>> callback_items_;
};

class RedisContext {
 public:
  RedisContext(boost::asio::io_service &io_service)
      : io_service_(io_service), context_(nullptr) {}

  ~RedisContext();

  Status Connect(const std::string &address, int port, bool sharding,
                 const std::string &password);

  /// Run an operation on some table key synchronously.
  ///
  /// \param command The command to run. This must match a registered Ray Redis
  /// command. These are strings of the format "RAY.TABLE_*".
  /// \param id The table key to run the operation at.
  /// \param data The data to add to the table key, if any.
  /// \param length The length of the data to be added, if data is provided.
  /// \param prefix The prefix of table key.
  /// \param pubsub_channel The channel that update operations to the table
  /// should be published on.
  /// \param log_length The RAY.TABLE_APPEND command takes in an optional index
  /// at which the data must be appended. For all other commands, set to
  /// -1 for unused. If set, then data must be provided.
  /// \return The reply from redis.
  template <typename ID>
  std::shared_ptr<CallbackReply> RunSync(const std::string &command, const ID &id,
                                         const void *data, size_t length,
                                         const TablePrefix prefix,
                                         const TablePubsub pubsub_channel,
                                         int log_length = -1);

  /// Run an arbitrary Redis command synchronously.
  ///
  /// \param args The vector of command args to pass to Redis.
  /// \return CallbackReply(The reply from redis).
  std::unique_ptr<CallbackReply> RunArgvSync(const std::vector<std::string> &args);

  /// Run an operation on some table key.
  ///
  /// \param command The command to run. This must match a registered Ray Redis
  /// command. These are strings of the format "RAY.TABLE_*".
  /// \param id The table key to run the operation at.
  /// \param data The data to add to the table key, if any.
  /// \param length The length of the data to be added, if data is provided.
  /// \param prefix The prefix of table key.
  /// \param pubsub_channel The channel that update operations to the table
  /// should be published on.
  /// \param redisCallback The Redis callback function.
  /// \param log_length The RAY.TABLE_APPEND command takes in an optional index
  /// at which the data must be appended. For all other commands, set to
  /// -1 for unused. If set, then data must be provided.
  /// \return Status.
  template <typename ID>
  Status RunAsync(const std::string &command, const ID &id, const void *data,
                  size_t length, const TablePrefix prefix,
                  const TablePubsub pubsub_channel, RedisCallback redisCallback,
                  int log_length = -1);

  /// Run an arbitrary Redis command without a callback.
  ///
  /// \param args The vector of command args to pass to Redis.
  /// \param redis_callback The Redis callback function.
  /// \return Status.
  Status RunArgvAsync(const std::vector<std::string> &args,
                      const RedisCallback &redis_callback = nullptr);

  /// Subscribe to a specific Pub-Sub channel.
  ///
  /// \param client_id The client ID that subscribe this message.
  /// \param pubsub_channel The Pub-Sub channel to subscribe to.
  /// \param redisCallback The callback function that the notification calls.
  /// \param out_callback_index The output pointer to callback index.
  /// \return Status.
  Status SubscribeAsync(const ClientID &client_id, const TablePubsub pubsub_channel,
                        const RedisCallback &redisCallback, int64_t *out_callback_index);

  /// Subscribes the client to the given pattern.
  ///
  /// \param pattern The pattern of subscription channel.
  /// \param redisCallback The callback function that the notification calls.
  /// \param out_callback_index The output pointer to callback index.
  /// \return Status.
  Status PSubscribeAsync(const std::string &pattern, const RedisCallback &redisCallback,
                         int64_t *out_callback_index);

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
  Status PublishAsync(const std::string &channel, const std::string &message,
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

  boost::asio::io_service &io_service() { return io_service_; }

 private:
  boost::asio::io_service &io_service_;
  redisContext *context_;
  std::unique_ptr<RedisAsyncContext> redis_async_context_;
  std::unique_ptr<RedisAsyncContext> async_redis_subscribe_context_;
};

template <typename ID>
Status RedisContext::RunAsync(const std::string &command, const ID &id, const void *data,
                              size_t length, const TablePrefix prefix,
                              const TablePubsub pubsub_channel,
                              RedisCallback redisCallback, int log_length) {
  RAY_CHECK(redis_async_context_);
  int64_t callback_index =
      RedisCallbackManager::instance().add(redisCallback, false, io_service_);
  Status status = Status::OK();
  if (length > 0) {
    if (log_length >= 0) {
      std::string redis_command = command + " %d %d %b %b %d";
      status = redis_async_context_->RedisAsyncCommand(
          reinterpret_cast<redisCallbackFn *>(&GlobalRedisCallback),
          reinterpret_cast<void *>(callback_index), redis_command.c_str(), prefix,
          pubsub_channel, id.Data(), id.Size(), data, length, log_length);
    } else {
      std::string redis_command = command + " %d %d %b %b";
      status = redis_async_context_->RedisAsyncCommand(
          reinterpret_cast<redisCallbackFn *>(&GlobalRedisCallback),
          reinterpret_cast<void *>(callback_index), redis_command.c_str(), prefix,
          pubsub_channel, id.Data(), id.Size(), data, length);
    }
  } else {
    RAY_CHECK(log_length == -1);
    std::string redis_command = command + " %d %d %b";
    status = redis_async_context_->RedisAsyncCommand(
        reinterpret_cast<redisCallbackFn *>(&GlobalRedisCallback),
        reinterpret_cast<void *>(callback_index), redis_command.c_str(), prefix,
        pubsub_channel, id.Data(), id.Size());
  }
  return status;
}

template <typename ID>
std::shared_ptr<CallbackReply> RedisContext::RunSync(
    const std::string &command, const ID &id, const void *data, size_t length,
    const TablePrefix prefix, const TablePubsub pubsub_channel, int log_length) {
  RAY_CHECK(context_);
  void *redis_reply = nullptr;
  if (length > 0) {
    if (log_length >= 0) {
      std::string redis_command = command + " %d %d %b %b %d";
      redis_reply = redisCommand(context_, redis_command.c_str(), prefix, pubsub_channel,
                                 id.Data(), id.Size(), data, length, log_length);
    } else {
      std::string redis_command = command + " %d %d %b %b";
      redis_reply = redisCommand(context_, redis_command.c_str(), prefix, pubsub_channel,
                                 id.Data(), id.Size(), data, length);
    }
  } else {
    RAY_CHECK(log_length == -1);
    std::string redis_command = command + " %d %d %b";
    redis_reply = redisCommand(context_, redis_command.c_str(), prefix, pubsub_channel,
                               id.Data(), id.Size());
  }
  if (redis_reply == nullptr) {
    RAY_LOG(INFO) << "Run redis command failed , err is " << context_->err;
    return nullptr;
  } else {
    std::shared_ptr<CallbackReply> callback_reply =
        std::make_shared<CallbackReply>(reinterpret_cast<redisReply *>(redis_reply));
    freeReplyObject(redis_reply);
    return callback_reply;
  }
}

}  // namespace gcs

}  // namespace ray

#endif  // RAY_GCS_REDIS_CONTEXT_H
