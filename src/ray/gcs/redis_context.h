#ifndef RAY_GCS_REDIS_CONTEXT_H
#define RAY_GCS_REDIS_CONTEXT_H

#include <functional>
#include <memory>
#include <mutex>
#include <unordered_map>

#include "ray/common/id.h"
#include "ray/common/status.h"
#include "ray/util/logging.h"

#include "ray/gcs/redis_async_context_wrapper.h"
#include "ray/protobuf/gcs.pb.h"

extern "C" {
#include "ray/thirdparty/hiredis/adapters/ae.h"
#include "ray/thirdparty/hiredis/async.h"
#include "ray/thirdparty/hiredis/hiredis.h"
}

struct redisContext;
struct redisAsyncContext;
struct aeEventLoop;

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

  /// Read this reply data as a string.
  ///
  /// Note that this will return an empty string if
  /// the type of this reply is `nil` or `status`.
  std::string ReadAsString() const;

  /// Read this reply data as a status.
  Status ReadAsStatus() const;

  /// Read this reply data as a pub-sub data.
  std::string ReadAsPubsubData() const;

  /// Read this reply data as a string array.
  ///
  /// \param array Since the return-value may be large,
  /// make it as an output parameter.
  void ReadAsStringArray(std::vector<std::string> *array) const;

 private:
  redisReply *redis_reply_;
};

/// Every callback should take in a vector of the results from the Redis
/// operation.
using RedisCallback = std::function<void(const CallbackReply &)>;

void GlobalRedisCallback(void *c, void *r, void *privdata);

class RedisCallbackManager {
 public:
  static RedisCallbackManager &instance() {
    static RedisCallbackManager instance;
    return instance;
  }

  struct CallbackItem {
    CallbackItem() = default;

    CallbackItem(const RedisCallback &callback, bool is_subscription,
                 int64_t start_time) {
      this->callback = callback;
      this->is_subscription = is_subscription;
      this->start_time = start_time;
    }

    RedisCallback callback;
    bool is_subscription;
    int64_t start_time;
  };

  int64_t add(const RedisCallback &function, bool is_subscription);

  CallbackItem &get(int64_t callback_index);

  /// Remove a callback.
  void remove(int64_t callback_index);

 private:
  RedisCallbackManager() : num_callbacks_(0){};

  ~RedisCallbackManager() {}

  std::mutex mutex_;

  int64_t num_callbacks_ = 0;
  std::unordered_map<int64_t, CallbackItem> callback_items_;
};

class RedisContext {
 public:
  RedisContext() : context_(nullptr) {}

  ~RedisContext();

  Status Connect(const std::string &address, int port, bool sharding,
                 const std::string &password);

  /// Run an operation on some table key.
  ///
  /// \param command The command to run. This must match a registered Ray Redis
  /// command. These are strings of the format "RAY.TABLE_*".
  /// \param id The table key to run the operation at.
  /// \param data The data to add to the table key, if any.
  /// \param length The length of the data to be added, if data is provided.
  /// \param prefix
  /// \param pubsub_channel
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
  /// \return Status.
  Status RunArgvAsync(const std::vector<std::string> &args);

  /// Subscribe to a specific Pub-Sub channel.
  ///
  /// \param client_id The client ID that subscribe this message.
  /// \param pubsub_channel The Pub-Sub channel to subscribe to.
  /// \param redisCallback The callback function that the notification calls.
  /// \param out_callback_index The output pointer to callback index.
  /// \return Status.
  Status SubscribeAsync(const ClientID &client_id, const TablePubsub pubsub_channel,
                        const RedisCallback &redisCallback, int64_t *out_callback_index);

  redisContext *sync_context() { return context_; }

  RedisAsyncContextWrapper &async_context() { return *async_context_wrapper_; }

  RedisAsyncContextWrapper &subscribe_context() {
    return *async_subscribe_context_wrapper_;
  }

 private:
  redisContext *context_;
  std::unique_ptr<RedisAsyncContextWrapper> async_context_wrapper_;
  std::unique_ptr<RedisAsyncContextWrapper> async_subscribe_context_wrapper_;
};

template <typename ID>
Status RedisContext::RunAsync(const std::string &command, const ID &id, const void *data,
                              size_t length, const TablePrefix prefix,
                              const TablePubsub pubsub_channel,
                              RedisCallback redisCallback, int log_length) {
  int64_t callback_index = RedisCallbackManager::instance().add(redisCallback, false);
  Status status = Status::OK();
  if (length > 0) {
    if (log_length >= 0) {
      std::string redis_command = command + " %d %d %b %b %d";
      status = async_context_wrapper_->RedisAsyncCommand(
          reinterpret_cast<redisCallbackFn *>(&GlobalRedisCallback),
          reinterpret_cast<void *>(callback_index), redis_command.c_str(), prefix,
          pubsub_channel, id.Data(), id.Size(), data, length, log_length);
    } else {
      std::string redis_command = command + " %d %d %b %b";
      status = async_context_wrapper_->RedisAsyncCommand(
          reinterpret_cast<redisCallbackFn *>(&GlobalRedisCallback),
          reinterpret_cast<void *>(callback_index), redis_command.c_str(), prefix,
          pubsub_channel, id.Data(), id.Size(), data, length);
    }
  } else {
    RAY_CHECK(log_length == -1);
    std::string redis_command = command + " %d %d %b";
    status = async_context_wrapper_->RedisAsyncCommand(
        reinterpret_cast<redisCallbackFn *>(&GlobalRedisCallback),
        reinterpret_cast<void *>(callback_index), redis_command.c_str(), prefix,
        pubsub_channel, id.Data(), id.Size());
  }
  return status;
}

}  // namespace gcs

}  // namespace ray

#endif  // RAY_GCS_REDIS_CONTEXT_H
