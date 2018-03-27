#ifndef RAY_GCS_REDIS_CONTEXT_H
#define RAY_GCS_REDIS_CONTEXT_H

#include <functional>
#include <memory>
#include <unordered_map>

#include "ray/id.h"
#include "ray/status.h"
#include "ray/util/logging.h"

#include "ray/gcs/format/gcs_generated.h"

struct redisContext;
struct redisAsyncContext;
struct aeEventLoop;

namespace ray {

namespace gcs {

class RedisCallbackManager {
 public:
  /// Every callback should take in a vector of the results from the Redis
  /// operation and return a bool indicating whether the callback should be
  /// deleted once called.
  using RedisCallback = std::function<bool(const std::string &)>;

  static RedisCallbackManager &instance() {
    static RedisCallbackManager instance;
    return instance;
  }

  int64_t add(const RedisCallback &function);

  RedisCallback &get(int64_t callback_index);

  /// Remove a callback.
  void remove(int64_t callback_index);

 private:
  RedisCallbackManager() : num_callbacks(0){};

  ~RedisCallbackManager() { printf("shut down callback manager\n"); }

  int64_t num_callbacks;
  std::unordered_map<int64_t, std::unique_ptr<RedisCallback>> callbacks_;
};

class RedisContext {
 public:
  RedisContext() {}
  ~RedisContext();
  Status Connect(const std::string &address, int port);
  Status AttachToEventLoop(aeEventLoop *loop);

  /// Run an operation on some table key.
  ///
  /// \param command The command to run. This must match a registered Ray Redis
  ///        command. These are strings of the format "RAY.TABLE_*".
  /// \param id The table key to run the operation at.
  /// \param data The data to add to the table key, if any.
  /// \param length The length of the data to be added, if data is provided.
  /// \param prefix
  /// \param pubsub_channel
  /// \param callback_index
  /// \param log_length The RAY.TABLE_APPEND command takes in an optional index
  ///        at which the data must be appended. For all other commands, set to
  ///        -1 for unused. If set, then data must be provided.
  Status RunAsync(const std::string &command, const UniqueID &id, const uint8_t *data,
                  int64_t length, const TablePrefix prefix,
                  const TablePubsub pubsub_channel, int64_t callback_index,
                  int log_length = -1);

  Status SubscribeAsync(const ClientID &client_id, const TablePubsub pubsub_channel,
                        int64_t callback_index);
  redisAsyncContext *async_context() { return async_context_; }
  redisAsyncContext *subscribe_context() { return subscribe_context_; };

 private:
  redisContext *context_;
  redisAsyncContext *async_context_;
  redisAsyncContext *subscribe_context_;
};

}  // namespace gcs

}  // namespace ray

#endif  // RAY_GCS_REDIS_CONTEXT_H
