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
struct redisSSLContext;

namespace ray {

namespace gcs {

using rpc::TablePrefix;

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
                 int64_t start_time,
                 instrumented_io_context &io_service)
        : callback_(callback), start_time_(start_time), io_service_(&io_service) {}

    void Dispatch(std::shared_ptr<CallbackReply> &reply) {
      std::shared_ptr<CallbackItem> self = shared_from_this();
      if (callback_ != nullptr) {
        io_service_->post([self, reply]() { self->callback_(std::move(reply)); },
                          "RedisCallbackManager.DispatchCallback");
      }
    }

    RedisCallback callback_;
    int64_t start_time_;
    instrumented_io_context *io_service_;
  };

  /// Allocate an index at which we can add a callback later on.
  int64_t AllocateCallbackIndex();

  /// Add a callback at an optionally specified index.
  int64_t AddCallback(const RedisCallback &function,
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
  RedisContext(instrumented_io_context &io_service);

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
                 const std::string &password,
                 bool enable_ssl = false);

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

  redisContext *sync_context() {
    RAY_CHECK(context_);
    return context_;
  }

  RedisAsyncContext &async_context() {
    RAY_CHECK(redis_async_context_);
    return *redis_async_context_;
  }

  instrumented_io_context &io_service() { return io_service_; }

 private:
  // These functions avoid problems with dependence on hiredis headers with clang-cl.
  static int GetRedisError(redisContext *context);
  static void FreeRedisReply(void *reply);

  instrumented_io_context &io_service_;
  redisContext *context_;
  redisSSLContext *ssl_context_;
  std::unique_ptr<RedisAsyncContext> redis_async_context_;
};

}  // namespace gcs

}  // namespace ray
