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

extern "C" {
#include "hiredis/hiredis.h"
}

struct redisContext;
struct redisAsyncContext;
struct redisSSLContext;

namespace ray {

namespace gcs {

using rpc::TablePrefix;

void FreeRedisContext(redisContext *context);
void FreeRedisContext(redisAsyncContext *context);

template <typename RedisContextType>
struct RedisDeleter {
  RedisDeleter(){};

  void operator()(RedisContextType *context) { FreeRedisContext(context); }
};

/// A simple reply wrapper for redis reply.
class CallbackReply {
 public:
  explicit CallbackReply(redisReply *redis_reply);

  /// Whether this reply is `nil` type reply.
  bool IsNil() const;

  /// Whether an error happened;
  bool IsError() const;

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

  /// Reply data if reply_type_ is REDIS_REPLY_ERROR.
  std::string error_reply_;

  /// Reply data if reply_type_ is REDIS_REPLY_ARRAY.
  /// Represent the reply of StringArray or ScanArray.
  std::vector<std::optional<std::string>> string_array_reply_;

  /// Represent the reply of SCanArray, means the next scan cursor for scan request.
  size_t next_scan_cursor_reply_{0};
};

/// Every callback should take in a vector of the results from the Redis
/// operation.
using RedisCallback = std::function<void(std::shared_ptr<CallbackReply>)>;

class RedisContext;
struct RedisRequestContext {
  RedisRequestContext(instrumented_io_context &io_service,
                      RedisCallback callback,
                      std::shared_ptr<RedisAsyncContext> &context,
                      std::vector<std::string> args);

  static void RedisResponseFn(struct redisAsyncContext *async_context,
                              void *raw_reply,
                              void *privdata);

  void Run();

 private:
  ExponentialBackOff exp_back_off_;
  instrumented_io_context &io_service_;
  std::shared_ptr<RedisAsyncContext> redis_context_;
  size_t pending_retries_;
  RedisCallback callback_;
  absl::Time start_time_;

  std::vector<std::string> redis_cmds_;
  std::vector<const char *> argv_;
  std::vector<size_t> argc_;
};

class RedisContext {
 public:
  RedisContext(instrumented_io_context &io_service);

  virtual ~RedisContext();

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

  /// Disconnect from the server.
  void Disconnect();

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
  virtual void RunArgvAsync(std::vector<std::string> args,
                            RedisCallback redis_callback = nullptr);

  redisContext *sync_context() {
    RAY_CHECK(context_);
    return context_.get();
  }

  std::shared_ptr<RedisAsyncContext> async_context() {
    absl::MutexLock l(&mu_);
    RAY_CHECK(redis_async_context_);
    return redis_async_context_;
  }

  instrumented_io_context &io_service() { return io_service_; }

  std::pair<std::string, int> GetLeaderAddress();

 private:
  // These functions avoid problems with dependence on hiredis headers with clang-cl.
  static int GetRedisError(redisContext *context);
  static void FreeRedisReply(void *reply);

  instrumented_io_context &io_service_;

  std::unique_ptr<redisContext, RedisDeleter<redisContext>> context_;
  redisSSLContext *ssl_context_;
  absl::Mutex mu_;
  std::shared_ptr<RedisAsyncContext> redis_async_context_ ABSL_GUARDED_BY(mu_);
};

template <typename RedisContextType, typename RedisConnectFunctionType>
std::pair<Status, std::unique_ptr<RedisContextType, RedisDeleter<RedisContextType>>>
ConnectWithoutRetries(const std::string &address,
                      int port,
                      const RedisConnectFunctionType &connect_function) {
  // This currently returns the errorMessage in two different ways,
  // as an output parameter and in the Status::RedisError,
  // because we're not sure whether we'll want to change what this returns.
  RedisContextType *newContext = connect_function(address.c_str(), port);
  if (newContext == nullptr || (newContext)->err) {
    std::ostringstream oss;
    if (newContext == nullptr) {
      oss << "Could not allocate Redis context.";
    } else if (newContext->err) {
      oss << "Could not establish connection to Redis " << address << ":" << port
          << " (context.err = " << newContext->err << ").";
    }
    return std::make_pair(Status::RedisError(oss.str()), nullptr);
  }
  return std::make_pair(Status::OK(),
                        std::unique_ptr<RedisContextType, RedisDeleter<RedisContextType>>(
                            newContext, RedisDeleter<RedisContextType>()));
}

template <typename RedisContextType, typename RedisConnectFunctionType>
std::pair<Status, std::unique_ptr<RedisContextType, RedisDeleter<RedisContextType>>>
ConnectWithRetries(const std::string &address,
                   int port,
                   const RedisConnectFunctionType &connect_function) {
  RAY_LOG(INFO) << "Attempting to connect to address " << address << ":" << port << ".";
  int connection_attempts = 0;
  auto resp = ConnectWithoutRetries<RedisContextType>(address, port, connect_function);
  for (auto status = resp.first; !status.ok();) {
    if (connection_attempts >= RayConfig::instance().redis_db_connect_retries()) {
      RAY_LOG(FATAL) << RayConfig::instance().redis_db_connect_retries() << " attempts "
                     << "to connect have all failed. Please check whether the"
                     << " redis storage is alive or not. The last error message was: "
                     << status.ToString();
      break;
    }
    RAY_LOG_EVERY_MS(ERROR, 1000)
        << "Failed to connect to Redis due to: " << status.ToString()
        << ". Will retry in "
        << RayConfig::instance().redis_db_connect_wait_milliseconds() << "ms.";

    // Sleep for a little.
    std::this_thread::sleep_for(std::chrono::milliseconds(
        RayConfig::instance().redis_db_connect_wait_milliseconds()));
    resp = ConnectWithoutRetries<RedisContextType>(address, port, connect_function);
    connection_attempts += 1;
  }
  return resp;
}

}  // namespace gcs

}  // namespace ray
