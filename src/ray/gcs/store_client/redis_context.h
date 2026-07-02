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
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "ray/asio/instrumented_io_context.h"
#include "ray/asio/periodical_runner.h"
#include "ray/common/status.h"
#include "ray/gcs/store_client/redis_async_context.h"
#include "ray/gcs/store_client/redis_auth_provider.h"
#include "ray/stats/metric.h"
#include "ray/stats/tag_defs.h"
#include "ray/util/clock.h"
#include "ray/util/exponential_backoff.h"

extern "C" {
#include "hiredis/hiredis.h"
}

struct redisContext;
struct redisAsyncContext;
struct redisSSLContext;

namespace ray::gcs {

/// A simple reply wrapper for redis reply.
class CallbackReply {
 public:
  explicit CallbackReply(const redisReply &redis_reply);

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

  /// Read this reply data as a string array.
  const std::vector<std::optional<std::string>> &ReadAsStringArray() const;

  /// Read this reply data as a scan array.
  ///
  /// \param array The result array of scan.
  /// \return size_t The next cursor for scan.
  size_t ReadAsScanArray(std::vector<std::string> *array) const;

 private:
  /// Parse redis reply as string array or scan array.
  void ParseAsStringArrayOrScanArray(const redisReply &redis_reply);

  /// Parse redis reply as string array.
  void ParseAsStringArray(const redisReply &redis_reply);

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
                      RedisAsyncContext *context,
                      std::vector<std::string> args,
                      ClockInterface &clock);

  static void RedisResponseFn(redisAsyncContext *async_context,
                              void *raw_reply,
                              void *privdata);

  void Run();

 private:
  ExponentialBackoff exp_back_off_;
  instrumented_io_context &io_service_;
  RedisAsyncContext *redis_context_;
  size_t pending_retries_;
  RedisCallback callback_;
  absl::Time start_time_;

  std::vector<std::string> redis_cmds_;
  std::vector<const char *> argv_;
  std::vector<size_t> argc_;
  ClockInterface &clock_;

  // Ray metrics
  ray::stats::Histogram ray_metric_gcs_latency_{
      "gcs_latency",
      "The latency of a GCS (by default Redis) operation.",
      "us",
      {100, 200, 300, 400, 500, 600, 700, 800, 900, 1000},
      {"CustomKey"}};
};

class RedisContext {
 public:
  explicit RedisContext(instrumented_io_context &io_service, ClockInterface &clock);

  ~RedisContext();

  Status Connect(const std::string &address,
                 int port,
                 const std::string &username,
                 const std::string &password,
                 bool enable_ssl = false);

  /// Disconnect from the server.
  void Disconnect();

  /// If the configured auth provider issues credentials that expire (e.g.
  /// Microsoft Entra ID tokens), start a background timer that refreshes the
  /// credential and re-`AUTH`s the live connection before expiry. No-op for
  /// static credentials. Must be called after `Connect` succeeds.
  void StartTokenRefresh();

  /// Run an arbitrary Redis command without a callback.
  ///
  /// \param args The vector of command args to pass to Redis.
  /// \param redis_callback The Redis callback function.
  void RunArgvAsync(std::vector<std::string> args,
                    RedisCallback redis_callback = nullptr);

  redisContext *sync_context() {
    RAY_CHECK(context_);
    return context_.get();
  }

  RedisAsyncContext &async_context() {
    RAY_CHECK(redis_async_context_);
    return *redis_async_context_;
  }

  instrumented_io_context &io_service() { return io_service_; }

 private:
  /// Run an arbitrary Redis command synchronously.
  ///
  /// \param args The vector of command args to pass to Redis.
  /// \return CallbackReply(The reply from redis).
  std::unique_ptr<CallbackReply> RunArgvSync(const std::vector<std::string> &args);

  void ValidateRedisDB();

  bool IsRedisSentinel();

  Status ConnectRedisCluster(const std::string &username,
                             const std::string &password,
                             bool enable_ssl,
                             const std::string &redis_address);

  /// Lazily builds `auth_provider_` (once) based on RayConfig's REDIS_AUTH_MODE
  /// and the static username/password passed to `Connect`.
  void EnsureAuthProvider(const std::string &username, const std::string &password);

  /// Fetches the current credentials and, if the password changed since it was
  /// last applied, re-`AUTH`s both the sync and async connections.
  void RefreshAndReauthenticate();

  instrumented_io_context &io_service_;
  ClockInterface &clock_;

  std::unique_ptr<redisContext, RedisContextDeleter> context_;
  redisSSLContext *ssl_context_;
  std::unique_ptr<RedisAsyncContext> redis_async_context_;
  int64_t redis_db_probe_timeout_milliseconds_;

  /// Source of Redis AUTH credentials (static password or refreshable token).
  std::shared_ptr<RedisAuthProvider> auth_provider_;
  /// The password most recently applied via AUTH, to avoid redundant re-AUTHs.
  std::string last_applied_password_;
  /// Drives periodic token refresh; only created for refreshable providers.
  std::shared_ptr<PeriodicalRunner> token_refresh_runner_;
};

}  // namespace ray::gcs
