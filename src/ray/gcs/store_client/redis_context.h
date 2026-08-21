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
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "ray/asio/instrumented_io_context.h"
#include "ray/common/status.h"
#include "ray/common/status_or.h"
#include "ray/gcs/store_client/redis_async_context.h"
#include "ray/observability/metric_interface.h"
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

/// Payload bytes of a Redis reply: the sum of `len` over every string-shaped
/// node, recursing into arrays. Integer and nil nodes contribute 0, and RESP
/// framing is never included -- `redisReply::len` is the decoded string length.
///
/// Error nodes are measured like any other string, but that only matters for an
/// error nested inside an aggregate reply. A *top-level* error never reaches
/// this function: RedisRequestContext::RedisResponseFn routes it into the retry
/// path before recording, which is why
/// gcs_redis_response_payload_bytes documents error replies as contributing
/// nothing.
///
/// Callers must invoke this while hiredis still owns the reply. Measuring the
/// raw reply rather than the `CallbackReply` built from it keeps the
/// measurement independent of what that copy chooses to keep, and works for
/// reply shapes `CallbackReply` does not parse.
///
/// \param reply The reply to measure. Must be a live hiredis reply.
/// \return The payload bytes it carries, 0 for replies that carry none.
size_t ResponsePayloadBytes(const redisReply &reply);

/// Maps a Redis verb onto a closed label domain, so that adding a Redis command
/// elsewhere in the GCS cannot grow the exported time series count. Returns
/// "OTHER" for anything not on the allowlist. The returned view points into
/// static storage and outlives any request.
///
/// Matching is exact and case sensitive: every verb in the tree is an uppercase
/// string literal.
///
/// \param verb The Redis command verb to normalize.
/// \return The matching allowlist entry, or "OTHER". Points into static
/// storage, so it outlives the argument.
std::string_view NormalizeRedisCommandLabel(std::string_view verb);

/// A TableName label value for commands that address no single GCS table.
/// Explicit sentinels rather than "": a blank label value is indistinguishable
/// from a dropped label in Grafana, and the two cases below mean different
/// things.
///
/// kNoTable: the command has no table at all (PING).
/// kAllTables: the command spans the whole storage namespace (SCAN/DEL/UNLINK
/// in the cleanup path).
inline constexpr std::string_view kNoTable = "NONE";
inline constexpr std::string_view kAllTables = "ALL";

/// Metrics recorded for every async Redis command. Held by reference: the
/// referents are owned by GcsServerMetrics and outlive the RedisContext.
struct RedisMetrics {
  ray::observability::MetricInterface &request_payload_bytes_sum;
  ray::observability::MetricInterface &response_payload_bytes_sum;
  ray::observability::MetricInterface &command_count_counter;
};

/// Bounded label values for one Redis command.
///
/// `command` must already be normalized (see NormalizeRedisCommandLabel) and
/// point into storage that outlives the call; `table` is copied by the request
/// context, because the RedisCommand a caller derives it from does not outlive
/// the reply.
struct RedisCommandLabels {
  std::string_view command;
  std::string_view table;
};

class RedisContext;
struct RedisRequestContext {
  /// \param metrics Payload metrics to record into, or nullptr to record
  /// nothing. Null both when the payload metrics are disabled by config and in
  /// the standalone namespace-cleanup process, which has no metrics exporter.
  RedisRequestContext(instrumented_io_context &io_service,
                      RedisCallback callback,
                      RedisAsyncContext *context,
                      std::vector<std::string> args,
                      ClockInterface &clock,
                      RedisMetrics *metrics,
                      RedisCommandLabels labels);

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

  // Nullable; see the constructor docs.
  RedisMetrics *metrics_;
  // Owned copies: the reply is delivered long after the caller's RedisCommand
  // is gone. Both fit libstdc++'s small-string buffer for every value the GCS
  // produces (the longest is "PLACEMENT_GROUP", 15 chars), so this does not
  // allocate.
  std::string command_label_;
  std::string table_label_;

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
  /// \param metrics Payload metrics for every command run through this context,
  /// or nullopt to record nothing.
  ///
  /// Held **by value** rather than by pointer on purpose. A RedisContext is
  /// shared: RedisStoreClient::RedisScanner copies the shared_ptr and keeps
  /// itself alive for the duration of a scan, so the context routinely outlives
  /// the RedisStoreClient that created it. Borrowing the metrics from the store
  /// client would leave RedisRequestContext dereferencing freed storage on the
  /// next HSCAN reply. Owning them here ties their lifetime to the object that
  /// RedisRequestContext already depends on outliving it (it holds a raw
  /// RedisAsyncContext * into this one).
  explicit RedisContext(instrumented_io_context &io_service,
                        ClockInterface &clock,
                        std::optional<RedisMetrics> metrics = std::nullopt);

  ~RedisContext();

  Status Connect(const std::string &address,
                 int port,
                 const std::string &username,
                 const std::string &password,
                 bool enable_ssl = false);

  /// Disconnect from the server.
  void Disconnect();

  /// Run an arbitrary Redis command.
  ///
  /// \param args The vector of command args to pass to Redis.
  /// \param redis_callback The Redis callback function.
  /// \param labels Payload metric labels for this command. Deliberately not
  /// defaulted: a new command site must decide how it is attributed, so
  /// instrumentation cannot be silently skipped.
  void RunArgvAsync(std::vector<std::string> args,
                    RedisCallback redis_callback,
                    RedisCommandLabels labels);

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

  Status ValidateRedisDB();

  StatusOr<bool> IsRedisSentinel();

  Status ConnectRedisCluster(const std::string &username,
                             const std::string &password,
                             bool enable_ssl,
                             const std::string &redis_address);

  instrumented_io_context &io_service_;
  ClockInterface &clock_;
  // Owned; see the constructor docs. Never reassigned after construction, so
  // the pointer handed to each RedisRequestContext stays valid for this
  // context's lifetime.
  std::optional<RedisMetrics> metrics_;

  std::unique_ptr<redisContext, RedisContextDeleter> context_;
  redisSSLContext *ssl_context_;
  std::unique_ptr<RedisAsyncContext> redis_async_context_;
  int64_t redis_db_probe_timeout_milliseconds_;
};

}  // namespace ray::gcs
