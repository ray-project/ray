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

#include "ray/gcs/store_client/redis_context.h"

#include <cerrno>
#include <charconv>
#include <cstddef>
#include <cstring>
#include <limits>
#include <memory>
#include <optional>
#include <random>
#include <sstream>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#ifdef _WIN32
#include <winsock2.h>
#endif

#include "ray/asio/asio_util.h"
#include "ray/gcs/store_client/redis_tcp_keepalive.h"
#include "ray/util/network_util.h"

extern "C" {
#include "hiredis/async.h"
#include "hiredis/hiredis_ssl.h"
}

// TODO(pcm): Integrate into the C++ tree.
#include "absl/functional/function_ref.h"
#include "absl/strings/ascii.h"
#include "absl/strings/match.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_join.h"
#include "absl/strings/str_split.h"
#include "absl/time/clock.h"
#include "absl/time/time.h"
#include "ray/common/ray_config.h"
#include "ray/common/status_or.h"

namespace ray {

namespace gcs {

namespace {

constexpr size_t kMaxRedisCommandLabelLength = 16;

/// Whether a command only reads. Everything else counts as a write and gets a
/// write watermark marker, so a write command added later is covered without
/// touching this list; a read left off it only costs a marker.
bool IsReadOnlyCommand(std::string_view verb) {
  for (std::string_view read_only : {"HGET",
                                     "HMGET",
                                     "HGETALL",
                                     "HSCAN",
                                     "HEXISTS",
                                     "HKEYS",
                                     "HLEN",
                                     "GET",
                                     "MGET",
                                     "EXISTS",
                                     "SCAN",
                                     "PING"}) {
    if (absl::EqualsIgnoreCase(verb, read_only)) {
      return true;
    }
  }
  return false;
}

/// Parse a write watermark value, "<incarnation>:<n>", into n. Returns nullopt
/// for a value written by another incarnation, or one that does not parse.
std::optional<uint64_t> ParseWriteWatermark(std::string_view value,
                                            std::string_view incarnation) {
  if (value.size() <= incarnation.size() + 1 ||
      value.substr(0, incarnation.size()) != incarnation ||
      value[incarnation.size()] != ':') {
    return std::nullopt;
  }
  const std::string_view digits = value.substr(incarnation.size() + 1);
  uint64_t n = 0;
  auto [end, ec] = std::from_chars(digits.data(), digits.data() + digits.size(), n);
  if (ec != std::errc() || end != digits.data() + digits.size()) {
    return std::nullopt;
  }
  return n;
}

}  // namespace

size_t ResponsePayloadBytes(const redisReply &reply) {
  switch (reply.type) {
  case REDIS_REPLY_STRING:
  case REDIS_REPLY_STATUS:
  // Only reachable for an error nested inside an aggregate reply; a top-level
  // error is retried before it gets here. See the header.
  case REDIS_REPLY_ERROR:
  case REDIS_REPLY_VERB:
  case REDIS_REPLY_BIGNUM:
    return static_cast<size_t>(reply.len);
  case REDIS_REPLY_INTEGER: {
    char buffer[std::numeric_limits<long long>::digits10 + 3];
    const auto [end, error] =
        std::to_chars(buffer, buffer + sizeof(buffer), reply.integer);
    RAY_CHECK(error == std::errc{});
    return static_cast<size_t>(end - buffer);
  }
  case REDIS_REPLY_DOUBLE:
    // hiredis preserves the original RESP3 decimal representation in `str`.
    return static_cast<size_t>(reply.len);
  case REDIS_REPLY_BOOL:
    // RESP3 encodes booleans as one application byte: `t` or `f`.
    return 1;
  case REDIS_REPLY_NIL:
    return 0;
  case REDIS_REPLY_ARRAY:
  case REDIS_REPLY_MAP:
  case REDIS_REPLY_SET:
  case REDIS_REPLY_PUSH: {
    // hiredis leaves `element` uninitialized when `elements` is 0, so the loop
    // bound is what keeps this from dereferencing garbage.
    size_t total = 0;
    for (size_t i = 0; i < reply.elements; ++i) {
      total += ResponsePayloadBytes(*reply.element[i]);
    }
    return total;
  }
  default:
    // Unknown reply types have no payload definition.
    return 0;
  }
}

std::string NormalizeRedisCommandLabel(std::string_view verb) {
  std::string label(verb.substr(0, kMaxRedisCommandLabelLength));
  absl::AsciiStrToUpper(&label);
  return label;
}

CallbackReply::CallbackReply(const redisReply &redis_reply)
    : reply_type_(redis_reply.type) {
  switch (reply_type_) {
  case REDIS_REPLY_NIL: {
    break;
  }
  case REDIS_REPLY_ERROR: {
    // Do not crash on a Redis error reply: store it so callers can surface it
    // as a Status (e.g. ValidateRedisDB during a non-fatal Connect) instead of
    // aborting the process.
    error_reply_ = std::string(redis_reply.str, redis_reply.len);
    break;
  }
  case REDIS_REPLY_INTEGER: {
    int_reply_ = static_cast<int64_t>(redis_reply.integer);
    break;
  }
  case REDIS_REPLY_STATUS: {
    const std::string status_str(redis_reply.str, redis_reply.len);
    if (status_str == "OK") {
      status_reply_ = Status::OK();
    } else {
      status_reply_ = Status::RedisError(status_str);
    }
    break;
  }
  case REDIS_REPLY_STRING: {
    string_reply_ = std::string(redis_reply.str, redis_reply.len);
    break;
  }
  case REDIS_REPLY_ARRAY: {
    if (redis_reply.elements == 0) {
      break;
    }
    // Array replies are used for scan or get.
    ParseAsStringArrayOrScanArray(redis_reply);
    break;
  }
  default: {
    RAY_LOG(ERROR) << "Encountered unexpected redis reply type: " << reply_type_;
  }
  }
}

bool CallbackReply::IsError() const { return reply_type_ == REDIS_REPLY_ERROR; }

void CallbackReply::ParseAsStringArrayOrScanArray(const redisReply &redis_reply) {
  RAY_CHECK(REDIS_REPLY_ARRAY == redis_reply.type);
  const auto array_size = static_cast<size_t>(redis_reply.elements);
  if (array_size == 2) {
    auto *cursor_entry = redis_reply.element[0];
    auto *array_entry = redis_reply.element[1];
    if (REDIS_REPLY_ARRAY == array_entry->type) {
      // Parse as a scan array
      RAY_CHECK(REDIS_REPLY_STRING == cursor_entry->type);
      std::string cursor_str(cursor_entry->str, cursor_entry->len);
      next_scan_cursor_reply_ = std::stoull(cursor_str);
      const auto scan_array_size = array_entry->elements;
      string_array_reply_.reserve(scan_array_size);
      for (size_t i = 0; i < scan_array_size; ++i) {
        auto *entry = array_entry->element[i];
        RAY_CHECK(REDIS_REPLY_STRING == entry->type)
            << "Unexcepted type: " << entry->type;
        string_array_reply_.emplace_back(std::string(entry->str, entry->len));
      }
      return;
    }
  }
  ParseAsStringArray(redis_reply);
}

void CallbackReply::ParseAsStringArray(const redisReply &redis_reply) {
  RAY_CHECK(REDIS_REPLY_ARRAY == redis_reply.type);
  const auto array_size = static_cast<size_t>(redis_reply.elements);
  string_array_reply_.reserve(array_size);
  for (size_t i = 0; i < array_size; ++i) {
    auto *entry = redis_reply.element[i];
    if (entry->type == REDIS_REPLY_STRING) {
      string_array_reply_.emplace_back(std::string(entry->str, entry->len));
    } else {
      RAY_CHECK(REDIS_REPLY_NIL == entry->type) << "Unexcepted type: " << entry->type;
      string_array_reply_.emplace_back();
    }
  }
}

bool CallbackReply::IsNil() const { return REDIS_REPLY_NIL == reply_type_; }

int64_t CallbackReply::ReadAsInteger() const {
  RAY_CHECK(reply_type_ == REDIS_REPLY_INTEGER) << "Unexpected type: " << reply_type_;
  return int_reply_;
}

Status CallbackReply::ReadAsStatus() const {
  RAY_CHECK(reply_type_ == REDIS_REPLY_STATUS) << "Unexpected type: " << reply_type_;
  return status_reply_;
}

const std::string &CallbackReply::ReadAsError() const {
  RAY_CHECK(reply_type_ == REDIS_REPLY_ERROR) << "Unexpected type: " << reply_type_;
  return error_reply_;
}

const std::string &CallbackReply::ReadAsString() const {
  RAY_CHECK(reply_type_ == REDIS_REPLY_STRING) << "Unexpected type: " << reply_type_;
  return string_reply_;
}

size_t CallbackReply::ReadAsScanArray(std::vector<std::string> *array) const {
  RAY_CHECK(reply_type_ == REDIS_REPLY_ARRAY) << "Unexpected type: " << reply_type_;
  array->clear();
  array->reserve(string_array_reply_.size());
  for (const auto &element : string_array_reply_) {
    RAY_CHECK(element.has_value());
    array->emplace_back(*element);
  }
  return next_scan_cursor_reply_;
}

const std::vector<std::optional<std::string>> &CallbackReply::ReadAsStringArray() const {
  RAY_CHECK(reply_type_ == REDIS_REPLY_ARRAY) << "Unexpected type: " << reply_type_;
  return string_array_reply_;
}

RedisRequestContext::RedisRequestContext(instrumented_io_context &io_service,
                                         RedisCallback callback,
                                         RedisAsyncContext *context,
                                         std::vector<std::string> args,
                                         ClockInterface &clock,
                                         RedisMetrics *metrics,
                                         std::string_view table_label)
    : exp_back_off_(RayConfig::instance().redis_retry_base_ms(),
                    RayConfig::instance().redis_retry_multiplier(),
                    RayConfig::instance().redis_retry_max_ms()),
      io_service_(io_service),
      redis_context_(context),
      // Initialized before `redis_cmds_`, so it reads `args` before the move.
      is_write_(!args.empty() && !IsReadOnlyCommand(args.front())),
      context_alive_(context->GetAliveToken()),
      pending_retries_(RayConfig::instance().num_redis_request_retries() + 1),
      callback_(std::move(callback)),
      start_time_(clock.Now()),
      redis_cmds_(std::move(args)),
      clock_(clock),
      metrics_(metrics) {
  RAY_CHECK(!redis_cmds_.empty());
  argc_.reserve(redis_cmds_.size());
  argv_.reserve(redis_cmds_.size());
  for (size_t i = 0; i < redis_cmds_.size(); ++i) {
    argv_.push_back(redis_cmds_[i].data());
    argc_.push_back(redis_cmds_[i].size());
  }
  if (metrics_ != nullptr) {
    command_label_ = NormalizeRedisCommandLabel(redis_cmds_.front());
    table_label_ = std::string(table_label);
    // This context owns the argument buffers, so their sizes remain valid even
    // when a caller moved a value into the command on the way here.
    for (const auto &command_arg : redis_cmds_) {
      request_payload_bytes_ += command_arg.size();
    }
  }
}

void RedisRequestContext::HandleFailure(RedisRequestContext *request_cxt,
                                        bool reached_redis,
                                        std::string_view error_msg) {
  if (request_cxt->context_alive_.expired()) {
    // The owning RedisAsyncContext is being destroyed and is flushing its
    // pending callbacks; the event loop may be going away with it. There is
    // nobody left to answer, and scheduling a retry timer would touch a
    // dying io_context. Drop the request instead.
    delete request_cxt;
    return;
  }
  bool refunded = false;
  if (!reached_redis) {
    // The command never made it to Redis: the connection was gone, or hiredis
    // accepted it on a context that then died. The attempt says nothing about
    // the command, so give the retry back while the outage is young. The
    // deadline belongs to the outage, not to this request, so every in-flight
    // command expires at the same instant however long it had been retrying.
    const absl::Time now = request_cxt->clock_.Now();
    const absl::Time deadline = request_cxt->redis_context_->OutageDeadline(
        now, absl::Milliseconds(RayConfig::instance().redis_reconnect_grace_period_ms()));
    if (now < deadline) {
      ++request_cxt->pending_retries_;
      refunded = true;
    }
  }
  if (refunded) {
    // During an outage every in-flight command comes through here once per
    // backoff step, and nothing is being spent yet: one line per second is
    // plenty.
    RAY_LOG_EVERY_MS(WARNING, 1000)
        << "Redis request [" << absl::StrJoin(request_cxt->redis_cmds_, " ")
        << "] failed (" << error_msg
        << "). Holding its retries while the reconnect is in flight.";
  } else {
    RAY_LOG(ERROR) << "Redis request [" << absl::StrJoin(request_cxt->redis_cmds_, " ")
                   << "] failed due to error " << error_msg << ". "
                   << request_cxt->pending_retries_ << " retries left.";
  }
  auto delay = request_cxt->exp_back_off_.Current();
  request_cxt->exp_back_off_.Next();
  // Retry the request after a while.
  execute_after(
      request_cxt->io_service_,
      [request_cxt]() {
        // The context may have been destroyed while this retry waited.
        if (request_cxt->context_alive_.expired()) {
          delete request_cxt;
          return;
        }
        request_cxt->Run();
      },
      std::chrono::milliseconds(delay));
}

void RedisRequestContext::RedisResponseFn(redisAsyncContext *async_context,
                                          void *raw_reply,
                                          void *privdata) {
  auto *request_cxt = static_cast<RedisRequestContext *>(privdata);
  auto redis_reply = reinterpret_cast<redisReply *>(raw_reply);
  // Error happened.
  if (redis_reply == nullptr || redis_reply->type == REDIS_REPLY_ERROR) {
    std::string error_msg =
        redis_reply != nullptr     ? std::string(redis_reply->str, redis_reply->len)
        : async_context != nullptr ? std::string(async_context->errstr)
                                   : std::string("Redis connection unavailable");
    // -NOAUTH is not about the command: it arrives for commands queued behind a
    // reconnect AUTH that the server rejected, on a connection that rejection
    // is already tearing down. The command never ran, so count it like a reply
    // that never came.
    const bool reached_redis =
        redis_reply != nullptr && !absl::StartsWith(error_msg, "NOAUTH");
    HandleFailure(request_cxt, reached_redis, error_msg);
  } else {
    // Count the write as acknowledged before its callback can act on it, so a
    // reconnect check from here on requires Redis to still have it.
    if (request_cxt->write_number_ != 0) {
      request_cxt->redis_context_->AckWrite(request_cxt->write_number_);
    }
    // Measure while hiredis still owns the reply, and before anything is posted
    // to the io_service: `request_cxt` is deleted at the end of this branch, so
    // every read of it has to happen above the post.
    if (request_cxt->metrics_ != nullptr) {
      request_cxt->metrics_->response_payload_bytes_sum.Record(
          static_cast<double>(ResponsePayloadBytes(*redis_reply)),
          {{"Command", request_cxt->command_label_},
           {"TableName", request_cxt->table_label_}});
    }
    auto reply = std::make_shared<CallbackReply>(*redis_reply);
    request_cxt->io_service_.post(
        [reply, callback = std::move(request_cxt->callback_)]() {
          if (callback) {
            callback(std::move(reply));
          }
        },
        "RedisRequestContext.Callback");
    auto end_time = request_cxt->clock_.Now();
    request_cxt->ray_metric_gcs_latency_.Record(
        absl::ToDoubleMilliseconds(end_time - request_cxt->start_time_));
    delete request_cxt;
  }
}

void RedisRequestContext::Run() {
  if (pending_retries_ == 0) {
    RAY_LOG(FATAL) << "Failed to run redis cmds: [" << absl::StrJoin(redis_cmds_, " ")
                   << "] for " << RayConfig::instance().num_redis_request_retries()
                   << " times.";
  }

  --pending_retries_;

  struct AcceptedRequestMetrics {
    RedisMetrics metrics;
    std::string command_label;
    std::string table_label;
    size_t request_payload_bytes;
  };
  std::optional<AcceptedRequestMetrics> accepted;
  auto capture_acceptance = [&]() {
    if (request_metrics_claimed_) {
      return;
    }
    // The submission lock keeps this request alive while copying its labels.
    // Claim the records now: a retry may run before the records finish below.
    accepted.emplace(AcceptedRequestMetrics{
        *metrics_, command_label_, table_label_, request_payload_bytes_});
    request_metrics_claimed_ = true;
  };
  // The previous attempt, if any, failed without a reply, so nothing else reads
  // this until the submission below sets it under the lock.
  write_number_ = 0;
  Status status = redis_context_->RedisAsyncCommandArgv(
      RedisResponseFn,
      this,
      argv_.size(),
      argv_.data(),
      argc_.data(),
      metrics_ != nullptr
          ? std::make_optional(absl::FunctionRef<void()>(capture_acceptance))
          : std::nullopt,
      is_write_ ? &write_number_ : nullptr);
  if (status.ok()) {
    // The submission lock has been released, so a reply on the IO thread may
    // already have deleted this request. Only use the independent local snapshot,
    // and do not hold the Redis mutex while acquiring metric recorder locks.
    if (accepted.has_value()) {
      const std::vector<std::pair<std::string_view, std::string>> tags{
          {"Command", accepted->command_label}, {"TableName", accepted->table_label}};
      accepted->metrics.request_payload_bytes_sum.Record(
          static_cast<double>(accepted->request_payload_bytes), tags);
      accepted->metrics.command_count_counter.Record(1, tags);
    }
    return;
  }

  // A rejected submission has no hiredis callback. Use the owned Status for
  // diagnostics; the raw context may have been freed since submission returned.
  // HandleFailure logs it, rate-limited while the outage refunds retries: an
  // unconditional line here would fire once per retry of every in-flight
  // command for the whole grace period.
  HandleFailure(this, /*reached_redis=*/false, status.message());
}

#define REDIS_CHECK_ERROR(CONTEXT, REPLY)       \
  if (REPLY == nullptr) {                       \
    return Status::RedisError(CONTEXT->errstr); \
  }                                             \
  if (REPLY->type == REDIS_REPLY_ERROR) {       \
    return Status::RedisError(REPLY->str);      \
  }

RedisContext::RedisContext(instrumented_io_context &io_service,
                           ClockInterface &clock,
                           std::optional<RedisMetrics> metrics)
    : io_service_(io_service),
      clock_(clock),
      metrics_(std::move(metrics)),
      context_(nullptr),
      ssl_context_(nullptr),
      redis_db_probe_timeout_milliseconds_(
          RayConfig::instance().redis_db_probe_timeout_milliseconds()),
      reconnect_backoff_(RayConfig::instance().redis_retry_base_ms(),
                         RayConfig::instance().redis_retry_multiplier(),
                         RayConfig::instance().redis_retry_max_ms()),
      resolver_(io_service) {
  redisSSLContextError ssl_error;
  redisInitOpenSSL();

  const char *cacert = nullptr;
  if (!::RayConfig::instance().REDIS_CA_CERT().empty()) {
    cacert = ::RayConfig::instance().REDIS_CA_CERT().c_str();
  }

  const char *capath = nullptr;
  if (!::RayConfig::instance().REDIS_CA_PATH().empty()) {
    capath = ::RayConfig::instance().REDIS_CA_PATH().c_str();
  }

  const char *client_cert = nullptr;
  if (!::RayConfig::instance().REDIS_CLIENT_CERT().empty()) {
    client_cert = ::RayConfig::instance().REDIS_CLIENT_CERT().c_str();
  }

  const char *client_key = nullptr;
  if (!::RayConfig::instance().REDIS_CLIENT_KEY().empty()) {
    client_key = ::RayConfig::instance().REDIS_CLIENT_KEY().c_str();
  }

  const char *server_name = nullptr;
  if (!::RayConfig::instance().REDIS_SERVER_NAME().empty()) {
    server_name = ::RayConfig::instance().REDIS_SERVER_NAME().c_str();
  }

  ssl_error = REDIS_SSL_CTX_NONE;
  ssl_context_ = redisCreateSSLContext(
      cacert, capath, client_cert, client_key, server_name, &ssl_error);

  RAY_CHECK(ssl_context_ != nullptr && ssl_error == REDIS_SSL_CTX_NONE)
      << "Failed to construct a ssl context for redis client: "
      << redisSSLContextGetError(ssl_error);
}

RedisContext::~RedisContext() {
  // Expire the sentinel before tearing anything down, as ~RedisAsyncContext
  // does: Disconnect() below re-enters hiredis teardown, and posted reconnect
  // work or a flushed reconnect AUTH callback must see this object as gone.
  alive_.reset();
  Disconnect();
  if (ssl_context_) {
    redisFreeSSLContext(ssl_context_);
    ssl_context_ = nullptr;
  }
}

void RedisContext::Disconnect() {
  // Stop any in-flight reconnect and keep the disconnect callback below from
  // starting a new one: resetting the async context runs hiredis' teardown,
  // which calls back into us, and this may be ~RedisContext.
  disconnect_requested_ = true;
  dropped_while_connecting_ = false;
  reconnecting_ = false;
  reconnect_pending_ = false;
  resolver_.cancel();
  context_.reset();
  redis_async_context_.reset();
  // Drop the retained connection parameters too. If a caller connects this
  // object somewhere else next, a later reconnect must not dial the previous
  // server with the previous credentials because `Connect()` saw a leftover
  // address and skipped recording the new one.
  address_.clear();
  port_ = 0;
  username_.clear();
  password_.clear();
  enable_ssl_ = false;
  resolved_ip_.clear();
  origin_address_.clear();
  origin_port_ = 0;
  via_sentinel_ = false;
}

Status SetRedisProbeTimeout(redisContext *context, int64_t timeout_ms) {
  RAY_CHECK_GT(timeout_ms, 0) << "redis_db_probe_timeout_milliseconds must be positive.";
  struct timeval timeout;
  timeout.tv_sec = timeout_ms / 1000;
  timeout.tv_usec = (timeout_ms % 1000) * 1000;
  if (::redisSetTimeout(context, timeout) != REDIS_OK) {
    return Status::RedisError(std::string("Failed to set Redis probe timeout: ") +
                              context->errstr);
  }
  return Status::OK();
}

Status RestoreRedisTimeout(redisContext *context) {
  struct timeval no_timeout;
  no_timeout.tv_sec = 0;
  no_timeout.tv_usec = 0;
  if (::redisSetTimeout(context, no_timeout) != REDIS_OK) {
    return Status::RedisError(std::string("Failed to restore Redis timeout: ") +
                              context->errstr);
  }
  return Status::OK();
}

namespace {

// hiredis' async context embeds the sync context; the socket lives in c.fd.
redisContext *AsRedisContext(redisContext *context) { return context; }
redisContext *AsRedisContext(redisAsyncContext *context) { return &context->c; }

// Linux caps both TCP_KEEPIDLE and TCP_KEEPINTVL at 32767 seconds.
constexpr int64_t kMaxRedisTcpKeepaliveIntervalSeconds = 32767;
// Linux caps TCP_KEEPCNT at 127.
constexpr int64_t kMaxRedisTcpKeepaliveProbes = 127;

// Validates the keepalive configuration on its own, so a bad value fails before
// any connection attempt instead of surfacing as a Redis connectivity error
// after the connect-retry budget is spent.
Status ValidateRedisTcpKeepaliveConfig() {
  const int64_t interval = RayConfig::instance().redis_tcp_keepalive_interval_seconds();
  if (interval < 0 || interval > kMaxRedisTcpKeepaliveIntervalSeconds) {
    return Status::InvalidArgument(
        absl::StrCat("redis_tcp_keepalive_interval_seconds must be in [0, ",
                     kMaxRedisTcpKeepaliveIntervalSeconds,
                     "], got ",
                     interval,
                     ". Set RAY_redis_tcp_keepalive_interval_seconds=0 to disable "
                     "TCP keepalive."));
  }
  const int64_t probes = RayConfig::instance().redis_tcp_keepalive_probes();
  if (interval > 0 && (probes < 1 || probes > kMaxRedisTcpKeepaliveProbes)) {
    return Status::InvalidArgument(
        absl::StrCat("redis_tcp_keepalive_probes must be in [1, ",
                     kMaxRedisTcpKeepaliveProbes,
                     "] when TCP keepalive is enabled, got ",
                     probes,
                     "."));
  }
  return Status::OK();
}

std::string DescribeRedisTcpKeepalivePolicy(int64_t interval, int64_t probes) {
  if (interval == 0) {
    return "disabled by configuration";
  }
  static_cast<void>(probes);
#if defined(__linux__) && defined(TCP_KEEPIDLE) && defined(TCP_KEEPINTVL) && \
    defined(TCP_KEEPCNT)
  return absl::StrCat("idle=",
                      interval,
                      "s, probe interval=",
                      interval,
                      "s, probes=",
                      probes,
                      " (idle, unresponsive connection detected after ~",
                      interval * (1 + probes),
                      "s)");
#elif defined(__APPLE__) && defined(__MACH__)
  return absl::StrCat(
      "idle=", interval, "s; probe interval and count are controlled by the OS");
#elif defined(_WIN32)
  return absl::StrCat("idle=",
                      interval,
                      "s, probe interval=",
                      interval,
                      "s; probe count is controlled by the OS");
#else
  return "SO_KEEPALIVE enabled; idle time, probe interval, and probe count are "
         "controlled by the OS";
#endif
}

#ifdef _WIN32
/// Waits for the given context to finish connecting before returning.
///
/// Arguments:
///   \param context - The connection context
///   \param address - The address the context is trying to make a connection to
///   \param port - The port the context is trying to connect to
///
/// A connect that fails or doesn't finish in time is a connectivity error, so it
/// is retryable like any other.
Status WaitForRedisConnect(const redisContext *context,
                           const std::string &address,
                           int port) {
  const auto redis_socket = static_cast<SOCKET>(context->fd);
  fd_set writable;
  fd_set failed;
  FD_ZERO(&writable);
  FD_ZERO(&failed);
  FD_SET(redis_socket, &writable);
  FD_SET(redis_socket, &failed);
  timeval timeout{static_cast<long>(kRedisConnectWaitTimeoutSeconds), 0};
  const int ready = ::select(/*nfds=*/0, nullptr, &writable, &failed, &timeout);
  if (ready == SOCKET_ERROR) {
    return Status::RedisError(
        absl::StrCat("Failed to wait for the async Redis connection to ",
                     BuildAddress(address, port),
                     " (WSA error=",
                     ::WSAGetLastError(),
                     ")."));
  }
  if (ready == 0) {
    return Status::RedisError(absl::StrCat("The async Redis connection to ",
                                           BuildAddress(address, port),
                                           " did not finish connecting within ",
                                           kRedisConnectWaitTimeoutSeconds,
                                           "s."));
  }
  if (FD_ISSET(redis_socket, &failed)) {
    return Status::RedisError(absl::StrCat(
        "The async Redis connection to ", BuildAddress(address, port), " failed."));
  }
  return Status::OK();
}
#endif

// Applies the configured TCP keepalive policy to a freshly created hiredis
// context. Called from exactly one place (ConnectWithoutRetries) so that every
// external Redis socket - sync, async, Sentinel, Cluster-redirect, and cleanup
// connections - receives the same policy before TLS, AUTH, or any command.
//
// Validation and failures to enable keepalive are non-retryable. On POSIX,
// tuning failures leave keepalive enabled and warn without rejecting a usable
// connection. Windows retains hiredis' combined enable/tune operation.
Status ConfigureRedisTcpKeepalive(redisContext *context,
                                  const std::string &address,
                                  int port) {
  RAY_RETURN_NOT_OK(ValidateRedisTcpKeepaliveConfig());
  const int64_t interval = RayConfig::instance().redis_tcp_keepalive_interval_seconds();
  if (interval == 0) {
    RAY_LOG(INFO) << "TCP keepalive is disabled by configuration for the Redis "
                     "connection to "
                  << BuildAddress(address, port) << ".";
    return Status::OK();
  }
#ifdef _WIN32
  // Note: redisAsyncConnect starts a non-blocking connect and returns before it
  // finishes. Winsock doesn't support setting socket options while a connect is
  // in progress (see the Winsock connect() docs), and enabling keepalive on such
  // a socket without waiting for it to complete may fail if the setting is written
  // before the connection completes.
  if (!(context->flags & REDIS_BLOCK)) {
    RAY_RETURN_NOT_OK(WaitForRedisConnect(context, address, port));
  }
  if (::redisEnableKeepAliveWithInterval(context, static_cast<int>(interval)) !=
      REDIS_OK) {
    return Status::IOError(
        absl::StrCat("Failed to enable TCP keepalive on the Redis connection to ",
                     BuildAddress(address, port),
                     ": ",
                     context->errstr,
                     ". Set RAY_redis_tcp_keepalive_interval_seconds=0 to disable TCP "
                     "keepalive if this platform cannot support it."));
  }

#else
  // hiredis also tunes the timing, and marks context->err on any failure.
  // Apply options directly so a rejected timing option cannot poison an
  // otherwise usable hiredis context. This also applies Linux timing on musl.
  Status enable_status = Status::OK();
  const auto result = internal::SetRedisTcpKeepalive(
      static_cast<int>(interval),
      static_cast<int>(RayConfig::instance().redis_tcp_keepalive_probes()),
      [&](int level, int option, const char *name, int value) {
        if (::setsockopt(context->fd, level, option, &value, sizeof(value)) == 0) {
          return true;
        }
        const int saved_errno = errno;
        const auto error = absl::StrCat("Failed to set ",
                                        name,
                                        "=",
                                        value,
                                        " on the Redis connection to ",
                                        BuildAddress(address, port),
                                        " (errno=",
                                        saved_errno,
                                        "): ",
                                        std::strerror(saved_errno));
        if (level == SOL_SOCKET && option == SO_KEEPALIVE) {
          enable_status = Status::IOError(absl::StrCat(
              error,
              ". Set RAY_redis_tcp_keepalive_interval_seconds=0 to disable TCP "
              "keepalive if this platform cannot support it."));
        } else {
          RAY_LOG(WARNING) << error
                           << ". TCP keepalive remains enabled; this option retains "
                              "its previous value. The requested detection window "
                              "may not apply. Continuing with the connection.";
        }
        return false;
      });
  RAY_RETURN_NOT_OK(enable_status);
  if (result == internal::RedisTcpKeepaliveResult::kDegraded) {
    return Status::OK();
  }
#endif
  RAY_LOG(INFO) << "Enabled TCP keepalive on the Redis connection to "
                << BuildAddress(address, port) << ": "
                << DescribeRedisTcpKeepalivePolicy(
                       interval, RayConfig::instance().redis_tcp_keepalive_probes())
                << ".";
  return Status::OK();
}

}  // namespace

Status AuthenticateRedis(redisContext *context,
                         const std::string &username,
                         const std::string &password,
                         int64_t redis_db_probe_timeout_milliseconds) {
  if (password == "") {
    RAY_CHECK(username.empty());
    return Status::OK();
  }
  RAY_RETURN_NOT_OK(SetRedisProbeTimeout(context, redis_db_probe_timeout_milliseconds));

  redisReply *reply;
  if (username.empty()) {
    reply = reinterpret_cast<redisReply *>(
        redisCommand(context, "AUTH %s", password.c_str()));
  } else {
    reply = reinterpret_cast<redisReply *>(
        redisCommand(context, "AUTH %s %s", username.c_str(), password.c_str()));
  }

  if (reply == nullptr) {
    if (context->err == REDIS_ERR_IO && (errno == EAGAIN || errno == EWOULDBLOCK)) {
      return Status::RedisError("Timed out waiting for Redis auth reply");
    }
  }
  REDIS_CHECK_ERROR(context, reply);

  freeReplyObject(reply);
  RAY_RETURN_NOT_OK(RestoreRedisTimeout(context));
  return Status::OK();
}

Status AuthenticateRedis(redisAsyncContext *context,
                         const std::string &username,
                         const std::string &password) {
  if (password == "") {
    RAY_CHECK(username.empty());
    return Status::OK();
  }
  int status;
  if (username.empty()) {
    status = redisAsyncCommand(context, NULL, NULL, "AUTH %s", password.c_str());
  } else {
    status = redisAsyncCommand(
        context, NULL, NULL, "AUTH %s %s", username.c_str(), password.c_str());
  }
  if (status == REDIS_ERR) {
    return Status::RedisError(std::string(context->errstr));
  }
  return Status::OK();
}

void RedisAsyncContextDisconnectCallback(const redisAsyncContext *context, int status) {
  if (status == REDIS_OK) {
    // Deliberate disconnect (Disconnect()/teardown); keep quiet on shutdown.
    RAY_LOG(DEBUG) << "Redis async context disconnected. Status: " << status;
  } else {
    // Copy the error fields before hiredis frees the context below.
    // errno is not meaningful at this point (the failure happened
    // earlier, inside hiredis); errstr already carries the strerror(errno)
    // captured at the failure point.
    const int err = context->c.err;
    const std::string errstr =
        context->c.errstr[0] != '\0' ? context->c.errstr : "unknown error";
    RAY_LOG(WARNING) << "Redis async connection was closed unexpectedly (err = " << err
                     << ", " << errstr
                     << "). Current TCP keepalive configuration: interval="
                     << RayConfig::instance().redis_tcp_keepalive_interval_seconds()
                     << "s, probes=" << RayConfig::instance().redis_tcp_keepalive_probes()
                     << ". Applied socket settings may differ; see connection setup "
                        "logs.";
  }
  auto *async_context = reinterpret_cast<RedisAsyncContext *>(context->data);
  // Reset raw 'redisAsyncContext' to nullptr because hiredis will release this context.
  async_context->ResetRawRedisAsyncContext();
  // Let the owning RedisContext schedule a reconnect. The handler only posts
  // work: we are still inside hiredis' teardown here.
  async_context->NotifyDisconnected();
}

void RedisAsyncContextConnectCallback(const redisAsyncContext *context, int status) {
  auto *async_context = reinterpret_cast<RedisAsyncContext *>(context->data);
  if (status == REDIS_OK) {
    async_context->NotifyConnected();
    return;
  }
  // hiredis frees the context after a failed connect and does not run the
  // disconnect callback for it: __redisAsyncFree only runs that one once
  // REDIS_CONNECTED was set. Release here, or the next command dereferences
  // freed memory.
  RAY_LOG(WARNING) << "Redis async connect failed: " << context->errstr;
  async_context->ResetRawRedisAsyncContext();
  async_context->NotifyDisconnected();
}

void SetConnectionCallbacks(redisAsyncContext *raw_redis_async_context,
                            RedisAsyncContext *redis_async_context) {
  raw_redis_async_context->data = redis_async_context;
  redisAsyncSetConnectCallback(raw_redis_async_context, RedisAsyncContextConnectCallback);
  redisAsyncSetDisconnectCallback(raw_redis_async_context,
                                  RedisAsyncContextDisconnectCallback);
}

template <typename RedisContextType, typename RedisConnectFunctionType>
std::pair<Status, std::unique_ptr<RedisContextType, RedisContextDeleter>>
ConnectWithoutRetries(const std::string &address,
                      int port,
                      const RedisConnectFunctionType &connect_function) {
  // This currently returns the errorMessage in two different ways,
  // as an output parameter and in the Status::RedisError,
  // because we're not sure whether we'll want to change what this returns.
  RedisContextType *newContext = connect_function(address.c_str(), port);
  // Own the context right away so every failure path below frees it.
  std::unique_ptr<RedisContextType, RedisContextDeleter> context_holder(
      newContext, RedisContextDeleter());
  if (newContext == nullptr || (newContext)->err) {
    std::ostringstream oss;
    if (newContext == nullptr) {
      oss << "Could not allocate Redis context.";
    } else if (newContext->err) {
      oss << "Could not establish connection to Redis " << BuildAddress(address, port)
          << " (context.err = " << newContext->err << ").";
    }
    return std::make_pair(Status::RedisError(oss.str()), nullptr);
  }
  // Apply the TCP keepalive policy before the context is used for anything
  // (TLS, AUTH, or the first command). For the async context the TCP handshake
  // may not have completed yet. POSIX accepts the options on a connecting
  // socket; on Windows, ConfigureRedisTcpKeepalive waits for the connect first.
  Status status = ConfigureRedisTcpKeepalive(AsRedisContext(newContext), address, port);
  if (!status.ok()) {
    return std::make_pair(status, nullptr);
  }
  return std::make_pair(Status::OK(), std::move(context_holder));
}

template <typename RedisContextType, typename RedisConnectFunctionType>
std::pair<Status, std::unique_ptr<RedisContextType, RedisContextDeleter>>
ConnectWithRetries(const std::string &address,
                   int port,
                   const RedisConnectFunctionType &connect_function) {
  RAY_LOG(INFO) << "Attempting to connect to address " << BuildAddress(address, port)
                << ".";
  int connection_attempts = 0;
  auto resp = ConnectWithoutRetries<RedisContextType>(address, port, connect_function);
  auto status = resp.first;
  while (!status.ok()) {
    if (!status.IsRedisError()) {
      // Policy/configuration failures (e.g. TCP keepalive setup) are not
      // transient; retrying cannot fix them. Surface them immediately instead
      // of retrying for ~60s and then reporting Redis as unreachable.
      RAY_LOG(ERROR) << "Giving up connecting to Redis due to a non-retryable error: "
                     << status.ToString();
      break;
    }
    if (connection_attempts >= RayConfig::instance().redis_db_connect_retries()) {
      // Do not crash here. Return the failed status so callers (e.g. an
      // in-place reconnect) can decide how to recover. Existing callers still
      // RAY_CHECK_OK the result, so startup behavior is unchanged.
      RAY_LOG(ERROR) << RayConfig::instance().redis_db_connect_retries() << " attempts "
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
    status = resp.first;
    connection_attempts += 1;
  }
  return resp;
}

namespace {
std::optional<std::pair<std::string, int>> ParseIffMovedError(
    const std::string &error_msg) {
  std::vector<std::string> parts = absl::StrSplit(error_msg, " ");
  if (parts[0] != "MOVED") {
    return std::nullopt;
  }
  if (parts.size() != 3u) {
    return std::nullopt;
  }
  auto ip_port = ParseAddress(parts[2]);
  if (!ip_port.has_value()) {
    return std::nullopt;
  }
  return std::make_pair((*ip_port)[0], std::stoi((*ip_port)[1]));
}
}  // namespace

Status RedisContext::ValidateRedisDB() {
  auto reply = RunArgvSync(std::vector<std::string>{"INFO", "CLUSTER"});
  // cluster_state:ok
  // cluster_slots_assigned:16384
  // cluster_slots_ok:16384
  // cluster_slots_pfail:0
  // cluster_size:1
  if (!reply || reply->IsNil() || reply->IsError()) {
    return Status::RedisError("Failed to get Redis cluster info");
  }
  auto cluster_info = reply->ReadAsString();

  std::vector<std::string> parts = absl::StrSplit(cluster_info, "\r\n");
  bool cluster_mode = false;
  int cluster_size = 0;

  // Check the cluster status first
  for (const auto &part : parts) {
    if (part.empty() || part[0] == '#') {
      // it's a comment
      continue;
    }
    std::vector<std::string> kv = absl::StrSplit(part, ":");
    if (kv.size() != 2) {
      return Status::RedisError(
          absl::StrCat("Malformed INFO CLUSTER entry from Redis: ", part));
    }
    if (kv[0] == "cluster_state") {
      if (kv[1] == "ok") {
        cluster_mode = true;
      } else if (kv[1] == "fail") {
        return Status::RedisError(absl::StrCat(
            "The Redis cluster is not healthy. cluster_state shows failed status: ",
            cluster_info,
            ". Please check Redis cluster used."));
      }
    }
    if (kv[0] == "cluster_size") {
      cluster_size = std::stoi(kv[1]);
    }
  }

  if (cluster_mode && cluster_size != 1) {
    return Status::RedisError(
        "Ray currently doesn't support Redis Cluster with more than one shard.");
  }
  return Status::OK();
}

StatusOr<bool> RedisContext::IsRedisSentinel() {
  // Apply the probe timeout for the INFO SENTINEL command,
  // which is the first command we send to the Redis server if auth is not required.
  // (AUTH command is applied with the timeout as well.)
  //
  // This timeout is specifically to avoid blocking the GCS startup indefinitely
  // in the case where some Redis providers mischievously don't drop the connection
  // when they expect a TLS handshake instead of a normal command.
  // See the misbehaved tcpdump in https://github.com/ray-project/ray/pull/63148.
  //
  // Once we confirmed the Redis server behaves normally, we clear the timeout for
  // not affecting normal operations.
  RAY_RETURN_NOT_OK(
      SetRedisProbeTimeout(sync_context(), redis_db_probe_timeout_milliseconds_));

  auto reply = RunArgvSync(std::vector<std::string>{"INFO", "SENTINEL"});

  if (reply == nullptr) {
    return Status::RedisError(
        absl::StrCat("Failed to get Redis info. The command may have timed out after ",
                     redis_db_probe_timeout_milliseconds_,
                     "ms or failed due to a Redis connection error."));
  }

  RAY_RETURN_NOT_OK(RestoreRedisTimeout(sync_context()));

  if (reply->IsNil() || reply->IsError() || reply->ReadAsString().length() == 0) {
    return false;
  } else {
    return true;
  }
}

Status RedisContext::ConnectRedisCluster(const std::string &username,
                                         const std::string &password,
                                         bool enable_ssl,
                                         const std::string &redis_address) {
  RAY_LOG(INFO) << "Connect to Redis Cluster";
  // Ray has some restrictions for RedisDB. Validate it here.
  RAY_RETURN_NOT_OK(ValidateRedisDB());

  // Find the true leader
  std::vector<const char *> argv;
  std::vector<size_t> argc;
  std::vector<std::string> cmds = {"DEL", "DUMMY"};
  for (const auto &arg : cmds) {
    argv.push_back(arg.data());
    argc.push_back(arg.size());
  }

  auto redis_reply = reinterpret_cast<redisReply *>(
      ::redisCommandArgv(sync_context(), cmds.size(), argv.data(), argc.data()));

  if (redis_reply == nullptr) {
    // A null reply means the command could not be sent (e.g. the connection
    // dropped during cluster setup). Return instead of dereferencing it.
    return Status::RedisError(absl::StrCat(
        "Failed to run DEL DUMMY during Redis cluster setup: ", sync_context()->errstr));
  }
  if (redis_reply->type == REDIS_REPLY_ERROR) {
    // This should be a MOVED error
    // MOVED 14946 10.xx.xx.xx:7001
    std::string error_msg(redis_reply->str, redis_reply->len);
    freeReplyObject(redis_reply);
    auto maybe_ip_port = ParseIffMovedError(error_msg);
    if (!maybe_ip_port.has_value()) {
      Disconnect();
      return Status::RedisError(
          absl::StrCat("Setup Redis cluster failed in the dummy deletion: ", error_msg));
    }
    Disconnect();
    const auto &[ip, port] = maybe_ip_port.value();
    // Connect to the true leader.
    RAY_LOG(INFO) << "Redis cluster leader is " << BuildAddress(ip, port)
                  << ". Reconnect to it.";
    return Connect(ip, port, username, password, enable_ssl);
  } else {
    RAY_LOG(INFO) << "Redis cluster leader is " << redis_address;
    freeReplyObject(redis_reply);
  }

  return Status::OK();
}

/// Ask a Sentinel which node is currently the primary.
///
/// Split out of ConnectRedisSentinel so the reconnect path can re-resolve:
/// after a failover the primary sits at a different address, and a client that
/// keeps dialling the one it learned at startup never recovers.
Status QuerySentinelForPrimary(redisContext *sentinel_context,
                               std::string *out_ip,
                               int *out_port) {
  std::vector<const char *> argv;
  std::vector<size_t> argc;
  std::vector<std::string> cmds = {"SENTINEL", "MASTERS"};
  for (const auto &arg : cmds) {
    argv.push_back(arg.data());
    argc.push_back(arg.size());
  }

  // use raw redis context since we need to parse a complex reply.
  // sample reply (array of arrays):
  // 1)  1) "name"
  //     2) "redis-ha"
  //     3) "ip"
  //     4) "10.112.202.115"
  //     5) "port"
  //     6) "6379"
  //     7) "runid"
  //     8) "18a76cedbf445bd25bbd412c92e237137b5c7d4d"
  auto redis_reply = reinterpret_cast<redisReply *>(
      ::redisCommandArgv(sentinel_context, cmds.size(), argv.data(), argc.data()));

  if (redis_reply == nullptr) {
    return Status::RedisError("Failed to get redis sentinel masters info");
  }
  if (redis_reply->type != REDIS_REPLY_ARRAY) {
    const int reply_type = redis_reply->type;
    freeReplyObject(redis_reply);
    return Status::RedisError(absl::StrCat(
        "Redis sentinel master info should be REDIS_REPLY_ARRAY but got ", reply_type));
  }
  if (redis_reply->elements != 1UL) {
    freeReplyObject(redis_reply);
    return Status::RedisError(
        "There should be only one primary behind the Redis sentinel");
  }
  if (redis_reply->element[0] == nullptr ||
      redis_reply->element[0]->type != REDIS_REPLY_ARRAY ||
      redis_reply->element[0]->elements % 2 != 0) {
    freeReplyObject(redis_reply);
    return Status::RedisError("Malformed Redis sentinel master response");
  }
  auto primary = redis_reply->element[0];
  std::string actual_ip, actual_port;
  for (size_t i = 0; i < primary->elements; i += 2) {
    std::string key = primary->element[i]->str;        // Key (e.g., "name", "ip")
    std::string value = primary->element[i + 1]->str;  // Value corresponding to the key
    if ("ip" == key) {
      actual_ip = value;
    } else if ("port" == key) {
      actual_port = value;
    }
  }
  freeReplyObject(redis_reply);
  if (actual_ip.empty() || actual_port.empty()) {
    return Status::RedisError(
        "Failed to get the ip and port of the primary node from Redis sentinel");
  }
  *out_ip = actual_ip;
  *out_port = std::stoi(actual_port);
  return Status::OK();
}

Status ConnectRedisSentinel(RedisContext &context,
                            const std::string &username,
                            const std::string &password,
                            bool enable_ssl) {
  RAY_LOG(INFO) << "Connect to Redis sentinel";
  std::string actual_ip;
  int actual_port = 0;
  RAY_RETURN_NOT_OK(
      QuerySentinelForPrimary(context.sync_context(), &actual_ip, &actual_port));
  RAY_LOG(INFO) << "Connecting to the Redis primary node behind sentinel: "
                << BuildAddress(actual_ip, actual_port);
  context.Disconnect();
  return context.Connect(actual_ip, actual_port, username, password, enable_ssl);
}

std::vector<std::string> ResolveDNS(instrumented_io_context &io_service,
                                    const std::string &address,
                                    int port) {
  using namespace boost::asio;  // NOLINT
  ip::tcp::resolver resolver(io_service);
  ip::tcp::resolver::iterator iter = resolver.resolve(address, std::to_string(port));
  ip::tcp::resolver::iterator end;
  std::vector<std::string> ip_addresses;
  while (iter != end) {
    ip::tcp::endpoint endpoint = *iter++;
    ip_addresses.push_back(endpoint.address().to_string());
  }
  return ip_addresses;
}

Status RedisContext::Connect(const std::string &address,
                             int port,
                             const std::string &username,
                             const std::string &password,
                             bool enable_ssl) {
  ++connect_depth_;
  Status status = ConnectImpl(address, port, username, password, enable_ssl);
  // Leave before posting, so the posted handler sees connect_depth_ == 0.
  if (--connect_depth_ == 0 && dropped_while_connecting_.exchange(false) && status.ok()) {
    io_service_.post(
        [this, alive = std::weak_ptr<bool>(alive_)] {
          if (alive.expired()) {
            return;
          }
          OnAsyncDisconnected();
        },
        "RedisContext.DroppedWhileConnecting");
  }
  return status;
}

Status RedisContext::ConnectImpl(const std::string &address,
                                 int port,
                                 const std::string &username,
                                 const std::string &password,
                                 bool enable_ssl) {
  // Connect to the leader of the Redis cluster:
  //   1. Resolve the ip address from domain name.
  //      It might return multiple ip addresses
  //   2. Connect to the first ip address.
  //   3. Validate the Redis cluster to make sure it's configured in the way
  //      Ray accept:
  //        - If it's cluster mode redis, only 1 shard in the cluster.
  //        - Make sure the cluster is healthy.
  //   4. Send a dummy delete and check the return.
  //      - If return OK, connection is finished.
  //      - Otherwise, make sure it's MOVED error. And we'll get the leader
  //        address from the error message. Re-run this function with the
  //        right leader address.

  RAY_CHECK(!context_);
  RAY_CHECK(!redis_async_context_);
  // Reject a bad keepalive configuration before touching the network, so it
  // surfaces as itself rather than as a Redis connectivity error after DNS
  // resolution and the connect-retry budget.
  RAY_RETURN_NOT_OK(ValidateRedisTcpKeepaliveConfig());

  // A previous Disconnect() (including the one ConnectRedisSentinel does
  // before dialling the primary) parked the reconnect logic. Re-arm it.
  disconnect_requested_ = false;

  // Fetch the ip address from the address. It might return multiple
  // addresses and only the first one will be used.
  // ResolveDNS may throw (boost resolver) when the name cannot be resolved
  // (e.g. during a failover before the new primary's DNS has propagated), so
  // translate that into a Status instead of letting it abort the process.
  std::vector<std::string> ip_addresses;
  try {
    ip_addresses = ResolveDNS(io_service_, address, port);
  } catch (const std::exception &e) {
    return Status::RedisError(absl::StrCat(
        "Failed to resolve DNS for ", BuildAddress(address, port), ": ", e.what()));
  }
  if (ip_addresses.empty()) {
    return Status::RedisError(
        absl::StrCat("Failed to resolve DNS for ", BuildAddress(address, port)));
  }

  RAY_LOG(INFO) << "Resolve Redis address to " << absl::StrJoin(ip_addresses, ", ");

  {
    auto resp = ConnectWithRetries<redisContext>(ip_addresses[0], port, redisConnect);
    RAY_RETURN_NOT_OK(resp.first /* status */);
    context_ = std::move(resp.second /* redisContext */);
  }

  if (enable_ssl) {
    if (ssl_context_ == nullptr) {
      Disconnect();
      return Status::RedisError("SSL context is not initialized for encrypted Redis");
    }
    if (redisInitiateSSLWithContext(context_.get(), ssl_context_) != REDIS_OK) {
      Status status = Status::RedisError(
          absl::StrCat("Failed to setup encrypted redis: ", context_->errstr));
      Disconnect();
      return status;
    }
  }
  if (auto status = AuthenticateRedis(
          context_.get(), username, password, redis_db_probe_timeout_milliseconds_);
      !status.ok()) {
    Disconnect();
    return status;
  }

  // Connect to async context
  std::unique_ptr<redisAsyncContext, RedisContextDeleter> async_context;
  {
    auto resp =
        ConnectWithRetries<redisAsyncContext>(ip_addresses[0], port, redisAsyncConnect);
    if (!resp.first.ok()) {
      Disconnect();
      return resp.first;
    }
    async_context = std::move(resp.second);
  }
  if (enable_ssl) {
    if (ssl_context_ == nullptr) {
      Disconnect();
      return Status::RedisError("SSL context is not initialized for encrypted Redis");
    }
    if (redisInitiateSSLWithContext(&async_context->c, ssl_context_) != REDIS_OK) {
      Status status = Status::RedisError(
          absl::StrCat("Failed to setup encrypted redis: ", async_context->errstr));
      Disconnect();
      return status;
    }
  }
  if (auto status = AuthenticateRedis(async_context.get(), username, password);
      !status.ok()) {
    Disconnect();
    return status;
  }
  redis_async_context_.reset(
      new RedisAsyncContext(io_service_, std::move(async_context)));
  redis_async_context_->SetDisconnectHandler([this] { OnAsyncDisconnected(); });

  // handle validation and primary connection for different types of redis
  auto is_sentinel = IsRedisSentinel();
  if (!is_sentinel.ok()) {
    Disconnect();
    return is_sentinel.status();
  }
  Status status;
  if (is_sentinel.value()) {
    // The nested Connect() below records the primary's own address. Remember
    // that we got there through a Sentinel, and at which address, so a
    // reconnect can ask again instead of dialling a demoted node.
    status = ConnectRedisSentinel(*this, username, password, enable_ssl);
    if (status.ok()) {
      via_sentinel_ = true;
      origin_address_ = address;
      origin_port_ = port;
    }
  } else {
    status = ConnectRedisCluster(
        username, password, enable_ssl, BuildAddress(ip_addresses[0], port));
  }
  // Reset partial state on failure so a later attempt (e.g. an in-place
  // reconnect) starts from the clean precondition checked at the top.
  if (!status.ok()) {
    Disconnect();
    return status;
  }

  // Retain what a reconnect needs. ConnectRedisSentinel/ConnectRedisCluster may
  // have re-entered Connect() against the true primary, in which case those
  // nested calls already recorded the final address; don't overwrite it.
  if (address_.empty()) {
    address_ = address;
    port_ = port;
    username_ = username;
    password_ = password;
    enable_ssl_ = enable_ssl;
    resolved_ip_ = ip_addresses[0];
    // This context is the final one, so let hiredis drive it now and not
    // earlier: registering the connect callback arms its first write, which the
    // io_service picks up at once when it already runs on another thread, while
    // the Sentinel and Cluster discovery above may still destroy the context
    // from this one. A nested Connect() registered its own context instead.
    // The discovery commands above already run on the io_service, so take the
    // lock it reads the callbacks under.
    redis_async_context_->WithRawContextLocked([this](redisAsyncContext *raw) {
      SetConnectionCallbacks(raw, redis_async_context_.get());
    });
    if (async_connected_hook_for_testing_) {
      std::exchange(async_connected_hook_for_testing_, nullptr)(*redis_async_context_);
    }
  }
  return status;
}

void RedisContext::OnAsyncDisconnected() {
  if (disconnect_requested_) {
    // Torn down on purpose. Do not resurrect it, and do not touch io_service_:
    // this can run from ~RedisContext after the io_context is gone.
    return;
  }
  if (connect_depth_ > 0) {
    // Connect() is still running, possibly on another thread. It records the
    // address only at the end, so let it pass the drop back to us then.
    dropped_while_connecting_ = true;
    // Unless it finished between the check above and the store, and so no
    // longer looks at the flag. Whichever side clears the flag handles the drop.
    if (connect_depth_ > 0 || !dropped_while_connecting_.exchange(false)) {
      return;
    }
  }
  // Whatever connect was outstanding is settled now, one way or another.
  reconnect_pending_ = false;
  if (address_.empty()) {
    // Never finished a successful Connect(), so there is nothing to restore.
    return;
  }
  if (reconnecting_) {
    // A reconnect attempt's connect just failed. Retries are driven by
    // failure events like this one: schedule the next attempt here rather
    // than keeping a poll timer alive across the in-flight connect.
    ScheduleReconnectRetry();
    return;
  }
  reconnecting_ = true;
  reconnect_attempts_left_ = RayConfig::instance().redis_db_connect_retries();
  reconnect_backoff_.Reset();
  // Start the outage clock now, whether or not a command is in flight, so the
  // grace period runs from the drop rather than from whichever command
  // happens to fail first. A command flushed by hiredis before this callback
  // may have stamped it a moment earlier already; that stamp stands.
  if (redis_async_context_ != nullptr) {
    redis_async_context_->OutageDeadline(
        clock_.Now(),
        absl::Milliseconds(RayConfig::instance().redis_reconnect_grace_period_ms()));
  }
  RAY_LOG(WARNING) << "Redis connection to " << BuildAddress(address_, port_)
                   << " was lost. Attempting to reconnect in place.";
  // Do not reconnect synchronously: hiredis is still tearing the old context
  // down underneath us.
  io_service_.post(
      [this, alive = std::weak_ptr<bool>(alive_)] {
        if (alive.expired()) {
          return;
        }
        AttemptReconnect();
      },
      "RedisContext.Reconnect");
}

Status RedisContext::RefreshPrimaryFromSentinel(const std::string &sentinel_ip) {
  // A short-lived synchronous connection: the Sentinel is the one address that
  // survives a failover, and the stored sync context points at the old primary.
  // This runs on the io_service thread that also serves GCS storage, so every
  // step is bounded by a dedicated, short timeout rather than the 30s
  // redis_db_probe_timeout_milliseconds used at startup: a Sentinel that
  // accepts and then stalls must not park the io_context for long, once per
  // attempt, during the very failover the GCS is trying to ride out.
  const int64_t timeout_ms = RayConfig::instance().redis_reconnect_sentinel_timeout_ms();
  auto connect_with_timeout = [timeout_ms](const std::string &host, int port) {
    struct timeval timeout;
    timeout.tv_sec = timeout_ms / 1000;
    timeout.tv_usec = (timeout_ms % 1000) * 1000;
    return redisConnectWithTimeout(host.c_str(), port, timeout);
  };
  auto resp = ConnectWithoutRetries<redisContext>(
      sentinel_ip, origin_port_, connect_with_timeout);
  RAY_RETURN_NOT_OK(resp.first);
  auto sentinel_context = std::move(resp.second);
  if (enable_ssl_) {
    if (ssl_context_ == nullptr) {
      return Status::RedisError("SSL context is not initialized for encrypted Redis");
    }
    if (redisInitiateSSLWithContext(sentinel_context.get(), ssl_context_) != REDIS_OK) {
      return Status::RedisError(
          absl::StrCat("Failed to setup encrypted redis: ", sentinel_context->errstr));
    }
  }
  RAY_RETURN_NOT_OK(
      AuthenticateRedis(sentinel_context.get(), username_, password_, timeout_ms));

  // AuthenticateRedis restores the socket to no-timeout (and skips the setup
  // entirely without a password), so bound the query ourselves. The context is
  // discarded right after, so there is nothing to restore.
  RAY_RETURN_NOT_OK(SetRedisProbeTimeout(sentinel_context.get(), timeout_ms));
  std::string primary_ip;
  int primary_port = 0;
  RAY_RETURN_NOT_OK(
      QuerySentinelForPrimary(sentinel_context.get(), &primary_ip, &primary_port));
  // Record what Sentinel reports, which may be a host name when Sentinel runs
  // with announce-hostnames. The caller resolves it into resolved_ip_.
  if (primary_ip != address_ || primary_port != port_) {
    RAY_LOG(INFO) << "Redis Sentinel now reports the primary at "
                  << BuildAddress(primary_ip, primary_port) << ", was "
                  << BuildAddress(address_, port_) << ".";
    address_ = primary_ip;
    port_ = primary_port;
  }
  return Status::OK();
}

namespace {
/// privdata for ReconnectAuthCallback and ReconnectCheckCallback.
/// Heap-allocated because hiredis may hold it past the RedisContext's lifetime
/// (teardown flushes callbacks).
struct ReconnectProbe {
  RedisContext *context;
  std::weak_ptr<bool> alive;
  /// For the check: the highest write Redis had acknowledged when the check
  /// was queued, 0 when the check is a plain PING.
  uint64_t expected_write = 0;
  /// For the check: the prefix this process writes its markers with.
  std::string incarnation;
};
}  // namespace

Status RedisContext::ConnectToResolvedAddress() {
  auto resp =
      ConnectWithoutRetries<redisAsyncContext>(resolved_ip_, port_, redisAsyncConnect);
  RAY_RETURN_NOT_OK(resp.first);
  auto async_context = std::move(resp.second);

  if (enable_ssl_) {
    if (ssl_context_ == nullptr) {
      return Status::RedisError("SSL context is not initialized for encrypted Redis");
    }
    if (redisInitiateSSLWithContext(&async_context->c, ssl_context_) != REDIS_OK) {
      return Status::RedisError(
          absl::StrCat("Failed to setup encrypted redis: ", async_context->errstr));
    }
  }
  // Startup proves the credentials on a synchronous probe before the async
  // AUTH goes out; a reconnect has no such probe. Send AUTH with a reply
  // callback, so a promoted primary that rejects the credentials reads as a
  // failed attempt instead of a "Reconnected" log followed by -NOAUTH replies.
  if (!password_.empty()) {
    auto *probe = new ReconnectProbe{this, std::weak_ptr<bool>(alive_)};
    int auth_status = username_.empty()
                          ? redisAsyncCommand(async_context.get(),
                                              &RedisContext::ReconnectAuthCallback,
                                              probe,
                                              "AUTH %s",
                                              password_.c_str())
                          : redisAsyncCommand(async_context.get(),
                                              &RedisContext::ReconnectAuthCallback,
                                              probe,
                                              "AUTH %s %s",
                                              username_.c_str(),
                                              password_.c_str());
    if (auth_status == REDIS_ERR) {
      delete probe;
      return Status::RedisError(std::string(async_context->errstr));
    }
  }

  // Then the check, whose reply ends the attempt. A completed TCP connect
  // proves little: a proxy in front of a Redis that is down accepts the
  // connection and closes it right away. Counting that as success reset the
  // backoff and turned the outage into a tight loop of connects. Once Redis
  // has acknowledged a write, the check reads the write watermark back, so a
  // Redis that lost acknowledged writes is caught before GCS runs on.
  auto *check = new ReconnectProbe{this, std::weak_ptr<bool>(alive_)};
  int check_status = REDIS_ERR;
  auto watermark = redis_async_context_->GetWriteWatermark();
  const uint64_t expected_write = redis_async_context_->LastAckedWrite();
  if (watermark.has_value() && expected_write > 0) {
    check->expected_write = expected_write;
    check->incarnation = watermark->incarnation;
    check_status = redisAsyncCommand(async_context.get(),
                                     &RedisContext::ReconnectCheckCallback,
                                     check,
                                     "HGET %b %b",
                                     watermark->key.data(),
                                     watermark->key.size(),
                                     watermark->field.data(),
                                     watermark->field.size());
  } else {
    check_status = redisAsyncCommand(
        async_context.get(), &RedisContext::ReconnectCheckCallback, check, "PING");
  }
  if (check_status == REDIS_ERR) {
    delete check;
    return Status::RedisError(std::string(async_context->errstr));
  }

  // Finish every mutation of the raw context before Reset() publishes it:
  // from then on other threads may submit commands under the wrapper's lock,
  // and hiredis is not thread-safe. Nothing below can fail, so the callbacks
  // never outlive a context we abandon. Registering the connect callback asks
  // hiredis to arm a write before our event hooks exist; Reset() re-arms it.
  SetConnectionCallbacks(async_context.get(), redis_async_context_.get());

  // Rebind rather than recreate: in-flight RedisRequestContexts hold a raw
  // pointer to this RedisAsyncContext. Keep other commands out until the check
  // passes: on a Redis that lost data, a retried write would land on top of
  // the gap, and the restarted GCS would load it as if nothing were missing.
  redis_async_context_->PauseCommands(true);
  redis_async_context_->Reset(std::move(async_context));
  return Status::OK();
}

void RedisContext::ReconnectAuthCallback(redisAsyncContext *async_context,
                                         void *raw_reply,
                                         void *privdata) {
  std::unique_ptr<ReconnectProbe> probe(static_cast<ReconnectProbe *>(privdata));
  if (probe->alive.expired()) {
    // Flushed during teardown; the RedisContext is already gone.
    return;
  }
  auto *reply = static_cast<redisReply *>(raw_reply);
  // No reply means the connection died first, and the disconnect callback
  // drives the retry. Success needs no action: the check queued behind AUTH
  // decides the attempt.
  if (reply == nullptr || reply->type != REDIS_REPLY_ERROR) {
    return;
  }
  RAY_LOG(WARNING) << "Redis rejected AUTH after a reconnect: " << reply->str
                   << ". Tearing this connection down and retrying.";
  // Runs from inside a hiredis callback, where redisAsyncDisconnect is the
  // sanctioned teardown: it completes after the callback returns and then
  // invokes the disconnect callback, which schedules the next attempt.
  redisAsyncDisconnect(async_context);
}

void RedisContext::ReconnectCheckCallback(redisAsyncContext *async_context,
                                          void *raw_reply,
                                          void *privdata) {
  std::unique_ptr<ReconnectProbe> probe(static_cast<ReconnectProbe *>(privdata));
  if (probe->alive.expired()) {
    return;
  }
  auto *redis_context = probe->context;
  auto *reply = static_cast<redisReply *>(raw_reply);
  if (reply == nullptr) {
    // The connection died before the check was answered. The disconnect
    // callback drives the retry.
    return;
  }
  if ((async_context->c.flags & REDIS_DISCONNECTING) != 0) {
    // A rejected AUTH already started the teardown, which hiredis completes
    // after this callback; the disconnect callback drives the retry. A server
    // that refuses the AUTH can still answer the check, for example one with no
    // password configured, and counting that as a reconnect would start a new
    // episode, with a fresh budget, on every attempt.
    return;
  }
  if (reply->type == REDIS_REPLY_ERROR) {
    // -LOADING while Redis reads its data back in, or -NOAUTH behind a rejected
    // AUTH (which has already started the teardown). Either way this server
    // cannot take commands yet, so the attempt fails.
    if (!absl::StartsWith(reply->str, "NOAUTH")) {
      RAY_LOG(WARNING) << "Redis is not ready after a reconnect: " << reply->str
                       << ". Tearing this connection down and retrying.";
    }
    redisAsyncDisconnect(async_context);
    return;
  }
  if (probe->expected_write > 0) {
    // Redis keeps a prefix of the commands it ran, and each write's marker ran
    // right after it, so the marker shows how far this connection's writes
    // survived. A missing marker, or one from another incarnation, means
    // Redis lost everything since this process started writing.
    std::optional<uint64_t> found;
    if (reply->type == REDIS_REPLY_STRING) {
      found = ParseWriteWatermark(std::string_view(reply->str, reply->len),
                                  probe->incarnation);
    }
    if (!found.has_value() || *found < probe->expected_write) {
      redis_context->OnWritesLost(
          absl::StrCat("Redis acknowledged write ",
                       probe->expected_write,
                       " on this connection, but after the reconnect it holds ",
                       found.has_value() ? absl::StrCat("only writes up to ", *found)
                                         : std::string("none of this process's writes")));
      return;
    }
  }
  redis_context->OnReconnectConfirmed();
}

void RedisContext::OnReconnectConfirmed() {
  reconnect_pending_ = false;
  if (!reconnecting_) {
    return;
  }
  reconnecting_ = false;
  reconnect_backoff_.Reset();
  // Invalidate any retry timer still armed from this episode.
  ++reconnect_epoch_;
  // The outage is over; the next one gets a fresh grace period.
  redis_async_context_->ClearOutage();
  redis_async_context_->PauseCommands(false);
  RAY_LOG(INFO) << "Reconnected to Redis at " << BuildAddress(address_, port_) << ".";
}

void RedisContext::OnWritesLost(const std::string &reason) {
  const std::string message = absl::StrCat(
      "Redis at ",
      BuildAddress(address_, port_),
      " came back without writes it had acknowledged: ",
      reason,
      ". It lost data during the outage, for example by restarting without "
      "persistence, restoring an older snapshot, or failing over to a replica "
      "that was behind. Another gcs_server writing to the same storage namespace "
      "also causes this. Exiting so that gcs_server restarts and reloads its state "
      "from Redis.");
  if (writes_lost_handler_for_testing_) {
    writes_lost_handler_for_testing_(message);
    return;
  }
  RAY_LOG(FATAL) << message;
}

void RedisContext::EnableWriteWatermark(std::string key, std::string field) {
  if (!RayConfig::instance().redis_reconnect_verify_writes()) {
    return;
  }
  RAY_CHECK(redis_async_context_);
  // Tells this process's markers apart from those of an earlier gcs_server
  // that used the same field: a restored snapshot can hold one of those.
  std::random_device random_device;
  std::mt19937_64 generator((static_cast<uint64_t>(random_device()) << 32) ^
                            random_device() ^
                            static_cast<uint64_t>(absl::ToUnixNanos(absl::Now())));
  std::string incarnation = absl::StrCat(absl::Hex(generator(), absl::kZeroPad16),
                                         absl::Hex(generator(), absl::kZeroPad16));
  redis_async_context_->EnableWriteWatermark(RedisAsyncContext::WriteWatermark{
      std::move(key), std::move(field), std::move(incarnation)});
}

void RedisContext::ScheduleReconnectRetry() {
  auto delay = reconnect_backoff_.Current();
  reconnect_backoff_.Next();
  execute_after(
      io_service_,
      [this, alive = std::weak_ptr<bool>(alive_), epoch = reconnect_epoch_] {
        if (alive.expired()) {
          return;
        }
        if (epoch != reconnect_epoch_) {
          // This timer belongs to a reconnect episode that already ended in
          // success. Without this check it would join the next episode as a
          // second driver and spend its budget twice as fast.
          return;
        }
        AttemptReconnect();
      },
      std::chrono::milliseconds(delay));
}

void RedisContext::AttemptReconnect() {
  if (!reconnecting_) {
    return;
  }
  if (reconnect_pending_) {
    // An attempt issued earlier (its resolve or its connect) has not reported
    // back yet; that report drives the next step. Only a stale timer from
    // before the attempt started can get here, and it has nothing to add.
    return;
  }

  if (reconnect_attempts_left_ <= 0) {
    RAY_LOG(FATAL) << "Failed to reconnect to Redis at " << BuildAddress(address_, port_)
                   << " after " << RayConfig::instance().redis_db_connect_retries()
                   << " attempts.";
  }
  // Count the attempt we are about to make, not the one before it: decrementing
  // first spends the budget on a try that never happens, so a configured 1
  // would make zero attempts.
  --reconnect_attempts_left_;

  // From here until the resolve or the connect reports back, a stale retry
  // timer must not start a second attempt.
  reconnect_pending_ = true;

  if (via_sentinel_) {
    // Ask Sentinel where the primary lives now. The Sentinel's own address is
    // commonly a name too (a Kubernetes Service), so it is looked up
    // asynchronously first: the synchronous query would otherwise run
    // getaddrinfo inside hiredis, which the connect timeout does not cover.
    ResolveAsync(
        origin_address_, origin_port_, [this](StatusOr<std::string> sentinel_ip) {
          Status status = sentinel_ip.ok() ? RefreshPrimaryFromSentinel(*sentinel_ip)
                                           : sentinel_ip.status();
          if (!status.ok()) {
            ContinueReconnect(status);
            return;
          }
          ResolvePrimaryThenConnect();
        });
    return;
  }
  ResolvePrimaryThenConnect();
}

void RedisContext::ResolvePrimaryThenConnect() {
  // Connect() resolved the name once. Behind a Kubernetes Service or any other
  // name that can move, the address recorded then may now belong to nothing,
  // so resolve again rather than retrying a stale IP forever. The same holds
  // for a primary that Sentinel announces by host name, and the async connect
  // below must be handed an IP: given a name, hiredis would run getaddrinfo
  // synchronously on this io_context.
  ResolveAsync(address_, port_, [this](StatusOr<std::string> ip) {
    if (!ip.ok()) {
      ContinueReconnect(ip.status());
      return;
    }
    if (*ip != resolved_ip_ && *ip != address_) {
      RAY_LOG(INFO) << "Redis address " << address_ << " now resolves to " << *ip
                    << ", was " << resolved_ip_ << ".";
    }
    resolved_ip_ = *ip;
    ContinueReconnect(Status::OK());
  });
}

void RedisContext::ResolveAsync(const std::string &host,
                                int port,
                                std::function<void(StatusOr<std::string>)> done) {
  boost::system::error_code parse_error;
  boost::asio::ip::make_address(host, parse_error);
  if (!parse_error) {
    // A literal IP needs no lookup.
    done(host);
    return;
  }
  // Resolve asynchronously: this runs on the GCS io_context, and a resolver
  // that blackholes during the outage must not stall it.
  resolver_.async_resolve(
      host,
      std::to_string(port),
      [this, alive = std::weak_ptr<bool>(alive_), host, port, done = std::move(done)](
          const boost::system::error_code &ec,
          const boost::asio::ip::tcp::resolver::results_type &results) {
        if (alive.expired() || ec == boost::asio::error::operation_aborted) {
          return;
        }
        if (!reconnecting_) {
          // Disconnect() ended the episode while the lookup was in flight.
          return;
        }
        if (ec || results.empty()) {
          done(Status::RedisError(absl::StrCat("Failed to resolve DNS for ",
                                               BuildAddress(host, port),
                                               ": ",
                                               ec ? ec.message() : "no addresses")));
          return;
        }
        done(results.begin()->endpoint().address().to_string());
      });
}

void RedisContext::ContinueReconnect(Status address_status) {
  // Success here only means the connect was issued. `redisAsyncConnect` is
  // non-blocking and reports a healthy context even when nothing is
  // listening, so the connect callback is what decides: success ends the
  // episode, failure comes back through OnAsyncDisconnected, which schedules
  // the next attempt. Nothing needs to poll in the meantime.
  Status status = address_status.ok() ? ConnectToResolvedAddress() : address_status;
  if (status.ok()) {
    return;
  }
  reconnect_pending_ = false;
  RAY_LOG(WARNING) << "Redis reconnect attempt failed: " << status << ". "
                   << reconnect_attempts_left_ << " attempts left.";
  ScheduleReconnectRetry();
}

std::unique_ptr<CallbackReply> RedisContext::RunArgvSync(
    const std::vector<std::string> &args) {
  RAY_CHECK(context_);
  // Build the arguments.
  std::vector<const char *> argv;
  std::vector<size_t> argc;
  for (const auto &arg : args) {
    argv.push_back(arg.data());
    argc.push_back(arg.size());
  }
  auto redis_reply = reinterpret_cast<redisReply *>(
      ::redisCommandArgv(context_.get(), args.size(), argv.data(), argc.data()));
  if (redis_reply == nullptr) {
    if (context_->err == REDIS_ERR_IO && (errno == EAGAIN || errno == EWOULDBLOCK)) {
      RAY_LOG(ERROR) << "Timed out waiting for redis command reply (sync).";
    } else {
      RAY_LOG(ERROR) << "Failed to run redis command (sync): " << context_->errstr;
    }
    return nullptr;
  }
  auto callback_reply = std::make_unique<CallbackReply>(*redis_reply);
  freeReplyObject(redis_reply);
  return callback_reply;
}

void RedisContext::RunArgvAsync(std::vector<std::string> args,
                                RedisCallback redis_callback,
                                std::string_view table_label) {
  RAY_CHECK(redis_async_context_);
  RAY_CHECK(!args.empty());
  auto request_context =
      new RedisRequestContext(io_service_,
                              std::move(redis_callback),
                              redis_async_context_.get(),
                              std::move(args),
                              clock_,
                              metrics_.has_value() ? &*metrics_ : nullptr,
                              table_label);
  // RedisRequestContext is thread safe.
  request_context->Run();
}

}  // namespace gcs

}  // namespace ray
