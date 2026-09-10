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
#include <memory>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "ray/asio/asio_util.h"
#include "ray/util/network_util.h"

extern "C" {
#include "hiredis/async.h"
#include "hiredis/hiredis_ssl.h"
}

// TODO(pcm): Integrate into the C++ tree.
#include "absl/strings/str_cat.h"
#include "absl/strings/str_join.h"
#include "absl/strings/str_split.h"
#include "ray/common/ray_config.h"
#include "ray/common/status_or.h"

namespace ray {

namespace gcs {

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
                                         ClockInterface &clock)
    : exp_back_off_(RayConfig::instance().redis_retry_base_ms(),
                    RayConfig::instance().redis_retry_multiplier(),
                    RayConfig::instance().redis_retry_max_ms()),
      io_service_(io_service),
      redis_context_(context),
      context_alive_(context->GetAliveToken()),
      pending_retries_(RayConfig::instance().num_redis_request_retries() + 1),
      callback_(std::move(callback)),
      start_time_(clock.Now()),
      redis_cmds_(std::move(args)),
      clock_(clock) {
  argc_.reserve(redis_cmds_.size());
  argv_.reserve(redis_cmds_.size());
  for (size_t i = 0; i < redis_cmds_.size(); ++i) {
    argv_.push_back(redis_cmds_[i].data());
    argc_.push_back(redis_cmds_[i].size());
  }
}

void RedisRequestContext::RedisResponseFn(redisAsyncContext *async_context,
                                          void *raw_reply,
                                          void *privdata) {
  auto *request_cxt = static_cast<RedisRequestContext *>(privdata);
  auto redis_reply = reinterpret_cast<redisReply *>(raw_reply);
  // Error happened.
  if (redis_reply == nullptr || redis_reply->type == REDIS_REPLY_ERROR) {
    if (request_cxt->context_alive_.expired()) {
      // The owning RedisAsyncContext is being destroyed and is flushing its
      // pending callbacks; the event loop may be going away with it. There is
      // nobody left to answer, and scheduling a retry timer would touch a
      // dying io_context. Drop the request instead.
      delete request_cxt;
      return;
    }
    bool refunded = false;
    if (redis_reply == nullptr &&
        request_cxt->clock_.Now() - request_cxt->start_time_ <
            absl::Milliseconds(RayConfig::instance().redis_reconnect_grace_period_ms())) {
      // No reply at all means the command never made it to Redis: the
      // connection was gone, or the context was still mid-connect when hiredis
      // accepted it and then freed it. Either way the attempt says nothing
      // about the command, so give the retry back and let the reconnect finish.
      // The wall clock above is what bounds the wait.
      ++request_cxt->pending_retries_;
      refunded = true;
    }
    auto error_msg = redis_reply ? redis_reply->str
                                 : (async_context ? async_context->errstr
                                                  : "Redis connection unavailable");
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
                     << "]"
                     << " failed due to error " << error_msg << ". "
                     << request_cxt->pending_retries_ << " retries left.";
    }
    auto delay = request_cxt->exp_back_off_.Current();
    request_cxt->exp_back_off_.Next();
    // Retry the request after a while.
    execute_after(
        request_cxt->io_service_,
        [request_cxt]() { request_cxt->Run(); },
        std::chrono::milliseconds(delay));
  } else {
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

  Status status = redis_context_->RedisAsyncCommandArgv(
      RedisResponseFn, this, argv_.size(), argv_.data(), argc_.data());

  if (!status.ok()) {
    // Reports through the same path as a dropped reply, which is where the
    // retry budget is refunded while a reconnect is in flight. Pass a null
    // context rather than the raw pointer: reading it here is unsynchronized
    // against the io_service thread freeing it, and the callback only wants
    // it for errstr.
    RedisResponseFn(nullptr, nullptr, this);
  }
}

#define REDIS_CHECK_ERROR(CONTEXT, REPLY)       \
  if (REPLY == nullptr) {                       \
    return Status::RedisError(CONTEXT->errstr); \
  }                                             \
  if (REPLY->type == REDIS_REPLY_ERROR) {       \
    return Status::RedisError(REPLY->str);      \
  }

RedisContext::RedisContext(instrumented_io_context &io_service, ClockInterface &clock)
    : io_service_(io_service),
      clock_(clock),
      context_(nullptr),
      ssl_context_(nullptr),
      redis_db_probe_timeout_milliseconds_(
          RayConfig::instance().redis_db_probe_timeout_milliseconds()),
      reconnect_backoff_(RayConfig::instance().redis_retry_base_ms(),
                         RayConfig::instance().redis_retry_multiplier(),
                         RayConfig::instance().redis_retry_max_ms()) {
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
  reconnecting_ = false;
  reconnect_pending_ = false;
  auth_pending_ = false;
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
  RAY_LOG(DEBUG) << "Redis async context disconnected. Status: " << status;
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

void SetConnectionCallbacks(RedisAsyncContext *redis_async_context) {
  redisAsyncContext *raw_redis_async_context =
      redis_async_context->GetRawRedisAsyncContext();
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
  return std::make_pair(Status::OK(),
                        std::unique_ptr<RedisContextType, RedisContextDeleter>(
                            newContext, RedisContextDeleter()));
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
  redis_async_context_->SetConnectHandler([this] { OnAsyncConnected(); });
  SetConnectionCallbacks(redis_async_context_.get());

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
  }
  return status;
}

void RedisContext::OnAsyncDisconnected() {
  // Whatever connect was outstanding is settled now, one way or another.
  reconnect_pending_ = false;
  if (disconnect_requested_) {
    // Torn down on purpose. Do not resurrect it, and do not touch io_service_:
    // this can run from ~RedisContext after the io_context is gone.
    return;
  }
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

Status RedisContext::RefreshPrimaryFromSentinel() {
  if (!via_sentinel_) {
    return Status::OK();
  }
  // A short-lived synchronous connection: the Sentinel is the one address that
  // survives a failover, and the stored sync context points at the old primary.
  // This runs on the io_service thread, so every step must be bounded: the
  // connect by a timeout, the query below by the probe timeout.
  const int64_t timeout_ms = redis_db_probe_timeout_milliseconds_;
  auto connect_with_timeout = [timeout_ms](const std::string &host, int port) {
    struct timeval timeout;
    timeout.tv_sec = timeout_ms / 1000;
    timeout.tv_usec = (timeout_ms % 1000) * 1000;
    return redisConnectWithTimeout(host.c_str(), port, timeout);
  };
  auto resp = ConnectWithoutRetries<redisContext>(
      origin_address_, origin_port_, connect_with_timeout);
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
  RAY_RETURN_NOT_OK(AuthenticateRedis(sentinel_context.get(),
                                      username_,
                                      password_,
                                      redis_db_probe_timeout_milliseconds_));

  // AuthenticateRedis restores the socket to no-timeout (and skips the setup
  // entirely without a password), so bound the query ourselves. The context is
  // discarded right after, so there is nothing to restore.
  RAY_RETURN_NOT_OK(
      SetRedisProbeTimeout(sentinel_context.get(), redis_db_probe_timeout_milliseconds_));
  std::string primary_ip;
  int primary_port = 0;
  RAY_RETURN_NOT_OK(
      QuerySentinelForPrimary(sentinel_context.get(), &primary_ip, &primary_port));
  if (primary_ip != resolved_ip_ || primary_port != port_) {
    RAY_LOG(INFO) << "Redis Sentinel now reports the primary at "
                  << BuildAddress(primary_ip, primary_port) << ", was "
                  << BuildAddress(resolved_ip_, port_) << ".";
    resolved_ip_ = primary_ip;
    port_ = primary_port;
    address_ = primary_ip;
  }
  return Status::OK();
}

Status RedisContext::RefreshResolvedAddress() {
  // Connect() resolved the name once. Behind a Kubernetes Service or any other
  // name that can move, the address recorded then may now belong to nothing,
  // so resolve again rather than retrying a stale IP forever. A literal IP
  // resolves to itself, which makes this a no-op for that case.
  std::vector<std::string> ip_addresses;
  try {
    ip_addresses = ResolveDNS(io_service_, address_, port_);
  } catch (const std::exception &e) {
    return Status::RedisError(absl::StrCat(
        "Failed to resolve DNS for ", BuildAddress(address_, port_), ": ", e.what()));
  }
  if (ip_addresses.empty()) {
    return Status::RedisError(
        absl::StrCat("Failed to resolve DNS for ", BuildAddress(address_, port_)));
  }
  if (ip_addresses[0] != resolved_ip_) {
    RAY_LOG(INFO) << "Redis address " << address_ << " now resolves to "
                  << ip_addresses[0] << ", was " << resolved_ip_ << ".";
    resolved_ip_ = ip_addresses[0];
  }
  return Status::OK();
}

namespace {
/// privdata for ReconnectAuthCallback. Heap-allocated because hiredis may
/// hold it past the RedisContext's lifetime (teardown flushes callbacks).
struct ReconnectAuthProbe {
  RedisContext *context;
  std::weak_ptr<bool> alive;
};
}  // namespace

Status RedisContext::ReconnectAsyncContext() {
  if (via_sentinel_) {
    // Sentinel is authoritative for where the primary lives; a DNS pass over
    // the IP it just handed out would resolve it to itself.
    RAY_RETURN_NOT_OK(RefreshPrimaryFromSentinel());
  } else {
    RAY_RETURN_NOT_OK(RefreshResolvedAddress());
  }
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
  // callback and let that decide the attempt, so a promoted primary that
  // rejects the credentials reads as a failed attempt instead of a
  // "Reconnected" log followed by -NOAUTH replies. Queued before Reset() so
  // it stays ahead of any retried command in the pipeline.
  auth_pending_ = false;
  if (!password_.empty()) {
    auto *probe = new ReconnectAuthProbe{this, std::weak_ptr<bool>(alive_)};
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
    auth_pending_ = true;
  }

  // Rebind rather than recreate: in-flight RedisRequestContexts hold a raw
  // pointer to this RedisAsyncContext.
  redis_async_context_->Reset(std::move(async_context));
  SetConnectionCallbacks(redis_async_context_.get());
  return Status::OK();
}

void RedisContext::ReconnectAuthCallback(redisAsyncContext *async_context,
                                         void *raw_reply,
                                         void *privdata) {
  std::unique_ptr<ReconnectAuthProbe> probe(static_cast<ReconnectAuthProbe *>(privdata));
  if (probe->alive.expired()) {
    // Flushed during teardown; the RedisContext is already gone.
    return;
  }
  auto *redis_context = probe->context;
  auto *reply = static_cast<redisReply *>(raw_reply);
  if (reply == nullptr) {
    // The connection died before AUTH was answered. The disconnect callback
    // drives the retry; there is nothing to decide here.
    redis_context->auth_pending_ = false;
    return;
  }
  if (reply->type == REDIS_REPLY_ERROR) {
    RAY_LOG(WARNING) << "Redis rejected AUTH after a reconnect: " << reply->str
                     << ". Tearing this connection down and retrying.";
    redis_context->auth_pending_ = false;
    // Runs from inside a hiredis callback, where redisAsyncDisconnect is the
    // sanctioned teardown: it completes after the callback returns and then
    // invokes the disconnect callback, which schedules the next attempt.
    if (redis_context->redis_async_context_ != nullptr) {
      redisAsyncContext *raw =
          redis_context->redis_async_context_->GetRawRedisAsyncContext();
      if (raw != nullptr) {
        redisAsyncDisconnect(raw);
      }
    }
    return;
  }
  redis_context->auth_pending_ = false;
  redis_context->OnAsyncConnected();
}

void RedisContext::OnAsyncConnected() {
  if (auth_pending_) {
    // TCP is up, but the server has not accepted the credentials yet.
    // ReconnectAuthCallback declares the outcome.
    return;
  }
  reconnect_pending_ = false;
  if (!reconnecting_) {
    return;
  }
  reconnecting_ = false;
  reconnect_backoff_.Reset();
  // Invalidate any retry timer still armed from this episode.
  ++reconnect_epoch_;
  RAY_LOG(INFO) << "Reconnected to Redis at " << BuildAddress(address_, port_) << ".";
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
  if (redis_async_context_ == nullptr) {
    // Disconnect() ran concurrently; we are shutting down.
    reconnecting_ = false;
    return;
  }
  if (reconnect_pending_) {
    // A connect issued earlier has not reported back yet; its callback drives
    // the next step. Only a stale timer from before that connect was issued
    // can get here, and it has nothing to add.
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

  // Success here only means the connect was issued. `redisAsyncConnect` is
  // non-blocking and reports a healthy context even when nothing is
  // listening, so the connect callback is what decides.
  Status status = ReconnectAsyncContext();
  if (status.ok()) {
    // The connect callback settles this attempt: success ends the episode,
    // failure comes back through OnAsyncDisconnected, which schedules the
    // next attempt. Nothing needs to poll in the meantime.
    reconnect_pending_ = true;
  } else {
    RAY_LOG(WARNING) << "Redis reconnect attempt failed: " << status << ". "
                     << reconnect_attempts_left_ << " attempts left.";
    ScheduleReconnectRetry();
  }
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
                                RedisCallback redis_callback) {
  RAY_CHECK(redis_async_context_);
  auto request_context = new RedisRequestContext(io_service_,
                                                 std::move(redis_callback),
                                                 redis_async_context_.get(),
                                                 std::move(args),
                                                 clock_);
  // RedisRequestContext is thread safe.
  request_context->Run();
}

}  // namespace gcs

}  // namespace ray
