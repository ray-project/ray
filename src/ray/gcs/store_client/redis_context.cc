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

#include <memory>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "ray/common/asio/asio_util.h"
#include "ray/stats/metric_defs.h"
#include "ray/util/network_util.h"

extern "C" {
#include "hiredis/async.h"
#include "hiredis/hiredis_ssl.h"
}

// TODO(pcm): Integrate into the C++ tree.
#include "absl/strings/str_join.h"
#include "absl/strings/str_split.h"
#include "ray/common/ray_config.h"

namespace ray {

namespace gcs {

CallbackReply::CallbackReply(const redisReply &redis_reply)
    : reply_type_(redis_reply.type) {
  switch (reply_type_) {
  case REDIS_REPLY_NIL: {
    break;
  }
  case REDIS_REPLY_ERROR: {
    RAY_LOG(FATAL) << "Got an error in redis reply: " << redis_reply.str;
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
                                         std::vector<std::string> args)
    : exp_back_off_(RayConfig::instance().redis_retry_base_ms(),
                    RayConfig::instance().redis_retry_multiplier(),
                    RayConfig::instance().redis_retry_max_ms()),
      io_service_(io_service),
      redis_context_(context),
      pending_retries_(RayConfig::instance().num_redis_request_retries() + 1),
      callback_(std::move(callback)),
      start_time_(absl::Now()),
      redis_cmds_(std::move(args)) {
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
    auto error_msg = redis_reply ? redis_reply->str : async_context->errstr;
    RAY_LOG(ERROR) << "Redis request [" << absl::StrJoin(request_cxt->redis_cmds_, " ")
                   << "]"
                   << " failed due to error " << error_msg << ". "
                   << request_cxt->pending_retries_ << " retries left.";
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
    auto end_time = absl::Now();
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
    RedisResponseFn(redis_context_->GetRawRedisAsyncContext(), nullptr, this);
  }
}

#define REDIS_CHECK_ERROR(CONTEXT, REPLY)       \
  if (REPLY == nullptr) {                       \
    return Status::RedisError(CONTEXT->errstr); \
  }                                             \
  if (REPLY->type == REDIS_REPLY_ERROR) {       \
    return Status::RedisError(REPLY->str);      \
  }

RedisContext::RedisContext(instrumented_io_context &io_service)
    : io_service_(io_service), context_(nullptr), ssl_context_(nullptr) {
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
  context_.reset();
  redis_async_context_.reset();
}

Status AuthenticateRedis(redisContext *context,
                         const std::string &username,
                         const std::string &password) {
  if (password == "") {
    RAY_CHECK(username.empty());
    return Status::OK();
  }
  redisReply *reply;
  if (username.empty()) {
    reply = reinterpret_cast<redisReply *>(
        redisCommand(context, "AUTH %s", password.c_str()));
  } else {
    reply = reinterpret_cast<redisReply *>(
        redisCommand(context, "AUTH %s %s", username.c_str(), password.c_str()));
  }
  REDIS_CHECK_ERROR(context, reply);
  freeReplyObject(reply);
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
  // Reset raw 'redisAsyncContext' to nullptr because hiredis will release this context.
  reinterpret_cast<RedisAsyncContext *>(context->data)->ResetRawRedisAsyncContext();
}

void SetDisconnectCallback(RedisAsyncContext *redis_async_context) {
  redisAsyncContext *raw_redis_async_context =
      redis_async_context->GetRawRedisAsyncContext();
  raw_redis_async_context->data = redis_async_context;
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
  RAY_CHECK_EQ(parts.size(), 3u);
  auto ip_port = ParseAddress(parts[2]);
  RAY_CHECK(ip_port.has_value());
  return std::make_pair((*ip_port)[0], std::stoi((*ip_port)[1]));
}
}  // namespace

void RedisContext::ValidateRedisDB() {
  auto reply = RunArgvSync(std::vector<std::string>{"INFO", "CLUSTER"});
  // cluster_state:ok
  // cluster_slots_assigned:16384
  // cluster_slots_ok:16384
  // cluster_slots_pfail:0
  // cluster_size:1
  RAY_CHECK(reply && !reply->IsNil()) << "Failed to get Redis cluster info";
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
    RAY_CHECK(kv.size() == 2);
    if (kv[0] == "cluster_state") {
      if (kv[1] == "ok") {
        cluster_mode = true;
      } else if (kv[1] == "fail") {
        RAY_LOG(FATAL)
            << "The Redis cluster is not healthy. cluster_state shows failed status: "
            << cluster_info << "."
            << " Please check Redis cluster used.";
      }
    }
    if (kv[0] == "cluster_size") {
      cluster_size = std::stoi(kv[1]);
    }
  }

  if (cluster_mode) {
    RAY_CHECK(cluster_size == 1)
        << "Ray currently doesn't support Redis Cluster with more than one shard. ";
  }
}

bool RedisContext::IsRedisSentinel() {
  auto reply = RunArgvSync(std::vector<std::string>{"INFO", "SENTINEL"});
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
  ValidateRedisDB();

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

  if (redis_reply->type == REDIS_REPLY_ERROR) {
    // This should be a MOVED error
    // MOVED 14946 10.xx.xx.xx:7001
    std::string error_msg(redis_reply->str, redis_reply->len);
    freeReplyObject(redis_reply);
    auto maybe_ip_port = ParseIffMovedError(error_msg);
    RAY_CHECK(maybe_ip_port.has_value())
        << "Setup Redis cluster failed in the dummy deletion: " << error_msg;
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

Status ConnectRedisSentinel(RedisContext &context,
                            const std::string &username,
                            const std::string &password,
                            bool enable_ssl) {
  RAY_LOG(INFO) << "Connect to Redis sentinel";

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
      ::redisCommandArgv(context.sync_context(), cmds.size(), argv.data(), argc.data()));

  RAY_CHECK(redis_reply) << "Failed to get redis sentinel masters info";
  RAY_CHECK_EQ(redis_reply->type, REDIS_REPLY_ARRAY)
      << "Redis sentinel master info should be REDIS_REPLY_ARRAY but got "
      << redis_reply->type;
  RAY_CHECK_EQ(redis_reply->elements, 1UL)
      << "There should be only one primary behind the Redis sentinel";
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
    RAY_LOG(ERROR)
        << "Failed to get the ip and port of the primary node from Redis sentinel";
    return Status::RedisError(
        "Failed to get the ip and port of the primary node from Redis sentinel");
  } else {
    RAY_LOG(INFO) << "Connecting to the Redis primary node behind sentinel: "
                  << BuildAddress(actual_ip, actual_port);
    context.Disconnect();
    return context.Connect(
        actual_ip, std::stoi(actual_port), username, password, enable_ssl);
  }
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
  // Fetch the ip address from the address. It might return multiple
  // addresses and only the first one will be used.
  auto ip_addresses = ResolveDNS(io_service_, address, port);
  RAY_CHECK(!ip_addresses.empty())
      << "Failed to resolve DNS for " << BuildAddress(address, port);

  RAY_LOG(INFO) << "Resolve Redis address to " << absl::StrJoin(ip_addresses, ", ");

  {
    auto resp = ConnectWithRetries<redisContext>(ip_addresses[0], port, redisConnect);
    RAY_CHECK_OK(resp.first /* status */);
    context_ = std::move(resp.second /* redisContext */);
  }

  if (enable_ssl) {
    RAY_CHECK(ssl_context_ != nullptr);
    RAY_CHECK(redisInitiateSSLWithContext(context_.get(), ssl_context_) == REDIS_OK)
        << "Failed to setup encrypted redis: " << context_->errstr;
  }
  RAY_CHECK_OK(AuthenticateRedis(context_.get(), username, password));

  // Connect to async context
  std::unique_ptr<redisAsyncContext, RedisContextDeleter> async_context;
  {
    auto resp =
        ConnectWithRetries<redisAsyncContext>(ip_addresses[0], port, redisAsyncConnect);
    RAY_CHECK_OK(resp.first);
    async_context = std::move(resp.second);
  }
  if (enable_ssl) {
    RAY_CHECK(ssl_context_ != nullptr);
    RAY_CHECK(redisInitiateSSLWithContext(&async_context->c, ssl_context_) == REDIS_OK)
        << "Failed to setup encrypted redis: " << async_context->errstr;
  }
  RAY_CHECK_OK(AuthenticateRedis(async_context.get(), username, password));
  redis_async_context_.reset(
      new RedisAsyncContext(io_service_, std::move(async_context)));
  SetDisconnectCallback(redis_async_context_.get());

  // handle validation and primary connection for different types of redis
  if (IsRedisSentinel()) {
    return ConnectRedisSentinel(*this, username, password, enable_ssl);
  } else {
    return ConnectRedisCluster(
        username, password, enable_ssl, BuildAddress(ip_addresses[0], port));
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
    RAY_LOG(ERROR) << "Failed to send redis command (sync): " << context_->errstr;
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
                                                 std::move(args));
  // RedisRequestContext is thread safe.
  request_context->Run();
}

}  // namespace gcs

}  // namespace ray
