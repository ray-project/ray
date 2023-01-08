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

#include "ray/gcs/redis_context.h"

#include <sstream>

#include "ray/stats/metric_defs.h"
#include "ray/util/util.h"

extern "C" {
#include "hiredis/async.h"
#include "hiredis/hiredis.h"
#include "hiredis/hiredis_ssl.h"
}

// TODO(pcm): Integrate into the C++ tree.
#include "ray/common/ray_config.h"

namespace {

/// A helper function to call the callback and delete it from the callback
/// manager if necessary.
void ProcessCallback(int64_t callback_index,
                     std::shared_ptr<ray::gcs::CallbackReply> callback_reply) {
  RAY_CHECK(callback_index >= 0) << "The callback index must be greater than 0, "
                                 << "but it actually is " << callback_index;
  auto callback_item =
      ray::gcs::RedisCallbackManager::instance().GetCallback(callback_index);

  // Record the redis latency
  auto end_time = absl::GetCurrentTimeNanos() / 1000;
  ray::stats::GcsLatency().Record(end_time - callback_item->start_time_);

  // Dispatch the callback.
  callback_item->Dispatch(callback_reply);

  // Delete the callback
  ray::gcs::RedisCallbackManager::instance().RemoveCallback(callback_index);
}

}  // namespace

namespace ray {

namespace gcs {

CallbackReply::CallbackReply(redisReply *redis_reply) : reply_type_(redis_reply->type) {
  RAY_CHECK(nullptr != redis_reply);

  switch (reply_type_) {
  case REDIS_REPLY_NIL: {
    break;
  }
  case REDIS_REPLY_ERROR: {
    RAY_CHECK(false) << "Got an error in redis reply: " << redis_reply->str;
    break;
  }
  case REDIS_REPLY_INTEGER: {
    int_reply_ = static_cast<int64_t>(redis_reply->integer);
    break;
  }
  case REDIS_REPLY_STATUS: {
    const std::string status_str(redis_reply->str, redis_reply->len);
    if (status_str == "OK") {
      status_reply_ = Status::OK();
    } else {
      status_reply_ = Status::RedisError(status_str);
    }
    break;
  }
  case REDIS_REPLY_STRING: {
    string_reply_ = std::string(redis_reply->str, redis_reply->len);
    break;
  }
  case REDIS_REPLY_ARRAY: {
    if (redis_reply->elements == 0) {
      break;
    }
    // Array replies are used for scan or get.
    ParseAsStringArrayOrScanArray(redis_reply);
    break;
  }
  default: {
    RAY_LOG(WARNING) << "Encountered unexpected redis reply type: " << reply_type_;
  }
  }
}

void CallbackReply::ParseAsStringArrayOrScanArray(redisReply *redis_reply) {
  RAY_CHECK(REDIS_REPLY_ARRAY == redis_reply->type);
  const auto array_size = static_cast<size_t>(redis_reply->elements);
  if (array_size == 2) {
    auto *cursor_entry = redis_reply->element[0];
    auto *array_entry = redis_reply->element[1];
    if (REDIS_REPLY_ARRAY == array_entry->type) {
      // Parse as a scan array
      RAY_CHECK(REDIS_REPLY_STRING == cursor_entry->type);
      std::string cursor_str(cursor_entry->str, cursor_entry->len);
      next_scan_cursor_reply_ = std::stoi(cursor_str);
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

void CallbackReply::ParseAsStringArray(redisReply *redis_reply) {
  RAY_CHECK(REDIS_REPLY_ARRAY == redis_reply->type);
  const auto array_size = static_cast<size_t>(redis_reply->elements);
  string_array_reply_.reserve(array_size);
  for (size_t i = 0; i < array_size; ++i) {
    auto *entry = redis_reply->element[i];
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

// This is a global redis callback which will be registered for every
// asynchronous redis call. It dispatches the appropriate callback
// that was registered with the RedisCallbackManager.
void GlobalRedisCallback(void *c, void *r, void *privdata) {
  if (r == nullptr) {
    return;
  }
  int64_t callback_index = reinterpret_cast<int64_t>(privdata);
  redisReply *reply = reinterpret_cast<redisReply *>(r);
  ProcessCallback(callback_index, std::make_shared<CallbackReply>(reply));
}

int64_t RedisCallbackManager::AllocateCallbackIndex() {
  std::lock_guard<std::mutex> lock(mutex_);
  return num_callbacks_++;
}

int64_t RedisCallbackManager::AddCallback(const RedisCallback &function,
                                          instrumented_io_context &io_service,
                                          int64_t callback_index) {
  auto start_time = absl::GetCurrentTimeNanos() / 1000;

  std::lock_guard<std::mutex> lock(mutex_);
  if (callback_index == -1) {
    // No callback index was specified. Allocate a new callback index.
    callback_index = num_callbacks_;
    num_callbacks_++;
  }
  callback_items_.emplace(
      callback_index, std::make_shared<CallbackItem>(function, start_time, io_service));
  return callback_index;
}

std::shared_ptr<RedisCallbackManager::CallbackItem> RedisCallbackManager::GetCallback(
    int64_t callback_index) const {
  std::lock_guard<std::mutex> lock(mutex_);
  auto it = callback_items_.find(callback_index);
  RAY_CHECK(it != callback_items_.end()) << callback_index;
  return it->second;
}

void RedisCallbackManager::RemoveCallback(int64_t callback_index) {
  std::lock_guard<std::mutex> lock(mutex_);
  callback_items_.erase(callback_index);
}

#define REDIS_CHECK_ERROR(CONTEXT, REPLY)                     \
  if (REPLY == nullptr || REPLY->type == REDIS_REPLY_ERROR) { \
    return Status::RedisError(CONTEXT->errstr);               \
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
  if (context_) {
    redisFree(context_);
    context_ = nullptr;
  }
  if (ssl_context_) {
    redisFreeSSLContext(ssl_context_);
    ssl_context_ = nullptr;
  }
}

Status AuthenticateRedis(redisContext *context, const std::string &password) {
  if (password == "") {
    return Status::OK();
  }
  redisReply *reply =
      reinterpret_cast<redisReply *>(redisCommand(context, "AUTH %s", password.c_str()));
  REDIS_CHECK_ERROR(context, reply);
  freeReplyObject(reply);
  return Status::OK();
}

Status AuthenticateRedis(redisAsyncContext *context, const std::string &password) {
  if (password == "") {
    return Status::OK();
  }
  int status = redisAsyncCommand(context, NULL, NULL, "AUTH %s", password.c_str());
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

void FreeRedisContext(redisContext *context) { redisFree(context); }

void FreeRedisContext(redisAsyncContext *context) {}

void FreeRedisContext(RedisAsyncContext *context) {}

template <typename RedisContext, typename RedisConnectFunction>
Status ConnectWithoutRetries(const std::string &address,
                             int port,
                             const RedisConnectFunction &connect_function,
                             RedisContext **context,
                             std::string &errorMessage) {
  // This currently returns the errorMessage in two different ways,
  // as an output parameter and in the Status::RedisError,
  // because we're not sure whether we'll want to change what this returns.
  RedisContext *newContext = connect_function(address.c_str(), port);
  if (newContext == nullptr || (newContext)->err) {
    std::ostringstream oss(errorMessage);
    if (newContext == nullptr) {
      oss << "Could not allocate Redis context.";
    } else if (newContext->err) {
      oss << "Could not establish connection to Redis " << address << ":" << port
          << " (context.err = " << newContext->err << ").";
    }
    return Status::RedisError(errorMessage);
  }
  if (context != nullptr) {
    // Don't crash if the RedisContext** is null.
    *context = newContext;
  } else {
    FreeRedisContext(newContext);
  }
  return Status::OK();
}

template <typename RedisContext, typename RedisConnectFunction>
Status ConnectWithRetries(const std::string &address,
                          int port,
                          const RedisConnectFunction &connect_function,
                          RedisContext **context) {
  int connection_attempts = 0;
  std::string errorMessage = "";
  Status status =
      ConnectWithoutRetries(address, port, connect_function, context, errorMessage);
  while (!status.ok()) {
    if (connection_attempts >= RayConfig::instance().redis_db_connect_retries()) {
      RAY_LOG(FATAL) << RayConfig::instance().redis_db_connect_retries() << " attempts "
                     << "to connect have all failed. Please check whether the"
                     << " redis storage is alive or not. The last error message was: "
                     << errorMessage;
      break;
    }
    RAY_LOG(WARNING) << errorMessage << " Will retry in "
                     << RayConfig::instance().redis_db_connect_wait_milliseconds()
                     << " milliseconds. Each retry takes about two minutes.";
    // Sleep for a little.
    std::this_thread::sleep_for(std::chrono::milliseconds(
        RayConfig::instance().redis_db_connect_wait_milliseconds()));
    status =
        ConnectWithoutRetries(address, port, connect_function, context, errorMessage);
    connection_attempts += 1;
  }
  return Status::OK();
}

Status RedisContext::PingPort(const std::string &address, int port) {
  std::string errorMessage;
  return ConnectWithoutRetries(
      address, port, redisConnect, static_cast<redisContext **>(nullptr), errorMessage);
}

Status RedisContext::Connect(const std::string &address,
                             int port,
                             bool sharding,
                             const std::string &password,
                             bool enable_ssl) {
  RAY_CHECK(!context_);
  RAY_CHECK(!redis_async_context_);

  RAY_CHECK_OK(ConnectWithRetries(address, port, redisConnect, &context_));
  if (enable_ssl) {
    RAY_CHECK(ssl_context_ != nullptr);
    RAY_CHECK(redisInitiateSSLWithContext(context_, ssl_context_) == REDIS_OK)
        << "Failed to setup encrypted redis: " << context_->errstr;
  }
  RAY_CHECK_OK(AuthenticateRedis(context_, password));

  // Connect to async context
  redisAsyncContext *async_context = nullptr;
  RAY_CHECK_OK(ConnectWithRetries(address, port, redisAsyncConnect, &async_context));
  if (enable_ssl) {
    RAY_CHECK(ssl_context_ != nullptr);
    RAY_CHECK(redisInitiateSSLWithContext(&async_context->c, ssl_context_) == REDIS_OK)
        << "Failed to setup encrypted redis: " << context_->errstr;
  }
  RAY_CHECK_OK(AuthenticateRedis(async_context, password));
  redis_async_context_.reset(new RedisAsyncContext(async_context));
  SetDisconnectCallback(redis_async_context_.get());

  return Status::OK();
}

std::unique_ptr<CallbackReply> RedisContext::RunArgvSync(
    const std::vector<std::string> &args) {
  RAY_CHECK(context_);
  // Build the arguments.
  std::vector<const char *> argv;
  std::vector<size_t> argc;
  for (size_t i = 0; i < args.size(); ++i) {
    RAY_LOG(INFO) << "CMD: " << i << ", " << std::string(args[i].data(), args[i].size());
    argv.push_back(args[i].data());
    argc.push_back(args[i].size());
  }
  auto redis_reply = reinterpret_cast<redisReply *>(
      ::redisCommandArgv(context_, args.size(), argv.data(), argc.data()));
  if (redis_reply == nullptr) {
    RAY_LOG(ERROR) << "Failed to send redis command (sync).";
    return nullptr;
  }
  std::unique_ptr<CallbackReply> callback_reply(new CallbackReply(redis_reply));
  freeReplyObject(redis_reply);
  return callback_reply;
}

Status RedisContext::RunArgvAsync(const std::vector<std::string> &args,
                                  const RedisCallback &redis_callback) {
  RAY_CHECK(redis_async_context_);
  // Build the arguments.
  std::vector<const char *> argv;
  std::vector<size_t> argc;
  for (size_t i = 0; i < args.size(); ++i) {
    RAY_LOG(INFO) << "CMD: " << i << ", " << std::string(args[i].data(), args[i].size());
    argv.push_back(args[i].data());
    argc.push_back(args[i].size());
  }
  int64_t callback_index =
      RedisCallbackManager::instance().AddCallback(redis_callback, io_service_);
  // Run the Redis command.
  Status status = redis_async_context_->RedisAsyncCommandArgv(
      reinterpret_cast<redisCallbackFn *>(&GlobalRedisCallback),
      reinterpret_cast<void *>(callback_index),
      args.size(),
      argv.data(),
      argc.data());
  return status;
}

void RedisContext::FreeRedisReply(void *reply) { return freeReplyObject(reply); }

int RedisContext::GetRedisError(redisContext *context) { return context->err; }

}  // namespace gcs

}  // namespace ray
