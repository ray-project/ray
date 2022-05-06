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
  if (!callback_item->is_subscription_) {
    // Record the redis latency for non-subscription redis operations.
    auto end_time = absl::GetCurrentTimeNanos() / 1000;
    ray::stats::GcsLatency().Record(end_time - callback_item->start_time_);
  }

  // Dispatch the callback.
  callback_item->Dispatch(callback_reply);

  if (!callback_item->is_subscription_) {
    // Delete the callback if it's not a subscription callback.
    ray::gcs::RedisCallbackManager::instance().RemoveCallback(callback_index);
  }
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
    redisReply *message_type = redis_reply->element[0];
    if (message_type->type == REDIS_REPLY_STRING) {
      if (strcmp(message_type->str, "subscribe") == 0 ||
          strcmp(message_type->str, "psubscribe") == 0) {
        // If the message is for the initial subscription call, return the empty
        // string as a response to signify that subscription was successful.
        is_subscribe_callback_ = true;
        break;
      } else if (strcmp(message_type->str, "punsubscribe") == 0 ||
                 strcmp(message_type->str, "unsubscribe") == 0) {
        is_unsubscribe_callback_ = true;
        break;
      } else if (strcmp(message_type->str, "message") == 0 ||
                 strcmp(message_type->str, "pmessage") == 0) {
        // If the message is from a PUBLISH, make sure the data is nonempty.
        redisReply *message = redis_reply->element[redis_reply->elements - 1];
        // data is a notification message.
        string_reply_ = std::string(message->str, message->len);
        RAY_CHECK(!string_reply_.empty())
            << "Empty message received on subscribe channel.";
        break;
      }
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

const std::string &CallbackReply::ReadAsPubsubData() const {
  RAY_CHECK(reply_type_ == REDIS_REPLY_ARRAY) << "Unexpected type: " << reply_type_;
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
                                          bool is_subscription,
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
      callback_index,
      std::make_shared<CallbackItem>(function, is_subscription, start_time, io_service));
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

RedisContext::~RedisContext() {
  if (context_) {
    redisFree(context_);
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
                     << "to connect have all failed. The last error message was: "
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
                             const std::string &password) {
  RAY_CHECK(!context_);
  RAY_CHECK(!redis_async_context_);
  RAY_CHECK(!async_redis_subscribe_context_);

  RAY_CHECK_OK(ConnectWithRetries(address, port, redisConnect, &context_));
  RAY_CHECK_OK(AuthenticateRedis(context_, password));

  redisReply *reply = reinterpret_cast<redisReply *>(
      redisCommand(context_, "CONFIG SET notify-keyspace-events Kl"));
  REDIS_CHECK_ERROR(context_, reply);
  freeReplyObject(reply);

  // Connect to async context
  redisAsyncContext *async_context = nullptr;
  RAY_CHECK_OK(ConnectWithRetries(address, port, redisAsyncConnect, &async_context));
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
  for (const auto &arg : args) {
    argv.push_back(arg.data());
    argc.push_back(arg.size());
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
    argv.push_back(args[i].data());
    argc.push_back(args[i].size());
  }
  int64_t callback_index =
      RedisCallbackManager::instance().AddCallback(redis_callback, false, io_service_);
  // Run the Redis command.
  Status status = redis_async_context_->RedisAsyncCommandArgv(
      reinterpret_cast<redisCallbackFn *>(&GlobalRedisCallback),
      reinterpret_cast<void *>(callback_index),
      args.size(),
      argv.data(),
      argc.data());
  return status;
}

Status RedisContext::SubscribeAsync(const NodeID &node_id,
                                    const TablePubsub pubsub_channel,
                                    const RedisCallback &redisCallback,
                                    int64_t *out_callback_index) {
  RAY_CHECK(pubsub_channel != TablePubsub::NO_PUBLISH)
      << "Client requested subscribe on a table that does not support pubsub";
  RAY_CHECK(async_redis_subscribe_context_);

  int64_t callback_index =
      RedisCallbackManager::instance().AddCallback(redisCallback, true, io_service_);
  RAY_CHECK(out_callback_index != nullptr);
  *out_callback_index = callback_index;
  Status status = Status::OK();
  if (node_id.IsNil()) {
    // Subscribe to all messages.
    std::string redis_command = "SUBSCRIBE %d";
    status = async_redis_subscribe_context_->RedisAsyncCommand(
        reinterpret_cast<redisCallbackFn *>(&GlobalRedisCallback),
        reinterpret_cast<void *>(callback_index),
        redis_command.c_str(),
        pubsub_channel);
  } else {
    // Subscribe only to messages sent to this client.
    std::string redis_command = "SUBSCRIBE %d:%b";
    status = async_redis_subscribe_context_->RedisAsyncCommand(
        reinterpret_cast<redisCallbackFn *>(&GlobalRedisCallback),
        reinterpret_cast<void *>(callback_index),
        redis_command.c_str(),
        pubsub_channel,
        node_id.Data(),
        node_id.Size());
  }

  return status;
}

Status RedisContext::SubscribeAsync(const std::string &channel,
                                    const RedisCallback &redisCallback,
                                    int64_t callback_index) {
  RAY_CHECK(async_redis_subscribe_context_);

  RAY_UNUSED(RedisCallbackManager::instance().AddCallback(
      redisCallback, true, io_service_, callback_index));
  std::string redis_command = "SUBSCRIBE %b";
  return async_redis_subscribe_context_->RedisAsyncCommand(
      reinterpret_cast<redisCallbackFn *>(&GlobalRedisCallback),
      reinterpret_cast<void *>(callback_index),
      redis_command.c_str(),
      channel.c_str(),
      channel.size());
}

Status RedisContext::UnsubscribeAsync(const std::string &channel) {
  RAY_CHECK(async_redis_subscribe_context_);

  std::string redis_command = "UNSUBSCRIBE %b";
  return async_redis_subscribe_context_->RedisAsyncCommand(
      reinterpret_cast<redisCallbackFn *>(&GlobalRedisCallback),
      reinterpret_cast<void *>(-1),
      redis_command.c_str(),
      channel.c_str(),
      channel.size());
}

Status RedisContext::PSubscribeAsync(const std::string &pattern,
                                     const RedisCallback &redisCallback,
                                     int64_t callback_index) {
  RAY_CHECK(async_redis_subscribe_context_);

  RAY_UNUSED(RedisCallbackManager::instance().AddCallback(
      redisCallback, true, io_service_, callback_index));
  std::string redis_command = "PSUBSCRIBE %b";
  return async_redis_subscribe_context_->RedisAsyncCommand(
      reinterpret_cast<redisCallbackFn *>(&GlobalRedisCallback),
      reinterpret_cast<void *>(callback_index),
      redis_command.c_str(),
      pattern.c_str(),
      pattern.size());
}

Status RedisContext::PUnsubscribeAsync(const std::string &pattern) {
  RAY_CHECK(async_redis_subscribe_context_);

  std::string redis_command = "PUNSUBSCRIBE %b";
  return async_redis_subscribe_context_->RedisAsyncCommand(
      reinterpret_cast<redisCallbackFn *>(&GlobalRedisCallback),
      reinterpret_cast<void *>(-1),
      redis_command.c_str(),
      pattern.c_str(),
      pattern.size());
}

Status RedisContext::PublishAsync(const std::string &channel,
                                  const std::string &message,
                                  const RedisCallback &redisCallback) {
  std::vector<std::string> args = {"PUBLISH", channel, message};
  return RunArgvAsync(args, redisCallback);
}

void RedisContext::FreeRedisReply(void *reply) { return freeReplyObject(reply); }

int RedisContext::GetRedisError(redisContext *context) { return context->err; }

}  // namespace gcs

}  // namespace ray
