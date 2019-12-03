#include "ray/gcs/redis_context.h"

#include <unistd.h>

#include <sstream>

#include "ray/stats/stats.h"
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
  auto callback_item = ray::gcs::RedisCallbackManager::instance().get(callback_index);
  if (!callback_item->is_subscription_) {
    // Record the redis latency for non-subscription redis operations.
    auto end_time = absl::GetCurrentTimeNanos() / 1000;
    ray::stats::RedisLatency().Record(end_time - callback_item->start_time_);
  }

  // Dispatch the callback.
  callback_item->Dispatch(callback_reply);

  if (!callback_item->is_subscription_) {
    // Delete the callback if it's not a subscription callback.
    ray::gcs::RedisCallbackManager::instance().remove(callback_index);
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
    // Array replies are only used for pub-sub messages. Parse the published message.
    redisReply *message_type = redis_reply->element[0];
    if (strcmp(message_type->str, "subscribe") == 0) {
      // If the message is for the initial subscription call, return the empty
      // string as a response to signify that subscription was successful.
    } else if (strcmp(message_type->str, "message") == 0) {
      // If the message is from a PUBLISH, make sure the data is nonempty.
      redisReply *message = redis_reply->element[redis_reply->elements - 1];
      // data is a notification message.
      string_reply_ = std::string(message->str, message->len);
      RAY_CHECK(!string_reply_.empty()) << "Empty message received on subscribe channel.";
    } else {
      RAY_LOG(FATAL) << "This is not a pubsub reply: data=" << message_type->str;
    }
    break;
  }
  default: {
    RAY_LOG(WARNING) << "Encountered unexpected redis reply type: " << reply_type_;
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

std::string CallbackReply::ReadAsString() const {
  RAY_CHECK(reply_type_ == REDIS_REPLY_STRING) << "Unexpected type: " << reply_type_;
  return string_reply_;
}

std::string CallbackReply::ReadAsPubsubData() const {
  RAY_CHECK(reply_type_ == REDIS_REPLY_ARRAY) << "Unexpected type: " << reply_type_;
  return string_reply_;
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

int64_t RedisCallbackManager::add(const RedisCallback &function, bool is_subscription,
                                  boost::asio::io_service &io_service) {
  auto start_time = absl::GetCurrentTimeNanos() / 1000;

  std::lock_guard<std::mutex> lock(mutex_);
  callback_items_.emplace(
      num_callbacks_,
      std::make_shared<CallbackItem>(function, is_subscription, start_time, io_service));
  return num_callbacks_++;
}

std::shared_ptr<RedisCallbackManager::CallbackItem> RedisCallbackManager::get(
    int64_t callback_index) {
  std::lock_guard<std::mutex> lock(mutex_);
  RAY_CHECK(callback_items_.find(callback_index) != callback_items_.end());
  return callback_items_[callback_index];
}

void RedisCallbackManager::remove(int64_t callback_index) {
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

template <typename RedisContext, typename RedisConnectFunction>
Status ConnectWithRetries(const std::string &address, int port,
                          const RedisConnectFunction &connect_function,
                          RedisContext **context) {
  int connection_attempts = 0;
  *context = connect_function(address.c_str(), port);
  while (*context == nullptr || (*context)->err) {
    if (connection_attempts >= RayConfig::instance().redis_db_connect_retries()) {
      if (*context == nullptr) {
        RAY_LOG(FATAL) << "Could not allocate redis context.";
      }
      if ((*context)->err) {
        RAY_LOG(FATAL) << "Could not establish connection to redis " << address << ":"
                       << port << " (context.err = " << (*context)->err << ")";
      }
      break;
    }
    RAY_LOG(WARNING) << "Failed to connect to Redis, retrying.";
    // Sleep for a little.
    usleep(RayConfig::instance().redis_db_connect_wait_milliseconds() * 1000);
    *context = connect_function(address.c_str(), port);
    connection_attempts += 1;
  }
  return Status::OK();
}

Status RedisContext::Connect(const std::string &address, int port, bool sharding,
                             const std::string &password = "") {
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

  // Connect to subscribe context
  redisAsyncContext *subscribe_context = nullptr;
  RAY_CHECK_OK(ConnectWithRetries(address, port, redisAsyncConnect, &subscribe_context));
  RAY_CHECK_OK(AuthenticateRedis(subscribe_context, password));
  async_redis_subscribe_context_.reset(new RedisAsyncContext(subscribe_context));
  SetDisconnectCallback(async_redis_subscribe_context_.get());

  return Status::OK();
}

Status RedisContext::RunArgvAsync(const std::vector<std::string> &args) {
  RAY_CHECK(redis_async_context_);
  // Build the arguments.
  std::vector<const char *> argv;
  std::vector<size_t> argc;
  for (size_t i = 0; i < args.size(); ++i) {
    argv.push_back(args[i].data());
    argc.push_back(args[i].size());
  }
  // Run the Redis command.
  Status status = redis_async_context_->RedisAsyncCommandArgv(
      nullptr, nullptr, args.size(), argv.data(), argc.data());
  return status;
}

Status RedisContext::SubscribeAsync(const ClientID &client_id,
                                    const TablePubsub pubsub_channel,
                                    const RedisCallback &redisCallback,
                                    int64_t *out_callback_index) {
  RAY_CHECK(pubsub_channel != TablePubsub::NO_PUBLISH)
      << "Client requested subscribe on a table that does not support pubsub";
  RAY_CHECK(async_redis_subscribe_context_);

  int64_t callback_index =
      RedisCallbackManager::instance().add(redisCallback, true, io_service_);
  RAY_CHECK(out_callback_index != nullptr);
  *out_callback_index = callback_index;
  Status status = Status::OK();
  if (client_id.IsNil()) {
    // Subscribe to all messages.
    std::string redis_command = "SUBSCRIBE %d";
    status = async_redis_subscribe_context_->RedisAsyncCommand(
        reinterpret_cast<redisCallbackFn *>(&GlobalRedisCallback),
        reinterpret_cast<void *>(callback_index), redis_command.c_str(), pubsub_channel);
  } else {
    // Subscribe only to messages sent to this client.
    std::string redis_command = "SUBSCRIBE %d:%b";
    status = async_redis_subscribe_context_->RedisAsyncCommand(
        reinterpret_cast<redisCallbackFn *>(&GlobalRedisCallback),
        reinterpret_cast<void *>(callback_index), redis_command.c_str(), pubsub_channel,
        client_id.Data(), client_id.Size());
  }

  return status;
}

}  // namespace gcs

}  // namespace ray
