#include "ray/gcs/redis_context.h"

#include <unistd.h>

#include <sstream>

#include "ray/stats/stats.h"
#include "ray/util/util.h"

extern "C" {
#include "ray/thirdparty/hiredis/adapters/ae.h"
#include "ray/thirdparty/hiredis/async.h"
#include "ray/thirdparty/hiredis/hiredis.h"
}

// TODO(pcm): Integrate into the C++ tree.
#include "ray/ray_config.h"

namespace {

/// A helper function to call the callback and delete it from the callback
/// manager if necessary.
void ProcessCallback(int64_t callback_index, const std::string &data) {
  RAY_CHECK(callback_index >= 0) << "The callback index must be greater than 0, "
                                 << "but it actually is " << callback_index;
  auto callback_item = ray::gcs::RedisCallbackManager::instance().get(callback_index);
  if (!callback_item.is_subscription) {
    // Record the redis latency for non-subscription redis operations.
    auto end_time = current_sys_time_us();
    ray::stats::RedisLatency().Record(end_time - callback_item.start_time);
  }
  // Invoke the callback.
  if (callback_item.callback != nullptr) {
    callback_item.callback(data);
  }
  if (!callback_item.is_subscription) {
    // Delete the callback if it's not a subscription callback.
    ray::gcs::RedisCallbackManager::instance().remove(callback_index);
  }
}

}  // namespace

namespace ray {

namespace gcs {

// This is a global redis callback which will be registered for every
// asynchronous redis call. It dispatches the appropriate callback
// that was registered with the RedisCallbackManager.
void GlobalRedisCallback(void *c, void *r, void *privdata) {
  if (r == nullptr) {
    return;
  }
  int64_t callback_index = reinterpret_cast<int64_t>(privdata);
  redisReply *reply = reinterpret_cast<redisReply *>(r);
  std::string data = "";
  // Parse the response.
  switch (reply->type) {
  case (REDIS_REPLY_NIL): {
    // Do not add any data for a nil response.
  } break;
  case (REDIS_REPLY_STRING): {
    data = std::string(reply->str, reply->len);
  } break;
  case (REDIS_REPLY_STATUS): {
  } break;
  case (REDIS_REPLY_ERROR): {
    RAY_LOG(FATAL) << "Redis error: " << reply->str;
  } break;
  case (REDIS_REPLY_INTEGER): {
    data = std::to_string(reply->integer);
    break;
  }
  default:
    RAY_LOG(FATAL) << "Fatal redis error of type " << reply->type << " and with string "
                   << reply->str;
  }
  ProcessCallback(callback_index, data);
}

void SubscribeRedisCallback(void *c, void *r, void *privdata) {
  if (r == nullptr) {
    return;
  }
  int64_t callback_index = reinterpret_cast<int64_t>(privdata);
  redisReply *reply = reinterpret_cast<redisReply *>(r);
  std::string data = "";
  // Parse the response.
  switch (reply->type) {
  case (REDIS_REPLY_ARRAY): {
    // Parse the published message.
    redisReply *message_type = reply->element[0];
    if (strcmp(message_type->str, "subscribe") == 0) {
      // If the message is for the initial subscription call, return the empty
      // string as a response to signify that subscription was successful.
    } else if (strcmp(message_type->str, "message") == 0) {
      // If the message is from a PUBLISH, make sure the data is nonempty.
      redisReply *message = reply->element[reply->elements - 1];
      auto notification = std::string(message->str, message->len);
      RAY_CHECK(!notification.empty()) << "Empty message received on subscribe channel";
      data = notification;
    } else {
      RAY_LOG(FATAL) << "Fatal redis error during subscribe" << message_type->str;
    }

  } break;
  case (REDIS_REPLY_ERROR): {
    RAY_LOG(FATAL) << "Redis error: " << reply->str;
  } break;
  default:
    RAY_LOG(FATAL) << "Fatal redis error of type " << reply->type << " and with string "
                   << reply->str;
  }
  ProcessCallback(callback_index, data);
}

int64_t RedisCallbackManager::add(const RedisCallback &function, bool is_subscription) {
  auto start_time = current_sys_time_us();
  callback_items_.emplace(num_callbacks_,
                          CallbackItem(function, is_subscription, start_time));
  return num_callbacks_++;
}

RedisCallbackManager::CallbackItem &RedisCallbackManager::get(int64_t callback_index) {
  RAY_CHECK(callback_items_.find(callback_index) != callback_items_.end());
  return callback_items_[callback_index];
}

void RedisCallbackManager::remove(int64_t callback_index) {
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
  if (async_context_) {
    redisAsyncFree(async_context_);
  }
  if (subscribe_context_) {
    redisAsyncFree(subscribe_context_);
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
  RAY_CHECK_OK(ConnectWithRetries(address, port, redisConnect, &context_));
  RAY_CHECK_OK(AuthenticateRedis(context_, password));

  redisReply *reply = reinterpret_cast<redisReply *>(
      redisCommand(context_, "CONFIG SET notify-keyspace-events Kl"));
  REDIS_CHECK_ERROR(context_, reply);
  freeReplyObject(reply);

  // Connect to async context
  RAY_CHECK_OK(ConnectWithRetries(address, port, redisAsyncConnect, &async_context_));
  RAY_CHECK_OK(AuthenticateRedis(async_context_, password));

  // Connect to subscribe context
  RAY_CHECK_OK(ConnectWithRetries(address, port, redisAsyncConnect, &subscribe_context_));
  RAY_CHECK_OK(AuthenticateRedis(subscribe_context_, password));

  return Status::OK();
}

Status RedisContext::AttachToEventLoop(aeEventLoop *loop) {
  if (redisAeAttach(loop, async_context_) != REDIS_OK ||
      redisAeAttach(loop, subscribe_context_) != REDIS_OK) {
    return Status::RedisError("could not attach redis event loop");
  } else {
    return Status::OK();
  }
}

Status RedisContext::RunAsync(const std::string &command, const UniqueID &id,
                              const uint8_t *data, int64_t length,
                              const TablePrefix prefix, const TablePubsub pubsub_channel,
                              RedisCallback redisCallback, int log_length) {
  int64_t callback_index = RedisCallbackManager::instance().add(redisCallback, false);
  if (length > 0) {
    if (log_length >= 0) {
      std::string redis_command = command + " %d %d %b %b %d";
      int status = redisAsyncCommand(
          async_context_, reinterpret_cast<redisCallbackFn *>(&GlobalRedisCallback),
          reinterpret_cast<void *>(callback_index), redis_command.c_str(), prefix,
          pubsub_channel, id.data(), id.size(), data, length, log_length);
      if (status == REDIS_ERR) {
        return Status::RedisError(std::string(async_context_->errstr));
      }
    } else {
      std::string redis_command = command + " %d %d %b %b";
      int status = redisAsyncCommand(
          async_context_, reinterpret_cast<redisCallbackFn *>(&GlobalRedisCallback),
          reinterpret_cast<void *>(callback_index), redis_command.c_str(), prefix,
          pubsub_channel, id.data(), id.size(), data, length);
      if (status == REDIS_ERR) {
        return Status::RedisError(std::string(async_context_->errstr));
      }
    }
  } else {
    RAY_CHECK(log_length == -1);
    std::string redis_command = command + " %d %d %b";
    int status = redisAsyncCommand(
        async_context_, reinterpret_cast<redisCallbackFn *>(&GlobalRedisCallback),
        reinterpret_cast<void *>(callback_index), redis_command.c_str(), prefix,
        pubsub_channel, id.data(), id.size());
    if (status == REDIS_ERR) {
      return Status::RedisError(std::string(async_context_->errstr));
    }
  }
  return Status::OK();
}

Status RedisContext::RunArgvAsync(const std::vector<std::string> &args) {
  // Build the arguments.
  std::vector<const char *> argv;
  std::vector<size_t> argc;
  for (size_t i = 0; i < args.size(); ++i) {
    argv.push_back(args[i].data());
    argc.push_back(args[i].size());
  }
  // Run the Redis command.
  int status;
  status = redisAsyncCommandArgv(async_context_, nullptr, nullptr, args.size(),
                                 argv.data(), argc.data());
  if (status == REDIS_ERR) {
    return Status::RedisError(std::string(async_context_->errstr));
  }
  return Status::OK();
}

Status RedisContext::SubscribeAsync(const ClientID &client_id,
                                    const TablePubsub pubsub_channel,
                                    const RedisCallback &redisCallback,
                                    int64_t *out_callback_index) {
  RAY_CHECK(pubsub_channel != TablePubsub::NO_PUBLISH)
      << "Client requested subscribe on a table that does not support pubsub";

  int64_t callback_index = RedisCallbackManager::instance().add(redisCallback, true);
  RAY_CHECK(out_callback_index != nullptr);
  *out_callback_index = callback_index;
  int status = 0;
  if (client_id.is_nil()) {
    // Subscribe to all messages.
    std::string redis_command = "SUBSCRIBE %d";
    status = redisAsyncCommand(
        subscribe_context_, reinterpret_cast<redisCallbackFn *>(&SubscribeRedisCallback),
        reinterpret_cast<void *>(callback_index), redis_command.c_str(), pubsub_channel);
  } else {
    // Subscribe only to messages sent to this client.
    std::string redis_command = "SUBSCRIBE %d:%b";
    status = redisAsyncCommand(
        subscribe_context_, reinterpret_cast<redisCallbackFn *>(&SubscribeRedisCallback),
        reinterpret_cast<void *>(callback_index), redis_command.c_str(), pubsub_channel,
        client_id.data(), client_id.size());
  }

  if (status == REDIS_ERR) {
    return Status::RedisError(std::string(subscribe_context_->errstr));
  }
  return Status::OK();
}

}  // namespace gcs

}  // namespace ray
