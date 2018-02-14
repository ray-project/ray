#include "ray/gcs/redis_context.h"

#include <unistd.h>

extern "C" {
#include "hiredis/async.h"
#include "hiredis/hiredis.h"
#include "hiredis/adapters/ae.h"
}

// TODO(pcm): Integrate into the C++ tree.
#include "state/ray_config.h"

namespace ray {

namespace gcs {

// This is a global redis callback which will be registered for every
// asynchronous redis call. It dispatches the appropriate callback
// that was registered with the RedisCallbackManager.
void GlobalRedisCallback(void *c, void *r, void *privdata) {
  if (r == NULL) {
    return;
  }
  int64_t callback_index = reinterpret_cast<int64_t>(privdata);
  redisReply *reply = reinterpret_cast<redisReply *>(r);
  std::string data = "";
  if (reply->type == REDIS_REPLY_NIL) {
  } else if (reply->type == REDIS_REPLY_STRING) {
    data = std::string(reply->str, reply->len);
  } else if (reply->type == REDIS_REPLY_STATUS) {
  } else if (reply->type == REDIS_REPLY_ERROR) {
    RAY_LOG(ERROR) << "Redis error " << reply->str;
  } else {
    RAY_LOG(FATAL) << "Fatal redis error of type " << reply->type
                   << " and with string " << reply->str;
  }
  RedisCallbackManager::instance().get(callback_index)(data);
}

int64_t RedisCallbackManager::add(const RedisCallback &function) {
  callbacks_.emplace(num_callbacks, std::unique_ptr<RedisCallback>(
                                        new RedisCallback(function)));
  return num_callbacks++;
}

RedisCallbackManager::RedisCallback &RedisCallbackManager::get(
    int64_t callback_index) {
  return *callbacks_[callback_index];
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
}

Status RedisContext::Connect(const std::string &address, int port) {
  int connection_attempts = 0;
  context_ = redisConnect(address.c_str(), port);
  while (context_ == nullptr || context_->err) {
    if (connection_attempts >=
        RayConfig::instance().redis_db_connect_retries()) {
      if (context_ == nullptr) {
        RAY_LOG(FATAL) << "Could not allocate redis context.";
      }
      if (context_->err) {
        RAY_LOG(FATAL) << "Could not establish connection to redis " << address
                       << ":" << port;
      }
      break;
    }
    RAY_LOG(WARNING) << "Failed to connect to Redis, retrying.";
    // Sleep for a little.
    usleep(RayConfig::instance().redis_db_connect_wait_milliseconds() * 1000);
    context_ = redisConnect(address.c_str(), port);
    connection_attempts += 1;
  }
  redisReply *reply = reinterpret_cast<redisReply *>(
      redisCommand(context_, "CONFIG SET notify-keyspace-events Kl"));
  REDIS_CHECK_ERROR(context_, reply);
  freeReplyObject(reply);

  // Connect to async context
  async_context_ = redisAsyncConnect(address.c_str(), port);
  if (async_context_ == nullptr || async_context_->err) {
    RAY_LOG(FATAL) << "Could not establish connection to redis " << address
                   << ":" << port;
  }
  return Status::OK();
}

Status RedisContext::AttachToEventLoop(aeEventLoop *loop) {
  if (redisAeAttach(loop, async_context_) != REDIS_OK) {
    return Status::RedisError("could not attach redis event loop");
  } else {
    return Status::OK();
  }
}

Status RedisContext::RunAsync(const std::string &command,
                              const UniqueID &id,
                              uint8_t *data,
                              int64_t length,
                              int64_t callback_index) {
  if (length > 0) {
    std::string redis_command = command + " %b %b";
    int status = redisAsyncCommand(
        async_context_,
        reinterpret_cast<redisCallbackFn *>(&GlobalRedisCallback),
        reinterpret_cast<void *>(callback_index), redis_command.c_str(),
        id.data(), id.size(), data, length);
    if (status == REDIS_ERR) {
      return Status::RedisError(std::string(async_context_->errstr));
    }
  } else {
    std::string redis_command = command + " %b";
    int status = redisAsyncCommand(
        async_context_,
        reinterpret_cast<redisCallbackFn *>(&GlobalRedisCallback),
        reinterpret_cast<void *>(callback_index), redis_command.c_str(),
        id.data(), id.size());
    if (status == REDIS_ERR) {
      return Status::RedisError(std::string(async_context_->errstr));
    }
  }
  return Status::OK();
}

}  // namespace gcs

}  // namespace ray
