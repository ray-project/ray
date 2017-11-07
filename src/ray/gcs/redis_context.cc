// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "ray/gcs/redis_context.h"

#include <unistd.h>

#include "hiredis/async.h"
#include "hiredis/hiredis.h"

namespace ray {

namespace gcs {

void GlobalRedisCallback(void* context, void* r, void* privdata) {
  int64_t callback_index = reinterpret_cast<int64_t>(privdata);
  RedisCallbackManager::instance().get(callback_index)();
}

int64_t RedisCallbackManager::add(const std::function<void(void)>& function) {
  callbacks_.emplace(num_callbacks, std::unique_ptr<RedisCallback>(new RedisCallback(function)));
  return num_callbacks++;
}

RedisCallbackManager::RedisCallback& RedisCallbackManager::get(int64_t callback_index) {
  return *callbacks_[callback_index];
}

constexpr int64_t kRedisConnectionAttempts = 50;
constexpr int64_t kConnectTimeoutMillisecs = 100;

#define REDIS_CHECK_ERROR(CONTEXT, REPLY) \
  if (REPLY == nullptr || REPLY->type == REDIS_REPLY_ERROR) { \
    return Status::RedisError(CONTEXT->errstr); \
  }

RedisContext::~RedisContext() {
  if (context_) {
    redisFree(context_);
  }
  if (async_context_) {
    redisAsyncDisconnect(async_context_);
    redisAsyncFree(async_context_);
  }
}

Status RedisContext::Connect(const std::string& address, int port) {
  int connection_attempts = 0;
  context_ = redisConnect(address.c_str(), port);
  while (context_ == nullptr || context_->err) {
    if (connection_attempts >= kRedisConnectionAttempts) {
      if (context_ == nullptr) {
        RAY_LOG(FATAL) << "Could not allocate redis context.";
      }
      if (context_->err) {
        RAY_LOG(FATAL) << "Could not establish connection to redis " << address << ":" << port;
      }
      break;
    }
    RAY_LOG(WARNING) << "Failed to connect to Redis, retrying.";
    // Sleep for a little.
    usleep(kConnectTimeoutMillisecs * 1000);
    context_ = redisConnect(address.c_str(), port);
    connection_attempts += 1;
  }
  redisReply *reply = reinterpret_cast<redisReply*>(
    redisCommand(context_, "CONFIG SET notify-keyspace-events Kl"));
  REDIS_CHECK_ERROR(context_, reply);

  // Connect to async context
  async_context_ = redisAsyncConnect(address.c_str(), port);
  if (async_context_ == nullptr || async_context_->err) {
    RAY_LOG(FATAL) << "Could not establish connection to redis " << address << ":" << port;
  }
  return Status::OK();
}

Status RedisContext::RunAsync(const std::string& command, const UniqueID& id, uint8_t* data, int64_t length, int64_t callback_index) {
  std::string redis_command = command + " %b %b";
  int status = redisAsyncCommand(async_context_, reinterpret_cast<redisCallbackFn *>(&GlobalRedisCallback),
                                 reinterpret_cast<void*>(callback_index), redis_command.c_str(), id.data(), id.size(),
                                 data, length);
  std::cout << "XXX status " << status << std::endl;
  return Status::OK();
}

}  // namespace gcs

}  // namespace ray
