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

#include "ray/gcs/redis_async_context.h"

extern "C" {
#include "hiredis/async.h"
#include "hiredis/hiredis.h"
}

namespace ray {

namespace gcs {

RedisAsyncContext::RedisAsyncContext(redisAsyncContext *redis_async_context)
    : redis_async_context_(redis_async_context) {
  RAY_CHECK(redis_async_context_ != nullptr);
}

RedisAsyncContext::~RedisAsyncContext() {
  if (redis_async_context_ != nullptr) {
    redisAsyncFree(redis_async_context_);
    redis_async_context_ = nullptr;
  }
}

redisAsyncContext *RedisAsyncContext::GetRawRedisAsyncContext() {
  return redis_async_context_;
}

void RedisAsyncContext::ResetRawRedisAsyncContext() {
  // Reset redis_async_context_ to nullptr because hiredis has released this context.
  redis_async_context_ = nullptr;
}

void RedisAsyncContext::RedisAsyncHandleRead() {
  // `redisAsyncHandleRead` will mutate `redis_async_context_`, use a lock to protect
  // it.
  // This function will execute the callbacks which are registered by
  // `redisvAsyncCommand`, `redisAsyncCommandArgv` and so on.
  std::lock_guard<std::mutex> lock(mutex_);
  // TODO(mehrdadn): Remove this when the bug is resolved.
  // Somewhat consistently reproducible via
  // python/ray/tests/test_basic.py::test_background_tasks_with_max_calls
  // with -c opt on Windows.
  RAY_CHECK(redis_async_context_) << "redis_async_context_ must not be NULL here";
  redisAsyncHandleRead(redis_async_context_);
}

void RedisAsyncContext::RedisAsyncHandleWrite() {
  // `redisAsyncHandleWrite` will mutate `redis_async_context_`, use a lock to protect
  // it.
  std::lock_guard<std::mutex> lock(mutex_);
  redisAsyncHandleWrite(redis_async_context_);
}

Status RedisAsyncContext::RedisAsyncCommand(redisCallbackFn *fn,
                                            void *privdata,
                                            const char *format,
                                            ...) {
  va_list ap;
  va_start(ap, format);

  int ret_code = 0;
  {
    // `redisvAsyncCommand` will mutate `redis_async_context_`, use a lock to protect it.
    std::lock_guard<std::mutex> lock(mutex_);
    if (!redis_async_context_) {
      return Status::Disconnected("Redis is disconnected");
    }
    ret_code = redisvAsyncCommand(redis_async_context_, fn, privdata, format, ap);
  }

  va_end(ap);

  if (ret_code == REDIS_ERR) {
    return Status::RedisError(std::string(redis_async_context_->errstr));
  }
  RAY_CHECK(ret_code == REDIS_OK);
  return Status::OK();
}

Status RedisAsyncContext::RedisAsyncCommandArgv(redisCallbackFn *fn,
                                                void *privdata,
                                                int argc,
                                                const char **argv,
                                                const size_t *argvlen) {
  int ret_code = 0;
  {
    // `redisAsyncCommandArgv` will mutate `redis_async_context_`, use a lock to protect
    // it.
    std::lock_guard<std::mutex> lock(mutex_);
    if (!redis_async_context_) {
      return Status::Disconnected("Redis is disconnected");
    }
    ret_code =
        redisAsyncCommandArgv(redis_async_context_, fn, privdata, argc, argv, argvlen);
  }

  if (ret_code == REDIS_ERR) {
    return Status::RedisError(std::string(redis_async_context_->errstr));
  }
  RAY_CHECK(ret_code == REDIS_OK);
  return Status::OK();
}

}  // namespace gcs

}  // namespace ray
