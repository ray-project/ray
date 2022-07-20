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

#pragma once

#include <stdarg.h>

#include <mutex>

#include "ray/common/status.h"

// These are forward declarations from hiredis.
extern "C" {
struct redisAsyncContext;
struct redisReply;
typedef void redisCallbackFn(struct redisAsyncContext *, void *, void *);
}

namespace ray {

namespace gcs {

/// \class RedisAsyncContext
/// RedisAsyncContext class is a wrapper of hiredis `asyncRedisContext`, providing
/// C++ style and thread-safe API.
class RedisAsyncContext {
 public:
  explicit RedisAsyncContext(redisAsyncContext *redis_async_context);

  ~RedisAsyncContext();

  /// Get the raw 'redisAsyncContext' pointer.
  ///
  /// \return redisAsyncContext *
  redisAsyncContext *GetRawRedisAsyncContext();

  /// Reset the raw 'redisAsyncContext' pointer to nullptr.
  void ResetRawRedisAsyncContext();

  /// Perform command 'redisAsyncHandleRead'. Thread-safe.
  void RedisAsyncHandleRead();

  /// Perform command 'redisAsyncHandleWrite'. Thread-safe.
  void RedisAsyncHandleWrite();

  /// Perform command 'redisvAsyncCommand'. Thread-safe.
  ///
  /// \param fn Callback that will be called after the command finishes.
  /// \param privdata User-defined pointer.
  /// \param format Command format.
  /// \param ... Command list.
  /// \return Status
  Status RedisAsyncCommand(redisCallbackFn *fn, void *privdata, const char *format, ...);

  /// Perform command 'redisAsyncCommandArgv'. Thread-safe.
  ///
  /// \param fn Callback that will be called after the command finishes.
  /// \param privdata User-defined pointer.
  /// \param argc Number of arguments.
  /// \param argv Array with arguments.
  /// \param argvlen Array with each argument's length.
  /// \return Status
  Status RedisAsyncCommandArgv(redisCallbackFn *fn,
                               void *privdata,
                               int argc,
                               const char **argv,
                               const size_t *argvlen);

 private:
  /// This mutex is used to protect `redis_async_context`.
  /// NOTE(micafan): All the `redisAsyncContext`-related functions only manipulate memory
  /// data and don't actually do any IO operations. So the perf impact of adding the lock
  /// should be minimum.
  std::mutex mutex_;
  redisAsyncContext *redis_async_context_{nullptr};
};

}  // namespace gcs

}  // namespace ray
