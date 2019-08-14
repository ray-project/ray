#ifndef RAY_GCS_REDIS_ASYNC_CONTEXT_H
#define RAY_GCS_REDIS_ASYNC_CONTEXT_H

#include <stdarg.h>
#include <mutex>
#include "ray/common/status.h"

extern "C" {
#include "ray/thirdparty/hiredis/async.h"
#include "ray/thirdparty/hiredis/hiredis.h"
}

namespace ray {

namespace gcs {

/// \class RedisAsyncContext
/// RedisAsyncContext class is a wrapper of hiredis `asyncRedisContext`, providing
/// C++ style and thread-safe API.
/// NOTE(micafan): We use a lock to make the `asyncRedisContext` commands thread-safe.
/// All these commands only manipulate memory data and don't actually do any IO
/// operations. So the perf impact of adding the lock should be minimum.
class RedisAsyncContext {
 public:
  explicit RedisAsyncContext(redisAsyncContext *redis_async_context)
      : redis_async_context_(redis_async_context) {
    RAY_CHECK(redis_async_context_ != nullptr);
  }

  ~RedisAsyncContext() {
    redisAsyncFree(redis_async_context_);
    redis_async_context_ = nullptr;
  }

  /// Get the raw 'redisAsyncContext' pointer.
  redisAsyncContext *GetRawRedisAsyncContext() {
    RAY_CHECK(redis_async_context_ != nullptr);
    return redis_async_context_;
  }

  /// Perform command 'redisAsyncHandleRead'. Thread-safe.
  void RedisAsyncHandleRead() {
    // `redisAsyncHandleRead` is already thread-safe, so no lock here.
    redisAsyncHandleRead(redis_async_context_);
  }

  /// Perform command 'redisAsyncHandleWrite'. Thread-safe.
  void RedisAsyncHandleWrite() {
    // `redisAsyncHandleWrite` will mutate `redis_async_context_`, use a lock to protect
    // it.
    std::lock_guard<std::mutex> lock(mutex_);
    redisAsyncHandleWrite(redis_async_context_);
  }

  /// Perform command 'redisvAsyncCommand'. Thread-safe.
  Status RedisAsyncCommand(redisCallbackFn *fn, void *privdata, const char *format, ...);

  /// Perform command 'redisAsyncCommandArgv'. Thread-safe.
  Status RedisAsyncCommandArgv(redisCallbackFn *fn, void *privdata, int argc,
                               const char **argv, const size_t *argvlen);

 private:
  std::mutex mutex_;
  redisAsyncContext *redis_async_context_{nullptr};
};

}  // namespace gcs

}  // namespace ray

#endif  // RAY_GCS_REDIS_ASYNC_CONTEXT_H
