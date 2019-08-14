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
/// RedisAsyncContext class is a C++ wrapper of hiredis asyncRedisContext.
/// The purpose of this class is to make redis async commands thread-safe by
/// locking.
/// Sending a redis request is a low frequency operation,
/// so even in the case of multithreading, the lock conflict should be low frequency.
/// Raylet has only one thread, so there won't be lock competition.
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
    // redisAsyncContext redisReader's read operations are always performed
    // in the socket thread, so no lock here.
    redisAsyncHandleRead(redis_async_context_);
  }

  /// Perform command 'redisAsyncHandleWrite'. Thread-safe.
  void RedisAsyncHandleWrite() {
    // Lock the obuf of redisContext for reading and writing.
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
