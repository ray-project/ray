#ifndef RAY_GCS_REDIS_ASYNC_CONTEXT_WRAPPER_H
#define RAY_GCS_REDIS_ASYNC_CONTEXT_WRAPPER_H

#include <stdarg.h>
#include <mutex>
#include "ray/common/status.h"

extern "C" {
#include "ray/thirdparty/hiredis/async.h"
#include "ray/thirdparty/hiredis/hiredis.h"
}

namespace ray {

namespace gcs {

/// \class RedisAsyncContextWrapper
/// RedisAsyncContextWrapper class make async execution of redis commands thread-safe by
/// locking.
class RedisAsyncContextWrapper {
 public:
  explicit RedisAsyncContextWrapper(redisAsyncContext *redis_async_context)
      : redis_async_context_(redis_async_context) {
    RAY_CHECK(redis_async_context_ != nullptr);
  }

  ~RedisAsyncContextWrapper() {}

  /// Free 'redisAsyncContext' which hold by this wrapper.
  /// If already setted disconnect callback when connect,
  /// 'redisAsyncContext' will be free by hiredis, then no need to call this function.
  void RedisAsyncFree() {
    if (redis_async_context_ != nullptr) {
      redisAsyncFree(redis_async_context_);
      redis_async_context_ = nullptr;
    }
  }

  /// Get mutable context.
  redisAsyncContext *MutableContext() {
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

#endif  // RAY_GCS_REDIS_ASYNC_CONTEXT_WRAPPER_H
