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
  Status RedisAsyncCommandArgv(redisCallbackFn *fn, void *privdata, int argc,
                               const char **argv, const size_t *argvlen);

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

#endif  // RAY_GCS_REDIS_ASYNC_CONTEXT_H
