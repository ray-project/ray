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

#include <boost/asio.hpp>
#include <boost/asio/error.hpp>
#include <boost/bind/bind.hpp>
#include <memory>
#include <mutex>

#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/status.h"

// These are forward declarations from hiredis.
extern "C" {
struct redisAsyncContext;
struct redisReply;
typedef void redisCallbackFn(struct redisAsyncContext *, void *, void *);

#include "hiredis/async.h"
#include "hiredis/hiredis.h"
}

namespace ray {
namespace gcs {
// Adaptor callback functions for hiredis redis_async_context->ev
void CallbackAddRead(void *);
void CallbackDelRead(void *);
void CallbackAddWrite(void *);
void CallbackDelWrite(void *);
void CallbackCleanup(void *);

struct RedisContextDeleter {
  RedisContextDeleter(){};

  void operator()(redisContext *context) { redisFree(context); }
  void operator()(redisAsyncContext *context) { redisAsyncFree(context); }
};

/// \class RedisAsyncContext
/// RedisAsyncContext class is a wrapper of hiredis `asyncRedisContext`, providing
/// C++ style and thread-safe API.
class RedisAsyncContext {
 public:
  /// Constructor of RedisAsyncContext.
  /// Use single-threaded io_service as event loop (because the redis commands
  /// that will run in the event loop are non-thread safe).
  ///
  /// \param io_service The single-threaded event loop for this client.
  /// \param redis_async_context The raw redis async context used to execute redis
  /// commands.
  explicit RedisAsyncContext(
      instrumented_io_context &io_service,
      std::unique_ptr<redisAsyncContext, RedisContextDeleter> redis_async_context);

  /// Get the raw 'redisAsyncContext' pointer.
  ///
  /// \return redisAsyncContext *
  redisAsyncContext *GetRawRedisAsyncContext();

  /// Reset the raw 'redisAsyncContext' pointer to nullptr.
  void ResetRawRedisAsyncContext();

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
  std::unique_ptr<redisAsyncContext, RedisContextDeleter> redis_async_context_;

  instrumented_io_context &io_service_;
  boost::asio::ip::tcp::socket socket_;
  // Hiredis wanted to add a read operation to the event loop
  // but the read might not have happened yet
  bool read_requested_{false};
  // Hiredis wanted to add a write operation to the event loop
  // but the read might not have happened yet
  bool write_requested_{false};
  // A read is currently in progress
  bool read_in_progress_{false};
  // A write is currently in progress
  bool write_in_progress_{false};

  /// Issue async socket operations depending on the state of the redis async context.
  void Operate();
  /// The callback function for socket operations
  ///
  /// \param error_code The error code of the socket operation.
  /// \param write true if it is a write operation, false otherwise.
  void HandleIo(boost::system::error_code error_code, bool write);

  // Real callback functions bound to RedisAsyncContext
  void AddRead();
  void DelRead();
  void AddWrite();
  void DelWrite();
  void Cleanup();

  friend void CallbackAddRead(void *);
  friend void CallbackDelRead(void *);
  friend void CallbackAddWrite(void *);
  friend void CallbackDelWrite(void *);
  friend void CallbackCleanup(void *);
};
}  // namespace gcs
}  // namespace ray
