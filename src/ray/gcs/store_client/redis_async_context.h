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
#include <functional>
#include <memory>
#include <mutex>

#include "ray/asio/instrumented_io_context.h"
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

  ~RedisAsyncContext();

  /// Get the raw 'redisAsyncContext' pointer.
  ///
  /// \return redisAsyncContext *
  redisAsyncContext *GetRawRedisAsyncContext();

  /// Reset the raw 'redisAsyncContext' pointer to nullptr.
  void ResetRawRedisAsyncContext();

  /// Rebind this object to a freshly connected raw 'redisAsyncContext'.
  ///
  /// The object's own address is preserved, which matters because in-flight
  /// `RedisRequestContext`s hold a raw pointer to it. Recreating the
  /// `RedisAsyncContext` instead (as `RedisContext::Connect` does) would leave
  /// those pointers dangling, which is what blocked an in-place reconnect.
  ///
  /// \param redis_async_context An already-connected raw context to adopt.
  void Reset(std::unique_ptr<redisAsyncContext, RedisContextDeleter> redis_async_context);

  /// Whether a live raw context is currently attached. Test-only: the answer
  /// can be stale as soon as the lock is dropped, so production code should
  /// just issue the command and handle Status::Disconnected.
  bool IsConnected();

  /// Set a handler invoked when hiredis reports the connection was lost.
  ///
  /// The handler runs on the io_service thread from inside hiredis' teardown,
  /// so it must not reconnect synchronously.
  void SetDisconnectHandler(std::function<void()> handler);

  /// Invoke the disconnect handler, if one is set. Called by the hiredis
  /// disconnect callback after the raw context has been released.
  void NotifyDisconnected();

  /// Set a handler invoked when hiredis reports the connection came up.
  ///
  /// `redisAsyncConnect` is non-blocking, so a context that looks healthy may
  /// still be mid-handshake. Only this handler proves the connection works.
  void SetConnectHandler(std::function<void()> handler);

  /// Invoke the connect handler, if one is set.
  void NotifyConnected();

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

  /// Adopt `redis_async_context` and wire it up to `socket_` and the hiredis
  /// event hooks. `mutex_` must be held.
  void AttachLocked(
      std::unique_ptr<redisAsyncContext, RedisContextDeleter> redis_async_context);

  instrumented_io_context &io_service_;
  boost::asio::ip::tcp::socket socket_;
  /// Invoked when hiredis reports the connection was lost. Set once at
  /// construction time by RedisContext, so it needs no lock.
  std::function<void()> disconnect_handler_;
  /// Invoked when hiredis reports the connection is up. Set once at
  /// construction time by RedisContext, so it needs no lock.
  std::function<void()> connect_handler_;
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
  /// Bumped every time a new raw context, and with it a new socket, is
  /// adopted. A socket operation queued against an earlier socket must not
  /// touch the flags above once they describe a different connection.
  uint64_t socket_generation_{0};
  /// Sentinel letting a queued socket operation notice that this object was
  /// destroyed before its handler ran. Registering a hiredis connect callback
  /// arms a write wait immediately, and RedisContext::Connect tears the
  /// context down again as soon as it learns it was talking to a Sentinel.
  std::shared_ptr<bool> alive_ = std::make_shared<bool>(true);

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
