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

#include <atomic>
#include <boost/asio.hpp>
#include <boost/asio/error.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/bind/bind.hpp>
#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>

#include "absl/functional/function_ref.h"
#include "absl/time/time.h"
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

  /// Run `fn` on the raw context while holding the lock HandleIo takes. For a
  /// caller that mutates the context after the io_service may have started
  /// driving it on another thread. `fn` must not submit commands.
  void WithRawContextLocked(const std::function<void(redisAsyncContext *)> &fn);

  /// Rebind this object to a freshly connected raw 'redisAsyncContext'.
  ///
  /// The object's own address is preserved, which matters because in-flight
  /// `RedisRequestContext`s hold a raw pointer to it. Recreating the
  /// `RedisAsyncContext` instead (as `RedisContext::Connect` does) would leave
  /// those pointers dangling.
  ///
  /// The caller must have finished every mutation of the raw context (its
  /// `data` pointer, connect/disconnect callbacks, queued commands) before
  /// calling this: once published here, other threads may submit commands on
  /// it. Because hiredis arms its first write while those callbacks are being
  /// registered, before our event hooks exist, Reset() arms it again itself.
  ///
  /// Until the connect completes, Reset() also checks it every
  /// kConnectCheckInterval and gives up after
  /// redis_reconnect_connect_timeout_ms, reporting a failed connect through the
  /// connect callback.
  ///
  /// \param redis_async_context A raw context to adopt, typically with its
  /// non-blocking connect still in progress.
  void Reset(std::unique_ptr<redisAsyncContext, RedisContextDeleter> redis_async_context);

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

  /// Token that expires when this object is destroyed. Freeing the raw
  /// context flushes its pending callbacks, and by then the event loop
  /// itself may be mid-destruction: a callback that would schedule more
  /// work checks this first and gives up instead.
  std::weak_ptr<bool> GetAliveToken() const { return alive_; }

  /// Deadline until which a command that failed without reaching Redis may be
  /// retried for free during the current outage. The first caller of an outage
  /// stamps it at `now + grace`; everyone after sees the same deadline, so
  /// every in-flight command expires together no matter when it was issued.
  /// Thread-safe.
  ///
  /// \param now The current time.
  /// \param grace The grace period, used only by the call that stamps it.
  /// \return The deadline of the current outage.
  absl::Time OutageDeadline(absl::Time now, absl::Duration grace);

  /// End the current outage, so the next one gets its own deadline.
  /// Thread-safe.
  void ClearOutage();

  /// Where the write watermark of this connection lives in Redis, and the
  /// value prefix that marks it as written by this process.
  struct WriteWatermark {
    std::string key;
    std::string field;
    std::string incarnation;
  };

  /// From now on, number every write submitted and, each time the output
  /// buffer is flushed, queue a marker write `HSET key field
  /// "<incarnation>:<n>"` behind it, where n is the last write queued. Redis
  /// runs a connection's commands in order and whatever it keeps after a
  /// crash, a restore or a failover is a prefix of what it ran, so if marker n
  /// survived, write n and every write before it did too.
  /// Marker 1 is queued here, ahead of every later write, and counts as
  /// acknowledged once Redis replies to it: from then on a reconnect check also
  /// covers what Redis held before this process wrote. If the connection is
  /// down or a reconnect check is pending, it is sent with the first flush
  /// after the check passes. Thread-safe.
  ///
  /// \param watermark Where to write the marker and what to prefix it with.
  void EnableWriteWatermark(WriteWatermark watermark);

  /// The watermark set by EnableWriteWatermark(), if any. Thread-safe.
  std::optional<WriteWatermark> GetWriteWatermark();

  /// Record that Redis replied to write `n`. Thread-safe.
  ///
  /// \param n The number EnableWriteWatermark() gave the write.
  void AckWrite(uint64_t n);

  /// The highest write number Redis replied to, 0 if none. Thread-safe.
  uint64_t LastAckedWrite() const;

  /// Turn commands away, as if the connection were down, until called again
  /// with false. RedisContext pauses a reconnected context until Redis has
  /// passed its reconnect check, so no retried write lands on a server that
  /// turns out to have lost data. Commands queued on the raw context before
  /// Reset() are not affected. Thread-safe.
  ///
  /// \param paused Whether to turn commands away.
  void PauseCommands(bool paused);

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
  /// \param on_accepted Optional notification called synchronously
  /// after hiredis accepts the command, while the reply-handling mutex is held.
  /// Not retained or called on rejection. Must not reenter this context, invoke
  /// user callbacks or metric recorders, or throw. Only capture bookkeeping state
  /// here; record metrics after this method returns and releases the mutex.
  /// \param write_number Non-null for a command that changes data. Once the
  /// command is accepted and a write watermark is enabled, the marker write is
  /// queued right behind it and the write's number is stored here, before the
  /// reply can arrive. Left untouched otherwise.
  /// \return Status
  Status RedisAsyncCommandArgv(
      redisCallbackFn *fn,
      void *privdata,
      int argc,
      const char **argv,
      const size_t *argvlen,
      std::optional<absl::FunctionRef<void()>> on_accepted = std::nullopt,
      uint64_t *write_number = nullptr);

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
  /// See WatchConnect(). Only used on the io_service thread.
  boost::asio::steady_timer connect_timer_;
  /// When the connect adopted by the last Reset() times out. Guarded by
  /// `mutex_`.
  std::chrono::steady_clock::time_point connect_deadline_;
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
  /// An addWrite request is posted to the io_service and has not run yet. Set
  /// by submitting threads, cleared on the io_service thread.
  std::atomic<bool> write_dispatch_pending_{false};
  /// Bumped every time a new raw context, and with it a new socket, is
  /// adopted. A socket operation queued against an earlier socket must not
  /// touch the flags above once they describe a different connection.
  /// Atomic because the socket handler reads it before taking `mutex_`.
  std::atomic<uint64_t> socket_generation_{0};
  /// See OutageDeadline(). `kNoOutage` means no outage is in progress.
  static constexpr int64_t kNoOutage = INT64_MIN;
  std::atomic<int64_t> outage_deadline_ns_{kNoOutage};
  /// See EnableWriteWatermark(). Guarded by `mutex_`.
  std::optional<WriteWatermark> write_watermark_;
  /// Number of the last write submitted. Guarded by `mutex_`, so numbers
  /// follow the order in which writes reach the socket.
  uint64_t last_submitted_write_ = 0;
  /// Number carried by the last marker queued. Guarded by `mutex_`.
  uint64_t last_marked_write_ = 0;
  /// Marker 1 could not be sent when the watermark was enabled and goes out
  /// with the next flush. Written under `mutex_`; atomic because
  /// PauseCommands() reads it without the lock.
  std::atomic<bool> first_marker_pending_{false};
  /// privdata of FirstMarkerReplyFn.
  struct FirstMarker {
    std::weak_ptr<bool> alive;
    uint64_t number;
  };
  /// See AckWrite().
  std::atomic<uint64_t> last_acked_write_{0};
  /// See PauseCommands(). Read under `mutex_` at submission.
  std::atomic<bool> commands_paused_{false};
  /// Sentinel letting a queued socket operation notice that this object was
  /// destroyed before its handler ran. Registering a hiredis connect callback
  /// arms a write wait immediately, and RedisContext::Connect tears the
  /// context down again as soon as it learns it was talking to a Sentinel.
  std::shared_ptr<bool> alive_ = std::make_shared<bool>(true);

  /// Issue async socket operations depending on the state of the redis async context.
  void Operate();

  /// How often an in-progress connect adopted by Reset() is checked.
  static constexpr std::chrono::milliseconds kConnectCheckInterval{50};

  /// Arm the next connect check for the connection of `generation`. Runs on
  /// the io_service thread.
  void WatchConnect(uint64_t generation);

  /// Let hiredis check whether the connect of `generation` completed, or time
  /// it out once connect_deadline_ has passed. hiredis only checks on a write
  /// event, and epoll reports EPOLLERR or EPOLLHUP for a socket whose connect
  /// is still in progress: the write wait completes, hiredis finds the connect
  /// unfinished, and nothing asks for another write event. Before a reconnect
  /// held commands back, the next submitted command asked for one.
  void CheckConnect(uint64_t generation);
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

  /// Queue a marker for the writes submitted since the last one, right before
  /// the output buffer is flushed. `mutex_` must be held.
  void AppendMarkerLocked();

  /// Queue the first marker, carrying the last write number so far. `mutex_`
  /// must be held and the connection up.
  void QueueFirstMarkerLocked();

  /// Reply callback of the first marker: acknowledges its number. privdata is
  /// a heap-allocated FirstMarker, freed here.
  static void FirstMarkerReplyFn(redisAsyncContext *context,
                                 void *raw_reply,
                                 void *privdata);
  friend class RedisAsyncContextTest;
};
}  // namespace gcs
}  // namespace ray
