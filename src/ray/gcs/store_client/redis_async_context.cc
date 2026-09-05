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

#include "ray/gcs/store_client/redis_async_context.h"

#include <memory>
#include <string>
#include <utility>

#ifndef _WIN32
#include <netinet/in.h>
#include <sys/socket.h>
#endif

extern "C" {
#include "hiredis/async.h"
#include "hiredis/hiredis.h"
}

namespace ray {
namespace gcs {
RedisAsyncContext::RedisAsyncContext(
    instrumented_io_context &io_service,
    std::unique_ptr<redisAsyncContext, RedisContextDeleter> redis_async_context)
    : io_service_(io_service), socket_(io_service) {
  std::lock_guard<std::mutex> lock(mutex_);
  AttachLocked(std::move(redis_async_context));
}

RedisAsyncContext::~RedisAsyncContext() {
  // Expire the sentinel before any member is torn down, so that a request or a
  // socket operation already queued on the io_service bails out instead of
  // running against a half-destroyed object.
  alive_.reset();
  // Free the raw context while every member is still alive. Members are
  // destroyed in reverse declaration order, so leaving this to the implicit
  // member destruction would free it after the handlers: redisAsyncFree runs
  // the disconnect callback for a connected context, and that callback walks
  // back into this object through NotifyDisconnected().
  redis_async_context_.reset();
}

void RedisAsyncContext::AttachLocked(
    std::unique_ptr<redisAsyncContext, RedisContextDeleter> redis_async_context) {
  redis_async_context_ = std::move(redis_async_context);
  RAY_CHECK(redis_async_context_ != nullptr);
  ++socket_generation_;

  // gives access to c->fd
  redisContext *c = &(redis_async_context_->c);

#ifdef _WIN32
  SOCKET sock = SOCKET_ERROR;
  WSAPROTOCOL_INFO pi;
  if (WSADuplicateSocket(c->fd, GetCurrentProcessId(), &pi) == 0) {
    DWORD flag = WSA_FLAG_OVERLAPPED;
    sock = WSASocket(pi.iAddressFamily, pi.iSocketType, pi.iProtocol, &pi, 0, flag);
  }
  const boost::asio::ip::tcp::socket::native_handle_type handle(sock);
#else
  const boost::asio::ip::tcp::socket::native_handle_type handle(dup(c->fd));
#endif

  // hiredis is already connected
  // use the existing native socket
#ifdef _WIN32
  boost::asio::ip::tcp protocol = (pi.iAddressFamily == AF_INET6)
                                      ? boost::asio::ip::tcp::v6()
                                      : boost::asio::ip::tcp::v4();
  socket_.assign(protocol, handle);
#else
  struct sockaddr_storage addr;
  socklen_t addr_len = sizeof(addr);
  if (getsockname(c->fd, reinterpret_cast<struct sockaddr *>(&addr), &addr_len) == 0) {
    boost::asio::ip::tcp protocol = (addr.ss_family == AF_INET6)
                                        ? boost::asio::ip::tcp::v6()
                                        : boost::asio::ip::tcp::v4();
    socket_.assign(protocol, handle);
  } else {
    // Fallback to IPv4
    socket_.assign(boost::asio::ip::tcp::v4(), handle);
  }
#endif

  // register hooks with the hiredis async context
  redis_async_context_->ev.addRead = CallbackAddRead;
  redis_async_context_->ev.delRead = CallbackDelRead;
  redis_async_context_->ev.addWrite = CallbackAddWrite;
  redis_async_context_->ev.delWrite = CallbackDelWrite;
  redis_async_context_->ev.cleanup = CallbackCleanup;

  // C wrapper functions will use this pointer to call class members.
  redis_async_context_->ev.data = this;
}

redisAsyncContext *RedisAsyncContext::GetRawRedisAsyncContext() {
  return redis_async_context_.get();
}

void RedisAsyncContext::ResetRawRedisAsyncContext() {
  // NOTE: deliberately does not take `mutex_`. hiredis calls this from its
  // disconnect callback, which we reach from `redisAsyncHandleRead`/`Write`
  // while `mutex_` is already held in HandleIo -- `std::mutex` is not
  // recursive, so locking here would self-deadlock. Being reached only through
  // that path, the reset is already serialized against command submission.
  redis_async_context_.release();
}

void RedisAsyncContext::Reset(
    std::unique_ptr<redisAsyncContext, RedisContextDeleter> redis_async_context) {
  std::lock_guard<std::mutex> lock(mutex_);
  // The previous raw context was already freed by hiredis, which released
  // `redis_async_context_` via ResetRawRedisAsyncContext. Only the duplicated
  // descriptor held by `socket_` is still ours to close.
  if (socket_.is_open()) {
    boost::system::error_code ec;
    socket_.close(ec);
    if (ec) {
      RAY_LOG(DEBUG) << "Failed to close the stale Redis socket: " << ec.message();
    }
  }
  read_requested_ = false;
  write_requested_ = false;
  read_in_progress_ = false;
  write_in_progress_ = false;
  AttachLocked(std::move(redis_async_context));
}

bool RedisAsyncContext::IsConnected() {
  std::lock_guard<std::mutex> lock(mutex_);
  return redis_async_context_ != nullptr;
}

void RedisAsyncContext::SetDisconnectHandler(std::function<void()> handler) {
  disconnect_handler_ = std::move(handler);
}

void RedisAsyncContext::NotifyDisconnected() {
  if (disconnect_handler_) {
    disconnect_handler_();
  }
}

void RedisAsyncContext::SetConnectHandler(std::function<void()> handler) {
  connect_handler_ = std::move(handler);
}

void RedisAsyncContext::NotifyConnected() {
  if (connect_handler_) {
    connect_handler_();
  }
}

Status RedisAsyncContext::RedisAsyncCommand(redisCallbackFn *fn,
                                            void *privdata,
                                            const char *format,
                                            ...) {
  va_list ap;
  va_start(ap, format);

  int ret_code = 0;
  std::string errstr;
  {
    // `redisAsyncCommand` will mutate `redis_async_context_`, use a lock to protect it.
    std::lock_guard<std::mutex> lock(mutex_);
    if (!redis_async_context_) {
      va_end(ap);
      return Status::Disconnected("Redis is disconnected");
    }
    ret_code = redisvAsyncCommand(redis_async_context_.get(), fn, privdata, format, ap);
    // Copy errstr under the lock: a disconnect on the io_service thread can
    // release `redis_async_context_` as soon as we drop it.
    if (ret_code == REDIS_ERR) {
      errstr = redis_async_context_->errstr;
    }
  }

  va_end(ap);

  if (ret_code == REDIS_ERR) {
    return Status::RedisError(errstr);
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
  std::string errstr;
  {
    // `redisAsyncCommandArgv` will mutate `redis_async_context_`, use a lock to protect
    // it.
    std::lock_guard<std::mutex> lock(mutex_);
    if (!redis_async_context_) {
      return Status::Disconnected("Redis is disconnected");
    }
    ret_code = redisAsyncCommandArgv(
        redis_async_context_.get(), fn, privdata, argc, argv, argvlen);
    // Copy errstr under the lock: a disconnect on the io_service thread can
    // release `redis_async_context_` as soon as we drop it.
    if (ret_code == REDIS_ERR) {
      errstr = redis_async_context_->errstr;
    }
  }

  if (ret_code == REDIS_ERR) {
    return Status::RedisError(errstr);
  }
  RAY_CHECK(ret_code == REDIS_OK);
  return Status::OK();
}

void RedisAsyncContext::Operate() {
  // Closing a socket does not withdraw the handlers already queued against it;
  // they still run, with a raw pointer to this object and flags that by then
  // describe a different connection. Both guards below cover that.
  auto guard = [this, generation = socket_generation_](const std::weak_ptr<bool> &alive,
                                                       bool write) {
    return [this, alive, generation, write](const boost::system::error_code &error_code,
                                            std::size_t /*bytes*/) {
      if (alive.expired()) {
        return;
      }
      if (generation != socket_generation_) {
        return;
      }
      HandleIo(error_code, write);
    };
  };

  if (read_requested_ && !read_in_progress_) {
    read_in_progress_ = true;
    socket_.async_read_some(boost::asio::null_buffers(), guard(alive_, /*write=*/false));
  }

  if (write_requested_ && !write_in_progress_) {
    write_in_progress_ = true;
    socket_.async_write_some(boost::asio::null_buffers(), guard(alive_, /*write=*/true));
  }
}

void RedisAsyncContext::HandleIo(boost::system::error_code error_code, bool write) {
  RAY_CHECK(!error_code || error_code == boost::asio::error::would_block ||
            error_code == boost::asio::error::connection_reset ||
            error_code == boost::asio::error::operation_aborted)
      << "handle_io(error_code = " << error_code << ")";
  (write ? write_in_progress_ : read_in_progress_) = false;
  if (error_code != boost::asio::error::operation_aborted) {
    // `redisAsyncHandleRead` and `redisAsyncHandleWrite` will mutate
    // `redis_async_context_`, use a lock to protect it.
    const std::lock_guard lock(mutex_);
    // A socket operation queued before the connection dropped can complete
    // after hiredis already freed the context (which releases
    // `redis_async_context_`). There is nothing left to hand to hiredis, and
    // the reconnect path will re-arm the socket.
    if (redis_async_context_ == nullptr) {
      return;
    }
    write ? redisAsyncHandleWrite(redis_async_context_.get())
          : redisAsyncHandleRead(redis_async_context_.get());
  }

  if (error_code == boost::asio::error::would_block) {
    Operate();
  }
}

void RedisAsyncContext::AddRead() {
  // Because redis commands are non-thread safe, dispatch the operation to backend thread.
  // Registering a connect callback makes hiredis ask for a write before the
  // connection is even established, and RedisContext::Connect tears the context
  // down again the moment it discovers it dialled a Sentinel. The sentinel
  // below keeps a queued request from arming a socket on a destroyed object.
  io_service_.dispatch(
      [this, alive = std::weak_ptr<bool>(alive_)] {
        if (alive.expired()) {
          return;
        }
        read_requested_ = true;
        Operate();
      },
      "RedisAsyncContext.addRead");
}

void RedisAsyncContext::AddWrite() {
  // Because redis commands are non-thread safe, dispatch the operation to backend thread.
  io_service_.dispatch(
      [this, alive = std::weak_ptr<bool>(alive_)] {
        if (alive.expired()) {
          return;
        }
        write_requested_ = true;
        Operate();
      },
      "RedisAsyncContext.addWrite");
}

void RedisAsyncContext::DelRead() { read_requested_ = false; }

void RedisAsyncContext::DelWrite() { write_requested_ = false; }

void RedisAsyncContext::Cleanup() {
  DelRead();
  DelWrite();
}

void CallbackAddRead(void *private_data) {
  RAY_CHECK(private_data != nullptr);
  static_cast<RedisAsyncContext *>(private_data)->AddRead();
}

void CallbackDelRead(void *private_data) {
  RAY_CHECK(private_data != nullptr);
  static_cast<RedisAsyncContext *>(private_data)->DelRead();
}

void CallbackAddWrite(void *private_data) {
  RAY_CHECK(private_data != nullptr);
  static_cast<RedisAsyncContext *>(private_data)->AddWrite();
}

void CallbackDelWrite(void *private_data) {
  RAY_CHECK(private_data != nullptr);
  static_cast<RedisAsyncContext *>(private_data)->DelWrite();
}

void CallbackCleanup(void *private_data) {
  RAY_CHECK(private_data != nullptr);
  static_cast<RedisAsyncContext *>(private_data)->Cleanup();
}
}  // namespace gcs
}  // namespace ray
