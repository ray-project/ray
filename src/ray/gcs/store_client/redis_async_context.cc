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
    : redis_async_context_(std::move(redis_async_context)),
      io_service_(io_service),
      socket_(io_service) {
  RAY_CHECK(redis_async_context_ != nullptr);

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
  // Reset redis_async_context_ to nullptr because hiredis has released this context.
  redis_async_context_.release();
}

Status RedisAsyncContext::RedisAsyncCommand(redisCallbackFn *fn,
                                            void *privdata,
                                            const char *format,
                                            ...) {
  va_list ap;
  va_start(ap, format);

  int ret_code = 0;
  {
    // `redisAsyncCommand` will mutate `redis_async_context_`, use a lock to protect it.
    std::lock_guard<std::mutex> lock(mutex_);
    if (!redis_async_context_) {
      return Status::Disconnected("Redis is disconnected");
    }
    ret_code = redisvAsyncCommand(redis_async_context_.get(), fn, privdata, format, ap);
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
    ret_code = redisAsyncCommandArgv(
        redis_async_context_.get(), fn, privdata, argc, argv, argvlen);
  }

  if (ret_code == REDIS_ERR) {
    return Status::RedisError(std::string(redis_async_context_->errstr));
  }
  RAY_CHECK(ret_code == REDIS_OK);
  return Status::OK();
}

void RedisAsyncContext::Operate() {
  if (read_requested_ && !read_in_progress_) {
    read_in_progress_ = true;
    socket_.async_read_some(
        boost::asio::null_buffers(),
        boost::bind(
            &RedisAsyncContext::HandleIo, this, boost::asio::placeholders::error, false));
  }

  if (write_requested_ && !write_in_progress_) {
    write_in_progress_ = true;
    socket_.async_write_some(
        boost::asio::null_buffers(),
        boost::bind(
            &RedisAsyncContext::HandleIo, this, boost::asio::placeholders::error, true));
  }
}

void RedisAsyncContext::HandleIo(boost::system::error_code error_code, bool write) {
  RAY_CHECK(!error_code || error_code == boost::asio::error::would_block ||
            error_code == boost::asio::error::connection_reset ||
            error_code == boost::asio::error::operation_aborted)
      << "handle_io(error_code = " << error_code << ")";
  (write ? write_in_progress_ : read_in_progress_) = false;
  if (error_code != boost::asio::error::operation_aborted) {
    RAY_CHECK(redis_async_context_) << "redis_async_context_ must not be NULL";
    {
      // `redisAsyncHandleRead` and `redisAsyncHandleWrite` will mutate
      // `redis_async_context_`, use a lock to protect it.
      const std::lock_guard lock(mutex_);
      write ? redisAsyncHandleWrite(redis_async_context_.get())
            : redisAsyncHandleRead(redis_async_context_.get());
    }
  }

  if (error_code == boost::asio::error::would_block) {
    Operate();
  }
}

void RedisAsyncContext::AddRead() {
  // Because redis commands are non-thread safe, dispatch the operation to backend thread.
  io_service_.dispatch(
      [this] {
        read_requested_ = true;
        Operate();
      },
      "RedisAsyncContext.addRead");
}

void RedisAsyncContext::AddWrite() {
  // Because redis commands are non-thread safe, dispatch the operation to backend thread.
  io_service_.dispatch(
      [this] {
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
