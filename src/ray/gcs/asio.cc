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

#include "ray/gcs/asio.h"

#include "ray/util/logging.h"

extern "C" {
#include "hiredis/async.h"
}

RedisAsioClient::RedisAsioClient(instrumented_io_context &io_service,
                                 ray::gcs::RedisAsyncContext &redis_async_context)
    : redis_async_context_(redis_async_context),
      io_service_(io_service),
      socket_(io_service),
      read_requested_(false),
      write_requested_(false),
      read_in_progress_(false),
      write_in_progress_(false) {
  redisAsyncContext *async_context = redis_async_context_.GetRawRedisAsyncContext();

  // gives access to c->fd
  redisContext *c = &(async_context->c);

#ifdef _WIN32
  SOCKET sock = SOCKET_ERROR;
  WSAPROTOCOL_INFO pi;
  if (WSADuplicateSocket(c->fd, GetCurrentProcessId(), &pi) == 0) {
    DWORD flag = WSA_FLAG_OVERLAPPED;
    sock = WSASocket(pi.iAddressFamily, pi.iSocketType, pi.iProtocol, &pi, 0, flag);
  }
  boost::asio::ip::tcp::socket::native_handle_type handle(sock);
#else
  boost::asio::ip::tcp::socket::native_handle_type handle(dup(c->fd));
#endif

  // hiredis is already connected
  // use the existing native socket
  socket_.assign(boost::asio::ip::tcp::v4(), handle);

  // register hooks with the hiredis async context
  async_context->ev.addRead = call_C_addRead;
  async_context->ev.delRead = call_C_delRead;
  async_context->ev.addWrite = call_C_addWrite;
  async_context->ev.delWrite = call_C_delWrite;
  async_context->ev.cleanup = call_C_cleanup;

  // C wrapper functions will use this pointer to call class members.
  async_context->ev.data = this;
}

void RedisAsioClient::operate() {
  if (read_requested_ && !read_in_progress_) {
    read_in_progress_ = true;
    socket_.async_read_some(
        boost::asio::null_buffers(),
        boost::bind(
            &RedisAsioClient::handle_io, this, boost::asio::placeholders::error, false));
  }

  if (write_requested_ && !write_in_progress_) {
    write_in_progress_ = true;
    socket_.async_write_some(
        boost::asio::null_buffers(),
        boost::bind(
            &RedisAsioClient::handle_io, this, boost::asio::placeholders::error, true));
  }
}

void RedisAsioClient::handle_io(boost::system::error_code error_code, bool write) {
  RAY_CHECK(!error_code || error_code == boost::asio::error::would_block ||
            error_code == boost::asio::error::connection_reset ||
            error_code == boost::asio::error::operation_aborted)
      << "handle_io(error_code = " << error_code << ")";
  (write ? write_in_progress_ : read_in_progress_) = false;
  if (error_code != boost::asio::error::operation_aborted) {
    if (!redis_async_context_.GetRawRedisAsyncContext()) {
      RAY_LOG(FATAL) << "redis_async_context_ must not be NULL";
    }
    write ? redis_async_context_.RedisAsyncHandleWrite()
          : redis_async_context_.RedisAsyncHandleRead();
  }

  if (error_code == boost::asio::error::would_block) {
    operate();
  }
}

void RedisAsioClient::add_io(bool write) {
  // Because redis commands are non-thread safe, dispatch the operation to backend thread.
  io_service_.dispatch(
      [this, write]() {
        (write ? write_requested_ : read_requested_) = true;
        operate();
      },
      "RedisAsioClient.add_io");
}

void RedisAsioClient::del_io(bool write) {
  (write ? write_requested_ : read_requested_) = false;
}

void RedisAsioClient::cleanup() {}

static inline RedisAsioClient *cast_to_client(void *private_data) {
  RAY_CHECK(private_data != nullptr);
  return static_cast<RedisAsioClient *>(private_data);
}

extern "C" void call_C_addRead(void *private_data) {
  cast_to_client(private_data)->add_io(false);
}

extern "C" void call_C_delRead(void *private_data) {
  cast_to_client(private_data)->del_io(false);
}

extern "C" void call_C_addWrite(void *private_data) {
  cast_to_client(private_data)->add_io(true);
}

extern "C" void call_C_delWrite(void *private_data) {
  cast_to_client(private_data)->del_io(true);
}

extern "C" void call_C_cleanup(void *private_data) {
  cast_to_client(private_data)->cleanup();
}
