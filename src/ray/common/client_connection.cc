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

#include "client_connection.h"

#include <stdio.h>

#include <boost/asio/buffer.hpp>
#include <boost/asio/generic/stream_protocol.hpp>
#include <boost/asio/placeholders.hpp>
#include <boost/asio/read.hpp>
#include <boost/asio/write.hpp>
#include <boost/bind.hpp>
#include <sstream>

#include "ray/common/ray_config.h"
#include "ray/util/util.h"

namespace ray {

std::shared_ptr<ServerConnection> ServerConnection::Create(
    boost::asio::generic::stream_protocol::socket &&socket) {
  std::shared_ptr<ServerConnection> self(new ServerConnection(std::move(socket)));
  return self;
}

ServerConnection::ServerConnection(boost::asio::generic::stream_protocol::socket &&socket)
    : socket_(std::move(socket)),
      async_write_max_messages_(1),
      async_write_queue_(),
      async_write_in_flight_(false),
      async_write_broken_pipe_(false) {}

ServerConnection::~ServerConnection() {
  // If there are any pending messages, invoke their callbacks with an IOError status.
  for (const auto &write_buffer : async_write_queue_) {
    write_buffer->handler(Status::IOError("Connection closed."));
  }
}

Status ServerConnection::WriteBuffer(
    const std::vector<boost::asio::const_buffer> &buffer) {
  boost::system::error_code error;
  // Loop until all bytes are written while handling interrupts.
  // When profiling with pprof, unhandled interrupts were being sent by the profiler to
  // the raylet process, which was causing synchronous reads and writes to fail.
  for (const auto &b : buffer) {
    uint64_t bytes_remaining = boost::asio::buffer_size(b);
    uint64_t position = 0;
    while (bytes_remaining != 0) {
      size_t bytes_written =
          socket_.write_some(boost::asio::buffer(b + position, bytes_remaining), error);
      position += bytes_written;
      bytes_remaining -= bytes_written;
      if (error.value() == EINTR) {
        continue;
      } else if (error.value() != boost::system::errc::errc_t::success) {
        return boost_to_ray_status(error);
      }
    }
  }
  return ray::Status::OK();
}

Status ServerConnection::ReadBuffer(
    const std::vector<boost::asio::mutable_buffer> &buffer) {
  boost::system::error_code error;
  // Loop until all bytes are read while handling interrupts.
  for (const auto &b : buffer) {
    uint64_t bytes_remaining = boost::asio::buffer_size(b);
    uint64_t position = 0;
    while (bytes_remaining != 0) {
      size_t bytes_read =
          socket_.read_some(boost::asio::buffer(b + position, bytes_remaining), error);
      position += bytes_read;
      bytes_remaining -= bytes_read;
      if (error.value() == EINTR) {
        continue;
      } else if (error.value() != boost::system::errc::errc_t::success) {
        return boost_to_ray_status(error);
      }
    }
  }
  return Status::OK();
}

ray::Status ServerConnection::WriteMessage(int64_t type, int64_t length,
                                           const uint8_t *message) {
  sync_writes_ += 1;
  bytes_written_ += length;

  std::vector<boost::asio::const_buffer> message_buffers;
  auto write_cookie = RayConfig::instance().ray_cookie();
  message_buffers.push_back(boost::asio::buffer(&write_cookie, sizeof(write_cookie)));
  message_buffers.push_back(boost::asio::buffer(&type, sizeof(type)));
  message_buffers.push_back(boost::asio::buffer(&length, sizeof(length)));
  message_buffers.push_back(boost::asio::buffer(message, length));
  return WriteBuffer(message_buffers);
}

void ServerConnection::WriteMessageAsync(
    int64_t type, int64_t length, const uint8_t *message,
    const std::function<void(const ray::Status &)> &handler) {
  async_writes_ += 1;
  bytes_written_ += length;

  auto write_buffer = std::unique_ptr<AsyncWriteBuffer>(new AsyncWriteBuffer());
  write_buffer->write_cookie = RayConfig::instance().ray_cookie();
  write_buffer->write_type = type;
  write_buffer->write_length = length;
  write_buffer->write_message.resize(length);
  write_buffer->write_message.assign(message, message + length);
  write_buffer->handler = handler;

  auto size = async_write_queue_.size();
  auto size_is_power_of_two = (size & (size - 1)) == 0;
  if (size > 1000 && size_is_power_of_two) {
    RAY_LOG(WARNING) << "ServerConnection has " << size << " buffered async writes";
  }

  async_write_queue_.push_back(std::move(write_buffer));

  if (!async_write_in_flight_) {
    DoAsyncWrites();
  }
}

void ServerConnection::DoAsyncWrites() {
  // Make sure we were not writing to the socket.
  RAY_CHECK(!async_write_in_flight_);
  async_write_in_flight_ = true;

  // Do an async write of everything currently in the queue to the socket.
  std::vector<boost::asio::const_buffer> message_buffers;
  int num_messages = 0;
  for (const auto &write_buffer : async_write_queue_) {
    message_buffers.push_back(boost::asio::buffer(&write_buffer->write_cookie,
                                                  sizeof(write_buffer->write_cookie)));
    message_buffers.push_back(
        boost::asio::buffer(&write_buffer->write_type, sizeof(write_buffer->write_type)));
    message_buffers.push_back(boost::asio::buffer(&write_buffer->write_length,
                                                  sizeof(write_buffer->write_length)));
    message_buffers.push_back(boost::asio::buffer(write_buffer->write_message));
    num_messages++;
    if (num_messages >= async_write_max_messages_) {
      break;
    }
  }

  // Helper function to call all handlers with the input status.
  auto call_handlers = [this](const ray::Status &status, int num_messages) {
    for (int i = 0; i < num_messages; i++) {
      auto write_buffer = std::move(async_write_queue_.front());
      write_buffer->handler(status);
      async_write_queue_.pop_front();
    }
    // We finished writing, so mark that we're no longer doing an async write.
    async_write_in_flight_ = false;
    // If there is more to write, try to write the rest.
    if (!async_write_queue_.empty()) {
      DoAsyncWrites();
    }
  };

  if (async_write_broken_pipe_) {
    // Call the handlers directly. Because writing messages to a connection
    // with broken-pipe status will result in the callbacks never being called.
    call_handlers(ray::Status::IOError("Broken pipe"), num_messages);
    return;
  }
  auto this_ptr = this->shared_from_this();
  boost::asio::async_write(
      ServerConnection::socket_, message_buffers,
      [this, this_ptr, num_messages, call_handlers](
          const boost::system::error_code &error, size_t bytes_transferred) {
        ray::Status status = boost_to_ray_status(error);
        if (error.value() == boost::system::errc::errc_t::broken_pipe) {
          RAY_LOG(ERROR) << "Broken Pipe happened during calling "
                         << "ServerConnection::DoAsyncWrites.";
          // From now on, calling DoAsyncWrites will directly call the handler
          // with this broken-pipe status.
          async_write_broken_pipe_ = true;
        } else if (!status.ok()) {
          RAY_LOG(ERROR) << "Error encountered during calling "
                         << "ServerConnection::DoAsyncWrites, message: "
                         << status.message()
                         << ", error code: " << static_cast<int>(error.value());
        }
        call_handlers(status, num_messages);
      });
}

std::shared_ptr<ClientConnection> ClientConnection::Create(
    ClientHandler &client_handler, MessageHandler &message_handler,
    boost::asio::generic::stream_protocol::socket &&socket,
    const std::string &debug_label,
    const std::vector<std::string> &message_type_enum_names, int64_t error_message_type) {
  std::shared_ptr<ClientConnection> self(
      new ClientConnection(message_handler, std::move(socket), debug_label,
                           message_type_enum_names, error_message_type));
  // Let our manager process our new connection.
  client_handler(*self);
  return self;
}

ClientConnection::ClientConnection(
    MessageHandler &message_handler,
    boost::asio::generic::stream_protocol::socket &&socket,
    const std::string &debug_label,
    const std::vector<std::string> &message_type_enum_names, int64_t error_message_type)
    : ServerConnection(std::move(socket)),
      registered_(false),
      message_handler_(message_handler),
      debug_label_(debug_label),
      message_type_enum_names_(message_type_enum_names),
      error_message_type_(error_message_type) {}

void ClientConnection::Register() {
  RAY_CHECK(!registered_);
  registered_ = true;
}

void ClientConnection::ProcessMessages() {
  // Wait for a message header from the client. The message header includes the
  // protocol version, the message type, and the length of the message.
  std::vector<boost::asio::mutable_buffer> header;
  header.push_back(boost::asio::buffer(&read_cookie_, sizeof(read_cookie_)));
  header.push_back(boost::asio::buffer(&read_type_, sizeof(read_type_)));
  header.push_back(boost::asio::buffer(&read_length_, sizeof(read_length_)));
  boost::asio::async_read(
      ServerConnection::socket_, header,
      boost::bind(&ClientConnection::ProcessMessageHeader,
                  shared_ClientConnection_from_this(), boost::asio::placeholders::error));
}

void ClientConnection::ProcessMessageHeader(const boost::system::error_code &error) {
  if (error) {
    // If there was an error, disconnect the client.
    read_type_ = error_message_type_;
    read_length_ = 0;
    ProcessMessage(error);
    return;
  }

  // If there was no error, make sure the ray cookie matches.
  if (!CheckRayCookie()) {
    ServerConnection::Close();
    return;
  }

  // Resize the message buffer to match the received length.
  read_message_.resize(read_length_);
  ServerConnection::bytes_read_ += read_length_;
  // Wait for the message to be read.
  boost::asio::async_read(
      ServerConnection::socket_, boost::asio::buffer(read_message_),
      boost::bind(&ClientConnection::ProcessMessage, shared_ClientConnection_from_this(),
                  boost::asio::placeholders::error));
}

bool ClientConnection::CheckRayCookie() {
  if (read_cookie_ == RayConfig::instance().ray_cookie()) {
    return true;
  }

  // Cookie is not matched.
  // Only assert if the message is coming from a known remote endpoint,
  // which is indicated by a non-nil client ID. This is to protect raylet
  // against miscellaneous connections. We did see cases where bad data
  // is received from local unknown program which crashes raylet.
  std::ostringstream ss;
  ss << " ray cookie mismatch for received message. "
     << "received cookie: " << read_cookie_ << ", debug label: " << debug_label_;
  auto remote_endpoint_info = RemoteEndpointInfo();
  if (!remote_endpoint_info.empty()) {
    ss << ", remote endpoint info: " << remote_endpoint_info;
  }

  if (registered_) {
    // This is from a known client, which indicates a bug.
    RAY_LOG(FATAL) << ss.str();
  } else {
    // It's not from a known client, log this message, and stop processing the connection.
    RAY_LOG(WARNING) << ss.str();
  }
  return false;
}

std::string ClientConnection::RemoteEndpointInfo() {
  return EndpointToUrl(ServerConnection::socket_.remote_endpoint(), false);
}

void ClientConnection::ProcessMessage(const boost::system::error_code &error) {
  if (error) {
    read_type_ = error_message_type_;
  }

  int64_t start_ms = current_time_ms();
  message_handler_(shared_ClientConnection_from_this(), read_type_, read_message_.data());
  int64_t interval = current_time_ms() - start_ms;
  if (interval > RayConfig::instance().handler_warning_timeout_ms()) {
    std::string message_type;
    if (message_type_enum_names_.empty()) {
      message_type = std::to_string(read_type_);
    } else {
      message_type = message_type_enum_names_[read_type_];
    }
    RAY_LOG(WARNING) << "[" << debug_label_ << "]ProcessMessage with type "
                     << message_type << " took " << interval << " ms.";
  }
}

std::string ServerConnection::DebugString() const {
  std::stringstream result;
  result << "\n- bytes read: " << bytes_read_;
  result << "\n- bytes written: " << bytes_written_;
  result << "\n- num async writes: " << async_writes_;
  result << "\n- num sync writes: " << sync_writes_;
  result << "\n- writing: " << async_write_in_flight_;
  int64_t num_bytes = 0;
  for (auto &buffer : async_write_queue_) {
    num_bytes += buffer->write_length;
  }
  result << "\n- pending async bytes: " << num_bytes;
  return result.str();
}

}  // namespace ray
