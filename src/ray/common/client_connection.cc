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

#include "ray/common/client_connection.h"

#include <boost/asio/buffer.hpp>
#include <boost/asio/generic/stream_protocol.hpp>
#include <boost/asio/placeholders.hpp>
#include <boost/asio/read.hpp>
#include <boost/asio/write.hpp>
#include <boost/bind/bind.hpp>
#include <chrono>
#include <sstream>
#include <thread>

#include "ray/common/event_stats.h"
#include "ray/common/ray_config.h"
#include "ray/util/util.h"

namespace ray {

Status ConnectSocketRetry(local_stream_socket &socket,
                          const std::string &endpoint,
                          int num_retries,
                          int64_t timeout_in_ms) {
  RAY_CHECK(num_retries != 0);
  // Pick the default values if the user did not specify.
  if (num_retries < 0) {
    num_retries = RayConfig::instance().raylet_client_num_connect_attempts();
  }
  if (timeout_in_ms < 0) {
    timeout_in_ms = RayConfig::instance().raylet_client_connect_timeout_milliseconds();
  }
  boost::system::error_code ec;
  for (int num_attempts = 0; num_attempts < num_retries; ++num_attempts) {
    // The latest boost::asio always returns void for connect(). Do not
    // treat its return value as error code anymore.
    socket.connect(ParseUrlEndpoint(endpoint), ec);
    if (!ec) {
      break;
    }
    if (num_attempts > 0) {
      // Socket is created by the raylet. Due to a race condition it might not
      // be created before we try connecting.
      RAY_LOG(INFO) << "Retrying to connect to socket for endpoint " << endpoint
                    << " (num_attempts = " << num_attempts
                    << ", num_retries = " << num_retries << ")";
    }
    // Sleep for timeout milliseconds.
    std::this_thread::sleep_for(std::chrono::milliseconds(timeout_in_ms));
  }
  return boost_to_ray_status(ec);
}

std::shared_ptr<ServerConnection> ServerConnection::Create(local_stream_socket &&socket) {
  std::shared_ptr<ServerConnection> self(new ServerConnection(std::move(socket)));
  return self;
}

ServerConnection::ServerConnection(local_stream_socket &&socket)
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

void ServerConnection::WriteBufferAsync(
    const std::vector<boost::asio::const_buffer> &buffer,
    const std::function<void(const ray::Status &)> &handler) {
  // Wait for the message to be written.
  if (RayConfig::instance().event_stats()) {
    auto &io_context =
        static_cast<instrumented_io_context &>(socket_.get_executor().context());
    const auto stats_handle =
        io_context.stats().RecordStart("ClientConnection.async_write.WriteBufferAsync");
    boost::asio::async_write(
        socket_,
        buffer,
        [handler, stats_handle = std::move(stats_handle)](
            const boost::system::error_code &ec, size_t bytes_transferred) {
          EventTracker::RecordExecution(
              [handler, ec]() { handler(boost_to_ray_status(ec)); },
              std::move(stats_handle));
        });
  } else {
    boost::asio::async_write(
        socket_,
        buffer,
        [handler](const boost::system::error_code &ec, size_t bytes_transferred) {
          handler(boost_to_ray_status(ec));
        });
  }
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

void ServerConnection::ReadBufferAsync(
    const std::vector<boost::asio::mutable_buffer> &buffer,
    const std::function<void(const ray::Status &)> &handler) {
  // Wait for the message to be read.
  if (RayConfig::instance().event_stats()) {
    auto &io_context =
        static_cast<instrumented_io_context &>(socket_.get_executor().context());
    const auto stats_handle =
        io_context.stats().RecordStart("ClientConnection.async_read.ReadBufferAsync");
    boost::asio::async_read(
        socket_,
        buffer,
        [handler, stats_handle = std::move(stats_handle)](
            const boost::system::error_code &ec, size_t bytes_transferred) {
          EventTracker::RecordExecution(
              [handler, ec]() { handler(boost_to_ray_status(ec)); },
              std::move(stats_handle));
        });
  } else {
    boost::asio::async_read(
        socket_,
        buffer,
        [handler](const boost::system::error_code &ec, size_t bytes_transferred) {
          handler(boost_to_ray_status(ec));
        });
  }
}

ray::Status ServerConnection::WriteMessage(int64_t type,
                                           int64_t length,
                                           const uint8_t *message) {
  sync_writes_ += 1;
  bytes_written_ += length;

  auto write_cookie = RayConfig::instance().ray_cookie();
  return WriteBuffer({
      boost::asio::buffer(&write_cookie, sizeof(write_cookie)),
      boost::asio::buffer(&type, sizeof(type)),
      boost::asio::buffer(&length, sizeof(length)),
      boost::asio::buffer(message, length),
  });
}

Status ServerConnection::ReadMessage(int64_t type, std::vector<uint8_t> *message) {
  int64_t read_cookie, read_type, read_length;
  // Wait for a message header from the client. The message header includes the
  // protocol version, the message type, and the length of the message.
  RAY_RETURN_NOT_OK(ReadBuffer({
      boost::asio::buffer(&read_cookie, sizeof(read_cookie)),
      boost::asio::buffer(&read_type, sizeof(read_type)),
      boost::asio::buffer(&read_length, sizeof(read_length)),
  }));
  if (read_cookie != RayConfig::instance().ray_cookie()) {
    std::ostringstream ss;
    ss << "Ray cookie mismatch for received message. "
       << "Received cookie: " << read_cookie;
    return Status::IOError(ss.str());
  }
  if (type != read_type) {
    std::ostringstream ss;
    ss << "Connection corrupted. Expected message type: " << type
       << ", receviced message type: " << read_type;
    return Status::IOError(ss.str());
  }
  message->resize(read_length);
  return ReadBuffer({boost::asio::buffer(*message)});
}

void ServerConnection::WriteMessageAsync(
    int64_t type,
    int64_t length,
    const uint8_t *message,
    const std::function<void(const ray::Status &)> &handler) {
  async_writes_ += 1;
  bytes_written_ += length;

  auto write_buffer = std::make_unique<AsyncWriteBuffer>();
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
  if (RayConfig::instance().event_stats()) {
    auto &io_context =
        static_cast<instrumented_io_context &>(socket_.get_executor().context());
    const auto stats_handle =
        io_context.stats().RecordStart("ClientConnection.async_write.DoAsyncWrites");
    boost::asio::async_write(
        socket_,
        message_buffers,
        [this,
         this_ptr,
         num_messages,
         call_handlers,
         stats_handle = std::move(stats_handle)](const boost::system::error_code &error,
                                                 size_t bytes_transferred) {
          EventTracker::RecordExecution(
              [this, this_ptr, num_messages, call_handlers, error]() {
                ray::Status status = boost_to_ray_status(error);
                if (error.value() == boost::system::errc::errc_t::broken_pipe) {
                  RAY_LOG(ERROR) << "Broken Pipe happened during calling "
                                 << "ServerConnection::DoAsyncWrites.";
                  // From now on, calling DoAsyncWrites will directly call the handler
                  // with this broken-pipe status.
                  async_write_broken_pipe_ = true;
                } else if (!status.ok()) {
                  RAY_LOG(ERROR)
                      << "Error encountered during calling "
                      << "ServerConnection::DoAsyncWrites, message: " << status.message()
                      << ", error code: " << static_cast<int>(error.value());
                }
                call_handlers(status, num_messages);
              },
              std::move(stats_handle));
        });
  } else {
    boost::asio::async_write(
        ServerConnection::socket_,
        message_buffers,
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
}

std::shared_ptr<ClientConnection> ClientConnection::Create(
    ClientHandler &client_handler,
    MessageHandler &message_handler,
    local_stream_socket &&socket,
    const std::string &debug_label,
    const std::vector<std::string> &message_type_enum_names,
    int64_t error_message_type) {
  std::shared_ptr<ClientConnection> self(new ClientConnection(message_handler,
                                                              std::move(socket),
                                                              debug_label,
                                                              message_type_enum_names,
                                                              error_message_type));
  // Let our manager process our new connection.
  client_handler(*self);
  return self;
}

ClientConnection::ClientConnection(
    MessageHandler &message_handler,
    local_stream_socket &&socket,
    const std::string &debug_label,
    const std::vector<std::string> &message_type_enum_names,
    int64_t error_message_type)
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
  std::vector<boost::asio::mutable_buffer> header{
      boost::asio::buffer(&read_cookie_, sizeof(read_cookie_)),
      boost::asio::buffer(&read_type_, sizeof(read_type_)),
      boost::asio::buffer(&read_length_, sizeof(read_length_)),
  };
  if (RayConfig::instance().event_stats()) {
    auto this_ptr = shared_ClientConnection_from_this();
    auto &io_context = static_cast<instrumented_io_context &>(
        ServerConnection::socket_.get_executor().context());
    const auto stats_handle =
        io_context.stats().RecordStart("ClientConnection.async_read.ReadBufferAsync");
    boost::asio::async_read(
        ServerConnection::socket_,
        header,
        [this, this_ptr, stats_handle = std::move(stats_handle)](
            const boost::system::error_code &ec, size_t bytes_transferred) {
          EventTracker::RecordExecution(
              [this, this_ptr, ec]() { ProcessMessageHeader(ec); },
              std::move(stats_handle));
        });
  } else {
    boost::asio::async_read(ServerConnection::socket_,
                            header,
                            boost::bind(&ClientConnection::ProcessMessageHeader,
                                        shared_ClientConnection_from_this(),
                                        boost::asio::placeholders::error));
  }
}

void ClientConnection::ProcessMessageHeader(const boost::system::error_code &error) {
  if (error) {
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
  if (RayConfig::instance().event_stats()) {
    auto this_ptr = shared_ClientConnection_from_this();
    auto &io_context = static_cast<instrumented_io_context &>(
        ServerConnection::socket_.get_executor().context());
    const auto stats_handle =
        io_context.stats().RecordStart("ClientConnection.async_read.ReadBufferAsync");
    boost::asio::async_read(
        ServerConnection::socket_,
        boost::asio::buffer(read_message_),
        [this, this_ptr, stats_handle = std::move(stats_handle)](
            const boost::system::error_code &ec, size_t bytes_transferred) {
          EventTracker::RecordExecution([this, this_ptr, ec]() { ProcessMessage(ec); },
                                        std::move(stats_handle));
        });
  } else {
    boost::asio::async_read(ServerConnection::socket_,
                            boost::asio::buffer(read_message_),
                            boost::bind(&ClientConnection::ProcessMessage,
                                        shared_ClientConnection_from_this(),
                                        boost::asio::placeholders::error));
  }
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
    flatbuffers::FlatBufferBuilder fbb;
    const auto &disconnect_detail = fbb.CreateString(absl::StrCat(
        "Worker unexpectedly exits with a connection error code ",
        error.value(),
        ". ",
        error.message(),
        ". There are some potential root causes. (1) The process is killed by "
        "SIGKILL by OOM killer due to high memory usage. (2) ray stop --force is "
        "called. (3) The worker is crashed unexpectedly due to SIGSEGV or other "
        "unexpected errors."));
    protocol::DisconnectClientBuilder builder(fbb);
    builder.add_disconnect_type(static_cast<int>(ray::rpc::WorkerExitType::SYSTEM_ERROR));
    builder.add_disconnect_detail(disconnect_detail);
    fbb.Finish(builder.Finish());
    std::vector<uint8_t> error_data(fbb.GetBufferPointer(),
                                    fbb.GetBufferPointer() + fbb.GetSize());
    read_type_ = error_message_type_;
    read_message_ = error_data;
  }

  int64_t start_ms = current_time_ms();
  message_handler_(shared_ClientConnection_from_this(), read_type_, read_message_);
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
