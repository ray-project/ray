#include "client_connection.h"

#include <stdio.h>
#include <boost/bind.hpp>
#include <sstream>

#include "ray/common/ray_config.h"
#include "ray/util/util.h"

namespace ray {

ray::Status TcpConnect(boost::asio::ip::tcp::socket &socket,
                       const std::string &ip_address_string, int port) {
  // Disable Nagle's algorithm, which caused transfer delays of 10s of ms in
  // certain cases.
  socket.open(boost::asio::ip::tcp::v4());
  boost::asio::ip::tcp::no_delay option(true);
  socket.set_option(option);

  boost::asio::ip::address ip_address =
      boost::asio::ip::address::from_string(ip_address_string);
  boost::asio::ip::tcp::endpoint endpoint(ip_address, port);
  boost::system::error_code error;
  socket.connect(endpoint, error);
  const auto status = boost_to_ray_status(error);
  if (!status.ok()) {
    // Close the socket if the connect failed.
    boost::system::error_code close_error;
    socket.close(close_error);
  }
  return status;
}

template <class T>
std::shared_ptr<ServerConnection<T>> ServerConnection<T>::Create(
    boost::asio::basic_stream_socket<T> &&socket) {
  std::shared_ptr<ServerConnection<T>> self(new ServerConnection(std::move(socket)));
  return self;
}

template <class T>
ServerConnection<T>::ServerConnection(boost::asio::basic_stream_socket<T> &&socket)
    : socket_(std::move(socket)),
      async_write_max_messages_(1),
      async_write_queue_(),
      async_write_in_flight_(false),
      async_write_broken_pipe_(false) {}

template <class T>
ServerConnection<T>::~ServerConnection() {
  // If there are any pending messages, invoke their callbacks with an IOError status.
  for (const auto &write_buffer : async_write_queue_) {
    write_buffer->handler(Status::IOError("Connection closed."));
  }
}

template <class T>
Status ServerConnection<T>::WriteBuffer(
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

template <class T>
Status ServerConnection<T>::ReadBuffer(
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

template <class T>
ray::Status ServerConnection<T>::WriteMessage(int64_t type, int64_t length,
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

template <class T>
void ServerConnection<T>::WriteMessageAsync(
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

template <class T>
void ServerConnection<T>::DoAsyncWrites() {
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
      ServerConnection<T>::socket_, message_buffers,
      [this, this_ptr, num_messages, call_handlers](
          const boost::system::error_code &error, size_t bytes_transferred) {
        ray::Status status = boost_to_ray_status(error);
        if (error.value() == boost::system::errc::errc_t::broken_pipe) {
          RAY_LOG(ERROR) << "Broken Pipe happened during calling "
                         << "ServerConnection<T>::DoAsyncWrites.";
          // From now on, calling DoAsyncWrites will directly call the handler
          // with this broken-pipe status.
          async_write_broken_pipe_ = true;
        } else if (!status.ok()) {
          RAY_LOG(ERROR) << "Error encountered during calling "
                         << "ServerConnection<T>::DoAsyncWrites, message: "
                         << status.message()
                         << ", error code: " << static_cast<int>(error.value());
        }
        call_handlers(status, num_messages);
      });
}

template <class T>
std::shared_ptr<ClientConnection<T>> ClientConnection<T>::Create(
    ClientHandler<T> &client_handler, MessageHandler<T> &message_handler,
    boost::asio::basic_stream_socket<T> &&socket, const std::string &debug_label,
    const std::vector<std::string> &message_type_enum_names, int64_t error_message_type) {
  std::shared_ptr<ClientConnection<T>> self(
      new ClientConnection(message_handler, std::move(socket), debug_label,
                           message_type_enum_names, error_message_type));
  // Let our manager process our new connection.
  client_handler(*self);
  return self;
}

template <class T>
ClientConnection<T>::ClientConnection(
    MessageHandler<T> &message_handler, boost::asio::basic_stream_socket<T> &&socket,
    const std::string &debug_label,
    const std::vector<std::string> &message_type_enum_names, int64_t error_message_type)
    : ServerConnection<T>(std::move(socket)),
      registered_(false),
      message_handler_(message_handler),
      debug_label_(debug_label),
      message_type_enum_names_(message_type_enum_names),
      error_message_type_(error_message_type) {}

template <class T>
void ClientConnection<T>::Register() {
  RAY_CHECK(!registered_);
  registered_ = true;
}

template <class T>
void ClientConnection<T>::ProcessMessages() {
  // Wait for a message header from the client. The message header includes the
  // protocol version, the message type, and the length of the message.
  std::vector<boost::asio::mutable_buffer> header;
  header.push_back(boost::asio::buffer(&read_cookie_, sizeof(read_cookie_)));
  header.push_back(boost::asio::buffer(&read_type_, sizeof(read_type_)));
  header.push_back(boost::asio::buffer(&read_length_, sizeof(read_length_)));
  boost::asio::async_read(
      ServerConnection<T>::socket_, header,
      boost::bind(&ClientConnection<T>::ProcessMessageHeader,
                  shared_ClientConnection_from_this(), boost::asio::placeholders::error));
}

template <class T>
void ClientConnection<T>::ProcessMessageHeader(const boost::system::error_code &error) {
  if (error) {
    // If there was an error, disconnect the client.
    read_type_ = error_message_type_;
    read_length_ = 0;
    ProcessMessage(error);
    return;
  }

  // If there was no error, make sure the ray cookie matches.
  if (!CheckRayCookie()) {
    ServerConnection<T>::Close();
    return;
  }

  // Resize the message buffer to match the received length.
  read_message_.resize(read_length_);
  ServerConnection<T>::bytes_read_ += read_length_;
  // Wait for the message to be read.
  boost::asio::async_read(
      ServerConnection<T>::socket_, boost::asio::buffer(read_message_),
      boost::bind(&ClientConnection<T>::ProcessMessage,
                  shared_ClientConnection_from_this(), boost::asio::placeholders::error));
}

template <class T>
bool ClientConnection<T>::CheckRayCookie() {
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

template <class T>
std::string ClientConnection<T>::RemoteEndpointInfo() {
  return std::string();
}

template <>
std::string ClientConnection<boost::asio::ip::tcp>::RemoteEndpointInfo() {
  const auto &remote_endpoint =
      ServerConnection<boost::asio::ip::tcp>::socket_.remote_endpoint();
  return remote_endpoint.address().to_string() + ":" +
         std::to_string(remote_endpoint.port());
}

template <class T>
void ClientConnection<T>::ProcessMessage(const boost::system::error_code &error) {
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

template <class T>
std::string ServerConnection<T>::DebugString() const {
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

template class ServerConnection<boost::asio::local::stream_protocol>;
template class ServerConnection<boost::asio::ip::tcp>;
template class ClientConnection<boost::asio::local::stream_protocol>;
template class ClientConnection<boost::asio::ip::tcp>;

}  // namespace ray
