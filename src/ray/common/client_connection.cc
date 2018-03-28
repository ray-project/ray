#include "client_connection.h"

#include <boost/bind.hpp>

#include "common.h"
#include "ray/raylet/format/node_manager_generated.h"

namespace ray {

template <class T>
std::shared_ptr<ClientConnection<T>> ClientConnection<T>::Create(
    ClientManager<T> &manager, boost::asio::basic_stream_socket<T> &&socket) {
  std::shared_ptr<ClientConnection<T>> self(
      new ClientConnection(manager, std::move(socket)));
  // Let our manager process our new connection.
  self->manager_.ProcessNewClient(self);
  return self;
}

template <class T>
ClientConnection<T>::ClientConnection(ClientManager<T> &manager,
                                      boost::asio::basic_stream_socket<T> &&socket)
    : socket_(std::move(socket)), manager_(manager) {}

template <class T>
void ClientConnection<T>::ProcessMessages() {
  // Wait for a message header from the client. The message header includes the
  // protocol version, the message type, and the length of the message.
  std::vector<boost::asio::mutable_buffer> header;
  header.push_back(boost::asio::buffer(&read_version_, sizeof(read_version_)));
  header.push_back(boost::asio::buffer(&read_type_, sizeof(read_type_)));
  header.push_back(boost::asio::buffer(&read_length_, sizeof(read_length_)));
  boost::asio::async_read(
      socket_, header,
      boost::bind(&ClientConnection<T>::ProcessMessageHeader, this->shared_from_this(),
                  boost::asio::placeholders::error));
}

template <class T>
void ClientConnection<T>::ProcessMessageHeader(const boost::system::error_code &error) {
  if (error) {
    // If there was an error, disconnect the client.
    read_type_ = protocol::MessageType_DisconnectClient;
    read_length_ = 0;
    ProcessMessage(error);
    return;
  }

  // If there was no error, make sure the protocol version matches.
  RAY_CHECK(read_version_ == RayConfig::instance().ray_protocol_version());
  // Resize the message buffer to match the received length.
  read_message_.resize(read_length_);
  // Wait for the message to be read.
  boost::asio::async_read(
      socket_, boost::asio::buffer(read_message_),
      boost::bind(&ClientConnection<T>::ProcessMessage, this->shared_from_this(),
                  boost::asio::placeholders::error));
}

template <class T>
void ClientConnection<T>::WriteMessage(int64_t type, size_t length,
                                       const uint8_t *message) {
  std::vector<boost::asio::const_buffer> message_buffers;
  write_version_ = RayConfig::instance().ray_protocol_version();
  write_type_ = type;
  write_length_ = length;
  write_message_.assign(message, message + length);
  message_buffers.push_back(boost::asio::buffer(&write_version_, sizeof(write_version_)));
  message_buffers.push_back(boost::asio::buffer(&write_type_, sizeof(write_type_)));
  message_buffers.push_back(boost::asio::buffer(&write_length_, sizeof(write_length_)));
  message_buffers.push_back(boost::asio::buffer(write_message_));
  boost::system::error_code error;
  // Write the message and then wait for more messages.
  boost::asio::async_write(
      socket_, message_buffers,
      boost::bind(&ClientConnection<T>::ProcessMessages, this->shared_from_this(),
                  boost::asio::placeholders::error));
}

template <class T>
void ClientConnection<T>::ProcessMessage(const boost::system::error_code &error) {
  if (error) {
    // TODO(hme): Disconnect differently & remove dependency on node_manager_generated.h
    read_type_ = protocol::MessageType_DisconnectClient;
  }
  manager_.ProcessClientMessage(this->shared_from_this(), read_type_,
                                read_message_.data());
}

template <class T>
void ClientConnection<T>::ProcessMessages(const boost::system::error_code &error) {
  if (error) {
    ProcessMessage(error);
  } else {
    ProcessMessages();
  }
}

template class ClientConnection<boost::asio::local::stream_protocol>;
template class ClientConnection<boost::asio::ip::tcp>;

template <class T>
ClientManager<T>::~ClientManager<T>() {}

template class ClientManager<boost::asio::local::stream_protocol>;
template class ClientManager<boost::asio::ip::tcp>;

}  // namespace ray
