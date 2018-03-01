#ifndef CLIENT_CONNECTION_CC
#define CLIENT_CONNECTION_CC

#include "client_connection.h"

#include <boost/bind.hpp>

#include "common.h"
#include "format/nm_generated.h"
#include "raylet.h"
#include "worker.h"

using namespace std;
namespace ray {

template <class T>
shared_ptr<ClientConnection<T>> ClientConnection<T>::Create(
    ClientManager<T>& manager,
    boost::asio::basic_stream_socket<T> &&socket) {
  return shared_ptr<ClientConnection<T>>(new ClientConnection(manager, std::move(socket)));
}

template <class T>
ClientConnection<T>::ClientConnection(
        ClientManager<T>& manager,
        boost::asio::basic_stream_socket<T> &&socket)
  : socket_(std::move(socket)),
    manager_(manager) {
}

template <class T>
void ClientConnection<T>::ProcessMessages() {
  // Wait for a message header from the client. The message header includes the
  // protocol version, the message type, and the length of the message.
  std::vector<boost::asio::mutable_buffer> header;
  header.push_back(boost::asio::buffer(&read_version_, sizeof(read_version_)));
  header.push_back(boost::asio::buffer(&read_type_, sizeof(read_type_)));
  header.push_back(boost::asio::buffer(&read_length_, sizeof(read_length_)));
  boost::asio::async_read(socket_, header, boost::bind(&ClientConnection<T>::processMessageHeader, this->shared_from_this(), boost::asio::placeholders::error));
}

template <class T>
void ClientConnection<T>::processMessageHeader(const boost::system::error_code& error) {
  if (error) {
    // If there was an error, disconnect the client.
    read_type_ = MessageType_DisconnectClient;
    read_length_ = 0;
    processMessage(error);
    return;
  }

  // If there was no error, make sure the protocol version matches.
  CHECK(read_version_ == RayConfig::instance().ray_protocol_version());
  // Resize the message buffer to match the received length.
  read_message_.resize(read_length_);
  // Wait for the message to be read.
  boost::asio::async_read(socket_, boost::asio::buffer(read_message_),
      boost::bind(&ClientConnection<T>::processMessage, this->shared_from_this(), boost::asio::placeholders::error)
      );
}

template <class T>
void ClientConnection<T>::WriteMessage(int64_t type, size_t length, const uint8_t *message) {
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
  boost::asio::async_write(socket_, message_buffers, boost::bind(&ClientConnection<T>::processMessages, this->shared_from_this(), boost::asio::placeholders::error));
}

template <class T>
void ClientConnection<T>::processMessage(const boost::system::error_code& error) {
  if (error) {
    read_type_ = MessageType_DisconnectClient;
  }
  manager_.ProcessClientMessage(this->shared_from_this(), read_type_, read_message_.data());
}

template <class T>
void ClientConnection<T>::processMessages(const boost::system::error_code& error) {
  if (error) {
    processMessage(error);
  } else {
    ProcessMessages();
  }
}

template class ClientConnection<boost::asio::local::stream_protocol>;
template class ClientConnection<boost::asio::ip::tcp>;

template <class T>
ClientManager<T>::~ClientManager<T>() {
}

template class ClientManager<boost::asio::local::stream_protocol>;
template class ClientManager<boost::asio::ip::tcp>;

} // end namespace ray

#endif
