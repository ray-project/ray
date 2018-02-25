#ifndef CLIENT_CONNECTION_CC
#define CLIENT_CONNECTION_CC

#include "client_connection.h"

#include <boost/bind.hpp>

#include "common.h"
#include "format/nm_generated.h"
#include "node_manager.h"
#include "Worker.h"

using namespace std;
namespace ray {

shared_ptr<ClientConnection> ClientConnection::Create(
    ClientManager& manager,
    boost::asio::local::stream_protocol::socket &&socket) {
  return shared_ptr<ClientConnection>(new ClientConnection(manager, std::move(socket)));
}

ClientConnection::ClientConnection(
        ClientManager& manager,
        boost::asio::local::stream_protocol::socket &&socket)
  : socket_(std::move(socket)),
    manager_(manager) {
}

void ClientConnection::ProcessMessages() {
  // Wait for a message header from the client. The message header includes the
  // protocol version, the message type, and the length of the message.
  std::vector<boost::asio::mutable_buffer> header;
  header.push_back(boost::asio::buffer(&version_, sizeof(version_)));
  header.push_back(boost::asio::buffer(&type_, sizeof(type_)));
  header.push_back(boost::asio::buffer(&length_, sizeof(length_)));
  boost::asio::async_read(socket_, header, boost::bind(&ClientConnection::processMessageHeader, shared_from_this(), boost::asio::placeholders::error));
}

void ClientConnection::processMessageHeader(const boost::system::error_code& error) {
  if (error) {
    // If there was an error, disconnect the client.
    type_ = MessageType_DisconnectClient;
    length_ = 0;
    processMessage(error);
    return;
  }

  // If there was no error, make sure the protocol version matches.
  CHECK(version_ == RayConfig::instance().ray_protocol_version());
  // Resize the message buffer to match the received length.
  message_.resize(length_);
  // Wait for the message to be read.
  boost::asio::async_read(socket_, boost::asio::buffer(message_),
      boost::bind(&ClientConnection::processMessage, shared_from_this(), boost::asio::placeholders::error)
      );
}

void ClientConnection::WriteMessage(int64_t type, size_t length, const uint8_t *message) {
  std::vector<boost::asio::const_buffer> message_buffers;
  version_ = RayConfig::instance().ray_protocol_version();
  type_ = type;
  length_ = length;
  message_buffers.push_back(boost::asio::buffer(&version_, sizeof(version_)));
  message_buffers.push_back(boost::asio::buffer(&type_, sizeof(type_)));
  message_buffers.push_back(boost::asio::buffer(&length_, sizeof(length_)));
  message_buffers.push_back(boost::asio::buffer(message, length));
  boost::system::error_code error;
  // Write the message and then wait for more messages.
  boost::asio::async_write(socket_, message_buffers, boost::bind(&ClientConnection::processMessages, shared_from_this(), boost::asio::placeholders::error));
}

void ClientConnection::processMessage(const boost::system::error_code& error) {
  if (error) {
    type_ = MessageType_DisconnectClient;
  }
  manager_.ProcessClientMessage(shared_from_this(), type_, message_.data());
}

void ClientConnection::processMessages(const boost::system::error_code& error) {
  if (error) {
    processMessage(error);
  } else {
    ProcessMessages();
  }
}

TCPClientConnection::TCPClientConnection(boost::asio::io_service& io_service) : socket_(io_service) {}

TCPClientConnection::pointer TCPClientConnection::Create(boost::asio::io_service& io_service) {
  return TCPClientConnection::pointer(new TCPClientConnection(io_service));
}

boost::asio::ip::tcp::socket& TCPClientConnection::GetSocket() {
  return socket_;
}

} // end namespace ray

#endif
