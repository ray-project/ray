#include <iostream>

#include <boost/bind.hpp>

#include "common.h"
#include "format/nm_generated.h"
#include "node_manager.h"

NodeServer::NodeServer(boost::asio::io_service& io_service, const std::string &socket_name)
    : acceptor_(io_service, boost::asio::local::stream_protocol::endpoint(socket_name)),
      socket_(io_service) {
  // Start listening for clients.
  doAccept();
}

void NodeServer::doAccept() {
  acceptor_.async_accept(socket_,
      boost::bind(&NodeServer::handleAccept, this, boost::asio::placeholders::error)
      );
}

void NodeServer::handleAccept(const boost::system::error_code& error) {
  if (!error) {
    // Accept a new client.
    // TODO(swang): Remove the client upon disconnection.
    auto new_connection = new NodeClientConnection(std::move(socket_));
    new_connection->ProcessMessages();
    clients_.push_back(std::unique_ptr<NodeClientConnection>(new_connection));
  }
  // We're ready to accept another client.
  doAccept();
}

NodeClientConnection::NodeClientConnection(boost::asio::local::stream_protocol::socket &&socket)
  : socket_(std::move(socket)) {
}

void NodeClientConnection::ProcessMessages() {
  // Wait for a message header from the client. The message header includes the
  // protocol version, the message type, and the length of the message.
  std::vector<boost::asio::mutable_buffer> header;
  header.push_back(boost::asio::buffer(&version_, sizeof(version_)));
  header.push_back(boost::asio::buffer(&type_, sizeof(type_)));
  header.push_back(boost::asio::buffer(&length_, sizeof(length_)));
  boost::asio::async_read(socket_, header, boost::bind(&NodeClientConnection::processMessageHeader, this, boost::asio::placeholders::error));
}

void NodeClientConnection::processMessageHeader(const boost::system::error_code& error) {
  if (error) {
    // If there was an error, disconnect the client.
    type_ = MessageType_DisconnectClient;
    length_ = 0;
  } else {
    // If there was no error, make sure the protocol version matches.
    CHECK(version_ == RayConfig::instance().ray_protocol_version());
  }
  LOG_INFO("Message of type %" PRId64, type_);
  // Resize the message buffer to match the received length.
  if (message_.size() < length_) {
    message_.resize(length_);
  }
  // Wait for the message to be read.
  boost::asio::async_read(socket_, boost::asio::buffer(message_),
      boost::bind(&NodeClientConnection::processMessage, this, boost::asio::placeholders::error)
      );
}

void NodeClientConnection::processMessage(const boost::system::error_code& error) {
  if (error) {
    type_ = MessageType_DisconnectClient;
  }

  /* Processing */
  std::cout << "hello" << std::endl;

  if (type_ != MessageType_DisconnectClient) {
    ProcessMessages();
  }
}

int main(int argc, char *argv[]) {
  CHECK(argc == 2);

  boost::asio::io_service io_service;
  NodeServer server(io_service, std::string(argv[1]));
  io_service.run();
}
