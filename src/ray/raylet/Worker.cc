#ifndef WORKER_CC
#define WORKER_CC

#include "Worker.h"

#include <boost/bind.hpp>

#include "common.h"
#include "format/nm_generated.h"

using namespace std;
namespace ray {

ClientConnection::ClientConnection(
        boost::asio::local::stream_protocol::socket &&socket,
        boost::function<void (pid_t)> worker_registered_handler)
  : socket_(std::move(socket)) {
  worker_registered_handler_ = worker_registered_handler;
}

void ClientConnection::ProcessMessages() {
  // Wait for a message header from the client. The message header includes the
  // protocol version, the message type, and the length of the message.
  std::vector<boost::asio::mutable_buffer> header;
  header.push_back(boost::asio::buffer(&version_, sizeof(version_)));
  header.push_back(boost::asio::buffer(&type_, sizeof(type_)));
  header.push_back(boost::asio::buffer(&length_, sizeof(length_)));
  boost::asio::async_read(socket_, header, boost::bind(&ClientConnection::processMessageHeader, this, boost::asio::placeholders::error));
}

void ClientConnection::processMessageHeader(const boost::system::error_code& error) {
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
      boost::bind(&ClientConnection::processMessage, this, boost::asio::placeholders::error)
      );
}

void ClientConnection::processMessage(const boost::system::error_code& error) {
  if (error) {
    type_ = MessageType_DisconnectClient;
  }

  /* Processing */
  std::cout << "hello" << std::endl;
  switch (type_) {
  case MessageType_RegisterClientRequest: {
    auto message = flatbuffers::GetRoot<RegisterClientRequest>(message_.data());
    worker_registered_handler_(message->worker_pid());
  } break;
  case MessageType_DisconnectClient: {
    LOG_INFO("Client disconnecting");
    return;
  } break;
  default:
    CHECK(0);
  }

  ProcessMessages();
}


/// A constructor responsible for initializing the state of a worker.
Worker::Worker() {

}

} // end namespace ray

#endif
