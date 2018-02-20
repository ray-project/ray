#ifndef WORKER_CC
#define WORKER_CC

#include "Worker.h"

#include <boost/bind.hpp>

#include "common.h"
#include "format/nm_generated.h"
#include "node_manager.h"
#include "Task.h"
#include "WorkerPool.h"

using namespace std;
namespace ray {

shared_ptr<ClientConnection> ClientConnection::Create(
    NodeServer& server,
    boost::asio::local::stream_protocol::socket &&socket,
    WorkerPool& worker_pool) {
  return shared_ptr<ClientConnection>(new ClientConnection(server, std::move(socket), worker_pool));
}

ClientConnection::ClientConnection(
        NodeServer& server,
        boost::asio::local::stream_protocol::socket &&socket,
        WorkerPool& worker_pool)
  : socket_(std::move(socket)),
    worker_pool_(worker_pool),
    server_(server) {
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
  if (message_.size() < length_) {
    message_.resize(length_);
  }
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

  LOG_INFO("Message of type %" PRId64, type_);

  switch (type_) {
  case MessageType_RegisterClientRequest: {
    auto message = flatbuffers::GetRoot<RegisterClientRequest>(message_.data());
    // Create a new worker from the registration request.
    Worker worker(message->worker_pid(), shared_from_this());
    // Add the new worker to the pool.
    worker_pool_.AddWorker(std::move(worker));

    // Reply to the worker's registration request. TODO(swang): This is legacy
    // code and should be removed once actor creation tasks are implemented.
    flatbuffers::FlatBufferBuilder fbb;
    auto reply =
        CreateRegisterClientReply(fbb, fbb.CreateVector(std::vector<int>()));
    fbb.Finish(reply);
    WriteMessage(MessageType_RegisterClientReply, fbb.GetSize(), fbb.GetBufferPointer());
  } break;
  case MessageType_DisconnectClient: {
    // Remove the dead worker from the pool.
    worker_pool_.RemoveWorker(shared_from_this());
  } break;
  case MessageType_SubmitTask: {
    // Read the task submitted by the client.
    auto message = flatbuffers::GetRoot<SubmitTaskRequest>(message_.data());
    TaskSpecification task_spec(message->task_spec()->data(), message->task_spec()->size());
    Task task(task_spec);
    // Submit the task to the local scheduler.
    server_.SubmitTask(task);
  } break;
  default:
    CHECK(0);
  }
}

void ClientConnection::processMessages(const boost::system::error_code& error) {
  if (error) {
    processMessage(error);
  } else {
    ProcessMessages();
  }
}

/// A constructor responsible for initializing the state of a worker.
Worker::Worker(pid_t pid, shared_ptr<ClientConnection> connection) {
  pid_ = pid;
  connection_ = connection;
}

pid_t Worker::Pid() {
  return pid_;
}

const shared_ptr<ClientConnection> Worker::Connection() {
  return connection_;
}

} // end namespace ray

#endif
