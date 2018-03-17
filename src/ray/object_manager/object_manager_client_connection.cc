#include "object_manager_client_connection.h"

namespace ray {

SenderConnection::pointer SenderConnection::Create(boost::asio::io_service &io_service,
                                                   const std::string &ip, uint16_t port) {
  boost::asio::ip::tcp::socket socket(io_service);
  RAY_CHECK_OK(TcpConnect(socket, ip, port));
  return pointer(new SenderConnection(std::move(socket)));
};

SenderConnection::SenderConnection(boost::asio::basic_stream_socket<boost::asio::ip::tcp> &&socket)
    : ServerConnection<boost::asio::ip::tcp>(std::move(socket)), send_queue_() {
};

boost::asio::ip::tcp::socket &SenderConnection::GetSocket() { return socket_; };

bool SenderConnection::IsObjectIdQueueEmpty() { return send_queue_.empty(); }

bool SenderConnection::ObjectIdQueued(const ObjectID &object_id) {
  return std::find(send_queue_.begin(), send_queue_.end(), object_id) !=
         send_queue_.end();
}

void SenderConnection::QueueObjectId(const ObjectID &object_id) {
  send_queue_.push_back(ObjectID(object_id));
}

ObjectID SenderConnection::DequeueObjectId() {
  ObjectID object_id = send_queue_.front();
  send_queue_.pop_front();
  return object_id;
}

void SenderConnection::AddSendRequest(const ObjectID &object_id,
                                      SendRequest &send_request) {
  send_requests_.emplace(object_id, send_request);
}

void SenderConnection::RemoveSendRequest(const ObjectID &object_id) {
  send_requests_.erase(object_id);
}

SendRequest &SenderConnection::GetSendRequest(const ObjectID &object_id) {
  return send_requests_[object_id];
};

}  // namespace ray
