#include "object_manager_client_connection.h"

namespace ray {

SenderConnection::pointer SenderConnection::Create(boost::asio::io_service &io_service,
                                                   const std::string &ip, uint16_t port) {
  return pointer(new SenderConnection(io_service, ip, port));
};

SenderConnection::SenderConnection(boost::asio::io_service &io_service,
                                   const std::string &ip, uint16_t port)
    : socket_(io_service), send_queue_() {
  boost::asio::ip::address ip_address = boost::asio::ip::address::from_string(ip);
  boost::asio::ip::tcp::endpoint endpoint(ip_address, port);
  socket_.connect(endpoint);
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

TCPClientConnection::TCPClientConnection(boost::asio::io_service &io_service)
    : socket_(io_service) {}

TCPClientConnection::pointer TCPClientConnection::Create(
    boost::asio::io_service &io_service) {
  return TCPClientConnection::pointer(new TCPClientConnection(io_service));
}

boost::asio::ip::tcp::socket &TCPClientConnection::GetSocket() { return socket_; }
}  // namespace ray
