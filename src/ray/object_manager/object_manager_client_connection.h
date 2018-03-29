#ifndef RAY_OBJECT_MANAGER_OBJECT_MANAGER_CLIENT_CONNECTION_H
#define RAY_OBJECT_MANAGER_OBJECT_MANAGER_CLIENT_CONNECTION_H

#include <deque>
#include <memory>
#include <unordered_map>

#include <boost/asio.hpp>
#include <boost/asio/error.hpp>
#include <boost/enable_shared_from_this.hpp>

#include "ray/id.h"

namespace ray {

struct SendRequest {
  ObjectID object_id;
  ClientID client_id;
  int64_t object_size;
  uint8_t *data;
};

// TODO(hme): Document public API after integration with common connection.
class SenderConnection : public boost::enable_shared_from_this<SenderConnection> {
 public:
  typedef boost::shared_ptr<SenderConnection> pointer;
  typedef std::unordered_map<ray::ObjectID, SendRequest, UniqueIDHasher> SendRequestsType;
  typedef std::deque<ray::ObjectID> SendQueueType;

  static pointer Create(boost::asio::io_service &io_service, const std::string &ip,
                        uint16_t port);

  explicit SenderConnection(boost::asio::io_service &io_service, const std::string &ip,
                            uint16_t port);

  boost::asio::ip::tcp::socket &GetSocket();

  bool IsObjectIdQueueEmpty();
  bool ObjectIdQueued(const ObjectID &object_id);
  void QueueObjectId(const ObjectID &object_id);
  ObjectID DequeueObjectId();

  void AddSendRequest(const ObjectID &object_id, SendRequest &send_request);
  void RemoveSendRequest(const ObjectID &object_id);
  SendRequest &GetSendRequest(const ObjectID &object_id);

 private:
  boost::asio::ip::tcp::socket socket_;
  SendQueueType send_queue_;
  SendRequestsType send_requests_;
};

// TODO(hme): Document public API after integration with common connection.
class TCPClientConnection : public boost::enable_shared_from_this<TCPClientConnection> {
 public:
  typedef boost::shared_ptr<TCPClientConnection> pointer;
  static pointer Create(boost::asio::io_service &io_service);
  boost::asio::ip::tcp::socket &GetSocket();

  TCPClientConnection(boost::asio::io_service &io_service);

  int64_t message_type_;
  uint64_t message_length_;

 private:
  boost::asio::ip::tcp::socket socket_;
};

}  // namespace ray

#endif  // RAY_OBJECT_MANAGER_OBJECT_MANAGER_CLIENT_CONNECTION_H
