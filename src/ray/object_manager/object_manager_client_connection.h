#ifndef RAY_OBJECT_MANAGER_OBJECT_MANAGER_CLIENT_CONNECTION_H
#define RAY_OBJECT_MANAGER_OBJECT_MANAGER_CLIENT_CONNECTION_H

#include <deque>
#include <memory>
#include <unordered_map>

#include <boost/asio.hpp>
#include <boost/asio/error.hpp>
#include <boost/enable_shared_from_this.hpp>

#include "ray/id.h"
// #include "common/state/ray_config.h"

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

  void WriteMessage(int64_t type, size_t length, const uint8_t *message) {
    std::vector<boost::asio::const_buffer> message_buffers;
    // TODO (hme): Don't hard code this..
    write_version_ = 0x0000000000000000;
    // write_version_ = RayConfig::instance().ray_protocol_version();
    write_type_ = type;
    write_length_ = length;
    write_message_.assign(message, message + length);
    message_buffers.push_back(boost::asio::buffer(&write_version_, sizeof(write_version_)));
    message_buffers.push_back(boost::asio::buffer(&write_type_, sizeof(write_type_)));
    message_buffers.push_back(boost::asio::buffer(&write_length_, sizeof(write_length_)));
    message_buffers.push_back(boost::asio::buffer(write_message_));
    boost::system::error_code error;
    boost::asio::write(socket_, message_buffers, error);
    assert(error.value() == 0);
  }

 private:
  int64_t write_version_;
  int64_t write_type_;
  uint64_t write_length_;
  std::vector<uint8_t> write_message_;

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
