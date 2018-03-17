#ifndef RAY_OBJECT_MANAGER_OBJECT_MANAGER_CLIENT_CONNECTION_H
#define RAY_OBJECT_MANAGER_OBJECT_MANAGER_CLIENT_CONNECTION_H

#include <deque>
#include <memory>
#include <unordered_map>

#include <boost/asio.hpp>
#include <boost/asio/error.hpp>
#include <boost/bind.hpp>
#include <boost/enable_shared_from_this.hpp>

#include "ray/id.h"
#include "ray/common/client_connection.h"
#include "common/state/ray_config.h"

namespace ray {

struct SendRequest {
  ObjectID object_id;
  ClientID client_id;
  int64_t object_size;
  uint8_t *data;
};

// TODO(hme): Document public API after integration with common connection.
class SenderConnection : public ServerConnection<boost::asio::ip::tcp>,
                         public boost::enable_shared_from_this<SenderConnection> {
 public:
  typedef boost::shared_ptr<SenderConnection> pointer;
  typedef std::unordered_map<ray::ObjectID, SendRequest, UniqueIDHasher> SendRequestsType;
  typedef std::deque<ray::ObjectID> SendQueueType;

  static pointer Create(boost::asio::io_service &io_service, const std::string &ip,
                        uint16_t port);

  explicit SenderConnection(boost::asio::basic_stream_socket<boost::asio::ip::tcp> &&socket);

  boost::asio::ip::tcp::socket &GetSocket();

  bool IsObjectIdQueueEmpty();
  bool ObjectIdQueued(const ObjectID &object_id);
  void QueueObjectId(const ObjectID &object_id);
  ObjectID DequeueObjectId();

  void AddSendRequest(const ObjectID &object_id, SendRequest &send_request);
  void RemoveSendRequest(const ObjectID &object_id);
  SendRequest &GetSendRequest(const ObjectID &object_id);

 private:
  SendQueueType send_queue_;
  SendRequestsType send_requests_;

};

class ObjectManagerClientConnection;

using ObjectManagerClientHandler = std::function<void(std::shared_ptr<ObjectManagerClientConnection>)>;

using ObjectManagerMessageHandler =
std::function<void(std::shared_ptr<ObjectManagerClientConnection>, int64_t, const uint8_t *)>;

// TODO(hme): Subclass ClientConnection?
class ObjectManagerClientConnection : public ServerConnection<boost::asio::ip::tcp>,
                                      public std::enable_shared_from_this<ObjectManagerClientConnection>{

 public:

  static std::shared_ptr<ObjectManagerClientConnection> Create(
      ObjectManagerClientHandler &client_handler,
      ObjectManagerMessageHandler &message_handler,
      boost::asio::basic_stream_socket<boost::asio::ip::tcp> &&socket) {
    std::shared_ptr<ObjectManagerClientConnection> self(
        new ObjectManagerClientConnection(message_handler, std::move(socket)));
    client_handler(self);
    return self;
  }

  boost::asio::basic_stream_socket<boost::asio::ip::tcp> &GetSocket(){
    return socket_;
  }

  void ProcessMessages() {
    // Wait for a message header from the client. The message header includes the
    // protocol version, the message type, and the length of the message.
    std::vector<boost::asio::mutable_buffer> header;
    header.push_back(boost::asio::buffer(&read_version_, sizeof(read_version_)));
    header.push_back(boost::asio::buffer(&read_type_, sizeof(read_type_)));
    header.push_back(boost::asio::buffer(&read_length_, sizeof(read_length_)));
    boost::asio::async_read(
        ServerConnection<boost::asio::ip::tcp>::socket_, header,
        boost::bind(&ObjectManagerClientConnection::ProcessMessageHeader, this->shared_from_this(),
                    boost::asio::placeholders::error));
  }

 private:

  ObjectManagerClientConnection(ObjectManagerMessageHandler &message_handler,
                                boost::asio::basic_stream_socket<boost::asio::ip::tcp> &&socket)
      : ServerConnection<boost::asio::ip::tcp>(std::move(socket)), message_handler_(message_handler){
  }

  void ProcessMessageHeader(const boost::system::error_code &error) {
    if (error) {
      // TODO(hme): Disconnect.
      return;
    }

    // If there was no error, make sure the protocol version matches.
    // RAY_CHECK(read_version_ == RayConfig::instance().ray_protocol_version());
    RAY_CHECK(read_version_ == 0x0000000000000000);

    // Resize the message buffer to match the received length.
    read_message_.resize(read_length_);
    // Wait for the message to be read.
    boost::asio::async_read(
        ServerConnection<boost::asio::ip::tcp>::socket_, boost::asio::buffer(read_message_),
        boost::bind(&ObjectManagerClientConnection::ProcessMessage, this->shared_from_this(),
                    boost::asio::placeholders::error));
  }

  void ProcessMessage(const boost::system::error_code &error) {
    if (error) {
      // TODO(hme): Disconnect.
      return;
    }
    message_handler_(std::static_pointer_cast<ObjectManagerClientConnection>(this->shared_from_this()),
                       read_type_, read_message_.data());
  }

  /// The handler for a message from the client.
  ObjectManagerMessageHandler message_handler_;
  /// Buffers for the current message being read rom the client.
  int64_t read_version_;
  int64_t read_type_;
  uint64_t read_length_;
  std::vector<uint8_t> read_message_;

};

}  // namespace ray

#endif  // RAY_OBJECT_MANAGER_OBJECT_MANAGER_CLIENT_CONNECTION_H
