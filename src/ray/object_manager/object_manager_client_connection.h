#ifndef RAY_OBJECT_MANAGER_OBJECT_MANAGER_CLIENT_CONNECTION_H
#define RAY_OBJECT_MANAGER_OBJECT_MANAGER_CLIENT_CONNECTION_H

#include <deque>
#include <memory>
#include <unordered_map>

#include <boost/asio.hpp>
#include <boost/asio/error.hpp>
#include <boost/bind.hpp>
#include <boost/enable_shared_from_this.hpp>

#include "common/state/ray_config.h"
#include "ray/common/client_connection.h"
#include "ray/id.h"

namespace ray {

class SenderConnection : public ServerConnection<boost::asio::ip::tcp>,
                         public boost::enable_shared_from_this<SenderConnection> {
 public:
  /// Pointer type for this class.
  typedef boost::shared_ptr<SenderConnection> pointer;

  /// Create a connection for sending data to other object managers.
  ///
  /// \param io_service The service to which the created socket should attach.
  /// \param client_id The ClientID of the remote node.
  /// \param ip The ip address of the remote node server.
  /// \param port The port of the remote node server.
  /// \return A connection to the remote object manager.
  static pointer Create(boost::asio::io_service &io_service, const ClientID &client_id,
                        const std::string &ip, uint16_t port);

  /// \param socket A reference to the socket created by the static Create method.
  /// \param client_id The ClientID of the remote node.
  explicit SenderConnection(
      boost::asio::basic_stream_socket<boost::asio::ip::tcp> &&socket,
      const ClientID &client_id);

  /// \return Socket for this connection.
  boost::asio::ip::tcp::socket &GetSocket();

  /// \return The ClientID of this connection.
  const ClientID &GetClientID() { return client_id_; }

 private:
  friend bool operator==(const SenderConnection &conn1, const SenderConnection &conn2) {
    return conn1.connection_id_ == conn2.connection_id_;
  }

  static uint64_t id_counter_;
  uint64_t connection_id_;
  ClientID client_id_;
};

class ReceiverConnection;

using ReceiverClientHandler = std::function<void(std::shared_ptr<ReceiverConnection>)>;

using ReceiverMessageHandler =
    std::function<void(std::shared_ptr<ReceiverConnection>, int64_t, const uint8_t *)>;

// TODO(hme): Implement ClientConnection by composition.
// TODO(hme): Document and move implementation to .cc file once this is finalized.
class ReceiverConnection : public ServerConnection<boost::asio::ip::tcp>,
                           public std::enable_shared_from_this<ReceiverConnection> {
 public:
  static std::shared_ptr<ReceiverConnection> Create(
      ReceiverClientHandler &client_handler, ReceiverMessageHandler &message_handler,
      boost::asio::basic_stream_socket<boost::asio::ip::tcp> &&socket) {
    std::shared_ptr<ReceiverConnection> self(
        new ReceiverConnection(message_handler, std::move(socket)));
    client_handler(self);
    return self;
  }

  boost::asio::basic_stream_socket<boost::asio::ip::tcp> &GetSocket() { return socket_; }

  void ProcessMessages() {
    // Wait for a message header from the client. The message header includes the
    // protocol version, the message type, and the length of the message.
    std::vector<boost::asio::mutable_buffer> header;
    header.push_back(boost::asio::buffer(&read_version_, sizeof(read_version_)));
    header.push_back(boost::asio::buffer(&read_type_, sizeof(read_type_)));
    header.push_back(boost::asio::buffer(&read_length_, sizeof(read_length_)));
    boost::asio::async_read(
        ServerConnection<boost::asio::ip::tcp>::socket_, header,
        boost::bind(&ReceiverConnection::ProcessMessageHeader, this->shared_from_this(),
                    boost::asio::placeholders::error));
  }

  const ClientID &GetClientID() { return client_id_; }

  void SetClientID(const ClientID &client_id) { client_id_ = client_id; }

 private:
  ReceiverConnection(ReceiverMessageHandler &message_handler,
                     boost::asio::basic_stream_socket<boost::asio::ip::tcp> &&socket)
      : ServerConnection<boost::asio::ip::tcp>(std::move(socket)),
        message_handler_(message_handler) {}

  void ProcessMessageHeader(const boost::system::error_code &error) {
    if (error) {
      // TODO(hme): Disconnect.
      return;
    }

    // If there was no error, make sure the protocol version matches.
    RAY_CHECK(read_version_ == RayConfig::instance().ray_protocol_version());

    // Resize the message buffer to match the received length.
    read_message_.resize(read_length_);
    // Wait for the message to be read.
    boost::asio::async_read(
        ServerConnection<boost::asio::ip::tcp>::socket_,
        boost::asio::buffer(read_message_),
        boost::bind(&ReceiverConnection::ProcessMessage, this->shared_from_this(),
                    boost::asio::placeholders::error));
  }

  void ProcessMessage(const boost::system::error_code &error) {
    if (error) {
      // TODO(hme): Disconnect.
      return;
    }
    message_handler_(
        std::static_pointer_cast<ReceiverConnection>(this->shared_from_this()),
        read_type_, read_message_.data());
  }

  /// The handler for a message from the client.
  ReceiverMessageHandler message_handler_;
  /// Buffers for the current message being read from the client.
  // This is okay since handlers for distinct connections are executed
  // concurrently. Handlers for the same connection will never execute
  // concurrently. This can only happen if the sender writes messages
  // concurrently, which would be a bug.
  int64_t read_version_;
  int64_t read_type_;
  uint64_t read_length_;
  std::vector<uint8_t> read_message_;

  ClientID client_id_;
};

}  // namespace ray

#endif  // RAY_OBJECT_MANAGER_OBJECT_MANAGER_CLIENT_CONNECTION_H
