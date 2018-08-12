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

class SenderConnection : public boost::enable_shared_from_this<SenderConnection> {
 public:
  /// Create a connection for sending data to other object managers.
  ///
  /// \param io_service The service to which the created socket should attach.
  /// \param client_id The ClientID of the remote node.
  /// \param ip The ip address of the remote node server.
  /// \param port The port of the remote node server.
  /// \return A connection to the remote object manager. This is null if the
  /// connection was unsuccessful.
  static std::shared_ptr<SenderConnection> Create(boost::asio::io_service &io_service,
                                                  const ClientID &client_id,
                                                  const std::string &ip, uint16_t port);

  /// \param socket A reference to the socket created by the static Create method.
  /// \param client_id The ClientID of the remote node.
  SenderConnection(std::shared_ptr<TcpServerConnection> conn, const ClientID &client_id);

  /// Write a message to the client.
  ///
  /// \param type The message type (e.g., a flatbuffer enum).
  /// \param length The size in bytes of the message.
  /// \param message A pointer to the message buffer.
  /// \return Status.
  ray::Status WriteMessage(int64_t type, uint64_t length, const uint8_t *message) {
    return conn_->WriteMessage(type, length, message);
  }

  /// Write a buffer to this connection.
  ///
  /// \param buffer The buffer.
  /// \param ec The error code object in which to store error codes.
  Status WriteBuffer(const std::vector<boost::asio::const_buffer> &buffer) {
    return conn_->WriteBuffer(buffer);
  }

  /// Read a buffer from this connection.
  ///
  /// \param buffer The buffer.
  /// \param ec The error code object in which to store error codes.
  void ReadBuffer(const std::vector<boost::asio::mutable_buffer> &buffer,
                  boost::system::error_code &ec) {
    return conn_->ReadBuffer(buffer, ec);
  }

  /// \return The ClientID of this connection.
  const ClientID &GetClientID() { return client_id_; }

 private:
  bool operator==(const SenderConnection &rhs) const {
    return connection_id_ == rhs.connection_id_;
  }

  static uint64_t id_counter_;
  uint64_t connection_id_;
  ClientID client_id_;
  std::shared_ptr<TcpServerConnection> conn_;
};

}  // namespace ray

#endif  // RAY_OBJECT_MANAGER_OBJECT_MANAGER_CLIENT_CONNECTION_H
