#ifndef RAY_COMMON_CLIENT_CONNECTION_H
#define RAY_COMMON_CLIENT_CONNECTION_H

#include <memory>

#include <boost/asio.hpp>
#include <boost/asio/error.hpp>
#include <boost/enable_shared_from_this.hpp>

#include "ray/id.h"
#include "ray/status.h"

namespace ray {

/// Connect a TCP socket.
///
/// \param socket The socket to connect.
/// \param ip_address The IP address to connect to.
/// \param port The port to connect to.
/// \return Status.
ray::Status TcpConnect(boost::asio::ip::tcp::socket &socket,
                       const std::string &ip_address, int port);

/// \typename ServerConnection
///
/// A generic type representing a client connection to a server. This typename
/// can be used to write messages synchronously to the server.
template <typename T>
class ServerConnection {
 public:
  /// Create a connection to the server.
  ServerConnection(boost::asio::basic_stream_socket<T> &&socket);

  /// Write a message to the client.
  ///
  /// \param type The message type (e.g., a flatbuffer enum).
  /// \param length The size in bytes of the message.
  /// \param message A pointer to the message buffer.
  /// \return Status.
  ray::Status WriteMessage(int64_t type, int64_t length, const uint8_t *message);

  /// Write a buffer to this connection.
  ///
  /// \param buffer The buffer.
  /// \param ec The error code object in which to store error codes.
  void WriteBuffer(const std::vector<boost::asio::const_buffer> &buffer,
                   boost::system::error_code &ec);

  /// Read a buffer from this connection.
  ///
  /// \param buffer The buffer.
  /// \param ec The error code object in which to store error codes.
  void ReadBuffer(const std::vector<boost::asio::mutable_buffer> &buffer,
                  boost::system::error_code &ec);

 protected:
  /// The socket connection to the server.
  boost::asio::basic_stream_socket<T> socket_;
};

template <typename T>
class ClientConnection;

template <typename T>
using ClientHandler = std::function<void(ClientConnection<T> &)>;
template <typename T>
using MessageHandler =
    std::function<void(std::shared_ptr<ClientConnection<T>>, int64_t, const uint8_t *)>;

/// \typename ClientConnection
///
/// A generic type representing a client connection on a server. In addition to
/// writing messages to the client, like in ServerConnection, this typename can
/// also be used to process messages asynchronously from client.
template <typename T>
class ClientConnection : public ServerConnection<T>,
                         public std::enable_shared_from_this<ClientConnection<T>> {
 public:
  /// Allocate a new node client connection.
  ///
  /// \param new_client_handler A reference to the client handler.
  /// \param message_handler A reference to the message handler.
  /// \param socket The client socket.
  /// \return std::shared_ptr<ClientConnection>.
  static std::shared_ptr<ClientConnection<T>> Create(
      ClientHandler<T> &new_client_handler, MessageHandler<T> &message_handler,
      boost::asio::basic_stream_socket<T> &&socket);

  /// \return The ClientID of the remote client.
  const ClientID &GetClientID();

  /// \param client_id The ClientID of the remote client.
  void SetClientID(const ClientID &client_id);

  /// Listen for and process messages from the client connection. Once a
  /// message has been fully received, the client manager's
  /// ProcessClientMessage handler will be called.
  void ProcessMessages();

 private:
  /// A private constructor for a node client connection.
  ClientConnection(MessageHandler<T> &message_handler,
                   boost::asio::basic_stream_socket<T> &&socket);
  /// Process an error from the last operation, then process the  message
  /// header from the client.
  void ProcessMessageHeader(const boost::system::error_code &error);
  /// Process an error from reading the message header, then process the
  /// message from the client.
  void ProcessMessage(const boost::system::error_code &error);

  /// The ClientID of the remote client.
  ClientID client_id_;
  /// The handler for a message from the client.
  MessageHandler<T> message_handler_;
  /// Buffers for the current message being read rom the client.
  int64_t read_version_;
  int64_t read_type_;
  uint64_t read_length_;
  std::vector<uint8_t> read_message_;
};

using LocalServerConnection = ServerConnection<boost::asio::local::stream_protocol>;
using TcpServerConnection = ServerConnection<boost::asio::ip::tcp>;
using LocalClientConnection = ClientConnection<boost::asio::local::stream_protocol>;
using TcpClientConnection = ClientConnection<boost::asio::ip::tcp>;

}  // namespace ray

#endif  // RAY_COMMON_CLIENT_CONNECTION_H
