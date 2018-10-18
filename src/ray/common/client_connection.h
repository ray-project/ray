#ifndef RAY_COMMON_CLIENT_CONNECTION_H
#define RAY_COMMON_CLIENT_CONNECTION_H

#include <list>
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
class ServerConnection : public std::enable_shared_from_this<ServerConnection<T>> {
 public:
  /// Allocate a new server connection.
  ///
  /// \param socket A reference to the server socket.
  /// \return std::shared_ptr<ServerConnection>.
  static std::shared_ptr<ServerConnection<T>> Create(boost::asio::basic_stream_socket<T> &&socket);

  /// Write a message to the client.
  ///
  /// \param type The message type (e.g., a flatbuffer enum).
  /// \param length The size in bytes of the message.
  /// \param message A pointer to the message buffer.
  /// \return Status.
  ray::Status WriteMessage(int64_t type, int64_t length, const uint8_t *message);

  /// Write a message to the client asynchronously.
  ///
  /// \param type The message type (e.g., a flatbuffer enum).
  /// \param length The size in bytes of the message.
  /// \param message A pointer to the message buffer.
  /// \param handler A callback to run on write completion.
  void WriteMessageAsync(int64_t type, int64_t length, const uint8_t *message,
                         const std::function<void(const ray::Status &)> &handler);

  /// Write a buffer to this connection.
  ///
  /// \param buffer The buffer.
  /// \param ec The error code object in which to store error codes.
  Status WriteBuffer(const std::vector<boost::asio::const_buffer> &buffer);

  /// Read a buffer from this connection.
  ///
  /// \param buffer The buffer.
  /// \param ec The error code object in which to store error codes.
  void ReadBuffer(const std::vector<boost::asio::mutable_buffer> &buffer,
                  boost::system::error_code &ec);

  /// Shuts down socket for this connection.
  void Close() {
    boost::system::error_code ec;
    socket_.close(ec);
  }

 protected:
  /// A private constructor for a server connection.
  ServerConnection(boost::asio::basic_stream_socket<T> &&socket);

  /// A message that is queued for writing asynchronously.
  struct AsyncWriteBuffer {
    int64_t write_version;
    int64_t write_type;
    uint64_t write_length;
    std::vector<uint8_t> write_message;
    std::function<void(const ray::Status &)> handler;
  };

  /// The socket connection to the server.
  boost::asio::basic_stream_socket<T> socket_;

  /// Max number of messages to write out at once.
  const int async_write_max_messages_;

  /// List of pending messages to write.
  std::list<std::unique_ptr<AsyncWriteBuffer>> async_write_queue_;

  /// Whether we are in the middle of an async write.
  bool async_write_in_flight_;

 private:
  /// Asynchronously flushes the write queue. While async writes are running, the flag
  /// async_write_in_flight_ will be set. This should only be called when no async writes
  /// are currently in flight.
  void DoAsyncWrites();
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
class ClientConnection : public ServerConnection<T> {
 public:
  using std::enable_shared_from_this<ServerConnection<T>>::shared_from_this;

  /// Allocate a new node client connection.
  ///
  /// \param new_client_handler A reference to the client handler.
  /// \param message_handler A reference to the message handler.
  /// \param socket The client socket.
  /// \return std::shared_ptr<ClientConnection>.
  static std::shared_ptr<ClientConnection<T>> Create(
      ClientHandler<T> &new_client_handler, MessageHandler<T> &message_handler,
      boost::asio::basic_stream_socket<T> &&socket, const std::string &debug_label);

  std::shared_ptr<ClientConnection<T>> shared_ClientConnection_from_this() {
    return std::static_pointer_cast<ClientConnection<T>>(shared_from_this());
  }

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
                   boost::asio::basic_stream_socket<T> &&socket,
                   const std::string &debug_label);
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
  /// A label used for debug messages.
  const std::string debug_label_;
  /// Buffers for the current message being read from the client.
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
