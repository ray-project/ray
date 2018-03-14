#ifndef RAY_COMMON_CLIENT_CONNECTION_H
#define RAY_COMMON_CLIENT_CONNECTION_H

#include <memory>

#include <boost/asio.hpp>
#include <boost/asio/error.hpp>
#include <boost/enable_shared_from_this.hpp>

namespace ray {

class ServerConnection {
 public:
  ServerConnection(boost::asio::io_service &io_service, const std::string &ip, int port);

  void WriteMessage(int64_t type, size_t length, const uint8_t *message);

 private:
  boost::asio::ip::tcp::socket socket_;
  /// Buffers for the current message being written to the client.
  int64_t write_version_;
  int64_t write_type_;
  uint64_t write_length_;
  std::vector<uint8_t> write_message_;
};

/// \class ClientConnection
///
/// A generic type representing a client connection on a server. This class can
/// be used to process and write messages asynchronously from and to the
/// client.
template <class T>
class ClientConnection : public std::enable_shared_from_this<ClientConnection<T>> {
  using ClientHandler = std::function<void(std::shared_ptr<ClientConnection<T>>)>;
  using MessageHandler =
      std::function<void(std::shared_ptr<ClientConnection<T>>, int64_t, const uint8_t *)>;

 public:
  /// Allocate a new node client connection.
  ///
  /// \param ClientManager A reference to the manager that will process a
  /// message from this client.
  /// \param socket The client socket.
  /// \return std::shared_ptr<ClientConnection>.
  static std::shared_ptr<ClientConnection<T>> Create(
      ClientHandler &new_client_handler, MessageHandler &&message_handler,
      boost::asio::basic_stream_socket<T> &&socket);

  /// Listen for and process messages from the client connection. Once a
  /// message has been fully received, the client manager's
  /// ProcessClientMessage handler will be called.
  void ProcessMessages();

  /// Write a message to the client and then listen for more messages.
  ///
  /// \param type The message type (e.g., a flatbuffer enum).
  /// \param length The size in bytes of the message.
  /// \param message A pointer to the message buffer. This will be copied into
  /// the ClientConnection's buffer.
  void WriteMessage(int64_t type, size_t length, const uint8_t *message);

 private:
  /// A private constructor for a node client connection.
  ClientConnection(MessageHandler &&message_handler,
                   boost::asio::basic_stream_socket<T> &&socket);
  /// Process an error from the last operation, then process the  message
  /// header from the client.
  void ProcessMessageHeader(const boost::system::error_code &error);
  /// Process an error from reading the message header, then process the
  /// message from the client.
  void ProcessMessage(const boost::system::error_code &error);
  /// Process an error from the last operation and then listen for more
  /// messages.
  void ProcessMessages(const boost::system::error_code &error);

  /// The client socket.
  boost::asio::basic_stream_socket<T> socket_;
  MessageHandler message_handler_;
  /// Buffers for the current message being read rom the client.
  int64_t read_version_;
  int64_t read_type_;
  uint64_t read_length_;
  std::vector<uint8_t> read_message_;
  /// Buffers for the current message being written to the client.
  int64_t write_version_;
  int64_t write_type_;
  uint64_t write_length_;
  std::vector<uint8_t> write_message_;
};

using LocalClientConnection = ClientConnection<boost::asio::local::stream_protocol>;
using TcpClientConnection = ClientConnection<boost::asio::ip::tcp>;

}  // namespace ray

#endif  // RAY_COMMON_CLIENT_CONNECTION_H
