#ifndef CLIENT_CONNECTION_H
#define CLIENT_CONNECTION_H

#include <boost/asio.hpp>
#include <boost/asio/error.hpp>
#include <boost/enable_shared_from_this.hpp>
#include <memory>

namespace ray {

template <class T>
class ClientManager;

// A generic type representing a client connection on a server. This class can
// be used to process and write messages asynchronously to the client on the
// other end.
template <class T>
class ClientConnection : public std::enable_shared_from_this<ClientConnection<T>> {
 public:
  /// Create a new node client connection.
  static std::shared_ptr<ClientConnection<T>> Create(
      ClientManager<T>& manager,
      boost::asio::basic_stream_socket<T> &&socket);
  /// Listen for and process messages from a client connection.
  void ProcessMessages();
  /// Write a message to the client and then listen for more messages.  /
  /// This overwrites any message that was buffered.
  void WriteMessage(int64_t type, size_t length, const uint8_t *message);
 private:
  /// A private constructor for a node client connection.
  ClientConnection(
      ClientManager<T>& manager,
      boost::asio::basic_stream_socket<T> &&socket);
  /// Process a message header from the client.
  void processMessageHeader(const boost::system::error_code& error);
  /// Process the message from the client.
  void processMessage(const boost::system::error_code& error);
  /// Process an error and then listen for more messages.
  void processMessages(const boost::system::error_code& error);

  /// The client socket.
  boost::asio::basic_stream_socket<T> socket_;
  /// A reference to the node manager.
  ClientManager<T>& manager_;
  /// The current message being received from the client.
  // TODO(swang): Split these fields out into read and write copies, so we
  // don't accidentally overwrite a message to send.
  int64_t version_;
  int64_t type_;
  uint64_t length_;
  std::vector<uint8_t> message_;
};

using LocalClientConnection = ClientConnection<boost::asio::local::stream_protocol>;
using TcpClientConnection = ClientConnection<boost::asio::ip::tcp>;

// A generic client manager. This class should define a method for processing a
// message on the server sent by the client.
template <class T>
class ClientManager {
 public:
  /// Process a message from a client, then listen for more messages if the
  /// client is still alive.
  virtual void ProcessClientMessage(
      std::shared_ptr<ClientConnection<T>> client,
      int64_t message_type,
      const uint8_t *message) = 0;
  virtual ~ClientManager() = 0;
};

class TCPClientConnection : public boost::enable_shared_from_this<TCPClientConnection> {

 public:
  typedef boost::shared_ptr<TCPClientConnection> pointer;
  static pointer Create(boost::asio::io_service& io_service);
  boost::asio::ip::tcp::socket& GetSocket();

  TCPClientConnection(boost::asio::io_service& io_service);

  int64_t message_type_;
  uint64_t message_length_;

 private:
  boost::asio::ip::tcp::socket socket_;

};

} // end namespace ray

#endif
