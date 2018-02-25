#ifndef CLIENT_CONNECTION_H
#define CLIENT_CONNECTION_H

#include <boost/asio.hpp>
#include <boost/asio/error.hpp>
#include <memory>

namespace ray {

class ClientManager;

class ClientConnection : public std::enable_shared_from_this<ClientConnection> {
 public:
  /// Create a new node client connection.
  static std::shared_ptr<ClientConnection> Create(
      ClientManager& manager,
      boost::asio::local::stream_protocol::socket &&socket);
  /// Listen for and process messages from a client connection.
  void ProcessMessages();
  /// Write a message to the client and then listen for more messages.  /
  /// This overwrites any message that was buffered.
  void WriteMessage(int64_t type, size_t length, const uint8_t *message);
 private:
  /// A private constructor for a node client connection.
  ClientConnection(
      ClientManager& manager,
      boost::asio::local::stream_protocol::socket &&socket);
  /// Process a message header from the client.
  void processMessageHeader(const boost::system::error_code& error);
  /// Process the message from the client.
  void processMessage(const boost::system::error_code& error);
  /// Process an error and then listen for more messages.
  void processMessages(const boost::system::error_code& error);

  /// The client socket.
  boost::asio::local::stream_protocol::socket socket_;
  /// A reference to the node manager.
  ClientManager& manager_;
  /// The current message being received from the client.
  int64_t version_;
  int64_t type_;
  uint64_t length_;
  std::vector<uint8_t> message_;
};

} // end namespace ray

#endif
