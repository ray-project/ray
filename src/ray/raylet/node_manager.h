#ifndef RAY_NODE_MANAGER_H
#define RAY_NODE_MANAGER_H

#include <list>

#include <boost/asio.hpp>
#include <boost/asio/error.hpp>

class NodeServer;
class NodeClientConnection;

class NodeServer {
 public:
  /// Create a node manager server and listen for new clients.
  NodeServer(boost::asio::io_service& io_service, const std::string &socket_name);

 private:
  /// Accept a client connection.
  void doAccept();
  /// Handle an accepted client connection.
  void handleAccept(const boost::system::error_code& error);

  /// The list of active clients.
  std::list<std::unique_ptr<NodeClientConnection>> clients_;
  /// An acceptor for new clients.
  boost::asio::local::stream_protocol::acceptor acceptor_;
  /// The socket to listen on for new clients.
  boost::asio::local::stream_protocol::socket socket_;
};

class NodeClientConnection {
 public:
  /// Create a new node client connection.
  NodeClientConnection(boost::asio::local::stream_protocol::socket &&socket);
  /// Listen for and process messages from a client connection.
  void ProcessMessages();
 private:
  /// Process a message header from the client.
  void processMessageHeader(const boost::system::error_code& error);
  /// Process the message from the client.
  void processMessage(const boost::system::error_code& error);

  /// The client socket.
  boost::asio::local::stream_protocol::socket socket_;

  /// The current message being received from the client.
  int64_t version_;
  int64_t type_;
  uint64_t length_;
  std::vector<char> message_;
};

#endif  // RAY_NODE_MANAGER_H
