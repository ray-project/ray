#ifndef WORKER_H
#define WORKER_H

#include <boost/asio.hpp>
#include <boost/asio/error.hpp>
#include <boost/function.hpp>

using namespace std;
namespace ray {

class ClientConnection;
class Worker;

class ClientConnection {
 public:
  /// Create a new node client connection.
  ClientConnection(
      boost::asio::local::stream_protocol::socket &&socket,
      boost::function<void (pid_t)> worker_registered_handler);
  /// Listen for and process messages from a client connection.
  void ProcessMessages();
 private:
  /// Process a message header from the client.
  void processMessageHeader(const boost::system::error_code& error);
  /// Process the message from the client.
  void processMessage(const boost::system::error_code& error);

  /// The client socket.
  boost::asio::local::stream_protocol::socket socket_;
  boost::function<void (pid_t)> worker_registered_handler_;
  /// The current message being received from the client.
  int64_t version_;
  int64_t type_;
  uint64_t length_;
  std::vector<char> message_;
};

/// Worker class encapsulates the implementation details of a worker. A worker
/// is the execution container around a unit of Ray work, such as a task or an
/// actor. Ray units of work execute in the context of a Worker.
class Worker {
public:
  /// A constructor that initializes a worker object.
  Worker();
  /// A destructor responsible for freeing all worker state.
  ~Worker() {}
private:
  /// Connection state of a worker.
  /// TODO(swang): provide implementation details for ClientConnection
  //ClientConnection conn_;

};


} // end namespace ray

#endif
