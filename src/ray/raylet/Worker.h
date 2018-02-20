#ifndef WORKER_H
#define WORKER_H

#include <boost/asio.hpp>
#include <boost/asio/error.hpp>
#include <boost/function.hpp>
#include <unordered_set>

using namespace std;
namespace ray {

class NodeServer;
class WorkerPool;

class ClientConnection : public enable_shared_from_this<ClientConnection> {
 public:
  /// Create a new node client connection.
  static shared_ptr<ClientConnection> Create(
      NodeServer& server,
      boost::asio::local::stream_protocol::socket &&socket,
      WorkerPool& worker_pool);
  /// Listen for and process messages from a client connection.
  void ProcessMessages();
 private:
  /// A private constructor for a node client connection.
  ClientConnection(
      NodeServer& server,
      boost::asio::local::stream_protocol::socket &&socket,
      WorkerPool& worker_pool);
  /// Process a message header from the client.
  void processMessageHeader(const boost::system::error_code& error);
  /// Process the message from the client.
  void processMessage(const boost::system::error_code& error);
  /// Write a message to the client. Note that this overwrites any message that
  /// was buffered.
  void writeMessage(int64_t type, size_t length, const uint8_t *message);

  /// The client socket.
  boost::asio::local::stream_protocol::socket socket_;
  /// A reference to the worker pool that stores the client connections.
  WorkerPool& worker_pool_;
  /// A reference to the node manager.
  NodeServer& server_;
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
  Worker(pid_t pid, shared_ptr<ClientConnection> connection);
  /// A destructor responsible for freeing all worker state.
  ~Worker() {}
  /// Return the worker's PID.
  pid_t Pid();
  /// Return the worker's connection.
  const shared_ptr<ClientConnection> Connection();
private:
  /// The worker's PID.
  pid_t pid_;
  /// Connection state of a worker.
  shared_ptr<ClientConnection> connection_;
};


} // end namespace ray

#endif
