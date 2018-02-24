#ifndef WORKER_H
#define WORKER_H

#include <boost/asio.hpp>
#include <boost/asio/error.hpp>
#include <boost/function.hpp>
#include <unordered_set>

using namespace std;
namespace ray {

class ClientManager;

/// Worker class encapsulates the implementation details of a worker. A worker
/// is the execution container around a unit of Ray work, such as a task or an
/// actor. Ray units of work execute in the context of a Worker.
class Worker {
public:
  /// A constructor that initializes a worker object.
  Worker(pid_t pid);
  /// Return the worker's PID.
  pid_t Pid() const;
private:
  /// The worker's PID.
  pid_t pid_;
};

class ClientConnection : public enable_shared_from_this<ClientConnection> {
 public:
  /// Create a new node client connection.
  static shared_ptr<ClientConnection> Create(
      ClientManager& manager,
      boost::asio::local::stream_protocol::socket &&socket);
  /// Listen for and process messages from a client connection.
  void ProcessMessages();
  /// Write a message to the client and then listen for more messages.  /
  /// This overwrites any message that was buffered.
  void WriteMessage(int64_t type, size_t length, const uint8_t *message);
  void SetWorker(Worker &&worker);
  const Worker &GetWorker() const;
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

  /// Worker information.
  Worker worker_;
};


} // end namespace ray

#endif
