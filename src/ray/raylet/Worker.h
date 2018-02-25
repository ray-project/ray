#ifndef WORKER_H
#define WORKER_H

#include "client_connection.h"

#include <memory>
#include <unistd.h>

namespace ray {

class ClientConnection;

/// Worker class encapsulates the implementation details of a worker. A worker
/// is the execution container around a unit of Ray work, such as a task or an
/// actor. Ray units of work execute in the context of a Worker.
class Worker {
public:
  /// A constructor that initializes a worker object.
  Worker(pid_t pid, std::shared_ptr<ClientConnection> connection);
  /// A destructor responsible for freeing all worker state.
  ~Worker() {}
  /// Return the worker's PID.
  pid_t Pid() const;
  /// Return the worker's connection.
  const std::shared_ptr<ClientConnection> Connection() const;
private:
  /// The worker's PID.
  pid_t pid_;
  /// Connection state of a worker.
  std::shared_ptr<ClientConnection> connection_;
};


} // end namespace ray

#endif
