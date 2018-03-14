#ifndef RAY_RAYLET_WORKER_H
#define RAY_RAYLET_WORKER_H

#include <memory>

#include "ray/common/client_connection.h"
#include "ray/id.h"

namespace ray {

/// Worker class encapsulates the implementation details of a worker. A worker
/// is the execution container around a unit of Ray work, such as a task or an
/// actor. Ray units of work execute in the context of a Worker.
class Worker {
 public:
  /// A constructor that initializes a worker object.
  Worker(pid_t pid, std::shared_ptr<LocalClientConnection> connection);
  /// A destructor responsible for freeing all worker state.
  ~Worker() {}
  /// Return the worker's PID.
  pid_t Pid() const;
  void AssignTaskId(const TaskID &task_id);
  const TaskID &GetAssignedTaskId() const;
  /// Return the worker's connection.
  const std::shared_ptr<LocalClientConnection> Connection() const;

 private:
  /// The worker's PID.
  pid_t pid_;
  /// Connection state of a worker.
  std::shared_ptr<LocalClientConnection> connection_;
  TaskID assigned_task_id_;
};

}  // namespace ray

#endif  // RAY_RAYLET_WORKER_H
