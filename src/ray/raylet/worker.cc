#include "worker.h"

#include <boost/bind.hpp>

#include "common.h"
#include "ray/raylet/format/node_manager_generated.h"
#include "ray/raylet/raylet.h"

namespace ray {

/// A constructor responsible for initializing the state of a worker.
Worker::Worker(pid_t pid, std::shared_ptr<LocalClientConnection> connection)
    : pid_(pid), connection_(connection), assigned_task_id_(TaskID::nil()) {}

pid_t Worker::Pid() const { return pid_; }

void Worker::AssignTaskId(const TaskID &task_id) { assigned_task_id_ = task_id; }

const TaskID &Worker::GetAssignedTaskId() const { return assigned_task_id_; }

const std::shared_ptr<LocalClientConnection> Worker::Connection() const {
  return connection_;
}

}  // end namespace ray
