#ifndef WORKER_CC
#define WORKER_CC

#include "Worker.h"

#include <boost/bind.hpp>

#include "common.h"
#include "format/nm_generated.h"
#include "node_manager.h"

using namespace std;
namespace ray {

/// A constructor responsible for initializing the state of a worker.
Worker::Worker(pid_t pid, shared_ptr<ClientConnection> connection)
  : pid_(pid),
    connection_(connection),
    assigned_task_id_(TaskID::nil()) {
}

pid_t Worker::Pid() const {
  return pid_;
}

void Worker::AssignTaskId(const TaskID &task_id) {
  assigned_task_id_ = task_id;
}

const TaskID &Worker::GetAssignedTaskId() const {
  return assigned_task_id_;
}

const shared_ptr<ClientConnection> Worker::Connection() const {
  return connection_;
}

} // end namespace ray

#endif
