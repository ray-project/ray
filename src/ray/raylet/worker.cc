#include "worker.h"

#include <boost/bind.hpp>

#include "common.h"
#include "ray/raylet/format/node_manager_generated.h"
#include "ray/raylet/raylet.h"

namespace ray {

namespace raylet {

/// A constructor responsible for initializing the state of a worker.
Worker::Worker(pid_t pid, std::shared_ptr<LocalClientConnection> connection)
    : pid_(pid),
      connection_(connection),
      assigned_task_id_(TaskID::nil()),
      actor_id_(ActorID::nil()),
      blocked_(false) {}

void Worker::MarkBlocked() { blocked_ = true; }

void Worker::MarkUnblocked() { blocked_ = false; }

bool Worker::IsBlocked() const { return blocked_; }

pid_t Worker::Pid() const { return pid_; }

void Worker::AssignTaskId(const TaskID &task_id) { assigned_task_id_ = task_id; }

const TaskID &Worker::GetAssignedTaskId() const { return assigned_task_id_; }

void Worker::AssignActorId(const ActorID &actor_id) {
  RAY_CHECK(actor_id_.is_nil())
      << "A worker that is already an actor cannot be assigned an actor ID again.";
  RAY_CHECK(!actor_id.is_nil());
  actor_id_ = actor_id;
}

const ActorID &Worker::GetActorId() const { return actor_id_; }

const std::shared_ptr<LocalClientConnection> Worker::Connection() const {
  return connection_;
}

}  // namespace raylet

}  // end namespace ray
