#include "reconstruction_policy.h"

namespace ray {

namespace raylet {

ReconstructionPolicy::ReconstructionPolicy(
    boost::asio::io_service &io_service,
    std::function<void(const TaskID &)> reconstruction_handler,
    int64_t initial_reconstruction_timeout_ms, const ClientID &client_id,
    gcs::PubsubInterface<TaskID> &task_lease_pubsub)
    : io_service_(io_service),
      reconstruction_handler_(reconstruction_handler),
      initial_reconstruction_timeout_ms_(initial_reconstruction_timeout_ms),
      client_id_(client_id),
      task_lease_pubsub_(task_lease_pubsub) {}

void ReconstructionPolicy::SetTaskTimeout(
    std::unordered_map<TaskID, ReconstructionTask>::iterator task_it,
    int64_t timeout_ms) {
  task_it->second.expires_at = current_time_ms() + timeout_ms;
  auto timeout = boost::posix_time::milliseconds(timeout_ms);
  task_it->second.reconstruction_timer->expires_from_now(timeout);
  const TaskID task_id = task_it->first;
  task_it->second.reconstruction_timer->async_wait(
      [this, task_id](const boost::system::error_code &error) {
        if (!error) {
          HandleReconstructionTimeout(task_id);
        }
      });
}

void ReconstructionPolicy::Reconstruct(const TaskID &task_id) {
  auto it = listening_tasks_.find(task_id);
  RAY_CHECK(it != listening_tasks_.end());
  // TODO(swang): Suppress simultaneous attempts to reconstruct the task using
  // the task reconstruction log.
  reconstruction_handler_(task_id);
  // Reset the timer to wait for notifications again.
  SetTaskTimeout(it, initial_reconstruction_timeout_ms_);
}

void ReconstructionPolicy::HandleReconstructionTimeout(const TaskID &task_id) {
  auto it = listening_tasks_.find(task_id);
  RAY_CHECK(it != listening_tasks_.end());

  if (it->second.subscribed) {
    // We did not receive a task lease notification within the lease period.
    // The lease is expired, so attempt to reconstruct the task.
    Reconstruct(task_id);
  } else {
    // This task is still required, so subscribe to task lease notifications.
    // Reconstruction will be triggered if the current task lease expires, or
    // if no one has acquired the task lease.
    task_lease_pubsub_.RequestNotifications(JobID::nil(), task_id, client_id_);
    it->second.subscribed = true;
  }
}

void ReconstructionPolicy::HandleTaskLeaseNotification(const TaskID &task_id,
                                                       int64_t expires_at_ms) {
  auto it = listening_tasks_.find(task_id);
  if (it == listening_tasks_.end()) {
    // We are no longer listening for this task, so ignore the notification.
    return;
  }

  auto now_ms = current_time_ms();
  if (now_ms >= expires_at_ms) {
    // The current lease has expired. Reconstruct the task.
    Reconstruct(task_id);
  } else {
    RAY_CHECK(expires_at_ms >= it->second.expires_at);
    // The current lease is still active. Reset the timer to the lease's
    // expiration time.
    SetTaskTimeout(it, expires_at_ms - now_ms);
  }
}

void ReconstructionPolicy::Listen(const ObjectID &object_id) {
  TaskID task_id = ComputeTaskId(object_id);
  auto it = listening_tasks_.find(task_id);
  // Add this object to the list of objects created by the same task.
  if (it != listening_tasks_.end()) {
    it->second.created_objects.insert(object_id);
    return;
  }

  auto inserted = listening_tasks_.emplace(task_id, ReconstructionTask(io_service_));
  it = inserted.first;

  // Set a timer for the task that created the object. If the lease for that
  // task expires, then reconstruction of that task will be triggered.
  SetTaskTimeout(it, initial_reconstruction_timeout_ms_);
}

void ReconstructionPolicy::Cancel(const ObjectID &object_id) {
  TaskID task_id = ComputeTaskId(object_id);
  auto it = listening_tasks_.find(task_id);
  if (it == listening_tasks_.end()) {
    // We already stopped listening for this task.
    return;
  }

  it->second.created_objects.erase(object_id);
  // If there are no more needed objects created by this task, stop listening
  // for notifications.
  if (it->second.created_objects.empty()) {
    listening_tasks_.erase(it);
  }
}

}  // namespace raylet

}  // end namespace ray
