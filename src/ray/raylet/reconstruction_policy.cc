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

void ReconstructionPolicy::Reconstruct(const TaskID &task_id) {
  auto it = listening_tasks_.find(task_id);
  RAY_CHECK(it != listening_tasks_.end());
  // TODO(swang): Suppress simultaneous attempts to reconstruct the task using
  // the task reconstruction log.
  reconstruction_handler_(task_id);
  // TODO(swang): Reset the timer to wait for notifications again.
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

  if (expires_at_ms != 0) {
    RAY_CHECK(expires_at_ms >= it->second.expires_at);
  }

  auto now_ms = current_time_ms();
  if (now_ms >= expires_at_ms) {
    // The current lease has expired. Reconstruct the task.
    Reconstruct(task_id);
  } else {
    RAY_LOG(INFO) << "Lease renewal received " << task_id;
    // The current lease is still active. Reset the timer to the lease's
    // expiration time.
    auto timeout = boost::posix_time::milliseconds(expires_at_ms - now_ms);
    it->second.reconstruction_timer->expires_from_now(timeout);
    it->second.expires_at = expires_at_ms;
    it->second.reconstruction_timer->async_wait(
        [this, task_id](const boost::system::error_code &error) {
          if (!error) {
            HandleReconstructionTimeout(task_id);
          }
        });
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

  auto task_entry = ReconstructionTask(io_service_);
  task_entry.created_objects.insert(object_id);

  // Set a timer for the task that created the object. If the lease for that
  // task expires, then reconstruction of that task will be triggered.
  task_entry.expires_at = current_time_ms() + initial_reconstruction_timeout_ms_;
  auto reconstruction_timeout =
      boost::posix_time::milliseconds(initial_reconstruction_timeout_ms_);
  task_entry.reconstruction_timer->expires_from_now(reconstruction_timeout);
  task_entry.reconstruction_timer->async_wait(
      [this, task_id](const boost::system::error_code &error) {
        if (!error) {
          HandleReconstructionTimeout(task_id);
        }
      });

  listening_tasks_.emplace(task_id, std::move(task_entry));
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
