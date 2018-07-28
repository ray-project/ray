#include "reconstruction_policy.h"

namespace ray {

namespace raylet {

ReconstructionPolicy::ReconstructionPolicy(
    boost::asio::io_service &io_service,
    std::function<void(const TaskID &)> reconstruction_handler,
    int64_t initial_reconstruction_timeout_ms, const ClientID &client_id,
    gcs::PubsubInterface<TaskID> &task_lease_pubsub,
    std::shared_ptr<ObjectDirectoryInterface> object_directory)
    : io_service_(io_service),
      reconstruction_handler_(reconstruction_handler),
      initial_reconstruction_timeout_ms_(initial_reconstruction_timeout_ms),
      client_id_(client_id),
      task_lease_pubsub_(task_lease_pubsub),
      object_directory_(std::move(object_directory)) {}

void ReconstructionPolicy::SetTaskTimeout(
    std::unordered_map<TaskID, ReconstructionTask>::iterator task_it,
    int64_t timeout_ms) {
  task_it->second.expires_at = current_sys_time_ms() + timeout_ms;
  auto timeout = boost::posix_time::milliseconds(timeout_ms);
  task_it->second.reconstruction_timer->expires_from_now(timeout);
  const TaskID task_id = task_it->first;
  task_it->second.reconstruction_timer->async_wait(
      [this, task_id](const boost::system::error_code &error) {
        if (!error) {
          auto it = listening_tasks_.find(task_id);
          RAY_CHECK(it != listening_tasks_.end());

          if (it->second.subscribed) {
            // We did not receive a task lease notification within the lease period.
            // The lease is expired, so attempt to reconstruct the task.
            HandleTaskLeaseExpired(task_id);
          } else {
            // This task is still required, so subscribe to task lease notifications.
            // Reconstruction will be triggered if the current task lease expires, or
            // if no one has acquired the task lease.
            RAY_CHECK_OK(task_lease_pubsub_.RequestNotifications(JobID::nil(), task_id,
                                                                 client_id_));
            it->second.subscribed = true;
          }
        }
      });
}

void ReconstructionPolicy::AttemptReconstruction(const TaskID &task_id,
                                                 const ObjectID &object_id,
                                                 int reconstruction_attempt) {
  // If we are no longer listening for objects created this task, give up.
  auto it = listening_tasks_.find(task_id);
  if (it == listening_tasks_.end()) {
    return;
  }

  if (it->second.created_objects.count(object_id) == 0) {
    return;
  }

  // If we're already past this reconstruction attempt, give up.
  if (reconstruction_attempt != it->second.reconstruction_attempt) {
    return;
  }
  // Increment the number of times reconstruction has been attempted.
  it->second.reconstruction_attempt++;

  // Reset the timer to wait for task lease notifications again. NOTE(swang):
  // The timer should already be set here, but we extend it to give some time
  // for the reconstructed task to propagate notifications.
  SetTaskTimeout(it, initial_reconstruction_timeout_ms_);
  // TODO(swang): Suppress simultaneous attempts to reconstruct the task using
  // the task reconstruction log.
  reconstruction_handler_(task_id);
}

void ReconstructionPolicy::HandleTaskLeaseExpired(const TaskID &task_id) {
  auto it = listening_tasks_.find(task_id);
  RAY_CHECK(it != listening_tasks_.end());
  int reconstruction_attempt = it->second.reconstruction_attempt;
  // Lookup the objects created by this task in the object directory. If any
  // objects no longer exist on any live nodes, then reconstruction will be
  // attempted asynchronously.
  for (const auto &created_object_id : it->second.created_objects) {
    RAY_CHECK_OK(object_directory_->LookupLocations(
        created_object_id,
        [this, task_id, reconstruction_attempt](const std::vector<ray::ClientID> &clients,
                                                const ray::ObjectID &object_id) {
          if (clients.empty()) {
            // The required object no longer exists on any live nodes. Attempt
            // reconstruction.
            AttemptReconstruction(task_id, object_id, reconstruction_attempt);
          }
        }));
  }
  // Reset the timer to wait for task lease notifications again.
  SetTaskTimeout(it, initial_reconstruction_timeout_ms_);
}

void ReconstructionPolicy::HandleTaskLeaseNotification(const TaskID &task_id,
                                                       int64_t expires_at_ms) {
  auto it = listening_tasks_.find(task_id);
  if (it == listening_tasks_.end()) {
    // We are no longer listening for this task, so ignore the notification.
    return;
  }

  auto now_ms = current_sys_time_ms();
  if (now_ms >= expires_at_ms) {
    // The current lease has expired.
    HandleTaskLeaseExpired(task_id);
  } else if (expires_at_ms > it->second.expires_at) {
    // The current lease is still active. Reset the timer to the lease's
    // expiration time.
    SetTaskTimeout(it, expires_at_ms - now_ms);
  }
}

void ReconstructionPolicy::Listen(const ObjectID &object_id) {
  TaskID task_id = ComputeTaskId(object_id);
  auto it = listening_tasks_.find(task_id);
  // Add this object to the list of objects created by the same task.
  if (it == listening_tasks_.end()) {
    auto inserted = listening_tasks_.emplace(task_id, ReconstructionTask(io_service_));
    it = inserted.first;
    // Set a timer for the task that created the object. If the lease for that
    // task expires, then reconstruction of that task will be triggered.
    SetTaskTimeout(it, initial_reconstruction_timeout_ms_);
  }
  it->second.created_objects.insert(object_id);
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
