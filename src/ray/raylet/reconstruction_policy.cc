#include "reconstruction_policy.h"

#include "ray/stats/stats.h"

namespace ray {

namespace raylet {

ReconstructionPolicy::ReconstructionPolicy(
    boost::asio::io_service &io_service,
    std::function<void(const TaskID &)> reconstruction_handler,
    int64_t initial_reconstruction_timeout_ms, const ClientID &client_id,
    gcs::PubsubInterface<TaskID> &task_lease_pubsub,
    std::shared_ptr<ObjectDirectoryInterface> object_directory,
    gcs::LogInterface<TaskID, TaskReconstructionData> &task_reconstruction_log)
    : io_service_(io_service),
      reconstruction_handler_(reconstruction_handler),
      initial_reconstruction_timeout_ms_(initial_reconstruction_timeout_ms),
      client_id_(client_id),
      task_lease_pubsub_(task_lease_pubsub),
      object_directory_(std::move(object_directory)),
      task_reconstruction_log_(task_reconstruction_log) {}

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
          auto it = listening_tasks_.find(task_id);
          if (it == listening_tasks_.end()) {
            return;
          }
          if (it->second.subscribed) {
            // If the timer expired and we were subscribed to notifications,
            // then this means that we did not receive a task lease
            // notification within the lease period. Otherwise, the timer
            // would have been reset when the most recent notification was
            // received. The current lease is now considered expired.
            HandleTaskLeaseExpired(task_id);
          } else {
            // This task is still required, so subscribe to task lease
            // notifications.  Reconstruction will be triggered if the current
            // task lease expires, or if no one has acquired the task lease.
            // NOTE(swang): When reconstruction for a task is first requested,
            // we do not initially subscribe to task lease notifications, which
            // requires at least one GCS operation. This is in case the objects
            // required by the task are no longer needed soon after.  If the
            // task is still required after this initial period, then we now
            // subscribe to task lease notifications.
            RAY_CHECK_OK(task_lease_pubsub_.RequestNotifications(JobID::Nil(), task_id,
                                                                 client_id_));
            it->second.subscribed = true;
          }
        } else {
          // Check that the error was due to the timer being canceled.
          RAY_CHECK(error == boost::asio::error::operation_aborted);
        }
      });
}

void ReconstructionPolicy::HandleReconstructionLogAppend(const TaskID &task_id,
                                                         bool success) {
  auto it = listening_tasks_.find(task_id);
  if (it == listening_tasks_.end()) {
    return;
  }

  // Reset the timer to wait for task lease notifications again. NOTE(swang):
  // The timer should already be set here, but we extend it to give some time
  // for the reconstructed task to propagate notifications.
  SetTaskTimeout(it, initial_reconstruction_timeout_ms_);

  if (success) {
    reconstruction_handler_(task_id);
  }
}

void ReconstructionPolicy::AttemptReconstruction(const TaskID &task_id,
                                                 const ObjectID &required_object_id,
                                                 int reconstruction_attempt) {
  // If we are no longer listening for objects created by this task, give up.
  auto it = listening_tasks_.find(task_id);
  if (it == listening_tasks_.end()) {
    return;
  }

  // If the object is no longer required, give up.
  if (it->second.created_objects.count(required_object_id) == 0) {
    return;
  }

  // Suppress duplicate reconstructions of the same task. This can happen if,
  // for example, a task creates two different objects that both require
  // reconstruction.
  if (reconstruction_attempt != it->second.reconstruction_attempt) {
    // Through some other path, reconstruction was already attempted more than
    // reconstruction_attempt many times.
    return;
  }

  // Attempt to reconstruct the task by inserting an entry into the task
  // reconstruction log. This will fail if another node has already inserted
  // an entry for this reconstruction.
  auto reconstruction_entry = std::make_shared<TaskReconstructionData>();
  reconstruction_entry->set_num_reconstructions(reconstruction_attempt);
  reconstruction_entry->set_node_manager_id(client_id_.Binary());
  RAY_CHECK_OK(task_reconstruction_log_.AppendAt(
      JobID::Nil(), task_id, reconstruction_entry,
      /*success_callback=*/
      [this](gcs::RedisGcsClient *client, const TaskID &task_id,
             const TaskReconstructionData &data) {
        HandleReconstructionLogAppend(task_id, /*success=*/true);
      },
      /*failure_callback=*/
      [this](gcs::RedisGcsClient *client, const TaskID &task_id,
             const TaskReconstructionData &data) {
        HandleReconstructionLogAppend(task_id, /*success=*/false);
      },
      reconstruction_attempt));

  // Increment the number of times reconstruction has been attempted. This is
  // used to suppress duplicate reconstructions of the same task. If
  // reconstruction is attempted again, the next attempt will try to insert a
  // task reconstruction entry at the next index in the log.
  it->second.reconstruction_attempt++;
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
        created_object_id, [this, task_id, reconstruction_attempt](
                               const ray::ObjectID &object_id,
                               const std::unordered_set<ray::ClientID> &clients) {
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
                                                       int64_t lease_timeout_ms) {
  auto it = listening_tasks_.find(task_id);
  if (it == listening_tasks_.end()) {
    // We are no longer listening for this task, so ignore the notification.
    return;
  }

  if (lease_timeout_ms == 0) {
    HandleTaskLeaseExpired(task_id);
  } else if ((current_time_ms() + lease_timeout_ms) > it->second.expires_at) {
    // The current lease is longer than the timer's current expiration time.
    // Reset the timer according to the current lease.
    SetTaskTimeout(it, lease_timeout_ms);
  }
}

void ReconstructionPolicy::ListenAndMaybeReconstruct(const ObjectID &object_id) {
  TaskID task_id = object_id.TaskId();
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
  TaskID task_id = object_id.TaskId();
  auto it = listening_tasks_.find(task_id);
  if (it == listening_tasks_.end()) {
    // We already stopped listening for this task.
    return;
  }

  it->second.created_objects.erase(object_id);
  // If there are no more needed objects created by this task, stop listening
  // for notifications.
  if (it->second.created_objects.empty()) {
    // Cancel notifications for the task lease if we were subscribed to them.
    if (it->second.subscribed) {
      RAY_CHECK_OK(
          task_lease_pubsub_.CancelNotifications(JobID::Nil(), task_id, client_id_));
    }
    listening_tasks_.erase(it);
  }
}

std::string ReconstructionPolicy::DebugString() const {
  std::stringstream result;
  result << "ReconstructionPolicy:";
  result << "\n- num reconstructing: " << listening_tasks_.size();
  return result.str();
}

void ReconstructionPolicy::RecordMetrics() const {
  stats::ReconstructionPolicyStats().Record(
      listening_tasks_.size(), {{stats::ValueTypeKey, "num_reconstructing_tasks"}});
}

}  // namespace raylet

}  // end namespace ray
