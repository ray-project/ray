#include "reconstruction_policy.h"

namespace ray {

namespace raylet {

void ReconstructionPolicy::Listen(const ObjectID &object_id) {
  // We're already listening for this object, so do nothing.
  if (listening_objects_.count(object_id) == 1) {
    return;
  }
  // Listen for this object.
  ObjectEntry entry;
  entry.object_id = object_id;
  entry.version = 0;
  entry.num_ticks = 2;
  listening_objects_.insert({object_id, entry});

  TaskID task_id = ComputeTaskId(object_id);
  auto task_entry = reconstructing_tasks_.find(task_id);
  if (task_entry != reconstructing_tasks_.end()) {
    // We're currently attempting to re-execute the task that created this
    // object.
    auto it = std::find(task_entry->second.begin(), task_entry->second.end(), object_id);
    // Add this object to the list of objects created by the task.
    if (it == task_entry->second.end()) {
      task_entry->second.push_back(object_id);
    }
  } else {
    // Wait for notifications about this object. If we don't receive a
    // notification within the timeout, or if we're notified of eviction or
    // failure, then we will attempt to re-execute the task that created the
    // object.
    object_ticks_.insert({object_id, entry.num_ticks});
  }
}

void ReconstructionPolicy::Notify(const ObjectID &object_id) {
  auto num_ticks = listening_objects_[object_id].num_ticks;
  RAY_CHECK(num_ticks > 0);
  // Reset this object's timer.
  object_ticks_[object_id] = num_ticks;
}

void ReconstructionPolicy::Cancel(const ObjectID &object_id) {
  listening_objects_.erase(object_id);
  object_ticks_.erase(object_id);
  TaskID task_id = ComputeTaskId(object_id);
  auto task_entry = reconstructing_tasks_.find(task_id);
  if (task_entry != reconstructing_tasks_.end()) {
    auto it = std::find(task_entry->second.begin(), task_entry->second.end(), object_id);
    if (it != task_entry->second.end()) {
      task_entry->second.erase(it);
    }
  }
}

void ReconstructionPolicy::HandleNotification(
    const ObjectID &object_id, const std::vector<ObjectTableDataT> new_locations) {
  throw std::runtime_error("Method not implemented");
}

void ReconstructionPolicy::HandleTaskLogAppend(
    const TaskID &task_id, std::shared_ptr<TaskReconstructionDataT> data) {
  auto task_entry = reconstructing_tasks_.find(task_id);
  RAY_CHECK(task_entry != reconstructing_tasks_.end());
  auto object_ids = std::move(task_entry->second);
  reconstructing_tasks_.erase(task_entry);
  if (object_ids.empty()) {
    return;
  }

  // We are still listening for objects created by this task. Trigger
  // reconstruction by calling the registered handler.
  // TODO(swang): Handle failure to Append.
  RAY_LOG(DEBUG) << "reconstruction triggered: " << task_id.hex();
  reconstruction_handler_(task_id);
  int max_version = data->num_executions;
  for (const auto &object_id : object_ids) {
    if (listening_objects_[object_id].version > max_version) {
      max_version = listening_objects_[object_id].version;
    }
  }
  // Increase the version number for each of the objects that was
  // created by this task and reset their timers until the next
  // reconstruction attempt.
  for (const auto &object_id : object_ids) {
    auto entry = listening_objects_.find(object_id);
    entry->second.version = max_version;
    object_ticks_.insert({object_id, entry->second.num_ticks});
  }
}

void ReconstructionPolicy::Reconstruct(const ObjectID &object_id) {
  auto object_entry = listening_objects_.find(object_id);
  TaskID task_id = ComputeTaskId(object_id);
  reconstructing_tasks_[task_id].push_back(object_id);
  // If we weren't already trying to re-execute the task that created this
  // object, try to re-execute the task now.
  if (reconstructing_tasks_[task_id].size() == 1) {
    auto reconstruction_entry = std::make_shared<TaskReconstructionDataT>();
    reconstruction_entry->num_executions = object_entry->second.version + 1;
    reconstruction_entry->node_manager_id = client_id_.binary();
    // TODO(swang): JobID.
    RAY_CHECK_OK(task_reconstruction_log_.Append(
        JobID::nil(), task_id, reconstruction_entry,
        [this](gcs::AsyncGcsClient *client, const TaskID &task_id,
               std::shared_ptr<TaskReconstructionDataT> data) {
          HandleTaskLogAppend(task_id, data);
        }));
  }
}

void ReconstructionPolicy::Tick() {
  for (auto it = object_ticks_.begin(); it != object_ticks_.end();) {
    // Decrement the number of ticks left before timeout.
    it->second--;
    if (it->second == 0) {
      // It's been at least `num_ticks` since the last notification for this
      // object. Try to re-execute the task that created the object.
      Reconstruct(it->first);
      // Stop the timer for this object.
      it = object_ticks_.erase(it);
    } else {
      it++;
    }
  }

  auto period = boost::posix_time::milliseconds(reconstruction_timeout_ms_);
  reconstruction_timer_.expires_from_now(period);
  reconstruction_timer_.async_wait([this](const boost::system::error_code &error) {
    if (!error) {
      Tick();
    }
  });
}

}  // namespace raylet

}  // end namespace ray
