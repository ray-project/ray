#include "reconstruction_policy.h"

namespace {

/// A helper function to process location entries from the object table log.
/// Each location entry contains a node manager ID and a flag indicating
/// whether the object was added or evicted from that node.
void ReduceObjectTableDataT(std::unordered_set<ClientID, UniqueIDHasher> &locations,
                            const std::vector<ObjectTableDataT> &new_location_entries) {
  for (const auto &location_entry : new_location_entries) {
    ClientID node_manager_id = ClientID::from_binary(location_entry.manager);
    if (location_entry.is_eviction) {
      // The object was evicted from the node. Erase the node manager from the
      // set of known locations.
      locations.erase(node_manager_id);
    } else {
      // The object was made available at the node. Add the node manager to the
      // set of known locations.
      RAY_CHECK(locations.count(node_manager_id) == 0);
      locations.insert(node_manager_id);
    }
  }
}
}

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
  entry.num_reconstructions = 0;
  entry.num_ticks = 2;
  listening_objects_.insert({object_id, entry});

  // For each object that we listen for, we are either waiting for them to time
  // out, or they have already timed out and we are attempting to reconstruct
  // the task that created the object.
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

void ReconstructionPolicy::Cancel(const ObjectID &object_id) {
  // Stop listening for the object.
  listening_objects_.erase(object_id);
  // Stop the timer for this object.
  object_ticks_.erase(object_id);
  // If we were attempting to re-execute the task that reconstructed the
  // object, stop.
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
    const ObjectID &object_id, const std::vector<ObjectTableDataT> new_location_entries) {
  auto entry = listening_objects_.find(object_id);
  // Do nothing for objects we are not listening for.
  if (entry == listening_objects_.end()) {
    return;
  }
  // Reset the timer for this object ID. If the object ID does not have a
  // corresponding timer, but we are listening to the object, then we do
  // nothing since we are already attempting to reconstruct it.
  auto timer = object_ticks_.find(object_id);
  if (timer != object_ticks_.end()) {
    // Apply the new location entries from the log to the cached set of
    // locations.
    ReduceObjectTableDataT(entry->second.locations, new_location_entries);
    // Check whether the object now exists at some location.
    if (entry->second.locations.empty()) {
      if (new_location_entries.empty()) {
        // Notifications with empty data are heartbeats. Reset the timer for the
        // object ID and wait for the node creating the object to finish.
        timer->second = entry->second.num_ticks;
      } else {
        // We received a notification with a new location entry, but now there
        // are no more known locations for the object. The object must have been
        // evicted or lost from failure, so erase the timer and attempt to
        // reconstruct the object.
        object_ticks_.erase(timer);
        Reconstruct(object_id);
      }
    } else {
      // There is a known location for the object. Reset the timer for the
      // object ID and wait for the transfer.
      timer->second = entry->second.num_ticks;
    }
  }
}

void ReconstructionPolicy::HandleTaskLogAppend(
    const TaskID &task_id, std::shared_ptr<TaskReconstructionDataT> data, bool appended) {
  auto task_entry = reconstructing_tasks_.find(task_id);
  RAY_CHECK(task_entry != reconstructing_tasks_.end());
  // Check which objects are being listened for and were created by this task.
  auto object_ids = std::move(task_entry->second);
  reconstructing_tasks_.erase(task_entry);
  if (object_ids.empty()) {
    // If we are no longer listening for objects created by this task, then do
    // not trigger reconstruction.
    return;
  }

  // If we successfully appended this task re-execution to the global log, then
  // trigger reconstruction by calling the registered handler.
  if (appended) {
    RAY_LOG(DEBUG) << "reconstruction triggered: " << task_id.hex();
    reconstruction_handler_(task_id);
  }

  // Compute the reconstruction_index at which we should try to append the task
  // reconstruction
  // entry next. Each object records the number of times that we've attempted
  // reconstruction for it so far, so the reconstruction_index is the maximum of these,
  // versus
  // one past the reconstruction_index just attempted.
  int max_reconstructions = data->num_reconstructions + 1;
  for (const auto &object_id : object_ids) {
    if (listening_objects_[object_id].num_reconstructions > max_reconstructions) {
      max_reconstructions = listening_objects_[object_id].num_reconstructions;
    }
  }
  // Increase the num_reconstructions number for each of the objects that was
  // created by this task and reset their timers until the next
  // reconstruction attempt.
  for (const auto &object_id : object_ids) {
    auto entry = listening_objects_.find(object_id);
    entry->second.num_reconstructions = max_reconstructions;
    object_ticks_[object_id] = entry->second.num_ticks;
  }
}

void ReconstructionPolicy::Reconstruct(const ObjectID &object_id) {
  auto object_entry = listening_objects_.find(object_id);
  TaskID task_id = ComputeTaskId(object_id);
  reconstructing_tasks_[task_id].push_back(object_id);
  // If we weren't already trying to re-execute the task that created this
  // object, try to re-execute the task now.
  if (reconstructing_tasks_[task_id].size() == 1) {
    // Get the index at which we should try to append the task reconstruction
    // data.
    auto reconstruction_index = object_entry->second.num_reconstructions;
    // Increment the number of times that we've tried to reconstruct this
    // object.
    object_entry->second.num_reconstructions++;

    // Attempt to reconstruct the task by inserting an entry into the task
    // reconstruction log. This will fail if another node has already inserted
    // an entry for this reconstruction.
    auto reconstruction_entry = std::make_shared<TaskReconstructionDataT>();
    reconstruction_entry->num_reconstructions = reconstruction_index;
    reconstruction_entry->node_manager_id = client_id_.binary();
    // TODO(swang): JobID.
    RAY_CHECK_OK(task_reconstruction_log_.AppendAt(
        JobID::nil(), task_id, reconstruction_entry,
        /*success_callback=*/
        [this](gcs::AsyncGcsClient *client, const TaskID &task_id,
               std::shared_ptr<TaskReconstructionDataT> data) {
          HandleTaskLogAppend(task_id, data, true);
        },
        /*failure_callback=*/
        [this](gcs::AsyncGcsClient *client, const TaskID &task_id,
               std::shared_ptr<TaskReconstructionDataT> data) {
          HandleTaskLogAppend(task_id, data, false);
        },
        reconstruction_index));
  }
}

void ReconstructionPolicy::Tick() {
  // Process any objects that have timed out.
  for (auto it = object_ticks_.begin(); it != object_ticks_.end();) {
    // Decrement the number of ticks left before timeout.
    it->second--;
    if (it->second == 0) {
      ObjectID object_id = it->first;
      it = object_ticks_.erase(it);
      // It's been at least `num_ticks` since the last notification for this
      // object. Try to re-execute the task that created the object.
      Reconstruct(object_id);
    } else {
      it++;
    }
  }

  // Fire the timer again after another period.
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
