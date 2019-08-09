#include "task_dependency_manager.h"

#include "ray/stats/stats.h"

namespace ray {

namespace raylet {

TaskDependencyManager::TaskDependencyManager(
    ObjectManagerInterface &object_manager,
    ReconstructionPolicyInterface &reconstruction_policy,
    boost::asio::io_service &io_service, const ClientID &client_id,
    int64_t initial_lease_period_ms,
    gcs::TableInterface<TaskID, TaskLeaseData> &task_lease_table)
    : object_manager_(object_manager),
      reconstruction_policy_(reconstruction_policy),
      io_service_(io_service),
      client_id_(client_id),
      initial_lease_period_ms_(initial_lease_period_ms),
      task_lease_table_(task_lease_table) {}

bool TaskDependencyManager::CheckObjectLocal(const ObjectID &object_id) const {
  return local_objects_.count(object_id) == 1;
}

bool TaskDependencyManager::CheckObjectRequired(const ObjectID &object_id) const {
  const TaskID task_id = object_id.TaskId();
  auto task_entry = required_tasks_.find(task_id);
  // If there are no subscribed tasks that are dependent on the object, then do
  // nothing.
  if (task_entry == required_tasks_.end()) {
    return false;
  }
  if (task_entry->second.count(object_id) == 0) {
    return false;
  }
  // If the object is already local, then the dependency is fulfilled. Do
  // nothing.
  if (local_objects_.count(object_id) == 1) {
    return false;
  }
  // If the task that creates the object is pending execution, then the
  // dependency will be fulfilled locally. Do nothing.
  if (pending_tasks_.count(task_id) == 1) {
    return false;
  }
  return true;
}

void TaskDependencyManager::HandleRemoteDependencyRequired(const ObjectID &object_id) {
  bool required = CheckObjectRequired(object_id);
  // If the object is required, then try to make the object available locally.
  if (required) {
    auto inserted = required_objects_.insert(object_id);
    if (inserted.second) {
      // If we haven't already, request the object manager to pull it from a
      // remote node.
      RAY_CHECK_OK(object_manager_.Pull(object_id));
      reconstruction_policy_.ListenAndMaybeReconstruct(object_id);
    }
  }
}

void TaskDependencyManager::HandleRemoteDependencyCanceled(const ObjectID &object_id) {
  bool required = CheckObjectRequired(object_id);
  // If the object is no longer required, then cancel the object.
  if (!required) {
    auto it = required_objects_.find(object_id);
    if (it != required_objects_.end()) {
      object_manager_.CancelPull(object_id);
      reconstruction_policy_.Cancel(object_id);
      required_objects_.erase(it);
    }
  }
}

std::vector<TaskID> TaskDependencyManager::HandleObjectLocal(
    const ray::ObjectID &object_id) {
  // Add the object to the table of locally available objects.
  auto inserted = local_objects_.insert(object_id);
  RAY_CHECK(inserted.second);

  // Find all tasks and workers that depend on the newly available object.
  std::vector<TaskID> ready_task_ids;
  auto creating_task_entry = required_tasks_.find(object_id.TaskId());
  if (creating_task_entry != required_tasks_.end()) {
    auto object_entry = creating_task_entry->second.find(object_id);
    if (object_entry != creating_task_entry->second.end()) {
      // Loop through all tasks that depend on the newly available object.
      for (const auto &dependent_task_id : object_entry->second.dependent_tasks) {
        auto &task_entry = task_dependencies_[dependent_task_id];
        task_entry.num_missing_get_dependencies--;
        // If the dependent task now has all of its arguments ready, it's ready
        // to run.
        if (task_entry.num_missing_get_dependencies == 0) {
          ready_task_ids.push_back(dependent_task_id);
        }
      }
      // Remove the dependency from all workers that called `ray.wait` on the
      // newly available object.
      for (const auto &worker_id : object_entry->second.dependent_workers) {
        RAY_CHECK(worker_dependencies_[worker_id].erase(object_id) > 0);
      }
      // Clear all workers that called `ray.wait` on this object, since the
      // `ray.wait` calls can now return the object as ready.
      object_entry->second.dependent_workers.clear();

      // If there are no more tasks or workers dependent on the local object or
      // the task that created it, then remove the entry completely.
      if (object_entry->second.Empty()) {
        creating_task_entry->second.erase(object_entry);
        if (creating_task_entry->second.empty()) {
          required_tasks_.erase(creating_task_entry);
        }
      }
    }
  }

  // The object is now local, so cancel any in-progress operations to make the
  // object local.
  HandleRemoteDependencyCanceled(object_id);

  return ready_task_ids;
}

std::vector<TaskID> TaskDependencyManager::HandleObjectMissing(
    const ray::ObjectID &object_id) {
  // Remove the object from the table of locally available objects.
  auto erased = local_objects_.erase(object_id);
  RAY_CHECK(erased == 1);

  // Find any tasks that are dependent on the missing object.
  std::vector<TaskID> waiting_task_ids;
  TaskID creating_task_id = object_id.TaskId();
  auto creating_task_entry = required_tasks_.find(creating_task_id);
  if (creating_task_entry != required_tasks_.end()) {
    auto object_entry = creating_task_entry->second.find(object_id);
    if (object_entry != creating_task_entry->second.end()) {
      for (auto &dependent_task_id : object_entry->second.dependent_tasks) {
        auto &task_entry = task_dependencies_[dependent_task_id];
        // If the dependent task had all of its arguments ready, it was ready to
        // run but must be switched to waiting since one of its arguments is now
        // missing.
        if (task_entry.num_missing_get_dependencies == 0) {
          waiting_task_ids.push_back(dependent_task_id);
          // During normal execution we should be able to include the check
          // RAY_CHECK(pending_tasks_.count(dependent_task_id) == 1);
          // However, this invariant will not hold during unit test execution.
        }
        task_entry.num_missing_get_dependencies++;
      }
    }
  }
  // The object is no longer local. Try to make the object local if necessary.
  HandleRemoteDependencyRequired(object_id);
  // Process callbacks for all of the tasks dependent on the object that are
  // now ready to run.
  return waiting_task_ids;
}

bool TaskDependencyManager::SubscribeGetDependencies(
    const TaskID &task_id, const std::vector<ObjectID> &required_objects) {
  auto &task_entry = task_dependencies_[task_id];

  // Record the task's dependencies.
  for (const auto &object_id : required_objects) {
    auto inserted = task_entry.get_dependencies.insert(object_id);
    if (inserted.second) {
      RAY_LOG(DEBUG) << "Task " << task_id << " blocked on object " << object_id;
      // Get the ID of the task that creates the dependency.
      TaskID creating_task_id = object_id.TaskId();
      // Determine whether the dependency can be fulfilled by the local node.
      if (local_objects_.count(object_id) == 0) {
        // The object is not local.
        task_entry.num_missing_get_dependencies++;
      }
      // Add the subscribed task to the mapping from object ID to list of
      // dependent tasks.
      required_tasks_[creating_task_id][object_id].dependent_tasks.insert(task_id);
    }
  }

  // These dependencies are required by the given task. Try to make them local
  // if necessary.
  for (const auto &object_id : required_objects) {
    HandleRemoteDependencyRequired(object_id);
  }

  // Return whether all dependencies are local.
  return (task_entry.num_missing_get_dependencies == 0);
}

void TaskDependencyManager::SubscribeWaitDependencies(
    const WorkerID &worker_id, const std::vector<ObjectID> &required_objects) {
  auto &worker_entry = worker_dependencies_[worker_id];

  // Record the worker's dependencies.
  for (const auto &object_id : required_objects) {
    if (local_objects_.count(object_id) == 0) {
      RAY_LOG(DEBUG) << "Worker " << worker_id << " called ray.wait on remote object "
                     << object_id;
      // Only add the dependency if the object is not local. If the object is
      // local, then the `ray.wait` call can already return it.
      auto inserted = worker_entry.insert(object_id);
      if (inserted.second) {
        // Get the ID of the task that creates the dependency.
        // TODO(qwang): Refine here to:
        // if (object_id.CreatedByTask()) {// ...}
        TaskID creating_task_id = object_id.TaskId();
        // Add the subscribed worker to the mapping from object ID to list of
        // dependent workers.
        required_tasks_[creating_task_id][object_id].dependent_workers.insert(worker_id);
      }
    }
  }

  // These dependencies are required by the given worker. Try to make them
  // local if necessary.
  for (const auto &object_id : required_objects) {
    HandleRemoteDependencyRequired(object_id);
  }
}

bool TaskDependencyManager::UnsubscribeGetDependencies(const TaskID &task_id) {
  RAY_LOG(DEBUG) << "Task " << task_id << " no longer blocked";
  // Remove the task from the table of subscribed tasks.
  auto it = task_dependencies_.find(task_id);
  if (it == task_dependencies_.end()) {
    return false;
  }
  const TaskDependencies task_entry = std::move(it->second);
  task_dependencies_.erase(it);

  // Remove the task's dependencies.
  for (const auto &object_id : task_entry.get_dependencies) {
    // Get the ID of the task that creates the dependency.
    TaskID creating_task_id = object_id.TaskId();
    auto creating_task_entry = required_tasks_.find(creating_task_id);
    // Remove the task from the list of tasks that are dependent on this
    // object.
    auto &dependent_tasks = creating_task_entry->second[object_id].dependent_tasks;
    RAY_CHECK(dependent_tasks.erase(task_id) > 0);
    // If nothing else depends on the object, then erase the object entry.
    if (creating_task_entry->second[object_id].Empty()) {
      creating_task_entry->second.erase(object_id);
      // Remove the task that creates this object if there are no more object
      // dependencies created by the task.
      if (creating_task_entry->second.empty()) {
        required_tasks_.erase(creating_task_entry);
      }
    }
  }

  // These dependencies are no longer required by the given task. Cancel any
  // in-progress operations to make them local.
  for (const auto &object_id : task_entry.get_dependencies) {
    HandleRemoteDependencyCanceled(object_id);
  }

  return true;
}

void TaskDependencyManager::UnsubscribeWaitDependencies(const WorkerID &worker_id) {
  RAY_LOG(DEBUG) << "Worker " << worker_id << " no longer blocked";
  // Remove the task from the table of subscribed tasks.
  auto it = worker_dependencies_.find(worker_id);
  if (it == worker_dependencies_.end()) {
    return;
  }
  const WorkerDependencies worker_entry = std::move(it->second);
  worker_dependencies_.erase(it);

  // Remove the task's dependencies.
  for (const auto &object_id : worker_entry) {
    // Get the ID of the task that creates the dependency.
    TaskID creating_task_id = object_id.TaskId();
    auto creating_task_entry = required_tasks_.find(creating_task_id);
    // Remove the worker from the list of workers that are dependent on this
    // object.
    auto &dependent_workers = creating_task_entry->second[object_id].dependent_workers;
    RAY_CHECK(dependent_workers.erase(worker_id) > 0);
    // If nothing else depends on the object, then erase the object entry.
    if (creating_task_entry->second[object_id].Empty()) {
      creating_task_entry->second.erase(object_id);
      // Remove the task that creates this object if there are no more object
      // dependencies created by the task.
      if (creating_task_entry->second.empty()) {
        required_tasks_.erase(creating_task_entry);
      }
    }
  }

  // These dependencies are no longer required by the given task. Cancel any
  // in-progress operations to make them local.
  for (const auto &object_id : worker_entry) {
    HandleRemoteDependencyCanceled(object_id);
  }
}

std::vector<TaskID> TaskDependencyManager::GetPendingTasks() const {
  std::vector<TaskID> keys;
  keys.reserve(pending_tasks_.size());
  for (const auto &id_task_pair : pending_tasks_) {
    keys.push_back(id_task_pair.first);
  }
  return keys;
}

void TaskDependencyManager::TaskPending(const Task &task) {
  TaskID task_id = task.GetTaskSpecification().TaskId();
  RAY_LOG(DEBUG) << "Task execution " << task_id << " pending";

  // Record that the task is pending execution.
  auto inserted =
      pending_tasks_.emplace(task_id, PendingTask(initial_lease_period_ms_, io_service_));
  if (inserted.second) {
    // This is the first time we've heard that this task is pending.  Find any
    // subscribed tasks that are dependent on objects created by the pending
    // task.
    auto remote_task_entry = required_tasks_.find(task_id);
    if (remote_task_entry != required_tasks_.end()) {
      for (const auto &object_entry : remote_task_entry->second) {
        // This object created by the pending task will appear locally once the
        // task completes execution. Cancel any in-progress operations to make
        // the object local.
        HandleRemoteDependencyCanceled(object_entry.first);
      }
    }

    // Acquire the lease for the task's execution in the global lease table.
    AcquireTaskLease(task_id);
  }
}

void TaskDependencyManager::AcquireTaskLease(const TaskID &task_id) {
  auto it = pending_tasks_.find(task_id);
  int64_t now_ms = current_time_ms();
  if (it == pending_tasks_.end()) {
    return;
  }

  // Check that we were able to renew the task lease before the previous one
  // expired.
  if (now_ms > it->second.expires_at) {
    RAY_LOG(WARNING) << "Task lease to renew has already expired by "
                     << (it->second.expires_at - now_ms) << "ms";
  }

  auto task_lease_data = std::make_shared<TaskLeaseData>();
  task_lease_data->set_node_manager_id(client_id_.Hex());
  task_lease_data->set_acquired_at(current_sys_time_ms());
  task_lease_data->set_timeout(it->second.lease_period);
  RAY_CHECK_OK(task_lease_table_.Add(JobID::Nil(), task_id, task_lease_data, nullptr));

  auto period = boost::posix_time::milliseconds(it->second.lease_period / 2);
  it->second.lease_timer->expires_from_now(period);
  it->second.lease_timer->async_wait(
      [this, task_id](const boost::system::error_code &error) {
        if (!error) {
          AcquireTaskLease(task_id);
        } else {
          // Check that the error was due to the timer being canceled.
          RAY_CHECK(error == boost::asio::error::operation_aborted);
        }
      });

  it->second.expires_at = now_ms + it->second.lease_period;
  it->second.lease_period = std::min(it->second.lease_period * 2,
                                     RayConfig::instance().max_task_lease_timeout_ms());
}

void TaskDependencyManager::TaskCanceled(const TaskID &task_id) {
  RAY_LOG(DEBUG) << "Task execution " << task_id << " canceled";
  // Record that the task is no longer pending execution.
  auto it = pending_tasks_.find(task_id);
  if (it == pending_tasks_.end()) {
    return;
  }
  pending_tasks_.erase(it);

  // Find any subscribed tasks that are dependent on objects created by the
  // canceled task.
  auto remote_task_entry = required_tasks_.find(task_id);
  if (remote_task_entry != required_tasks_.end()) {
    for (const auto &object_entry : remote_task_entry->second) {
      // This object created by the task will no longer appear locally since
      // the task is canceled.  Try to make the object local if necessary.
      HandleRemoteDependencyRequired(object_entry.first);
    }
  }
}

void TaskDependencyManager::RemoveTasksAndRelatedObjects(
    const std::unordered_set<TaskID> &task_ids) {
  // Collect a list of all the unique objects that these tasks were subscribed
  // to.
  std::unordered_set<ObjectID> required_objects;
  for (auto it = task_ids.begin(); it != task_ids.end(); it++) {
    auto task_it = task_dependencies_.find(*it);
    if (task_it != task_dependencies_.end()) {
      // Add the objects that this task was subscribed to.
      required_objects.insert(task_it->second.get_dependencies.begin(),
                              task_it->second.get_dependencies.end());
    }
    // The task no longer depends on anything.
    task_dependencies_.erase(*it);
    // The task is no longer pending execution.
    pending_tasks_.erase(*it);
  }

  // Cancel all of the objects that were required by the removed tasks.
  for (const auto &object_id : required_objects) {
    TaskID creating_task_id = object_id.TaskId();
    required_tasks_.erase(creating_task_id);
    HandleRemoteDependencyCanceled(object_id);
  }

  // Make sure that the tasks in task_ids no longer have tasks dependent on
  // them.
  for (const auto &task_id : task_ids) {
    RAY_CHECK(required_tasks_.find(task_id) == required_tasks_.end())
        << "RemoveTasksAndRelatedObjects was called on " << task_id
        << ", but another task depends on it that was not included in the argument";
  }
}

std::string TaskDependencyManager::DebugString() const {
  std::stringstream result;
  result << "TaskDependencyManager:";
  result << "\n- task dep map size: " << task_dependencies_.size();
  result << "\n- task req map size: " << required_tasks_.size();
  result << "\n- req objects map size: " << required_objects_.size();
  result << "\n- local objects map size: " << local_objects_.size();
  result << "\n- pending tasks map size: " << pending_tasks_.size();
  return result.str();
}

void TaskDependencyManager::RecordMetrics() const {
  stats::TaskDependencyManagerStats().Record(
      task_dependencies_.size(), {{stats::ValueTypeKey, "num_task_dependencies"}});
  stats::TaskDependencyManagerStats().Record(
      required_tasks_.size(), {{stats::ValueTypeKey, "num_required_tasks"}});
  stats::TaskDependencyManagerStats().Record(
      required_objects_.size(), {{stats::ValueTypeKey, "num_required_objects"}});
  stats::TaskDependencyManagerStats().Record(
      local_objects_.size(), {{stats::ValueTypeKey, "num_local_objects"}});
  stats::TaskDependencyManagerStats().Record(
      pending_tasks_.size(), {{stats::ValueTypeKey, "num_pending_tasks"}});
}

}  // namespace raylet

}  // namespace ray
