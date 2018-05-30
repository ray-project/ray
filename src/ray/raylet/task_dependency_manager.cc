#include "task_dependency_manager.h"

namespace ray {

namespace raylet {

TaskDependencyManager::TaskDependencyManager(ObjectManagerInterface &object_manager)
    : object_manager_(object_manager) {}

bool TaskDependencyManager::CheckObjectLocal(const ObjectID &object_id) const {
  return local_objects_.count(object_id) == 1;
}

bool TaskDependencyManager::CheckObjectRequired(const ObjectID &object_id) const {
  const TaskID task_id = ComputeTaskId(object_id);
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
    }
  }
}

void TaskDependencyManager::HandleRemoteDependencyCanceled(const ObjectID &object_id) {
  bool required = CheckObjectRequired(object_id);
  // If the object is no longer required, then cancel the object.
  if (!required) {
    auto it = required_objects_.find(object_id);
    if (it != required_objects_.end()) {
      RAY_CHECK_OK(object_manager_.Cancel(object_id));
      required_objects_.erase(it);
    }
  }
}

std::vector<TaskID> TaskDependencyManager::HandleObjectLocal(
    const ray::ObjectID &object_id) {
  RAY_LOG(DEBUG) << "object ready " << object_id.hex();
  // Add the object to the table of locally available objects.
  auto inserted = local_objects_.insert(object_id);
  RAY_CHECK(inserted.second);

  // Find any tasks that are dependent on the newly available object.
  std::vector<TaskID> ready_task_ids;
  auto creating_task_entry = required_tasks_.find(ComputeTaskId(object_id));
  if (creating_task_entry != required_tasks_.end()) {
    auto object_entry = creating_task_entry->second.find(object_id);
    if (object_entry != creating_task_entry->second.end()) {
      for (auto &dependent_task_id : object_entry->second) {
        auto &task_entry = task_dependencies_[dependent_task_id];
        task_entry.num_missing_dependencies--;
        // If the dependent task now has all of its arguments ready, it's ready
        // to run.
        if (task_entry.num_missing_dependencies == 0) {
          ready_task_ids.push_back(dependent_task_id);
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
  // Add the object to the table of locally available objects.
  auto erased = local_objects_.erase(object_id);
  RAY_CHECK(erased == 1);

  // Find any tasks that are dependent on the missing object.
  std::vector<TaskID> waiting_task_ids;
  TaskID creating_task_id = ComputeTaskId(object_id);
  auto creating_task_entry = required_tasks_.find(creating_task_id);
  if (creating_task_entry != required_tasks_.end()) {
    auto object_entry = creating_task_entry->second.find(object_id);
    if (object_entry != creating_task_entry->second.end()) {
      for (auto &dependent_task_id : object_entry->second) {
        auto &task_entry = task_dependencies_[dependent_task_id];
        // If the dependent task had all of its arguments ready, it was ready to
        // run but must be switched to waiting since one of its arguments is now
        // missing.
        if (task_entry.num_missing_dependencies == 0) {
          waiting_task_ids.push_back(dependent_task_id);
        }
        task_entry.num_missing_dependencies++;
      }
    }
  }
  // The object is no longer local. Try to make the object local if necessary.
  HandleRemoteDependencyRequired(object_id);
  // Process callbacks for all of the tasks dependent on the object that are
  // now ready to run.
  return waiting_task_ids;
}

bool TaskDependencyManager::SubscribeDependencies(
    const TaskID &task_id, const std::vector<ObjectID> &required_objects) {
  auto &task_entry = task_dependencies_[task_id];

  // Record the task's dependencies.
  for (const auto &object_id : required_objects) {
    auto inserted = task_entry.object_dependencies.insert(object_id);
    if (inserted.second) {
      // Get the ID of the task that creates the dependency.
      TaskID creating_task_id = ComputeTaskId(object_id);
      // Determine whether the dependency can be fulfilled by the local node.
      if (local_objects_.count(object_id) == 0) {
        // The object is not local.
        task_entry.num_missing_dependencies++;
      }
      // Add the subscribed task to the mapping from object ID to list of
      // dependent tasks.
      required_tasks_[creating_task_id][object_id].push_back(task_id);
    }
  }

  // These dependencies are required by the given task. Try to make them local
  // if necessary.
  for (const auto &object_id : required_objects) {
    HandleRemoteDependencyRequired(object_id);
  }

  // Return whether all dependencies are local.
  return (task_entry.num_missing_dependencies == 0);
}

void TaskDependencyManager::UnsubscribeDependencies(const TaskID &task_id) {
  // Remove the task from the table of subscribed tasks.
  auto it = task_dependencies_.find(task_id);
  RAY_CHECK(it != task_dependencies_.end());
  const TaskDependencies task_entry = std::move(it->second);
  task_dependencies_.erase(it);

  // Remove the task's dependencies.
  for (const auto &object_id : task_entry.object_dependencies) {
    // Remove the task from the list of tasks that are dependent on this
    // object.
    // Get the ID of the task that creates the dependency.
    TaskID creating_task_id = ComputeTaskId(object_id);
    auto creating_task_entry = required_tasks_.find(creating_task_id);
    std::vector<TaskID> &dependent_tasks = creating_task_entry->second[object_id];
    auto it = std::find(dependent_tasks.begin(), dependent_tasks.end(), task_id);
    RAY_CHECK(it != dependent_tasks.end());
    dependent_tasks.erase(it);
    // If the unsubscribed task was the only task dependent on the object, then
    // erase the object entry.
    if (dependent_tasks.empty()) {
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
  for (const auto &object_id : task_entry.object_dependencies) {
    HandleRemoteDependencyCanceled(object_id);
  }
}

void TaskDependencyManager::TaskPending(const Task &task) {
  TaskID task_id = task.GetTaskSpecification().TaskId();

  // Record that the task is pending execution.
  pending_tasks_.insert(task_id);
  // Find any subscribed tasks that are dependent on objects created by the
  // pending task.
  auto remote_task_entry = required_tasks_.find(task_id);
  if (remote_task_entry != required_tasks_.end()) {
    for (const auto &object_entry : remote_task_entry->second) {
      // This object created by the pending task will appear locally once the
      // task completes execution. Cancel any in-progress operations to make
      // the object local.
      HandleRemoteDependencyCanceled(object_entry.first);
    }
  }
}

void TaskDependencyManager::TaskCanceled(const TaskID &task_id) {
  // Record that the task is no longer pending execution.
  pending_tasks_.erase(task_id);
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

}  // namespace raylet

}  // namespace ray
