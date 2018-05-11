#include "task_dependency_manager.h"

namespace ray {

namespace raylet {

TaskDependencyManager::TaskDependencyManager() {}

bool TaskDependencyManager::CheckObjectLocal(const ObjectID &object_id) const {
  return local_objects_.count(object_id) == 1;
}

std::vector<TaskID> TaskDependencyManager::HandleObjectLocal(
    const ray::ObjectID &object_id) {
  RAY_LOG(DEBUG) << "object ready " << object_id.hex();
  // Add the object to the table of locally available objects.
  auto inserted = local_objects_.insert(object_id);
  RAY_CHECK(inserted.second);

  // Find any tasks that are dependent on the newly available object.
  std::vector<TaskID> ready_task_ids;
  auto object_entry = remote_object_dependencies_.find(object_id);
  if (object_entry != remote_object_dependencies_.end()) {
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

  return ready_task_ids;
}

std::vector<TaskID> TaskDependencyManager::HandleObjectMissing(
    const ray::ObjectID &object_id) {
  // Add the object to the table of locally available objects.
  auto erased = local_objects_.erase(object_id);
  RAY_CHECK(erased == 1);

  // Find any tasks that are dependent on the missing object.
  std::vector<TaskID> waiting_task_ids;
  auto object_entry = remote_object_dependencies_.find(object_id);
  if (object_entry != remote_object_dependencies_.end()) {
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
  // Process callbacks for all of the tasks dependent on the object that are
  // now ready to run.
  return waiting_task_ids;
}

bool TaskDependencyManager::SubscribeDependencies(
    const TaskID &task_id, const std::vector<ObjectID> &required_objects,
    std::vector<ObjectID> &remote_objects) {
  auto &task_entry = task_dependencies_[task_id];

  // Add the task's dependencies to the table of subscribed tasks.
  for (const auto &object_id : required_objects) {
    auto inserted = task_entry.object_dependencies.insert(object_id);
    if (inserted.second) {
      // Record the task's dependency in the corresponding object entry.
      if (local_objects_.count(object_id) == 0) {
        // The object is not local.
        task_entry.num_missing_dependencies++;
        if (pending_tasks_.count(ComputeTaskId(object_id)) == 0) {
          // If the object is not local and the task that creates the object is not
          // pending on this node, then the object must be remote.
          remote_objects.push_back(object_id);
        }
      }
      remote_object_dependencies_[object_id].push_back(task_id);
    }
  }

  return (task_entry.num_missing_dependencies == 0);
}

void TaskDependencyManager::UnsubscribeDependencies(
    const TaskID &task_id, std::vector<ObjectID> &canceled_objects) {
  // Remove the task from the table of subscribed tasks.
  auto it = task_dependencies_.find(task_id);
  RAY_CHECK(it != task_dependencies_.end());
  const TaskDependencies task_entry = std::move(it->second);
  task_dependencies_.erase(it);

  // Remove the task from the table of objects to dependent tasks.
  for (const auto &object_id : task_entry.object_dependencies) {
    // Remove the task from the list of tasks that are dependent on this
    // object.
    auto object_entry = remote_object_dependencies_.find(object_id);
    RAY_CHECK(object_entry != remote_object_dependencies_.end());
    std::vector<TaskID> &dependent_tasks = object_entry->second;
    for (auto it = dependent_tasks.begin(); it != dependent_tasks.end(); it++) {
      if (*it == task_id) {
        it = dependent_tasks.erase(it);
        break;
      }
    }
    if (dependent_tasks.empty()) {
      remote_object_dependencies_.erase(object_entry);
      canceled_objects.push_back(object_id);
    }
  }
}

void TaskDependencyManager::TaskPending(const Task &task,
                                        std::vector<ObjectID> &canceled_objects) {
  TaskID task_id = task.GetTaskSpecification().TaskId();

  // Record that the results of this task are pending creation.
  pending_tasks_.insert(task_id);

  for (const auto &object_entry : remote_object_dependencies_) {
    if (ComputeTaskId(object_entry.first) == task_id) {
      canceled_objects.push_back(object_entry.first);
    }
  }
}

void TaskDependencyManager::TaskCanceled(const TaskID &task_id,
                                         std::vector<ObjectID> &remote_objects) {
  for (const auto &object_entry : remote_object_dependencies_) {
    if (ComputeTaskId(object_entry.first) == task_id &&
        local_objects_.count(object_entry.first) == 0) {
      remote_objects.push_back(object_entry.first);
    }
  }

  pending_tasks_.erase(task_id);
}

}  // namespace raylet

}  // namespace ray
