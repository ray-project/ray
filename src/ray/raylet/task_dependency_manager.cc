#include "task_dependency_manager.h"

namespace ray {

TaskDependencyManager::TaskDependencyManager(
    ObjectManager &object_manager,
    // ReconstructionPolicy &reconstruction_policy,
    std::function<void(const TaskID &)> handler)
    : object_manager_(object_manager),
      // reconstruction_policy_(reconstruction_policy),
      task_ready_callback_(handler) {
  // TODO(swang): Check return status.
  ray::Status status = object_manager_.SubscribeObjAdded(
      [this](const ObjectID &object_id) { handleObjectReady(object_id); });
  // TODO(swang): Subscribe to object removed notifications.
}

bool TaskDependencyManager::argumentsReady(const std::vector<ObjectID> arguments) const {
  for (auto &argument : arguments) {
    // Check if any argument is missing.
    if (local_objects_.count(argument) == 0) {
      return false;
    }
  }
  // All arguments are ready.
  return true;
}

void TaskDependencyManager::handleObjectReady(const ray::ObjectID &object_id) {
  RAY_LOG(DEBUG) << "object ready " << object_id.hex();
  // Add the object to the table of locally available objects.
  RAY_CHECK(local_objects_.count(object_id) == 0);
  local_objects_.insert(object_id);

  // Handle any tasks that were dependent on the newly available object.
  std::vector<TaskID> ready_task_ids;
  auto dependent_tasks = remote_object_dependencies_.find(object_id);
  if (dependent_tasks != remote_object_dependencies_.end()) {
    for (auto &dependent_task_id : dependent_tasks->second) {
      // If the dependent task now has all of its arguments ready, it's ready
      // to run.
      if (argumentsReady(task_dependencies_[dependent_task_id])) {
        ready_task_ids.push_back(dependent_task_id);
      }
    }
    remote_object_dependencies_.erase(dependent_tasks);
  }
  // Process callbacks for all of the tasks dependent on the object that are
  // now ready to run.
  for (auto &ready_task_id : ready_task_ids) {
    UnsubscribeTaskReady(ready_task_id);
    task_ready_callback_(ready_task_id);
  }
}

bool TaskDependencyManager::TaskReady(const Task &task) const {
  const std::vector<ObjectID> arguments = task.GetDependencies();
  return argumentsReady(arguments);
}

void TaskDependencyManager::SubscribeTaskReady(const Task &task) {
  TaskID task_id = task.GetTaskSpecification().TaskId();
  const std::vector<ObjectID> arguments = task.GetDependencies();
  // Add the task's arguments to the table of subscribed tasks.
  task_dependencies_[task_id] = arguments;
  // Add the task's remote arguments to the table of remote objects.
  int num_missing_arguments = 0;
  for (auto &argument : arguments) {
    if (local_objects_.count(argument) == 0) {
      remote_object_dependencies_[argument].push_back(task_id);
      num_missing_arguments++;
      // TODO(swang): Check return status.
      // TODO(swang): Handle Pull failure (if object manager does not retry).
      // TODO(atumanov): pull return status should be propagated back to the caller.
      ray::Status status = object_manager_.Pull(argument);
    }
  }
  // Check that the task has some missing arguments.
  RAY_CHECK(num_missing_arguments > 0);
}

void TaskDependencyManager::UnsubscribeTaskReady(const TaskID &task_id) {
  const std::vector<ObjectID> arguments = task_dependencies_[task_id];
  // Remove the task from the table of subscribed tasks.
  task_dependencies_.erase(task_id);
  // Remove the task from the table of remote objects to dependent tasks.
  for (auto &argument : arguments) {
    if (local_objects_.count(argument) == 1) {
      continue;
    }
    // The argument is not local. Remove the task from the list of tasks that
    // are dependent on this object.
    std::vector<TaskID> &dependent_tasks = remote_object_dependencies_[task_id];
    for (auto it = dependent_tasks.begin(); it != dependent_tasks.end(); it++) {
      if (*it == task_id) {
        it = dependent_tasks.erase(it);
        break;
      }
    }
  }
}

void TaskDependencyManager::MarkDependencyReady(const ObjectID &object) {
  throw std::runtime_error("Method not implemented");
}

}  // namespace ray
