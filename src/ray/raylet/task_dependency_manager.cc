#ifndef TASK_DEPENDENCY_MANAGER_CC
#define TASK_DEPENDENCY_MANAGER_CC

#include "task_dependency_manager.h"

#include "common.h"

using namespace std;
namespace ray {


TaskDependencyManager::TaskDependencyManager(
    ObjectManager &object_manager,
    //ReconstructionPolicy &reconstruction_policy,
    std::function<void(const TaskID&)> handler)
    : object_manager_(object_manager),
      //reconstruction_policy_(reconstruction_policy),
      task_ready_callback_(handler) {
  // TODO(swang): Check return status.
  ray::Status status = object_manager_.SubscribeObjAdded(
      std::bind(&TaskDependencyManager::handleObjectReady, this, std::placeholders::_1));
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

void TaskDependencyManager::handleObjectReady(const ray::ObjectID& object_id) {
  LOG_INFO("object %s ready", object_id.hex().c_str());
  CHECK(local_objects_.count(object_id) == 0);
  local_objects_.insert(object_id);

  std::vector<TaskID> ready_task_ids;
  auto dependent_tasks = remote_object_dependencies_.find(object_id);
  if (dependent_tasks != remote_object_dependencies_.end()) {
    for (auto &dependent_task_id : dependent_tasks->second) {
      if (argumentsReady(task_dependencies_[dependent_task_id])) {
        ready_task_ids.push_back(dependent_task_id);
      }
    }
    remote_object_dependencies_.erase(dependent_tasks);
  }
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
    }
  }
  // Check that the task has some missing arguments.
  CHECK(num_missing_arguments > 0);
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

} // end namespace ray

#endif  // TASK_DEPENDENCY_MANAGER_CC
