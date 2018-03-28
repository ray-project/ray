#include "task_dependency_manager.h"

namespace ray {

namespace raylet {

TaskDependencyManager::TaskDependencyManager(
    std::function<void(const ObjectID)> object_remote_handler,
    std::function<void(const TaskID &)> task_ready_handler,
    std::function<void(const TaskID &)> task_waiting_handler)
    : object_remote_callback_(object_remote_handler),
      task_ready_callback_(task_ready_handler),
      task_waiting_callback_(task_waiting_handler) {
  // TODO(swang): Subscribe to object removed notifications.
}

bool TaskDependencyManager::argumentsReady(
    const std::vector<ObjectID> argument_ids) const {
  for (auto &argument_id : argument_ids) {
    // Check if any argument is missing.
    const auto entry = local_objects_.find(argument_id);
    if (entry == local_objects_.end()) {
      return false;
    }
    if (entry->second.status == ObjectAvailability::kRemote) {
      return false;
    }
  }
  // All arguments are ready.
  return true;
}

void TaskDependencyManager::HandleObjectReady(const ray::ObjectID &object_id) {
  RAY_LOG(DEBUG) << "object ready " << object_id.hex();
  // Add the object to the table of locally available objects.
  auto &object_entry = local_objects_[object_id];
  RAY_CHECK(object_entry.status != ObjectAvailability::kLocal);
  object_entry.status = ObjectAvailability::kLocal;

  // Find any tasks that are dependent on the newly available object.
  std::vector<TaskID> ready_task_ids;
  for (auto &dependent_task_id : object_entry.dependent_tasks) {
    auto &task_entry = task_dependencies_[dependent_task_id];
    task_entry.num_missing_arguments--;
    // If the dependent task now has all of its arguments ready, it's ready
    // to run.
    if (task_entry.num_missing_arguments == 0) {
      ready_task_ids.push_back(dependent_task_id);
    }
  }
  // Process callbacks for all of the tasks dependent on the object that are
  // now ready to run.
  for (auto &ready_task_id : ready_task_ids) {
    task_ready_callback_(ready_task_id);
  }
}

void TaskDependencyManager::HandleObjectMissing(const ray::ObjectID &object_id) {
  RAY_LOG(DEBUG) << "object ready " << object_id.hex();
  // Add the object to the table of locally available objects.
  auto &object_entry = local_objects_[object_id];
  RAY_CHECK(object_entry.status == ObjectAvailability::kLocal);
  object_entry.status = ObjectAvailability::kRemote;

  // Find any tasks that are dependent on the missing object.
  std::vector<TaskID> waiting_task_ids;
  for (auto &dependent_task_id : object_entry.dependent_tasks) {
    auto &task_entry = task_dependencies_[dependent_task_id];
    // If the dependent task had all of its arguments ready, it was ready to
    // run but must be switched to waiting since one of its arguments is now
    // missing.
    if (task_entry.num_missing_arguments == 0) {
      waiting_task_ids.push_back(dependent_task_id);
    }
    task_entry.num_missing_arguments++;
  }
  // Process callbacks for all of the tasks dependent on the object that are
  // now ready to run.
  for (auto &waiting_task_id : waiting_task_ids) {
    task_waiting_callback_(waiting_task_id);
  }
}

bool TaskDependencyManager::TaskReady(const Task &task) const {
  const std::vector<ObjectID> arguments = task.GetDependencies();
  return argumentsReady(arguments);
}

void TaskDependencyManager::SubscribeTaskReady(const Task &task) {
  TaskID task_id = task.GetTaskSpecification().TaskId();
  TaskEntry task_entry;

  // Add the task's arguments to the table of subscribed tasks.
  task_entry.arguments = task.GetDependencies();
  task_entry.num_missing_arguments = task_entry.arguments.size();
  // Record the task as being dependent on each of its arguments.
  for (const auto &argument_id : task_entry.arguments) {
    auto &argument_entry = local_objects_[argument_id];
    if (argument_entry.status == ObjectAvailability::kRemote) {
      object_remote_callback_(argument_id);
    } else if (argument_entry.status == ObjectAvailability::kLocal) {
      task_entry.num_missing_arguments--;
    }
    argument_entry.dependent_tasks.push_back(task_id);
  }

  for (int i = 0; i < task.GetTaskSpecification().NumReturns(); i++) {
    auto return_id = task.GetTaskSpecification().ReturnId(i);
    task_entry.returns.push_back(return_id);
  }
  // Record the task's return values as pending creation.
  for (const auto &return_id : task_entry.returns) {
    auto &return_entry = local_objects_[return_id];
    // Some of a task's return values may already be local if this is a
    // re-executed task and it created multiple objects, only some of which
    // needed to be reconstructed. We only want to mark the object as waiting
    // for creation if it was previously not available at all.
    if (return_entry.status < ObjectAvailability::kWaiting) {
      // The object does not already exist locally. Mark the object as
      // pending creation.
      return_entry.status = ObjectAvailability::kWaiting;
    }
  }

  auto emplaced = task_dependencies_.emplace(task_id, task_entry);
  RAY_CHECK(emplaced.second);

  if (task_entry.num_missing_arguments == 0) {
    task_ready_callback_(task_id);
  } else {
    task_waiting_callback_(task_id);
  }
}

void TaskDependencyManager::UnsubscribeTaskReady(const TaskID &task_id) {
  // Remove the task from the table of subscribed tasks.
  auto it = task_dependencies_.find(task_id);
  RAY_CHECK(it != task_dependencies_.end());
  const TaskEntry task_entry = std::move(it->second);
  task_dependencies_.erase(it);

  // Remove the task from the table of objects to dependent tasks.
  for (const auto &argument_id : task_entry.arguments) {
    // Remove the task from the list of tasks that are dependent on this
    // object.
    auto argument_entry = local_objects_.find(argument_id);
    std::vector<TaskID> &dependent_tasks = argument_entry->second.dependent_tasks;
    for (auto it = dependent_tasks.begin(); it != dependent_tasks.end(); it++) {
      if (*it == task_id) {
        it = dependent_tasks.erase(it);
        break;
      }
    }
    if (dependent_tasks.empty()) {
      if (argument_entry->second.status == ObjectAvailability::kRemote) {
        local_objects_.erase(argument_entry);
      }
    }
  }

  // Record the task's return values as remote.
  for (const auto &return_id : task_entry.returns) {
    auto return_entry = local_objects_.find(return_id);
    RAY_CHECK(return_entry != local_objects_.end());
    // Some of a task's return values may already be local if this is a
    // re-executed task and it created multiple objects, only some of which
    // needed to be reconstructed. We only want to downgrade the object to
    // REMOTE if it was previously not available at all.
    // TODO(swang): Pass in flag for tasks whose objects will soon be created,
    // so that we don't mark them as remote while waiting for the object store
    // notification.
    if (return_entry->second.status == ObjectAvailability::kWaiting) {
      // The object was pending creation on this node. Mark the object as
      // remote.
      return_entry->second.status = ObjectAvailability::kRemote;
    }
    if (return_entry->second.dependent_tasks.empty()) {
      local_objects_.erase(return_entry);
    } else if (return_entry->second.status == ObjectAvailability::kRemote) {
      object_remote_callback_(return_id);
    }
  }
}

}  // namespace raylet

}  // namespace ray
