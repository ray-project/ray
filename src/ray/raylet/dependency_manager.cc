#include "ray/raylet/dependency_manager.h"

namespace ray {

namespace raylet {

bool DependencyManager::CheckObjectLocal(const ObjectID &object_id) const {
  return local_objects_.count(object_id) == 1;
}

bool DependencyManager::GetOwnerAddress(const ObjectID &object_id,
                                        rpc::Address *owner_address) const {
  auto obj = required_objects_.find(object_id);
  if (obj == required_objects_.end()) {
    return false;
  }

  *owner_address = obj->second.owner_address;
  return !owner_address->worker_id().empty();
}

void DependencyManager::RemoveObjectIfNotNeeded(
    absl::flat_hash_map<ObjectID, DependencyManager::ObjectDependencies>::iterator
        required_object_it) {
  const auto &object_id = required_object_it->first;
  if (required_object_it->second.Empty()) {
    RAY_LOG(DEBUG) << "Object " << object_id << " no longer needed";
    if (required_object_it->second.wait_request_id > 0) {
      RAY_LOG(DEBUG) << "Canceling pull for wait request of object " << object_id
                     << " request: " << required_object_it->second.wait_request_id;
      object_manager_.CancelPull(required_object_it->second.wait_request_id);
    }
    if (!local_objects_.count(object_id)) {
      reconstruction_policy_.Cancel(object_id);
    }
    required_objects_.erase(required_object_it);
  }
}

absl::flat_hash_map<ObjectID, DependencyManager::ObjectDependencies>::iterator
DependencyManager::GetOrInsertRequiredObject(const ObjectID &object_id,
                                             const rpc::ObjectReference &ref) {
  auto it = required_objects_.find(object_id);
  if (it == required_objects_.end()) {
    it = required_objects_.emplace(object_id, ref).first;
    if (local_objects_.count(object_id) == 0) {
      reconstruction_policy_.ListenAndMaybeReconstruct(object_id, ref.owner_address());
    }
  }
  return it;
}

void DependencyManager::StartOrUpdateWaitRequest(
    const WorkerID &worker_id,
    const std::vector<rpc::ObjectReference> &required_objects) {
  RAY_LOG(DEBUG) << "Starting wait request for worker " << worker_id;
  auto &wait_request = wait_requests_[worker_id];
  for (const auto &ref : required_objects) {
    const auto obj_id = ObjectRefToId(ref);
    if (local_objects_.count(obj_id)) {
      // Object is already local. No need to fetch it.
      continue;
    }

    if (wait_request.insert(obj_id).second) {
      RAY_LOG(DEBUG) << "Worker " << worker_id << " called ray.wait on non-local object "
                     << obj_id;
      auto it = GetOrInsertRequiredObject(obj_id, ref);
      it->second.dependent_wait_requests.insert(worker_id);
      if (it->second.wait_request_id == 0) {
        it->second.wait_request_id = object_manager_.Pull({ref});
        RAY_LOG(DEBUG) << "Started pull for wait request for object " << obj_id
                       << " request: " << it->second.wait_request_id;
      }
    }
  }

  // No new objects to wait on. Delete the empty entry that was created.
  if (wait_request.empty()) {
    wait_requests_.erase(worker_id);
  }
}

void DependencyManager::CancelWaitRequest(const WorkerID &worker_id) {
  RAY_LOG(DEBUG) << "Canceling wait request for worker " << worker_id;
  auto it = wait_requests_.find(worker_id);
  if (it == wait_requests_.end()) {
    return;
  }

  for (const auto &obj_id : it->second) {
    auto it = required_objects_.find(obj_id);
    RAY_CHECK(it != required_objects_.end());
    it->second.dependent_wait_requests.erase(worker_id);
    RemoveObjectIfNotNeeded(it);
  }

  wait_requests_.erase(it);
}

void DependencyManager::StartOrUpdateGetRequest(
    const WorkerID &worker_id,
    const std::vector<rpc::ObjectReference> &required_objects) {
  RAY_LOG(DEBUG) << "Starting get request for worker " << worker_id;
  auto &get_request = get_requests_[worker_id];
  bool modified = false;
  for (const auto &ref : required_objects) {
    const auto obj_id = ObjectRefToId(ref);
    if (get_request.first.insert(obj_id).second) {
      RAY_LOG(DEBUG) << "Worker " << worker_id << " called ray.get on object " << obj_id;
      auto it = GetOrInsertRequiredObject(obj_id, ref);
      it->second.dependent_get_requests.insert(worker_id);
      modified = true;
    }
  }

  if (modified) {
    std::vector<rpc::ObjectReference> refs;
    for (auto &obj_id : get_request.first) {
      auto it = required_objects_.find(obj_id);
      RAY_CHECK(it != required_objects_.end());
      refs.push_back(ObjectIdToRef(obj_id, it->second.owner_address));
    }
    // Pull the new dependencies before canceling the old request, in case some
    // of the old dependencies are still being fetched.
    uint64_t new_request_id = object_manager_.Pull(refs);
    if (get_request.second != 0) {
      RAY_LOG(DEBUG) << "Canceling pull for get request from worker " << worker_id
                     << " request: " << get_request.second;
      object_manager_.CancelPull(get_request.second);
    }
    get_request.second = new_request_id;
    RAY_LOG(DEBUG) << "Started pull for get request from worker " << worker_id
                   << " request: " << get_request.second;
  }
}

void DependencyManager::CancelGetRequest(const WorkerID &worker_id) {
  RAY_LOG(DEBUG) << "Canceling get request for worker " << worker_id;
  auto it = get_requests_.find(worker_id);
  if (it == get_requests_.end()) {
    return;
  }

  RAY_LOG(DEBUG) << "Canceling pull for get request from worker " << worker_id
                 << " request: " << it->second.second;
  object_manager_.CancelPull(it->second.second);

  for (const auto &obj_id : it->second.first) {
    auto it = required_objects_.find(obj_id);
    RAY_CHECK(it != required_objects_.end());
    it->second.dependent_get_requests.erase(worker_id);
    RemoveObjectIfNotNeeded(it);
  }

  get_requests_.erase(it);
}

/// Request dependencies for a queued task.
bool DependencyManager::RequestTaskDependencies(
    const TaskID &task_id, const std::vector<rpc::ObjectReference> &required_objects) {
  RAY_LOG(DEBUG) << "Adding dependencies for task " << task_id;
  auto inserted = queued_task_requests_.emplace(task_id, required_objects);
  RAY_CHECK(inserted.second) << "Task depedencies can be requested only once per task.";
  auto &task_entry = inserted.first->second;

  for (const auto &ref : required_objects) {
    const auto obj_id = ObjectRefToId(ref);
    RAY_LOG(DEBUG) << "Task " << task_id << " blocked on object " << obj_id;

    auto it = GetOrInsertRequiredObject(obj_id, ref);
    it->second.dependent_tasks.insert(task_id);

    if (local_objects_.count(obj_id)) {
      task_entry.num_missing_dependencies--;
    }
  }

  if (!required_objects.empty()) {
    task_entry.pull_request_id = object_manager_.Pull(required_objects);
    RAY_LOG(DEBUG) << "Started pull for dependencies of task " << task_id
                   << " request: " << task_entry.pull_request_id;
  }

  return task_entry.num_missing_dependencies == 0;
}

void DependencyManager::RemoveTaskDependencies(const TaskID &task_id) {
  RAY_LOG(DEBUG) << "Removing dependencies for task " << task_id;
  auto task_entry = queued_task_requests_.find(task_id);
  RAY_CHECK(task_entry != queued_task_requests_.end())
      << "Can't remove dependencies of tasks that are not queued.";

  if (task_entry->second.pull_request_id > 0) {
    RAY_LOG(DEBUG) << "Canceling pull for dependencies of task " << task_id
                   << " request: " << task_entry->second.pull_request_id;
    object_manager_.CancelPull(task_entry->second.pull_request_id);
  }

  for (const auto &obj_id : task_entry->second.dependencies) {
    auto it = required_objects_.find(obj_id);
    RAY_CHECK(it != required_objects_.end());
    it->second.dependent_tasks.erase(task_id);
    RemoveObjectIfNotNeeded(it);
  }

  queued_task_requests_.erase(task_entry);
}

std::vector<TaskID> DependencyManager::HandleObjectMissing(
    const ray::ObjectID &object_id) {
  RAY_CHECK(local_objects_.erase(object_id))
      << "Evicted object was not local " << object_id;

  // Find any tasks that are dependent on the missing object.
  std::vector<TaskID> waiting_task_ids;
  auto object_entry = required_objects_.find(object_id);
  if (object_entry != required_objects_.end()) {
    for (auto &dependent_task_id : object_entry->second.dependent_tasks) {
      auto it = queued_task_requests_.find(dependent_task_id);
      RAY_CHECK(it != queued_task_requests_.end());
      auto &task_entry = it->second;
      // If the dependent task had all of its arguments ready, it was ready to
      // run but must be switched to waiting since one of its arguments is now
      // missing.
      if (task_entry.num_missing_dependencies == 0) {
        waiting_task_ids.push_back(dependent_task_id);
        // During normal execution we should be able to include the check
        // RAY_CHECK(pending_tasks_.count(dependent_task_id) == 1);
        // However, this invariant will not hold during unit test execution.
      }
      task_entry.num_missing_dependencies++;
    }

    // The object is missing and needed so wait for a possible failure again.
    reconstruction_policy_.ListenAndMaybeReconstruct(object_entry->first,
                                                     object_entry->second.owner_address);
  }

  // Process callbacks for all of the tasks dependent on the object that are
  // now ready to run.
  return waiting_task_ids;
}

std::vector<TaskID> DependencyManager::HandleObjectLocal(const ray::ObjectID &object_id) {
  // Add the object to the table of locally available objects.
  auto inserted = local_objects_.insert(object_id);
  RAY_CHECK(inserted.second) << "Local object was already local " << object_id;

  // Find all tasks and workers that depend on the newly available object.
  std::vector<TaskID> ready_task_ids;
  auto object_entry = required_objects_.find(object_id);
  if (object_entry != required_objects_.end()) {
    // Loop through all tasks that depend on the newly available object.
    for (const auto &dependent_task_id : object_entry->second.dependent_tasks) {
      auto it = queued_task_requests_.find(dependent_task_id);
      RAY_CHECK(it != queued_task_requests_.end());
      auto &task_entry = it->second;
      task_entry.num_missing_dependencies--;
      // If the dependent task now has all of its arguments ready, it's ready
      // to run.
      if (task_entry.num_missing_dependencies == 0) {
        ready_task_ids.push_back(dependent_task_id);
      }
    }

    // Remove the dependency from all workers that called `ray.wait` on the
    // newly available object.
    for (const auto &worker_id : object_entry->second.dependent_wait_requests) {
      auto worker_it = wait_requests_.find(worker_id);
      RAY_CHECK(worker_it != wait_requests_.end());
      RAY_CHECK(worker_it->second.erase(object_id) > 0);
      if (worker_it->second.empty()) {
        wait_requests_.erase(worker_it);
      }
    }
    // Clear all workers that called `ray.wait` on this object, since the
    // `ray.wait` calls can now return the object as ready.
    object_entry->second.dependent_wait_requests.clear();
    if (object_entry->second.wait_request_id > 0) {
      RAY_LOG(DEBUG) << "Canceling pull for wait request of object " << object_id
                     << " request: " << object_entry->second.wait_request_id;
      object_manager_.CancelPull(object_entry->second.wait_request_id);
      object_entry->second.wait_request_id = 0;
    }
    reconstruction_policy_.Cancel(object_entry->first);
    RemoveObjectIfNotNeeded(object_entry);
  }

  return ready_task_ids;
}

std::string DependencyManager::DebugString() const {
  std::stringstream result;
  result << "TaskDependencyManager:";
  result << "\n- task deps map size: " << queued_task_requests_.size();
  result << "\n- get req map size: " << get_requests_.size();
  result << "\n- wait req map size: " << wait_requests_.size();
  result << "\n- local objects map size: " << local_objects_.size();
  return result.str();
}

}  // namespace raylet

}  // namespace ray
