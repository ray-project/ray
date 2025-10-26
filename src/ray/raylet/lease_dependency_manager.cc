// Copyright 2020-2021 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "ray/raylet/lease_dependency_manager.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace ray {

namespace raylet {

bool LeaseDependencyManager::CheckObjectLocal(const ObjectID &object_id) const {
  return local_objects_.contains(object_id);
}

bool LeaseDependencyManager::GetOwnerAddress(const ObjectID &object_id,
                                             rpc::Address *owner_address) const {
  auto obj = required_objects_.find(object_id);
  if (obj == required_objects_.end()) {
    return false;
  }

  *owner_address = obj->second.owner_address;
  return !owner_address->worker_id().empty();
}

void LeaseDependencyManager::RemoveObjectIfNotNeeded(
    absl::flat_hash_map<ObjectID, LeaseDependencyManager::ObjectDependencies>::iterator
        required_object_it) {
  const auto &object_id = required_object_it->first;
  if (required_object_it->second.Empty()) {
    RAY_LOG(DEBUG) << "Object " << object_id << " no longer needed";
    if (required_object_it->second.wait_request_id > 0) {
      RAY_LOG(DEBUG) << "Canceling pull for wait request of object " << object_id
                     << " request: " << required_object_it->second.wait_request_id;
      object_manager_.CancelPull(required_object_it->second.wait_request_id);
    }
    required_objects_.erase(required_object_it);
  }
}

absl::flat_hash_map<ObjectID, LeaseDependencyManager::ObjectDependencies>::iterator
LeaseDependencyManager::GetOrInsertRequiredObject(const ObjectID &object_id,
                                                  const rpc::ObjectReference &ref) {
  auto it = required_objects_.find(object_id);
  if (it == required_objects_.end()) {
    it = required_objects_.emplace(object_id, ref).first;
  }
  return it;
}

void LeaseDependencyManager::StartOrUpdateWaitRequest(
    const WorkerID &worker_id,
    const std::vector<rpc::ObjectReference> &required_objects) {
  RAY_LOG(DEBUG) << "Starting wait request for worker " << worker_id;
  auto &wait_request = wait_requests_[worker_id];
  for (const auto &ref : required_objects) {
    const auto obj_id = ObjectRefToId(ref);
    if (local_objects_.contains(obj_id)) {
      // Object is already local. No need to fetch it.
      continue;
    }

    if (wait_request.insert(obj_id).second) {
      RAY_LOG(DEBUG) << "Worker " << worker_id << " called ray.wait on non-local object "
                     << obj_id;
      auto it = GetOrInsertRequiredObject(obj_id, ref);
      it->second.dependent_wait_requests.insert(worker_id);
      if (it->second.wait_request_id == 0) {
        it->second.wait_request_id =
            object_manager_.Pull({ref}, BundlePriority::WAIT_REQUEST, {"", false});
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

void LeaseDependencyManager::CancelWaitRequest(const WorkerID &worker_id) {
  RAY_LOG(DEBUG) << "Canceling wait request for worker " << worker_id;
  auto req_iter = wait_requests_.find(worker_id);
  if (req_iter == wait_requests_.end()) {
    return;
  }

  for (const auto &obj_id : req_iter->second) {
    auto obj_iter = required_objects_.find(obj_id);
    RAY_CHECK(obj_iter != required_objects_.end());
    obj_iter->second.dependent_wait_requests.erase(worker_id);
    RemoveObjectIfNotNeeded(obj_iter);
  }

  wait_requests_.erase(req_iter);
}

void LeaseDependencyManager::StartOrUpdateGetRequest(
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
      ray::rpc::ObjectReference ref;
      ref.set_object_id(obj_id.Binary());
      ref.mutable_owner_address()->CopyFrom(it->second.owner_address);
      refs.push_back(std::move(ref));
    }
    // Pull the new dependencies before canceling the old request, in case some
    // of the old dependencies are still being fetched.
    uint64_t new_request_id =
        object_manager_.Pull(refs, BundlePriority::GET_REQUEST, {"", false});
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

void LeaseDependencyManager::CancelGetRequest(const WorkerID &worker_id) {
  RAY_LOG(DEBUG) << "Canceling get request for worker " << worker_id;
  auto req_iter = get_requests_.find(worker_id);
  if (req_iter == get_requests_.end()) {
    return;
  }

  RAY_LOG(DEBUG) << "Canceling pull for get request from worker " << worker_id
                 << " request: " << req_iter->second.second;
  object_manager_.CancelPull(req_iter->second.second);

  for (const auto &obj_id : req_iter->second.first) {
    auto obj_iter = required_objects_.find(obj_id);
    RAY_CHECK(obj_iter != required_objects_.end());
    obj_iter->second.dependent_get_requests.erase(worker_id);
    RemoveObjectIfNotNeeded(obj_iter);
  }

  get_requests_.erase(req_iter);
}

/// Request dependencies for a queued lease.
bool LeaseDependencyManager::RequestLeaseDependencies(
    const LeaseID &lease_id,
    const std::vector<rpc::ObjectReference> &required_objects,
    const TaskMetricsKey &task_key) {
  RAY_LOG(DEBUG) << "Adding dependencies for lease " << lease_id
                 << ". Required objects length: " << required_objects.size();

  const auto required_ids = ObjectRefsToIds(required_objects);
  absl::flat_hash_set<ObjectID> deduped_ids(required_ids.begin(), required_ids.end());
  auto inserted = queued_lease_requests_.emplace(
      lease_id,
      std::make_unique<LeaseDependencies>(
          std::move(deduped_ids), waiting_leases_counter_, task_key));
  RAY_CHECK(inserted.second) << "Lease depedencies can be requested only once per lease. "
                             << lease_id;
  auto &lease_entry = inserted.first->second;

  for (const auto &ref : required_objects) {
    const auto obj_id = ObjectRefToId(ref);
    RAY_LOG(DEBUG).WithField(lease_id).WithField(obj_id) << "Lease blocked on object";

    auto it = GetOrInsertRequiredObject(obj_id, ref);
    it->second.dependent_leases.insert(lease_id);
  }

  for (const auto &obj_id : lease_entry->dependencies_) {
    if (local_objects_.contains(obj_id)) {
      lease_entry->DecrementMissingDependencies();
    }
  }

  if (!required_objects.empty()) {
    lease_entry->pull_request_id_ =
        object_manager_.Pull(required_objects, BundlePriority::TASK_ARGS, task_key);
    RAY_LOG(DEBUG) << "Started pull for dependencies of lease " << lease_id
                   << " request: " << lease_entry->pull_request_id_;
  }

  return lease_entry->num_missing_dependencies_ == 0;
}

void LeaseDependencyManager::RemoveLeaseDependencies(const LeaseID &lease_id) {
  RAY_LOG(DEBUG) << "Removing dependencies for lease " << lease_id;
  auto lease_entry = queued_lease_requests_.find(lease_id);
  RAY_CHECK(lease_entry != queued_lease_requests_.end())
      << "Can't remove dependencies of tasks that are not queued.";

  if (lease_entry->second->pull_request_id_ > 0) {
    RAY_LOG(DEBUG) << "Canceling pull for dependencies of lease " << lease_id
                   << " request: " << lease_entry->second->pull_request_id_;
    object_manager_.CancelPull(lease_entry->second->pull_request_id_);
  }

  for (const auto &obj_id : lease_entry->second->dependencies_) {
    auto it = required_objects_.find(obj_id);
    RAY_CHECK(it != required_objects_.end());
    it->second.dependent_leases.erase(lease_id);
    RemoveObjectIfNotNeeded(it);
  }

  queued_lease_requests_.erase(lease_entry);
}

std::vector<LeaseID> LeaseDependencyManager::HandleObjectMissing(
    const ray::ObjectID &object_id) {
  RAY_CHECK(local_objects_.erase(object_id))
      << "Evicted object was not local " << object_id;

  // Find any leases that are dependent on the missing object.
  std::vector<LeaseID> waiting_lease_ids;
  auto object_entry = required_objects_.find(object_id);
  if (object_entry != required_objects_.end()) {
    for (auto &dependent_lease_id : object_entry->second.dependent_leases) {
      auto it = queued_lease_requests_.find(dependent_lease_id);
      RAY_CHECK(it != queued_lease_requests_.end());
      auto &lease_entry = it->second;
      // If the dependent lease had all of its arguments ready, it was ready to
      // run but must be switched to waiting since one of its arguments is now
      // missing.
      if (lease_entry->num_missing_dependencies_ == 0) {
        waiting_lease_ids.push_back(dependent_lease_id);
        // During normal execution we should be able to include the check
        // RAY_CHECK(pending_leases_.count(dependent_lease_id) == 1);
        // However, this invariant will not hold during unit test execution.
      }
      lease_entry->IncrementMissingDependencies();
    }
  }

  // Process callbacks for all of the leases dependent on the object that are
  // now ready to run.
  return waiting_lease_ids;
}

std::vector<LeaseID> LeaseDependencyManager::HandleObjectLocal(
    const ray::ObjectID &object_id) {
  // Add the object to the table of locally available objects.
  auto inserted = local_objects_.insert(object_id);
  RAY_CHECK(inserted.second) << "Local object was already local " << object_id;

  // Find all leases and workers that depend on the newly available object.
  std::vector<LeaseID> ready_lease_ids;
  auto object_entry = required_objects_.find(object_id);
  if (object_entry != required_objects_.end()) {
    // Loop through all leases that depend on the newly available object.
    for (const auto &dependent_lease_id : object_entry->second.dependent_leases) {
      auto it = queued_lease_requests_.find(dependent_lease_id);
      RAY_CHECK(it != queued_lease_requests_.end());
      auto &lease_entry = it->second;
      lease_entry->DecrementMissingDependencies();
      // If the dependent lease now has all of its arguments ready, it's ready
      // to run.
      if (lease_entry->num_missing_dependencies_ == 0) {
        ready_lease_ids.push_back(dependent_lease_id);
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
    RemoveObjectIfNotNeeded(object_entry);
  }

  return ready_lease_ids;
}

bool LeaseDependencyManager::LeaseDependenciesBlocked(const LeaseID &lease_id) const {
  auto it = queued_lease_requests_.find(lease_id);
  RAY_CHECK(it != queued_lease_requests_.end());
  RAY_CHECK(it->second->pull_request_id_ != 0);
  return !object_manager_.PullRequestActiveOrWaitingForMetadata(
      it->second->pull_request_id_);
}

std::string LeaseDependencyManager::DebugString() const {
  std::stringstream result;
  result << "LeaseDependencyManager:";
  result << "\n- lease deps map size: " << queued_lease_requests_.size();
  result << "\n- get req map size: " << get_requests_.size();
  result << "\n- wait req map size: " << wait_requests_.size();
  result << "\n- local objects map size: " << local_objects_.size();
  return result.str();
}

void LeaseDependencyManager::RecordMetrics() {
  waiting_leases_counter_.FlushOnChangeCallbacks();
}

}  // namespace raylet

}  // namespace ray
