#include "ray/core_worker/reference_count.h"

namespace ray {

void ReferenceCounter::AddReference(const ObjectID &object_id, size_t num_references) {
  absl::MutexLock lock(&mutex_);
  auto entry = object_id_refs_.find(object_id);
  if (entry == object_id_refs_.end()) {
    object_id_refs_[object_id] = std::make_pair(num_references, nullptr);
  } else {
    entry->second.first += num_references;
  }
}

void ReferenceCounter::SetDependencies(
    const ObjectID &object_id, std::shared_ptr<std::vector<ObjectID>> dependencies) {
  absl::MutexLock lock(&mutex_);
  auto entry = object_id_refs_.find(object_id);
  if (entry == object_id_refs_.end()) {
    object_id_refs_[object_id] = std::make_pair(0, dependencies);
  } else {
    RAY_CHECK(!entry->second.second);
    entry->second.second = dependencies;
  }
}

void ReferenceCounter::RemoveReference(const ObjectID &object_id) {
  absl::MutexLock lock(&mutex_);
  RemoveReferenceRecursive(object_id);
}

void ReferenceCounter::RemoveReferenceRecursive(const ObjectID &object_id) {
  auto entry = object_id_refs_.find(object_id);
  if (entry == object_id_refs_.end()) {
    RAY_LOG(WARNING) << "Tried to decrease ref count for nonexistent object ID: "
                     << object_id;
    return;
  }
  if (--entry->second.first == 0) {
    // If the reference count reached 0, decrease the reference count for each dependency.
    if (entry->second.second) {
      for (ObjectID &pending_task_object_id : *entry->second.second) {
        RemoveReferenceRecursive(pending_task_object_id);
      }
    }
    object_id_refs_.erase(object_id);
  }
}

std::unordered_set<ObjectID> ReferenceCounter::GetAllInScopeObjectIDs() {
  absl::MutexLock lock(&mutex_);
  std::unordered_set<ObjectID> in_scope_object_ids;
  in_scope_object_ids.reserve(object_id_refs_.size());
  for (auto it : object_id_refs_) {
    in_scope_object_ids.insert(it.first);
  }
  return in_scope_object_ids;
}

void ReferenceCounter::LogDebugString() {
  absl::MutexLock lock(&mutex_);

  RAY_LOG(DEBUG) << "ReferenceCounter state:";
  if (object_id_refs_.empty()) {
    RAY_LOG(DEBUG) << "\tEMPTY";
    return;
  }

  for (auto entry : object_id_refs_) {
    RAY_LOG(DEBUG) << "\t" << entry.first.Hex();
    RAY_LOG(DEBUG) << "\t\treference count: " << entry.second.first;
    RAY_LOG(DEBUG) << "\t\tdependencies: ";
    if (!entry.second.second) {
      RAY_LOG(DEBUG) << "\t\t\tNULL";
    } else {
      for (ObjectID &pending_task_object_id : *entry.second.second) {
        RAY_LOG(DEBUG) << "\t\t\t" << pending_task_object_id.Hex();
      }
    }
  }
}

}  // namespace ray
