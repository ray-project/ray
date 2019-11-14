#include "ray/core_worker/reference_count.h"

namespace ray {

void ReferenceCounter::AddReference(const ObjectID &object_id) {
  absl::MutexLock lock(&mutex_);
  AddReferenceInternal(object_id);
}

void ReferenceCounter::AddReferenceInternal(const ObjectID &object_id) {
  auto entry = object_id_refs_.find(object_id);
  if (entry == object_id_refs_.end()) {
    object_id_refs_[object_id] = std::make_pair(1, nullptr);
  } else {
    entry->second.first++;
  }
}

void ReferenceCounter::SetDependencies(
    const ObjectID &object_id, std::shared_ptr<std::vector<ObjectID>> dependencies) {
  absl::MutexLock lock(&mutex_);

  auto entry = object_id_refs_.find(object_id);
  if (entry == object_id_refs_.end()) {
    // If the entry doesn't exist, we initialize the direct reference count to zero
    // because this corresponds to a submitted task whose return ObjectID will be created
    // in the frontend language, incrementing the reference count.
    object_id_refs_[object_id] = std::make_pair(0, dependencies);
  } else {
    RAY_CHECK(!entry->second.second);
    entry->second.second = dependencies;
  }

  for (const ObjectID &dependency_id : *dependencies) {
    AddReferenceInternal(dependency_id);
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
      for (const ObjectID &pending_task_object_id : *entry->second.second) {
        RemoveReferenceRecursive(pending_task_object_id);
      }
    }
    object_id_refs_.erase(object_id);
  }
}

size_t ReferenceCounter::NumObjectIDsInScope() const {
  absl::MutexLock lock(&mutex_);
  return object_id_refs_.size();
}

std::unordered_set<ObjectID> ReferenceCounter::GetAllInScopeObjectIDs() const {
  absl::MutexLock lock(&mutex_);
  std::unordered_set<ObjectID> in_scope_object_ids;
  in_scope_object_ids.reserve(object_id_refs_.size());
  for (auto it : object_id_refs_) {
    in_scope_object_ids.insert(it.first);
  }
  return in_scope_object_ids;
}

void ReferenceCounter::LogDebugString() const {
  absl::MutexLock lock(&mutex_);

  RAY_LOG(DEBUG) << "ReferenceCounter state:";
  if (object_id_refs_.empty()) {
    RAY_LOG(DEBUG) << "\tEMPTY";
    return;
  }

  for (const auto &entry : object_id_refs_) {
    RAY_LOG(DEBUG) << "\t" << entry.first.Hex();
    RAY_LOG(DEBUG) << "\t\treference count: " << entry.second.first;
    RAY_LOG(DEBUG) << "\t\tdependencies: ";
    if (!entry.second.second) {
      RAY_LOG(DEBUG) << "\t\t\tNULL";
    } else {
      for (const ObjectID &pending_task_object_id : *entry.second.second) {
        RAY_LOG(DEBUG) << "\t\t\t" << pending_task_object_id.Hex();
      }
    }
  }
}

}  // namespace ray
