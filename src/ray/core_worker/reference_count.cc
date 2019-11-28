#include "ray/core_worker/reference_count.h"

namespace ray {

void ReferenceCounter::AddBorrowedObject(const ObjectID &object_id,
                                         const TaskID &owner_id,
                                         const rpc::Address &owner_address) {
  absl::MutexLock lock(&mutex_);
  RAY_CHECK(
      object_id_refs_.emplace(object_id, Reference(owner_id, owner_address)).second);
}

void ReferenceCounter::AddOwnedObject(
    const ObjectID &object_id, const TaskID &owner_id, const rpc::Address &owner_address,
    std::shared_ptr<std::vector<ObjectID>> dependencies) {
  absl::MutexLock lock(&mutex_);

  for (const ObjectID &dependency_id : *dependencies) {
    AddLocalReferenceInternal(dependency_id);
  }

  RAY_CHECK(object_id_refs_.count(object_id) == 0)
      << "Cannot create an object that already exists. ObjectID: " << object_id;
  // If the entry doesn't exist, we initialize the direct reference count to zero
  // because this corresponds to a submitted task whose return ObjectID will be created
  // in the frontend language, incrementing the reference count.
  object_id_refs_.emplace(object_id, Reference(owner_id, owner_address, dependencies));
}

void ReferenceCounter::AddLocalReferenceInternal(const ObjectID &object_id) {
  auto entry = object_id_refs_.find(object_id);
  if (entry == object_id_refs_.end()) {
    // TODO: Once ref counting is implemented, we should always know how the
    // ObjectID was created, so there should always ben an entry.
    entry = object_id_refs_.emplace(object_id, Reference()).first;
  }
  entry->second.local_ref_count++;
}

void ReferenceCounter::AddLocalReference(const ObjectID &object_id) {
  absl::MutexLock lock(&mutex_);
  AddLocalReferenceInternal(object_id);
}

void ReferenceCounter::RemoveLocalReference(const ObjectID &object_id,
                                            std::vector<ObjectID> *deleted) {
  absl::MutexLock lock(&mutex_);
  RemoveReferenceRecursive(object_id, deleted);
}

void ReferenceCounter::RemoveReferenceRecursive(const ObjectID &object_id,
                                                std::vector<ObjectID> *deleted) {
  auto entry = object_id_refs_.find(object_id);
  if (entry == object_id_refs_.end()) {
    RAY_LOG(WARNING) << "Tried to decrease ref count for nonexistent object ID: "
                     << object_id;
    return;
  }
  if (--entry->second.local_ref_count == 0) {
    // If the reference count reached 0, decrease the reference count for each dependency.
    if (entry->second.dependencies) {
      for (const ObjectID &pending_task_object_id : *entry->second.dependencies) {
        RemoveReferenceRecursive(pending_task_object_id, deleted);
      }
    }
    object_id_refs_.erase(entry);
    deleted->push_back(object_id);
  }
}

bool ReferenceCounter::GetOwner(const ObjectID &object_id, TaskID *owner_id,
                                rpc::Address *owner_address) const {
  absl::MutexLock lock(&mutex_);
  auto it = object_id_refs_.find(object_id);
  if (it == object_id_refs_.end()) {
    return false;
  }

  if (it->second.owner.has_value()) {
    *owner_id = it->second.owner.value().first;
    *owner_address = it->second.owner.value().second;
    return true;
  } else {
    return false;
  }
}

bool ReferenceCounter::HasReference(const ObjectID &object_id) const {
  absl::MutexLock lock(&mutex_);
  return object_id_refs_.find(object_id) != object_id_refs_.end();
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
    RAY_LOG(DEBUG) << "\t\treference count: " << entry.second.local_ref_count;
    RAY_LOG(DEBUG) << "\t\tdependencies: ";
    if (!entry.second.dependencies) {
      RAY_LOG(DEBUG) << "\t\t\tNULL";
    } else {
      for (const ObjectID &pending_task_object_id : *entry.second.dependencies) {
        RAY_LOG(DEBUG) << "\t\t\t" << pending_task_object_id.Hex();
      }
    }
  }
}

}  // namespace ray
