#include "ray/core_worker/reference_count.h"

namespace ray {

void ReferenceCounter::AddBorrowedObject(const ObjectID &object_id,
                                         const TaskID &owner_id,
                                         const rpc::Address &owner_address) {
  absl::MutexLock lock(&mutex_);
  auto it = object_id_refs_.find(object_id);
  RAY_CHECK(it != object_id_refs_.end());

  if (!it->second.owner.has_value()) {
    it->second.owner = {owner_id, owner_address};
  }
}

void ReferenceCounter::AddOwnedObject(const ObjectID &object_id, const TaskID &owner_id,
                                      const rpc::Address &owner_address) {
  absl::MutexLock lock(&mutex_);
  RAY_CHECK(object_id_refs_.count(object_id) == 0)
      << "Tried to create an owned object that already exists: " << object_id;
  // If the entry doesn't exist, we initialize the direct reference count to zero
  // because this corresponds to a submitted task whose return ObjectID will be created
  // in the frontend language, incrementing the reference count.
  object_id_refs_.emplace(object_id, Reference(owner_id, owner_address));
}

void ReferenceCounter::AddLocalReference(const ObjectID &object_id) {
  absl::MutexLock lock(&mutex_);
  auto entry = object_id_refs_.find(object_id);
  if (entry == object_id_refs_.end()) {
    // TODO: Once ref counting is implemented, we should always know how the
    // ObjectID was created, so there should always be an entry.
    entry = object_id_refs_.emplace(object_id, Reference()).first;
  }
  entry->second.local_ref_count++;
}

void ReferenceCounter::RemoveLocalReference(const ObjectID &object_id,
                                            std::vector<ObjectID> *deleted) {
  absl::MutexLock lock(&mutex_);
  auto entry = object_id_refs_.find(object_id);
  if (entry == object_id_refs_.end()) {
    RAY_LOG(WARNING) << "Tried to decrease ref count for nonexistent object ID: "
                     << object_id;
    return;
  }
  if (--entry->second.local_ref_count == 0 &&
      entry->second.submitted_task_ref_count == 0) {
    object_id_refs_.erase(entry);
    deleted->push_back(object_id);
  }
}

void ReferenceCounter::AddSubmittedTaskReferences(
    const std::vector<ObjectID> &object_ids) {
  absl::MutexLock lock(&mutex_);
  for (const ObjectID &object_id : object_ids) {
    auto entry = object_id_refs_.find(object_id);
    if (entry == object_id_refs_.end()) {
      // TODO: Once ref counting is implemented, we should always know how the
      // ObjectID was created, so there should always be an entry.
      entry = object_id_refs_.emplace(object_id, Reference()).first;
    }
    entry->second.submitted_task_ref_count++;
  }
}

void ReferenceCounter::RemoveSubmittedTaskReferences(
    const std::vector<ObjectID> &object_ids, std::vector<ObjectID> *deleted) {
  absl::MutexLock lock(&mutex_);
  for (const ObjectID &object_id : object_ids) {
    auto entry = object_id_refs_.find(object_id);
    if (entry == object_id_refs_.end()) {
      RAY_LOG(WARNING) << "Tried to decrease ref count for nonexistent object ID: "
                       << object_id;
      return;
    }
    if (--entry->second.submitted_task_ref_count == 0 &&
        entry->second.local_ref_count == 0) {
      object_id_refs_.erase(entry);
      deleted->push_back(object_id);
    }
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

std::unordered_map<ObjectID, std::pair<size_t, size_t>>
ReferenceCounter::GetAllReferenceCounts() const {
  absl::MutexLock lock(&mutex_);
  std::unordered_map<ObjectID, std::pair<size_t, size_t>> all_ref_counts;
  all_ref_counts.reserve(object_id_refs_.size());
  for (auto it : object_id_refs_) {
    all_ref_counts.emplace(it.first,
                           std::pair<size_t, size_t>(it.second.local_ref_count,
                                                     it.second.submitted_task_ref_count));
  }
  return all_ref_counts;
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
    RAY_LOG(DEBUG) << "\t\tlocal refcount: " << entry.second.local_ref_count;
    RAY_LOG(DEBUG) << "\t\tsubmitted task refcount: "
                   << entry.second.submitted_task_ref_count;
  }
}

}  // namespace ray
