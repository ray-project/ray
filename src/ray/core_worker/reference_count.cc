// Copyright 2017 The Ray Authors.
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

#include "ray/core_worker/reference_count.h"

#define PRINT_REF_COUNT(it)                                                              \
  RAY_LOG(DEBUG) << "REF " << it->first << " borrowers: " << it->second.borrowers.size() \
                 << " local_ref_count: " << it->second.local_ref_count                   \
                 << " submitted_count: " << it->second.submitted_task_ref_count          \
                 << " contained_in_owned: " << it->second.contained_in_owned.size()      \
                 << " contained_in_borrowed: "                                           \
                 << (it->second.contained_in_borrowed_id.has_value()                     \
                         ? *it->second.contained_in_borrowed_id                          \
                         : ObjectID::Nil())                                              \
                 << " contains: " << it->second.contains.size()                          \
                 << " lineage_ref_count: " << it->second.lineage_ref_count;

namespace {}  // namespace

namespace ray {

bool ReferenceCounter::OwnObjects() const {
  absl::MutexLock lock(&mutex_);
  return !object_id_refs_.empty();
}

void ReferenceCounter::DrainAndShutdown(std::function<void()> shutdown) {
  absl::MutexLock lock(&mutex_);
  if (object_id_refs_.empty()) {
    shutdown();
  } else {
    RAY_LOG(WARNING)
        << "This worker is still managing " << object_id_refs_.size()
        << " objects, waiting for them to go out of scope before shutting down.";
    shutdown_hook_ = shutdown;
  }
}

void ReferenceCounter::ShutdownIfNeeded() {
  if (shutdown_hook_ && object_id_refs_.empty()) {
    RAY_LOG(WARNING)
        << "All object references have gone out of scope, shutting down worker.";
    shutdown_hook_();
  }
}

ReferenceCounter::ReferenceTable ReferenceCounter::ReferenceTableFromProto(
    const ReferenceTableProto &proto) {
  ReferenceTable refs;
  for (const auto &ref : proto) {
    refs.emplace(ray::ObjectID::FromBinary(ref.reference().object_id()),
                 Reference::FromProto(ref));
  }
  return refs;
}

void ReferenceCounter::ReferenceTableToProto(const ReferenceTable &table,
                                             ReferenceTableProto *proto) {
  for (const auto &id_ref : table) {
    auto ref = proto->Add();
    id_ref.second.ToProto(ref);
    ref->mutable_reference()->set_object_id(id_ref.first.Binary());
  }
}

bool ReferenceCounter::AddBorrowedObject(const ObjectID &object_id,
                                         const ObjectID &outer_id,
                                         const rpc::Address &owner_address) {
  absl::MutexLock lock(&mutex_);
  return AddBorrowedObjectInternal(object_id, outer_id, owner_address);
}

bool ReferenceCounter::AddBorrowedObjectInternal(const ObjectID &object_id,
                                                 const ObjectID &outer_id,
                                                 const rpc::Address &owner_address) {
  auto it = object_id_refs_.find(object_id);
  RAY_CHECK(it != object_id_refs_.end());

  RAY_LOG(DEBUG) << "Adding borrowed object " << object_id;
  // Skip adding this object as a borrower if we already have ownership info.
  // If we already have ownership info, then either we are the owner or someone
  // else already knows that we are a borrower.
  if (it->second.owner_address) {
    RAY_LOG(DEBUG) << "Skipping add borrowed object " << object_id;
    return false;
  }

  it->second.owner_address = owner_address;

  if (!outer_id.IsNil()) {
    auto outer_it = object_id_refs_.find(outer_id);
    if (outer_it != object_id_refs_.end() && !outer_it->second.owned_by_us) {
      RAY_LOG(DEBUG) << "Setting borrowed inner ID " << object_id
                     << " contained_in_borrowed: " << outer_id;
      RAY_CHECK(!it->second.contained_in_borrowed_id.has_value());
      it->second.contained_in_borrowed_id = outer_id;
      outer_it->second.contains.insert(object_id);
    }
  }
  return true;
}

void ReferenceCounter::AddObjectRefStats(
    const absl::flat_hash_map<ObjectID, std::pair<int64_t, std::string>> pinned_objects,
    rpc::CoreWorkerStats *stats) const {
  absl::MutexLock lock(&mutex_);
  for (const auto &ref : object_id_refs_) {
    auto ref_proto = stats->add_object_refs();
    ref_proto->set_object_id(ref.first.Binary());
    ref_proto->set_call_site(ref.second.call_site);
    ref_proto->set_object_size(ref.second.object_size);
    ref_proto->set_local_ref_count(ref.second.local_ref_count);
    ref_proto->set_submitted_task_ref_count(ref.second.submitted_task_ref_count);
    auto it = pinned_objects.find(ref.first);
    if (it != pinned_objects.end()) {
      ref_proto->set_pinned_in_memory(true);
      // If some info isn't available, fallback to getting it from the pinned info.
      if (ref.second.object_size <= 0) {
        ref_proto->set_object_size(it->second.first);
      }
      if (ref.second.call_site.empty()) {
        ref_proto->set_call_site(it->second.second);
      }
    }
    for (const auto &obj_id : ref.second.contained_in_owned) {
      ref_proto->add_contained_in_owned(obj_id.Binary());
    }
  }
  // Also include any unreferenced objects that are pinned in memory.
  for (const auto &entry : pinned_objects) {
    if (object_id_refs_.find(entry.first) == object_id_refs_.end()) {
      auto ref_proto = stats->add_object_refs();
      ref_proto->set_object_id(entry.first.Binary());
      ref_proto->set_object_size(entry.second.first);
      ref_proto->set_call_site(entry.second.second);
      ref_proto->set_pinned_in_memory(true);
    }
  }
}

void ReferenceCounter::AddOwnedObject(const ObjectID &object_id,
                                      const std::vector<ObjectID> &inner_ids,
                                      const rpc::Address &owner_address,
                                      const std::string &call_site,
                                      const int64_t object_size, bool is_reconstructable,
                                      const absl::optional<NodeID> &pinned_at_raylet_id) {
  RAY_LOG(DEBUG) << "Adding owned object " << object_id;
  absl::MutexLock lock(&mutex_);
  RAY_CHECK(object_id_refs_.count(object_id) == 0)
      << "Tried to create an owned object that already exists: " << object_id;
  // If the entry doesn't exist, we initialize the direct reference count to zero
  // because this corresponds to a submitted task whose return ObjectID will be created
  // in the frontend language, incrementing the reference count.
  auto it = object_id_refs_
                .emplace(object_id, Reference(owner_address, call_site, object_size,
                                              is_reconstructable, pinned_at_raylet_id))
                .first;
  if (!inner_ids.empty()) {
    // Mark that this object ID contains other inner IDs. Then, we will not GC
    // the inner objects until the outer object ID goes out of scope.
    AddNestedObjectIdsInternal(object_id, inner_ids, rpc_address_);
  }
  if (pinned_at_raylet_id.has_value()) {
    // We eagerly add the pinned location to the set of object locations.
    AddObjectLocationInternal(it, pinned_at_raylet_id.value());
  }
}

void ReferenceCounter::RemoveOwnedObject(const ObjectID &object_id) {
  absl::MutexLock lock(&mutex_);
  auto it = object_id_refs_.find(object_id);
  RAY_CHECK(it != object_id_refs_.end())
      << "Tried to remove reference for nonexistent owned object " << object_id
      << ", object must be added with ReferenceCounter::AddOwnedObject() before it "
      << "can be removed";
  RAY_CHECK(it->second.RefCount() == 0)
      << "Tried to remove reference for owned object " << object_id << " that has "
      << it->second.RefCount() << " references, must have 0 references to be removed";
  RAY_LOG(DEBUG) << "Removing owned object " << object_id;
  DeleteReferenceInternal(it, nullptr);
}

void ReferenceCounter::UpdateObjectSize(const ObjectID &object_id, int64_t object_size) {
  absl::MutexLock lock(&mutex_);
  auto it = object_id_refs_.find(object_id);
  if (it != object_id_refs_.end()) {
    it->second.object_size = object_size;
    PushToLocationSubscribers(it);
  }
}

void ReferenceCounter::AddLocalReference(const ObjectID &object_id,
                                         const std::string &call_site) {
  absl::MutexLock lock(&mutex_);
  auto it = object_id_refs_.find(object_id);
  if (it == object_id_refs_.end()) {
    // NOTE: ownership info for these objects must be added later via AddBorrowedObject.
    it = object_id_refs_.emplace(object_id, Reference(call_site, -1)).first;
  }
  it->second.local_ref_count++;
  RAY_LOG(DEBUG) << "Add local reference " << object_id;
  PRINT_REF_COUNT(it);
}

void ReferenceCounter::RemoveLocalReference(const ObjectID &object_id,
                                            std::vector<ObjectID> *deleted) {
  absl::MutexLock lock(&mutex_);
  auto it = object_id_refs_.find(object_id);
  if (it == object_id_refs_.end()) {
    RAY_LOG(WARNING) << "Tried to decrease ref count for nonexistent object ID: "
                     << object_id;
    return;
  }
  if (it->second.local_ref_count == 0) {
    RAY_LOG(WARNING)
        << "Tried to decrease ref count for object ID that has count 0 " << object_id
        << ". This should only happen if ray.internal.free was called earlier.";
    return;
  }
  it->second.local_ref_count--;
  RAY_LOG(DEBUG) << "Remove local reference " << object_id;
  PRINT_REF_COUNT(it);
  if (it->second.RefCount() == 0) {
    DeleteReferenceInternal(it, deleted);
  }
}

void ReferenceCounter::UpdateSubmittedTaskReferences(
    const std::vector<ObjectID> &argument_ids_to_add,
    const std::vector<ObjectID> &argument_ids_to_remove, std::vector<ObjectID> *deleted) {
  absl::MutexLock lock(&mutex_);
  for (const ObjectID &argument_id : argument_ids_to_add) {
    RAY_LOG(DEBUG) << "Increment ref count for submitted task argument " << argument_id;
    auto it = object_id_refs_.find(argument_id);
    if (it == object_id_refs_.end()) {
      // This happens if a large argument is transparently passed by reference
      // because we don't hold a Python reference to its ObjectID.
      it = object_id_refs_.emplace(argument_id, Reference()).first;
    }
    it->second.submitted_task_ref_count++;
    // The lineage ref will get released once the task finishes and cannot be
    // retried again.
    it->second.lineage_ref_count++;
  }
  // Release the submitted task ref and the lineage ref for any argument IDs
  // whose values were inlined.
  RemoveSubmittedTaskReferences(argument_ids_to_remove, /*release_lineage=*/true,
                                deleted);
}

void ReferenceCounter::UpdateResubmittedTaskReferences(
    const std::vector<ObjectID> &argument_ids) {
  absl::MutexLock lock(&mutex_);
  for (const ObjectID &argument_id : argument_ids) {
    auto it = object_id_refs_.find(argument_id);
    RAY_CHECK(it != object_id_refs_.end());
    it->second.submitted_task_ref_count++;
  }
}

void ReferenceCounter::UpdateFinishedTaskReferences(
    const std::vector<ObjectID> &argument_ids, bool release_lineage,
    const rpc::Address &worker_addr, const ReferenceTableProto &borrowed_refs,
    std::vector<ObjectID> *deleted) {
  absl::MutexLock lock(&mutex_);
  // Must merge the borrower refs before decrementing any ref counts. This is
  // to make sure that for serialized IDs, we increment the borrower count for
  // the inner ID before decrementing the submitted_task_ref_count for the
  // outer ID.
  const auto refs = ReferenceTableFromProto(borrowed_refs);
  if (!refs.empty()) {
    RAY_CHECK(!WorkerID::FromBinary(worker_addr.worker_id()).IsNil());
  }
  for (const ObjectID &argument_id : argument_ids) {
    MergeRemoteBorrowers(argument_id, worker_addr, refs);
  }

  RemoveSubmittedTaskReferences(argument_ids, release_lineage, deleted);
}

void ReferenceCounter::ReleaseLineageReferences(
    const std::vector<ObjectID> &argument_ids) {
  absl::MutexLock lock(&mutex_);
  ReleaseLineageReferencesInternal(argument_ids);
}

void ReferenceCounter::ReleaseLineageReferencesInternal(
    const std::vector<ObjectID> &argument_ids) {
  for (const ObjectID &argument_id : argument_ids) {
    auto it = object_id_refs_.find(argument_id);
    if (it == object_id_refs_.end()) {
      // References can get evicted early when lineage pinning is disabled.
      RAY_CHECK(!lineage_pinning_enabled_);
      continue;
    }

    if (it->second.lineage_ref_count == 0) {
      // References can get evicted early when lineage pinning is disabled.
      RAY_CHECK(!lineage_pinning_enabled_);
      continue;
    }

    RAY_LOG(DEBUG) << "Releasing lineage internal for argument " << argument_id;
    it->second.lineage_ref_count--;
    if (it->second.lineage_ref_count == 0) {
      // Don't have to pass in a deleted vector here because the reference
      // cannot have gone out of scope here since we are only modifying the
      // lineage ref count.
      DeleteReferenceInternal(it, nullptr);
    }
  }
}

void ReferenceCounter::RemoveSubmittedTaskReferences(
    const std::vector<ObjectID> &argument_ids, bool release_lineage,
    std::vector<ObjectID> *deleted) {
  for (const ObjectID &argument_id : argument_ids) {
    RAY_LOG(DEBUG) << "Releasing ref for submitted task argument " << argument_id;
    auto it = object_id_refs_.find(argument_id);
    if (it == object_id_refs_.end()) {
      RAY_LOG(WARNING) << "Tried to decrease ref count for nonexistent object ID: "
                       << argument_id;
      return;
    }
    RAY_CHECK(it->second.submitted_task_ref_count > 0);
    it->second.submitted_task_ref_count--;
    if (release_lineage) {
      if (it->second.lineage_ref_count > 0) {
        it->second.lineage_ref_count--;
      } else {
        // References can get evicted early when lineage pinning is disabled.
        RAY_CHECK(!lineage_pinning_enabled_);
      }
    }
    if (it->second.RefCount() == 0) {
      DeleteReferenceInternal(it, deleted);
    }
  }
}

bool ReferenceCounter::GetOwner(const ObjectID &object_id,
                                rpc::Address *owner_address) const {
  absl::MutexLock lock(&mutex_);
  return GetOwnerInternal(object_id, owner_address);
}

bool ReferenceCounter::GetOwnerInternal(const ObjectID &object_id,
                                        rpc::Address *owner_address) const {
  auto it = object_id_refs_.find(object_id);
  if (it == object_id_refs_.end()) {
    return false;
  }

  if (it->second.owner_address) {
    *owner_address = *it->second.owner_address;
    return true;
  } else {
    return false;
  }
}

std::vector<rpc::Address> ReferenceCounter::GetOwnerAddresses(
    const std::vector<ObjectID> object_ids) const {
  absl::MutexLock lock(&mutex_);
  std::vector<rpc::Address> owner_addresses;
  for (const auto &object_id : object_ids) {
    rpc::Address owner_addr;
    bool has_owner = GetOwnerInternal(object_id, &owner_addr);
    if (!has_owner) {
      RAY_LOG(WARNING)
          << " Object IDs generated randomly (ObjectID.from_random()) or out-of-band "
             "(ObjectID.from_binary(...)) cannot be passed to ray.get(), ray.wait(), or "
             "as "
             "a task argument because Ray does not know which task will create them. "
             "If this was not how your object ID was generated, please file an issue "
             "at https://github.com/ray-project/ray/issues/";
      // TODO(swang): Java does not seem to keep the ref count properly, so the
      // entry may get deleted.
      owner_addresses.push_back(rpc::Address());
    } else {
      owner_addresses.push_back(owner_addr);
    }
  }
  return owner_addresses;
}

bool ReferenceCounter::IsPlasmaObjectFreed(const ObjectID &object_id) const {
  absl::MutexLock lock(&mutex_);
  return freed_objects_.find(object_id) != freed_objects_.end();
}

void ReferenceCounter::FreePlasmaObjects(const std::vector<ObjectID> &object_ids) {
  absl::MutexLock lock(&mutex_);
  for (const ObjectID &object_id : object_ids) {
    auto it = object_id_refs_.find(object_id);
    if (it == object_id_refs_.end()) {
      RAY_LOG(WARNING) << "Tried to free an object " << object_id
                       << " that is already out of scope";
      continue;
    }
    // The object is still in scope. It will be removed from this set
    // once its Reference has been deleted.
    freed_objects_.insert(object_id);
    if (!it->second.owned_by_us) {
      RAY_LOG(WARNING)
          << "Tried to free an object " << object_id
          << " that we did not create. The object value may not be released.";
      continue;
    }
    // Free only the plasma value. We must keep the reference around so that we
    // have the ownership information.
    ReleasePlasmaObject(it);
  }
}

void ReferenceCounter::DeleteReferenceInternal(ReferenceTable::iterator it,
                                               std::vector<ObjectID> *deleted) {
  const ObjectID id = it->first;
  RAY_LOG(DEBUG) << "Attempting to delete object " << id;
  if (it->second.RefCount() == 0 && it->second.on_ref_removed) {
    RAY_LOG(DEBUG) << "Calling on_ref_removed for object " << id;
    it->second.on_ref_removed(id);
    it->second.on_ref_removed = nullptr;
  }
  PRINT_REF_COUNT(it);

  // Whether it is safe to unpin the value.
  bool should_delete_value = false;

  // If distributed ref counting is not enabled, then delete the object as soon
  // as its local ref count goes to 0.
  size_t local_ref_count =
      it->second.local_ref_count + it->second.submitted_task_ref_count;
  if (!distributed_ref_counting_enabled_ && local_ref_count == 0) {
    should_delete_value = true;
  }

  if (it->second.OutOfScope(lineage_pinning_enabled_)) {
    // If distributed ref counting is enabled, then delete the object once its
    // ref count across all processes is 0.
    should_delete_value = true;
    for (const auto &inner_id : it->second.contains) {
      auto inner_it = object_id_refs_.find(inner_id);
      if (inner_it != object_id_refs_.end()) {
        RAY_LOG(DEBUG) << "Try to delete inner object " << inner_id;
        if (it->second.owned_by_us) {
          // If this object ID was nested in an owned object, make sure that
          // the outer object counted towards the ref count for the inner
          // object.
          RAY_CHECK(inner_it->second.contained_in_owned.erase(id));
        } else {
          // If this object ID was nested in a borrowed object, make sure that
          // we have already returned this information through a previous
          // GetAndClearLocalBorrowers call.
          RAY_CHECK(!inner_it->second.contained_in_borrowed_id.has_value())
              << "Outer object " << id << ", inner object " << inner_id;
        }
        DeleteReferenceInternal(inner_it, deleted);
      }
    }
  }

  // Perform the deletion.
  if (should_delete_value) {
    ReleasePlasmaObject(it);
    if (deleted) {
      deleted->push_back(id);
    }
  }
  if (it->second.ShouldDelete(lineage_pinning_enabled_)) {
    RAY_LOG(DEBUG) << "Deleting Reference to object " << id;
    // TODO(swang): Update lineage_ref_count for nested objects?
    if (on_lineage_released_ && it->second.owned_by_us) {
      RAY_LOG(DEBUG) << "Releasing lineage for object " << id;
      std::vector<ObjectID> ids_to_release;
      on_lineage_released_(id, &ids_to_release);
      ReleaseLineageReferencesInternal(ids_to_release);
    }

    freed_objects_.erase(id);
    object_id_refs_.erase(it);
    ShutdownIfNeeded();
  }
}

void ReferenceCounter::ReleasePlasmaObject(ReferenceTable::iterator it) {
  if (it->second.on_delete) {
    RAY_LOG(DEBUG) << "Calling on_delete for object " << it->first;
    it->second.on_delete(it->first);
    it->second.on_delete = nullptr;
  }
  it->second.pinned_at_raylet_id.reset();
}

bool ReferenceCounter::SetDeleteCallback(
    const ObjectID &object_id, const std::function<void(const ObjectID &)> callback) {
  absl::MutexLock lock(&mutex_);
  auto it = object_id_refs_.find(object_id);
  if (it == object_id_refs_.end()) {
    return false;
  } else if (it->second.OutOfScope(lineage_pinning_enabled_) &&
             !it->second.ShouldDelete(lineage_pinning_enabled_)) {
    // The object has already gone out of scope but cannot be deleted yet. Do
    // not set the deletion callback because it may never get called.
    return false;
  } else if (freed_objects_.count(object_id) > 0) {
    // The object has been freed by the language frontend, so it
    // should be deleted immediately.
    return false;
  } else if (it->second.spilled) {
    // The object has been spilled, so it can be released immediately.
    return false;
  }

  // NOTE: In two cases, `GcsActorManager` will send `WaitForActorOutOfScope` request more
  // than once, causing the delete callback to be set repeatedly.
  // 1.If actors have not been registered successfully before GCS restarts, gcs client
  // will resend the registration request after GCS restarts.
  // 2.After GCS restarts, GCS will send `WaitForActorOutOfScope` request to owned actors
  // again.
  it->second.on_delete = callback;
  return true;
}

std::vector<ObjectID> ReferenceCounter::ResetObjectsOnRemovedNode(
    const NodeID &raylet_id) {
  absl::MutexLock lock(&mutex_);
  std::vector<ObjectID> lost_objects;
  for (auto it = object_id_refs_.begin(); it != object_id_refs_.end(); it++) {
    const auto &object_id = it->first;
    if (it->second.pinned_at_raylet_id.value_or(NodeID::Nil()) == raylet_id) {
      lost_objects.push_back(object_id);
      ReleasePlasmaObject(it);
    }
  }
  return lost_objects;
}

void ReferenceCounter::UpdateObjectPinnedAtRaylet(const ObjectID &object_id,
                                                  const NodeID &raylet_id) {
  absl::MutexLock lock(&mutex_);
  auto it = object_id_refs_.find(object_id);
  if (it != object_id_refs_.end()) {
    if (freed_objects_.count(object_id) > 0) {
      // The object has been freed by the language frontend.
      return;
    }

    // The object is still in scope. Track the raylet location until the object
    // has gone out of scope or the raylet fails, whichever happens first.
    RAY_CHECK(!it->second.pinned_at_raylet_id.has_value());
    // Only the owner tracks the location.
    RAY_CHECK(it->second.owned_by_us);
    if (!it->second.OutOfScope(lineage_pinning_enabled_)) {
      it->second.pinned_at_raylet_id = raylet_id;
      // We eagerly add the pinned location to the set of object locations.
      AddObjectLocationInternal(it, raylet_id);
    }
  }
}

bool ReferenceCounter::IsPlasmaObjectPinnedOrSpilled(const ObjectID &object_id,
                                                     bool *owned_by_us, NodeID *pinned_at,
                                                     bool *spilled) const {
  absl::MutexLock lock(&mutex_);
  auto it = object_id_refs_.find(object_id);
  if (it != object_id_refs_.end()) {
    if (it->second.owned_by_us) {
      *owned_by_us = true;
      *spilled = it->second.spilled;
      *pinned_at = it->second.pinned_at_raylet_id.value_or(NodeID::Nil());
    }
    return true;
  }
  return false;
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

void ReferenceCounter::GetAndClearLocalBorrowers(
    const std::vector<ObjectID> &borrowed_ids,
    ReferenceCounter::ReferenceTableProto *proto) {
  absl::MutexLock lock(&mutex_);
  ReferenceTable borrowed_refs;
  for (const auto &borrowed_id : borrowed_ids) {
    RAY_CHECK(GetAndClearLocalBorrowersInternal(borrowed_id, &borrowed_refs))
        << borrowed_id;
    // Decrease the ref count for each of the borrowed IDs. This is because we
    // artificially increment each borrowed ID to keep it pinned during task
    // execution. However, this should not count towards the final ref count
    // returned to the task's caller.
    auto it = borrowed_refs.find(borrowed_id);
    if (it != borrowed_refs.end()) {
      it->second.local_ref_count--;
    }
  }
  ReferenceTableToProto(borrowed_refs, proto);
}

bool ReferenceCounter::GetAndClearLocalBorrowersInternal(const ObjectID &object_id,
                                                         ReferenceTable *borrowed_refs) {
  RAY_LOG(DEBUG) << "Pop " << object_id;
  auto it = object_id_refs_.find(object_id);
  if (it == object_id_refs_.end()) {
    return false;
  }

  // We only borrow objects that we do not own. This is not an assertion
  // because it is possible to receive a reference to an object that we already
  // own, e.g., if we execute a task that has an object ID in its arguments
  // that we created in an earlier task.
  if (it->second.owned_by_us) {
    // Return true because we have the ref, but there is no need to return it
    // since we own the object.
    return true;
  }

  borrowed_refs->emplace(object_id, it->second);
  // Clear the local list of borrowers that we have accumulated. The receiver
  // of the returned borrowed_refs must merge this list into their own list
  // until all active borrowers are merged into the owner.
  it->second.borrowers.clear();
  it->second.stored_in_objects.clear();

  if (it->second.contained_in_borrowed_id.has_value()) {
    /// This ID was nested in another ID that we (or a nested task) borrowed.
    /// Make sure that we also returned the ID that contained it.
    RAY_CHECK(borrowed_refs->count(it->second.contained_in_borrowed_id.value()) > 0);
    /// Clear the fact that this ID was nested because we are including it in
    /// the returned borrowed_refs. If the nested ID is not being borrowed by
    /// us, then it will be deleted recursively when deleting the outer ID.
    it->second.contained_in_borrowed_id.reset();
  }

  // Attempt to pop children.
  for (const auto &contained_id : it->second.contains) {
    GetAndClearLocalBorrowersInternal(contained_id, borrowed_refs);
  }

  return true;
}

void ReferenceCounter::MergeRemoteBorrowers(const ObjectID &object_id,
                                            const rpc::WorkerAddress &worker_addr,
                                            const ReferenceTable &borrowed_refs) {
  RAY_LOG(DEBUG) << "Merging ref " << object_id;
  auto borrower_it = borrowed_refs.find(object_id);
  if (borrower_it == borrowed_refs.end()) {
    return;
  }
  const auto &borrower_ref = borrower_it->second;
  RAY_LOG(DEBUG) << "Borrower ref " << object_id << " has "
                 << borrower_ref.borrowers.size() << " borrowers "
                 << ", has local: " << borrower_ref.local_ref_count
                 << " submitted: " << borrower_ref.submitted_task_ref_count
                 << " contained_in_owned " << borrower_ref.contained_in_owned.size();

  auto it = object_id_refs_.find(object_id);
  if (it == object_id_refs_.end()) {
    it = object_id_refs_.emplace(object_id, Reference()).first;
  }
  if (!it->second.owner_address && borrower_ref.contained_in_borrowed_id.has_value()) {
    // We don't have owner information about this object ID yet and the worker
    // received it because it was nested in another ID that the worker was
    // borrowing. Copy this information to our local table.
    RAY_CHECK(borrower_ref.owner_address);
    AddBorrowedObjectInternal(object_id, *borrower_it->second.contained_in_borrowed_id,
                              *borrower_ref.owner_address);
  }
  std::vector<rpc::WorkerAddress> new_borrowers;

  // The worker is still using the reference, so it is still a borrower.
  if (borrower_ref.RefCount() > 0) {
    auto inserted = it->second.borrowers.insert(worker_addr).second;
    // If we are the owner of id, then send WaitForRefRemoved to borrower.
    if (inserted) {
      RAY_LOG(DEBUG) << "Adding borrower " << worker_addr.ip_address << ":"
                     << worker_addr.port << " to id " << object_id;
      new_borrowers.push_back(worker_addr);
    }
  }

  // Add any other workers that this worker passed the ID to as new borrowers.
  for (const auto &nested_borrower : borrower_ref.borrowers) {
    auto inserted = it->second.borrowers.insert(nested_borrower).second;
    if (inserted) {
      RAY_LOG(DEBUG) << "Adding borrower " << nested_borrower.ip_address << ":"
                     << nested_borrower.port << " to id " << object_id;
      new_borrowers.push_back(nested_borrower);
    }
  }

  // If we own this ID, then wait for all new borrowers to reach a ref count
  // of 0 before GCing the object value.
  if (it->second.owned_by_us) {
    for (const auto &addr : new_borrowers) {
      WaitForRefRemoved(it, addr);
    }
  }

  // If the borrower stored this object ID inside another object ID that it did
  // not own, then mark that the object ID is nested inside another.
  for (const auto &stored_in_object : borrower_ref.stored_in_objects) {
    AddNestedObjectIdsInternal(stored_in_object.first, {object_id},
                               stored_in_object.second);
  }

  // Recursively merge any references that were contained in this object, to
  // handle any borrowers of nested objects.
  for (const auto &inner_id : borrower_ref.contains) {
    MergeRemoteBorrowers(inner_id, worker_addr, borrowed_refs);
  }
}

void ReferenceCounter::WaitForRefRemoved(const ReferenceTable::iterator &ref_it,
                                         const rpc::WorkerAddress &addr,
                                         const ObjectID &contained_in_id) {
  const ObjectID &object_id = ref_it->first;
  rpc::WaitForRefRemovedRequest request;
  // Only the owner should send requests to borrowers.
  RAY_CHECK(ref_it->second.owned_by_us);
  request.mutable_reference()->set_object_id(object_id.Binary());
  request.mutable_reference()->mutable_owner_address()->CopyFrom(
      *ref_it->second.owner_address);
  request.set_contained_in_id(contained_in_id.Binary());
  request.set_intended_worker_id(addr.worker_id.Binary());

  auto conn = borrower_pool_.GetOrConnect(addr.ToProto());

  RAY_LOG(DEBUG) << "Sending WaitForRefRemoved to borrower " << addr.ip_address << ":"
                 << addr.port << " for object " << object_id;
  // Send the borrower a message about this object. The borrower responds once
  // it is no longer using the object ID.
  conn->WaitForRefRemoved(
      request, [this, object_id, addr](const Status &status,
                                       const rpc::WaitForRefRemovedReply &reply) {
        RAY_LOG(DEBUG) << "Received reply from borrower " << addr.ip_address << ":"
                       << addr.port << " of object " << object_id;
        absl::MutexLock lock(&mutex_);

        // Merge in any new borrowers that the previous borrower learned of.
        const ReferenceTable new_borrower_refs =
            ReferenceTableFromProto(reply.borrowed_refs());
        MergeRemoteBorrowers(object_id, addr, new_borrower_refs);

        // Erase the previous borrower.
        auto it = object_id_refs_.find(object_id);
        RAY_CHECK(it != object_id_refs_.end());
        RAY_CHECK(it->second.borrowers.erase(addr));
        DeleteReferenceInternal(it, nullptr);
      });
}

void ReferenceCounter::AddNestedObjectIds(const ObjectID &object_id,
                                          const std::vector<ObjectID> &inner_ids,
                                          const rpc::WorkerAddress &owner_address) {
  absl::MutexLock lock(&mutex_);
  AddNestedObjectIdsInternal(object_id, inner_ids, owner_address);
}

void ReferenceCounter::AddNestedObjectIdsInternal(
    const ObjectID &object_id, const std::vector<ObjectID> &inner_ids,
    const rpc::WorkerAddress &owner_address) {
  RAY_CHECK(!owner_address.worker_id.IsNil());
  auto it = object_id_refs_.find(object_id);
  if (owner_address.worker_id == rpc_address_.worker_id) {
    // We own object_id. This is a `ray.put()` case OR returning an object ID
    // from a task and the task's caller executed in the same process as us.
    if (it != object_id_refs_.end()) {
      RAY_CHECK(it->second.owned_by_us);
      // The outer object is still in scope. Mark the inner ones as being
      // contained in the outer object ID so we do not GC the inner objects
      // until the outer object goes out of scope.
      for (const auto &inner_id : inner_ids) {
        it->second.contains.insert(inner_id);
        auto inner_it = object_id_refs_.find(inner_id);
        RAY_CHECK(inner_it != object_id_refs_.end());
        RAY_LOG(DEBUG) << "Setting inner ID " << inner_id
                       << " contained_in_owned: " << object_id;
        inner_it->second.contained_in_owned.insert(object_id);
      }
    }
  } else {
    // We do not own object_id. This is the case where we returned an object ID
    // from a task, and the task's caller executed in a remote process.
    for (const auto &inner_id : inner_ids) {
      RAY_LOG(DEBUG) << "Adding borrower " << owner_address.ip_address << ":"
                     << owner_address.port << " to id " << inner_id
                     << ", borrower owns outer ID " << object_id;
      auto inner_it = object_id_refs_.find(inner_id);
      RAY_CHECK(inner_it != object_id_refs_.end());
      // Add the task's caller as a borrower.
      if (inner_it->second.owned_by_us) {
        auto inserted = inner_it->second.borrowers.insert(owner_address).second;
        if (inserted) {
          // Wait for it to remove its reference.
          WaitForRefRemoved(inner_it, owner_address, object_id);
        }
      } else {
        auto inserted =
            inner_it->second.stored_in_objects.emplace(object_id, owner_address).second;
        // This should be the first time that we have stored this object ID
        // inside this return ID.
        RAY_CHECK(inserted);
      }
    }
  }
}

void ReferenceCounter::HandleRefRemoved(const ObjectID &object_id,
                                        rpc::WaitForRefRemovedReply *reply,
                                        rpc::SendReplyCallback send_reply_callback) {
  ReferenceTable borrowed_refs;
  RAY_UNUSED(GetAndClearLocalBorrowersInternal(object_id, &borrowed_refs));
  for (const auto &pair : borrowed_refs) {
    RAY_LOG(DEBUG) << pair.first << " has " << pair.second.borrowers.size()
                   << " borrowers";
  }
  auto it = object_id_refs_.find(object_id);
  if (it != object_id_refs_.end()) {
    // We should only have called this callback once our local ref count for
    // the object was zero. Also, we should have stripped all distributed ref
    // count information and returned it to the owner. Therefore, it should be
    // okay to delete the object, if it wasn't already deleted.
    RAY_CHECK(it->second.OutOfScope(lineage_pinning_enabled_));
  }
  // Send the owner information about any new borrowers.
  ReferenceTableToProto(borrowed_refs, reply->mutable_borrowed_refs());

  RAY_LOG(DEBUG) << "Replying to WaitForRefRemoved, reply has "
                 << reply->borrowed_refs().size();
  send_reply_callback(Status::OK(), nullptr, nullptr);
}

void ReferenceCounter::SetRefRemovedCallback(
    const ObjectID &object_id, const ObjectID &contained_in_id,
    const rpc::Address &owner_address,
    const ReferenceCounter::ReferenceRemovedCallback &ref_removed_callback) {
  absl::MutexLock lock(&mutex_);
  RAY_LOG(DEBUG) << "Received WaitForRefRemoved " << object_id << " contained in "
                 << contained_in_id;

  auto it = object_id_refs_.find(object_id);
  if (it == object_id_refs_.end()) {
    it = object_id_refs_.emplace(object_id, Reference()).first;
  }

  // If we are borrowing the ID because we own an object that contains it, then
  // add the outer object to the inner ID's ref count. We will not respond to
  // the owner of the inner ID until the outer object ID goes out of scope.
  if (!contained_in_id.IsNil()) {
    AddNestedObjectIdsInternal(contained_in_id, {object_id}, rpc_address_);
  }

  if (it->second.RefCount() == 0) {
    RAY_LOG(DEBUG) << "Ref count for borrowed object " << object_id
                   << " is already 0, responding to WaitForRefRemoved";
    // We already stopped borrowing the object ID. Respond to the owner
    // immediately.
    ref_removed_callback(object_id);
    DeleteReferenceInternal(it, nullptr);
  } else {
    // We are still borrowing the object ID. Respond to the owner once we have
    // stopped borrowing it.
    if (it->second.on_ref_removed != nullptr) {
      // TODO(swang): If the owner of an object dies and and is re-executed, it
      // is possible that we will receive a duplicate request to set
      // on_ref_removed. If messages are delayed and we overwrite the
      // callback here, it's possible we will drop the request that was sent by
      // the more recent owner. We should fix this by setting multiple
      // callbacks or by versioning the owner requests.
      RAY_LOG(WARNING) << "on_ref_removed already set for " << object_id
                       << ". The owner task must have died and been re-executed.";
    }
    it->second.on_ref_removed = ref_removed_callback;
  }
}

void ReferenceCounter::SetReleaseLineageCallback(
    const LineageReleasedCallback &callback) {
  RAY_CHECK(on_lineage_released_ == nullptr);
  on_lineage_released_ = callback;
}

bool ReferenceCounter::AddObjectLocation(const ObjectID &object_id,
                                         const NodeID &node_id) {
  absl::MutexLock lock(&mutex_);
  auto it = object_id_refs_.find(object_id);
  if (it == object_id_refs_.end()) {
    RAY_LOG(INFO) << "Tried to add an object location for an object " << object_id
                  << " that doesn't exist in the reference table";
    return false;
  }
  AddObjectLocationInternal(it, node_id);
  return true;
}

void ReferenceCounter::AddObjectLocationInternal(ReferenceTable::iterator it,
                                                 const NodeID &node_id) {
  if (it->second.locations.emplace(node_id).second) {
    // Only push to subscribers if we added a new location. We eagerly add the pinned
    // location without waiting for the object store notification to trigger a location
    // report, so there's a chance that we already knew about the node_id location.
    PushToLocationSubscribers(it);
  }
}

bool ReferenceCounter::RemoveObjectLocation(const ObjectID &object_id,
                                            const NodeID &node_id) {
  absl::MutexLock lock(&mutex_);
  auto it = object_id_refs_.find(object_id);
  if (it == object_id_refs_.end()) {
    RAY_LOG(INFO) << "Tried to remove an object location for an object " << object_id
                  << " that doesn't exist in the reference table";
    return false;
  }
  it->second.locations.erase(node_id);
  PushToLocationSubscribers(it);
  return true;
}

absl::optional<absl::flat_hash_set<NodeID>> ReferenceCounter::GetObjectLocations(
    const ObjectID &object_id) {
  absl::MutexLock lock(&mutex_);
  auto it = object_id_refs_.find(object_id);
  if (it == object_id_refs_.end()) {
    RAY_LOG(WARNING) << "Tried to get the object locations for an object " << object_id
                     << " that doesn't exist in the reference table";
    return absl::nullopt;
  }
  return it->second.locations;
}

size_t ReferenceCounter::GetObjectSize(const ObjectID &object_id) const {
  absl::MutexLock lock(&mutex_);
  auto it = object_id_refs_.find(object_id);
  if (it == object_id_refs_.end()) {
    return 0;
  }
  return it->second.object_size;
}

bool ReferenceCounter::HandleObjectSpilled(const ObjectID &object_id,
                                           const std::string spilled_url,
                                           const NodeID &spilled_node_id, int64_t size,
                                           bool release) {
  absl::MutexLock lock(&mutex_);
  auto it = object_id_refs_.find(object_id);
  if (it == object_id_refs_.end()) {
    RAY_LOG(WARNING) << "Spilled object " << object_id << " already out of scope";
    return false;
  }

  it->second.spilled = true;
  if (spilled_url != "") {
    it->second.spilled_url = spilled_url;
  }
  if (!spilled_node_id.IsNil()) {
    it->second.spilled_node_id = spilled_node_id;
  }
  if (size > 0) {
    it->second.object_size = size;
  }
  PushToLocationSubscribers(it);
  if (release) {
    // Release the primary plasma copy, if any.
    ReleasePlasmaObject(it);
  }
  return true;
}

absl::optional<LocalityData> ReferenceCounter::GetLocalityData(
    const ObjectID &object_id) {
  absl::MutexLock lock(&mutex_);
  // Uses the reference table to return locality data for an object.
  auto it = object_id_refs_.find(object_id);
  if (it == object_id_refs_.end()) {
    // We don't have any information about this object so we can't return valid locality
    // data.
    RAY_LOG(DEBUG) << "Object " << object_id
                   << " not in reference table, locality data not available";
    return absl::nullopt;
  }

  // The size of this object.
  const auto object_size = it->second.object_size;
  if (object_size < 0) {
    // We don't know the object size so we can't returned valid locality data.
    RAY_LOG(DEBUG) << "Reference " << it->second.call_site << " for object " << object_id
                   << " has an unknown object size, locality data not available";
    return absl::nullopt;
  }

  // The locations of this object.
  // - If we own this object and the ownership-based object directory is enabled, this
  // will contain the complete up-to-date set of object locations.
  // - If we own this object and the ownership-based object directory is disabled, this
  // will only contain the pinned location, if known.
  // - If we don't own this object, this will be empty.
  const auto &node_ids = it->second.locations;

  // We should only reach here if we have valid locality data to return.
  absl::optional<LocalityData> locality_data(
      {static_cast<uint64_t>(object_size), node_ids});
  return locality_data;
}

bool ReferenceCounter::ReportLocalityData(const ObjectID &object_id,
                                          const absl::flat_hash_set<NodeID> &locations,
                                          uint64_t object_size) {
  absl::MutexLock lock(&mutex_);
  auto it = object_id_refs_.find(object_id);
  if (it == object_id_refs_.end()) {
    RAY_LOG(INFO) << "Tried to report locality data for an object " << object_id
                  << " that doesn't exist in the reference table."
                  << " The object has probably already been freed.";
    return false;
  }
  RAY_CHECK(!it->second.owned_by_us)
      << "ReportLocalityData should only be used for borrowed references.";
  for (const auto &location : locations) {
    it->second.locations.emplace(location);
  }
  if (object_size > 0) {
    it->second.object_size = object_size;
  }
  return true;
}

void ReferenceCounter::PushToLocationSubscribers(ReferenceTable::iterator it) {
  const auto callbacks = it->second.location_subscription_callbacks;
  it->second.location_subscription_callbacks.clear();
  it->second.location_version++;
  for (const auto &callback : callbacks) {
    callback(it->second.locations, it->second.object_size, it->second.spilled_url,
             it->second.spilled_node_id, it->second.location_version);
  }
}

Status ReferenceCounter::SubscribeObjectLocations(
    const ObjectID &object_id, int64_t last_location_version,
    const LocationSubscriptionCallback &callback) {
  absl::MutexLock lock(&mutex_);
  auto it = object_id_refs_.find(object_id);
  if (it == object_id_refs_.end()) {
    RAY_LOG(INFO) << "Tried to register a location subscriber for an object " << object_id
                  << " that doesn't exist in the reference table."
                  << " The object has probably already been freed.";
    return Status::ObjectNotFound("Object " + object_id.Hex() + " not found");
  }

  if (last_location_version < it->second.location_version) {
    // If the last location version is less than the current location version, we
    // already have location data that the subscriber hasn't seen yet, so we immediately
    // invoke the callback.
    callback(it->second.locations, it->second.object_size, it->second.spilled_url,
             it->second.spilled_node_id, it->second.location_version);
  } else {
    // Otherwise, save the callback for later invocation.
    it->second.location_subscription_callbacks.push_back(callback);
  }
  return Status::OK();
}

ReferenceCounter::Reference ReferenceCounter::Reference::FromProto(
    const rpc::ObjectReferenceCount &ref_count) {
  Reference ref;
  ref.owner_address = ref_count.reference().owner_address();
  ref.local_ref_count = ref_count.has_local_ref() ? 1 : 0;

  for (const auto &borrower : ref_count.borrowers()) {
    ref.borrowers.insert(rpc::WorkerAddress(borrower));
  }
  for (const auto &object : ref_count.stored_in_objects()) {
    const auto &object_id = ObjectID::FromBinary(object.object_id());
    ref.stored_in_objects.emplace(object_id, rpc::WorkerAddress(object.owner_address()));
  }
  for (const auto &id : ref_count.contains()) {
    ref.contains.insert(ObjectID::FromBinary(id));
  }
  const auto contained_in_borrowed_id =
      ObjectID::FromBinary(ref_count.contained_in_borrowed_id());
  if (!contained_in_borrowed_id.IsNil()) {
    ref.contained_in_borrowed_id = contained_in_borrowed_id;
  }
  return ref;
}

void ReferenceCounter::Reference::ToProto(rpc::ObjectReferenceCount *ref) const {
  if (owner_address) {
    ref->mutable_reference()->mutable_owner_address()->CopyFrom(*owner_address);
  }
  bool has_local_ref = RefCount() > 0;
  ref->set_has_local_ref(has_local_ref);
  for (const auto &borrower : borrowers) {
    ref->add_borrowers()->CopyFrom(borrower.ToProto());
  }
  for (const auto &object : stored_in_objects) {
    auto ref_object = ref->add_stored_in_objects();
    ref_object->set_object_id(object.first.Binary());
    ref_object->mutable_owner_address()->CopyFrom(object.second.ToProto());
  }
  if (contained_in_borrowed_id.has_value()) {
    ref->set_contained_in_borrowed_id(contained_in_borrowed_id->Binary());
  }
  for (const auto &contains_id : contains) {
    ref->add_contains(contains_id.Binary());
  }
}

}  // namespace ray
