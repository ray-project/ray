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
                 << " contains: " << it->second.contains.size();

namespace {}  // namespace

namespace ray {

ReferenceCounter::ReferenceTable ReferenceCounter::ReferenceTableFromProto(
    const ReferenceTableProto &proto) {
  ReferenceTable refs;
  for (const auto &ref : proto) {
    refs[ray::ObjectID::FromBinary(ref.reference().object_id())] =
        Reference::FromProto(ref);
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
                                         const ObjectID &outer_id, const TaskID &owner_id,
                                         const rpc::Address &owner_address) {
  absl::MutexLock lock(&mutex_);
  return AddBorrowedObjectInternal(object_id, outer_id, owner_id, owner_address);
}

bool ReferenceCounter::AddBorrowedObjectInternal(const ObjectID &object_id,
                                                 const ObjectID &outer_id,
                                                 const TaskID &owner_id,
                                                 const rpc::Address &owner_address) {
  auto it = object_id_refs_.find(object_id);
  RAY_CHECK(it != object_id_refs_.end());

  RAY_LOG(DEBUG) << "Adding borrowed object " << object_id;
  // Skip adding this object as a borrower if we already have ownership info.
  // If we already have ownership info, then either we are the owner or someone
  // else already knows that we are a borrower.
  if (it->second.owner.has_value()) {
    RAY_LOG(DEBUG) << "Skipping add borrowed object " << object_id;
    return false;
  }

  it->second.owner = {owner_id, owner_address};

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

void ReferenceCounter::AddOwnedObject(const ObjectID &object_id,
                                      const std::vector<ObjectID> &inner_ids,
                                      const TaskID &owner_id,
                                      const rpc::Address &owner_address) {
  RAY_LOG(DEBUG) << "Adding owned object " << object_id;
  absl::MutexLock lock(&mutex_);
  RAY_CHECK(object_id_refs_.count(object_id) == 0)
      << "Tried to create an owned object that already exists: " << object_id;
  // If the entry doesn't exist, we initialize the direct reference count to zero
  // because this corresponds to a submitted task whose return ObjectID will be created
  // in the frontend language, incrementing the reference count.
  object_id_refs_.emplace(object_id, Reference(owner_id, owner_address));
  if (!inner_ids.empty()) {
    // Mark that this object ID contains other inner IDs. Then, we will not GC
    // the inner objects until the outer object ID goes out of scope.
    AddNestedObjectIdsInternal(object_id, inner_ids, rpc_address_);
  }
}

void ReferenceCounter::AddLocalReference(const ObjectID &object_id) {
  absl::MutexLock lock(&mutex_);
  auto it = object_id_refs_.find(object_id);
  if (it == object_id_refs_.end()) {
    // NOTE: ownership info for these objects must be added later via AddBorrowedObject.
    it = object_id_refs_.emplace(object_id, Reference()).first;
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
    auto it = object_id_refs_.find(argument_id);
    if (it == object_id_refs_.end()) {
      // This happens if a large argument is transparently passed by reference
      // because we don't hold a Python reference to its ObjectID.
      it = object_id_refs_.emplace(argument_id, Reference()).first;
    }
    it->second.submitted_task_ref_count++;
  }
  RemoveSubmittedTaskReferences(argument_ids_to_remove, deleted);
}

void ReferenceCounter::UpdateFinishedTaskReferences(
    const std::vector<ObjectID> &argument_ids, const rpc::Address &worker_addr,
    const ReferenceTableProto &borrowed_refs, std::vector<ObjectID> *deleted) {
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

  RemoveSubmittedTaskReferences(argument_ids, deleted);
}

void ReferenceCounter::RemoveSubmittedTaskReferences(
    const std::vector<ObjectID> &argument_ids, std::vector<ObjectID> *deleted) {
  for (const ObjectID &argument_id : argument_ids) {
    auto it = object_id_refs_.find(argument_id);
    if (it == object_id_refs_.end()) {
      RAY_LOG(WARNING) << "Tried to decrease ref count for nonexistent object ID: "
                       << argument_id;
      return;
    }
    it->second.submitted_task_ref_count--;
    if (it->second.RefCount() == 0) {
      DeleteReferenceInternal(it, deleted);
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

void ReferenceCounter::DeleteReferences(const std::vector<ObjectID> &object_ids) {
  absl::MutexLock lock(&mutex_);
  for (const ObjectID &object_id : object_ids) {
    auto it = object_id_refs_.find(object_id);
    if (it == object_id_refs_.end()) {
      return;
    }
    it->second.local_ref_count = 0;
    it->second.submitted_task_ref_count = 0;
    if (distributed_ref_counting_enabled_ && !it->second.CanDelete()) {
      RAY_LOG(ERROR)
          << "ray.internal.free does not currently work for objects that are still in "
             "scope when distributed reference "
             "counting is enabled. Try disabling ref counting by passing "
             "distributed_ref_counting_enabled: 0 in the ray.init internal config.";
    }
    DeleteReferenceInternal(it, nullptr);
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
  // Whether it is safe to delete the Reference.
  bool should_delete_reference = false;

  // If distributed ref counting is not enabled, then delete the object as soon
  // as its local ref count goes to 0.
  size_t local_ref_count =
      it->second.local_ref_count + it->second.submitted_task_ref_count;
  if (!distributed_ref_counting_enabled_ && local_ref_count == 0) {
    should_delete_value = true;
  }

  if (it->second.CanDelete()) {
    // If distributed ref counting is enabled, then delete the object once its
    // ref count across all processes is 0.
    should_delete_value = true;
    should_delete_reference = true;
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
    if (it->second.on_delete) {
      RAY_LOG(DEBUG) << "Calling on_delete for object " << id;
      it->second.on_delete(id);
      it->second.on_delete = nullptr;
    }
    if (deleted) {
      deleted->push_back(id);
    }
  }
  if (should_delete_reference) {
    RAY_LOG(DEBUG) << "Deleting Reference to object " << id;
    object_id_refs_.erase(it);
  }
}

bool ReferenceCounter::SetDeleteCallback(
    const ObjectID &object_id, const std::function<void(const ObjectID &)> callback) {
  absl::MutexLock lock(&mutex_);
  auto it = object_id_refs_.find(object_id);
  if (it == object_id_refs_.end()) {
    return false;
  }
  RAY_CHECK(!it->second.on_delete) << object_id;
  it->second.on_delete = callback;
  return true;
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
  if (!it->second.owner.has_value() &&
      borrower_ref.contained_in_borrowed_id.has_value()) {
    // We don't have owner information about this object ID yet and the worker
    // received it because it was nested in another ID that the worker was
    // borrowing. Copy this information to our local table.
    RAY_CHECK(borrower_ref.owner.has_value());
    AddBorrowedObjectInternal(object_id, *borrower_it->second.contained_in_borrowed_id,
                              borrower_ref.owner->first, borrower_ref.owner->second);
  }
  std::vector<rpc::WorkerAddress> new_borrowers;

  // The worker is still using the reference, so it is still a borrower.
  if (borrower_ref.RefCount() > 0) {
    auto inserted = it->second.borrowers.insert(worker_addr).second;
    // If we are the owner of id, then send WaitForRefRemoved to borrower.
    if (inserted) {
      RAY_LOG(DEBUG) << "Adding borrower " << worker_addr.ip_address << " to id "
                     << object_id;
      new_borrowers.push_back(worker_addr);
    }
  }

  // Add any other workers that this worker passed the ID to as new borrowers.
  for (const auto &nested_borrower : borrower_ref.borrowers) {
    auto inserted = it->second.borrowers.insert(nested_borrower).second;
    if (inserted) {
      RAY_LOG(DEBUG) << "Adding borrower " << nested_borrower.ip_address << " to id "
                     << object_id;
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
  request.mutable_reference()->set_owner_id(ref_it->second.owner->first.Binary());
  request.mutable_reference()->mutable_owner_address()->CopyFrom(
      ref_it->second.owner->second);
  request.set_contained_in_id(contained_in_id.Binary());
  request.set_intended_worker_id(addr.worker_id.Binary());

  auto it = borrower_cache_.find(addr);
  if (it == borrower_cache_.end()) {
    RAY_CHECK(client_factory_ != nullptr);
    it = borrower_cache_.emplace(addr, client_factory_(addr.ToProto())).first;
  }

  RAY_LOG(DEBUG) << "Sending WaitForRefRemoved to borrower " << addr.ip_address << ":"
                 << addr.port << " for object " << object_id;
  // Send the borrower a message about this object. The borrower responds once
  // it is no longer using the object ID.
  RAY_CHECK_OK(it->second->WaitForRefRemoved(
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
      }));
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
      RAY_LOG(DEBUG) << "Adding borrower " << owner_address.ip_address << " to id "
                     << inner_id << ", borrower owns outer ID " << object_id;
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
    RAY_CHECK(it->second.CanDelete());
  }
  // Send the owner information about any new borrowers.
  ReferenceTableToProto(borrowed_refs, reply->mutable_borrowed_refs());

  RAY_LOG(DEBUG) << "Replying to WaitForRefRemoved, reply has "
                 << reply->borrowed_refs().size();
  send_reply_callback(Status::OK(), nullptr, nullptr);
}

void ReferenceCounter::SetRefRemovedCallback(
    const ObjectID &object_id, const ObjectID &contained_in_id, const TaskID &owner_id,
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

ReferenceCounter::Reference ReferenceCounter::Reference::FromProto(
    const rpc::ObjectReferenceCount &ref_count) {
  Reference ref;
  ref.owner = {TaskID::FromBinary(ref_count.reference().owner_id()),
               ref_count.reference().owner_address()};
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
  if (owner.has_value()) {
    ref->mutable_reference()->set_owner_id(owner->first.Binary());
    ref->mutable_reference()->mutable_owner_address()->CopyFrom(owner->second);
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
