#include "ray/core_worker/reference_count.h"

namespace {

ray::rpc::WaitForRefRemovedReply ReferenceTableToWaitForRefRemovedReply(const ray::ReferenceCounter::ReferenceTable &refs) {
  ray::rpc::WaitForRefRemovedReply reply;
  for (const auto &id_ref : refs) {
    auto ref = id_ref.second.ToProto();
    ref.set_object_id(id_ref.first.Binary());
    reply.add_borrower_refs()->CopyFrom(ref);
  }
  return reply;
}

}

namespace ray {

void ReferenceCounter::AddBorrowedObject(const ObjectID &outer_id, const ObjectID &object_id,
                                         const TaskID &owner_id,
                                         const rpc::Address &owner_address) {
  absl::MutexLock lock(&mutex_);
  auto it = object_id_refs_.find(object_id);
  RAY_CHECK(it != object_id_refs_.end());

  if (!it->second.owner.has_value()) {
    it->second.owner = {owner_id, owner_address};
  }

  auto outer_it = object_id_refs_.find(outer_id);
  if (outer_it == object_id_refs_.end()) {
    outer_it = object_id_refs_.emplace(outer_id, Reference()).first;
  }
  if (it->second.owned_by_us) {
    it->second.contained_in.insert(outer_id);
  }
  outer_it->second.contains.insert(object_id);
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
  auto it = object_id_refs_.find(object_id);
  if (it == object_id_refs_.end()) {
    // NOTE: ownership info for these objects must be added later via AddBorrowedObject.
    it = object_id_refs_.emplace(object_id, Reference()).first;
  }
  it->second.local_ref_count++;
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
  it->second.local_ref_count--;
  if (it->second.RefCount() == 0) {
    DeleteReferenceInternal(it, deleted);
  }
}

void ReferenceCounter::AddSubmittedTaskReferences(
    const std::vector<ObjectID> &object_ids) {
  absl::MutexLock lock(&mutex_);
  for (const ObjectID &object_id : object_ids) {
    auto it = object_id_refs_.find(object_id);
    if (it == object_id_refs_.end()) {
      // This happens if a large argument is transparently passed by reference
      // because we don't hold a Python reference to its ObjectID.
      it = object_id_refs_.emplace(object_id, Reference()).first;
    }
    it->second.submitted_task_ref_count++;
  }
}

void ReferenceCounter::RemoveSubmittedTaskReferences(
    const std::vector<ObjectID> &object_ids,
    const rpc::Address &borrower,
    const ReferenceTable &borrower_refs,
    std::vector<ObjectID> *deleted) {
  absl::MutexLock lock(&mutex_);
  // Must merge the borrower refs before decrementing any ref counts. This is
  // to make sure that for serialized IDs, we increment the borrower count for
  // the inner ID before decrementing the submitted_task_ref_count for the
  // outer ID.
  MergeBorrowerRefs(borrower, borrower_refs);

  for (const ObjectID &object_id : object_ids) {
    auto it = object_id_refs_.find(object_id);
    if (it == object_id_refs_.end()) {
      RAY_LOG(WARNING) << "Tried to decrease ref count for nonexistent object ID: "
                       << object_id;
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
    DeleteReferenceInternal(it, nullptr);
  }
}

void ReferenceCounter::DeleteReferenceInternal(
    ReferenceTable::iterator it,
    std::vector<ObjectID> *deleted) {
  if (it->second.RefCount() == 0 && it->second.on_local_ref_deleted) {
    auto on_local_ref_deleted = it->second.on_local_ref_deleted;
    it->second.on_local_ref_deleted = nullptr;
    on_local_ref_deleted();
  }
  if (it->second.RefCount() + it->second.NumBorrowers() == 0) {
    const auto contains = std::move(it->second.contains);

    if (it->second.on_delete) {
      it->second.on_delete(it->first);
    }
    if (deleted) {
      deleted->push_back(it->first);
    }
    object_id_refs_.erase(it);

    for (const auto &inner_id : contains) {
      RAY_LOG(DEBUG) << "Decrementing ref count for inner ID " << inner_id;
      auto inner_it = object_id_refs_.find(inner_id);
      // The inner ID can go out of scope before the outer one.
      if (inner_it != object_id_refs_.end()) {
        inner_it->second.contained_in.erase(it->first);
        DeleteReferenceInternal(inner_it, deleted);
      }
    }
  }
}

bool ReferenceCounter::SetDeleteCallback(
    const ObjectID &object_id, const std::function<void(const ObjectID &)> callback) {
  absl::MutexLock lock(&mutex_);
  auto it = object_id_refs_.find(object_id);
  if (it == object_id_refs_.end()) {
    return false;
  }
  RAY_CHECK(!it->second.on_delete);
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

ReferenceCounter::ReferenceTable ReferenceCounter::PopBorrowerRefs(const ObjectID &object_id) {
  absl::MutexLock lock(&mutex_);
  ReferenceTable borrower_refs;
  RAY_CHECK(PopBorrowerRefsInternal(object_id, &borrower_refs));
  return borrower_refs;
}

bool ReferenceCounter::PopBorrowerRefsInternal(const ObjectID &object_id, ReferenceTable *borrower_refs) {
  auto it = object_id_refs_.find(object_id);
  if (it == object_id_refs_.end()) {
    return false;
  }

  borrower_refs->emplace(object_id, it->second);
  it->second.borrowers.clear();

  if (it->second.RefCount() == 0) {
    DeleteReferenceInternal(it, nullptr);
  }

  for (const auto &contained_id : (*borrower_refs)[object_id].contains) {
    PopBorrowerRefsInternal(contained_id, borrower_refs);
  }

  return true;
}

void ReferenceCounter::MergeBorrowerRefs(const rpc::WorkerAddress &borrower, const ReferenceTable &borrower_refs) {
  for (const auto &id_ref : borrower_refs) {
    const auto &id = id_ref.first;
    const auto &ref = id_ref.second;
    auto it = object_id_refs_.find(id);
    RAY_CHECK(it != object_id_refs_.end());
    std::vector<rpc::WorkerAddress> new_borrowers;

    // The worker is still using the reference, so it is still a borrower.
    if (ref.RefCount() > 0) {
      auto inserted = it->second.borrowers.insert(borrower).second;
      // If we are the owner of id, then send WaitForRefRemoved to borrower.
      if (inserted) {
        new_borrowers.push_back(borrower);
      }
    }

    for (const auto &nested_borrower : ref.borrowers) {
      auto inserted = it->second.borrowers.insert(nested_borrower).second;
      if (inserted) {
        new_borrowers.push_back(nested_borrower);
      }
    }

    // If we own this ID, then wait for all new borrowers to reach a ref count
    // of 0.
    rpc::WaitForRefRemovedRequest request;
    request.set_object_id(id.Binary());
    if (it->second.owned_by_us) {
      for (const auto &addr : new_borrowers) {
        auto it = borrower_cache_.find(addr);
        if (it == borrower_cache_.end()) {
          it = borrower_cache_.emplace(addr, client_factory_(addr.ip_address, addr.port)).first;
          RAY_LOG(DEBUG) << "Connected to borrower " << addr.ip_address << ":" << addr.port;
        }
        request.set_intended_worker_id(addr.worker_id.Binary());
        it->second->WaitForRefRemoved(request, [this, id, addr](const Status &status, const rpc::WaitForRefRemovedReply &reply) {
          RAY_LOG(DEBUG) << "Received reply from borrower " << addr.ip_address << ":" << addr.port;
          absl::MutexLock lock(&mutex_);
          auto it = object_id_refs_.find(id);
          RAY_CHECK(it != object_id_refs_.end());
          RAY_CHECK(it->second.borrowers.erase(addr));

          ReferenceTable new_borrower_refs;
          for (const auto &ref : reply.borrower_refs()) {
            Reference reference;
            reference.local_ref_count = ref.has_local_ref() ? 1 : 0;
            for (const auto &borrower : ref.borrowers()) {
              reference.borrowers.insert(rpc::WorkerAddress(borrower));
            }
            new_borrower_refs[ObjectID::FromBinary(ref.object_id())] = Reference();
          }
          MergeBorrowerRefs(addr, new_borrower_refs);
          RAY_LOG(DEBUG) << "Ref count is now " << it->second.RefCount() << " num borrowers " << it->second.NumBorrowers();
          if (it->second.RefCount() + it->second.NumBorrowers() == 0) {
            DeleteReferenceInternal(it, nullptr);
          }
        });
      }
    }
  }
}

void ReferenceCounter::WrapObjectId(const ObjectID &object_id, const std::vector<ObjectID> &inner_ids, bool ray_put) {
  absl::MutexLock lock(&mutex_);
  auto it = object_id_refs_.find(object_id);
  RAY_CHECK(it != object_id_refs_.end());
  for (const auto &inner_id : inner_ids) {
    it->second.contains.insert(inner_id);
    auto inner_it = object_id_refs_.find(inner_id);
    RAY_CHECK(inner_it != object_id_refs_.end());
    if (it->second.owned_by_us) {
      // `ray.put()` case. We are wrapping an object ID that we have a
      // reference to inside another object that we own.
      inner_it->second.contained_in.insert(object_id);
    } else {
      // TODO: Task return case. We are putting an object ID that we have a
      // reference to inside an object that is owned by the task's caller.
    }
  }
}

void ReferenceCounter::HandleWaitForRefRemoved(const ObjectID &object_id, const absl::optional<ObjectID> contained_in_id, rpc::WaitForRefRemovedReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  auto ref_removed_callback = [this, object_id, reply, send_reply_callback]() {
    ReferenceTable borrower_refs;
    RAY_CHECK(PopBorrowerRefsInternal(object_id, &borrower_refs));
    // We should only have called this callback once our local ref count for
    // the object was zero. Also, we should have popped our borrowers and
    // returned them in the reply to the owner, so we should have removed this
    // reference completely.
    RAY_CHECK(object_id_refs_.count(object_id) == 0);
    *reply = ReferenceTableToWaitForRefRemovedReply(borrower_refs);
    send_reply_callback(Status::OK(), nullptr, nullptr);
  };

  auto it = object_id_refs_.find(object_id);
  if (it == object_id_refs_.end() || it->second.RefCount() == 0) {
    ref_removed_callback();
  } else {
    it->second.on_local_ref_deleted = ref_removed_callback;
  }
}

}  // namespace ray
