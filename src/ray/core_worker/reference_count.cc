#include "ray/core_worker/reference_count.h"

#define PRINT_REF_COUNT(it)                                                              \
  RAY_LOG(DEBUG) << "REF " << it->first << " borrowers: " << it->second.borrowers.size() \
                 << " local_ref_count: " << it->second.local_ref_count                   \
                 << " submitted_count: " << it->second.submitted_task_ref_count          \
                 << " contained_in_owned: " << it->second.contained_in_owned.size()      \
                 << " contained_in_borrowed: "                                           \
                 << (it->second.contained_in_borrowed_id.has_value()                     \
                         ? *it->second.contained_in_borrowed_id                          \
                         : ObjectID::Nil());

namespace {}  // namespace

namespace ray {

ReferenceCounter::ReferenceTable ReferenceCounter::ReferenceTableFromProto(
    const ReferenceTableProto &proto) {
  ReferenceTable new_borrower_refs;
  for (const auto &ref : proto) {
    new_borrower_refs[ray::ObjectID::FromBinary(ref.reference().object_id())] =
        Reference::FromProto(ref);
  }
  return new_borrower_refs;
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
    if (outer_it == object_id_refs_.end()) {
      outer_it = object_id_refs_.emplace(outer_id, Reference()).first;
    }
    if (outer_it->second.owned_by_us) {
      RAY_LOG(DEBUG) << "Setting borrowed inner ID " << object_id
                     << " contained_in_owned: " << outer_id;
      it->second.contained_in_owned.insert(outer_id);
    } else {
      RAY_LOG(DEBUG) << "Setting borrowed inner ID " << object_id
                     << " contained_in_borrowed: " << outer_id;
      RAY_CHECK(!it->second.contained_in_borrowed_id.has_value());
      it->second.contained_in_borrowed_id = outer_id;
    }
    outer_it->second.contains.insert(object_id);
  }
  return true;
}

void ReferenceCounter::AddOwnedObject(const ObjectID &object_id, const TaskID &owner_id,
                                      const rpc::Address &owner_address) {
  RAY_LOG(DEBUG) << "Adding owned object " << object_id;
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
  RAY_CHECK(it->second.local_ref_count > 0)
      << "Tried to decrease ref count for object ID that has count 0" << object_id;
  it->second.local_ref_count--;
  RAY_LOG(DEBUG) << "Remove local reference " << object_id;
  PRINT_REF_COUNT(it);
  if (it->second.RefCount() == 0) {
    RAY_LOG(DEBUG) << "xxx";
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
    const std::vector<ObjectID> &object_ids, const rpc::Address &worker_addr,
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
  for (const ObjectID &object_id : object_ids) {
    MergeBorrowerRefs(object_id, worker_addr, refs);
  }

  for (const ObjectID &object_id : object_ids) {
    auto it = object_id_refs_.find(object_id);
    if (it == object_id_refs_.end()) {
      RAY_LOG(WARNING) << "Tried to decrease ref count for nonexistent object ID: "
                       << object_id;
      return;
    }
    it->second.submitted_task_ref_count--;
    if (it->second.RefCount() == 0) {
      RAY_LOG(DEBUG) << "yyy";
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
    RAY_LOG(DEBUG) << "zzz";
    DeleteReferenceInternal(it, nullptr);
  }
}

void ReferenceCounter::DeleteReferenceInternal(ReferenceTable::iterator it,
                                               std::vector<ObjectID> *deleted) {
  const ObjectID id = it->first;
  RAY_LOG(DEBUG) << "Attempting to delete object " << id;
  if (it->second.RefCount() == 0 && it->second.on_local_ref_deleted) {
    RAY_LOG(DEBUG) << "Calling on_local_ref_deleted for object " << id;
    it->second.on_local_ref_deleted();
  }
  PRINT_REF_COUNT(it);
  if (it->second.CanDelete()) {
    for (const auto &inner_id : it->second.contains) {
      RAY_LOG(DEBUG) << "Try to delete inner object " << inner_id;
      auto inner_it = object_id_refs_.find(inner_id);
      RAY_CHECK(inner_it != object_id_refs_.end());
      if (it->second.owned_by_us) {
        RAY_CHECK(inner_it->second.contained_in_owned.erase(id));
      } else {
        RAY_CHECK(!inner_it->second.contained_in_borrowed_id.has_value());
      }
      RAY_LOG(DEBUG) << "aaa";
      DeleteReferenceInternal(inner_it, deleted);
    }

    RAY_LOG(DEBUG) << "Deleting object " << id;
    if (it->second.on_delete) {
      it->second.on_delete(id);
    }
    if (deleted) {
      deleted->push_back(id);
    }
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

void ReferenceCounter::GetAndStripBorrowedRefs(
    const std::vector<ObjectID> &borrowed_ids,
    ReferenceCounter::ReferenceTableProto *proto) {
  absl::MutexLock lock(&mutex_);
  ReferenceTable borrower_refs;
  for (const auto &borrowed_id : borrowed_ids) {
    RAY_CHECK(GetAndStripBorrowedRefsInternal(borrowed_id, &borrower_refs));
    // Decrease the ref count for each of the borrowed IDs. This is because we
    // artificially increment each borrowed ID to keep it pinned during task
    // execution. However, this should not count towards the final ref count
    // returned to the task's caller.
    auto it = borrower_refs.find(borrowed_id);
    RAY_CHECK(it != borrower_refs.end());
    it->second.local_ref_count--;
  }
  ReferenceTableToProto(borrower_refs, proto);
}

bool ReferenceCounter::GetAndStripBorrowedRefsInternal(const ObjectID &object_id,
                                                       ReferenceTable *borrower_refs) {
  RAY_LOG(DEBUG) << "Pop " << object_id;
  auto it = object_id_refs_.find(object_id);
  if (it == object_id_refs_.end()) {
    return false;
  }

  if (it->second.owned_by_us) {
    return false;
  }

  borrower_refs->emplace(object_id, it->second);
  it->second.borrowers.clear();

  if (it->second.contained_in_borrowed_id.has_value()) {
    if (borrower_refs->count(it->second.contained_in_borrowed_id.value()) > 0) {
      it->second.contained_in_borrowed_id.reset();
    } else {
      // ??? ID was deserialized from 2 different IDs. is this even reachable?
    }
  }

  // Attempt to Pop children.
  for (const auto &contained_id : it->second.contains) {
    GetAndStripBorrowedRefsInternal(contained_id, borrower_refs);
  }

  return true;
}

void ReferenceCounter::MergeBorrowerRefs(const ObjectID &object_id,
                                         const rpc::WorkerAddress &borrower,
                                         const ReferenceTable &borrower_refs) {
  auto borrower_it = borrower_refs.find(object_id);
  if (borrower_it == borrower_refs.end()) {
    return;
  }
  const auto &borrower_ref = borrower_it->second;
  RAY_LOG(DEBUG) << "Merging ref " << object_id << " has "
                 << borrower_ref.borrowers.size() << " borrowers "
                 << ", has local: " << borrower_ref.local_ref_count
                 << " submitted: " << borrower_ref.submitted_task_ref_count
                 << " contained_in_owned " << borrower_ref.contained_in_owned.size();

  auto it = object_id_refs_.find(object_id);
  if (it == object_id_refs_.end()) {
    RAY_CHECK(borrower_ref.contained_in_borrowed_id.has_value());
    RAY_CHECK(borrower_ref.owner.has_value());
    it = object_id_refs_.emplace(object_id, Reference()).first;
    AddBorrowedObjectInternal(object_id, *borrower_it->second.contained_in_borrowed_id,
                              borrower_ref.owner->first, borrower_ref.owner->second);
  }
  std::vector<rpc::WorkerAddress> new_borrowers;

  // The worker is still using the reference, so it is still a borrower.
  if (borrower_ref.RefCount() > 0) {
    auto inserted = it->second.borrowers.insert(borrower).second;
    // If we are the owner of id, then send WaitForRefRemoved to borrower.
    if (inserted) {
      RAY_LOG(DEBUG) << "Adding borrower " << borrower.ip_address << " to id "
                     << object_id;
      new_borrowers.push_back(borrower);
    }
  }

  for (const auto &nested_borrower : borrower_ref.borrowers) {
    auto inserted = it->second.borrowers.insert(nested_borrower).second;
    if (inserted) {
      RAY_LOG(DEBUG) << "Adding borrower " << nested_borrower.ip_address << " to id "
                     << object_id;
      new_borrowers.push_back(nested_borrower);
    }
  }

  // If we own this ID, then wait for all new borrowers to reach a ref count
  // of 0.
  if (it->second.owned_by_us) {
    for (const auto &addr : new_borrowers) {
      WaitForRefRemoved(it, addr);
    }
  }

  for (const auto &inner_id : borrower_ref.contains) {
    MergeBorrowerRefs(inner_id, borrower, borrower_refs);
  }
}

void ReferenceCounter::WaitForRefRemoved(const ReferenceTable::iterator &ref_it,
                                         const rpc::WorkerAddress &addr,
                                         const ObjectID &contained_in_id) {
  const ObjectID &object_id = ref_it->first;
  rpc::WaitForRefRemovedRequest request;
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
    RAY_LOG(DEBUG) << "Connected to borrower " << addr.ip_address << ":" << addr.port
                   << " for object " << object_id;
  }

  RAY_CHECK_OK(it->second->WaitForRefRemoved(
      request, [this, object_id, addr](const Status &status,
                                       const rpc::WaitForRefRemovedReply &reply) {
        RAY_LOG(DEBUG) << "Received reply from borrower " << addr.ip_address << ":"
                       << addr.port << " of object " << object_id;
        absl::MutexLock lock(&mutex_);
        auto it = object_id_refs_.find(object_id);
        RAY_CHECK(it != object_id_refs_.end());
        RAY_CHECK(it->second.borrowers.erase(addr));

        const ReferenceTable new_borrower_refs =
            ReferenceTableFromProto(reply.borrower_refs());

        MergeBorrowerRefs(object_id, addr, new_borrower_refs);
        RAY_LOG(DEBUG) << "ddd";
        DeleteReferenceInternal(it, nullptr);
      }));
}

void ReferenceCounter::WrapObjectId(
    const ObjectID &object_id, const std::vector<ObjectID> &inner_ids,
    const absl::optional<rpc::WorkerAddress> &owner_address) {
  absl::MutexLock lock(&mutex_);
  auto it = object_id_refs_.find(object_id);
  bool owned_by_us = false;
  if (it != object_id_refs_.end()) {
    owned_by_us = it->second.owned_by_us;
  }
  if (!owner_address.has_value()) {
    // `ray.put()` case. The owner of the object ID was not specified by the
    // caller, so it must be us.
    RAY_CHECK(owned_by_us);
  }

  if (owned_by_us) {
    // `ray.put()` case OR returning an object ID from a task and the task's
    // caller executed in the same process as us.
    for (const auto &inner_id : inner_ids) {
      it->second.contains.insert(inner_id);
      auto inner_it = object_id_refs_.find(inner_id);
      RAY_CHECK(inner_it != object_id_refs_.end());
      RAY_LOG(DEBUG) << "Setting inner ID " << inner_id
                     << " contained_in_owned: " << object_id;
      inner_it->second.contained_in_owned.insert(object_id);
    }
  } else {
    RAY_CHECK(owner_address.has_value());
    // Returning an object ID from a task, and the task's caller executed in a
    // remote process.
    // TODO: Handle the case where we return a BORROWED id.
    for (const auto &inner_id : inner_ids) {
      auto inner_it = object_id_refs_.find(inner_id);
      RAY_CHECK(inner_it != object_id_refs_.end());
      if (!inner_it->second.owned_by_us) {
        RAY_LOG(WARNING)
            << "Ref counting currently does not support returning an object ID that was "
               "not created by the worker executing the task. The object may be evicted "
               "before all references are out of scope.";
      }
      // Add the task's caller as a borrower and wait for it to remove its
      // reference.
      inner_it->second.borrowers.insert(*owner_address);
      WaitForRefRemoved(inner_it, *owner_address, object_id);
    }
  }
}

void ReferenceCounter::OnRefRemoved(const ObjectID &object_id,
                                    rpc::WaitForRefRemovedReply *reply,
                                    rpc::SendReplyCallback send_reply_callback) {
  ReferenceTable borrower_refs;
  RAY_UNUSED(GetAndStripBorrowedRefsInternal(object_id, &borrower_refs));
  for (const auto &pair : borrower_refs) {
    RAY_LOG(DEBUG) << pair.first << " has " << pair.second.borrowers.size()
                   << " borrowers";
  }
  auto it = object_id_refs_.find(object_id);
  if (it != object_id_refs_.end()) {
    // We should only have called this callback once our local ref count for
    // the object was zero. Also, we should have popped our borrowers and
    // returned them in the reply to the owner.
    RAY_CHECK(it->second.CanDelete());
  }
  // RAY_CHECK(object_id_refs_.count(object_id) == 0);
  ReferenceTableToProto(borrower_refs, reply->mutable_borrower_refs());

  RAY_LOG(DEBUG) << "Replying to WaitForRefRemoved, reply has "
                 << reply->borrower_refs().size();
  send_reply_callback(Status::OK(), nullptr, nullptr);
}

void ReferenceCounter::HandleWaitForRefRemoved(
    const rpc::WaitForRefRemovedRequest &request, rpc::WaitForRefRemovedReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  absl::MutexLock lock(&mutex_);
  const ObjectID &object_id = ObjectID::FromBinary(request.reference().object_id());
  RAY_LOG(DEBUG) << "Received WaitForRefRemoved " << object_id;
  auto ref_removed_callback = boost::bind(&ReferenceCounter::OnRefRemoved, this,
                                          object_id, reply, send_reply_callback);

  auto it = object_id_refs_.find(object_id);

  ObjectID contained_in_id = ObjectID::FromBinary(request.contained_in_id());
  if (!contained_in_id.IsNil()) {
    auto outer_it = object_id_refs_.find(contained_in_id);
    if (outer_it != object_id_refs_.end()) {
      RAY_CHECK(outer_it->second.owned_by_us);
      if (it == object_id_refs_.end()) {
        it = object_id_refs_.emplace(object_id, Reference()).first;
      }
      AddBorrowedObjectInternal(object_id, contained_in_id,
                                TaskID::FromBinary(request.reference().owner_id()),
                                request.reference().owner_address());
    }
  }

  if (it == object_id_refs_.end() || it->second.RefCount() == 0) {
    ref_removed_callback();
  } else {
    RAY_CHECK(it->second.on_local_ref_deleted == nullptr);
    it->second.on_local_ref_deleted = ref_removed_callback;
  }
}

bool ReferenceCounter::HasReference(const ObjectID &object_id) {
  const auto it = object_id_refs_.find(object_id);
  return it != object_id_refs_.end();
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
  if (contained_in_borrowed_id.has_value()) {
    ref->set_contained_in_borrowed_id(contained_in_borrowed_id->Binary());
  }
  for (const auto &contains_id : contains) {
    ref->add_contains(contains_id.Binary());
  }
}

}  // namespace ray
