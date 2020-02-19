#ifndef RAY_CORE_WORKER_REF_COUNT_H
#define RAY_CORE_WORKER_REF_COUNT_H

#include "absl/base/thread_annotations.h"
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/synchronization/mutex.h"
#include "ray/common/id.h"
#include "ray/protobuf/common.pb.h"
#include "ray/rpc/grpc_server.h"
#include "ray/rpc/worker/core_worker_client.h"
#include "ray/util/logging.h"

#include <boost/bind.hpp>

namespace ray {

/// Class used by the core worker to keep track of ObjectID reference counts for garbage
/// collection. This class is thread safe.
class ReferenceCounter {
 public:
  using ReferenceTableProto =
      ::google::protobuf::RepeatedPtrField<rpc::ObjectReferenceCount>;
  using ReferenceRemovedCallback = std::function<void(const ObjectID &)>;

  ReferenceCounter(const rpc::WorkerAddress &rpc_address,
                   bool distributed_ref_counting_enabled = true,
                   rpc::ClientFactoryFn client_factory = nullptr)
      : rpc_address_(rpc_address),
        distributed_ref_counting_enabled_(distributed_ref_counting_enabled),
        client_factory_(client_factory) {}

  ~ReferenceCounter() {}

  /// Increase the reference count for the ObjectID by one. If there is no
  /// entry for the ObjectID, one will be created. The object ID will not have
  /// any owner information, since we don't know how it was created.
  ///
  /// \param[in] object_id The object to to increment the count for.
  void AddLocalReference(const ObjectID &object_id) LOCKS_EXCLUDED(mutex_);

  /// Decrease the local reference count for the ObjectID by one.
  ///
  /// \param[in] object_id The object to decrement the count for.
  /// \param[out] deleted List to store objects that hit zero ref count.
  void RemoveLocalReference(const ObjectID &object_id, std::vector<ObjectID> *deleted)
      LOCKS_EXCLUDED(mutex_);

  /// Add references for the provided object IDs that correspond to them being
  /// dependencies to a submitted task.
  ///
  /// \param[in] object_ids The object IDs to add references for.
  void UpdateSubmittedTaskReferences(
      const std::vector<ObjectID> &argument_ids_to_add,
      const std::vector<ObjectID> &argument_ids_to_remove = std::vector<ObjectID>(),
      std::vector<ObjectID> *deleted = nullptr) LOCKS_EXCLUDED(mutex_);

  /// Update object references that were given to a submitted task. The task
  /// may still be borrowing any object IDs that were contained in its
  /// arguments. This should be called when inlined dependencies are inlined or
  /// when the task finishes for plasma dependencies.
  ///
  /// \param[in] object_ids The object IDs to remove references for.
  /// \param[in] worker_addr The address of the worker that executed the task.
  /// \param[in] borrowed_refs The references that the worker borrowed during
  /// the task. This table includes all task arguments that were passed by
  /// reference and any object IDs that were transitively nested in the
  /// arguments. Some references in this table may still be borrowed by the
  /// worker and/or a task that the worker submitted.
  /// \param[out] deleted The object IDs whos reference counts reached zero.
  void UpdateFinishedTaskReferences(const std::vector<ObjectID> &argument_ids,
                                    const rpc::Address &worker_addr,
                                    const ReferenceTableProto &borrowed_refs,
                                    std::vector<ObjectID> *deleted)
      LOCKS_EXCLUDED(mutex_);

  /// Add an object that we own. The object may depend on other objects.
  /// Dependencies for each ObjectID must be set at most once. The local
  /// reference count for the ObjectID is set to zero, which assumes that an
  /// ObjectID for it will be created in the language frontend after this call.
  ///
  /// TODO(swang): We could avoid copying the owner_id and owner_address since
  /// we are the owner, but it is easier to store a copy for now, since the
  /// owner ID will change for workers executing normal tasks and it is
  /// possible to have leftover references after a task has finished.
  ///
  /// \param[in] object_id The ID of the object that we own.
  /// \param[in] inner_ids ObjectIDs that are contained in the object's value.
  /// As long as the object_id is in scope, the inner objects should not be GC'ed.
  /// \param[in] owner_id The ID of the object's owner.
  /// \param[in] owner_address The address of the object's owner.
  /// \param[in] dependencies The objects that the object depends on.
  void AddOwnedObject(const ObjectID &object_id,
                      const std::vector<ObjectID> &contained_ids, const TaskID &owner_id,
                      const rpc::Address &owner_address) LOCKS_EXCLUDED(mutex_);

  /// Add an object that we are borrowing.
  ///
  /// \param[in] object_id The ID of the object that we are borrowing.
  /// \param[in] outer_id The ID of the object that contained this object ID,
  /// if one exists. An outer_id may not exist if object_id was inlined
  /// directly in a task spec, or if it was passed in the application
  /// out-of-band.
  /// \param[in] owner_id The ID of the owner of the object. This is either the
  /// task ID (for non-actors) or the actor ID of the owner.
  /// \param[in] owner_address The owner's address.
  bool AddBorrowedObject(const ObjectID &object_id, const ObjectID &outer_id,
                         const TaskID &owner_id, const rpc::Address &owner_address)
      LOCKS_EXCLUDED(mutex_);

  /// Get the owner ID and address of the given object.
  ///
  /// \param[in] object_id The ID of the object to look up.
  /// \param[out] owner_id The TaskID of the object owner.
  /// \param[out] owner_address The address of the object owner.
  bool GetOwner(const ObjectID &object_id, TaskID *owner_id,
                rpc::Address *owner_address) const LOCKS_EXCLUDED(mutex_);

  /// Manually delete the objects from the reference counter.
  void DeleteReferences(const std::vector<ObjectID> &object_ids) LOCKS_EXCLUDED(mutex_);

  /// Sets the callback that will be run when the object goes out of scope.
  /// Returns true if the object was in scope and the callback was added, else false.
  bool SetDeleteCallback(const ObjectID &object_id,
                         const std::function<void(const ObjectID &)> callback)
      LOCKS_EXCLUDED(mutex_);

  /// Set a callback for when we are no longer borrowing this object (when our
  /// ref count goes to 0).
  ///
  /// \param[in] object_id The object ID to set the callback for.
  /// \param[in] contained_in_id The object ID that contains object_id, if any.
  /// This is used for cases when object_id was returned from a task that we
  /// submitted. Then, as long as we have contained_in_id in scope, we are
  /// borrowing object_id.
  /// \param[in] owner_id The ID of the owner of object_id. This is either the
  /// task ID (for non-actors) or the actor ID of the owner.
  /// \param[in] owner_address The owner of object_id's address.
  /// \param[in] ref_removed_callback The callback to call when we are no
  /// longer borrowing the object.
  void SetRefRemovedCallback(const ObjectID &object_id, const ObjectID &contained_in_id,
                             const TaskID &owner_id, const rpc::Address &owner_address,
                             const ReferenceRemovedCallback &ref_removed_callback)
      LOCKS_EXCLUDED(mutex_);

  /// Respond to the object's owner once we are no longer borrowing it.  The
  /// sender is the owner of the object ID. We will send the reply when our
  /// RefCount() for the object ID goes to 0.
  ///
  /// \param[in] object_id The object that we were borrowing.
  /// \param[in] reply A reply sent to the owner when we are no longer
  /// borrowing the object ID. This reply also includes any new borrowers and
  /// any object IDs that were nested inside the object that we or others are
  /// now borrowing.
  /// \param[in] send_reply_callback The callback to send the reply.
  void HandleRefRemoved(const ObjectID &object_id, rpc::WaitForRefRemovedReply *reply,
                        rpc::SendReplyCallback send_reply_callback)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  /// Returns the total number of ObjectIDs currently in scope.
  size_t NumObjectIDsInScope() const LOCKS_EXCLUDED(mutex_);

  /// Returns a set of all ObjectIDs currently in scope (i.e., nonzero reference count).
  std::unordered_set<ObjectID> GetAllInScopeObjectIDs() const LOCKS_EXCLUDED(mutex_);

  /// Returns a map of all ObjectIDs currently in scope with a pair of their
  /// (local, submitted_task) reference counts. For debugging purposes.
  std::unordered_map<ObjectID, std::pair<size_t, size_t>> GetAllReferenceCounts() const
      LOCKS_EXCLUDED(mutex_);

  /// Populate a table with ObjectIDs that we were or are still borrowing.
  /// This should be called when a task returns, and the argument should be any
  /// IDs that were passed by reference in the task spec or that were
  /// serialized in inlined arguments.
  ///
  /// See GetAndClearLocalBorrowersInternal for the spec of the returned table
  /// and how this mutates the local reference count.
  ///
  /// \param[in] borrowed_ids The object IDs that we or another worker were or
  /// are still borrowing. These are the IDs that were given to us via task
  /// submission and includes: (1) any IDs that were passed by reference in the
  /// task spec, and (2) any IDs that were serialized in the task's inlined
  /// arguments.
  /// \param[out] proto The protobuf table to populate with the borrowed
  /// references.
  void GetAndClearLocalBorrowers(const std::vector<ObjectID> &borrowed_ids,
                                 ReferenceTableProto *proto) LOCKS_EXCLUDED(mutex_);

  /// Mark that this ObjectID contains another ObjectID(s). This should be
  /// called in two cases:
  /// 1. We are storing the value of an object and the value contains
  /// serialized copies of other ObjectIDs. If the outer object is owned by a
  /// remote process, then they are now a borrower of the nested IDs.
  /// 2. We submitted a task that returned an ObjectID(s) in its return values
  /// and we are processing the worker's reply. In this case, we own the task's
  /// return objects and are borrowing the nested IDs.
  ///
  /// \param[in] object_id The ID of the object that contains other ObjectIDs.
  /// \param[in] inner_ids The object IDs are nested in object_id's value.
  /// \param[in] owner_address The owner address of the outer object_id. If
  /// this is not provided, then the outer object ID must be owned by us. the
  /// outer object ID is not owned by us, then this is used to contact the
  /// outer object's owner, since it is considered a borrower for the inner
  /// IDs.
  void AddNestedObjectIds(const ObjectID &object_id,
                          const std::vector<ObjectID> &inner_ids,
                          const rpc::WorkerAddress &owner_address) LOCKS_EXCLUDED(mutex_);

  /// Whether we have a reference to a particular ObjectID.
  ///
  /// \param[in] object_id The object ID to check for.
  /// \return Whether we have a reference to the object ID.
  bool HasReference(const ObjectID &object_id) const LOCKS_EXCLUDED(mutex_);

 private:
  struct Reference {
    /// Constructor for a reference whose origin is unknown.
    Reference() : owned_by_us(false) {}
    /// Constructor for a reference that we created.
    Reference(const TaskID &owner_id, const rpc::Address &owner_address)
        : owned_by_us(true), owner({owner_id, owner_address}) {}

    /// Constructor from a protobuf. This is assumed to be a message from
    /// another process, so the object defaults to not being owned by us.
    static Reference FromProto(const rpc::ObjectReferenceCount &ref_count);
    /// Serialize to a protobuf.
    void ToProto(rpc::ObjectReferenceCount *ref) const;

    /// The reference count. This number includes:
    /// - Python references to the ObjectID.
    /// - Pending submitted tasks that depend on the object.
    /// - ObjectIDs that we own, that contain this ObjectID, and that are still
    ///   in scope.
    size_t RefCount() const {
      return local_ref_count + submitted_task_ref_count + contained_in_owned.size();
    }

    /// Whether we can delete this reference. A reference can NOT be deleted if
    /// any of the following are true:
    /// - The reference is still being used by this process.
    /// - The reference was contained in another ID that we were borrowing, and
    ///   we haven't told the process that gave us that ID yet.
    /// - We gave the reference to at least one other process.
    bool CanDelete() const {
      bool in_scope = RefCount() > 0;
      bool was_contained_in_borrowed_id = contained_in_borrowed_id.has_value();
      bool has_borrowers = borrowers.size() > 0;
      bool was_stored_in_objects = stored_in_objects.size() > 0;
      return !(in_scope || was_contained_in_borrowed_id || has_borrowers ||
               was_stored_in_objects);
    }

    /// Whether we own the object. If we own the object, then we are
    /// responsible for tracking the state of the task that creates the object
    /// (see task_manager.h).
    bool owned_by_us;
    /// The object's owner, if we know it. This has no value if the object is
    /// if we do not know the object's owner (because distributed ref counting
    /// is not yet implemented).
    absl::optional<std::pair<TaskID, rpc::Address>> owner;

    /// The local ref count for the ObjectID in the language frontend.
    size_t local_ref_count = 0;
    /// The ref count for submitted tasks that depend on the ObjectID.
    size_t submitted_task_ref_count = 0;
    /// Object IDs that we own and that contain this object ID.
    /// ObjectIDs are added to this field when we discover that this object
    /// contains other IDs. This can happen in 2 cases:
    ///  1. We call ray.put() and store the inner ID(s) in the outer object.
    ///  2. A task that we submitted returned an ID(s).
    /// ObjectIDs are erased from this field when their Reference is deleted.
    absl::flat_hash_set<ObjectID> contained_in_owned;
    /// An Object ID that we (or one of our children) borrowed that contains
    /// this object ID, which is also borrowed. This is used in cases where an
    /// ObjectID is nested. We need to notify the owner of the outer ID of any
    /// borrowers of this object, so we keep this field around until
    /// GetAndClearLocalBorrowersInternal is called on the outer ID. This field
    /// is updated in 2 cases:
    ///  1. We deserialize an ID that we do not own and that was stored in
    ///     another object that we do not own.
    ///  2. Case (1) occurred for a task that we submitted and we also do not
    ///     own the inner or outer object. Then, we need to notify our caller
    ///     that the task we submitted is a borrower for the inner ID.
    /// This field is reset to null once GetAndClearLocalBorrowersInternal is
    /// called on contained_in_borrowed_id. For each borrower, this field is
    /// set at most once during the reference's lifetime. If the object ID is
    /// later found to be nested in a second object, we do not need to remember
    /// the second ID because we will already have notified the owner of the
    /// first outer object about our reference.
    absl::optional<ObjectID> contained_in_borrowed_id;
    /// The object IDs contained in this object. These could be objects that we
    /// own or are borrowing. This field is updated in 2 cases:
    ///  1. We call ray.put() on this ID and store the contained IDs.
    ///  2. We call ray.get() on an ID whose contents we do not know and we
    ///     discover that it contains these IDs.
    absl::flat_hash_set<ObjectID> contains;
    /// A list of processes that are we gave a reference to that are still
    /// borrowing the ID. This field is updated in 2 cases:
    ///  1. If we are a borrower of the ID, then we add a process to this list
    ///     if we passed that process a copy of the ID via task submission and
    ///     the process is still using the ID by the time it finishes its task.
    ///     Borrowers are removed from the list when we recursively merge our
    ///     list into the owner.
    ///  2. If we are the owner of the ID, then either the above case, or when
    ///     we hear from a borrower that it has passed the ID to other
    ///     borrowers. A borrower is removed from the list when it responds
    ///     that it is no longer using the reference.
    absl::flat_hash_set<rpc::WorkerAddress> borrowers;
    /// When a process that is borrowing an object ID stores the ID inside the
    /// return value of a task that it executes, the caller of the task is also
    /// considered a borrower for as long as its reference to the task's return
    /// ID stays in scope. Thus, the borrower must notify the owner that the
    /// task's caller is also a borrower. The key is the task's return ID, and
    /// the value is the task ID and address of the task's caller.
    absl::flat_hash_map<ObjectID, rpc::WorkerAddress> stored_in_objects;

    /// Callback that will be called when this ObjectID no longer has
    /// references.
    std::function<void(const ObjectID &)> on_delete;
    /// Callback that is called when this process is no longer a borrower
    /// (RefCount() == 0).
    std::function<void(const ObjectID &)> on_ref_removed;
  };

  using ReferenceTable = absl::flat_hash_map<ObjectID, Reference>;

  /// Deserialize a ReferenceTable.
  static ReferenceTable ReferenceTableFromProto(const ReferenceTableProto &proto);

  /// Serialize a ReferenceTable.
  static void ReferenceTableToProto(const ReferenceTable &table,
                                    ReferenceTableProto *proto);

  /// Remove references for the provided object IDs that correspond to them
  /// being dependencies to a submitted task. This should be called when
  /// inlined dependencies are inlined or when the task finishes for plasma
  /// dependencies.
  void RemoveSubmittedTaskReferences(const std::vector<ObjectID> &argument_ids,
                                     std::vector<ObjectID> *deleted)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  /// Helper method to mark that this ObjectID contains another ObjectID(s).
  ///
  /// \param[in] object_id The ID of the object that contains other ObjectIDs.
  /// \param[in] inner_ids The object IDs are nested in object_id's value.
  /// \param[in] owner_address The owner address of the outer object_id. If
  /// this is not provided, then the outer object ID must be owned by us. the
  /// outer object ID is not owned by us, then this is used to contact the
  /// outer object's owner, since it is considered a borrower for the inner
  /// IDs.
  void AddNestedObjectIdsInternal(const ObjectID &object_id,
                                  const std::vector<ObjectID> &inner_ids,
                                  const rpc::WorkerAddress &owner_address)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  /// Populates the table with the ObjectID that we were or are still
  /// borrowing. The table also includes any IDs that we discovered were
  /// contained in the ID. For each borrowed ID, we will return:
  /// - The borrowed ID's owner's address.
  /// - Whether we are still using the ID or not (RefCount() > 0).
  /// - Addresses of new borrowers that we passed the ID to.
  /// - Whether the borrowed ID was contained in another ID that we borrowed.
  ///
  /// We will also attempt to clear the information put into the returned table
  /// that we no longer need in our local table. Each reference in the local
  /// table is modified in the following way:
  /// - For each borrowed ID, remove the addresses of any new borrowers. We
  ///   don't need these anymore because the receiver of the borrowed_refs is
  ///   either the owner or another borrow who will eventually return the list
  ///   to the owner.
  /// - For each ID that was contained in a borrowed ID, forget that the ID
  ///   that contained it. We don't need this anymore because we already marked
  ///   that the borrowed ID contained another ID in the returned
  ///   borrowed_refs.
  bool GetAndClearLocalBorrowersInternal(const ObjectID &object_id,
                                         ReferenceTable *borrowed_refs)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  /// Merge remote borrowers into our local ref count. This will add any
  /// workers that are still borrowing the given object ID to the local ref
  /// counts, and recursively any workers that are borrowing object IDs that
  /// were nested inside. This is the converse of GetAndClearLocalBorrowers.
  /// For each borrowed object ID, we will:
  /// - Add the worker to our list of borrowers if it is still using the
  ///   reference.
  /// - Add the worker's accumulated borrowers to our list of borrowers.
  /// - If the borrowed ID was nested in another borrowed ID, then mark it as
  ///   such so that we can later merge the inner ID's reference into its
  ///   owner.
  /// - If we are the owner of the ID, then also contact any new borrowers and
  ///   wait for them to stop using the reference.
  void MergeRemoteBorrowers(const ObjectID &object_id,
                            const rpc::WorkerAddress &worker_addr,
                            const ReferenceTable &borrowed_refs)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  /// Wait for a borrower to stop using its reference. This should only be
  /// called by the owner of the ID.
  /// \param[in] reference_it Iterator pointing to the reference that we own.
  /// \param[in] addr The address of the borrower.
  /// \param[in] contained_in_id Whether the owned ID was contained in another
  /// ID. This is used in cases where we return an object ID that we own inside
  /// an object that we do not own. Then, we must notify the owner of the outer
  /// object that they are borrowing the inner.
  void WaitForRefRemoved(const ReferenceTable::iterator &reference_it,
                         const rpc::WorkerAddress &addr,
                         const ObjectID &contained_in_id = ObjectID::Nil())
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  /// Helper method to add an object that we are borrowing. This is used when
  /// deserializing IDs from a task's arguments, or when deserializing an ID
  /// during ray.get().
  bool AddBorrowedObjectInternal(const ObjectID &object_id, const ObjectID &outer_id,
                                 const TaskID &owner_id,
                                 const rpc::Address &owner_address)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  /// Helper method to delete an entry from the reference map and run any necessary
  /// callbacks. Assumes that the entry is in object_id_refs_ and invalidates the
  /// iterator.
  void DeleteReferenceInternal(ReferenceTable::iterator entry,
                               std::vector<ObjectID> *deleted)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  /// Address of our RPC server. This is used to determine whether we own a
  /// given object or not, by comparing our WorkerID with the WorkerID of the
  /// object's owner.
  rpc::WorkerAddress rpc_address_;

  /// Feature flag for distributed ref counting. If this is false, then we will
  /// keep the distributed ref count, but only the local ref count will be used
  /// to decide when objects can be evicted.
  bool distributed_ref_counting_enabled_;

  /// Factory for producing new core worker clients.
  rpc::ClientFactoryFn client_factory_;

  /// Map from worker address to core worker client. The owner of an object
  /// uses this client to request a notification from borrowers once the
  /// borrower's ref count for the ID goes to 0.
  absl::flat_hash_map<rpc::WorkerAddress, std::shared_ptr<rpc::CoreWorkerClientInterface>>
      borrower_cache_ GUARDED_BY(mutex_);

  /// Protects access to the reference counting state.
  mutable absl::Mutex mutex_;

  /// Holds all reference counts and dependency information for tracked ObjectIDs.
  ReferenceTable object_id_refs_ GUARDED_BY(mutex_);
};

}  // namespace ray

#endif  // RAY_CORE_WORKER_REF_COUNT_H
