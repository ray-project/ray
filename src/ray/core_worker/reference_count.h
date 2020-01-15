#ifndef RAY_CORE_WORKER_REF_COUNT_H
#define RAY_CORE_WORKER_REF_COUNT_H

#include "absl/base/thread_annotations.h"
#include "absl/container/flat_hash_map.h"
#include "absl/synchronization/mutex.h"
#include "ray/common/id.h"
#include "ray/protobuf/common.pb.h"
#include "ray/util/logging.h"

namespace ray {

/// Class used by the core worker to keep track of ObjectID reference counts for garbage
/// collection. This class is thread safe.
class ReferenceCounter {
 public:
  ReferenceCounter() {}

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
  void AddSubmittedTaskReferences(const std::vector<ObjectID> &object_ids)
      LOCKS_EXCLUDED(mutex_);

  /// Remove references for the provided object IDs that correspond to them being
  /// dependencies to a submitted task. This should be called when inlined
  /// dependencies are inlined or when the task finishes for plasma dependencies.
  ///
  /// \param[in] object_ids The object IDs to remove references for.
  /// \param[out] deleted The object IDs whos reference counts reached zero.
  void RemoveSubmittedTaskReferences(const std::vector<ObjectID> &object_ids,
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
  /// \param[in] owner_id The ID of the object's owner.
  /// \param[in] owner_address The address of the object's owner.
  /// \param[in] dependencies The objects that the object depends on.
  void AddOwnedObject(const ObjectID &object_id, const TaskID &owner_id,
                      const rpc::Address &owner_address) LOCKS_EXCLUDED(mutex_);

  /// Add an object that we are borrowing.
  ///
  /// \param[in] object_id The ID of the object that we are borrowing.
  /// \param[in] owner_id The ID of the owner of the object. This is either the
  /// task ID (for non-actors) or the actor ID of the owner.
  /// \param[in] owner_address The owner's address.
  void AddBorrowedObject(const ObjectID &object_id, const TaskID &owner_id,
                         const rpc::Address &owner_address) LOCKS_EXCLUDED(mutex_);

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

  /// Returns the total number of ObjectIDs currently in scope.
  size_t NumObjectIDsInScope() const LOCKS_EXCLUDED(mutex_);

  /// Returns whether this object has an active reference.
  bool HasReference(const ObjectID &object_id) const LOCKS_EXCLUDED(mutex_);

  /// Returns a set of all ObjectIDs currently in scope (i.e., nonzero reference count).
  std::unordered_set<ObjectID> GetAllInScopeObjectIDs() const LOCKS_EXCLUDED(mutex_);

  /// Returns a map of all ObjectIDs currently in scope with a pair of their
  /// (local, submitted_task) reference counts. For debugging purposes.
  std::unordered_map<ObjectID, std::pair<size_t, size_t>> GetAllReferenceCounts() const
      LOCKS_EXCLUDED(mutex_);

 private:
  /// Metadata for an ObjectID reference in the language frontend.
  struct Reference {
    /// Constructor for a reference whose origin is unknown.
    Reference() : owned_by_us(false) {}
    /// Constructor for a reference that we created.
    Reference(const TaskID &owner_id, const rpc::Address &owner_address)
        : owned_by_us(true), owner({owner_id, owner_address}) {}
    /// The local ref count for the ObjectID in the language frontend.
    size_t local_ref_count = 0;
    /// The ref count for submitted tasks that depend on the ObjectID.
    size_t submitted_task_ref_count = 0;
    /// Whether we own the object. If we own the object, then we are
    /// responsible for tracking the state of the task that creates the object
    /// (see task_manager.h).
    bool owned_by_us;
    /// The object's owner, if we know it. This has no value if the object is
    /// if we do not know the object's owner (because distributed ref counting
    /// is not yet implemented).
    absl::optional<std::pair<TaskID, rpc::Address>> owner;
    /// Callback that will be called when this ObjectID no longer has references.
    std::function<void(const ObjectID &)> on_delete;
  };

  /// Helper method to delete an entry from the reference map and run any necessary
  /// callbacks. Assumes that the entry is in object_id_refs_ and invalidates the
  /// iterator.
  void DeleteReferenceInternal(absl::flat_hash_map<ObjectID, Reference>::iterator entry,
                               std::vector<ObjectID> *deleted)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  /// Protects access to the reference counting state.
  mutable absl::Mutex mutex_;

  /// Holds all reference counts and dependency information for tracked ObjectIDs.
  absl::flat_hash_map<ObjectID, Reference> object_id_refs_ GUARDED_BY(mutex_);
};

}  // namespace ray

#endif  // RAY_CORE_WORKER_REF_COUNT_H
