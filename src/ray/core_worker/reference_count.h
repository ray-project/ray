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

  /// Decrease the reference count for the ObjectID by one. If the reference count reaches
  /// zero, it will be erased from the map and the reference count for all of its
  /// dependencies will be decreased be one.
  ///
  /// \param[in] object_id The object to to decrement the count for.
  /// \param[out] deleted List to store objects that hit zero ref count.
  void RemoveLocalReference(const ObjectID &object_id, std::vector<ObjectID> *deleted)
      LOCKS_EXCLUDED(mutex_);

  /// Add an object that we own. The object may depend on other objects.
  /// Dependencies for each ObjectID must be set at most once. The direct
  /// reference count for the ObjectID is set to zero and the reference count
  /// for each dependency is incremented.
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
                      const rpc::Address &owner_address,
                      std::shared_ptr<std::vector<ObjectID>> dependencies)
      LOCKS_EXCLUDED(mutex_);

  /// Add an object that we are borrowing.
  ///
  /// \param[in] object_id The ID of the object that we are borrowing.
  /// \param[in] owner_id The ID of the owner of the object. This is either the
  /// task ID (for non-actors) or the actor ID of the owner.
  /// \param[in] owner_address The owner's address.
  void AddBorrowedObject(const ObjectID &object_id, const TaskID &owner_id,
                         const rpc::Address &owner_address) LOCKS_EXCLUDED(mutex_);

  bool GetOwner(const ObjectID &object_id, TaskID *owner_id,
                rpc::Address *owner_address) const LOCKS_EXCLUDED(mutex_);

  /// Returns the total number of ObjectIDs currently in scope.
  size_t NumObjectIDsInScope() const LOCKS_EXCLUDED(mutex_);

  /// Returns whether this object has an active reference.
  bool HasReference(const ObjectID &object_id) const LOCKS_EXCLUDED(mutex_);

  /// Returns a set of all ObjectIDs currently in scope (i.e., nonzero reference count).
  std::unordered_set<ObjectID> GetAllInScopeObjectIDs() const LOCKS_EXCLUDED(mutex_);

  /// Dumps information about all currently tracked references to RAY_LOG(DEBUG).
  void LogDebugString() const LOCKS_EXCLUDED(mutex_);

 private:
  /// Metadata for an ObjectID reference in the language frontend.
  struct Reference {
    /// Constructor for a reference whose origin is unknown.
    Reference() : owned_by_us(false) {}
    /// Constructor for a reference that we created.
    Reference(const TaskID &owner_id, const rpc::Address &owner_address,
              std::shared_ptr<std::vector<ObjectID>> deps)
        : dependencies(std::move(deps)),
          owned_by_us(true),
          owner({owner_id, owner_address}) {}
    /// Constructor for a reference that was given to us.
    Reference(const TaskID &owner_id, const rpc::Address &owner_address)
        : owned_by_us(false), owner({owner_id, owner_address}) {}
    /// The local ref count for the ObjectID in the language frontend.
    size_t local_ref_count = 0;
    /// The objects that this object depends on. Tracked only by the owner of
    /// the object. Dependencies are stored as shared_ptrs because the same set
    /// of dependencies can be shared among multiple entries. For example, when
    /// a task has multiple return values, the entry for each return ObjectID
    /// depends on all task dependencies.
    std::shared_ptr<std::vector<ObjectID>> dependencies;
    /// Whether we own the object. If we own the object, then we are
    /// responsible for tracking the state of the task that creates the object
    /// (see task_manager.h).
    bool owned_by_us;
    /// The object's owner, if we know it. This has no value if the object is
    /// if we do not know the object's owner (because distributed ref counting
    /// is not yet implemented).
    const absl::optional<std::pair<TaskID, rpc::Address>> owner;
  };

  /// Helper function with the same semantics as AddReference to allow adding a reference
  /// while already holding mutex_.
  void AddLocalReferenceInternal(const ObjectID &object_id)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  /// Recursive helper function for decreasing reference counts. Will recursively call
  /// itself on any dependencies whose reference count reaches zero as a result of
  /// removing the reference.
  ///
  /// \param[in] object_id The object to to decrement the count for.
  /// \param[in] deleted List to store objects that hit zero ref count.
  void RemoveReferenceRecursive(const ObjectID &object_id, std::vector<ObjectID> *deleted)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  /// Protects access to the reference counting state.
  mutable absl::Mutex mutex_;

  /// Holds all reference counts and dependency information for tracked ObjectIDs.
  absl::flat_hash_map<ObjectID, Reference> object_id_refs_ GUARDED_BY(mutex_);
};

}  // namespace ray

#endif  // RAY_CORE_WORKER_REF_COUNT_H
