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

#pragma once

#include <boost/bind.hpp>

#include "absl/base/thread_annotations.h"
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/synchronization/mutex.h"
#include "ray/common/id.h"
#include "ray/core_worker/lease_policy.h"
#include "ray/pubsub/publisher.h"
#include "ray/pubsub/subscriber.h"
#include "ray/rpc/grpc_server.h"
#include "ray/rpc/worker/core_worker_client.h"
#include "ray/rpc/worker/core_worker_client_pool.h"
#include "ray/util/logging.h"
#include "src/ray/protobuf/common.pb.h"

namespace ray {

// Interface for mocking.
class ReferenceCounterInterface {
 public:
  virtual void AddLocalReference(const ObjectID &object_id,
                                 const std::string &call_site) = 0;
  virtual bool AddBorrowedObject(const ObjectID &object_id, const ObjectID &outer_id,
                                 const rpc::Address &owner_address) = 0;
  virtual void AddOwnedObject(
      const ObjectID &object_id, const std::vector<ObjectID> &contained_ids,
      const rpc::Address &owner_address, const std::string &call_site,
      const int64_t object_size, bool is_reconstructable,
      const absl::optional<NodeID> &pinned_at_raylet_id = absl::optional<NodeID>()) = 0;
  virtual bool SetDeleteCallback(
      const ObjectID &object_id,
      const std::function<void(const ObjectID &)> callback) = 0;

  virtual ~ReferenceCounterInterface() {}
};

// Callback for location subscriptions.
using LocationSubscriptionCallback =
    std::function<void(const absl::flat_hash_set<NodeID> &, int64_t, const std::string &,
                       const NodeID &, int64_t)>;

/// Class used by the core worker to keep track of ObjectID reference counts for garbage
/// collection. This class is thread safe.
class ReferenceCounter : public ReferenceCounterInterface,
                         public LocalityDataProviderInterface {
 public:
  using ReferenceTableProto =
      ::google::protobuf::RepeatedPtrField<rpc::ObjectReferenceCount>;
  using ReferenceRemovedCallback = std::function<void(const ObjectID &)>;
  using LineageReleasedCallback =
      std::function<void(const ObjectID &, std::vector<ObjectID> *)>;

  ReferenceCounter(const rpc::WorkerAddress &rpc_address,
                   std::shared_ptr<pubsub::PublisherInterface> object_status_publisher,
                   std::shared_ptr<pubsub::SubscriberInterface> object_status_subscriber,
                   bool distributed_ref_counting_enabled = true,
                   bool lineage_pinning_enabled = false,
                   rpc::ClientFactoryFn client_factory = nullptr)
      : rpc_address_(rpc_address),
        distributed_ref_counting_enabled_(distributed_ref_counting_enabled),
        lineage_pinning_enabled_(lineage_pinning_enabled),
        borrower_pool_(client_factory),
        object_status_publisher_(object_status_publisher),
        object_status_subscriber_(object_status_subscriber) {}

  ~ReferenceCounter() {}

  /// Wait for all object references to go out of scope, and then shutdown.
  ///
  /// \param shutdown The shutdown callback to call.
  void DrainAndShutdown(std::function<void()> shutdown);

  /// Return true if the worker owns any object.
  bool OwnObjects() const;

  /// Increase the reference count for the ObjectID by one. If there is no
  /// entry for the ObjectID, one will be created. The object ID will not have
  /// any owner information, since we don't know how it was created.
  ///
  /// \param[in] object_id The object to to increment the count for.
  void AddLocalReference(const ObjectID &object_id, const std::string &call_site)
      LOCKS_EXCLUDED(mutex_);

  /// Decrease the local reference count for the ObjectID by one.
  ///
  /// \param[in] object_id The object to decrement the count for.
  /// \param[out] deleted List to store objects that hit zero ref count.
  void RemoveLocalReference(const ObjectID &object_id, std::vector<ObjectID> *deleted)
      LOCKS_EXCLUDED(mutex_);

  /// Add references for the provided object IDs that correspond to them being
  /// dependencies to a submitted task. If lineage pinning is enabled, then
  /// this will also pin the Reference entry for each new argument until the
  /// argument's lineage ref is released.
  ///
  /// \param[in] argument_ids_to_add The arguments of the task to add
  /// references for.
  /// \param[out] argument_ids_to_remove The arguments of the task to remove
  /// references for.
  /// \param[out] deleted Any objects that are newly out of scope after this
  /// function call.
  void UpdateSubmittedTaskReferences(
      const std::vector<ObjectID> &argument_ids_to_add,
      const std::vector<ObjectID> &argument_ids_to_remove = std::vector<ObjectID>(),
      std::vector<ObjectID> *deleted = nullptr) LOCKS_EXCLUDED(mutex_);

  /// Add references for the object dependencies of a resubmitted task. This
  /// does not increment the arguments' lineage ref counts because we should
  /// have already incremented them when the task was first submitted.
  ///
  /// \param[in] argument_ids The arguments of the task to add references for.
  void UpdateResubmittedTaskReferences(const std::vector<ObjectID> &argument_ids)
      LOCKS_EXCLUDED(mutex_);

  /// Update object references that were given to a submitted task. The task
  /// may still be borrowing any object IDs that were contained in its
  /// arguments. This should be called when inlined dependencies are inlined or
  /// when the task finishes for plasma dependencies.
  ///
  /// \param[in] object_ids The object IDs to remove references for.
  /// \param[in] release_lineage Whether to decrement the arguments' lineage
  /// ref count.
  /// \param[in] worker_addr The address of the worker that executed the task.
  /// \param[in] borrowed_refs The references that the worker borrowed during
  /// the task. This table includes all task arguments that were passed by
  /// reference and any object IDs that were transitively nested in the
  /// arguments. Some references in this table may still be borrowed by the
  /// worker and/or a task that the worker submitted.
  /// \param[out] deleted The object IDs whos reference counts reached zero.
  void UpdateFinishedTaskReferences(const std::vector<ObjectID> &argument_ids,
                                    bool release_lineage, const rpc::Address &worker_addr,
                                    const ReferenceTableProto &borrowed_refs,
                                    std::vector<ObjectID> *deleted)
      LOCKS_EXCLUDED(mutex_);

  /// Release the lineage ref count for this list of object IDs. An object's
  /// lineage ref count is the number of tasks that depend on the object that
  /// may be retried in the future (pending execution or finished but
  /// retryable). If the object is direct (not stored in plasma), then its
  /// lineage ref count is 0.
  ///
  /// \param[in] argument_ids The list of objects whose lineage ref counts we
  /// should decrement.
  void ReleaseLineageReferences(const std::vector<ObjectID> &argument_ids)
      LOCKS_EXCLUDED(mutex_);

  /// Add an object that we own. The object may depend on other objects.
  /// Dependencies for each ObjectID must be set at most once. The local
  /// reference count for the ObjectID is set to zero, which assumes that an
  /// ObjectID for it will be created in the language frontend after this call.
  ///
  /// TODO(swang): We could avoid copying the owner_address since
  /// we are the owner, but it is easier to store a copy for now, since the
  /// owner ID will change for workers executing normal tasks and it is
  /// possible to have leftover references after a task has finished.
  ///
  /// \param[in] object_id The ID of the object that we own.
  /// \param[in] contained_ids ObjectIDs that are contained in the object's value.
  /// As long as the object_id is in scope, the inner objects should not be GC'ed.
  /// \param[in] owner_address The address of the object's owner.
  /// \param[in] call_site Description of the call site where the reference was created.
  /// \param[in] object_size Object size if known, otherwise -1;
  /// \param[in] is_reconstructable Whether the object can be reconstructed
  /// through lineage re-execution.
  void AddOwnedObject(
      const ObjectID &object_id, const std::vector<ObjectID> &contained_ids,
      const rpc::Address &owner_address, const std::string &call_site,
      const int64_t object_size, bool is_reconstructable,
      const absl::optional<NodeID> &pinned_at_raylet_id = absl::optional<NodeID>())
      LOCKS_EXCLUDED(mutex_);

  /// Remove reference for an object that we own. The reference will only be
  /// removed if the object's ref count is 0. This should only be used when
  /// speculatively adding an owned reference that may need to be rolled back, e.g. if
  /// the creation of the corresponding Plasma object fails. All other references will
  /// be cleaned up via the reference counting protocol.
  ///
  /// \param[in] object_id The ID of the object that we own and wish to remove.
  void RemoveOwnedObject(const ObjectID &object_id) LOCKS_EXCLUDED(mutex_);

  /// Update the size of the object.
  ///
  /// \param[in] object_id The ID of the object.
  /// \param[in] size The known size of the object.
  void UpdateObjectSize(const ObjectID &object_id, int64_t object_size)
      LOCKS_EXCLUDED(mutex_);

  /// Add an object that we are borrowing.
  ///
  /// \param[in] object_id The ID of the object that we are borrowing.
  /// \param[in] outer_id The ID of the object that contained this object ID,
  /// if one exists. An outer_id may not exist if object_id was inlined
  /// directly in a task spec, or if it was passed in the application
  /// out-of-band.
  /// task ID (for non-actors) or the actor ID of the owner.
  /// \param[in] owner_address The owner's address.
  bool AddBorrowedObject(const ObjectID &object_id, const ObjectID &outer_id,
                         const rpc::Address &owner_address) LOCKS_EXCLUDED(mutex_);

  /// Get the owner address of the given object.
  ///
  /// \param[in] object_id The ID of the object to look up.
  /// \param[out] owner_address The address of the object owner.
  /// \return false if the object is out of scope or we do not yet have
  /// ownership information. The latter can happen when object IDs are pasesd
  /// out of band.
  bool GetOwner(const ObjectID &object_id, rpc::Address *owner_address = nullptr) const
      LOCKS_EXCLUDED(mutex_);

  /// Get the owner addresses of the given objects. The owner address
  /// must be registered for these objects.
  ///
  /// \param[in] object_ids The IDs of the object to look up.
  /// \return The addresses of the objects' owners.
  std::vector<rpc::Address> GetOwnerAddresses(
      const std::vector<ObjectID> object_ids) const;

  /// Check whether an object value has been freed.
  ///
  /// \param[in] object_id The object to check.
  /// \return Whether the object value has been freed.
  bool IsPlasmaObjectFreed(const ObjectID &object_id) const;

  /// Release the underlying value from plasma (if any) for these objects.
  ///
  /// \param[in] object_ids The IDs whose values to free.
  void FreePlasmaObjects(const std::vector<ObjectID> &object_ids) LOCKS_EXCLUDED(mutex_);

  /// Sets the callback that will be run when the object goes out of scope.
  /// Returns true if the object was in scope and the callback was added, else false.
  bool SetDeleteCallback(const ObjectID &object_id,
                         const std::function<void(const ObjectID &)> callback)
      LOCKS_EXCLUDED(mutex_);

  void ResetDeleteCallbacks(const std::vector<ObjectID> &object_ids)
      LOCKS_EXCLUDED(mutex_);

  /// Set a callback for when we are no longer borrowing this object (when our
  /// ref count goes to 0).
  ///
  /// \param[in] object_id The object ID to set the callback for.
  /// \param[in] contained_in_id The object ID that contains object_id, if any.
  /// This is used for cases when object_id was returned from a task that we
  /// submitted. Then, as long as we have contained_in_id in scope, we are
  /// borrowing object_id.
  /// \param[in] owner_address The owner of object_id's address.
  /// \param[in] ref_removed_callback The callback to call when we are no
  /// longer borrowing the object.
  void SetRefRemovedCallback(const ObjectID &object_id, const ObjectID &contained_in_id,
                             const rpc::Address &owner_address,
                             const ReferenceRemovedCallback &ref_removed_callback)
      LOCKS_EXCLUDED(mutex_);

  /// Set a callback to call whenever a Reference that we own is deleted. A
  /// Reference can only be deleted if:
  /// 1. The ObjectID's ref count is 0 on all workers.
  /// 2. There are no tasks that depend on the object that may be retried in
  /// the future.
  ///
  /// \param[in] callback The callback to call.
  void SetReleaseLineageCallback(const LineageReleasedCallback &callback);

  /// Respond to the object's owner once we are no longer borrowing it.  The
  /// sender is the owner of the object ID. We will send the reply when our
  /// RefCount() for the object ID goes to 0.
  ///
  /// \param[in] object_id The object that we were borrowing.
  /// \param[in] subscriber_worker_id The worker id of the worker that subscribes
  /// this reference to be removed.
  void HandleRefRemoved(const ObjectID &object_id, const WorkerID &subscriber_worker_id)
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

  /// Update the pinned location of an object stored in plasma.
  ///
  /// \param[in] object_id The object to update.
  /// \param[in] raylet_id The raylet that is now pinning the object ID.
  void UpdateObjectPinnedAtRaylet(const ObjectID &object_id, const NodeID &raylet_id)
      LOCKS_EXCLUDED(mutex_);

  /// Check whether the object is pinned at a remote plasma store node or
  /// spilled to external storage. In either case, a copy of the object is
  /// available to fetch.
  ///
  /// \param[in] object_id The object to check.
  /// \param[out] owned_by_us Whether this object is owned by us. The pinned_at
  /// and spilled out-parameters are set if this is true.
  /// \param[out] pinned_at The node ID of the raylet at which this object is
  /// \param[out] spilled Whether this object has been spilled.
  /// pinned. Set to nil if the object is not pinned.
  /// \return True if the reference exists, false otherwise.
  bool IsPlasmaObjectPinnedOrSpilled(const ObjectID &object_id, bool *owned_by_us,
                                     NodeID *pinned_at, bool *spilled) const
      LOCKS_EXCLUDED(mutex_);

  /// Get and reset the objects that were pinned on the given node.  This
  /// method should be called upon a node failure, to determine which plasma
  /// objects were lost. If a deletion callback was set for a lost object, it
  /// will be invoked and reset.
  ///
  /// \param[in] node_id The node whose object store has been removed.
  /// \return The set of objects that were pinned on the given node.
  std::vector<ObjectID> ResetObjectsOnRemovedNode(const NodeID &raylet_id);

  /// Whether we have a reference to a particular ObjectID.
  ///
  /// \param[in] object_id The object ID to check for.
  /// \return Whether we have a reference to the object ID.
  bool HasReference(const ObjectID &object_id) const LOCKS_EXCLUDED(mutex_);

  /// Write the current reference table to the given proto.
  ///
  /// \param[out] stats The proto to write references to.
  void AddObjectRefStats(
      const absl::flat_hash_map<ObjectID, std::pair<int64_t, std::string>> pinned_objects,
      rpc::CoreWorkerStats *stats) const LOCKS_EXCLUDED(mutex_);

  /// Add a new location for the given object. The owner must have the object ref in
  /// scope.
  ///
  /// \param[in] object_id The object to update.
  /// \param[in] node_id The new object location to be added.
  /// \return True if the reference exists, false otherwise.
  bool AddObjectLocation(const ObjectID &object_id, const NodeID &node_id)
      LOCKS_EXCLUDED(mutex_);

  /// Remove a location for the given object. The owner must have the object ref in
  /// scope.
  ///
  /// \param[in] object_id The object to update.
  /// \param[in] node_id The object location to be removed.
  /// \return True if the reference exists, false otherwise.
  bool RemoveObjectLocation(const ObjectID &object_id, const NodeID &node_id)
      LOCKS_EXCLUDED(mutex_);

  /// Get the locations of the given object. The owner must have the object ref in
  /// scope.
  ///
  /// \param[in] object_id The object to get locations for.
  /// \return The nodes that have the object if the reference exists, empty optional
  ///         otherwise.
  absl::optional<absl::flat_hash_set<NodeID>> GetObjectLocations(
      const ObjectID &object_id) LOCKS_EXCLUDED(mutex_);

  /// Subscribe to object location changes that are more recent than the given version.
  /// The provided callback will be invoked when new locations become available.
  ///
  /// \param[in] object_id The object whose locations we want.
  /// \param[in] last_location_version The version of the last location update the
  /// caller received. Only more recent location updates will be returned.
  /// \param[in] callback The callback to invoke with the location update.
  /// \return The status of the location get.
  Status SubscribeObjectLocations(const ObjectID &object_id,
                                  int64_t last_location_version,
                                  const LocationSubscriptionCallback &callback)
      LOCKS_EXCLUDED(mutex_);

  /// Get an object's size. This will return 0 if the object is out of scope.
  ///
  /// \param[in] object_id The object whose size to get.
  /// \return Object size, or 0 if the object is out of scope.
  size_t GetObjectSize(const ObjectID &object_id) const;

  /// Handle an object has been spilled to external storage.
  ///
  /// This notifies the primary raylet that the object is safe to release and
  /// records the spill URL, spill node ID, and updated object size.
  /// \param[in] object_id The object that has been spilled.
  /// \param[in] spilled_url The URL to which the object has been spilled.
  /// \param[in] spilled_node_id The ID of the node on which the object was spilled.
  /// \param[in] size The size of the object.
  /// \param[in] release Whether to release the reference.
  /// \return True if the reference exists, false otherwise.
  bool HandleObjectSpilled(const ObjectID &object_id, const std::string spilled_url,
                           const NodeID &spilled_node_id, int64_t size, bool release);

  /// Get locality data for object. This is used by the leasing policy to implement
  /// locality-aware leasing.
  ///
  /// \param[in] object_id Object whose locality data we want.
  /// \return Locality data.
  absl::optional<LocalityData> GetLocalityData(const ObjectID &object_id);

  /// Report locality data for object. This is used by the FutureResolver to report
  /// locality data for borrowed refs.
  ///
  /// \param[in] object_id Object whose locality data we're reporting.
  /// \param[in] locations Locations of the object.
  /// \param[in] object_size Size of the object.
  /// \return True if the reference exists, false otherwise.
  bool ReportLocalityData(const ObjectID &object_id,
                          const absl::flat_hash_set<NodeID> &locations,
                          uint64_t object_size);

 private:
  struct Reference {
    /// Constructor for a reference whose origin is unknown.
    Reference() {}
    Reference(std::string call_site, const int64_t object_size)
        : call_site(call_site), object_size(object_size) {}
    /// Constructor for a reference that we created.
    Reference(const rpc::Address &owner_address, std::string call_site,
              const int64_t object_size, bool is_reconstructable,
              const absl::optional<NodeID> &pinned_at_raylet_id)
        : call_site(call_site),
          object_size(object_size),
          owned_by_us(true),
          owner_address(owner_address),
          pinned_at_raylet_id(pinned_at_raylet_id),
          is_reconstructable(is_reconstructable) {}

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

    /// Whether this reference is no longer in scope. A reference is in scope
    /// if any of the following are true:
    /// - The reference is still being used by this process.
    /// - The reference was contained in another ID that we were borrowing, and
    ///   we haven't told the process that gave us that ID yet.
    /// - We gave the reference to at least one other process.
    bool OutOfScope(bool lineage_pinning_enabled) const {
      bool in_scope = RefCount() > 0;
      bool was_contained_in_borrowed_id = contained_in_borrowed_id.has_value();
      bool has_borrowers = borrowers.size() > 0;
      bool was_stored_in_objects = stored_in_objects.size() > 0;

      bool has_lineage_references = false;
      if (lineage_pinning_enabled && owned_by_us && !is_reconstructable) {
        has_lineage_references = lineage_ref_count > 0;
      }

      return !(in_scope || was_contained_in_borrowed_id || has_borrowers ||
               was_stored_in_objects || has_lineage_references);
    }

    /// Whether the Reference can be deleted. A Reference can only be deleted
    /// if:
    /// 1. The ObjectID's ref count is 0 on all workers.
    /// 2. If lineage pinning is enabled, there are no tasks that depend on
    /// the object that may be retried in the future.
    bool ShouldDelete(bool lineage_pinning_enabled) const {
      if (lineage_pinning_enabled) {
        return OutOfScope(lineage_pinning_enabled) && (lineage_ref_count == 0);
      } else {
        return OutOfScope(lineage_pinning_enabled);
      }
    }

    /// Description of the call site where the reference was created.
    std::string call_site = "<unknown>";
    /// Object size if known, otherwise -1;
    int64_t object_size = -1;

    /// Whether we own the object. If we own the object, then we are
    /// responsible for tracking the state of the task that creates the object
    /// (see task_manager.h).
    bool owned_by_us = false;
    /// The object's owner's address, if we know it. If this process is the
    /// owner, then this is added during creation of the Reference. If this is
    /// process is a borrower, the borrower must add the owner's address before
    /// using the ObjectID.
    absl::optional<rpc::Address> owner_address;
    /// If this object is owned by us and stored in plasma, and reference
    /// counting is enabled, then some raylet must be pinning the object value.
    /// This is the address of that raylet.
    absl::optional<NodeID> pinned_at_raylet_id;
    /// If this object is owned by us and stored in plasma, this contains all
    /// object locations.
    absl::flat_hash_set<NodeID> locations;
    /// A logical counter for object location updates, used for object location
    /// subscriptions. Subscribers use -1 to indicate that they want us to
    /// immediately send them the current location data.
    int64_t location_version = 0;
    // Whether this object can be reconstructed via lineage. If false, then the
    // object's value will be pinned as long as it is referenced by any other
    // object's lineage.
    const bool is_reconstructable = false;

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
    /// The number of tasks that depend on this object that may be retried in
    /// the future (pending execution or finished but retryable). If the object
    /// is inlined (not stored in plasma), then its lineage ref count is 0
    /// because any dependent task will already have the value of the object.
    size_t lineage_ref_count = 0;
    /// Whether this object has been spilled to external storage.
    bool spilled = false;
    /// For objects that have been spilled to external storage, the URL from which
    /// they can be retrieved.
    std::string spilled_url = "";
    /// The ID of the node that spilled the object.
    /// This will be Nil if the object has not been spilled or if it is spilled
    /// distributed external storage.
    NodeID spilled_node_id = NodeID::Nil();
    /// Location subscription callbacks registered by async location get requests.
    /// These will be invoked whenever locations or object_size are changed.
    std::vector<LocationSubscriptionCallback> location_subscription_callbacks;
    /// Callback that will be called when this ObjectID no longer has
    /// references.
    std::function<void(const ObjectID &)> on_delete;
    /// Callback that is called when this process is no longer a borrower
    /// (RefCount() == 0).
    std::function<void(const ObjectID &)> on_ref_removed;
  };

  using ReferenceTable = absl::flat_hash_map<ObjectID, Reference>;

  bool GetOwnerInternal(const ObjectID &object_id,
                        rpc::Address *owner_address = nullptr) const
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  /// Release the pinned plasma object, if any. Also unsets the raylet address
  /// that the object was pinned at, if the address was set.
  void ReleasePlasmaObject(ReferenceTable::iterator it);

  /// Shutdown if all references have gone out of scope and shutdown
  /// is scheduled.
  void ShutdownIfNeeded() EXCLUSIVE_LOCKS_REQUIRED(mutex_);

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
                                     bool release_lineage, std::vector<ObjectID> *deleted)
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
                                 const rpc::Address &owner_address)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  /// Helper method to delete an entry from the reference map and run any necessary
  /// callbacks. Assumes that the entry is in object_id_refs_ and invalidates the
  /// iterator.
  void DeleteReferenceInternal(ReferenceTable::iterator entry,
                               std::vector<ObjectID> *deleted)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  /// Helper method to decrement the lineage ref count for a list of objects.
  void ReleaseLineageReferencesInternal(const std::vector<ObjectID> &argument_ids)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  /// Add a new location for the given object. The owner must have the object ref in
  /// scope, and the caller must have already acquired mutex_.
  ///
  /// \param[in] it The reference iterator for the object.
  /// \param[in] node_id The new object location to be added.
  void AddObjectLocationInternal(ReferenceTable::iterator it, const NodeID &node_id)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  /// Pushes location updates to subscribers of a particular reference, invoking all
  /// callbacks registered for the reference by GetLocationsAsync calls. This method
  /// also increments the reference's location version counter.
  ///
  /// \param[in] it The reference iterator for the object.
  void PushToLocationSubscribers(ReferenceTable::iterator it)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  /// Clean up borrowers and references when the reference is removed from borrowers.
  /// It should be used as a WaitForRefRemoved callback.
  void CleanupBorrowersOnRefRemoved(const ReferenceTable &new_borrower_refs,
                                    const ObjectID &object_id,
                                    rpc::WorkerAddress borrower_addr)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  /// Address of our RPC server. This is used to determine whether we own a
  /// given object or not, by comparing our WorkerID with the WorkerID of the
  /// object's owner.
  rpc::WorkerAddress rpc_address_;

  /// Feature flag for distributed ref counting. If this is false, then we will
  /// keep the distributed ref count, but only the local ref count will be used
  /// to decide when objects can be evicted.
  const bool distributed_ref_counting_enabled_;

  /// Feature flag for lineage pinning. If this is false, then we will keep the
  /// lineage ref count, but this will not be used to decide when the object's
  /// Reference can be deleted. The object's lineage ref count is the number of
  /// tasks that depend on that object that may be retried in the future.
  const bool lineage_pinning_enabled_;

  /// Factory for producing new core worker clients.
  rpc::ClientFactoryFn client_factory_;

  /// Pool from worker address to core worker client. The owner of an object
  /// uses this client to request a notification from borrowers once the
  /// borrower's ref count for the ID goes to 0.
  rpc::CoreWorkerClientPool borrower_pool_;

  /// Protects access to the reference counting state.
  mutable absl::Mutex mutex_;

  /// Holds all reference counts and dependency information for tracked ObjectIDs.
  ReferenceTable object_id_refs_ GUARDED_BY(mutex_);

  /// Objects whose values have been freed by the language frontend.
  /// The values in plasma will not be pinned. An object ID is
  /// removed from this set once its Reference has been deleted
  /// locally.
  absl::flat_hash_set<ObjectID> freed_objects_ GUARDED_BY(mutex_);

  /// The callback to call once an object ID that we own is no longer in scope
  /// and it has no tasks that depend on it that may be retried in the future.
  /// The object's Reference will be erased after this callback.
  LineageReleasedCallback on_lineage_released_;
  /// Optional shutdown hook to call when all references have gone
  /// out of scope.
  std::function<void()> shutdown_hook_ GUARDED_BY(mutex_) = nullptr;

  /// Object status publisher. It is used to publish the ref removed message for the
  /// reference counting protocol. It is not guarded by a lock because the class itself is
  /// thread-safe.
  std::shared_ptr<pubsub::PublisherInterface> object_status_publisher_;

  /// Object status subscriber. It is used to subscribe the ref removed information from
  /// other workers.
  std::shared_ptr<pubsub::SubscriberInterface> object_status_subscriber_
      GUARDED_BY(mutex_);
};

}  // namespace ray
