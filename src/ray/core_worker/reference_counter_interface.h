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
// Type definitions used by ReferenceCounterInterface

#pragma once

#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "ray/common/id.h"
#include "ray/core_worker/lease_policy.h"
#include "ray/pubsub/publisher_interface.h"
#include "ray/pubsub/subscriber_interface.h"
#include "ray/rpc/utils.h"

namespace ray {
namespace core {

class ReferenceCounterInterface {
 protected:
  // Returns the amount of lineage in bytes released.
  using LineageReleasedCallback =
      std::function<int64_t(const ObjectID &, std::vector<ObjectID> *)>;

 public:
  using ReferenceTableProto =
      ::google::protobuf::RepeatedPtrField<rpc::ObjectReferenceCount>;

  /// Wait for all object references to go out of scope, and then shutdown.
  ///
  /// \param shutdown The shutdown callback to call.
  virtual void DrainAndShutdown(std::function<void()> shutdown) = 0;

  /// Return the size of the reference count table
  /// (i.e. the number of objects that have references).
  virtual size_t Size() const = 0;

  /// Return true if the object is owned by us.
  virtual bool OwnedByUs(const ObjectID &object_id) const = 0;

  /// Increase the reference count for the ObjectID by one. If there is no
  /// entry for the ObjectID, one will be created. The object ID will not have
  /// any owner information, since we don't know how it was created.
  ///
  /// \param[in] object_id The object to to increment the count for.
  virtual void AddLocalReference(const ObjectID &object_id,
                                 const std::string &call_site) = 0;

  /// Decrease the local reference count for the ObjectID by one.
  ///
  /// \param[in] object_id The object to decrement the count for.
  /// \param[out] deleted List to store objects that hit zero ref count.
  virtual void RemoveLocalReference(const ObjectID &object_id,
                                    std::vector<ObjectID> *deleted) = 0;

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
  virtual void UpdateSubmittedTaskReferences(
      const std::vector<ObjectID> &return_ids,
      const std::vector<ObjectID> &argument_ids_to_add,
      const std::vector<ObjectID> &argument_ids_to_remove = std::vector<ObjectID>(),
      std::vector<ObjectID> *deleted = nullptr) = 0;

  /// Add references for the object dependencies of a resubmitted task. This
  /// does not increment the arguments' lineage ref counts because we should
  /// have already incremented them when the task was first submitted.
  ///
  /// \param[in] argument_ids The arguments of the task to add references for.
  virtual void UpdateResubmittedTaskReferences(
      const std::vector<ObjectID> &argument_ids) = 0;

  /// Update object references that were given to a submitted task. The task
  /// may still be borrowing any object IDs that were contained in its
  /// arguments. This should be called when the task finishes.
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
  virtual void UpdateFinishedTaskReferences(
      const std::vector<ObjectID> &return_ids,
      const std::vector<ObjectID> &argument_ids,
      bool release_lineage,
      const rpc::Address &worker_addr,
      const ::google::protobuf::RepeatedPtrField<rpc::ObjectReferenceCount>
          &borrowed_refs,
      std::vector<ObjectID> *deleted) = 0;

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
  /// \param[in] add_local_ref Whether to initialize the local ref count to 1.
  /// This is used to ensure that the ref is considered in scope before the
  /// corresponding ObjectRef has been returned to the language frontend.
  /// \param[in] pinned_at_node_id The primary location for the object, if it
  /// is already known. This is only used for ray.put calls.
  /// \param[in] tensor_transport The transport used for the object.
  virtual void AddOwnedObject(
      const ObjectID &object_id,
      const std::vector<ObjectID> &contained_ids,
      const rpc::Address &owner_address,
      const std::string &call_site,
      const int64_t object_size,
      bool is_reconstructable,
      bool add_local_ref,
      const std::optional<NodeID> &pinned_at_node_id = std::optional<NodeID>(),
      rpc::TensorTransport tensor_transport = rpc::TensorTransport::OBJECT_STORE) = 0;

  /// Add an owned object that was dynamically created. These are objects that
  /// were created by a task that we called, but that we own.
  ///
  /// \param[in] object_id The ID of the object that we now own.
  /// \param[in] generator_id The ID of the object that wraps the dynamically
  /// created object ref. This should be an object that we own, and we will
  /// update its ref count info to show that it contains the dynamically
  /// created ObjectID.
  virtual void AddDynamicReturn(const ObjectID &object_id,
                                const ObjectID &generator_id) = 0;

  /// Own an object that the current owner (current process) dynamically created.
  ///
  /// The API is idempotent.
  ///
  /// TODO(sang): This API should be merged with AddDynamicReturn when
  /// we turn on streaming generator by default.
  ///
  /// For normal task return, the owner creates and owns the references before
  /// the object values are created. However, when you dynamically create objects,
  /// the owner doesn't know (i.e., own) the references until it is reported from
  /// the executor side.
  ///
  /// This API is used to own this type of dynamically generated references.
  /// The executor should ensure the objects are not GC'ed until the owner
  /// registers the dynamically created references by this API.
  ///
  /// \param[in] object_id The ID of the object that we now own.
  /// \param[in] generator_id The Object ID of the streaming generator task.
  virtual void OwnDynamicStreamingTaskReturnRef(const ObjectID &object_id,
                                                const ObjectID &generator_id) = 0;

  /// Try to decrement the local ref count for the given objects, if they are
  /// still in scope.
  ///
  /// \param[in] object_ids The object refs to decrement the count for, if they
  /// are in scope.
  /// \param[out] deleted Any released object refs that went out of scope. The
  /// object values should be deleted.
  virtual void TryReleaseLocalRefs(const std::vector<ObjectID> &object_ids,
                                   std::vector<ObjectID> *deleted) = 0;

  /// Check if a generator's lineage has gone out of scope. This checks if we
  /// still have entries for the generator ref and all refs returned by the
  /// generator, including the sentinel EOF object. If true, then the lineage
  /// (task and stream metadata) is safe to remove.
  ///
  /// \param[in] generator_id The generator ID.
  /// \param[in] num_objects_generated The total number of objects generated by
  /// the streaming generator task, including the EOF object.
  /// \return true if the generators' returned refs have gone out of scope.
  virtual bool CheckGeneratorRefsLineageOutOfScope(const ObjectID &generator_id,
                                                   int64_t num_objects_generated) = 0;

  /// Update the size of the object.
  ///
  /// \param[in] object_id The ID of the object.
  /// \param[in] size The known size of the object.
  virtual void UpdateObjectSize(const ObjectID &object_id, int64_t object_size) = 0;

  /// Add an object that we are borrowing.
  ///
  /// \param[in] object_id The ID of the object that we are borrowing.
  /// \param[in] outer_id The ID of the object that contained this object ID,
  /// if one exists. An outer_id may not exist if object_id was inlined
  /// directly in a task spec, or if it was passed in the application
  /// out-of-band.
  /// \param[in] owner_address The owner's address.
  virtual bool AddBorrowedObject(const ObjectID &object_id,
                                 const ObjectID &outer_id,
                                 const rpc::Address &owner_address,
                                 bool foreign_owner_already_monitoring = false) = 0;

  /// Get the owner address of the given object.
  ///
  /// Use `HasOwner` instead if the caller doesn't need to use owner_address for
  /// performance.
  ///
  /// \param[in] object_id The ID of the object to look up.
  /// \param[out] owner_address The address of the object owner.
  /// \return false if the object is out of scope or we do not yet have
  /// ownership information. The latter can happen when object IDs are passed
  /// out of band.
  virtual bool GetOwner(const ObjectID &object_id,
                        rpc::Address *owner_address = nullptr) const = 0;

  /// Check if the object has an owner.
  ///
  /// \param[in] object_id The ID of the object.
  /// \return if the object has an owner.
  virtual bool HasOwner(const ObjectID &object_id) const = 0;

  //// Checks to see if objects have an owner.
  ///
  /// \param[in] object_ids The IDs of the objects.
  /// \return StatusT::OK if all objects have owners.
  /// \return StatusT::NotFound if any object does not have an owner. The error message
  /// contains objects without owners.
  virtual StatusSet<StatusT::NotFound> HasOwner(
      const std::vector<ObjectID> &object_ids) const = 0;

  /// Get the owner addresses of the given objects. The owner address
  /// must be registered for these objects.
  ///
  /// \param[in] object_ids The IDs of the object to look up.
  /// \return The addresses of the objects' owners.
  virtual std::vector<rpc::Address> GetOwnerAddresses(
      const std::vector<ObjectID> &object_ids) const = 0;

  /// Check whether an object value has been freed.
  ///
  /// \param[in] object_id The object to check.
  /// \return Whether the object value has been freed.
  virtual bool IsPlasmaObjectFreed(const ObjectID &object_id) const = 0;

  /// Mark an object that was freed as being in use again. If another copy of
  /// the object is subsequently pinned, we will not delete it until free is
  /// called again, or the ObjectRef goes out of scope.
  ///
  /// \param[in] object_id The object to un-free.
  /// \return Whether it was successful. This call will fail if the object ref
  /// is no longer in scope or if the object was not actually freed.
  virtual bool TryMarkFreedObjectInUseAgain(const ObjectID &object_id) = 0;

  /// Release the underlying value from plasma (if any) for these objects.
  ///
  /// \param[in] object_ids The IDs whose values to free.
  virtual void FreePlasmaObjects(const std::vector<ObjectID> &object_ids) = 0;

  /// Adds the callback that will be run when the object goes out of scope
  /// (Reference.OutOfScope() returns true).
  /// Returns true if the object was in scope and the callback was added, else false.
  virtual bool AddObjectOutOfScopeOrFreedCallback(
      const ObjectID &object_id,
      const std::function<void(const ObjectID &)> callback) = 0;

  /// Stores the callback that will be run when the object reference is deleted
  /// from the reference table (all refs including lineage ref count go to 0).
  /// There could be multiple callbacks for the same object due to retries and we store
  /// them all to prevent the message reordering case where an earlier callback overwrites
  /// the later one.
  /// Returns true if the object was in the reference table and the callback was added
  /// else false.
  virtual bool AddObjectRefDeletedCallback(
      const ObjectID &object_id, std::function<void(const ObjectID &)> callback) = 0;

  /// So we call PublishRefRemovedInternal when we are no longer borrowing this object
  /// (when our ref count goes to 0).
  ///
  /// \param[in] object_id The object ID to set the callback for.
  /// \param[in] contained_in_id The object ID that contains object_id, if any.
  /// This is used for cases when object_id was returned from a task that we
  /// submitted. Then, as long as we have contained_in_id in scope, we are
  /// borrowing object_id.
  /// \param[in] owner_address The owner of object_id's address.
  virtual void SubscribeRefRemoved(const ObjectID &object_id,
                                   const ObjectID &contained_in_id,
                                   const rpc::Address &owner_address) = 0;

  /// Set a callback to call whenever a Reference that we own is deleted. A
  /// Reference can only be deleted if:
  /// 1. The ObjectID's ref count is 0 on all workers.
  /// 2. There are no tasks that depend on the object that may be retried in
  /// the future.
  ///
  /// \param[in] callback The callback to call.
  virtual void SetReleaseLineageCallback(const LineageReleasedCallback &callback) = 0;

  /// Just calls PublishRefRemovedInternal with a lock.
  virtual void PublishRefRemoved(const ObjectID &object_id) = 0;

  /// Returns the total number of ObjectIDs currently in scope.
  virtual size_t NumObjectIDsInScope() const = 0;

  /// Returns the total number of objects owned by this worker.
  virtual size_t NumObjectsOwnedByUs() const = 0;

  /// Returns the total number of actors owned by this worker.
  virtual size_t NumActorsOwnedByUs() const = 0;

  /// Reports observability metrics to underlying monitoring system
  virtual void RecordMetrics() = 0;

  /// Returns a set of all ObjectIDs currently in scope (i.e., nonzero reference count).
  virtual std::unordered_set<ObjectID> GetAllInScopeObjectIDs() const = 0;

  /// Returns a map of all ObjectIDs currently in scope with a pair of their
  /// (local, submitted_task) reference counts. For debugging purposes.
  virtual std::unordered_map<ObjectID, std::pair<size_t, size_t>> GetAllReferenceCounts()
      const = 0;

  virtual std::string DebugString() const = 0;

  /// Populate a table with ObjectIDs that we were or are still borrowing.
  /// This should be called when a task returns, and the argument should be any
  /// IDs that were passed by reference in the task spec or that were
  /// serialized in inlined arguments.
  ///
  /// NOTE(swang): Task arguments should be pinned with a fake local reference
  /// during task execution. This method removes the fake references so that
  /// the reference deletion is atomic with removing the ref count information.
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
  virtual void PopAndClearLocalBorrowers(
      const std::vector<ObjectID> &borrowed_ids,
      ::google::protobuf::RepeatedPtrField<rpc::ObjectReferenceCount> *proto,
      std::vector<ObjectID> *deleted) = 0;

  /// Mark that this ObjectID contains another ObjectID(s). This should be
  /// called in two cases:
  /// 1. We are storing the value of an object and the value contains
  /// serialized copies of other ObjectIDs. If the outer object is owned by a
  /// remote process, then they are now a borrower of the nested IDs.
  /// 2. We submitted a task that returned an ObjectID(s) in its return values
  /// and we are processing the worker's reply. In this case, we own the task's
  /// return objects and are borrowing the nested IDs.
  ///
  /// This method is idempotent.
  ///
  /// \param[in] object_id The ID of the object that contains other ObjectIDs.
  /// \param[in] inner_ids The object IDs are nested in object_id's value.
  /// \param[in] owner_address The owner address of the outer object_id. If
  /// this is not provided, then the outer object ID must be owned by us. the
  /// outer object ID is not owned by us, then this is used to contact the
  /// outer object's owner, since it is considered a borrower for the inner
  /// IDs.
  virtual void AddNestedObjectIds(const ObjectID &object_id,
                                  const std::vector<ObjectID> &inner_ids,
                                  const rpc::Address &owner_address) = 0;

  /// Update the pinned location of an object stored in plasma.
  ///
  /// \param[in] object_id The object to update.
  /// \param[in] node_id The raylet that is now pinning the object ID.
  virtual void UpdateObjectPinnedAtRaylet(const ObjectID &object_id,
                                          const NodeID &node_id) = 0;

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
  virtual bool IsPlasmaObjectPinnedOrSpilled(const ObjectID &object_id,
                                             bool *owned_by_us,
                                             NodeID *pinned_at,
                                             bool *spilled) const = 0;

  /// Get and reset the objects that were pinned or spilled on the given node.
  /// This method should be called upon a node failure, to trigger
  /// reconstruction for any lost objects that are still in scope.
  ///
  /// If a deletion callback was set for a lost object, it will be invoked and
  /// reset.
  ///
  /// \param[in] node_id The node whose object store has been removed.
  /// \return The set of objects that were pinned on the given node.
  virtual void ResetObjectsOnRemovedNode(const NodeID &node_id) = 0;

  virtual std::vector<ObjectID> FlushObjectsToRecover() = 0;

  /// Whether we have a reference to a particular ObjectID.
  ///
  /// \param[in] object_id The object ID to check for.
  /// \return Whether we have a reference to the object ID.
  virtual bool HasReference(const ObjectID &object_id) const = 0;

  /// Write the current reference table to the given proto.
  ///
  /// \param[out] stats The proto to write references to.
  virtual void AddObjectRefStats(
      const absl::flat_hash_map<ObjectID, std::pair<int64_t, std::string>>
          &pinned_objects,
      rpc::CoreWorkerStats *stats,
      const int64_t limit) const = 0;

  /// Add a new location for the given object. The owner must have the object ref in
  /// scope.
  ///
  /// \param[in] object_id The object to update.
  /// \param[in] node_id The new object location to be added.
  /// \return True if the reference exists, false otherwise.
  virtual bool AddObjectLocation(const ObjectID &object_id, const NodeID &node_id) = 0;

  /// Remove a location for the given object. The owner must have the object ref in
  /// scope.
  ///
  /// \param[in] object_id The object to update.
  /// \param[in] node_id The object location to be removed.
  /// \return True if the reference exists, false otherwise.
  virtual bool RemoveObjectLocation(const ObjectID &object_id, const NodeID &node_id) = 0;

  /// Get the locations of the given object. The owner must have the object ref in
  /// scope.
  ///
  /// \param[in] object_id The object to get locations for.
  /// \return The nodes that have the object if the reference exists, empty optional
  ///         otherwise.
  virtual std::optional<absl::flat_hash_set<NodeID>> GetObjectLocations(
      const ObjectID &object_id) = 0;

  /// Publish the snapshot of the object location for the given object id.
  /// Publish the empty locations if object is already evicted or not owned by this
  /// worker.
  ///
  /// \param[in] object_id The object whose locations we want.
  virtual void PublishObjectLocationSnapshot(const ObjectID &object_id) = 0;

  /// Fill up the object information.
  ///
  /// \param[in] object_id The object id
  /// \param[out] The object information that will be filled by a given object id.
  virtual void FillObjectInformation(
      const ObjectID &object_id, rpc::WorkerObjectLocationsPubMessage *object_info) = 0;

  /// Handle an object has been spilled to external storage.
  ///
  /// This notifies the primary raylet that the object is safe to release and
  /// records the spill URL, spill node ID, and updated object size.
  /// \param[in] object_id The object that has been spilled.
  /// \param[in] spilled_url The URL to which the object has been spilled.
  /// \param[in] spilled_node_id The ID of the node on which the object was spilled.
  /// \return True if the reference exists and is in scope, false otherwise.
  virtual bool HandleObjectSpilled(const ObjectID &object_id,
                                   const std::string &spilled_url,
                                   const NodeID &spilled_node_id) = 0;

  /// Get locality data for object. This is used by the leasing policy to implement
  /// locality-aware leasing.
  ///
  /// \param[in] object_id Object whose locality data we want.
  /// \return Locality data.
  virtual std::optional<LocalityData> GetLocalityData(
      const ObjectID &object_id) const = 0;

  /// Report locality data for object. This is used by the FutureResolver to report
  /// locality data for borrowed refs.
  ///
  /// \param[in] object_id Object whose locality data we're reporting.
  /// \param[in] locations Locations of the object.
  /// \param[in] object_size Size of the object.
  /// \return True if the reference exists, false otherwise.
  virtual bool ReportLocalityData(const ObjectID &object_id,
                                  const absl::flat_hash_set<NodeID> &locations,
                                  uint64_t object_size) = 0;

  /// Add borrower address in owner's worker. This function will add borrower address
  /// to the `object_id_refs_`, then call WaitForRefRemoved() to monitor borrowed
  /// object in borrower's worker.
  ///
  /// \param[in] object_id The ID of Object whose been borrowed.
  /// \param[in] borrower_address The address of borrower.
  virtual void AddBorrowerAddress(const ObjectID &object_id,
                                  const rpc::Address &borrower_address) = 0;

  virtual bool IsObjectReconstructable(const ObjectID &object_id,
                                       bool *lineage_evicted) const = 0;

  /// Evict lineage of objects that are still in scope. This evicts lineage in
  /// FIFO order, based on when the ObjectRef was created.
  ///
  /// \param[in] min_bytes_to_evict The minimum number of bytes to evict.
  virtual int64_t EvictLineage(int64_t min_bytes_to_evict) = 0;

  /// Update whether the object is pending creation.
  virtual void UpdateObjectPendingCreation(const ObjectID &object_id,
                                           bool pending_creation) = 0;

  /// Whether the object is pending creation (the task that creates it is
  /// scheduled/executing).
  virtual bool IsObjectPendingCreation(const ObjectID &object_id) const = 0;

  /// Release all local references which registered on this local.
  virtual void ReleaseAllLocalReferences() = 0;

  /// Get the tensor transport for the given object.
  virtual std::optional<rpc::TensorTransport> GetTensorTransport(
      const ObjectID &object_id) const = 0;

  virtual ~ReferenceCounterInterface() = default;
};

}  // namespace core
}  // namespace ray
