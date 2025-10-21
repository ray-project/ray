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

#include <list>
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "absl/base/thread_annotations.h"
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/synchronization/mutex.h"
#include "ray/common/id.h"
#include "ray/common/status.h"
#include "ray/core_worker/lease_policy.h"
#include "ray/core_worker/reference_counter_interface.h"
#include "ray/observability/metric_interface.h"
#include "ray/pubsub/publisher_interface.h"
#include "ray/pubsub/subscriber_interface.h"
#include "ray/rpc/utils.h"
#include "src/ray/protobuf/common.pb.h"

namespace ray {
namespace core {

/// Class used by the core worker to keep track of ObjectID reference counts for garbage
/// collection. This class is thread safe.
class ReferenceCounter : public ReferenceCounterInterface,
                         public LocalityDataProviderInterface {
 public:
  ReferenceCounter(
      rpc::Address rpc_address,
      pubsub::PublisherInterface *object_info_publisher,
      pubsub::SubscriberInterface *object_info_subscriber,
      std::function<bool(const NodeID &node_id)> is_node_dead,
      ray::observability::MetricInterface &owned_object_by_state_counter,
      ray::observability::MetricInterface &owned_object_sizes_by_state_counter,
      bool lineage_pinning_enabled = false)
      : rpc_address_(std::move(rpc_address)),
        lineage_pinning_enabled_(lineage_pinning_enabled),
        object_info_publisher_(object_info_publisher),
        object_info_subscriber_(object_info_subscriber),
        is_node_dead_(std::move(is_node_dead)),
        owned_object_count_by_state_(owned_object_by_state_counter),
        owned_object_sizes_by_state_(owned_object_sizes_by_state_counter) {}

  ~ReferenceCounter() override = default;

  void DrainAndShutdown(std::function<void()> shutdown) override
      ABSL_LOCKS_EXCLUDED(mutex_);

  size_t Size() const override ABSL_LOCKS_EXCLUDED(mutex_);

  bool OwnedByUs(const ObjectID &object_id) const override ABSL_LOCKS_EXCLUDED(mutex_);

  void AddLocalReference(const ObjectID &object_id, const std::string &call_site) override
      ABSL_LOCKS_EXCLUDED(mutex_);

  void RemoveLocalReference(const ObjectID &object_id,
                            std::vector<ObjectID> *deleted) override
      ABSL_LOCKS_EXCLUDED(mutex_);

  void UpdateSubmittedTaskReferences(
      const std::vector<ObjectID> &return_ids,
      const std::vector<ObjectID> &argument_ids_to_add,
      const std::vector<ObjectID> &argument_ids_to_remove = std::vector<ObjectID>(),
      std::vector<ObjectID> *deleted = nullptr) override ABSL_LOCKS_EXCLUDED(mutex_);

  void UpdateResubmittedTaskReferences(const std::vector<ObjectID> &argument_ids) override
      ABSL_LOCKS_EXCLUDED(mutex_);

  void UpdateFinishedTaskReferences(const std::vector<ObjectID> &return_ids,
                                    const std::vector<ObjectID> &argument_ids,
                                    bool release_lineage,
                                    const rpc::Address &worker_addr,
                                    const ReferenceTableProto &borrowed_refs,
                                    std::vector<ObjectID> *deleted) override
      ABSL_LOCKS_EXCLUDED(mutex_);

  void AddOwnedObject(
      const ObjectID &object_id,
      const std::vector<ObjectID> &contained_ids,
      const rpc::Address &owner_address,
      const std::string &call_site,
      const int64_t object_size,
      bool is_reconstructable,
      bool add_local_ref,
      const std::optional<NodeID> &pinned_at_node_id = std::optional<NodeID>(),
      rpc::TensorTransport tensor_transport = rpc::TensorTransport::OBJECT_STORE) override
      ABSL_LOCKS_EXCLUDED(mutex_);

  void AddDynamicReturn(const ObjectID &object_id, const ObjectID &generator_id) override
      ABSL_LOCKS_EXCLUDED(mutex_);

  void OwnDynamicStreamingTaskReturnRef(const ObjectID &object_id,
                                        const ObjectID &generator_id) override
      ABSL_LOCKS_EXCLUDED(mutex_);

  void TryReleaseLocalRefs(const std::vector<ObjectID> &object_ids,
                           std::vector<ObjectID> *deleted) override
      ABSL_LOCKS_EXCLUDED(mutex_);

  bool CheckGeneratorRefsLineageOutOfScope(const ObjectID &generator_id,
                                           int64_t num_objects_generated) override
      ABSL_LOCKS_EXCLUDED(mutex_);

  void UpdateObjectSize(const ObjectID &object_id, int64_t object_size) override
      ABSL_LOCKS_EXCLUDED(mutex_);

  bool AddBorrowedObject(const ObjectID &object_id,
                         const ObjectID &outer_id,
                         const rpc::Address &owner_address,
                         bool foreign_owner_already_monitoring = false) override
      ABSL_LOCKS_EXCLUDED(mutex_);

  bool GetOwner(const ObjectID &object_id,
                rpc::Address *owner_address = nullptr) const override
      ABSL_LOCKS_EXCLUDED(mutex_);

  bool HasOwner(const ObjectID &object_id) const override ABSL_LOCKS_EXCLUDED(mutex_);

  StatusSet<StatusT::NotFound> HasOwner(
      const std::vector<ObjectID> &object_ids) const override ABSL_LOCKS_EXCLUDED(mutex_);

  std::vector<rpc::Address> GetOwnerAddresses(
      const std::vector<ObjectID> &object_ids) const override;

  bool IsPlasmaObjectFreed(const ObjectID &object_id) const override;

  bool TryMarkFreedObjectInUseAgain(const ObjectID &object_id) override;

  void FreePlasmaObjects(const std::vector<ObjectID> &object_ids) override
      ABSL_LOCKS_EXCLUDED(mutex_);

  bool AddObjectOutOfScopeOrFreedCallback(
      const ObjectID &object_id,
      const std::function<void(const ObjectID &)> callback) override
      ABSL_LOCKS_EXCLUDED(mutex_);

  bool AddObjectRefDeletedCallback(
      const ObjectID &object_id, std::function<void(const ObjectID &)> callback) override
      ABSL_LOCKS_EXCLUDED(mutex_);

  void SubscribeRefRemoved(const ObjectID &object_id,
                           const ObjectID &contained_in_id,
                           const rpc::Address &owner_address) override
      ABSL_LOCKS_EXCLUDED(mutex_);

  void SetReleaseLineageCallback(const LineageReleasedCallback &callback) override;

  void PublishRefRemoved(const ObjectID &object_id) override ABSL_LOCKS_EXCLUDED(mutex_);

  size_t NumObjectIDsInScope() const override ABSL_LOCKS_EXCLUDED(mutex_);

  size_t NumObjectsOwnedByUs() const override ABSL_LOCKS_EXCLUDED(mutex_);

  size_t NumActorsOwnedByUs() const override ABSL_LOCKS_EXCLUDED(mutex_);

  void RecordMetrics() override;

  std::unordered_set<ObjectID> GetAllInScopeObjectIDs() const override
      ABSL_LOCKS_EXCLUDED(mutex_);

  std::unordered_map<ObjectID, std::pair<size_t, size_t>> GetAllReferenceCounts()
      const override ABSL_LOCKS_EXCLUDED(mutex_);

  std::string DebugString() const override ABSL_LOCKS_EXCLUDED(mutex_);

  void PopAndClearLocalBorrowers(const std::vector<ObjectID> &borrowed_ids,
                                 ReferenceTableProto *proto,
                                 std::vector<ObjectID> *deleted) override
      ABSL_LOCKS_EXCLUDED(mutex_);

  void AddNestedObjectIds(const ObjectID &object_id,
                          const std::vector<ObjectID> &inner_ids,
                          const rpc::Address &owner_address) override
      ABSL_LOCKS_EXCLUDED(mutex_);

  void UpdateObjectPinnedAtRaylet(const ObjectID &object_id,
                                  const NodeID &node_id) override
      ABSL_LOCKS_EXCLUDED(mutex_);

  bool IsPlasmaObjectPinnedOrSpilled(const ObjectID &object_id,
                                     bool *owned_by_us,
                                     NodeID *pinned_at,
                                     bool *spilled) const override
      ABSL_LOCKS_EXCLUDED(mutex_);

  void ResetObjectsOnRemovedNode(const NodeID &node_id) override;

  std::vector<ObjectID> FlushObjectsToRecover() override;

  bool HasReference(const ObjectID &object_id) const override ABSL_LOCKS_EXCLUDED(mutex_);

  void AddObjectRefStats(
      const absl::flat_hash_map<ObjectID, std::pair<int64_t, std::string>>
          &pinned_objects,
      rpc::CoreWorkerStats *stats,
      const int64_t limit) const override ABSL_LOCKS_EXCLUDED(mutex_);

  bool AddObjectLocation(const ObjectID &object_id, const NodeID &node_id) override
      ABSL_LOCKS_EXCLUDED(mutex_);

  bool RemoveObjectLocation(const ObjectID &object_id, const NodeID &node_id) override
      ABSL_LOCKS_EXCLUDED(mutex_);

  std::optional<absl::flat_hash_set<NodeID>> GetObjectLocations(
      const ObjectID &object_id) override ABSL_LOCKS_EXCLUDED(mutex_);

  void PublishObjectLocationSnapshot(const ObjectID &object_id) override
      ABSL_LOCKS_EXCLUDED(mutex_);

  void FillObjectInformation(const ObjectID &object_id,
                             rpc::WorkerObjectLocationsPubMessage *object_info) override
      ABSL_LOCKS_EXCLUDED(mutex_);

  bool HandleObjectSpilled(const ObjectID &object_id,
                           const std::string &spilled_url,
                           const NodeID &spilled_node_id) override;

  std::optional<LocalityData> GetLocalityData(const ObjectID &object_id) const override;

  bool ReportLocalityData(const ObjectID &object_id,
                          const absl::flat_hash_set<NodeID> &locations,
                          uint64_t object_size) override;

  void AddBorrowerAddress(const ObjectID &object_id,
                          const rpc::Address &borrower_address) override
      ABSL_LOCKS_EXCLUDED(mutex_);

  bool IsObjectReconstructable(const ObjectID &object_id,
                               bool *lineage_evicted) const override;

  int64_t EvictLineage(int64_t min_bytes_to_evict) override;

  void UpdateObjectPendingCreation(const ObjectID &object_id,
                                   bool pending_creation) override;

  bool IsObjectPendingCreation(const ObjectID &object_id) const override;

  void ReleaseAllLocalReferences() override;

  std::optional<rpc::TensorTransport> GetTensorTransport(
      const ObjectID &object_id) const override;

 private:
  /// Contains information related to nested object refs only.
  struct NestedReferenceCount {
    /// Object IDs that we own and that contain this object ID.
    /// ObjectIDs are added to this field when we discover that this object
    /// contains other IDs. This can happen in 2 cases:
    ///  1. We call ray.put() and store the inner ID(s) in the outer object.
    ///  2. A task that we submitted returned an ID(s).
    /// ObjectIDs are erased from this field when their Reference is deleted.
    absl::flat_hash_set<ObjectID> contained_in_owned;
    /// Object IDs that we borrowed and that contain this object ID.
    /// ObjectIDs are added to this field when we get the value of an ObjectRef
    /// (either by deserializing the object or receiving the GetObjectStatus
    /// reply for inlined objects) and it contains another ObjectRef.
    absl::flat_hash_set<ObjectID> contained_in_borrowed_ids;
    /// Reverse pointer for contained_in_owned and contained_in_borrowed_ids.
    /// The object IDs contained in this object. These could be objects that we
    /// own or are borrowing. This field is updated in 2 cases:
    ///  1. We call ray.put() on this ID and store the contained IDs.
    ///  2. We call ray.get() on an ID whose contents we do not know and we
    ///     discover that it contains these IDs.
    absl::flat_hash_set<ObjectID> contains;
  };

  /// Contains information related to borrowing only.
  struct BorrowInfo {
    /// When a process that is borrowing an object ID stores the ID inside the
    /// return value of a task that it executes, the caller of the task is also
    /// considered a borrower for as long as its reference to the task's return
    /// ID stays in scope. Thus, the borrower must notify the owner that the
    /// task's caller is also a borrower. The key is the task's return ID, and
    /// the value is the task ID and address of the task's caller.
    absl::flat_hash_map<ObjectID, rpc::Address> stored_in_objects;
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
    absl::flat_hash_set<rpc::Address> borrowers;
  };

  struct Reference {
    /// Constructor for a reference whose origin is unknown.
    Reference() = default;
    Reference(std::string call_site, int64_t object_size)
        : call_site_(std::move(call_site)), object_size_(object_size) {}
    /// Constructor for a reference that we created.
    Reference(rpc::Address owner_address,
              std::string call_site,
              int64_t object_size,
              bool is_reconstructable,
              std::optional<NodeID> pinned_at_node_id,
              rpc::TensorTransport tensor_transport)
        : call_site_(std::move(call_site)),
          object_size_(object_size),
          owner_address_(std::move(owner_address)),
          pinned_at_node_id_(std::move(pinned_at_node_id)),
          tensor_transport_(tensor_transport),
          owned_by_us_(true),
          is_reconstructable_(is_reconstructable),
          pending_creation_(!pinned_at_node_id_.has_value()) {}

    /// Constructor from a protobuf. This is assumed to be a message from
    /// another process, so the object defaults to not being owned by us.
    static Reference FromProto(const rpc::ObjectReferenceCount &ref_count);
    /// Serialize to a protobuf.
    /// When `deduct_local_ref` is true, one local ref should be removed
    /// when determining if the object has actual local references.
    void ToProto(rpc::ObjectReferenceCount *ref, bool deduct_local_ref = false) const;

    /// The reference count. This number includes:
    /// - Python references to the ObjectID.
    /// - Pending submitted tasks that depend on the object.
    /// - ObjectIDs containing this ObjectID that we own and that are still in
    /// scope.
    size_t RefCount() const {
      return local_ref_count + submitted_task_ref_count +
             nested().contained_in_owned.size();
    }

    /// Whether this reference is no longer in scope. A reference is in scope
    /// if any of the following are true:
    /// - The reference is still being used by this process.
    /// - The reference was contained in another ID that we were borrowing, and
    ///   we haven't told the process that gave us that ID yet.
    /// - We gave the reference to at least one other process.
    bool OutOfScope(bool lineage_pinning_enabled) const {
      bool in_scope = RefCount() > 0;
      bool is_nested = !nested().contained_in_borrowed_ids.empty();
      bool has_borrowers = !borrow().borrowers.empty();
      bool was_stored_in_objects = !borrow().stored_in_objects.empty();

      bool has_lineage_references = false;
      if (lineage_pinning_enabled && owned_by_us_ && !is_reconstructable_) {
        has_lineage_references = lineage_ref_count > 0;
      }

      return !(in_scope || is_nested || has_nested_refs_to_report || has_borrowers ||
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

    /// Access BorrowInfo without modifications.
    /// Returns the default value of the struct if it is not set.
    const BorrowInfo &borrow() const {
      if (borrow_info == nullptr) {
        static const BorrowInfo default_info;
        return default_info;
      }
      return *borrow_info;
    }

    /// Returns the borrow info for updates.
    /// Creates the underlying field if it is not set.
    BorrowInfo *mutable_borrow() {
      if (borrow_info == nullptr) {
        borrow_info = std::make_unique<BorrowInfo>();
      }
      return borrow_info.get();
    }

    /// Access NestedReferenceCount without modifications.
    /// Returns the default value of the struct if it is not set.
    const NestedReferenceCount &nested() const {
      if (nested_reference_count == nullptr) {
        static const NestedReferenceCount default_refs;
        return default_refs;
      }
      return *nested_reference_count;
    }

    /// Returns the containing references for updates.
    /// Creates the underlying field if it is not set.
    NestedReferenceCount *mutable_nested() {
      if (nested_reference_count == nullptr) {
        nested_reference_count = std::make_unique<NestedReferenceCount>();
      }
      return nested_reference_count.get();
    }

    std::string DebugString() const;

    /// Description of the call site where the reference was created.
    std::string call_site_ = "<unknown>";
    /// Object size if known, otherwise -1;
    int64_t object_size_ = -1;
    /// If this object is owned by us and stored in plasma, this contains all
    /// object locations.
    absl::flat_hash_set<NodeID> locations;
    /// The object's owner's address, if we know it. If this process is the
    /// owner, then this is added during creation of the Reference. If this is
    /// process is a borrower, the borrower must add the owner's address before
    /// using the ObjectID.
    std::optional<rpc::Address> owner_address_;
    /// If this object is owned by us and stored in plasma, and reference
    /// counting is enabled, then some raylet must be pinning the object value.
    /// This is the address of that raylet.
    std::optional<NodeID> pinned_at_node_id_;
    /// TODO(kevin85421): Make tensor_transport a required field for all constructors.
    ///
    /// The transport used for the object.
    rpc::TensorTransport tensor_transport_ = rpc::TensorTransport::OBJECT_STORE;
    /// Whether we own the object. If we own the object, then we are
    /// responsible for tracking the state of the task that creates the object
    /// (see task_manager.h).
    bool owned_by_us_ = false;

    // Whether this object can be reconstructed via lineage. If false, then the
    // object's value will be pinned as long as it is referenced by any other
    // object's lineage. This should be set to false if the object was created
    // by ray.put(), a task that cannot be retried, or its lineage was evicted.
    bool is_reconstructable_ = false;
    /// Whether the lineage of this object was evicted due to memory pressure.
    bool lineage_evicted = false;
    /// The number of tasks that depend on this object that may be retried in
    /// the future (pending execution or finished but retryable). If the object
    /// is inlined (not stored in plasma), then its lineage ref count is 0
    /// because any dependent task will already have the value of the object.
    size_t lineage_ref_count = 0;

    /// The local ref count for the ObjectID in the language frontend.
    size_t local_ref_count = 0;
    /// The ref count for submitted tasks that depend on the ObjectID.
    size_t submitted_task_ref_count = 0;

    /// Metadata related to nesting, including references that contain this
    /// reference, and references contained by this reference.
    std::unique_ptr<NestedReferenceCount> nested_reference_count;

    /// Metadata related to borrowing.
    std::unique_ptr<BorrowInfo> borrow_info;

    /// Callback that will be called when this object
    /// is out of scope or manually freed.
    /// Note: when an object is out of scope, it can still
    /// have lineage ref count and the callbacks in object_ref_deleted_callbacks
    /// will be called when lineage ref count is also 0.
    std::vector<std::function<void(const ObjectID &)>>
        on_object_out_of_scope_or_freed_callbacks;
    /// Callbacks that will be called when the object ref is deleted
    /// from the reference table (all refs including lineage ref count go to 0).
    std::vector<std::function<void(const ObjectID &)>> object_ref_deleted_callbacks;
    /// If this is set, we'll call PublishRefRemovedInternal when this process is no
    /// longer a borrower (RefCount() == 0).
    bool publish_ref_removed = false;

    /// For objects that have been spilled to external storage, the URL from which
    /// they can be retrieved.
    std::string spilled_url;
    /// The ID of the node that spilled the object.
    /// This will be Nil if the object has not been spilled or if it is spilled
    /// distributed external storage.
    NodeID spilled_node_id = NodeID::Nil();
    /// Whether this object has been spilled to external storage.
    bool spilled = false;

    /// Whether the object was created with a foreign owner (i.e., _owner set).
    /// In this case, the owner is already monitoring this reference with a
    /// WaitForRefRemoved() call, and it is an error to return borrower
    /// metadata to the parent of the current task.
    /// See https://github.com/ray-project/ray/pull/19910 for more context.
    bool foreign_owner_already_monitoring = false;

    /// ObjectRefs nested in this object that are or were in use. These objects
    /// are not owned by us, and we need to report that we are borrowing them
    /// to their owner. Nesting is transitive, so this flag is set as long as
    /// any child object is in scope.
    bool has_nested_refs_to_report = false;

    /// Whether the task that creates this object is scheduled/executing.
    bool pending_creation_ = false;

    /// Whether or not this object was spilled.
    bool did_spill = false;
  };

  using ReferenceTable = absl::flat_hash_map<ObjectID, Reference>;
  using ReferenceProtoTable = absl::flat_hash_map<ObjectID, rpc::ObjectReferenceCount>;

  bool AddOwnedObjectInternal(
      const ObjectID &object_id,
      const std::vector<ObjectID> &contained_ids,
      const rpc::Address &owner_address,
      const std::string &call_site,
      const int64_t object_size,
      bool is_reconstructable,
      bool add_local_ref,
      const std::optional<NodeID> &pinned_at_node_id,
      rpc::TensorTransport tensor_transport = rpc::TensorTransport::OBJECT_STORE)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  void SetNestedRefInUseRecursive(ReferenceTable::iterator inner_ref_it)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  bool GetOwnerInternal(const ObjectID &object_id,
                        rpc::Address *owner_address = nullptr) const
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  /// Unsets the raylet address
  /// that the object was pinned at or spilled at, if the address was set.
  void UnsetObjectPrimaryCopy(ReferenceTable::iterator it)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  /// This should be called whenever the object is out of scope or manually freed.
  void OnObjectOutOfScopeOrFreed(ReferenceTable::iterator it)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  /// Shutdown if all references have gone out of scope and shutdown
  /// is scheduled.
  void ShutdownIfNeeded() ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  /// Deserialize a ReferenceTable.
  static ReferenceTable ReferenceTableFromProto(const ReferenceTableProto &proto);

  /// Packs an object ID to ObjectReferenceCount map, into an array of
  /// ObjectReferenceCount. Consumes the input proto table.
  static void ReferenceTableToProto(ReferenceProtoTable &table,
                                    ReferenceTableProto *proto);

  /// Remove references for the provided object IDs that correspond to them
  /// being dependencies to a submitted task. This should be called when
  /// inlined dependencies are inlined or when the task finishes for plasma
  /// dependencies.
  void RemoveSubmittedTaskReferences(const std::vector<ObjectID> &argument_ids,
                                     bool release_lineage,
                                     std::vector<ObjectID> *deleted)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_);

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
                                  const rpc::Address &owner_address)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  /// Populates the table with the ObjectID that we were or are still
  /// borrowing. The table also includes any IDs that we discovered were
  /// contained in the ID. For each borrowed ID, we will return in proto:
  /// - The borrowed ID's owner's address.
  /// - Whether we are still using the ID or not:
  ///     RefCount() > 1 when deduct_local_ref, and RefCount() > 0 when not.
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
                                         bool for_ref_removed,
                                         bool deduct_local_ref,
                                         ReferenceProtoTable *borrowed_refs)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_);

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
                            const rpc::Address &worker_addr,
                            const ReferenceTable &borrowed_refs)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  /// Wait for a borrower to stop using its reference. This should only be
  /// called by the owner of the ID.
  /// \param[in] reference_it Iterator pointing to the reference that we own.
  /// \param[in] addr The address of the borrower.
  /// \param[in] contained_in_id Whether the owned ID was contained in another
  /// ID. This is used in cases where we return an object ID that we own inside
  /// an object that we do not own. Then, we must notify the owner of the outer
  /// object that they are borrowing the inner.
  void WaitForRefRemoved(const ReferenceTable::iterator &reference_it,
                         const rpc::Address &addr,
                         const ObjectID &contained_in_id = ObjectID::Nil())
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  /// Helper method to add an object that we are borrowing. This is used when
  /// deserializing IDs from a task's arguments, or when deserializing an ID
  /// during ray.get().
  ///
  /// \param[in] foreign_owner_already_monitoring Whether to set the bit that an
  ///            externally assigned owner is monitoring the lifetime of this
  ///            object. This is the case for `ray.put(..., _owner=ZZZ)`.
  bool AddBorrowedObjectInternal(const ObjectID &object_id,
                                 const ObjectID &outer_id,
                                 const rpc::Address &owner_address,
                                 bool foreign_owner_already_monitoring)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  /// Helper method to delete an entry from the reference map and run any necessary
  /// callbacks. Assumes that the entry is in object_id_refs_ and invalidates the
  /// iterator.
  void DeleteReferenceInternal(ReferenceTable::iterator entry,
                               std::vector<ObjectID> *deleted)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  /// To respond to the object's owner once we are no longer borrowing it.  The
  /// sender is the owner of the object ID. We will send the reply when our
  /// RefCount() for the object ID goes to 0.
  void PublishRefRemovedInternal(const ObjectID &object_id)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  /// Erase the Reference from the table. Assumes that the entry has no more
  /// references, normal or lineage.
  void EraseReference(ReferenceTable::iterator entry)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  /// Helper method to garbage-collect all out-of-scope References in the
  /// lineage for this object.
  int64_t ReleaseLineageReferences(ReferenceTable::iterator entry)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  /// Add a new location for the given object. The owner must have the object ref in
  /// scope, and the caller must have already acquired mutex_.
  ///
  /// \param[in] it The reference iterator for the object.
  /// \param[in] node_id The new object location to be added.
  void AddObjectLocationInternal(ReferenceTable::iterator it, const NodeID &node_id)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  /// Remove a location for the given object. The owner must have the object ref in
  /// scope, and the caller must have already acquired mutex_.
  ///
  /// \param[in] it The reference iterator for the object.
  /// \param[in] node_id The object location to be removed.
  void RemoveObjectLocationInternal(ReferenceTable::iterator it, const NodeID &node_id)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  void UpdateObjectPendingCreationInternal(const ObjectID &object_id,
                                           bool pending_creation)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  /// Update the owned object counters when a reference state changes.
  /// \param object_id The object ID of the reference.
  /// \param ref The reference whose state is changing.
  /// \param decrement If true, decrement the counters for the current state.
  /// If false, increment the counters for the current state.
  void UpdateOwnedObjectCounters(const ObjectID &object_id,
                                 const Reference &ref,
                                 bool decrement) ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  /// Publish object locations to all subscribers.
  ///
  /// \param[in] it The reference iterator for the object.
  void PushToLocationSubscribers(ReferenceTable::iterator it)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  /// Fill up the object information for the given iterator.
  void FillObjectInformationInternal(ReferenceTable::iterator it,
                                     rpc::WorkerObjectLocationsPubMessage *object_info)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  /// Clean up borrowers and references when the reference is removed from borrowers.
  /// It should be used as a WaitForRefRemoved callback.
  void CleanupBorrowersOnRefRemoved(const ReferenceTable &new_borrower_refs,
                                    const ObjectID &object_id,
                                    const rpc::Address &borrower_addr);

  /// Decrease the local reference count for the ObjectID by one.
  /// This method is internal and not thread-safe. mutex_ lock must be held before
  /// calling this method.
  void RemoveLocalReferenceInternal(const ObjectID &object_id,
                                    std::vector<ObjectID> *deleted)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  /// Address of our RPC server. This is used to determine whether we own a
  /// given object or not, by comparing our WorkerID with the WorkerID of the
  /// object's owner.
  rpc::Address rpc_address_;

  /// Feature flag for lineage pinning. If this is false, then we will keep the
  /// lineage ref count, but this will not be used to decide when the object's
  /// Reference can be deleted. The object's lineage ref count is the number of
  /// tasks that depend on that object that may be retried in the future.
  const bool lineage_pinning_enabled_;

  /// Protects access to the reference counting state.
  mutable absl::Mutex mutex_;

  /// Holds all reference counts and dependency information for tracked ObjectIDs.
  ReferenceTable object_id_refs_ ABSL_GUARDED_BY(mutex_);

  /// Objects whose values have been freed by the language frontend.
  /// The values in plasma will not be pinned. An object ID is
  /// removed from this set once its Reference has been deleted
  /// locally.
  absl::flat_hash_set<ObjectID> freed_objects_ ABSL_GUARDED_BY(mutex_);

  /// The callback to call once an object ID that we own is no longer in scope
  /// and it has no tasks that depend on it that may be retried in the future.
  /// The object's Reference will be erased after this callback.
  // Returns the amount of lineage in bytes released.
  LineageReleasedCallback on_lineage_released_;
  /// Optional shutdown hook to call when all references have gone
  /// out of scope.
  std::function<void()> shutdown_hook_ ABSL_GUARDED_BY(mutex_) = nullptr;

  /// Object status publisher. It is used to publish the ref removed message for the
  /// reference counting protocol. It is not guarded by a lock because the class itself is
  /// thread-safe.
  pubsub::PublisherInterface *object_info_publisher_;

  /// Object status subscriber. It is used to subscribe the ref removed information from
  /// other workers.
  pubsub::SubscriberInterface *object_info_subscriber_;

  /// Objects that we own that are still in scope at the application level and
  /// that may be reconstructed. These objects may have pinned lineage that
  /// should be evicted on memory pressure. The queue is in FIFO order, based
  /// on ObjectRef creation time.
  std::list<ObjectID> reconstructable_owned_objects_ ABSL_GUARDED_BY(mutex_);

  /// We keep a FIFO queue of objects in scope so that we can choose lineage to
  /// evict under memory pressure. This is an index from ObjectID to the
  /// object's place in the queue.
  absl::flat_hash_map<ObjectID, std::list<ObjectID>::iterator>
      reconstructable_owned_objects_index_ ABSL_GUARDED_BY(mutex_);

  /// Called to check whether a raylet died. This is used when adding
  /// the primary or spilled location of an object. If the node died, then
  /// the object will be added to the buffer objects to recover.
  const std::function<bool(const NodeID &node_id)> is_node_dead_;

  /// A buffer of the objects whose primary or spilled locations have been lost
  /// due to node failure. These objects are still in scope and need to be
  /// recovered.
  std::vector<ObjectID> objects_to_recover_ ABSL_GUARDED_BY(mutex_);

  /// Keep track of objects owend by this worker.
  size_t num_objects_owned_by_us_ ABSL_GUARDED_BY(mutex_) = 0;

  /// Keep track of actors owend by this worker.
  size_t num_actors_owned_by_us_ ABSL_GUARDED_BY(mutex_) = 0;

  /// Track counts of owned objects by state.
  /// These are atomic to allow lock-free reads via public getters.
  std::atomic<size_t> owned_objects_pending_creation_{0};
  std::atomic<size_t> owned_objects_in_memory_{0};
  std::atomic<size_t> owned_objects_spilled_{0};
  std::atomic<size_t> owned_objects_in_plasma_{0};

  /// Track sizes of owned objects by state.
  /// These are atomic to allow lock-free reads via public getters.
  std::atomic<int64_t> owned_objects_size_in_memory_{0};
  std::atomic<int64_t> owned_objects_size_spilled_{0};
  std::atomic<int64_t> owned_objects_size_in_plasma_{0};

  ray::observability::MetricInterface &owned_object_count_by_state_;
  ray::observability::MetricInterface &owned_object_sizes_by_state_;
};

}  // namespace core
}  // namespace ray
