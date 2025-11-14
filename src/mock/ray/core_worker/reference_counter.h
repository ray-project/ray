// Copyright 2024 The Ray Authors.
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
#include "gmock/gmock.h"
#include "ray/core_worker/reference_counter_interface.h"
namespace ray {
namespace core {

class MockReferenceCounter : public ReferenceCounterInterface {
 public:
  MockReferenceCounter() : ReferenceCounterInterface() {}

  MOCK_METHOD1(DrainAndShutdown, void(std::function<void()> shutdown));

  MOCK_CONST_METHOD0(Size, size_t());

  MOCK_CONST_METHOD1(OwnedByUs, bool(const ObjectID &object_id));

  MOCK_METHOD2(AddLocalReference,
               void(const ObjectID &object_id, const std::string &call_site));

  MOCK_METHOD2(RemoveLocalReference,
               void(const ObjectID &object_id, std::vector<ObjectID> *deleted));

  MOCK_METHOD4(UpdateSubmittedTaskReferences,
               void(const std::vector<ObjectID> &return_ids,
                    const std::vector<ObjectID> &argument_ids_to_add,
                    const std::vector<ObjectID> &argument_ids_to_remove,
                    std::vector<ObjectID> *deleted));

  MOCK_METHOD1(UpdateResubmittedTaskReferences,
               void(const std::vector<ObjectID> &argument_ids));

  MOCK_METHOD6(UpdateFinishedTaskReferences,
               void(const std::vector<ObjectID> &return_ids,
                    const std::vector<ObjectID> &argument_ids,
                    bool release_lineage,
                    const rpc::Address &worker_addr,
                    const ::google::protobuf::RepeatedPtrField<rpc::ObjectReferenceCount>
                        &borrowed_refs,
                    std::vector<ObjectID> *deleted));

  MOCK_METHOD9(AddOwnedObject,
               void(const ObjectID &object_id,
                    const std::vector<ObjectID> &contained_ids,
                    const rpc::Address &owner_address,
                    const std::string &call_site,
                    const int64_t object_size,
                    bool is_reconstructable,
                    bool add_local_ref,
                    const std::optional<NodeID> &pinned_at_node_id,
                    rpc::TensorTransport tensor_transport));

  MOCK_METHOD2(AddDynamicReturn,
               void(const ObjectID &object_id, const ObjectID &generator_id));

  MOCK_METHOD2(OwnDynamicStreamingTaskReturnRef,
               void(const ObjectID &object_id, const ObjectID &generator_id));

  MOCK_METHOD2(TryReleaseLocalRefs,
               void(const std::vector<ObjectID> &object_ids,
                    std::vector<ObjectID> *deleted));

  MOCK_METHOD2(CheckGeneratorRefsLineageOutOfScope,
               bool(const ObjectID &generator_id, int64_t num_objects_generated));

  MOCK_METHOD2(UpdateObjectSize, void(const ObjectID &object_id, int64_t object_size));

  MOCK_METHOD4(AddBorrowedObject,
               bool(const ObjectID &object_id,
                    const ObjectID &outer_id,
                    const rpc::Address &owner_address,
                    bool foreign_owner_already_monitoring));

  MOCK_CONST_METHOD2(GetOwner,
                     bool(const ObjectID &object_id, rpc::Address *owner_address));

  MOCK_CONST_METHOD1(HasOwner, bool(const ObjectID &object_id));

  MOCK_CONST_METHOD1(
      HasOwner, StatusSet<StatusT::NotFound>(const std::vector<ObjectID> &object_ids));

  MOCK_CONST_METHOD1(GetOwnerAddresses,
                     std::vector<rpc::Address>(const std::vector<ObjectID> &object_ids));

  MOCK_CONST_METHOD1(IsPlasmaObjectFreed, bool(const ObjectID &object_id));

  MOCK_METHOD1(TryMarkFreedObjectInUseAgain, bool(const ObjectID &object_id));

  MOCK_METHOD1(FreePlasmaObjects, void(const std::vector<ObjectID> &object_ids));

  MOCK_METHOD2(AddObjectOutOfScopeOrFreedCallback,
               bool(const ObjectID &object_id,
                    const std::function<void(const ObjectID &)> callback));

  MOCK_METHOD2(AddObjectRefDeletedCallback,
               bool(const ObjectID &object_id,
                    std::function<void(const ObjectID &)> callback));

  MOCK_METHOD3(SubscribeRefRemoved,
               void(const ObjectID &object_id,
                    const ObjectID &contained_in_id,
                    const rpc::Address &owner_address));

  MOCK_METHOD1(SetReleaseLineageCallback, void(const LineageReleasedCallback &callback));

  MOCK_METHOD1(PublishRefRemoved, void(const ObjectID &object_id));

  MOCK_CONST_METHOD0(NumObjectIDsInScope, size_t());

  MOCK_CONST_METHOD0(NumObjectsOwnedByUs, size_t());

  MOCK_CONST_METHOD0(NumActorsOwnedByUs, size_t());

  MOCK_CONST_METHOD0(GetAllInScopeObjectIDs, std::unordered_set<ObjectID>());

  MOCK_CONST_METHOD0(GetAllReferenceCounts,
                     std::unordered_map<ObjectID, std::pair<size_t, size_t>>());

  MOCK_CONST_METHOD0(DebugString, std::string());

  MOCK_METHOD3(
      PopAndClearLocalBorrowers,
      void(const std::vector<ObjectID> &borrowed_ids,
           ::google::protobuf::RepeatedPtrField<rpc::ObjectReferenceCount> *proto,
           std::vector<ObjectID> *deleted));

  MOCK_METHOD3(AddNestedObjectIds,
               void(const ObjectID &object_id,
                    const std::vector<ObjectID> &inner_ids,
                    const rpc::Address &owner_address));

  MOCK_METHOD2(UpdateObjectPinnedAtRaylet,
               void(const ObjectID &object_id, const NodeID &node_id));

  MOCK_CONST_METHOD4(IsPlasmaObjectPinnedOrSpilled,
                     bool(const ObjectID &object_id,
                          bool *owned_by_us,
                          NodeID *pinned_at,
                          bool *spilled));

  MOCK_METHOD1(ResetObjectsOnRemovedNode, void(const NodeID &node_id));

  MOCK_METHOD0(FlushObjectsToRecover, std::vector<ObjectID>());

  MOCK_CONST_METHOD1(HasReference, bool(const ObjectID &object_id));

  MOCK_CONST_METHOD3(
      AddObjectRefStats,
      void(const absl::flat_hash_map<ObjectID, std::pair<int64_t, std::string>>
               &pinned_objects,
           rpc::CoreWorkerStats *stats,
           const int64_t limit));

  MOCK_METHOD2(AddObjectLocation, bool(const ObjectID &object_id, const NodeID &node_id));

  MOCK_METHOD2(RemoveObjectLocation,
               bool(const ObjectID &object_id, const NodeID &node_id));

  MOCK_METHOD1(GetObjectLocations,
               std::optional<absl::flat_hash_set<NodeID>>(const ObjectID &object_id));

  MOCK_METHOD1(PublishObjectLocationSnapshot, void(const ObjectID &object_id));

  MOCK_METHOD2(FillObjectInformation,
               void(const ObjectID &object_id,
                    rpc::WorkerObjectLocationsPubMessage *object_info));

  MOCK_METHOD3(HandleObjectSpilled,
               bool(const ObjectID &object_id,
                    const std::string &spilled_url,
                    const NodeID &spilled_node_id));

  MOCK_CONST_METHOD1(GetLocalityData,
                     std::optional<LocalityData>(const ObjectID &object_id));

  MOCK_METHOD3(ReportLocalityData,
               bool(const ObjectID &object_id,
                    const absl::flat_hash_set<NodeID> &locations,
                    uint64_t object_size));

  MOCK_METHOD2(AddBorrowerAddress,
               void(const ObjectID &object_id, const rpc::Address &borrower_address));

  MOCK_CONST_METHOD2(IsObjectReconstructable,
                     bool(const ObjectID &object_id, bool *lineage_evicted));

  MOCK_METHOD1(EvictLineage, int64_t(int64_t min_bytes_to_evict));

  MOCK_METHOD2(UpdateObjectPendingCreation,
               void(const ObjectID &object_id, bool pending_creation));

  MOCK_CONST_METHOD1(IsObjectPendingCreation, bool(const ObjectID &object_id));

  MOCK_METHOD0(ReleaseAllLocalReferences, void());

  MOCK_CONST_METHOD1(GetTensorTransport,
                     std::optional<rpc::TensorTransport>(const ObjectID &object_id));

  MOCK_METHOD0(RecordMetrics, void());

  virtual ~MockReferenceCounter() {}
};

}  // namespace core
}  // namespace ray
