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

#include "ray/core_worker/object_recovery_manager.h"

#include <future>
#include <list>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "mock/ray/core_worker/task_manager_interface.h"
#include "mock/ray/pubsub/publisher.h"
#include "ray/common/test_utils.h"
#include "ray/core_worker/reference_counter.h"
#include "ray/core_worker/reference_counter_interface.h"
#include "ray/core_worker/store_provider/memory_store/memory_store.h"
#include "ray/observability/fake_metric.h"
#include "ray/pubsub/fake_subscriber.h"
#include "ray/raylet_rpc_client/fake_raylet_client.h"
#include "ray/raylet_rpc_client/raylet_client_interface.h"
#include "ray/util/clock.h"

namespace ray {
namespace core {

// Used to prevent leases from timing out when not testing that logic. It would
// be better to use a mock clock or lease manager interface, but that's high
// overhead for the very simple timeout logic we currently have.
int64_t kLongTimeout = 1024 * 1024 * 1024;

class MockTaskManager : public MockTaskManagerInterface {
 public:
  MockTaskManager() {}

  void AddTask(const TaskID &task_id, std::vector<ObjectID> task_deps) {
    task_specs[task_id] = task_deps;
  }

  void CancelTask(const TaskID &task_id) { cancelled_tasks.insert(task_id); }

  std::optional<rpc::ErrorType> ResubmitTask(const TaskID &task_id,
                                             std::vector<ObjectID> *task_deps) override {
    if (task_specs.find(task_id) == task_specs.end()) {
      return rpc::ErrorType::OBJECT_UNRECONSTRUCTABLE_MAX_ATTEMPTS_EXCEEDED;
    }
    if (cancelled_tasks.contains(task_id)) {
      return rpc::ErrorType::OBJECT_UNRECONSTRUCTABLE_TASK_CANCELLED;
    }

    for (const auto &dep : task_specs[task_id]) {
      task_deps->push_back(dep);
    }
    num_tasks_resubmitted++;
    return std::nullopt;
  }

  absl::flat_hash_map<TaskID, std::vector<ObjectID>> task_specs;
  absl::flat_hash_set<TaskID> cancelled_tasks;
  int num_tasks_resubmitted = 0;
};

class MockRayletClient : public rpc::FakeRayletClient {
 public:
  void PinObjectIDs(
      const rpc::Address &caller_address,
      const std::vector<ObjectID> &object_ids,
      const ObjectID &generator_id,
      const rpc::ClientCallback<rpc::PinObjectIDsReply> &callback) override {
    RAY_LOG(INFO) << "PinObjectIDs " << object_ids.size();
    callbacks.push_back(callback);
  }

  size_t Flush(bool success = true) {
    std::list<rpc::ClientCallback<rpc::PinObjectIDsReply>> callbacks_snapshot;
    std::swap(callbacks_snapshot, callbacks);
    size_t flushed = callbacks_snapshot.size();
    for (const auto &callback : callbacks_snapshot) {
      rpc::PinObjectIDsReply reply;
      reply.add_successes(success);
      callback(Status::OK(), std::move(reply));
    }
    return flushed;
  }

  std::list<rpc::ClientCallback<rpc::PinObjectIDsReply>> callbacks = {};
};

class MockObjectDirectory {
 public:
  void AsyncGetLocations(const ObjectID &object_id,
                         const ObjectLookupCallback &callback) {
    callbacks.push_back({object_id, callback});
  }

  void SetLocations(const ObjectID &object_id,
                    const std::vector<rpc::Address> &addresses) {
    locations[object_id] = addresses;
  }

  size_t Flush() {
    size_t flushed = callbacks.size();
    for (const auto &pair : callbacks) {
      pair.second(pair.first, locations[pair.first]);
    }
    for (size_t i = 0; i < flushed; i++) {
      callbacks.erase(callbacks.begin());
    }
    return flushed;
  }

  std::vector<std::pair<ObjectID, ObjectLookupCallback>> callbacks = {};
  absl::flat_hash_map<ObjectID, std::vector<rpc::Address>> locations;
};

class ObjectRecoveryManagerTestBase : public ::testing::Test {
 public:
  explicit ObjectRecoveryManagerTestBase(bool lineage_enabled)
      : local_node_id_(NodeID::FromRandom()),
        io_context_("TestOnly.ObjectRecoveryManagerTestBase"),
        publisher_(std::make_shared<pubsub::MockPublisher>()),
        subscriber_(std::make_shared<pubsub::FakeSubscriber>()),
        object_directory_(std::make_shared<MockObjectDirectory>()),
        memory_store_(
            std::make_shared<CoreWorkerMemoryStore>(io_context_.GetIoService(), clock_)),
        raylet_client_pool_(std::make_shared<rpc::RayletClientPool>(
            [&](const rpc::Address &) { return raylet_client_; })),
        raylet_client_(std::make_shared<MockRayletClient>()),
        task_manager_(std::make_shared<MockTaskManager>()),
        ref_counter_(std::make_shared<ReferenceCounter>(
            rpc::Address(),
            publisher_.get(),
            subscriber_.get(),
            /*is_node_dead=*/
            [this](const NodeID &node_id) { return dead_nodes_.contains(node_id); },
            /*free_object_on_nodes_async=*/
            [](const ObjectID &, const absl::flat_hash_set<NodeID> &) {},
            *std::make_shared<ray::observability::FakeGauge>(),
            *std::make_shared<ray::observability::FakeGauge>(),
            /*lineage_pinning_enabled=*/lineage_enabled)),
        manager_(
            rpc::Address(),
            raylet_client_pool_,
            [&](const ObjectID &object_id, const ObjectLookupCallback &callback) {
              object_directory_->AsyncGetLocations(object_id, callback);
            },
            *task_manager_,
            *ref_counter_,
            *memory_store_,
            [&](const ObjectID &object_id, rpc::ErrorType reason, bool pin_object) {
              RAY_CHECK(failed_reconstructions_.count(object_id) == 0);
              failed_reconstructions_[object_id] = reason;

              std::string meta =
                  std::to_string(static_cast<int>(rpc::ErrorType::OBJECT_IN_PLASMA));
              auto metadata =
                  const_cast<uint8_t *>(reinterpret_cast<const uint8_t *>(meta.data()));
              auto meta_buffer =
                  std::make_shared<LocalMemoryBuffer>(metadata, meta.size());
              auto data =
                  RayObject(nullptr, meta_buffer, std::vector<rpc::ObjectReference>());
              memory_store_->Put(data, object_id, ref_counter_->HasReference(object_id));
            }) {
    ref_counter_->SetReleaseLineageCallback(
        [](const ObjectID &, std::vector<ObjectID> *args) { return 0; });
  }

  void TearDown() override {
    // io_context_ must be joined and stopped before any other managers being
    // destructed, otherwise it may run callbacks that captured dangling objects.
    io_context_.Stop();
  }

  // Blocks until every callback already posted to the io_context has run. The
  // io_context is single-threaded and posts are FIFO, so once this sentinel runs
  // so have all callbacks posted before it -- including the memory store's async
  // get callbacks, one of which clears ObjectRecoveryManager's pending-recovery
  // set. Without this, a subsequent RecoverObject call would race that erase.
  void DrainIoContext() {
    std::promise<void> drained;
    io_context_.GetIoService().post([&drained] { drained.set_value(); },
                                    "TestOnly.DrainIoContext");
    drained.get_future().wait();
  }

  // Simulates a node failure. In production the owner's GCS liveness view and
  // its node-change callback are updated in the same call
  // (NodeInfoAccessor::HandleNotification), so these two always happen together:
  // nothing can observe is_node_dead() == true before the reference counter has
  // reset the primary copies that lived on that node.
  void KillNode(const NodeID &node_id) {
    dead_nodes_.insert(node_id);
    ref_counter_->ResetObjectsOnRemovedNode(node_id);
  }

  // Puts the OBJECT_IN_PLASMA sentinel that the owner uses to mark a large
  // object as available.
  void PutInPlasmaSentinel(const ObjectID &object_id) {
    std::string meta = std::to_string(static_cast<int>(rpc::ErrorType::OBJECT_IN_PLASMA));
    auto metadata = const_cast<uint8_t *>(reinterpret_cast<const uint8_t *>(meta.data()));
    auto meta_buffer = std::make_shared<LocalMemoryBuffer>(metadata, meta.size());
    memory_store_->Put(
        RayObject(nullptr, meta_buffer, std::vector<rpc::ObjectReference>()),
        object_id,
        ref_counter_->HasReference(object_id));
  }

  // Simulates a resubmitted task finishing: its return value lands in plasma on
  // `node_id` and the owner records the new primary copy. Drains the io_context
  // so the recovery is fully settled -- i.e. not still pending -- on return.
  void FinishReconstruction(const ObjectID &object_id, const NodeID &node_id) {
    PutInPlasmaSentinel(object_id);
    ref_counter_->UpdateObjectPinnedAtRaylet(object_id, node_id);
    DrainIoContext();
  }

  NodeID PinnedAt(const ObjectID &object_id) {
    bool owned_by_us = false;
    NodeID pinned_at;
    bool spilled = false;
    RAY_CHECK(ref_counter_->IsPlasmaObjectPinnedOrSpilled(
        object_id, &owned_by_us, &pinned_at, &spilled));
    return pinned_at;
  }

  bool IsSpilled(const ObjectID &object_id) {
    bool owned_by_us = false;
    NodeID pinned_at;
    bool spilled = false;
    RAY_CHECK(ref_counter_->IsPlasmaObjectPinnedOrSpilled(
        object_id, &owned_by_us, &pinned_at, &spilled));
    return spilled;
  }

  NodeID local_node_id_;
  // Nodes the owner's GCS view considers dead. Mutated only via KillNode.
  absl::flat_hash_set<NodeID> dead_nodes_;
  absl::flat_hash_map<ObjectID, rpc::ErrorType> failed_reconstructions_;

  // Used by memory_store_.
  InstrumentedIOContextWithThread io_context_;
  Clock clock_;
  std::shared_ptr<pubsub::MockPublisher> publisher_;
  std::shared_ptr<pubsub::FakeSubscriber> subscriber_;
  std::shared_ptr<MockObjectDirectory> object_directory_;
  std::shared_ptr<CoreWorkerMemoryStore> memory_store_;
  std::shared_ptr<rpc::RayletClientPool> raylet_client_pool_;
  std::shared_ptr<MockRayletClient> raylet_client_;
  std::shared_ptr<MockTaskManager> task_manager_;
  std::shared_ptr<ReferenceCounterInterface> ref_counter_;
  ObjectRecoveryManager manager_;
};

class ObjectRecoveryLineageDisabledTest : public ObjectRecoveryManagerTestBase {
 public:
  ObjectRecoveryLineageDisabledTest() : ObjectRecoveryManagerTestBase(false) {}
};

class ObjectRecoveryManagerTest : public ObjectRecoveryManagerTestBase {
 public:
  ObjectRecoveryManagerTest() : ObjectRecoveryManagerTestBase(true) {}
};

TEST_F(ObjectRecoveryLineageDisabledTest, TestNoReconstruction) {
  // Lineage recording disabled.
  ObjectID object_id = ObjectID::FromRandom();
  ref_counter_->AddOwnedObject(object_id,
                               {},
                               rpc::Address(),
                               "",
                               0,
                               LineageReconstructionEligibility::ELIGIBLE,
                               /*add_local_ref=*/true);
  ASSERT_FALSE(manager_.RecoverObject(object_id).has_value());
  ASSERT_TRUE(failed_reconstructions_.empty());
  ASSERT_EQ(object_directory_->Flush(), 1);
  // When lineage is disabled, reconstruction fails with LINEAGE_DISABLED error.
  ASSERT_EQ(failed_reconstructions_[object_id],
            rpc::ErrorType::OBJECT_UNRECONSTRUCTABLE_LINEAGE_DISABLED);
  ASSERT_EQ(task_manager_->num_tasks_resubmitted, 0);

  // Borrowed object.
  object_id = ObjectID::FromRandom();
  ref_counter_->AddLocalReference(object_id, "");
  ASSERT_EQ(manager_.RecoverObject(object_id),
            rpc::ErrorType::OBJECT_UNRECONSTRUCTABLE_BORROWED);
  ASSERT_EQ(task_manager_->num_tasks_resubmitted, 0);

  // Ref went out of scope.
  object_id = ObjectID::FromRandom();
  ASSERT_EQ(manager_.RecoverObject(object_id),
            rpc::ErrorType::OBJECT_UNRECONSTRUCTABLE_REF_NOT_FOUND);
  ASSERT_EQ(failed_reconstructions_.count(object_id), 0);
  ASSERT_EQ(task_manager_->num_tasks_resubmitted, 0);
}

TEST_F(ObjectRecoveryLineageDisabledTest, TestPinNewCopy) {
  ObjectID object_id = ObjectID::FromRandom();
  ref_counter_->AddOwnedObject(object_id,
                               {},
                               rpc::Address(),
                               "",
                               0,
                               LineageReconstructionEligibility::ELIGIBLE,
                               /*add_local_ref=*/true);
  rpc::Address address;
  address.set_node_id(NodeID::FromRandom().Binary());
  object_directory_->SetLocations(object_id, {address});

  ASSERT_FALSE(manager_.RecoverObject(object_id).has_value());
  ASSERT_EQ(object_directory_->Flush(), 1);
  ASSERT_EQ(raylet_client_->Flush(), 1);
  ASSERT_TRUE(failed_reconstructions_.empty());
  ASSERT_EQ(task_manager_->num_tasks_resubmitted, 0);
}

TEST_F(ObjectRecoveryManagerTest, TestPinNewCopy) {
  ObjectID object_id = ObjectID::FromRandom();
  ref_counter_->AddOwnedObject(object_id,
                               {},
                               rpc::Address(),
                               "",
                               0,
                               LineageReconstructionEligibility::ELIGIBLE,
                               /*add_local_ref=*/true);
  rpc::Address address1;
  address1.set_node_id(NodeID::FromRandom().Binary());
  rpc::Address address2;
  address2.set_node_id(NodeID::FromRandom().Binary());
  object_directory_->SetLocations(object_id, {address1, address2});

  ASSERT_FALSE(manager_.RecoverObject(object_id).has_value());
  ASSERT_EQ(object_directory_->Flush(), 1);
  // First copy is evicted so pin fails.
  ASSERT_EQ(raylet_client_->Flush(false), 1);
  // Second copy is present so pin succeeds.
  ASSERT_EQ(raylet_client_->Flush(true), 1);
  ASSERT_TRUE(failed_reconstructions_.empty());
  ASSERT_EQ(task_manager_->num_tasks_resubmitted, 0);
}

TEST_F(ObjectRecoveryManagerTest, TestReconstruction) {
  ObjectID object_id = ObjectID::FromRandom();
  ref_counter_->AddOwnedObject(object_id,
                               {},
                               rpc::Address(),
                               "",
                               0,
                               LineageReconstructionEligibility::ELIGIBLE,
                               /*add_local_ref=*/true);
  task_manager_->AddTask(object_id.TaskId(), {});

  ASSERT_FALSE(manager_.RecoverObject(object_id).has_value());
  ASSERT_TRUE(ref_counter_->IsObjectPendingCreation(object_id));
  ASSERT_EQ(object_directory_->Flush(), 1);

  ASSERT_TRUE(failed_reconstructions_.empty());
  ASSERT_EQ(task_manager_->num_tasks_resubmitted, 1);
}

TEST_F(ObjectRecoveryManagerTest, TestReconstructionSuppression) {
  ObjectID object_id = ObjectID::FromRandom();
  ref_counter_->AddOwnedObject(object_id,
                               {},
                               rpc::Address(),
                               "",
                               0,
                               LineageReconstructionEligibility::ELIGIBLE,
                               /*add_local_ref=*/true);
  ref_counter_->AddLocalReference(object_id, "");

  ASSERT_FALSE(manager_.RecoverObject(object_id).has_value());
  // A second attempt to recover the object will not trigger any more
  // callbacks.
  ASSERT_FALSE(manager_.RecoverObject(object_id).has_value());
  // A new copy of the object is pinned.
  NodeID remote_node_id = NodeID::FromRandom();
  rpc::Address address;
  address.set_node_id(remote_node_id.Binary());
  object_directory_->SetLocations(object_id, {address});
  ASSERT_EQ(object_directory_->Flush(), 1);
  ASSERT_EQ(raylet_client_->Flush(), 1);

  // The object has been marked as failed but it is still pinned on the new
  // node. Another attempt to recover the object will not trigger any
  // callbacks.
  ASSERT_FALSE(manager_.RecoverObject(object_id).has_value());
  ASSERT_EQ(object_directory_->Flush(), 0);

  // The object is removed and can be recovered again.
  ref_counter_->ResetObjectsOnRemovedNode(remote_node_id);
  auto objects = ref_counter_->FlushObjectsToRecover();
  ASSERT_EQ(objects.size(), 1);
  ASSERT_EQ(objects[0], object_id);
  memory_store_->Delete(objects);
  ASSERT_FALSE(manager_.RecoverObject(object_id).has_value());
  ASSERT_EQ(object_directory_->Flush(), 1);
}

TEST_F(ObjectRecoveryManagerTest, TestReconstructionChain) {
  std::vector<ObjectID> object_ids;
  std::vector<ObjectID> dependencies;
  for (int i = 0; i < 3; i++) {
    ObjectID object_id = ObjectID::FromRandom();
    ref_counter_->AddOwnedObject(object_id,
                                 {},
                                 rpc::Address(),
                                 "",
                                 0,
                                 LineageReconstructionEligibility::ELIGIBLE,
                                 /*add_local_ref=*/true);
    task_manager_->AddTask(object_id.TaskId(), dependencies);
    dependencies = {object_id};
    object_ids.push_back(object_id);
  }

  ASSERT_FALSE(manager_.RecoverObject(object_ids.back()).has_value());
  for (int i = 0; i < 3; i++) {
    RAY_LOG(INFO) << i;
    ASSERT_EQ(object_directory_->Flush(), 1);
    ASSERT_TRUE(failed_reconstructions_.empty());
    ASSERT_EQ(task_manager_->num_tasks_resubmitted, i + 1);
  }
}

TEST_F(ObjectRecoveryManagerTest, TestReconstructionFails) {
  ObjectID object_id = ObjectID::FromRandom();
  ref_counter_->AddOwnedObject(object_id,
                               {},
                               rpc::Address(),
                               "",
                               0,
                               LineageReconstructionEligibility::ELIGIBLE,
                               /*add_local_ref=*/true);

  ASSERT_FALSE(manager_.RecoverObject(object_id).has_value());
  ASSERT_EQ(object_directory_->Flush(), 1);

  ASSERT_TRUE(failed_reconstructions_[object_id] ==
              rpc::ErrorType::OBJECT_UNRECONSTRUCTABLE_MAX_ATTEMPTS_EXCEEDED);
  ASSERT_EQ(task_manager_->num_tasks_resubmitted, 0);
}

TEST_F(ObjectRecoveryManagerTest, TestDependencyReconstructionFails) {
  ObjectID dep_id = ObjectID::FromRandom();
  ref_counter_->AddOwnedObject(dep_id,
                               {},
                               rpc::Address(),
                               "",
                               0,
                               LineageReconstructionEligibility::ELIGIBLE,
                               /*add_local_ref=*/true);

  ObjectID object_id = ObjectID::FromRandom();
  ref_counter_->AddOwnedObject(object_id,
                               {},
                               rpc::Address(),
                               "",
                               0,
                               LineageReconstructionEligibility::ELIGIBLE,
                               /*add_local_ref=*/true);
  task_manager_->AddTask(object_id.TaskId(), {dep_id});
  RAY_LOG(INFO) << object_id;

  ASSERT_FALSE(manager_.RecoverObject(object_id).has_value());
  ASSERT_EQ(object_directory_->Flush(), 1);
  // Trigger callback for dep ID.
  ASSERT_EQ(object_directory_->Flush(), 1);
  ASSERT_EQ(failed_reconstructions_[dep_id],
            rpc::ErrorType::OBJECT_UNRECONSTRUCTABLE_MAX_ATTEMPTS_EXCEEDED);
  ASSERT_EQ(failed_reconstructions_.count(object_id), 0);
  ASSERT_EQ(task_manager_->num_tasks_resubmitted, 1);
}

TEST_F(ObjectRecoveryManagerTest, TestLineageEvicted) {
  ObjectID object_id = ObjectID::FromRandom();
  ref_counter_->AddOwnedObject(object_id,
                               {},
                               rpc::Address(),
                               "",
                               0,
                               LineageReconstructionEligibility::ELIGIBLE,
                               /*add_local_ref=*/true);
  ref_counter_->AddLocalReference(object_id, "");
  ref_counter_->EvictLineage(1);

  ASSERT_FALSE(manager_.RecoverObject(object_id).has_value());
  ASSERT_EQ(object_directory_->Flush(), 1);
  ASSERT_EQ(failed_reconstructions_[object_id],
            rpc::ErrorType::OBJECT_UNRECONSTRUCTABLE_LINEAGE_EVICTED);
}

TEST_F(ObjectRecoveryManagerTest, TestReconstructionSkipped) {
  // Test that if the object is already pinned or spilled,
  // reconstruction is skipped.
  ObjectID object_id = ObjectID::FromRandom();
  ref_counter_->AddOwnedObject(object_id,
                               {},
                               rpc::Address(),
                               "",
                               0,
                               LineageReconstructionEligibility::ELIGIBLE,
                               /*add_local_ref=*/true);
  ref_counter_->UpdateObjectPinnedAtRaylet(object_id, NodeID::FromRandom());

  memory_store_->Delete({object_id});
  ASSERT_FALSE(manager_.RecoverObject(object_id).has_value());
  ASSERT_TRUE(failed_reconstructions_.empty());
  ASSERT_EQ(object_directory_->Flush(), 0);
  ASSERT_EQ(raylet_client_->Flush(), 0);
  ASSERT_EQ(task_manager_->num_tasks_resubmitted, 0);
  // The object should be added back to the memory store
  // indicating the object is available again.
  bool in_plasma = false;
  ASSERT_TRUE(memory_store_->Contains(object_id, &in_plasma));
  ASSERT_TRUE(in_plasma);
}

TEST_F(ObjectRecoveryManagerTest, TestPutObjectReconstructionFails) {
  ObjectID object_id = ObjectID::FromRandom();
  ref_counter_->AddOwnedObject(object_id,
                               {},
                               rpc::Address(),
                               "",
                               0,
                               LineageReconstructionEligibility::INELIGIBLE_PUT,
                               /*add_local_ref=*/true);

  ASSERT_FALSE(manager_.RecoverObject(object_id).has_value());
  ASSERT_EQ(object_directory_->Flush(), 1);
  ASSERT_EQ(failed_reconstructions_[object_id],
            rpc::ErrorType::OBJECT_UNRECONSTRUCTABLE_PUT);
  ASSERT_EQ(task_manager_->num_tasks_resubmitted, 0);
}

TEST_F(ObjectRecoveryManagerTest, TestNoRetriesReconstructionFails) {
  ObjectID object_id = ObjectID::FromRandom();
  ref_counter_->AddOwnedObject(object_id,
                               {},
                               rpc::Address(),
                               "",
                               0,
                               LineageReconstructionEligibility::INELIGIBLE_NO_RETRIES,
                               /*add_local_ref=*/true);

  ASSERT_FALSE(manager_.RecoverObject(object_id).has_value());
  ASSERT_EQ(object_directory_->Flush(), 1);
  ASSERT_EQ(failed_reconstructions_[object_id],
            rpc::ErrorType::OBJECT_UNRECONSTRUCTABLE_RETRIES_DISABLED);
  ASSERT_EQ(task_manager_->num_tasks_resubmitted, 0);
}

TEST_F(ObjectRecoveryManagerTest, TestBorrowedObjectReconstructionFails) {
  // Create a borrowed dependency (we don't own it)
  ObjectID dep_id = ObjectID::FromRandom();
  ref_counter_->AddLocalReference(dep_id, "");

  // Create an owned object that depends on the borrowed object
  ObjectID object_id = ObjectID::FromRandom();
  ref_counter_->AddOwnedObject(object_id,
                               {},
                               rpc::Address(),
                               "",
                               0,
                               LineageReconstructionEligibility::ELIGIBLE,
                               /*add_local_ref=*/true);
  task_manager_->AddTask(object_id.TaskId(), {dep_id});

  // Try to recover the owned object
  ASSERT_FALSE(manager_.RecoverObject(object_id).has_value());
  ASSERT_EQ(object_directory_->Flush(), 1);

  // The task is resubmitted successfully
  ASSERT_EQ(task_manager_->num_tasks_resubmitted, 1);

  // But the dependency recovery fails with BORROWED error
  // because we don't own the dependency
  ASSERT_EQ(failed_reconstructions_[dep_id],
            rpc::ErrorType::OBJECT_UNRECONSTRUCTABLE_BORROWED);
  ASSERT_EQ(failed_reconstructions_.count(object_id), 0);
}

TEST_F(ObjectRecoveryManagerTest, TestTaskCancelledReconstructionFails) {
  ObjectID object_id = ObjectID::FromRandom();
  ref_counter_->AddOwnedObject(object_id,
                               {},
                               rpc::Address(),
                               "",
                               0,
                               LineageReconstructionEligibility::ELIGIBLE,
                               /*add_local_ref=*/true);
  task_manager_->AddTask(object_id.TaskId(), {});
  task_manager_->CancelTask(object_id.TaskId());

  ASSERT_FALSE(manager_.RecoverObject(object_id).has_value());
  ASSERT_EQ(object_directory_->Flush(), 1);
  ASSERT_EQ(failed_reconstructions_[object_id],
            rpc::ErrorType::OBJECT_UNRECONSTRUCTABLE_TASK_CANCELLED);
  ASSERT_EQ(task_manager_->num_tasks_resubmitted, 0);
}

// Regression test for a stale spill report poisoning an object's location state.
// This is the deterministic counterpart of
// python/ray/tests/test_reconstruction_spilled_out_of_scope.py::
// test_reconstruct_object_after_spill_report_from_dead_node, which needs an
// injected RPC delay to line the same events up.
//
// A raylet reports a spilled location to the owner as a fire-and-forget location
// update. If the reporting node dies while that update is in flight, the owner
// applies a report describing a copy that no longer exists. Doing so must not
// leave the reference claiming a location, or the owner will refuse to
// reconstruct the object the next time its real copy is lost.
TEST_F(ObjectRecoveryManagerTest, TestReconstructionAfterSpillReportFromDeadNode) {
  const NodeID node_a = NodeID::FromRandom();
  const NodeID node_b = NodeID::FromRandom();
  const ObjectID object_id = ObjectID::FromRandom();

  // 1. A task is submitted and its return value is pinned on node A.
  ref_counter_->AddOwnedObject(object_id,
                               {},
                               rpc::Address(),
                               "",
                               0,
                               LineageReconstructionEligibility::ELIGIBLE,
                               /*add_local_ref=*/true);
  task_manager_->AddTask(object_id.TaskId(), {});
  ref_counter_->UpdateObjectPinnedAtRaylet(object_id, node_a);
  // Node A spills the object and sends the owner a spilled-location update. The
  // owner has not processed it yet -- it is applied in step 4.

  // 2. Node A fails.
  KillNode(node_a);
  auto lost = ref_counter_->FlushObjectsToRecover();
  ASSERT_EQ(lost.size(), 1u);
  ASSERT_EQ(lost[0], object_id);
  memory_store_->Delete(lost);

  // 3. The object is reconstructed successfully: no copy is left to pin, so the
  //    task is resubmitted, and its output is pinned on node B.
  ASSERT_FALSE(manager_.RecoverObject(object_id).has_value());
  ASSERT_EQ(object_directory_->Flush(), 1);
  ASSERT_EQ(task_manager_->num_tasks_resubmitted, 1);
  ASSERT_TRUE(failed_reconstructions_.empty());
  FinishReconstruction(object_id, node_b);
  ASSERT_EQ(PinnedAt(object_id), node_b);
  ASSERT_FALSE(IsSpilled(object_id));

  // 4. Node A's spill report finally arrives, naming a node that is already
  //    dead. This is the first spill report the owner has processed for this
  //    object, so its stored spilled_node_id is still nil.
  ASSERT_TRUE(ref_counter_->HandleObjectSpilled(object_id, "/tmp/spill/url", node_a));

  // 5. The stale report must not be recorded as a location. `spilled` has to
  //    stay false, and the live copy on node B has to keep its primary-copy
  //    record -- node B's failure in step 6 matches on exactly that.
  // Non-fatal so that a broken owner reports the whole chain of consequences
  // below in one run, rather than stopping at the first corrupted field.
  EXPECT_FALSE(IsSpilled(object_id))
      << "a spill report naming a dead node was recorded as a live location";
  EXPECT_EQ(PinnedAt(object_id), node_b)
      << "a stale spill report discarded the primary copy on a different, live node";

  // Any recovery the report asks for is a no-op: the object really is available
  // on node B, so nothing is reconstructed.
  for (const auto &id : ref_counter_->FlushObjectsToRecover()) {
    ASSERT_FALSE(manager_.RecoverObject(id).has_value());
  }
  ASSERT_EQ(object_directory_->Flush(), 0);
  ASSERT_EQ(task_manager_->num_tasks_resubmitted, 1);
  DrainIoContext();

  // 6. Node B fails, taking the last remaining copy with it.
  KillNode(node_b);
  lost = ref_counter_->FlushObjectsToRecover();
  ASSERT_EQ(lost.size(), 1u)
      << "losing the last copy did not queue the object for recovery; the stale "
         "spill report had already cleared the primary copy that this matches on";
  ASSERT_EQ(lost[0], object_id);
  memory_store_->Delete(lost);

  // 7. So the task must be resubmitted again. Before the fix, HandleObjectSpilled
  //    set `spilled` before checking node liveness and its dead-node branch never
  //    assigned spilled_node_id, so UnsetObjectPrimaryCopy's
  //    `spilled && !spilled_node_id.IsNil()` guard could not clear the flag.
  //    RecoverObject's `pinned_at.IsNil() && !spilled` test then refused to
  //    reconstruct an object it believed was still spilled somewhere.
  ASSERT_FALSE(manager_.RecoverObject(object_id).has_value());
  ASSERT_EQ(object_directory_->Flush(), 1);
  ASSERT_EQ(task_manager_->num_tasks_resubmitted, 2)
      << "the object's last copy was lost and it was never reconstructed";
  ASSERT_TRUE(failed_reconstructions_.empty());
}

}  // namespace core
}  // namespace ray
