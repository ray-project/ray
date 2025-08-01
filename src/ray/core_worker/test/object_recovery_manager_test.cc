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

#include <list>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "fakes/ray/rpc/raylet/raylet_client.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "mock/ray/core_worker/task_manager_interface.h"
#include "mock/ray/pubsub/publisher.h"
#include "mock/ray/pubsub/subscriber.h"
#include "ray/common/task/task_spec.h"
#include "ray/common/task/task_util.h"
#include "ray/common/test_util.h"
#include "ray/core_worker/store_provider/memory_store/memory_store.h"
#include "ray/core_worker/transport/normal_task_submitter.h"
#include "ray/raylet_client/raylet_client.h"

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

  std::optional<rpc::ErrorType> ResubmitTask(const TaskID &task_id,
                                             std::vector<ObjectID> *task_deps) override {
    if (task_specs.find(task_id) == task_specs.end()) {
      return rpc::ErrorType::OBJECT_UNRECONSTRUCTABLE_MAX_ATTEMPTS_EXCEEDED;
    }

    for (const auto &dep : task_specs[task_id]) {
      task_deps->push_back(dep);
    }
    num_tasks_resubmitted++;
    return std::nullopt;
  }

  absl::flat_hash_map<TaskID, std::vector<ObjectID>> task_specs;
  int num_tasks_resubmitted = 0;
};

class MockRayletClient : public FakeRayletClient {
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
      : local_raylet_id_(NodeID::FromRandom()),
        io_context_("TestOnly.ObjectRecoveryManagerTestBase"),
        publisher_(std::make_shared<pubsub::MockPublisher>()),
        subscriber_(std::make_shared<pubsub::MockSubscriber>()),
        object_directory_(std::make_shared<MockObjectDirectory>()),
        memory_store_(
            std::make_shared<CoreWorkerMemoryStore>(io_context_.GetIoService())),
        raylet_client_pool_(std::make_shared<rpc::RayletClientPool>(
            [&](const rpc::Address &) { return raylet_client_; })),
        raylet_client_(std::make_shared<MockRayletClient>()),
        task_manager_(std::make_shared<MockTaskManager>()),
        ref_counter_(std::make_shared<ReferenceCounter>(
            rpc::Address(),
            publisher_.get(),
            subscriber_.get(),
            /*is_node_dead=*/[](const NodeID &) { return false; },
            /*lineage_pinning_enabled=*/lineage_enabled)),
        manager_(
            rpc::Address(),
            raylet_client_pool_,
            raylet_client_,
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
              RAY_CHECK(memory_store_->Put(data, object_id));
            }) {
    ref_counter_->SetReleaseLineageCallback(
        [](const ObjectID &, std::vector<ObjectID> *args) { return 0; });
  }

  void TearDown() override {
    // io_context_ must be joined and stopped before any other managers being
    // destructed, otherwise it may run callbacks that captured dangling objects.
    io_context_.Stop();
  }

  NodeID local_raylet_id_;
  absl::flat_hash_map<ObjectID, rpc::ErrorType> failed_reconstructions_;

  // Used by memory_store_.
  InstrumentedIOContextWithThread io_context_;
  std::shared_ptr<pubsub::MockPublisher> publisher_;
  std::shared_ptr<pubsub::MockSubscriber> subscriber_;
  std::shared_ptr<MockObjectDirectory> object_directory_;
  std::shared_ptr<CoreWorkerMemoryStore> memory_store_;
  std::shared_ptr<rpc::RayletClientPool> raylet_client_pool_;
  std::shared_ptr<MockRayletClient> raylet_client_;
  std::shared_ptr<MockTaskManager> task_manager_;
  std::shared_ptr<ReferenceCounter> ref_counter_;
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
                               true,
                               /*add_local_ref=*/true);
  ASSERT_TRUE(manager_.RecoverObject(object_id));
  ASSERT_TRUE(failed_reconstructions_.empty());
  ASSERT_EQ(object_directory_->Flush(), 1);
  ASSERT_EQ(failed_reconstructions_[object_id], rpc::ErrorType::OBJECT_LOST);
  ASSERT_EQ(task_manager_->num_tasks_resubmitted, 0);

  // Borrowed object.
  object_id = ObjectID::FromRandom();
  ref_counter_->AddLocalReference(object_id, "");
  ASSERT_FALSE(manager_.RecoverObject(object_id));
  ASSERT_EQ(task_manager_->num_tasks_resubmitted, 0);

  // Ref went out of scope.
  object_id = ObjectID::FromRandom();
  ASSERT_FALSE(manager_.RecoverObject(object_id));
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
                               true,
                               /*add_local_ref=*/true);
  std::vector<rpc::Address> addresses({rpc::Address()});
  object_directory_->SetLocations(object_id, addresses);

  ASSERT_TRUE(manager_.RecoverObject(object_id));
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
                               true,
                               /*add_local_ref=*/true);
  std::vector<rpc::Address> addresses({rpc::Address(), rpc::Address()});
  object_directory_->SetLocations(object_id, addresses);

  ASSERT_TRUE(manager_.RecoverObject(object_id));
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
                               true,
                               /*add_local_ref=*/true);
  task_manager_->AddTask(object_id.TaskId(), {});

  ASSERT_TRUE(manager_.RecoverObject(object_id));
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
                               true,
                               /*add_local_ref=*/true);
  ref_counter_->AddLocalReference(object_id, "");

  ASSERT_TRUE(manager_.RecoverObject(object_id));
  // A second attempt to recover the object will not trigger any more
  // callbacks.
  ASSERT_TRUE(manager_.RecoverObject(object_id));
  // A new copy of the object is pinned.
  NodeID remote_node_id = NodeID::FromRandom();
  rpc::Address address;
  address.set_raylet_id(remote_node_id.Binary());
  object_directory_->SetLocations(object_id, {address});
  ASSERT_EQ(object_directory_->Flush(), 1);
  ASSERT_EQ(raylet_client_->Flush(), 1);

  // The object has been marked as failed but it is still pinned on the new
  // node. Another attempt to recover the object will not trigger any
  // callbacks.
  ASSERT_TRUE(manager_.RecoverObject(object_id));
  ASSERT_EQ(object_directory_->Flush(), 0);

  // The object is removed and can be recovered again.
  ref_counter_->ResetObjectsOnRemovedNode(remote_node_id);
  auto objects = ref_counter_->FlushObjectsToRecover();
  ASSERT_EQ(objects.size(), 1);
  ASSERT_EQ(objects[0], object_id);
  memory_store_->Delete(objects);
  ASSERT_TRUE(manager_.RecoverObject(object_id));
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
                                 true,
                                 /*add_local_ref=*/true);
    task_manager_->AddTask(object_id.TaskId(), dependencies);
    dependencies = {object_id};
    object_ids.push_back(object_id);
  }

  ASSERT_TRUE(manager_.RecoverObject(object_ids.back()));
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
                               true,
                               /*add_local_ref=*/true);

  ASSERT_TRUE(manager_.RecoverObject(object_id));
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
                               true,
                               /*add_local_ref=*/true);

  ObjectID object_id = ObjectID::FromRandom();
  ref_counter_->AddOwnedObject(object_id,
                               {},
                               rpc::Address(),
                               "",
                               0,
                               true,
                               /*add_local_ref=*/true);
  task_manager_->AddTask(object_id.TaskId(), {dep_id});
  RAY_LOG(INFO) << object_id;

  ASSERT_TRUE(manager_.RecoverObject(object_id));
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
                               true,
                               /*add_local_ref=*/true);
  ref_counter_->AddLocalReference(object_id, "");
  ref_counter_->EvictLineage(1);

  ASSERT_TRUE(manager_.RecoverObject(object_id));
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
                               true,
                               /*add_local_ref=*/true);
  ref_counter_->UpdateObjectPinnedAtRaylet(object_id, NodeID::FromRandom());

  memory_store_->Delete({object_id});
  ASSERT_TRUE(manager_.RecoverObject(object_id));
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

}  // namespace core
}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
