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

#include "gtest/gtest.h"
#include "ray/common/task/task_spec.h"
#include "ray/common/task/task_util.h"
#include "ray/common/test_util.h"
#include "ray/core_worker/store_provider/memory_store/memory_store.h"
#include "ray/core_worker/transport/direct_task_transport.h"
#include "ray/raylet_client/raylet_client.h"

namespace ray {

// Used to prevent leases from timing out when not testing that logic. It would
// be better to use a mock clock or lease manager interface, but that's high
// overhead for the very simple timeout logic we currently have.
int64_t kLongTimeout = 1024 * 1024 * 1024;

class MockTaskResubmitter : public TaskResubmissionInterface {
 public:
  MockTaskResubmitter() {}

  void AddTask(const TaskID &task_id, std::vector<ObjectID> task_deps) {
    task_specs[task_id] = task_deps;
  }

  Status ResubmitTask(const TaskID &task_id, std::vector<ObjectID> *task_deps) {
    if (task_specs.find(task_id) == task_specs.end()) {
      return Status::Invalid("");
    }

    for (const auto &dep : task_specs[task_id]) {
      task_deps->push_back(dep);
    }
    num_tasks_resubmitted++;
    return Status::OK();
  }

  std::unordered_map<TaskID, std::vector<ObjectID>> task_specs;
  int num_tasks_resubmitted = 0;
};

class MockRayletClient : public PinObjectsInterface {
 public:
  void PinObjectIDs(
      const rpc::Address &caller_address, const std::vector<ObjectID> &object_ids,
      const ray::rpc::ClientCallback<ray::rpc::PinObjectIDsReply> &callback) override {
    RAY_LOG(INFO) << "PinObjectIDs " << object_ids.size();
    callbacks.push_back(callback);
  }

  size_t Flush() {
    size_t flushed = callbacks.size();
    for (const auto &callback : callbacks) {
      callback(Status::OK(), rpc::PinObjectIDsReply());
    }
    callbacks.clear();
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
  std::unordered_map<ObjectID, std::vector<rpc::Address>> locations;
};

class ObjectRecoveryManagerTest : public ::testing::Test {
 public:
  ObjectRecoveryManagerTest()
      : local_raylet_id_(NodeID::FromRandom()),
        object_directory_(std::make_shared<MockObjectDirectory>()),
        memory_store_(std::make_shared<CoreWorkerMemoryStore>()),
        raylet_client_(std::make_shared<MockRayletClient>()),
        task_resubmitter_(std::make_shared<MockTaskResubmitter>()),
        ref_counter_(std::make_shared<ReferenceCounter>(
            rpc::Address(), /*distributed_ref_counting_enabled=*/true,
            /*lineage_pinning_enabled=*/true)),
        manager_(rpc::Address(),
                 [&](const std::string &ip, int port) { return raylet_client_; },
                 raylet_client_,
                 [&](const ObjectID &object_id, const ObjectLookupCallback &callback) {
                   object_directory_->AsyncGetLocations(object_id, callback);
                   return Status::OK();
                 },
                 task_resubmitter_, ref_counter_, memory_store_,
                 [&](const ObjectID &object_id, bool pin_object) {
                   RAY_CHECK(failed_reconstructions_.count(object_id) == 0);
                   failed_reconstructions_[object_id] = pin_object;

                   std::string meta =
                       std::to_string(static_cast<int>(rpc::ErrorType::OBJECT_IN_PLASMA));
                   auto metadata = const_cast<uint8_t *>(
                       reinterpret_cast<const uint8_t *>(meta.data()));
                   auto meta_buffer =
                       std::make_shared<LocalMemoryBuffer>(metadata, meta.size());
                   auto data = RayObject(nullptr, meta_buffer, std::vector<ObjectID>());
                   RAY_CHECK(memory_store_->Put(data, object_id));
                 },
                 /*lineage_reconstruction_enabled=*/true) {}

  NodeID local_raylet_id_;
  std::unordered_map<ObjectID, bool> failed_reconstructions_;

  std::shared_ptr<MockObjectDirectory> object_directory_;
  std::shared_ptr<CoreWorkerMemoryStore> memory_store_;
  std::shared_ptr<MockRayletClient> raylet_client_;
  std::shared_ptr<MockTaskResubmitter> task_resubmitter_;
  std::shared_ptr<ReferenceCounter> ref_counter_;
  ObjectRecoveryManager manager_;
};

TEST_F(ObjectRecoveryManagerTest, TestNoReconstruction) {
  // Lineage recording disabled.
  ObjectID object_id = ObjectID::FromRandom();
  ref_counter_->AddOwnedObject(object_id, {}, rpc::Address(), "", 0, true);
  ASSERT_TRUE(manager_.RecoverObject(object_id));
  ASSERT_TRUE(failed_reconstructions_.empty());
  ASSERT_TRUE(object_directory_->Flush() == 1);
  ASSERT_TRUE(failed_reconstructions_.count(object_id) == 1);
  ASSERT_EQ(task_resubmitter_->num_tasks_resubmitted, 0);

  // Borrowed object.
  object_id = ObjectID::FromRandom();
  ref_counter_->AddLocalReference(object_id, "");
  ASSERT_TRUE(manager_.RecoverObject(object_id));
  ASSERT_TRUE(failed_reconstructions_.count(object_id) == 1);
  ASSERT_EQ(task_resubmitter_->num_tasks_resubmitted, 0);

  // Ref went out of scope.
  object_id = ObjectID::FromRandom();
  ASSERT_FALSE(manager_.RecoverObject(object_id));
  ASSERT_TRUE(failed_reconstructions_.count(object_id) == 0);
  ASSERT_EQ(task_resubmitter_->num_tasks_resubmitted, 0);
}

TEST_F(ObjectRecoveryManagerTest, TestPinNewCopy) {
  ObjectID object_id = ObjectID::FromRandom();
  ref_counter_->AddOwnedObject(object_id, {}, rpc::Address(), "", 0, true);
  std::vector<rpc::Address> addresses({rpc::Address()});
  object_directory_->SetLocations(object_id, addresses);

  ASSERT_TRUE(manager_.RecoverObject(object_id));
  ASSERT_TRUE(object_directory_->Flush() == 1);
  ASSERT_TRUE(raylet_client_->Flush() == 1);
  ASSERT_TRUE(failed_reconstructions_.empty());
  ASSERT_EQ(task_resubmitter_->num_tasks_resubmitted, 0);
}

TEST_F(ObjectRecoveryManagerTest, TestReconstruction) {
  ObjectID object_id = ObjectID::FromRandom();
  ref_counter_->AddOwnedObject(object_id, {}, rpc::Address(), "", 0, true);
  task_resubmitter_->AddTask(object_id.TaskId(), {});

  ASSERT_TRUE(manager_.RecoverObject(object_id));
  ASSERT_TRUE(object_directory_->Flush() == 1);

  ASSERT_TRUE(failed_reconstructions_.empty());
  ASSERT_EQ(task_resubmitter_->num_tasks_resubmitted, 1);
}

TEST_F(ObjectRecoveryManagerTest, TestReconstructionSuppression) {
  ObjectID object_id = ObjectID::FromRandom();
  ref_counter_->AddOwnedObject(object_id, {}, rpc::Address(), "", 0, true);
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
  ASSERT_TRUE(object_directory_->Flush() == 1);
  ASSERT_TRUE(raylet_client_->Flush() == 1);

  // The object has been marked as failed but it is still pinned on the new
  // node. Another attempt to recover the object will not trigger any
  // callbacks.
  ASSERT_TRUE(manager_.RecoverObject(object_id));
  ASSERT_EQ(object_directory_->Flush(), 0);

  // The object is removed and can be recovered again.
  auto objects = ref_counter_->ResetObjectsOnRemovedNode(remote_node_id);
  ASSERT_EQ(objects.size(), 1);
  ASSERT_EQ(objects[0], object_id);
  memory_store_->Delete(objects);
  ASSERT_TRUE(manager_.RecoverObject(object_id));
  ASSERT_TRUE(object_directory_->Flush() == 1);
}

TEST_F(ObjectRecoveryManagerTest, TestReconstructionChain) {
  std::vector<ObjectID> object_ids;
  std::vector<ObjectID> dependencies;
  for (int i = 0; i < 3; i++) {
    ObjectID object_id = ObjectID::FromRandom();
    ref_counter_->AddOwnedObject(object_id, {}, rpc::Address(), "", 0, true);
    task_resubmitter_->AddTask(object_id.TaskId(), dependencies);
    dependencies = {object_id};
    object_ids.push_back(object_id);
  }

  ASSERT_TRUE(manager_.RecoverObject(object_ids.back()));
  for (int i = 0; i < 3; i++) {
    RAY_LOG(INFO) << i;
    ASSERT_EQ(object_directory_->Flush(), 1);
    ASSERT_TRUE(failed_reconstructions_.empty());
    ASSERT_EQ(task_resubmitter_->num_tasks_resubmitted, i + 1);
  }
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
