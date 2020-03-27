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

#include "ray/core_worker/transport/direct_task_transport.h"

#include "gtest/gtest.h"
#include "ray/common/task/task_spec.h"
#include "ray/common/task/task_util.h"
#include "ray/common/test_util.h"
#include "ray/core_worker/object_recovery_manager.h"
#include "ray/core_worker/store_provider/memory_store/memory_store.h"
#include "ray/raylet/raylet_client.h"

namespace ray {

// Used to prevent leases from timing out when not testing that logic. It would
// be better to use a mock clock or lease manager interface, but that's high
// overhead for the very simple timeout logic we currently have.
int64_t kLongTimeout = 1024 * 1024 * 1024;

class MockPinnedObjects : public PinnedObjectsInterface {
 public:
  MockPinnedObjects() {}

  void MarkPlasmaObjectsPinnedAt(const std::vector<ObjectID> &plasma_returns_in_scope,
                                 const ClientID &node_id) override {
    for (const auto &return_id : plasma_returns_in_scope) {
      RAY_CHECK(pinned_objects.emplace(return_id, node_id).second);
    }
  }

  const bool IsPlasmaObjectPinned(const ObjectID &object_id, bool *pinned) override {
    *pinned = pinned_objects.count(object_id) > 0;
    return true;
  }

  std::unordered_map<ObjectID, ClientID> pinned_objects;
};

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
  ray::Status PinObjectIDs(
      const rpc::Address &caller_address, const std::vector<ObjectID> &object_ids,
      const ray::rpc::ClientCallback<ray::rpc::PinObjectIDsReply> &callback) override {
    RAY_LOG(INFO) << "PinObjectIDs " << object_ids.size();
    callbacks.push_back(callback);
    return Status::OK();
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
    callbacks.clear();
    return flushed;
  }

  std::vector<std::pair<ObjectID, ObjectLookupCallback>> callbacks = {};
  std::unordered_map<ObjectID, std::vector<rpc::Address>> locations;
};

class ObjectRecoveryManagerUnitTest : public ::testing::Test {
 public:
  ObjectRecoveryManagerUnitTest()
      : object_directory_(std::make_shared<MockObjectDirectory>()),
        memory_store_(std::make_shared<CoreWorkerMemoryStore>()),
        raylet_client_(std::make_shared<MockRayletClient>()),
        task_resubmitter_(std::make_shared<MockTaskResubmitter>()),
        pinned_objects_(std::make_shared<MockPinnedObjects>()),
        manager_(rpc::Address(),
                 [&](const std::string &ip, int port) { return raylet_client_; },
                 raylet_client_,
                 [&](const ObjectID &object_id, const ObjectLookupCallback &callback) {
                   object_directory_->AsyncGetLocations(object_id, callback);
                   return Status::OK();
                 },
                 task_resubmitter_, pinned_objects_, memory_store_,
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

  std::unordered_map<ObjectID, bool> failed_reconstructions_;

  std::shared_ptr<MockObjectDirectory> object_directory_;
  std::shared_ptr<CoreWorkerMemoryStore> memory_store_;
  std::shared_ptr<MockRayletClient> raylet_client_;
  std::shared_ptr<MockTaskResubmitter> task_resubmitter_;
  std::shared_ptr<MockPinnedObjects> pinned_objects_;
  ObjectRecoveryManager manager_;
};

TEST_F(ObjectRecoveryManagerUnitTest, TestNoReconstruction) {
  ObjectID object_id = ObjectID::FromRandom();
  ASSERT_TRUE(manager_.RecoverObject(object_id).ok());
  ASSERT_TRUE(failed_reconstructions_.empty());
  ASSERT_TRUE(object_directory_->Flush() == 1);
  ASSERT_TRUE(failed_reconstructions_.count(object_id) == 1);
  ASSERT_EQ(task_resubmitter_->num_tasks_resubmitted, 0);
}

TEST_F(ObjectRecoveryManagerUnitTest, TestPinNewCopy) {
  ObjectID object_id = ObjectID::FromRandom();
  std::vector<rpc::Address> addresses({rpc::Address()});
  object_directory_->SetLocations(object_id, addresses);

  ASSERT_TRUE(manager_.RecoverObject(object_id).ok());
  ASSERT_TRUE(object_directory_->Flush() == 1);
  ASSERT_TRUE(raylet_client_->Flush() == 1);
  ASSERT_TRUE(failed_reconstructions_.empty());
  ASSERT_EQ(task_resubmitter_->num_tasks_resubmitted, 0);
}

TEST_F(ObjectRecoveryManagerUnitTest, TestReconstruction) {
  ObjectID object_id = ObjectID::FromRandom();
  task_resubmitter_->AddTask(object_id.TaskId(), {});

  ASSERT_TRUE(manager_.RecoverObject(object_id).ok());
  ASSERT_TRUE(object_directory_->Flush() == 1);

  ASSERT_TRUE(failed_reconstructions_.empty());
  ASSERT_EQ(task_resubmitter_->num_tasks_resubmitted, 1);
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
