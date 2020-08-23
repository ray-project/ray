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

#include "ray/core_worker/actor_manager.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "ray/common/task/task_spec.h"
#include "ray/common/test_util.h"
#include "ray/core_worker/reference_count.h"
#include "ray/core_worker/transport/direct_actor_transport.h"
#include "ray/gcs/redis_accessor.h"
#include "ray/gcs/redis_gcs_client.h"

namespace ray {

using ::testing::_;

class MockActorInfoAccessor : public gcs::RedisActorInfoAccessor {
 public:
  MockActorInfoAccessor(gcs::RedisGcsClient *client)
      : gcs::RedisActorInfoAccessor(client) {}

  ~MockActorInfoAccessor() {}

  ray::Status AsyncSubscribe(
      const ActorID &actor_id,
      const gcs::SubscribeCallback<ActorID, rpc::ActorTableData> &subscribe,
      const gcs::StatusCallback &done) {
    auto callback_entry = std::make_pair(actor_id, subscribe);
    callback_map_.emplace(actor_id, subscribe);
    return Status::OK();
  }

  bool ActorStateNotificationPublished(const ActorID &actor_id,
                                       const gcs::ActorTableData &actor_data) {
    auto it = callback_map_.find(actor_id);
    if (it == callback_map_.end()) return false;
    auto actor_state_notification_callback = it->second;
    actor_state_notification_callback(actor_id, actor_data);
    return true;
  }

  bool CheckSubscriptionRequested(const ActorID &actor_id) {
    return callback_map_.find(actor_id) != callback_map_.end();
  }

  absl::flat_hash_map<ActorID, gcs::SubscribeCallback<ActorID, rpc::ActorTableData>>
      callback_map_;
};

class MockGcsClient : public gcs::RedisGcsClient {
 public:
  MockGcsClient(const gcs::GcsClientOptions &options) : gcs::RedisGcsClient(options) {}

  void Init(MockActorInfoAccessor *actor_accesor_mock) {
    actor_accessor_.reset(actor_accesor_mock);
  }

  ~MockGcsClient() {}
};

class MockDirectActorSubmitter : public CoreWorkerDirectActorTaskSubmitterInterface {
 public:
  MockDirectActorSubmitter() : CoreWorkerDirectActorTaskSubmitterInterface() {}

  MOCK_METHOD1(AddActorQueueIfNotExists, void(const ActorID &actor_id));
  MOCK_METHOD3(ConnectActor, void(const ActorID &actor_id, const rpc::Address &address,
                                  int64_t num_restarts));
  MOCK_METHOD3(DisconnectActor,
               void(const ActorID &actor_id, int64_t num_restarts, bool dead));
  MOCK_METHOD3(KillActor,
               void(const ActorID &actor_id, bool force_kill, bool no_restart));

  virtual ~MockDirectActorSubmitter() {}
};

class MockReferenceCounter : public ReferenceCounterInterface {
 public:
  MockReferenceCounter() : ReferenceCounterInterface() {}

  MOCK_METHOD2(AddLocalReference,
               void(const ObjectID &object_id, const std::string &call_sit));

  MOCK_METHOD3(AddBorrowedObject,
               bool(const ObjectID &object_id, const ObjectID &outer_id,
                    const rpc::Address &owner_address));

  MOCK_METHOD7(AddOwnedObject,
               void(const ObjectID &object_id, const std::vector<ObjectID> &contained_ids,
                    const rpc::Address &owner_address, const std::string &call_site,
                    const int64_t object_size, bool is_reconstructable,
                    const absl::optional<ClientID> &pinned_at_raylet_id));

  MOCK_METHOD2(SetDeleteCallback,
               bool(const ObjectID &object_id,
                    const std::function<void(const ObjectID &)> callback));

  virtual ~MockReferenceCounter() {}
};

class ActorManagerTest : public ::testing::Test {
 public:
  ActorManagerTest()
      : options_("", 1, ""),
        gcs_client_mock_(new MockGcsClient(options_)),
        actor_info_accessor_(new MockActorInfoAccessor(gcs_client_mock_.get())),
        direct_actor_submitter_(new MockDirectActorSubmitter()),
        reference_counter_(new MockReferenceCounter()) {
    gcs_client_mock_->Init(actor_info_accessor_);
  }

  ~ActorManagerTest() {}

  void SetUp() {
    actor_manager_ = std::make_shared<ActorManager>(
        gcs_client_mock_, direct_actor_submitter_, reference_counter_);
  }

  void TearDown() { actor_manager_.reset(); }

  ActorID AddActorHandle() const {
    JobID job_id = JobID::FromInt(1);
    const TaskID task_id = TaskID::ForDriverTask(job_id);
    ActorID actor_id = ActorID::Of(job_id, task_id, 1);
    const auto caller_address = rpc::Address();
    const auto call_site = "";
    RayFunction function(ray::Language::PYTHON,
                         ray::FunctionDescriptorBuilder::BuildPython("", "", "", ""));

    auto actor_handle = absl::make_unique<ActorHandle>(
        actor_id, TaskID::Nil(), rpc::Address(), job_id, ObjectID::FromRandom(),
        function.GetLanguage(), function.GetFunctionDescriptor(), "", 0);
    EXPECT_CALL(*reference_counter_, SetDeleteCallback(_, _))
        .WillRepeatedly(testing::Return(true));
    actor_manager_->AddNewActorHandle(move(actor_handle), task_id, call_site,
                                      caller_address, /*is_detached*/ false);
    return actor_id;
  }

  gcs::GcsClientOptions options_;
  std::shared_ptr<MockGcsClient> gcs_client_mock_;
  MockActorInfoAccessor *actor_info_accessor_;
  std::shared_ptr<MockDirectActorSubmitter> direct_actor_submitter_;
  std::shared_ptr<MockReferenceCounter> reference_counter_;
  std::shared_ptr<ActorManager> actor_manager_;
};

TEST_F(ActorManagerTest, TestAddAndGetActorHandleEndToEnd) {
  JobID job_id = JobID::FromInt(1);
  const TaskID task_id = TaskID::ForDriverTask(job_id);
  ActorID actor_id = ActorID::Of(job_id, task_id, 1);
  const auto caller_address = rpc::Address();
  const auto call_site = "";
  RayFunction function(ray::Language::PYTHON,
                       ray::FunctionDescriptorBuilder::BuildPython("", "", "", ""));
  auto actor_handle = absl::make_unique<ActorHandle>(
      actor_id, TaskID::Nil(), rpc::Address(), job_id, ObjectID::FromRandom(),
      function.GetLanguage(), function.GetFunctionDescriptor(), "", 0);
  EXPECT_CALL(*reference_counter_, SetDeleteCallback(_, _))
      .WillRepeatedly(testing::Return(true));

  // Add an actor handle.
  ASSERT_TRUE(actor_manager_->AddNewActorHandle(move(actor_handle), task_id, call_site,
                                                caller_address, false));
  // Make sure the subscription request is sent to GCS.
  ASSERT_TRUE(actor_info_accessor_->CheckSubscriptionRequested(actor_id));
  ASSERT_TRUE(actor_manager_->CheckActorHandleExists(actor_id));

  auto actor_handle2 = absl::make_unique<ActorHandle>(
      actor_id, TaskID::Nil(), rpc::Address(), job_id, ObjectID::FromRandom(),
      function.GetLanguage(), function.GetFunctionDescriptor(), "", 0);
  // Make sure the same actor id adding will return false.
  ASSERT_FALSE(actor_manager_->AddNewActorHandle(move(actor_handle2), task_id, call_site,
                                                 caller_address, false));
  // Make sure we can get an actor handle correctly.
  const std::unique_ptr<ActorHandle> &actor_handle_to_get =
      actor_manager_->GetActorHandle(actor_id);
  ASSERT_TRUE(actor_handle_to_get->GetActorID() == actor_id);

  // Check after the actor is created, if it is connected to an actor.
  EXPECT_CALL(*direct_actor_submitter_, ConnectActor(_, _, _)).Times(1);
  rpc::ActorTableData actor_table_data;
  actor_table_data.set_actor_id(actor_id.Binary());
  actor_table_data.set_state(rpc::ActorTableData::ALIVE);
  actor_info_accessor_->ActorStateNotificationPublished(actor_id, actor_table_data);

  // Now actor state is updated to DEAD. Make sure it is diconnected.
  EXPECT_CALL(*direct_actor_submitter_, DisconnectActor(_, _, _)).Times(1);
  actor_table_data.set_actor_id(actor_id.Binary());
  actor_table_data.set_state(rpc::ActorTableData::DEAD);
  actor_info_accessor_->ActorStateNotificationPublished(actor_id, actor_table_data);
}

TEST_F(ActorManagerTest, TestCheckActorHandleDoesntExists) {
  JobID job_id = JobID::FromInt(2);
  const TaskID task_id = TaskID::ForDriverTask(job_id);
  ActorID actor_id = ActorID::Of(job_id, task_id, 1);
  ASSERT_FALSE(actor_manager_->CheckActorHandleExists(actor_id));
}

TEST_F(ActorManagerTest, RegisterActorHandles) {
  JobID job_id = JobID::FromInt(1);
  const TaskID task_id = TaskID::ForDriverTask(job_id);
  ActorID actor_id = ActorID::Of(job_id, task_id, 1);
  const auto caller_address = rpc::Address();
  const auto call_site = "";
  RayFunction function(ray::Language::PYTHON,
                       ray::FunctionDescriptorBuilder::BuildPython("", "", "", ""));
  auto actor_handle = absl::make_unique<ActorHandle>(
      actor_id, TaskID::Nil(), rpc::Address(), job_id, ObjectID::FromRandom(),
      function.GetLanguage(), function.GetFunctionDescriptor(), "", 0);
  EXPECT_CALL(*reference_counter_, SetDeleteCallback(_, _))
      .WillRepeatedly(testing::Return(true));
  ObjectID outer_object_id = ObjectID::Nil();

  // Sinece RegisterActor happens in a non-owner worker, we should
  // make sure it borrows an object.
  EXPECT_CALL(*reference_counter_, AddBorrowedObject(_, _, _));
  ActorID returned_actor_id = actor_manager_->RegisterActorHandle(
      std::move(actor_handle), outer_object_id, task_id, call_site, caller_address);
  ASSERT_TRUE(returned_actor_id == actor_id);
  // Let's try to get the handle and make sure it works.
  const std::unique_ptr<ActorHandle> &actor_handle_to_get =
      actor_manager_->GetActorHandle(actor_id);
  ASSERT_TRUE(actor_handle_to_get->GetActorID() == actor_id);
  ASSERT_TRUE(actor_handle_to_get->CreationJobID() == job_id);
}

TEST_F(ActorManagerTest, TestActorStateNotificationPending) {
  ActorID actor_id = AddActorHandle();
  // Nothing happens if state is pending.
  EXPECT_CALL(*direct_actor_submitter_, ConnectActor(_, _, _)).Times(0);
  EXPECT_CALL(*direct_actor_submitter_, DisconnectActor(_, _, _)).Times(0);
  rpc::ActorTableData actor_table_data;
  actor_table_data.set_actor_id(actor_id.Binary());
  actor_table_data.set_state(rpc::ActorTableData::PENDING_CREATION);
  ASSERT_TRUE(
      actor_info_accessor_->ActorStateNotificationPublished(actor_id, actor_table_data));
}

TEST_F(ActorManagerTest, TestActorStateNotificationRestarting) {
  ActorID actor_id = AddActorHandle();
  // Should disconnect to an actor when actor is restarting.
  EXPECT_CALL(*direct_actor_submitter_, ConnectActor(_, _, _)).Times(0);
  EXPECT_CALL(*direct_actor_submitter_, DisconnectActor(_, _, _)).Times(1);
  rpc::ActorTableData actor_table_data;
  actor_table_data.set_actor_id(actor_id.Binary());
  actor_table_data.set_state(rpc::ActorTableData::RESTARTING);
  ASSERT_TRUE(
      actor_info_accessor_->ActorStateNotificationPublished(actor_id, actor_table_data));
}

TEST_F(ActorManagerTest, TestActorStateNotificationDead) {
  ActorID actor_id = AddActorHandle();
  // Should disconnect to an actor when actor is dead.
  EXPECT_CALL(*direct_actor_submitter_, ConnectActor(_, _, _)).Times(0);
  EXPECT_CALL(*direct_actor_submitter_, DisconnectActor(_, _, _)).Times(1);
  rpc::ActorTableData actor_table_data;
  actor_table_data.set_actor_id(actor_id.Binary());
  actor_table_data.set_state(rpc::ActorTableData::DEAD);
  ASSERT_TRUE(
      actor_info_accessor_->ActorStateNotificationPublished(actor_id, actor_table_data));
}

TEST_F(ActorManagerTest, TestActorStateNotificationAlive) {
  ActorID actor_id = AddActorHandle();
  // Should connect to an actor when actor is alive.
  EXPECT_CALL(*direct_actor_submitter_, ConnectActor(_, _, _)).Times(1);
  EXPECT_CALL(*direct_actor_submitter_, DisconnectActor(_, _, _)).Times(0);
  rpc::ActorTableData actor_table_data;
  actor_table_data.set_actor_id(actor_id.Binary());
  actor_table_data.set_state(rpc::ActorTableData::ALIVE);
  ASSERT_TRUE(
      actor_info_accessor_->ActorStateNotificationPublished(actor_id, actor_table_data));
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
