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
#include "ray/core_worker/actor_reporter.h"
#include "ray/core_worker/reference_count.h"
#include "ray/core_worker/transport/direct_actor_transport.h"
#include "ray/gcs/redis_accessor.h"
#include "ray/gcs/redis_gcs_client.h"

namespace ray {

using ::testing::_;

std::shared_ptr<ActorHandle> CreateActorHandle(ActorID &actor_id, JobID &job_id) {
  RayFunction function(ray::Language::PYTHON,
                       ray::FunctionDescriptorBuilder::BuildPython("", "", "", ""));
  return std::make_shared<ActorHandle>(actor_id, TaskID::Nil(), rpc::Address(), job_id,
                                       ObjectID::FromRandom(), function.GetLanguage(),
                                       function.GetFunctionDescriptor(), "", 0);
}

class MockActorInfoAccessor : public gcs::RedisActorInfoAccessor {
 public:
  MockActorInfoAccessor(gcs::RedisGcsClient *client)
      : gcs::RedisActorInfoAccessor(client) {}

  MOCK_METHOD2(AsyncRegister,
               ray::Status(const std::shared_ptr<gcs::ActorTableData> &data_ptr,
                           const gcs::StatusCallback &callback));
  MOCK_METHOD3(
      AsyncSubscribe,
      ray::Status(const ActorID &actor_id,
                  const gcs::SubscribeCallback<ActorID, rpc::ActorTableData> &subscribe,
                  const gcs::StatusCallback &done));

  ~MockActorInfoAccessor() {}
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
  MOCK_METHOD2(ConnectActor, void(const ActorID &actor_id, const rpc::Address &address));
  MOCK_METHOD2(DisconnectActor, void(const ActorID &actor_id, bool dead));
  MOCK_METHOD3(KillActor,
               void(const ActorID &actor_id, bool force_kill, bool no_restart));

  virtual ~MockDirectActorSubmitter() {}
};

class MockReferenceCounter : public ReferenceCounterInterface {
 public:
  MockReferenceCounter() : ReferenceCounterInterface() {}

  MOCK_METHOD2(AddLocalReference,
               void(const ObjectID &object_id, const std::string &call_sit));
  MOCK_METHOD4(AddBorrowedObject,
               bool(const ObjectID &object_id, const ObjectID &outer_id,
                    const TaskID &owner_id, const rpc::Address &owner_address));
  MOCK_METHOD8(AddOwnedObject,
               void(const ObjectID &object_id, const std::vector<ObjectID> &contained_ids,
                    const TaskID &owner_id, const rpc::Address &owner_address,
                    const std::string &call_site, const int64_t object_size,
                    bool is_reconstructable,
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

  gcs::GcsClientOptions options_;
  std::shared_ptr<MockGcsClient> gcs_client_mock_;
  MockActorInfoAccessor *actor_info_accessor_;
  std::shared_ptr<MockDirectActorSubmitter> direct_actor_submitter_;
  std::shared_ptr<MockReferenceCounter> reference_counter_;
  std::shared_ptr<ActorManager> actor_manager_;
};

TEST_F(ActorManagerTest, TestAddAndGetActorHandle) {
  JobID job_id = JobID::FromInt(1);
  const TaskID task_id = TaskID::ForDriverTask(job_id);
  ActorID actor_id = ActorID::Of(job_id, task_id, 1);
  const auto caller_address = rpc::Address();
  const auto call_site = "";
  std::shared_ptr<ActorHandle> actor_handle = CreateActorHandle(actor_id, job_id);
  EXPECT_CALL(*actor_info_accessor_, AsyncSubscribe(_, _, _))
      .WillRepeatedly(testing::Return(Status::OK()));
  EXPECT_CALL(*reference_counter_, SetDeleteCallback(_, _))
      .WillRepeatedly(testing::Return(true));

  // Add an actor handle.
  ASSERT_TRUE(actor_manager_->AddActorHandle(move(actor_handle), true, task_id, call_site,
                                             caller_address));
  std::shared_ptr<ActorHandle> actor_handle2 = CreateActorHandle(actor_id, job_id);
  // Make sure the same actor id adding will fail.
  ASSERT_FALSE(actor_manager_->AddActorHandle(move(actor_handle2), true, task_id,
                                              call_site, caller_address));
  // Make sure we can get an actor handle correctly.
  ActorHandle *actor_handle_to_get = nullptr;
  ASSERT_TRUE(actor_manager_->GetActorHandle(actor_id, &actor_handle_to_get).ok());
  ASSERT_TRUE(actor_handle_to_get->GetActorID() == actor_id);
}

TEST_F(ActorManagerTest, TestGetNotExistingActorHandles) {
  // Get actor handle that does not exist should fail.
  JobID job_id = JobID::FromInt(2);
  const TaskID task_id = TaskID::ForDriverTask(job_id);
  ActorID actor_id = ActorID::Of(job_id, task_id, 1);
  ActorHandle *actor_handle_that_doesnt_exist = nullptr;
  ASSERT_TRUE(actor_manager_->GetActorHandle(actor_id, &actor_handle_that_doesnt_exist)
                  .IsInvalid());
}

TEST_F(ActorManagerTest, TestSerializeAndDeserializeActorHandles) {
  JobID job_id = JobID::FromInt(1);
  const TaskID task_id = TaskID::ForDriverTask(job_id);
  ActorID actor_id = ActorID::Of(job_id, task_id, 1);
  const auto caller_address = rpc::Address();
  const auto call_site = "";
  EXPECT_CALL(*actor_info_accessor_, AsyncSubscribe(_, _, _))
      .WillRepeatedly(testing::Return(Status::OK()));
  EXPECT_CALL(*reference_counter_, SetDeleteCallback(_, _))
      .WillRepeatedly(testing::Return(true));

  // Make sure serialization fails when actor handle is not registered
  std::string serialized_bytes;
  ObjectID actor_handle_id;
  ASSERT_TRUE(
      actor_manager_->SerializeActorHandle(actor_id, &serialized_bytes, &actor_handle_id)
          .IsInvalid());
  // Add an actor handle.
  std::shared_ptr<ActorHandle> actor_handle = CreateActorHandle(actor_id, job_id);
  ASSERT_TRUE(actor_manager_->AddActorHandle(move(actor_handle), true, task_id, call_site,
                                             caller_address));
  // Serialize actor_handle first
  ASSERT_TRUE(
      actor_manager_->SerializeActorHandle(actor_id, &serialized_bytes, &actor_handle_id)
          .ok());
  // Make sure deserialization & register works.
  ObjectID outer_object_id = ObjectID::Nil();
  // Sinece DeserializeAndRegisterActorHandle happens in a non-owner worker, we should
  // make sure it borrows an object.
  EXPECT_CALL(*reference_counter_, AddBorrowedObject(_, _, _, _));
  ActorID returned_actor_id = actor_manager_->DeserializeAndRegisterActorHandle(
      serialized_bytes, outer_object_id, task_id, call_site, caller_address);
  ASSERT_TRUE(returned_actor_id == actor_id);
  // Let's try to get the handle and make sure it works.
  ActorHandle *actor_handle_to_get = nullptr;
  ASSERT_TRUE(actor_manager_->GetActorHandle(actor_id, &actor_handle_to_get).ok());
  ASSERT_TRUE(actor_handle_to_get->GetActorID() == actor_id);
  ASSERT_TRUE(actor_handle_to_get->CreationJobID() == job_id);
}

TEST_F(ActorManagerTest, TestActorStateNotificationPending) {
  JobID job_id = JobID::FromInt(1);
  const TaskID task_id = TaskID::ForDriverTask(job_id);
  ActorID actor_id = ActorID::Of(job_id, task_id, 1);

  // Nothing happens if state is pending.
  EXPECT_CALL(*direct_actor_submitter_, ConnectActor(_, _)).Times(0);
  EXPECT_CALL(*direct_actor_submitter_, DisconnectActor(_, _)).Times(0);
  rpc::ActorTableData actor_table_data;
  actor_table_data.set_actor_id(actor_id.Binary());
  actor_table_data.set_state(
      rpc::ActorTableData_ActorState::ActorTableData_ActorState_PENDING);
  actor_manager_->HandleActorStateNotification(actor_id, actor_table_data);
}

TEST_F(ActorManagerTest, TestActorStateNotificationRestarting) {
  JobID job_id = JobID::FromInt(1);
  const TaskID task_id = TaskID::ForDriverTask(job_id);
  ActorID actor_id = ActorID::Of(job_id, task_id, 1);

  // Should disconnect to an actor when actor is restarting.
  EXPECT_CALL(*direct_actor_submitter_, ConnectActor(_, _)).Times(0);
  EXPECT_CALL(*direct_actor_submitter_, DisconnectActor(_, _)).Times(1);
  rpc::ActorTableData actor_table_data;
  actor_table_data.set_actor_id(actor_id.Binary());
  actor_table_data.set_state(
      rpc::ActorTableData_ActorState::ActorTableData_ActorState_RESTARTING);
  actor_manager_->HandleActorStateNotification(actor_id, actor_table_data);
}

TEST_F(ActorManagerTest, TestActorStateNotificationDead) {
  JobID job_id = JobID::FromInt(1);
  const TaskID task_id = TaskID::ForDriverTask(job_id);
  ActorID actor_id = ActorID::Of(job_id, task_id, 1);

  // Should disconnect to an actor when actor is dead.
  EXPECT_CALL(*direct_actor_submitter_, ConnectActor(_, _)).Times(0);
  EXPECT_CALL(*direct_actor_submitter_, DisconnectActor(_, _)).Times(1);
  rpc::ActorTableData actor_table_data;
  actor_table_data.set_actor_id(actor_id.Binary());
  actor_table_data.set_state(
      rpc::ActorTableData_ActorState::ActorTableData_ActorState_DEAD);
  actor_manager_->HandleActorStateNotification(actor_id, actor_table_data);
}

TEST_F(ActorManagerTest, TestActorStateNotificationAlive) {
  JobID job_id = JobID::FromInt(1);
  const TaskID task_id = TaskID::ForDriverTask(job_id);
  ActorID actor_id = ActorID::Of(job_id, task_id, 1);

  // Should connect to an actor when actor is alive.
  EXPECT_CALL(*direct_actor_submitter_, ConnectActor(_, _)).Times(1);
  EXPECT_CALL(*direct_actor_submitter_, DisconnectActor(_, _)).Times(0);
  rpc::ActorTableData actor_table_data;
  actor_table_data.set_actor_id(actor_id.Binary());
  actor_table_data.set_state(
      rpc::ActorTableData_ActorState::ActorTableData_ActorState_ALIVE);
  actor_manager_->HandleActorStateNotification(actor_id, actor_table_data);
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
