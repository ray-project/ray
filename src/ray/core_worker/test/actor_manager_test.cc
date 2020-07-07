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

class MockActorInfoAccessor : public gcs::RedisActorInfoAccessor {
 public:
  MockActorInfoAccessor(gcs::RedisGcsClient *client)
      : gcs::RedisActorInfoAccessor(client) {}

  ~MockActorInfoAccessor() {}

  ray::Status AsyncSubscribe(
      const ActorID &actor_id,
      const gcs::SubscribeCallback<ActorID, rpc::ActorTableData> &subscribe,
      const gcs::StatusCallback &done) override {
    auto callback_entry = std::make_pair(actor_id, subscribe);
    callback_map_.emplace(actor_id, subscribe);
    return Status::OK();
  }

  ray::Status AsyncGet(
      const ActorID &actor_id,
      const gcs::OptionalItemCallback<rpc::ActorTableData> &callback) override {
    async_get_callback_map.emplace(actor_id, callback);
    return Status::OK();
  }

  void InvokeAsyncGetCallback(const ActorID &actor_id, ray::Status status,
                              const boost::optional<gcs::ActorTableData> &result) {
    auto it = async_get_callback_map.find(actor_id);
    RAY_CHECK(it != async_get_callback_map.end());
    auto callback = it->second;
    callback(status, result);
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

  absl::flat_hash_map<ActorID, gcs::OptionalItemCallback<rpc::ActorTableData>>
      async_get_callback_map;
};

class MockWorkerInfoAccessor : public gcs::RedisWorkerInfoAccessor {
 public:
  MockWorkerInfoAccessor(gcs::RedisGcsClient *client)
      : gcs::RedisWorkerInfoAccessor(client) {}

  ~MockWorkerInfoAccessor() {}

  ray::Status AsyncGet(
      const WorkerID &worker_id,
      const gcs::OptionalItemCallback<rpc::WorkerTableData> &callback) override {
    async_get_callback_map.emplace(worker_id, callback);
    return Status::OK();
  }

  void InvokeAsyncGetWorkerCallback(const WorkerID &worker_id, ray::Status status,
                                    const boost::optional<rpc::WorkerTableData> &result) {
    auto it = async_get_callback_map.find(worker_id);
    RAY_CHECK(it != async_get_callback_map.end());
    auto callback = it->second;
    callback(status, result);
  }

  absl::flat_hash_map<WorkerID, gcs::OptionalItemCallback<rpc::WorkerTableData>>
      async_get_callback_map;
};

class MockNodeInfoAccessor : public gcs::RedisNodeInfoAccessor {
 public:
  MockNodeInfoAccessor(gcs::RedisGcsClient *client)
      : gcs::RedisNodeInfoAccessor(client) {}

  ~MockNodeInfoAccessor() {}

  MOCK_CONST_METHOD1(Get, boost::optional<rpc::GcsNodeInfo>(const ClientID &node_id));
};

class MockGcsClient : public gcs::RedisGcsClient {
 public:
  MockGcsClient(const gcs::GcsClientOptions &options) : gcs::RedisGcsClient(options) {}

  void Init(MockActorInfoAccessor *actor_accesor_mock,
            MockWorkerInfoAccessor *worker_accessor_mock,
            MockNodeInfoAccessor *node_accessor_mock) {
    actor_accessor_.reset(actor_accesor_mock);
    node_accessor_.reset(node_accessor_mock);
    worker_accessor_.reset(worker_accessor_mock);
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
        worker_info_accessor_(new MockWorkerInfoAccessor(gcs_client_mock_.get())),
        node_info_accessor_(new MockNodeInfoAccessor(gcs_client_mock_.get())),
        direct_actor_submitter_(new MockDirectActorSubmitter()),
        reference_counter_(new MockReferenceCounter()) {
    gcs_client_mock_->Init(actor_info_accessor_, worker_info_accessor_,
                           node_info_accessor_);
  }

  ~ActorManagerTest() {}

  void SetUp() {
    actor_manager_ = std::make_shared<ActorManager>(
        gcs_client_mock_, direct_actor_submitter_, reference_counter_);
  }

  void TearDown() { actor_manager_.reset(); }

  ActorID AddActorHandle(WorkerID worker_id) const {
    JobID job_id = JobID::FromInt(1);
    const TaskID task_id = TaskID::ForDriverTask(job_id);
    ActorID actor_id = ActorID::Of(job_id, task_id, 1);
    rpc::Address owner_address;
    owner_address.set_worker_id(worker_id.Binary());
    const auto call_site = "";
    RayFunction function(ray::Language::PYTHON,
                         ray::FunctionDescriptorBuilder::BuildPython("", "", "", ""));

    auto actor_handle = absl::make_unique<ActorHandle>(
        actor_id, TaskID::Nil(), owner_address, job_id, ObjectID::FromRandom(),
        function.GetLanguage(), function.GetFunctionDescriptor(), "", 0);
    EXPECT_CALL(*reference_counter_, SetDeleteCallback(_, _))
        .WillRepeatedly(testing::Return(true));
    actor_manager_->AddNewActorHandle(move(actor_handle), task_id, call_site,
                                      owner_address, /*is_detached*/ false);
    return actor_id;
  }

  // This function accesses a private member of actor_manager_ to
  // check if actor_id is persisted to the actor_resolution_map.
  bool CheckActorIdInResolutionMap(const ActorID &actor_id) {
    absl::MutexLock lock(&actor_manager_->mutex_);
    auto it = actor_manager_->actors_pending_location_resolution_.find(actor_id);
    return it != actor_manager_->actors_pending_location_resolution_.end();
  }

  gcs::GcsClientOptions options_;
  std::shared_ptr<MockGcsClient> gcs_client_mock_;
  MockActorInfoAccessor *actor_info_accessor_;
  MockWorkerInfoAccessor *worker_info_accessor_;
  MockNodeInfoAccessor *node_info_accessor_;
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
  EXPECT_CALL(*direct_actor_submitter_, ConnectActor(_, _)).Times(1);
  rpc::ActorTableData actor_table_data;
  actor_table_data.set_actor_id(actor_id.Binary());
  actor_table_data.set_state(
      rpc::ActorTableData_ActorState::ActorTableData_ActorState_ALIVE);
  actor_info_accessor_->ActorStateNotificationPublished(actor_id, actor_table_data);

  // Now actor state is updated to DEAD. Make sure it is diconnected.
  EXPECT_CALL(*direct_actor_submitter_, DisconnectActor(_, _)).Times(1);
  actor_table_data.set_actor_id(actor_id.Binary());
  actor_table_data.set_state(
      rpc::ActorTableData_ActorState::ActorTableData_ActorState_DEAD);
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
  ActorID actor_id = AddActorHandle(WorkerID::FromRandom());
  // Nothing happens if state is pending.
  EXPECT_CALL(*direct_actor_submitter_, ConnectActor(_, _)).Times(0);
  EXPECT_CALL(*direct_actor_submitter_, DisconnectActor(_, _)).Times(0);
  rpc::ActorTableData actor_table_data;
  actor_table_data.set_actor_id(actor_id.Binary());
  actor_table_data.set_state(
      rpc::ActorTableData_ActorState::ActorTableData_ActorState_PENDING);
  ASSERT_TRUE(
      actor_info_accessor_->ActorStateNotificationPublished(actor_id, actor_table_data));
}

TEST_F(ActorManagerTest, TestActorStateNotificationRestarting) {
  ActorID actor_id = AddActorHandle(WorkerID::FromRandom());
  // Should disconnect to an actor when actor is restarting.
  EXPECT_CALL(*direct_actor_submitter_, ConnectActor(_, _)).Times(0);
  EXPECT_CALL(*direct_actor_submitter_, DisconnectActor(_, _)).Times(1);
  rpc::ActorTableData actor_table_data;
  actor_table_data.set_actor_id(actor_id.Binary());
  actor_table_data.set_state(
      rpc::ActorTableData_ActorState::ActorTableData_ActorState_RESTARTING);
  ASSERT_TRUE(
      actor_info_accessor_->ActorStateNotificationPublished(actor_id, actor_table_data));
}

TEST_F(ActorManagerTest, TestActorStateNotificationDead) {
  ActorID actor_id = AddActorHandle(WorkerID::FromRandom());
  // Should disconnect to an actor when actor is dead.
  EXPECT_CALL(*direct_actor_submitter_, ConnectActor(_, _)).Times(0);
  EXPECT_CALL(*direct_actor_submitter_, DisconnectActor(_, _)).Times(1);
  rpc::ActorTableData actor_table_data;
  actor_table_data.set_actor_id(actor_id.Binary());
  actor_table_data.set_state(
      rpc::ActorTableData_ActorState::ActorTableData_ActorState_DEAD);
  ASSERT_TRUE(
      actor_info_accessor_->ActorStateNotificationPublished(actor_id, actor_table_data));
}

TEST_F(ActorManagerTest, TestActorStateNotificationAlive) {
  ActorID actor_id = AddActorHandle(WorkerID::FromRandom());
  // Should connect to an actor when actor is alive.
  EXPECT_CALL(*direct_actor_submitter_, ConnectActor(_, _)).Times(1);
  EXPECT_CALL(*direct_actor_submitter_, DisconnectActor(_, _)).Times(0);
  rpc::ActorTableData actor_table_data;
  actor_table_data.set_actor_id(actor_id.Binary());
  actor_table_data.set_state(
      rpc::ActorTableData_ActorState::ActorTableData_ActorState_ALIVE);
  ASSERT_TRUE(
      actor_info_accessor_->ActorStateNotificationPublished(actor_id, actor_table_data));
}

TEST_F(ActorManagerTest, TestActorLocationResolutionNormal) {
  ActorID actor_id = AddActorHandle(WorkerID::FromRandom());
  const std::unique_ptr<ActorHandle> &actor_handle =
      actor_manager_->GetActorHandle(actor_id);
  ASSERT_FALSE(actor_handle->IsPersistedToGCS());
  ASSERT_TRUE(CheckActorIdInResolutionMap(actor_id));

  // Make sure state is properly updated after GCS notifies the actor information is
  // persisted.
  rpc::ActorTableData actor_table_data;
  actor_table_data.set_actor_id(actor_id.Binary());
  actor_table_data.set_state(
      rpc::ActorTableData_ActorState::ActorTableData_ActorState_ALIVE);
  actor_info_accessor_->ActorStateNotificationPublished(actor_id, actor_table_data);
  ASSERT_TRUE(actor_handle->IsPersistedToGCS());
  actor_manager_->ResolveActorsLocations();
  // Location should've been resolved.
  ASSERT_FALSE(CheckActorIdInResolutionMap(actor_id));
}

// Actor should be disconnected if worker failure event
// is reported before it receives a state update from GCS.
TEST_F(ActorManagerTest, TestActorLocationResolutionWorkerFailed) {
  auto worker_id = WorkerID::FromRandom();
  ActorID actor_id = AddActorHandle(worker_id);
  const std::unique_ptr<ActorHandle> &actor_handle =
      actor_manager_->GetActorHandle(actor_id);
  // const WorkerID &worker_id =
  //     WorkerID::FromBinary(actor_handle->GetOwnerAddress().worker_id());
  ASSERT_TRUE(CheckActorIdInResolutionMap(actor_id));

  actor_manager_->ResolveActorsLocations();
  rpc::WorkerTableData worker_failure_data;

  // If the RPC request fails, do nothing.
  worker_info_accessor_->InvokeAsyncGetWorkerCallback(
      worker_id, ray::Status::Invalid(""),
      boost::optional<rpc::WorkerTableData>(worker_failure_data));
  ASSERT_TRUE(CheckActorIdInResolutionMap(actor_id));

  // If worker failure data is returned, actor handle is disconnected from an actor.
  EXPECT_CALL(*direct_actor_submitter_, DisconnectActor(_, _)).Times(1);
  worker_info_accessor_->InvokeAsyncGetWorkerCallback(
      worker_id, ray::Status::OK(),
      boost::optional<rpc::WorkerTableData>(worker_failure_data));

  // If async get callback failed, it won't do anything.
  actor_info_accessor_->InvokeAsyncGetCallback(actor_id, ray::Status::Invalid(""),
                                               boost::none);
  ASSERT_TRUE(CheckActorIdInResolutionMap(actor_id));

  // If async get callback succeed, and there's no actor data,
  // it means the actor is dead before it is persisted.
  // Disconnect an actor and delete the entry from resolution list.
  actor_info_accessor_->InvokeAsyncGetCallback(actor_id, ray::Status::OK(), boost::none);
  // The actor_id should've been deleted from the resolution list.
  ASSERT_FALSE(CheckActorIdInResolutionMap(actor_id));
  ASSERT_FALSE(actor_handle->IsPersistedToGCS());
  // NOTE: There's no way to get notification from GCS at this point because the worker
  // failed before
  //       the actor information is persisted to GCS.
}

// Actor should be disconnected if the node of an actor failed.
// is reported before it receives a state update from GCS.
TEST_F(ActorManagerTest, TestActorLocationResolutionNodeFailed) {
  auto worker_id = WorkerID::FromRandom();
  ActorID actor_id = AddActorHandle(worker_id);
  const std::unique_ptr<ActorHandle> &actor_handle =
      actor_manager_->GetActorHandle(actor_id);
  const ClientID &node_id =
      ClientID::FromBinary(actor_handle->GetOwnerAddress().raylet_id());
  ASSERT_TRUE(CheckActorIdInResolutionMap(actor_id));

  actor_manager_->ResolveActorsLocations();
  rpc::GcsNodeInfo node_info;
  node_info.set_state(rpc::GcsNodeInfo_GcsNodeState_DEAD);
  // Node failure will be reported.
  EXPECT_CALL(*node_info_accessor_, Get(node_id))
      .WillRepeatedly(testing::Return(boost::optional<rpc::GcsNodeInfo>(node_info)));

  rpc::WorkerTableData worker_failure_data;
  worker_failure_data.set_is_alive(true);
  worker_info_accessor_->InvokeAsyncGetWorkerCallback(
      worker_id, ray::Status::OK(),
      boost::optional<rpc::WorkerTableData>(worker_failure_data));
  // If async get callback succeed, and there's no actor data,
  // it means the actor is dead before it is persisted.
  // Disconnect an actor and delete the entry from resolution list.
  EXPECT_CALL(*direct_actor_submitter_, DisconnectActor(_, _)).Times(1);
  actor_info_accessor_->InvokeAsyncGetCallback(actor_id, ray::Status::OK(), boost::none);
  // The actor_id should've been deleted from the resolution list.
  ASSERT_FALSE(CheckActorIdInResolutionMap(actor_id));
  ASSERT_FALSE(actor_handle->IsPersistedToGCS());
}

// There's one race condition that has to be tested. Sometimes,
// it is possible that the actor information is persisted to GCS "after"
// the node or worker failure is confirmed from our code.
// In this case, the resolution protocol should not disconnect an actor.
TEST_F(ActorManagerTest, TestActorLocationResolutionRaceConditionActorRegistered) {
  auto worker_id = WorkerID::FromRandom();
  ActorID actor_id = AddActorHandle(worker_id);
  const std::unique_ptr<ActorHandle> &actor_handle =
      actor_manager_->GetActorHandle(actor_id);
  const ClientID &node_id =
      ClientID::FromBinary(actor_handle->GetOwnerAddress().raylet_id());
  ASSERT_TRUE(CheckActorIdInResolutionMap(actor_id));

  actor_manager_->ResolveActorsLocations();
  rpc::GcsNodeInfo node_info;
  node_info.set_state(rpc::GcsNodeInfo_GcsNodeState_DEAD);
  // Node failure will be reported.
  EXPECT_CALL(*node_info_accessor_, Get(node_id))
      .WillRepeatedly(testing::Return(boost::optional<rpc::GcsNodeInfo>(node_info)));

  rpc::WorkerTableData worker_failure_data;
  worker_failure_data.set_is_alive(true);
  worker_info_accessor_->InvokeAsyncGetWorkerCallback(
      worker_id, ray::Status::OK(),
      boost::optional<rpc::WorkerTableData>(worker_failure_data));

  // Let's suppose here, the actor is persisted to GCS.
  // In this case, we should not disconnect.
  EXPECT_CALL(*direct_actor_submitter_, DisconnectActor(_, _)).Times(0);
  rpc::ActorTableData actor_table_data;
  actor_table_data.set_actor_id(actor_id.Binary());
  actor_table_data.set_state(
      rpc::ActorTableData_ActorState::ActorTableData_ActorState_ALIVE);

  // GCS reports the actor information. Do nothing.
  actor_info_accessor_->InvokeAsyncGetCallback(
      actor_id, ray::Status::OK(),
      boost::optional<gcs::ActorTableData>(actor_table_data));
  // The actor_id should've been deleted from the resolution list.
  ASSERT_TRUE(CheckActorIdInResolutionMap(actor_id));
  ASSERT_FALSE(actor_handle->IsPersistedToGCS());

  // Now, GCS publishes the actor information.
  actor_info_accessor_->ActorStateNotificationPublished(actor_id, actor_table_data);
  ASSERT_TRUE(actor_handle->IsPersistedToGCS());

  // In the next resolution check, this actor will be removed from the resolution map.
  actor_manager_->ResolveActorsLocations();
  ASSERT_FALSE(CheckActorIdInResolutionMap(actor_id));
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
