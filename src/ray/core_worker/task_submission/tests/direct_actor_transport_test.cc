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

#include <memory>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "mock/ray/core_worker/memory_store.h"
#include "mock/ray/core_worker/reference_counter.h"
#include "mock/ray/core_worker/task_manager_interface.h"
#include "mock/ray/gcs_client/gcs_client.h"
#include "ray/core_worker/actor_creator.h"
#include "ray/core_worker/task_submission/actor_task_submitter.h"

namespace ray {
namespace core {
using ::testing::_;

class DirectTaskTransportTest : public ::testing::Test {
 public:
  DirectTaskTransportTest() : io_work(io_context.get_executor()) {}

  void SetUp() override {
    gcs_client = std::make_shared<ray::gcs::MockGcsClient>();
    actor_creator = std::make_unique<ActorCreator>(gcs_client->Actors());

    task_manager = std::make_shared<MockTaskManagerInterface>();
    client_pool = std::make_shared<rpc::CoreWorkerClientPool>(
        [&](const rpc::Address &) { return nullptr; });
    memory_store = DefaultCoreWorkerMemoryStoreWithThread::Create();
    reference_counter = std::make_shared<MockReferenceCounter>();
    actor_task_submitter = std::make_unique<ActorTaskSubmitter>(
        *client_pool,
        *memory_store,
        *task_manager,
        *actor_creator,
        [](const ObjectID &object_id) { return rpc::TensorTransport::OBJECT_STORE; },
        nullptr,
        io_context,
        reference_counter);
  }

  TaskSpecification GetActorTaskSpec(const ActorID &actor_id) {
    rpc::TaskSpec task_spec;
    task_spec.set_type(rpc::TaskType::ACTOR_TASK);
    task_spec.mutable_actor_task_spec()->set_actor_id(actor_id.Binary());
    task_spec.set_task_id(
        TaskID::ForActorTask(JobID::FromInt(10), TaskID::Nil(), 0, actor_id).Binary());
    return TaskSpecification(task_spec);
  }

  TaskSpecification GetActorCreationTaskSpec(const ActorID &actor_id) {
    rpc::TaskSpec task_spec;
    task_spec.set_task_id(TaskID::ForActorCreationTask(actor_id).Binary());
    task_spec.set_type(rpc::TaskType::ACTOR_CREATION_TASK);
    rpc::ActorCreationTaskSpec actor_creation_task_spec;
    actor_creation_task_spec.set_actor_id(actor_id.Binary());
    task_spec.mutable_actor_creation_task_spec()->CopyFrom(actor_creation_task_spec);
    return TaskSpecification(task_spec);
  }

 protected:
  bool CheckSubmitTask(TaskSpecification task) {
    actor_task_submitter->SubmitTask(task);
    return 1 == io_context.poll_one();
  }

 protected:
  instrumented_io_context io_context;
  boost::asio::executor_work_guard<boost::asio::io_context::executor_type> io_work;
  std::unique_ptr<ActorTaskSubmitter> actor_task_submitter;
  std::shared_ptr<rpc::CoreWorkerClientPool> client_pool;
  std::unique_ptr<CoreWorkerMemoryStore> memory_store;
  std::shared_ptr<MockTaskManagerInterface> task_manager;
  std::unique_ptr<ActorCreator> actor_creator;
  std::shared_ptr<ray::gcs::MockGcsClient> gcs_client;
  std::shared_ptr<MockReferenceCounter> reference_counter;
};

TEST_F(DirectTaskTransportTest, ActorCreationOk) {
  auto actor_id = ActorID::FromHex("f4ce02420592ca68c1738a0d01000000");
  auto creation_task_spec = GetActorCreationTaskSpec(actor_id);
  EXPECT_CALL(*task_manager, CompletePendingTask(creation_task_spec.TaskId(), _, _, _));
  rpc::ClientCallback<rpc::CreateActorReply> create_cb;
  EXPECT_CALL(*gcs_client->mock_actor_accessor,
              AsyncCreateActor(creation_task_spec, ::testing::_))
      .WillOnce(::testing::DoAll(::testing::SaveArg<1>(&create_cb)));
  actor_task_submitter->SubmitActorCreationTask(creation_task_spec);
  create_cb(Status::OK(), rpc::CreateActorReply());
}

TEST_F(DirectTaskTransportTest, ActorCreationFail) {
  auto actor_id = ActorID::FromHex("f4ce02420592ca68c1738a0d01000000");
  auto creation_task_spec = GetActorCreationTaskSpec(actor_id);
  EXPECT_CALL(*task_manager, CompletePendingTask(_, _, _, _)).Times(0);
  EXPECT_CALL(
      *task_manager,
      FailPendingTask(
          creation_task_spec.TaskId(), rpc::ErrorType::ACTOR_CREATION_FAILED, _, _));
  rpc::ClientCallback<rpc::CreateActorReply> create_cb;
  EXPECT_CALL(*gcs_client->mock_actor_accessor,
              AsyncCreateActor(creation_task_spec, ::testing::_))
      .WillOnce(::testing::DoAll(::testing::SaveArg<1>(&create_cb)));
  actor_task_submitter->SubmitActorCreationTask(creation_task_spec);
  create_cb(Status::IOError(""), rpc::CreateActorReply());
}

TEST_F(DirectTaskTransportTest, ActorRegisterFailure) {
  auto actor_id = ActorID::FromHex("f4ce02420592ca68c1738a0d01000000");
  ASSERT_TRUE(ObjectID::IsActorID(ObjectID::ForActorHandle(actor_id)));
  ASSERT_EQ(actor_id, ObjectID::ToActorID(ObjectID::ForActorHandle(actor_id)));
  auto creation_task_spec = GetActorCreationTaskSpec(actor_id);
  auto task_spec = GetActorTaskSpec(actor_id);
  auto task_arg = task_spec.GetMutableMessage().add_args();
  auto inline_obj_ref = task_arg->add_nested_inlined_refs();
  inline_obj_ref->set_object_id(ObjectID::ForActorHandle(actor_id).Binary());
  std::function<void(Status)> register_cb;
  EXPECT_CALL(*gcs_client->mock_actor_accessor,
              AsyncRegisterActor(creation_task_spec, ::testing::_, ::testing::_))
      .WillOnce(::testing::DoAll(::testing::SaveArg<1>(&register_cb)));
  actor_creator->AsyncRegisterActor(creation_task_spec, nullptr);
  ASSERT_TRUE(actor_creator->IsActorInRegistering(actor_id));
  actor_task_submitter->AddActorQueueIfNotExists(actor_id,
                                                 -1,
                                                 /*allow_out_of_order_execution*/ false,
                                                 /*fail_if_actor_unreachable*/ true,
                                                 /*owned*/ false);
  ASSERT_TRUE(CheckSubmitTask(task_spec));
  EXPECT_CALL(
      *task_manager,
      FailOrRetryPendingTask(
          task_spec.TaskId(), rpc::ErrorType::DEPENDENCY_RESOLUTION_FAILED, _, _, _, _));
  register_cb(Status::IOError(""));
}

TEST_F(DirectTaskTransportTest, ActorRegisterOk) {
  auto actor_id = ActorID::FromHex("f4ce02420592ca68c1738a0d01000000");
  ASSERT_TRUE(ObjectID::IsActorID(ObjectID::ForActorHandle(actor_id)));
  ASSERT_EQ(actor_id, ObjectID::ToActorID(ObjectID::ForActorHandle(actor_id)));
  auto creation_task_spec = GetActorCreationTaskSpec(actor_id);
  auto task_spec = GetActorTaskSpec(actor_id);
  auto task_arg = task_spec.GetMutableMessage().add_args();
  auto inline_obj_ref = task_arg->add_nested_inlined_refs();
  inline_obj_ref->set_object_id(ObjectID::ForActorHandle(actor_id).Binary());
  std::function<void(Status)> register_cb;
  EXPECT_CALL(*gcs_client->mock_actor_accessor,
              AsyncRegisterActor(creation_task_spec, ::testing::_, ::testing::_))
      .WillOnce(::testing::DoAll(::testing::SaveArg<1>(&register_cb)));
  actor_creator->AsyncRegisterActor(creation_task_spec, nullptr);
  ASSERT_TRUE(actor_creator->IsActorInRegistering(actor_id));
  actor_task_submitter->AddActorQueueIfNotExists(actor_id,
                                                 -1,
                                                 /*allow_out_of_order_execution*/ false,
                                                 /*fail_if_actor_unreachable*/ true,
                                                 /*owned*/ false);
  ASSERT_TRUE(CheckSubmitTask(task_spec));
  EXPECT_CALL(*task_manager, FailOrRetryPendingTask(_, _, _, _, _, _)).Times(0);
  register_cb(Status::OK());
}

}  // namespace core
}  // namespace ray
