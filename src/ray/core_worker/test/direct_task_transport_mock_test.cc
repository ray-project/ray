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

// clang-format off
#include "ray/core_worker/transport/direct_task_transport.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "mock/ray/core_worker/actor_creator.h"
#include "mock/ray/core_worker/task_manager.h"
#include "mock/ray/core_worker/lease_policy.h"
#include "mock/ray/raylet_client/raylet_client.h"
// clang-format on

namespace ray {
namespace core {
using namespace ::testing;
class DirectTaskTransportTest : public ::testing::Test {
 public:
  void SetUp() override {
    raylet_client = std::make_shared<MockRayletClientInterface>();
    task_finisher = std::make_shared<MockTaskFinisherInterface>();
    actor_creator = std::make_shared<MockActorCreatorInterface>();
    lease_policy = std::make_shared<MockLeasePolicyInterface>();
    auto client_pool = std::make_shared<rpc::CoreWorkerClientPool>(
        [&](const rpc::Address &) { return nullptr; });
    task_submitter = std::make_unique<CoreWorkerDirectTaskSubmitter>(
        rpc::Address(), /* rpc_address */
        raylet_client,  /* lease_client */
        client_pool,    /* core_worker_client_pool */
        nullptr,        /* lease_client_factory */
        lease_policy,   /* lease_policy */
        std::make_shared<CoreWorkerMemoryStore>(),
        task_finisher,
        NodeID::Nil(),      /* local_raylet_id */
        WorkerType::WORKER, /* worker_type */
        0,                  /* lease_timeout_ms */
        actor_creator,
        JobID::Nil() /* job_id */);
  }

  TaskSpecification GetCreatingTaskSpec(const ActorID &actor_id) {
    rpc::TaskSpec task_spec;
    task_spec.set_task_id(TaskID::ForActorCreationTask(actor_id).Binary());
    task_spec.set_type(rpc::TaskType::ACTOR_CREATION_TASK);
    rpc::ActorCreationTaskSpec actor_creation_task_spec;
    actor_creation_task_spec.set_actor_id(actor_id.Binary());
    task_spec.mutable_actor_creation_task_spec()->CopyFrom(actor_creation_task_spec);
    return TaskSpecification(task_spec);
  }

  std::unique_ptr<CoreWorkerDirectTaskSubmitter> task_submitter;
  std::shared_ptr<MockRayletClientInterface> raylet_client;
  std::shared_ptr<MockTaskFinisherInterface> task_finisher;
  std::shared_ptr<MockActorCreatorInterface> actor_creator;
  std::shared_ptr<MockLeasePolicyInterface> lease_policy;
};

TEST_F(DirectTaskTransportTest, ActorRegisterOk) {
  auto actor_id = ActorID::FromHex("f4ce02420592ca68c1738a0d01000000");
  auto task_spec = GetCreatingTaskSpec(actor_id);
  EXPECT_CALL(*task_finisher, CompletePendingTask(task_spec.TaskId(), _, _));
  rpc::ClientCallback<rpc::CreateActorReply> create_cb;
  EXPECT_CALL(*actor_creator, AsyncCreateActor(task_spec, _))
      .WillOnce(DoAll(SaveArg<1>(&create_cb), Return(Status::OK())));
  ASSERT_TRUE(task_submitter->SubmitTask(task_spec).ok());
  create_cb(Status::OK(), rpc::CreateActorReply());
}

TEST_F(DirectTaskTransportTest, ActorCreationFail) {
  auto actor_id = ActorID::FromHex("f4ce02420592ca68c1738a0d01000000");
  auto task_spec = GetCreatingTaskSpec(actor_id);
  EXPECT_CALL(*task_finisher, CompletePendingTask(_, _, _)).Times(0);
  EXPECT_CALL(*task_finisher,
              FailOrRetryPendingTask(
                  task_spec.TaskId(), rpc::ErrorType::ACTOR_CREATION_FAILED, _, _, true));
  rpc::ClientCallback<rpc::CreateActorReply> create_cb;
  EXPECT_CALL(*actor_creator, AsyncCreateActor(task_spec, _))
      .WillOnce(DoAll(SaveArg<1>(&create_cb), Return(Status::OK())));
  ASSERT_TRUE(task_submitter->SubmitTask(task_spec).ok());
  create_cb(Status::IOError(""), rpc::CreateActorReply());
}

}  // namespace core
}  // namespace ray
