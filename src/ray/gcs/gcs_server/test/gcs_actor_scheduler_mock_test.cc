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
#include "gtest/gtest.h"
#include "gmock/gmock.h"
#include "ray/gcs/gcs_server/gcs_actor_manager.h"
#include "ray/gcs/gcs_server/gcs_actor_scheduler.h"
#include "mock/ray/gcs/store_client/store_client.h"
#include "mock/ray/gcs/gcs_server/gcs_node_manager.h"
#include "mock/ray/raylet_client/raylet_client.h"
#include "mock/ray/pubsub/subscriber.h"
#include "mock/ray/rpc/worker/core_worker_client.h"
// clang-format on
using namespace ::testing;

namespace ray {
namespace gcs {
struct MockCallback {
  MOCK_METHOD(void, Call, ((std::shared_ptr<GcsActor>)));
  void operator()(std::shared_ptr<GcsActor> a) { return Call(a); }
};

class GcsActorSchedulerTest : public Test {
 public:
  void SetUp() override {
    store_client = std::make_shared<MockStoreClient>();
    actor_table = std::make_unique<GcsActorTable>(store_client);
    gcs_node_manager = std::make_unique<MockGcsNodeManager>();
    raylet_client = std::make_shared<MockRayletClientInterface>();
    core_worker_client = std::make_shared<rpc::MockCoreWorkerClientInterface>();
    client_pool = std::make_shared<rpc::NodeManagerClientPool>(
        [this](const rpc::Address &) { return raylet_client; });
    actor_scheduler = std::make_unique<RayletBasedActorScheduler>(
        io_context, *actor_table, *gcs_node_manager,
        [this](auto a, auto b) { schedule_failure_handler(a); },
        [this](auto a, const rpc::PushTaskReply) { schedule_success_handler(a); },
        client_pool, [this](const rpc::Address &) { return core_worker_client; });
    auto node_info = std::make_shared<rpc::GcsNodeInfo>();
    node_info->set_state(rpc::GcsNodeInfo::ALIVE);
    node_id = NodeID::FromRandom();
    node_info->set_node_id(node_id.Binary());
    worker_id = WorkerID::FromRandom();
    gcs_node_manager->AddNode(node_info);
  }
  std::shared_ptr<MockRayletClientInterface> raylet_client;
  instrumented_io_context io_context;
  std::shared_ptr<MockStoreClient> store_client;
  std::unique_ptr<GcsActorTable> actor_table;
  std::unique_ptr<GcsActorScheduler> actor_scheduler;
  std::unique_ptr<MockGcsNodeManager> gcs_node_manager;
  std::shared_ptr<rpc::MockCoreWorkerClientInterface> core_worker_client;
  std::shared_ptr<rpc::NodeManagerClientPool> client_pool;
  MockCallback schedule_failure_handler;
  MockCallback schedule_success_handler;
  NodeID node_id;
  WorkerID worker_id;
};

TEST_F(GcsActorSchedulerTest, KillWorkerLeak1) {
  // Ensure worker is not leak in the following case:
  //   1. Gcs start to lease a worker
  //   2. Gcs cancel the actor
  //   3. Gcs lease reply with a grant
  // We'd like to test the worker got released eventually.
  // Worker is released with actor killing
  auto actor_id = ActorID::FromHex("f4ce02420592ca68c1738a0d01000000");
  rpc::ActorTableData actor_data;
  actor_data.set_state(rpc::ActorTableData::PENDING_CREATION);
  actor_data.set_actor_id(actor_id.Binary());
  auto actor = std::make_shared<GcsActor>(actor_data);
  std::function<void(const Status &, const rpc::RequestWorkerLeaseReply &)> cb;
  EXPECT_CALL(*raylet_client, RequestWorkerLease(An<const rpc::TaskSpec &>(), _, _, _, _))
      .WillOnce(testing::SaveArg<2>(&cb));
  // Ensure actor is killed
  EXPECT_CALL(*core_worker_client, KillActor(_, _));
  actor_scheduler->Schedule(actor);
  actor->GetMutableActorTableData()->set_state(rpc::ActorTableData::DEAD);
  actor_scheduler->CancelOnNode(node_id);
  ray::rpc::RequestWorkerLeaseReply reply;
  reply.mutable_worker_address()->set_raylet_id(node_id.Binary());
  reply.mutable_worker_address()->set_worker_id(worker_id.Binary());
  cb(Status::OK(), reply);
}

TEST_F(GcsActorSchedulerTest, KillWorkerLeak2) {
  // Ensure worker is not leak in the following case:
  //   1. Actor is in pending creation
  //   2. Gcs push creation task to run in worker
  //   3. Cancel the task
  //   4. Task creating reply received
  // We'd like to test the worker got released eventually.
  // Worker is released with actor killing
  auto actor_id = ActorID::FromHex("f4ce02420592ca68c1738a0d01000000");
  rpc::ActorTableData actor_data;
  actor_data.set_state(rpc::ActorTableData::PENDING_CREATION);
  actor_data.set_actor_id(actor_id.Binary());
  auto actor = std::make_shared<GcsActor>(actor_data);
  rpc::ClientCallback<rpc::RequestWorkerLeaseReply> request_worker_lease_cb;
  // Ensure actor is killed
  EXPECT_CALL(*core_worker_client, KillActor(_, _));
  EXPECT_CALL(*raylet_client, RequestWorkerLease(An<const rpc::TaskSpec &>(), _, _, _, _))
      .WillOnce(testing::SaveArg<2>(&request_worker_lease_cb));

  std::function<void(ray::Status)> async_put_with_index_cb;
  // Leasing successfully
  EXPECT_CALL(*store_client, AsyncPutWithIndex(_, _, _, _, _))
      .WillOnce(DoAll(SaveArg<4>(&async_put_with_index_cb), Return(Status::OK())));
  actor_scheduler->Schedule(actor);
  rpc::RequestWorkerLeaseReply reply;
  reply.mutable_worker_address()->set_raylet_id(node_id.Binary());
  reply.mutable_worker_address()->set_worker_id(worker_id.Binary());
  request_worker_lease_cb(Status::OK(), reply);

  rpc::ClientCallback<rpc::PushTaskReply> push_normal_task_cb;
  // Worker start to run task
  EXPECT_CALL(*core_worker_client, PushNormalTask(_, _))
      .WillOnce(testing::SaveArg<1>(&push_normal_task_cb));
  async_put_with_index_cb(Status::OK());
  actor->GetMutableActorTableData()->set_state(rpc::ActorTableData::DEAD);
  actor_scheduler->CancelOnWorker(node_id, worker_id);
  push_normal_task_cb(Status::OK(), rpc::PushTaskReply());
}
}  // namespace gcs
}  // namespace ray
