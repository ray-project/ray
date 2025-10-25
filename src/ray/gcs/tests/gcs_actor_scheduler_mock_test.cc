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
#include <string>
#include <utility>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "mock/ray/gcs/gcs_node_manager.h"
#include "mock/ray/gcs/store_client/store_client.h"
#include "mock/ray/raylet_client/raylet_client.h"
#include "mock/ray/rpc/worker/core_worker_client.h"
#include "ray/common/test_utils.h"
#include "ray/core_worker_rpc_client/core_worker_client_pool.h"
#include "ray/gcs/gcs_actor.h"
#include "ray/gcs/gcs_actor_scheduler.h"
#include "ray/observability/fake_metric.h"
#include "ray/observability/fake_ray_event_recorder.h"
#include "ray/util/counter_map.h"

using namespace ::testing;  // NOLINT

namespace ray {
namespace gcs {

struct MockCallback {
  MOCK_METHOD(void, Call, ((std::shared_ptr<GcsActor>)));
  void operator()(std::shared_ptr<GcsActor> a) { return Call(a); }
};

class GcsActorSchedulerMockTest : public Test {
 public:
  void SetUp() override {
    store_client = std::make_shared<MockStoreClient>();
    actor_table = std::make_unique<GcsActorTable>(store_client);
    raylet_client = std::make_shared<MockRayletClientInterface>();
    core_worker_client = std::make_shared<rpc::MockCoreWorkerClientInterface>();
    client_pool = std::make_unique<rpc::RayletClientPool>(
        [this](const rpc::Address &) { return raylet_client; });
    gcs_node_manager =
        std::make_unique<GcsNodeManager>(nullptr,
                                         nullptr,
                                         io_context,
                                         client_pool.get(),
                                         ClusterID::Nil(),
                                         /*ray_event_recorder=*/fake_ray_event_recorder_,
                                         /*session_name=*/"");
    local_node_id = NodeID::FromRandom();
    auto cluster_resource_scheduler = std::make_shared<ClusterResourceScheduler>(
        io_context,
        scheduling::NodeID(local_node_id.Binary()),
        NodeResources(),
        /*is_node_available_fn=*/
        [](auto) { return true; },
        /*is_local_node_with_raylet=*/false);
    local_lease_manager_ = std::make_unique<raylet::NoopLocalLeaseManager>();
    cluster_lease_manager = std::make_unique<ClusterLeaseManager>(
        local_node_id,
        *cluster_resource_scheduler,
        /*get_node_info=*/
        [this](const NodeID &nid) { return gcs_node_manager->GetAliveNodeAddress(nid); },
        /*announce_infeasible_lease=*/nullptr,
        *local_lease_manager_);
    counter.reset(
        new CounterMap<std::pair<rpc::ActorTableData::ActorState, std::string>>());
    worker_client_pool_ = std::make_unique<rpc::CoreWorkerClientPool>(
        [this](const rpc::Address &address) { return core_worker_client; });
    actor_scheduler = std::make_unique<GcsActorScheduler>(
        io_context,
        *actor_table,
        *gcs_node_manager,
        *cluster_lease_manager,
        [this](auto a, auto b, auto c) { schedule_failure_handler(a); },
        [this](auto a, const rpc::PushTaskReply) { schedule_success_handler(a); },
        *client_pool,
        *worker_client_pool_,
        fake_scheduler_placement_time_s_histogram_);
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
  std::unique_ptr<GcsNodeManager> gcs_node_manager;
  std::unique_ptr<raylet::LocalLeaseManagerInterface> local_lease_manager_;
  std::unique_ptr<ClusterLeaseManager> cluster_lease_manager;
  std::unique_ptr<GcsActorScheduler> actor_scheduler;
  std::shared_ptr<rpc::MockCoreWorkerClientInterface> core_worker_client;
  std::unique_ptr<rpc::CoreWorkerClientPool> worker_client_pool_;
  std::unique_ptr<rpc::RayletClientPool> client_pool;
  observability::FakeRayEventRecorder fake_ray_event_recorder_;
  observability::FakeHistogram fake_scheduler_placement_time_s_histogram_;
  std::shared_ptr<CounterMap<std::pair<rpc::ActorTableData::ActorState, std::string>>>
      counter;
  MockCallback schedule_failure_handler;
  MockCallback schedule_success_handler;
  NodeID node_id;
  WorkerID worker_id;
  NodeID local_node_id;
};

TEST_F(GcsActorSchedulerMockTest, KillWorkerLeak1) {
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
  auto actor = std::make_shared<GcsActor>(
      actor_data, rpc::TaskSpec(), counter, fake_ray_event_recorder_, "");
  rpc::ClientCallback<rpc::RequestWorkerLeaseReply> cb;
  EXPECT_CALL(*raylet_client,
              RequestWorkerLease(An<const rpc::LeaseSpec &>(), _, _, _, _))
      .WillOnce(testing::SaveArg<2>(&cb));
  // Ensure actor is killed
  EXPECT_CALL(*core_worker_client, KillActor(_, _));
  actor_scheduler->ScheduleByRaylet(actor);
  actor->GetMutableActorTableData()->set_state(rpc::ActorTableData::DEAD);
  actor_scheduler->CancelOnNode(node_id);
  ray::rpc::RequestWorkerLeaseReply reply;
  reply.mutable_worker_address()->set_node_id(node_id.Binary());
  reply.mutable_worker_address()->set_worker_id(worker_id.Binary());
  cb(Status::OK(), std::move(reply));
}

TEST_F(GcsActorSchedulerMockTest, KillWorkerLeak2) {
  // Ensure worker is not leak in the following case:
  //   1. Actor is in pending creation
  //   2. Gcs push creation task to run in worker
  //   3. Cancel the lease
  //   4. Lease creating reply received
  // We'd like to test the worker got released eventually.
  // Worker is released with actor killing
  auto actor_id = ActorID::FromHex("f4ce02420592ca68c1738a0d01000000");
  rpc::ActorTableData actor_data;
  actor_data.set_state(rpc::ActorTableData::PENDING_CREATION);
  actor_data.set_actor_id(actor_id.Binary());
  auto actor = std::make_shared<GcsActor>(
      actor_data, rpc::TaskSpec(), counter, fake_ray_event_recorder_, "");
  rpc::ClientCallback<rpc::RequestWorkerLeaseReply> request_worker_lease_cb;
  // Ensure actor is killed
  EXPECT_CALL(*core_worker_client, KillActor(_, _));
  EXPECT_CALL(*raylet_client,
              RequestWorkerLease(An<const rpc::LeaseSpec &>(), _, _, _, _))
      .WillOnce(testing::SaveArg<2>(&request_worker_lease_cb));

  // Postable is not default constructable, so we use a unique_ptr to hold one.
  std::unique_ptr<Postable<void(bool)>> async_put_with_index_cb;
  // Leasing successfully
  EXPECT_CALL(*store_client, AsyncPut(_, _, _, _, _))
      .WillOnce(DoAll(SaveArgToUniquePtr<4>(&async_put_with_index_cb),
                      InvokeWithoutArgs([]() {})));
  actor_scheduler->ScheduleByRaylet(actor);
  rpc::RequestWorkerLeaseReply reply;
  reply.mutable_worker_address()->set_node_id(node_id.Binary());
  reply.mutable_worker_address()->set_worker_id(worker_id.Binary());
  request_worker_lease_cb(Status::OK(), std::move(reply));

  rpc::ClientCallback<rpc::PushTaskReply> push_normal_task_cb;
  // Worker start to run task
  EXPECT_CALL(*core_worker_client, PushNormalTask(_, _))
      .WillOnce(testing::SaveArg<1>(&push_normal_task_cb));
  std::move(*async_put_with_index_cb).Post("GcsActorSchedulerMockTest", true);
  // actually run the io_context for async_put_with_index_cb.
  io_context.poll();
  actor->GetMutableActorTableData()->set_state(rpc::ActorTableData::DEAD);
  actor_scheduler->CancelOnWorker(node_id, worker_id);
  push_normal_task_cb(Status::OK(), rpc::PushTaskReply());
}

}  // namespace gcs
}  // namespace ray
