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
#include <vector>

#include "gtest/gtest.h"
#include "ray/asio/periodical_runner.h"
#include "ray/common/test_utils.h"
#include "ray/core_worker_rpc_client/core_worker_client_pool.h"
#include "ray/core_worker_rpc_client/fake_core_worker_client.h"
#include "ray/gcs/actor/gcs_actor.h"
#include "ray/gcs/actor/gcs_actor_scheduler.h"
#include "ray/gcs/store_client/fake_store_client.h"
#include "ray/observability/fake_metric.h"
#include "ray/observability/fake_ray_event_recorder.h"
#include "ray/pubsub/fake_publisher.h"
#include "ray/pubsub/gcs_publisher.h"
#include "ray/raylet/scheduling/cluster_resource_scheduler.h"
#include "ray/raylet_rpc_client/fake_raylet_client.h"
#include "ray/util/clock.h"
#include "ray/util/counter_map.h"

namespace ray {
namespace gcs {

// Hand-written fake for the schedule success/failure handlers. Records the
// actors it was invoked with.
struct FakeCallback {
  void operator()(std::shared_ptr<GcsActor> a) { actors.push_back(std::move(a)); }
  std::vector<std::shared_ptr<GcsActor>> actors;
};

class GcsActorSchedulerMockTest : public ::testing::Test {
 public:
  void SetUp() override {
    store_client = std::make_shared<FakeStoreClient>();
    actor_table = std::make_unique<GcsActorTable>(store_client);
    raylet_client = std::make_shared<rpc::FakeRayletClient>();
    core_worker_client = std::make_shared<rpc::FakeCoreWorkerClient>();
    client_pool = std::make_unique<rpc::RayletClientPool>(
        [this](const rpc::Address &) { return raylet_client; });
    fake_observability_publisher_ = std::make_unique<pubsub::ObservabilityPublisher>(
        std::make_unique<pubsub::FakePublisher>());
    gcs_node_manager =
        std::make_unique<GcsNodeManager>(nullptr,
                                         nullptr,
                                         io_context,
                                         client_pool.get(),
                                         ClusterID::Nil(),
                                         /*ray_event_recorder=*/fake_ray_event_recorder_,
                                         /*session_name=*/"",
                                         fake_observability_publisher_.get(),
                                         clock_);
    local_node_id = NodeID::FromRandom();
    auto cluster_resource_scheduler = std::make_shared<ClusterResourceScheduler>(
        PeriodicalRunner::Create(io_context),
        scheduling::NodeID(local_node_id.Binary()),
        NodeResources(),
        /*is_node_available_fn=*/
        [](auto) { return true; },
        fake_resource_usage_gauge_,
        clock_,
        /*is_local_node_with_raylet=*/false);
    counter.reset(
        new CounterMap<std::pair<rpc::ActorTableData::ActorState, std::string>>());
    worker_client_pool_ = std::make_unique<rpc::CoreWorkerClientPool>(
        [this](const rpc::Address &address) { return core_worker_client; });
    actor_scheduler = std::make_unique<GcsActorScheduler>(
        io_context,
        *actor_table,
        *gcs_node_manager,
        [this](auto a, auto b, auto c) { schedule_failure_handler(a); },
        [this](auto a, const rpc::PushTaskReply) { schedule_success_handler(a); },
        *client_pool,
        *worker_client_pool_,
        fake_scheduler_placement_time_ms_histogram_,
        clock_);
    auto node_info = std::make_shared<rpc::GcsNodeInfo>();
    node_info->set_state(rpc::GcsNodeInfo::ALIVE);
    node_id = NodeID::FromRandom();
    node_info->set_node_id(node_id.Binary());
    worker_id = WorkerID::FromRandom();
    gcs_node_manager->AddNode(node_info);
  }

  std::shared_ptr<rpc::FakeRayletClient> raylet_client;
  instrumented_io_context io_context;
  std::shared_ptr<FakeStoreClient> store_client;
  std::unique_ptr<GcsActorTable> actor_table;
  std::unique_ptr<pubsub::ObservabilityPublisher> fake_observability_publisher_;
  std::unique_ptr<GcsNodeManager> gcs_node_manager;
  std::unique_ptr<GcsActorScheduler> actor_scheduler;
  std::shared_ptr<rpc::FakeCoreWorkerClient> core_worker_client;
  std::unique_ptr<rpc::CoreWorkerClientPool> worker_client_pool_;
  std::unique_ptr<rpc::RayletClientPool> client_pool;
  observability::FakeRayEventRecorder fake_ray_event_recorder_;
  ray::observability::FakeGauge fake_resource_usage_gauge_;
  ray::Clock clock_;
  observability::FakeHistogram fake_scheduler_placement_time_ms_histogram_;
  std::shared_ptr<CounterMap<std::pair<rpc::ActorTableData::ActorState, std::string>>>
      counter;
  FakeCallback schedule_failure_handler;
  FakeCallback schedule_success_handler;
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
  actor_scheduler->Schedule(actor);
  actor->GetMutableActorTableData()->set_state(rpc::ActorTableData::DEAD);
  actor_scheduler->CancelOnNode(node_id);
  // Reply to the pending worker lease request with a granted worker.
  ASSERT_TRUE(raylet_client->GrantWorkerLease(
      /*address=*/"",
      /*port=*/0,
      worker_id,
      node_id,
      /*retry_at_node_id=*/NodeID::Nil()));
  // Ensure the actor is killed to release the leaked worker.
  ASSERT_EQ(raylet_client->killed_actors.size(), 1);
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
  actor_scheduler->Schedule(actor);

  // Lease granted -> the scheduler writes the actor to storage before pushing the
  // creation task.
  ASSERT_TRUE(raylet_client->GrantWorkerLease(
      /*address=*/"",
      /*port=*/0,
      worker_id,
      node_id,
      /*retry_at_node_id=*/NodeID::Nil()));
  ASSERT_NE(store_client->last_async_put_callback, nullptr);
  std::move(*store_client->last_async_put_callback)
      .Post("GcsActorSchedulerMockTest", true);
  // Actually run the io_context for the async put callback, which pushes the creation
  // task to the worker.
  io_context.poll();

  actor->GetMutableActorTableData()->set_state(rpc::ActorTableData::DEAD);
  actor_scheduler->CancelOnWorker(node_id, worker_id);
  // Reply the creation task push -> worker released by killing the actor.
  ASSERT_TRUE(core_worker_client->ReplyPushTask());
  ASSERT_EQ(raylet_client->killed_actors.size(), 1);
}

}  // namespace gcs
}  // namespace ray
