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
#include "ray/gcs/actor/gcs_actor_scheduler.h"

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "mock/ray/pubsub/publisher.h"
#include "ray/common/asio/asio_util.h"
#include "ray/common/test_utils.h"
#include "ray/core_worker_rpc_client/core_worker_client_pool.h"
#include "ray/core_worker_rpc_client/fake_core_worker_client.h"
#include "ray/gcs/actor/gcs_actor.h"
#include "ray/gcs/gcs_resource_manager.h"
#include "ray/gcs/store_client/in_memory_store_client.h"
#include "ray/observability/fake_metric.h"
#include "ray/observability/fake_ray_event_recorder.h"
#include "ray/raylet/scheduling/cluster_resource_manager.h"
#include "ray/raylet/scheduling/cluster_resource_scheduler.h"
#include "ray/raylet_rpc_client/fake_raylet_client.h"
#include "ray/raylet_rpc_client/raylet_client_pool.h"
#include "ray/util/counter_map.h"

namespace ray {
namespace gcs {

class MockedGcsActorScheduler : public gcs::GcsActorScheduler {
 public:
  using gcs::GcsActorScheduler::GcsActorScheduler;

 protected:
  void RetryLeasingWorkerFromNode(std::shared_ptr<gcs::GcsActor> actor,
                                  std::shared_ptr<const rpc::GcsNodeInfo> node) override {
    ++num_retry_leasing_count_;
    if (num_retry_leasing_count_ <= 1) {
      DoRetryLeasingWorkerFromNode(actor, node);
    }
  }

  void RetryCreatingActorOnWorker(std::shared_ptr<gcs::GcsActor> actor,
                                  std::shared_ptr<GcsLeasedWorker> worker) override {
    ++num_retry_creating_count_;
    DoRetryCreatingActorOnWorker(actor, worker);
  }

 public:
  int num_retry_leasing_count_ = 0;
  int num_retry_creating_count_ = 0;
};

class FakeGcsActorTable : public gcs::GcsActorTable {
 public:
  // The store_client and io_context args are NOT used.
  explicit FakeGcsActorTable(std::shared_ptr<gcs::InMemoryStoreClient> store_client)
      : GcsActorTable(store_client) {}

  void Put(const ActorID &key,
           const rpc::ActorTableData &value,
           Postable<void(Status)> callback) override {
    std::move(callback).Post("FakeGcsActorTable.Put", Status::OK());
  }

 private:
  std::shared_ptr<gcs::InMemoryStoreClient> store_client_ =
      std::make_shared<gcs::InMemoryStoreClient>();
};

class GcsActorSchedulerTest : public ::testing::Test {
 public:
  void SetUp() override {
    io_context_ =
        std::make_unique<InstrumentedIOContextWithThread>("GcsActorSchedulerTest");
    raylet_client_ = std::make_shared<rpc::FakeRayletClient>();
    raylet_client_pool_ = std::make_shared<rpc::RayletClientPool>(
        [this](const rpc::Address &addr) { return raylet_client_; });
    worker_client_ = std::make_shared<rpc::FakeCoreWorkerClient>();
    gcs_publisher_ = std::make_shared<pubsub::GcsPublisher>(
        std::make_unique<ray::pubsub::MockPublisher>());
    store_client_ = std::make_shared<gcs::InMemoryStoreClient>();
    gcs_table_storage_ =
        std::make_unique<gcs::GcsTableStorage>(std::make_unique<InMemoryStoreClient>());
    gcs_node_manager_ = std::make_shared<gcs::GcsNodeManager>(
        gcs_publisher_.get(),
        gcs_table_storage_.get(),
        io_context_->GetIoService(),
        raylet_client_pool_.get(),
        ClusterID::Nil(),
        /*ray_event_recorder=*/fake_ray_event_recorder_,
        /*session_name=*/"");
    gcs_actor_table_ = std::make_shared<FakeGcsActorTable>(store_client_);
    local_node_id_ = NodeID::FromRandom();
    cluster_resource_scheduler_ = std::make_unique<ClusterResourceScheduler>(
        io_context_->GetIoService(),
        scheduling::NodeID(local_node_id_.Binary()),
        NodeResources(),
        /*is_node_available_fn=*/
        [](auto) { return true; },
        fake_resource_usage_gauge_,
        /*is_local_node_with_raylet=*/false);
    counter.reset(
        new CounterMap<std::pair<rpc::ActorTableData::ActorState, std::string>>());
    auto gcs_resource_manager = std::make_shared<gcs::GcsResourceManager>(
        io_context_->GetIoService(),
        cluster_resource_scheduler_->GetClusterResourceManager(),
        *gcs_node_manager_,
        local_node_id_);
    worker_client_pool_ = std::make_unique<rpc::CoreWorkerClientPool>(
        [this](const rpc::Address &address) { return worker_client_; });
    gcs_actor_scheduler_ = std::make_shared<MockedGcsActorScheduler>(
        io_context_->GetIoService(),
        *gcs_actor_table_,
        *gcs_node_manager_,
        /*schedule_failure_handler=*/
        [this](std::shared_ptr<gcs::GcsActor> actor,
               const rpc::RequestWorkerLeaseReply::SchedulingFailureType failure_type,
               const std::string &scheduling_failure_message) {
          failure_actors_.emplace_back(std::move(actor));
        },
        /*schedule_success_handler=*/
        [this](std::shared_ptr<gcs::GcsActor> actor, const rpc::PushTaskReply &reply) {
          success_actors_.emplace_back(std::move(actor));
        },
        *raylet_client_pool_,
        *worker_client_pool_,
        fake_scheduler_placement_time_ms_histogram_);
  }

  void TearDown() override { io_context_->Stop(); }

  std::shared_ptr<gcs::GcsActor> NewGcsActor(
      const std::unordered_map<std::string, double> &required_placement_resources) {
    rpc::Address owner_address;
    owner_address.set_node_id(NodeID::FromRandom().Binary());
    owner_address.set_ip_address("127.0.0.1");
    owner_address.set_port(5678);
    owner_address.set_worker_id(WorkerID::FromRandom().Binary());
    auto job_id = JobID::FromInt(1);

    std::unordered_map<std::string, double> required_resources;

    required_resources.insert(required_placement_resources.begin(),
                              required_placement_resources.end());
    auto actor_creating_task_spec = GenActorCreationTask(job_id,
                                                         /*max_restarts=*/1,
                                                         /*detached=*/true,
                                                         /*name=*/"",
                                                         "",
                                                         owner_address,
                                                         required_resources,
                                                         required_placement_resources);
    return std::make_shared<gcs::GcsActor>(actor_creating_task_spec.GetMessage(),
                                           /*ray_namespace=*/"",
                                           /*counter=*/counter,
                                           /*recorder=*/fake_ray_event_recorder_,
                                           /*session_name=*/"");
  }

  std::shared_ptr<rpc::GcsNodeInfo> AddNewNode(
      std::unordered_map<std::string, double> node_resources) {
    auto node_info = GenNodeInfo();
    node_info->mutable_resources_total()->insert(node_resources.begin(),
                                                 node_resources.end());
    gcs_node_manager_->AddNode(node_info);
    scheduling::NodeID node_id(node_info->node_id());
    auto &cluster_resource_manager =
        cluster_resource_scheduler_->GetClusterResourceManager();
    auto resource_map = MapFromProtobuf(node_info->resources_total());
    auto node_resources_ = ResourceMapToNodeResources(resource_map, resource_map);
    cluster_resource_manager.AddOrUpdateNode(node_id, node_resources_);

    return node_info;
  }

 protected:
  std::unique_ptr<InstrumentedIOContextWithThread> io_context_;
  std::shared_ptr<gcs::InMemoryStoreClient> store_client_;
  std::shared_ptr<FakeGcsActorTable> gcs_actor_table_;
  std::shared_ptr<rpc::FakeRayletClient> raylet_client_;
  std::shared_ptr<rpc::FakeCoreWorkerClient> worker_client_;
  std::unique_ptr<rpc::CoreWorkerClientPool> worker_client_pool_;
  std::shared_ptr<gcs::GcsNodeManager> gcs_node_manager_;
  observability::FakeRayEventRecorder fake_ray_event_recorder_;
  ray::observability::FakeGauge fake_resource_usage_gauge_;
  std::unique_ptr<ClusterResourceScheduler> cluster_resource_scheduler_;
  std::shared_ptr<MockedGcsActorScheduler> gcs_actor_scheduler_;
  std::shared_ptr<CounterMap<std::pair<rpc::ActorTableData::ActorState, std::string>>>
      counter;
  std::vector<std::shared_ptr<gcs::GcsActor>> failure_actors_;
  std::vector<std::shared_ptr<gcs::GcsActor>> success_actors_;
  std::shared_ptr<pubsub::GcsPublisher> gcs_publisher_;
  std::shared_ptr<gcs::GcsTableStorage> gcs_table_storage_;
  std::shared_ptr<rpc::RayletClientPool> raylet_client_pool_;
  ray::observability::FakeHistogram fake_scheduler_placement_time_ms_histogram_;
  NodeID local_node_id_;
};

/**************************************************************/
/************* TESTS WITH RAYLET SCHEDULING BELOW *************/
/**************************************************************/

TEST_F(GcsActorSchedulerTest, TestScheduleFailedWithZeroNode) {
  ASSERT_EQ(0, gcs_node_manager_->GetAllAliveNodes().size());

  auto job_id = JobID::FromInt(1);
  auto create_actor_request = GenCreateActorRequest(job_id);
  auto actor = std::make_shared<gcs::GcsActor>(
      create_actor_request.task_spec(), "", counter, fake_ray_event_recorder_, "");

  // Schedule the actor with zero node.
  gcs_actor_scheduler_->Schedule(actor);

  // The lease request should not be send and the scheduling of actor should fail as there
  // are no available nodes.
  ASSERT_EQ(raylet_client_->num_workers_requested, 0);
  ASSERT_EQ(0, success_actors_.size());
  ASSERT_EQ(1, failure_actors_.size());
  ASSERT_TRUE(actor->GetNodeID().IsNil());
}

TEST_F(GcsActorSchedulerTest, TestScheduleActorSuccess) {
  auto node = GenNodeInfo();
  auto node_id = NodeID::FromBinary(node->node_id());
  gcs_node_manager_->AddNode(node);
  ASSERT_EQ(1, gcs_node_manager_->GetAllAliveNodes().size());

  auto job_id = JobID::FromInt(1);
  auto create_actor_request = GenCreateActorRequest(job_id);
  auto actor = std::make_shared<gcs::GcsActor>(
      create_actor_request.task_spec(), "", counter, fake_ray_event_recorder_, "");

  // Schedule the actor with 1 available node, and the lease request should be send to the
  // node.
  gcs_actor_scheduler_->Schedule(actor);
  ASSERT_EQ(1, raylet_client_->num_workers_requested);
  ASSERT_EQ(1, raylet_client_->callbacks.size());
  ASSERT_EQ(0, worker_client_->GetNumCallbacks());

  // Grant a worker, then the actor creation request should be send to the worker.
  WorkerID worker_id = WorkerID::FromRandom();
  ASSERT_TRUE(raylet_client_->GrantWorkerLease(node->node_manager_address(),
                                               node->node_manager_port(),
                                               worker_id,
                                               node_id,
                                               NodeID::Nil()));
  ASSERT_EQ(0, raylet_client_->callbacks.size());
  WaitForCondition([&]() { return worker_client_->GetNumCallbacks() == 1; }, 1000);

  // Reply the actor creation request, then the actor should be scheduled successfully.
  ASSERT_TRUE(worker_client_->ReplyPushTask());
  ASSERT_EQ(0, worker_client_->GetNumCallbacks());
  ASSERT_EQ(0, failure_actors_.size());
  ASSERT_EQ(1, success_actors_.size());
  ASSERT_EQ(actor, success_actors_.front());
  ASSERT_EQ(actor->GetNodeID(), node_id);
  ASSERT_EQ(actor->GetWorkerID(), worker_id);
}

TEST_F(GcsActorSchedulerTest, TestScheduleRetryWhenLeasing) {
  auto node = GenNodeInfo();
  auto node_id = NodeID::FromBinary(node->node_id());
  gcs_node_manager_->AddNode(node);
  ASSERT_EQ(1, gcs_node_manager_->GetAllAliveNodes().size());

  auto job_id = JobID::FromInt(1);
  auto create_actor_request = GenCreateActorRequest(job_id);
  auto actor = std::make_shared<gcs::GcsActor>(
      create_actor_request.task_spec(), "", counter, fake_ray_event_recorder_, "");

  // Schedule the actor with 1 available node, and the lease request should be send to the
  // node.
  gcs_actor_scheduler_->Schedule(actor);
  ASSERT_EQ(1, raylet_client_->num_workers_requested);
  ASSERT_EQ(1, raylet_client_->callbacks.size());
  ASSERT_EQ(0, worker_client_->GetNumCallbacks());
  ASSERT_EQ(0, gcs_actor_scheduler_->num_retry_leasing_count_);

  // Mock a IOError reply, then the lease request will retry again.
  ASSERT_TRUE(raylet_client_->GrantWorkerLease(node->node_manager_address(),
                                               node->node_manager_port(),
                                               WorkerID::FromRandom(),
                                               node_id,
                                               NodeID::Nil(),
                                               Status::IOError("")));
  ASSERT_EQ(1, gcs_actor_scheduler_->num_retry_leasing_count_);
  ASSERT_EQ(2, raylet_client_->num_workers_requested);
  ASSERT_EQ(1, raylet_client_->callbacks.size());
  ASSERT_EQ(0, worker_client_->GetNumCallbacks());

  // Grant a worker, then the actor creation request should be send to the worker.
  WorkerID worker_id = WorkerID::FromRandom();
  ASSERT_TRUE(raylet_client_->GrantWorkerLease(node->node_manager_address(),
                                               node->node_manager_port(),
                                               worker_id,
                                               node_id,
                                               NodeID::Nil()));
  ASSERT_EQ(0, raylet_client_->callbacks.size());
  WaitForCondition([&]() { return worker_client_->GetNumCallbacks() == 1; }, 1000);

  // Reply the actor creation request, then the actor should be scheduled successfully.
  ASSERT_TRUE(worker_client_->ReplyPushTask());
  ASSERT_EQ(0, worker_client_->GetNumCallbacks());
  ASSERT_EQ(0, failure_actors_.size());
  ASSERT_EQ(1, success_actors_.size());
  ASSERT_EQ(actor, success_actors_.front());
  ASSERT_EQ(actor->GetNodeID(), node_id);
  ASSERT_EQ(actor->GetWorkerID(), worker_id);
}

TEST_F(GcsActorSchedulerTest, TestScheduleRetryWhenCreating) {
  auto node = GenNodeInfo();
  auto node_id = NodeID::FromBinary(node->node_id());
  gcs_node_manager_->AddNode(node);
  ASSERT_EQ(1, gcs_node_manager_->GetAllAliveNodes().size());

  auto job_id = JobID::FromInt(1);
  auto create_actor_request = GenCreateActorRequest(job_id);
  auto actor = std::make_shared<gcs::GcsActor>(
      create_actor_request.task_spec(), "", counter, fake_ray_event_recorder_, "");

  // Schedule the actor with 1 available node, and the lease request should be send to the
  // node.
  gcs_actor_scheduler_->Schedule(actor);
  ASSERT_EQ(1, raylet_client_->num_workers_requested);
  ASSERT_EQ(1, raylet_client_->callbacks.size());
  ASSERT_EQ(0, worker_client_->GetNumCallbacks());

  // Grant a worker, then the actor creation request should be send to the worker.
  WorkerID worker_id = WorkerID::FromRandom();
  ASSERT_TRUE(raylet_client_->GrantWorkerLease(node->node_manager_address(),
                                               node->node_manager_port(),
                                               worker_id,
                                               node_id,
                                               NodeID::Nil()));
  ASSERT_EQ(0, raylet_client_->callbacks.size());
  WaitForCondition([&]() { return worker_client_->GetNumCallbacks() == 1; }, 1000);
  ASSERT_EQ(0, gcs_actor_scheduler_->num_retry_creating_count_);

  // Reply a IOError, then the actor creation request will retry again.
  ASSERT_TRUE(worker_client_->ReplyPushTask(Status::IOError("")));
  ASSERT_EQ(1, gcs_actor_scheduler_->num_retry_creating_count_);
  ASSERT_EQ(1, worker_client_->GetNumCallbacks());

  // Reply the actor creation request, then the actor should be scheduled successfully.
  ASSERT_TRUE(worker_client_->ReplyPushTask());
  ASSERT_EQ(0, worker_client_->GetNumCallbacks());
  ASSERT_EQ(0, failure_actors_.size());
  ASSERT_EQ(1, success_actors_.size());
  ASSERT_EQ(actor, success_actors_.front());
  ASSERT_EQ(actor->GetNodeID(), node_id);
  ASSERT_EQ(actor->GetWorkerID(), worker_id);
}

TEST_F(GcsActorSchedulerTest, TestNodeFailedWhenLeasing) {
  auto node = GenNodeInfo();
  auto node_id = NodeID::FromBinary(node->node_id());
  gcs_node_manager_->AddNode(node);
  ASSERT_EQ(1, gcs_node_manager_->GetAllAliveNodes().size());

  auto job_id = JobID::FromInt(1);
  auto create_actor_request = GenCreateActorRequest(job_id);
  auto actor = std::make_shared<gcs::GcsActor>(
      create_actor_request.task_spec(), "", counter, fake_ray_event_recorder_, "");

  // Schedule the actor with 1 available node, and the lease request should be send to the
  // node.
  gcs_actor_scheduler_->Schedule(actor);
  ASSERT_EQ(1, raylet_client_->num_workers_requested);
  ASSERT_EQ(1, raylet_client_->callbacks.size());

  // Remove the node and cancel the scheduling on this node, the scheduling should be
  // interrupted.
  rpc::NodeDeathInfo death_info;
  gcs_node_manager_->RemoveNode(node_id, death_info, rpc::GcsNodeInfo::DEAD, 1000);
  ASSERT_EQ(0, gcs_node_manager_->GetAllAliveNodes().size());
  auto actor_ids = gcs_actor_scheduler_->CancelOnNode(node_id);
  ASSERT_EQ(1, actor_ids.size());
  ASSERT_EQ(actor->GetActorID(), actor_ids.front());
  ASSERT_EQ(1, raylet_client_->num_workers_requested);
  ASSERT_EQ(1, raylet_client_->callbacks.size());

  // Grant a worker, which will influence nothing.
  ASSERT_TRUE(raylet_client_->GrantWorkerLease(node->node_manager_address(),
                                               node->node_manager_port(),
                                               WorkerID::FromRandom(),
                                               node_id,
                                               NodeID::Nil()));
  ASSERT_EQ(1, raylet_client_->num_workers_requested);
  ASSERT_EQ(0, raylet_client_->callbacks.size());
  ASSERT_EQ(0, gcs_actor_scheduler_->num_retry_leasing_count_);

  ASSERT_EQ(0, success_actors_.size());
  ASSERT_EQ(0, failure_actors_.size());
}

TEST_F(GcsActorSchedulerTest, TestLeasingCancelledWhenLeasing) {
  auto node = GenNodeInfo();
  auto node_id = NodeID::FromBinary(node->node_id());
  gcs_node_manager_->AddNode(node);
  ASSERT_EQ(1, gcs_node_manager_->GetAllAliveNodes().size());

  auto job_id = JobID::FromInt(1);
  auto create_actor_request = GenCreateActorRequest(job_id);
  auto actor = std::make_shared<gcs::GcsActor>(
      create_actor_request.task_spec(), "", counter, fake_ray_event_recorder_, "");

  // Schedule the actor with 1 available node, and the lease request should be send to the
  // node.
  gcs_actor_scheduler_->Schedule(actor);
  ASSERT_EQ(1, raylet_client_->num_workers_requested);
  ASSERT_EQ(1, raylet_client_->callbacks.size());

  // Cancel the lease request.
  gcs_actor_scheduler_->CancelOnLeasing(
      node_id, actor->GetActorID(), actor->GetLeaseSpecification().LeaseId());
  ASSERT_EQ(1, raylet_client_->num_workers_requested);
  ASSERT_EQ(1, raylet_client_->callbacks.size());

  // Grant a worker, which will influence nothing.
  ASSERT_TRUE(raylet_client_->GrantWorkerLease(node->node_manager_address(),
                                               node->node_manager_port(),
                                               WorkerID::FromRandom(),
                                               node_id,
                                               NodeID::Nil()));
  ASSERT_EQ(1, raylet_client_->num_workers_requested);
  ASSERT_EQ(0, raylet_client_->callbacks.size());
  ASSERT_EQ(0, gcs_actor_scheduler_->num_retry_leasing_count_);

  ASSERT_EQ(0, success_actors_.size());
  ASSERT_EQ(0, failure_actors_.size());
}

TEST_F(GcsActorSchedulerTest, TestNodeFailedWhenCreating) {
  auto node = GenNodeInfo();
  auto node_id = NodeID::FromBinary(node->node_id());
  gcs_node_manager_->AddNode(node);
  ASSERT_EQ(1, gcs_node_manager_->GetAllAliveNodes().size());

  auto job_id = JobID::FromInt(1);
  auto create_actor_request = GenCreateActorRequest(job_id);
  auto actor = std::make_shared<gcs::GcsActor>(
      create_actor_request.task_spec(), "", counter, fake_ray_event_recorder_, "");

  // Schedule the actor with 1 available node, and the lease request should be send to the
  // node.
  gcs_actor_scheduler_->Schedule(actor);
  ASSERT_EQ(1, raylet_client_->num_workers_requested);
  ASSERT_EQ(1, raylet_client_->callbacks.size());
  ASSERT_EQ(0, worker_client_->GetNumCallbacks());

  // Grant a worker, then the actor creation request should be send to the worker.
  ASSERT_TRUE(raylet_client_->GrantWorkerLease(node->node_manager_address(),
                                               node->node_manager_port(),
                                               WorkerID::FromRandom(),
                                               node_id,
                                               NodeID::Nil()));
  ASSERT_EQ(0, raylet_client_->callbacks.size());
  WaitForCondition([&]() { return worker_client_->GetNumCallbacks() == 1; }, 1000);

  // Remove the node and cancel the scheduling on this node, the scheduling should be
  // interrupted.
  rpc::NodeDeathInfo death_info;
  gcs_node_manager_->RemoveNode(node_id, death_info, rpc::GcsNodeInfo::DEAD, 1000);
  ASSERT_EQ(0, gcs_node_manager_->GetAllAliveNodes().size());
  auto actor_ids = gcs_actor_scheduler_->CancelOnNode(node_id);
  ASSERT_EQ(1, actor_ids.size());
  ASSERT_EQ(actor->GetActorID(), actor_ids.front());
  ASSERT_EQ(1, worker_client_->GetNumCallbacks());

  // Reply the actor creation request, which will influence nothing.
  ASSERT_TRUE(worker_client_->ReplyPushTask());
  ASSERT_EQ(0, worker_client_->GetNumCallbacks());
  ASSERT_EQ(0, gcs_actor_scheduler_->num_retry_creating_count_);

  ASSERT_EQ(0, success_actors_.size());
  ASSERT_EQ(0, failure_actors_.size());
}

TEST_F(GcsActorSchedulerTest, TestWorkerFailedWhenCreating) {
  auto node = GenNodeInfo();
  auto node_id = NodeID::FromBinary(node->node_id());
  gcs_node_manager_->AddNode(node);
  ASSERT_EQ(1, gcs_node_manager_->GetAllAliveNodes().size());

  auto job_id = JobID::FromInt(1);
  auto create_actor_request = GenCreateActorRequest(job_id);
  auto actor = std::make_shared<gcs::GcsActor>(
      create_actor_request.task_spec(), "", counter, fake_ray_event_recorder_, "");

  // Schedule the actor with 1 available node, and the lease request should be send to the
  // node.
  gcs_actor_scheduler_->Schedule(actor);
  ASSERT_EQ(1, raylet_client_->num_workers_requested);
  ASSERT_EQ(1, raylet_client_->callbacks.size());
  ASSERT_EQ(0, worker_client_->GetNumCallbacks());

  // Grant a worker, then the actor creation request should be send to the worker.
  auto worker_id = WorkerID::FromRandom();
  ASSERT_TRUE(raylet_client_->GrantWorkerLease(node->node_manager_address(),
                                               node->node_manager_port(),
                                               worker_id,
                                               node_id,
                                               NodeID::Nil()));
  ASSERT_EQ(0, raylet_client_->callbacks.size());
  WaitForCondition([&]() { return worker_client_->GetNumCallbacks() == 1; }, 1000);

  // Cancel the scheduling on this node, the scheduling should be interrupted.
  ASSERT_EQ(actor->GetActorID(),
            gcs_actor_scheduler_->CancelOnWorker(node_id, worker_id));
  ASSERT_EQ(1, worker_client_->GetNumCallbacks());

  // Reply the actor creation request, which will influence nothing.
  ASSERT_TRUE(worker_client_->ReplyPushTask());
  ASSERT_EQ(0, worker_client_->GetNumCallbacks());
  ASSERT_EQ(0, gcs_actor_scheduler_->num_retry_creating_count_);

  ASSERT_EQ(0, success_actors_.size());
  ASSERT_EQ(0, failure_actors_.size());
}

TEST_F(GcsActorSchedulerTest, TestSpillback) {
  auto node1 = GenNodeInfo();
  auto node_id_1 = NodeID::FromBinary(node1->node_id());
  gcs_node_manager_->AddNode(node1);
  ASSERT_EQ(1, gcs_node_manager_->GetAllAliveNodes().size());

  auto job_id = JobID::FromInt(1);
  auto create_actor_request = GenCreateActorRequest(job_id);
  auto actor = std::make_shared<gcs::GcsActor>(
      create_actor_request.task_spec(), "", counter, fake_ray_event_recorder_, "");

  // Schedule the actor with 1 available node, and the lease request should be send to the
  // node.
  gcs_actor_scheduler_->Schedule(actor);
  ASSERT_EQ(1, raylet_client_->num_workers_requested);
  ASSERT_EQ(1, raylet_client_->callbacks.size());
  ASSERT_EQ(0, worker_client_->GetNumCallbacks());

  // Add another node.
  auto node2 = GenNodeInfo();
  auto node_id_2 = NodeID::FromBinary(node2->node_id());
  gcs_node_manager_->AddNode(node2);
  ASSERT_EQ(2, gcs_node_manager_->GetAllAliveNodes().size());

  // Grant with an invalid spillback node, and schedule again.
  auto invalid_node_id = NodeID::FromBinary(GenNodeInfo()->node_id());
  ASSERT_TRUE(raylet_client_->GrantWorkerLease(node2->node_manager_address(),
                                               node2->node_manager_port(),
                                               WorkerID::Nil(),
                                               node_id_1,
                                               invalid_node_id));
  ASSERT_EQ(2, raylet_client_->num_workers_requested);
  ASSERT_EQ(1, raylet_client_->callbacks.size());
  ASSERT_EQ(0, worker_client_->GetNumCallbacks());

  // Grant with a spillback node(node2), and the lease request should be send to the
  // node2.
  ASSERT_TRUE(raylet_client_->GrantWorkerLease(node2->node_manager_address(),
                                               node2->node_manager_port(),
                                               WorkerID::Nil(),
                                               node_id_1,
                                               node_id_2));
  ASSERT_EQ(3, raylet_client_->num_workers_requested);
  ASSERT_EQ(1, raylet_client_->callbacks.size());
  ASSERT_EQ(0, worker_client_->GetNumCallbacks());

  // Grant a worker, then the actor creation request should be send to the worker.
  WorkerID worker_id = WorkerID::FromRandom();
  ASSERT_TRUE(raylet_client_->GrantWorkerLease(node2->node_manager_address(),
                                               node2->node_manager_port(),
                                               worker_id,
                                               node_id_2,
                                               NodeID::Nil()));
  ASSERT_EQ(0, raylet_client_->callbacks.size());
  WaitForCondition([&]() { return worker_client_->GetNumCallbacks() == 1; }, 1000);

  // Reply the actor creation request, then the actor should be scheduled successfully.
  ASSERT_TRUE(worker_client_->ReplyPushTask());
  ASSERT_EQ(0, worker_client_->GetNumCallbacks());

  ASSERT_EQ(0, failure_actors_.size());
  ASSERT_EQ(1, success_actors_.size());
  ASSERT_EQ(actor, success_actors_.front());
  ASSERT_EQ(actor->GetNodeID(), node_id_2);
  ASSERT_EQ(actor->GetWorkerID(), worker_id);
}

TEST_F(GcsActorSchedulerTest, TestReschedule) {
  auto node1 = GenNodeInfo();
  auto node_id_1 = NodeID::FromBinary(node1->node_id());
  gcs_node_manager_->AddNode(node1);
  ASSERT_EQ(1, gcs_node_manager_->GetAllAliveNodes().size());

  // 1.Actor is already tied to a leased worker.
  auto job_id = JobID::FromInt(1);
  auto create_actor_request = GenCreateActorRequest(job_id);
  auto actor = std::make_shared<gcs::GcsActor>(
      create_actor_request.task_spec(), "", counter, fake_ray_event_recorder_, "");
  rpc::Address address;
  WorkerID worker_id = WorkerID::FromRandom();
  address.set_node_id(node_id_1.Binary());
  address.set_worker_id(worker_id.Binary());
  actor->UpdateAddress(address);

  // Reschedule the actor with 1 available node, and the actor creation request should be
  // send to the worker.
  gcs_actor_scheduler_->Reschedule(actor);
  ASSERT_EQ(0, raylet_client_->num_workers_requested);
  ASSERT_EQ(0, raylet_client_->callbacks.size());
  ASSERT_EQ(1, worker_client_->GetNumCallbacks());

  // Reply the actor creation request, then the actor should be scheduled successfully.
  ASSERT_TRUE(worker_client_->ReplyPushTask());
  ASSERT_EQ(0, worker_client_->GetNumCallbacks());

  // 2.Actor is not tied to a leased worker.
  actor->UpdateAddress(rpc::Address());
  actor->GetMutableActorTableData()->clear_resource_mapping();

  // Reschedule the actor with 1 available node.
  gcs_actor_scheduler_->Reschedule(actor);

  // Grant a worker, then the actor creation request should be send to the worker.
  ASSERT_TRUE(raylet_client_->GrantWorkerLease(node1->node_manager_address(),
                                               node1->node_manager_port(),
                                               worker_id,
                                               node_id_1,
                                               NodeID::Nil()));
  ASSERT_EQ(0, raylet_client_->callbacks.size());
  WaitForCondition([&]() { return worker_client_->GetNumCallbacks() == 1; }, 1000);

  // Reply the actor creation request, then the actor should be scheduled successfully.
  ASSERT_TRUE(worker_client_->ReplyPushTask());
  ASSERT_EQ(0, worker_client_->GetNumCallbacks());

  ASSERT_EQ(0, failure_actors_.size());
  ASSERT_EQ(2, success_actors_.size());
}

TEST_F(GcsActorSchedulerTest, TestReleaseUnusedActorWorkers) {
  // Test the case that GCS won't send `RequestWorkerLease` request to the raylet,
  // if there is still a pending `ReleaseUnusedActorWorkers` request.

  // Add a node to the cluster.
  auto node = GenNodeInfo();
  auto node_id = NodeID::FromBinary(node->node_id());
  gcs_node_manager_->AddNode(node);
  ASSERT_EQ(1, gcs_node_manager_->GetAllAliveNodes().size());

  // Send a `ReleaseUnusedActorWorkers` request to the node.
  absl::flat_hash_map<NodeID, std::vector<WorkerID>> node_to_workers;
  node_to_workers[node_id].push_back({WorkerID::FromRandom()});
  gcs_actor_scheduler_->ReleaseUnusedActorWorkers(node_to_workers);
  ASSERT_EQ(1, raylet_client_->num_release_unused_workers);
  ASSERT_EQ(1, raylet_client_->release_callbacks.size());

  // Schedule an actor which is not tied to a worker, this should invoke the
  // `LeaseWorkerFromNode` method.
  // But since the `ReleaseUnusedActorWorkers` request hasn't finished,
  // `GcsActorScheduler` won't send `RequestWorkerLease` request to node immediately. But
  // instead, it will invoke the `RetryLeasingWorkerFromNode` to retry later.
  auto job_id = JobID::FromInt(1);
  auto request = GenCreateActorRequest(job_id);
  auto actor = std::make_shared<gcs::GcsActor>(
      request.task_spec(), "", counter, fake_ray_event_recorder_, "");
  gcs_actor_scheduler_->Schedule(actor);
  ASSERT_EQ(2, gcs_actor_scheduler_->num_retry_leasing_count_);
  ASSERT_EQ(raylet_client_->num_workers_requested, 0);

  // When `GcsActorScheduler` receives the `ReleaseUnusedActorWorkers` reply, it will send
  // out the `RequestWorkerLease` request.
  ASSERT_TRUE(raylet_client_->ReplyReleaseUnusedActorWorkers());
  gcs_actor_scheduler_->DoRetryLeasingWorkerFromNode(actor, node);
  ASSERT_EQ(raylet_client_->num_workers_requested, 1);
}

}  // namespace gcs
}  // namespace ray
