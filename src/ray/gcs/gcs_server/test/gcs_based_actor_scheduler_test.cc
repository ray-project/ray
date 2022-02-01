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

#include "gtest/gtest.h"
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/gcs/gcs_server/gcs_actor_distribution.h"
#include "ray/gcs/gcs_server/gcs_actor_manager.h"
#include "ray/gcs/gcs_server/gcs_resource_manager.h"
#include "ray/gcs/gcs_server/gcs_resource_scheduler.h"
#include "ray/gcs/gcs_server/test/gcs_server_test_util.h"
#include "ray/gcs/test/gcs_test_util.h"

namespace ray {
class GcsBasedActorSchedulerTest : public ::testing::Test {
 public:
  void SetUp() override {
    raylet_client_ = std::make_shared<GcsServerMocker::MockRayletClient>();
    raylet_client_pool_ = std::make_shared<rpc::NodeManagerClientPool>(
        [this](const rpc::Address &addr) { return raylet_client_; });
    worker_client_ = std::make_shared<GcsServerMocker::MockWorkerClient>();
    gcs_publisher_ = std::make_shared<gcs::GcsPublisher>(
        std::make_unique<GcsServerMocker::MockGcsPubSub>(redis_client_));
    gcs_table_storage_ = std::make_shared<gcs::RedisGcsTableStorage>(redis_client_);
    gcs_node_manager_ = std::make_shared<gcs::GcsNodeManager>(
        gcs_publisher_, gcs_table_storage_, raylet_client_pool_);
    store_client_ = std::make_shared<gcs::InMemoryStoreClient>(io_service_);
    gcs_actor_table_ =
        std::make_shared<GcsServerMocker::MockedGcsActorTable>(store_client_);
    gcs_resource_manager_ = std::make_shared<gcs::GcsResourceManager>(
        io_service_, gcs_publisher_, gcs_table_storage_, true);
    auto resource_scheduler =
        std::make_shared<gcs::GcsResourceScheduler>(*gcs_resource_manager_);
    gcs_actor_scheduler_ =
        std::make_shared<GcsServerMocker::MockedGcsBasedActorScheduler>(
            io_service_, *gcs_actor_table_, *gcs_node_manager_, gcs_resource_manager_,
            resource_scheduler,
            /*schedule_failure_handler=*/
            [this](
                std::shared_ptr<gcs::GcsActor> actor,
                const rpc::RequestWorkerLeaseReply::SchedulingFailureType failure_type) {
              failure_actors_.emplace_back(std::move(actor));
            },
            /*schedule_success_handler=*/
            [this](std::shared_ptr<gcs::GcsActor> actor,
                   const rpc::PushTaskReply &reply) {
              success_actors_.emplace_back(std::move(actor));
            },
            raylet_client_pool_,
            /*client_factory=*/
            [this](const rpc::Address &address) { return worker_client_; });
  }

  std::shared_ptr<gcs::GcsActor> NewGcsActor(
      const std::unordered_map<std::string, double> &required_placement_resources =
          std::unordered_map<std::string, double>()) {
    rpc::Address owner_address;
    owner_address.set_raylet_id(NodeID::FromRandom().Binary());
    owner_address.set_ip_address("127.0.0.1");
    owner_address.set_port(5678);
    owner_address.set_worker_id(WorkerID::FromRandom().Binary());
    auto job_id = JobID::FromInt(1);

    std::unordered_map<std::string, double> required_resources;
    auto actor_creating_task_spec = Mocker::GenActorCreationTask(
        job_id, /*max_restarts=*/1, /*detached=*/true, /*name=*/"", "", owner_address,
        required_resources, required_placement_resources);
    return std::make_shared<gcs::GcsActor>(actor_creating_task_spec.GetMessage(),
                                           /*ray_namespace=*/"");
  }

  std::shared_ptr<rpc::GcsNodeInfo> AddNewNode(
      std::unordered_map<std::string, double> node_resources) {
    auto node_info = Mocker::GenNodeInfo();
    node_info->mutable_resources_total()->insert(node_resources.begin(),
                                                 node_resources.end());
    gcs_node_manager_->AddNode(node_info);
    gcs_resource_manager_->OnNodeAdd(*node_info);
    return node_info;
  }

 protected:
  instrumented_io_context io_service_;
  std::shared_ptr<gcs::StoreClient> store_client_;
  std::shared_ptr<GcsServerMocker::MockedGcsActorTable> gcs_actor_table_;
  std::shared_ptr<GcsServerMocker::MockRayletClient> raylet_client_;
  std::shared_ptr<rpc::NodeManagerClientPool> raylet_client_pool_;
  std::shared_ptr<GcsServerMocker::MockWorkerClient> worker_client_;
  std::shared_ptr<gcs::GcsNodeManager> gcs_node_manager_;
  std::shared_ptr<gcs::GcsResourceManager> gcs_resource_manager_;
  std::shared_ptr<GcsServerMocker::MockedGcsBasedActorScheduler> gcs_actor_scheduler_;
  std::vector<std::shared_ptr<gcs::GcsActor>> success_actors_;
  std::vector<std::shared_ptr<gcs::GcsActor>> failure_actors_;
  std::shared_ptr<gcs::GcsPublisher> gcs_publisher_;
  std::shared_ptr<gcs::GcsTableStorage> gcs_table_storage_;
  std::shared_ptr<gcs::RedisClient> redis_client_;
};

TEST_F(GcsBasedActorSchedulerTest, TestScheduleFailedWithZeroNode) {
  ASSERT_EQ(0, gcs_node_manager_->GetAllAliveNodes().size());

  std::unordered_map<std::string, double> required_placement_resources = {
      {kMemory_ResourceLabel, 32}};
  auto actor = NewGcsActor(required_placement_resources);

  // Schedule with zero node.
  gcs_actor_scheduler_->Schedule(actor);

  // The lease request should not be send and the scheduling of actor should fail as there
  // are no available nodes.
  ASSERT_EQ(raylet_client_->num_workers_requested, 0);
  ASSERT_EQ(0, success_actors_.size());
  ASSERT_EQ(1, failure_actors_.size());
  ASSERT_EQ(actor, failure_actors_.front());
  ASSERT_TRUE(actor->GetNodeID().IsNil());
}

TEST_F(GcsBasedActorSchedulerTest, TestNotEnoughClusterResources) {
  // Add a node with 64 memory units and 8 CPU.
  std::unordered_map<std::string, double> node_resources = {{kMemory_ResourceLabel, 64},
                                                            {kCPU_ResourceLabel, 8}};
  AddNewNode(node_resources);
  ASSERT_EQ(1, gcs_node_manager_->GetAllAliveNodes().size());

  // Schedule a actor (requiring 128 memory units and 4 CPU).
  std::unordered_map<std::string, double> required_placement_resources = {
      {kMemory_ResourceLabel, 128}, {kCPU_ResourceLabel, 4}};
  auto actor = NewGcsActor(required_placement_resources);

  gcs_actor_scheduler_->Schedule(actor);

  // The lease request should not be sent and the scheduling of actor should fail as there
  // are not enough cluster resources.
  ASSERT_EQ(raylet_client_->num_workers_requested, 0);
  ASSERT_EQ(0, success_actors_.size());
  ASSERT_EQ(1, failure_actors_.size());
  ASSERT_EQ(actor, failure_actors_.front());
  ASSERT_TRUE(actor->GetNodeID().IsNil());
}

TEST_F(GcsBasedActorSchedulerTest, TestScheduleAndDestroyOneActor) {
  // Add a node with 64 memory units and 8 CPU.
  std::unordered_map<std::string, double> node_resources = {{kMemory_ResourceLabel, 64},
                                                            {kCPU_ResourceLabel, 8}};
  auto node = AddNewNode(node_resources);
  auto node_id = NodeID::FromBinary(node->node_id());
  ASSERT_EQ(1, gcs_node_manager_->GetAllAliveNodes().size());
  auto cluster_resources_before_scheduling = gcs_resource_manager_->GetClusterResources();
  ASSERT_TRUE(cluster_resources_before_scheduling.contains(node_id));

  // Schedule a actor (requiring 32 memory units and 4 CPU).
  std::unordered_map<std::string, double> required_placement_resources = {
      {kMemory_ResourceLabel, 32}, {kCPU_ResourceLabel, 4}};
  auto actor = NewGcsActor(required_placement_resources);

  gcs_actor_scheduler_->Schedule(actor);

  ASSERT_EQ(1, raylet_client_->num_workers_requested);
  ASSERT_EQ(1, raylet_client_->callbacks.size());
  ASSERT_EQ(0, worker_client_->callbacks.size());

  // Grant a worker, then the actor creation request should be sent to the worker.
  WorkerID worker_id = WorkerID::FromRandom();
  ASSERT_TRUE(raylet_client_->GrantWorkerLease(node->node_manager_address(),
                                               node->node_manager_port(), worker_id,
                                               node_id, NodeID::Nil()));
  ASSERT_EQ(0, raylet_client_->callbacks.size());
  ASSERT_EQ(1, worker_client_->callbacks.size());

  // Reply the actor creation request, then the actor should be scheduled successfully.
  ASSERT_TRUE(worker_client_->ReplyPushTask());
  ASSERT_EQ(0, worker_client_->callbacks.size());
  ASSERT_EQ(0, failure_actors_.size());
  ASSERT_EQ(1, success_actors_.size());
  ASSERT_EQ(actor, success_actors_.front());
  ASSERT_EQ(actor->GetNodeID(), node_id);
  ASSERT_EQ(actor->GetWorkerID(), worker_id);

  auto cluster_resources_after_scheduling = gcs_resource_manager_->GetClusterResources();
  ASSERT_TRUE(cluster_resources_after_scheduling.contains(node_id));
  ASSERT_FALSE(
      cluster_resources_before_scheduling[node_id].GetAvailableResources().IsEqual(
          cluster_resources_after_scheduling[node_id].GetAvailableResources()));

  // When destroying an actor, its acquired resources have to be returned.
  gcs_actor_scheduler_->OnActorDestruction(actor);
  auto cluster_resources_after_destruction = gcs_resource_manager_->GetClusterResources();
  ASSERT_TRUE(cluster_resources_after_destruction.contains(node_id));
  ASSERT_TRUE(
      cluster_resources_before_scheduling[node_id].GetAvailableResources().IsEqual(
          cluster_resources_after_destruction[node_id].GetAvailableResources()));
}

TEST_F(GcsBasedActorSchedulerTest, TestBalancedSchedule) {
  // Add two nodes, each with 10 memory units and 10 CPU.
  for (int i = 0; i < 2; i++) {
    std::unordered_map<std::string, double> node_resources = {{kMemory_ResourceLabel, 10},
                                                              {kCPU_ResourceLabel, 10}};
    AddNewNode(node_resources);
  }

  ASSERT_EQ(2, gcs_node_manager_->GetAllAliveNodes().size());

  std::unordered_map<std::string, double> required_placement_resources = {
      {kMemory_ResourceLabel, 1}, {kCPU_ResourceLabel, 1}};
  std::unordered_map<NodeID, int> sched_counts;

  // Schedule 10 actors, each requiring 1 memory unit and 1 CPU.
  for (int i = 0; i < 10; i++) {
    auto actor = NewGcsActor(required_placement_resources);

    gcs_actor_scheduler_->Schedule(actor);

    ASSERT_FALSE(actor->GetNodeID().IsNil());
    sched_counts[actor->GetNodeID()]++;
  }

  // Make sure the 10 actors are balanced.
  for (const auto &entry : sched_counts) {
    ASSERT_EQ(5, entry.second);
  }
}

TEST_F(GcsBasedActorSchedulerTest, TestRejectedRequestWorkerLeaseReply) {
  // Add a node with 64 memory units and 8 CPU.
  std::unordered_map<std::string, double> node_resources_1 = {{kMemory_ResourceLabel, 64},
                                                              {kCPU_ResourceLabel, 8}};
  auto node1 = AddNewNode(node_resources_1);
  auto node_id_1 = NodeID::FromBinary(node1->node_id());
  // Add a node with 32 memory units and 4 CPU.
  std::unordered_map<std::string, double> node_resources_2 = {{kMemory_ResourceLabel, 32},
                                                              {kCPU_ResourceLabel, 4}};
  auto node2 = AddNewNode(node_resources_2);
  auto node_id_2 = NodeID::FromBinary(node2->node_id());
  ASSERT_EQ(2, gcs_node_manager_->GetAllAliveNodes().size());

  // Schedule a actor (requiring 32 memory units and 4 CPU).
  std::unordered_map<std::string, double> required_placement_resources = {
      {kMemory_ResourceLabel, 32}, {kCPU_ResourceLabel, 4}};
  auto actor = NewGcsActor(required_placement_resources);

  // Schedule the actor, and the lease request should be sent to node1.
  gcs_actor_scheduler_->Schedule(actor);
  ASSERT_EQ(node_id_1, actor->GetNodeID());
  ASSERT_EQ(1, raylet_client_->num_workers_requested);
  ASSERT_EQ(1, raylet_client_->callbacks.size());
  ASSERT_EQ(0, worker_client_->callbacks.size());

  // Mock a rejected reply, then the actor will be rescheduled.
  ASSERT_TRUE(raylet_client_->GrantWorkerLease(
      node1->node_manager_address(), node1->node_manager_port(), WorkerID::FromRandom(),
      node_id_1, NodeID::Nil(), Status::OK(), /*rejected=*/true));
  ASSERT_EQ(2, raylet_client_->num_workers_requested);
  ASSERT_EQ(1, raylet_client_->callbacks.size());
  ASSERT_EQ(0, worker_client_->callbacks.size());

  // node1's resources have been preempted. The actor is rescheduled to node2.
  ASSERT_EQ(node_id_2, actor->GetNodeID());
}

TEST_F(GcsBasedActorSchedulerTest, TestScheduleRetryWhenLeasing) {
  // Add a node with 64 memory units and 8 CPU.
  std::unordered_map<std::string, double> node_resources = {{kMemory_ResourceLabel, 64},
                                                            {kCPU_ResourceLabel, 8}};
  auto node = AddNewNode(node_resources);
  auto node_id = NodeID::FromBinary(node->node_id());
  ASSERT_EQ(1, gcs_node_manager_->GetAllAliveNodes().size());

  // Schedule a actor (requiring 32 memory units and 4 CPU).
  std::unordered_map<std::string, double> required_placement_resources = {
      {kMemory_ResourceLabel, 32}, {kCPU_ResourceLabel, 4}};
  auto actor = NewGcsActor(required_placement_resources);

  // Schedule the actor with 1 available node, and the lease request should be sent to the
  // node.
  gcs_actor_scheduler_->Schedule(actor);
  ASSERT_EQ(1, raylet_client_->num_workers_requested);
  ASSERT_EQ(1, raylet_client_->callbacks.size());
  ASSERT_EQ(0, worker_client_->callbacks.size());
  ASSERT_EQ(0, gcs_actor_scheduler_->num_retry_leasing_count_);

  // Mock a IOError reply, then the lease request will retry again.
  ASSERT_TRUE(raylet_client_->GrantWorkerLease(
      node->node_manager_address(), node->node_manager_port(), WorkerID::FromRandom(),
      node_id, NodeID::Nil(), Status::IOError("")));
  ASSERT_EQ(1, gcs_actor_scheduler_->num_retry_leasing_count_);
  ASSERT_EQ(2, raylet_client_->num_workers_requested);
  ASSERT_EQ(1, raylet_client_->callbacks.size());
  ASSERT_EQ(0, worker_client_->callbacks.size());

  // Grant a worker, then the actor creation request should be sent to the worker.
  WorkerID worker_id = WorkerID::FromRandom();
  ASSERT_TRUE(raylet_client_->GrantWorkerLease(node->node_manager_address(),
                                               node->node_manager_port(), worker_id,
                                               node_id, NodeID::Nil()));
  ASSERT_EQ(0, raylet_client_->callbacks.size());
  ASSERT_EQ(1, worker_client_->callbacks.size());

  // Reply the actor creation request, then the actor should be scheduled successfully.
  ASSERT_TRUE(worker_client_->ReplyPushTask());
  ASSERT_EQ(0, worker_client_->callbacks.size());
  ASSERT_EQ(0, failure_actors_.size());
  ASSERT_EQ(1, success_actors_.size());
  ASSERT_EQ(actor, success_actors_.front());
  ASSERT_EQ(actor->GetNodeID(), node_id);
  ASSERT_EQ(actor->GetWorkerID(), worker_id);
}

TEST_F(GcsBasedActorSchedulerTest, TestScheduleRetryWhenCreating) {
  // Add a node with 64 memory units and 8 CPU.
  std::unordered_map<std::string, double> node_resources = {{kMemory_ResourceLabel, 64},
                                                            {kCPU_ResourceLabel, 8}};
  auto node = AddNewNode(node_resources);
  auto node_id = NodeID::FromBinary(node->node_id());
  ASSERT_EQ(1, gcs_node_manager_->GetAllAliveNodes().size());

  // Schedule a actor (requiring 32 memory units and 4 CPU).
  std::unordered_map<std::string, double> required_placement_resources = {
      {kMemory_ResourceLabel, 32}, {kCPU_ResourceLabel, 4}};
  auto actor = NewGcsActor(required_placement_resources);

  // Schedule the actor with 1 available node, and the lease request should be sent to the
  // node.
  gcs_actor_scheduler_->Schedule(actor);
  ASSERT_EQ(1, raylet_client_->num_workers_requested);
  ASSERT_EQ(1, raylet_client_->callbacks.size());
  ASSERT_EQ(0, worker_client_->callbacks.size());

  // Grant a worker, then the actor creation request should be sent to the worker.
  WorkerID worker_id = WorkerID::FromRandom();
  ASSERT_TRUE(raylet_client_->GrantWorkerLease(node->node_manager_address(),
                                               node->node_manager_port(), worker_id,
                                               node_id, NodeID::Nil()));
  ASSERT_EQ(0, raylet_client_->callbacks.size());
  ASSERT_EQ(1, worker_client_->callbacks.size());
  ASSERT_EQ(0, gcs_actor_scheduler_->num_retry_creating_count_);

  // Reply a IOError, then the actor creation request will retry again.
  ASSERT_TRUE(worker_client_->ReplyPushTask(Status::IOError("")));
  ASSERT_EQ(1, gcs_actor_scheduler_->num_retry_creating_count_);
  ASSERT_EQ(1, worker_client_->callbacks.size());

  // Reply the actor creation request, then the actor should be scheduled successfully.
  ASSERT_TRUE(worker_client_->ReplyPushTask());
  ASSERT_EQ(0, worker_client_->callbacks.size());
  ASSERT_EQ(0, failure_actors_.size());
  ASSERT_EQ(1, success_actors_.size());
  ASSERT_EQ(actor, success_actors_.front());
  ASSERT_EQ(actor->GetNodeID(), node_id);
  ASSERT_EQ(actor->GetWorkerID(), worker_id);
}

TEST_F(GcsBasedActorSchedulerTest, TestNodeFailedWhenLeasing) {
  // Add a node with 64 memory units and 8 CPU.
  std::unordered_map<std::string, double> node_resources = {{kMemory_ResourceLabel, 64},
                                                            {kCPU_ResourceLabel, 8}};
  auto node = AddNewNode(node_resources);
  auto node_id = NodeID::FromBinary(node->node_id());
  ASSERT_EQ(1, gcs_node_manager_->GetAllAliveNodes().size());

  // Schedule a actor (requiring 32 memory units and 4 CPU).
  std::unordered_map<std::string, double> required_placement_resources = {
      {kMemory_ResourceLabel, 32}, {kCPU_ResourceLabel, 4}};
  auto actor = NewGcsActor(required_placement_resources);

  // Schedule the actor with 1 available node, and the lease request should be sent to the
  // node.
  gcs_actor_scheduler_->Schedule(actor);
  ASSERT_EQ(1, raylet_client_->num_workers_requested);
  ASSERT_EQ(1, raylet_client_->callbacks.size());

  // Remove the node and cancel the scheduling on this node, the scheduling should be
  // interrupted.
  gcs_node_manager_->RemoveNode(node_id);
  ASSERT_EQ(0, gcs_node_manager_->GetAllAliveNodes().size());
  auto actor_ids = gcs_actor_scheduler_->CancelOnNode(node_id);
  ASSERT_EQ(1, actor_ids.size());
  ASSERT_EQ(actor->GetActorID(), actor_ids.front());
  ASSERT_EQ(1, raylet_client_->num_workers_requested);
  ASSERT_EQ(1, raylet_client_->callbacks.size());

  // Grant a worker, which will influence nothing.
  ASSERT_TRUE(raylet_client_->GrantWorkerLease(
      node->node_manager_address(), node->node_manager_port(), WorkerID::FromRandom(),
      node_id, NodeID::Nil()));
  ASSERT_EQ(1, raylet_client_->num_workers_requested);
  ASSERT_EQ(0, raylet_client_->callbacks.size());
  ASSERT_EQ(0, gcs_actor_scheduler_->num_retry_leasing_count_);

  ASSERT_EQ(0, success_actors_.size());
  ASSERT_EQ(0, failure_actors_.size());
}

TEST_F(GcsBasedActorSchedulerTest, TestLeasingCancelledWhenLeasing) {
  // Add a node with 64 memory units and 8 CPU.
  std::unordered_map<std::string, double> node_resources = {{kMemory_ResourceLabel, 64},
                                                            {kCPU_ResourceLabel, 8}};
  auto node = AddNewNode(node_resources);
  auto node_id = NodeID::FromBinary(node->node_id());
  ASSERT_EQ(1, gcs_node_manager_->GetAllAliveNodes().size());

  // Schedule a actor (requiring 32 memory units and 4 CPU).
  std::unordered_map<std::string, double> required_placement_resources = {
      {kMemory_ResourceLabel, 32}, {kCPU_ResourceLabel, 4}};
  auto actor = NewGcsActor(required_placement_resources);

  // Schedule the actor with 1 available node, and the lease request should be sent to the
  // node.
  gcs_actor_scheduler_->Schedule(actor);
  ASSERT_EQ(1, raylet_client_->num_workers_requested);
  ASSERT_EQ(1, raylet_client_->callbacks.size());

  // Cancel the lease request.
  const auto &task_id = actor->GetCreationTaskSpecification().TaskId();
  gcs_actor_scheduler_->CancelOnLeasing(node_id, actor->GetActorID(), task_id);
  ASSERT_EQ(1, raylet_client_->num_workers_requested);
  ASSERT_EQ(1, raylet_client_->callbacks.size());

  // Grant a worker, which will influence nothing.
  ASSERT_TRUE(raylet_client_->GrantWorkerLease(
      node->node_manager_address(), node->node_manager_port(), WorkerID::FromRandom(),
      node_id, NodeID::Nil()));
  ASSERT_EQ(1, raylet_client_->num_workers_requested);
  ASSERT_EQ(0, raylet_client_->callbacks.size());
  ASSERT_EQ(0, gcs_actor_scheduler_->num_retry_leasing_count_);

  ASSERT_EQ(0, success_actors_.size());
  ASSERT_EQ(0, failure_actors_.size());
}

TEST_F(GcsBasedActorSchedulerTest, TestNodeFailedWhenCreating) {
  // Add a node with 64 memory units and 8 CPU.
  std::unordered_map<std::string, double> node_resources = {{kMemory_ResourceLabel, 64},
                                                            {kCPU_ResourceLabel, 8}};
  auto node = AddNewNode(node_resources);
  auto node_id = NodeID::FromBinary(node->node_id());
  ASSERT_EQ(1, gcs_node_manager_->GetAllAliveNodes().size());

  // Schedule a actor (requiring 32 memory units and 4 CPU).
  std::unordered_map<std::string, double> required_placement_resources = {
      {kMemory_ResourceLabel, 32}, {kCPU_ResourceLabel, 4}};
  auto actor = NewGcsActor(required_placement_resources);

  // Schedule the actor with 1 available node, and the lease request should be sent to the
  // node.
  gcs_actor_scheduler_->Schedule(actor);
  ASSERT_EQ(1, raylet_client_->num_workers_requested);
  ASSERT_EQ(1, raylet_client_->callbacks.size());
  ASSERT_EQ(0, worker_client_->callbacks.size());

  // Grant a worker, then the actor creation request should be send to the worker.
  ASSERT_TRUE(raylet_client_->GrantWorkerLease(
      node->node_manager_address(), node->node_manager_port(), WorkerID::FromRandom(),
      node_id, NodeID::Nil()));
  ASSERT_EQ(0, raylet_client_->callbacks.size());
  ASSERT_EQ(1, worker_client_->callbacks.size());

  // Remove the node and cancel the scheduling on this node, the scheduling should be
  // interrupted.
  gcs_node_manager_->RemoveNode(node_id);
  ASSERT_EQ(0, gcs_node_manager_->GetAllAliveNodes().size());
  auto actor_ids = gcs_actor_scheduler_->CancelOnNode(node_id);
  ASSERT_EQ(1, actor_ids.size());
  ASSERT_EQ(actor->GetActorID(), actor_ids.front());
  ASSERT_EQ(1, worker_client_->callbacks.size());

  // Reply the actor creation request, which will influence nothing.
  ASSERT_TRUE(worker_client_->ReplyPushTask());
  ASSERT_EQ(0, worker_client_->callbacks.size());
  ASSERT_EQ(0, gcs_actor_scheduler_->num_retry_creating_count_);

  ASSERT_EQ(0, success_actors_.size());
  ASSERT_EQ(0, failure_actors_.size());
}

TEST_F(GcsBasedActorSchedulerTest, TestWorkerFailedWhenCreating) {
  // Add a node with 64 memory units and 8 CPU.
  std::unordered_map<std::string, double> node_resources = {{kMemory_ResourceLabel, 64},
                                                            {kCPU_ResourceLabel, 8}};
  auto node = AddNewNode(node_resources);
  auto node_id = NodeID::FromBinary(node->node_id());
  ASSERT_EQ(1, gcs_node_manager_->GetAllAliveNodes().size());

  // Schedule a actor (requiring 32 memory units and 4 CPU).
  std::unordered_map<std::string, double> required_placement_resources = {
      {kMemory_ResourceLabel, 32}, {kCPU_ResourceLabel, 4}};
  auto actor = NewGcsActor(required_placement_resources);

  // Schedule the actor with 1 available node, and the lease request should be sent to the
  // node.
  gcs_actor_scheduler_->Schedule(actor);
  ASSERT_EQ(1, raylet_client_->num_workers_requested);
  ASSERT_EQ(1, raylet_client_->callbacks.size());
  ASSERT_EQ(0, worker_client_->callbacks.size());

  // Grant a worker, then the actor creation request should be send to the worker.
  auto worker_id = WorkerID::FromRandom();
  ASSERT_TRUE(raylet_client_->GrantWorkerLease(node->node_manager_address(),
                                               node->node_manager_port(), worker_id,
                                               node_id, NodeID::Nil()));
  ASSERT_EQ(0, raylet_client_->callbacks.size());
  ASSERT_EQ(1, worker_client_->callbacks.size());

  // Cancel the scheduling on this node, the scheduling should be interrupted.
  ASSERT_EQ(actor->GetActorID(),
            gcs_actor_scheduler_->CancelOnWorker(node_id, worker_id));
  ASSERT_EQ(1, worker_client_->callbacks.size());

  // Reply the actor creation request, which will influence nothing.
  ASSERT_TRUE(worker_client_->ReplyPushTask());
  ASSERT_EQ(0, worker_client_->callbacks.size());
  ASSERT_EQ(0, gcs_actor_scheduler_->num_retry_creating_count_);

  ASSERT_EQ(0, success_actors_.size());
  ASSERT_EQ(0, failure_actors_.size());
}

TEST_F(GcsBasedActorSchedulerTest, TestReschedule) {
  // Add a node with 64 memory units and 8 CPU.
  std::unordered_map<std::string, double> node_resources = {{kMemory_ResourceLabel, 64},
                                                            {kCPU_ResourceLabel, 8}};
  auto node1 = AddNewNode(node_resources);
  auto node_id_1 = NodeID::FromBinary(node1->node_id());
  ASSERT_EQ(1, gcs_node_manager_->GetAllAliveNodes().size());

  // Schedule a actor (requiring 32 memory units and 4 CPU).
  std::unordered_map<std::string, double> required_placement_resources = {
      {kMemory_ResourceLabel, 32}, {kCPU_ResourceLabel, 4}};
  auto actor = NewGcsActor(required_placement_resources);

  // 1.Actor is already tied to a leased worker.
  rpc::Address address;
  WorkerID worker_id = WorkerID::FromRandom();
  address.set_raylet_id(node_id_1.Binary());
  address.set_worker_id(worker_id.Binary());
  actor->UpdateAddress(address);

  // Reschedule the actor with 1 available node, and the actor creation request should be
  // send to the worker.
  gcs_actor_scheduler_->Reschedule(actor);
  ASSERT_EQ(0, raylet_client_->num_workers_requested);
  ASSERT_EQ(0, raylet_client_->callbacks.size());
  ASSERT_EQ(1, worker_client_->callbacks.size());

  // Reply the actor creation request, then the actor should be scheduled successfully.
  ASSERT_TRUE(worker_client_->ReplyPushTask());
  ASSERT_EQ(0, worker_client_->callbacks.size());

  // 2.Actor is not tied to a leased worker.
  actor->UpdateAddress(rpc::Address());
  actor->GetMutableActorTableData()->clear_resource_mapping();

  // Reschedule the actor with 1 available node.
  gcs_actor_scheduler_->Reschedule(actor);

  // Grant a worker, then the actor creation request should be send to the worker.
  ASSERT_TRUE(raylet_client_->GrantWorkerLease(node1->node_manager_address(),
                                               node1->node_manager_port(), worker_id,
                                               node_id_1, NodeID::Nil()));
  ASSERT_EQ(0, raylet_client_->callbacks.size());
  ASSERT_EQ(1, worker_client_->callbacks.size());

  // Reply the actor creation request, then the actor should be scheduled successfully.
  ASSERT_TRUE(worker_client_->ReplyPushTask());
  ASSERT_EQ(0, worker_client_->callbacks.size());

  ASSERT_EQ(0, failure_actors_.size());
  ASSERT_EQ(2, success_actors_.size());
}

TEST_F(GcsBasedActorSchedulerTest, TestReleaseUnusedWorkers) {
  // Test the case that GCS won't send `RequestWorkerLease` request to the raylet,
  // if there is still a pending `ReleaseUnusedWorkers` request.

  // Add a node to the cluster.
  // Add a node with 64 memory units and 8 CPU.
  std::unordered_map<std::string, double> node_resources = {{kMemory_ResourceLabel, 64},
                                                            {kCPU_ResourceLabel, 8}};
  auto node = AddNewNode(node_resources);
  auto node_id = NodeID::FromBinary(node->node_id());
  ASSERT_EQ(1, gcs_node_manager_->GetAllAliveNodes().size());

  // Send a `ReleaseUnusedWorkers` request to the node.
  std::unordered_map<NodeID, std::vector<WorkerID>> node_to_workers;
  node_to_workers[node_id].push_back({WorkerID::FromRandom()});
  gcs_actor_scheduler_->ReleaseUnusedWorkers(node_to_workers);
  ASSERT_EQ(1, raylet_client_->num_release_unused_workers);
  ASSERT_EQ(1, raylet_client_->release_callbacks.size());

  // Schedule an actor which is not tied to a worker, this should invoke the
  // `LeaseWorkerFromNode` method.
  // But since the `ReleaseUnusedWorkers` request hasn't finished, `GcsActorScheduler`
  // won't send `RequestWorkerLease` request to node immediately. But instead, it will
  // invoke the `RetryLeasingWorkerFromNode` to retry later.
  // Schedule a actor (requiring 32 memory units and 4 CPU).
  std::unordered_map<std::string, double> required_placement_resources = {
      {kMemory_ResourceLabel, 32}, {kCPU_ResourceLabel, 4}};
  auto actor = NewGcsActor(required_placement_resources);
  gcs_actor_scheduler_->Schedule(actor);
  ASSERT_EQ(2, gcs_actor_scheduler_->num_retry_leasing_count_);
  ASSERT_EQ(raylet_client_->num_workers_requested, 0);

  // When `GcsActorScheduler` receives the `ReleaseUnusedWorkers` reply, it will send
  // out the `RequestWorkerLease` request.
  ASSERT_TRUE(raylet_client_->ReplyReleaseUnusedWorkers());
  gcs_actor_scheduler_->TryLeaseWorkerFromNodeAgain(actor, node);
  ASSERT_EQ(raylet_client_->num_workers_requested, 1);
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
