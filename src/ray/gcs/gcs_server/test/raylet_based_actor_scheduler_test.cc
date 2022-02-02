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
#include "ray/gcs/gcs_server/test/gcs_server_test_util.h"
#include "ray/gcs/test/gcs_test_util.h"

namespace ray {
class RayletBasedActorSchedulerTest : public ::testing::Test {
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
    gcs_actor_scheduler_ =
        std::make_shared<GcsServerMocker::MockedRayletBasedActorScheduler>(
            io_service_, *gcs_actor_table_, *gcs_node_manager_,
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

 protected:
  instrumented_io_context io_service_;
  std::shared_ptr<gcs::StoreClient> store_client_;
  std::shared_ptr<GcsServerMocker::MockedGcsActorTable> gcs_actor_table_;
  std::shared_ptr<GcsServerMocker::MockRayletClient> raylet_client_;
  std::shared_ptr<GcsServerMocker::MockWorkerClient> worker_client_;
  std::shared_ptr<gcs::GcsNodeManager> gcs_node_manager_;
  std::shared_ptr<GcsServerMocker::MockedRayletBasedActorScheduler> gcs_actor_scheduler_;
  std::vector<std::shared_ptr<gcs::GcsActor>> success_actors_;
  std::vector<std::shared_ptr<gcs::GcsActor>> failure_actors_;
  std::shared_ptr<gcs::GcsPublisher> gcs_publisher_;
  std::shared_ptr<gcs::GcsTableStorage> gcs_table_storage_;
  std::shared_ptr<gcs::RedisClient> redis_client_;
  std::shared_ptr<rpc::NodeManagerClientPool> raylet_client_pool_;
};

TEST_F(RayletBasedActorSchedulerTest, TestScheduleFailedWithZeroNode) {
  ASSERT_EQ(0, gcs_node_manager_->GetAllAliveNodes().size());

  auto job_id = JobID::FromInt(1);
  auto create_actor_request = Mocker::GenCreateActorRequest(job_id);
  auto actor = std::make_shared<gcs::GcsActor>(create_actor_request.task_spec(), "");

  // Schedule the actor with zero node.
  gcs_actor_scheduler_->Schedule(actor);

  // The lease request should not be send and the scheduling of actor should fail as there
  // are no available nodes.
  ASSERT_EQ(raylet_client_->num_workers_requested, 0);
  ASSERT_EQ(0, success_actors_.size());
  ASSERT_EQ(1, failure_actors_.size());
  ASSERT_EQ(actor, failure_actors_.front());
  ASSERT_TRUE(actor->GetNodeID().IsNil());
}

TEST_F(RayletBasedActorSchedulerTest, TestScheduleActorSuccess) {
  auto node = Mocker::GenNodeInfo();
  auto node_id = NodeID::FromBinary(node->node_id());
  gcs_node_manager_->AddNode(node);
  ASSERT_EQ(1, gcs_node_manager_->GetAllAliveNodes().size());

  auto job_id = JobID::FromInt(1);
  auto create_actor_request = Mocker::GenCreateActorRequest(job_id);
  auto actor = std::make_shared<gcs::GcsActor>(create_actor_request.task_spec(), "");

  // Schedule the actor with 1 available node, and the lease request should be send to the
  // node.
  gcs_actor_scheduler_->Schedule(actor);
  ASSERT_EQ(1, raylet_client_->num_workers_requested);
  ASSERT_EQ(1, raylet_client_->callbacks.size());
  ASSERT_EQ(0, worker_client_->callbacks.size());

  // Grant a worker, then the actor creation request should be send to the worker.
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

TEST_F(RayletBasedActorSchedulerTest, TestScheduleRetryWhenLeasing) {
  auto node = Mocker::GenNodeInfo();
  auto node_id = NodeID::FromBinary(node->node_id());
  gcs_node_manager_->AddNode(node);
  ASSERT_EQ(1, gcs_node_manager_->GetAllAliveNodes().size());

  auto job_id = JobID::FromInt(1);
  auto create_actor_request = Mocker::GenCreateActorRequest(job_id);
  auto actor = std::make_shared<gcs::GcsActor>(create_actor_request.task_spec(), "");

  // Schedule the actor with 1 available node, and the lease request should be send to the
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

  // Grant a worker, then the actor creation request should be send to the worker.
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

TEST_F(RayletBasedActorSchedulerTest, TestScheduleRetryWhenCreating) {
  auto node = Mocker::GenNodeInfo();
  auto node_id = NodeID::FromBinary(node->node_id());
  gcs_node_manager_->AddNode(node);
  ASSERT_EQ(1, gcs_node_manager_->GetAllAliveNodes().size());

  auto job_id = JobID::FromInt(1);
  auto create_actor_request = Mocker::GenCreateActorRequest(job_id);
  auto actor = std::make_shared<gcs::GcsActor>(create_actor_request.task_spec(), "");

  // Schedule the actor with 1 available node, and the lease request should be send to the
  // node.
  gcs_actor_scheduler_->Schedule(actor);
  ASSERT_EQ(1, raylet_client_->num_workers_requested);
  ASSERT_EQ(1, raylet_client_->callbacks.size());
  ASSERT_EQ(0, worker_client_->callbacks.size());

  // Grant a worker, then the actor creation request should be send to the worker.
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

TEST_F(RayletBasedActorSchedulerTest, TestNodeFailedWhenLeasing) {
  auto node = Mocker::GenNodeInfo();
  auto node_id = NodeID::FromBinary(node->node_id());
  gcs_node_manager_->AddNode(node);
  ASSERT_EQ(1, gcs_node_manager_->GetAllAliveNodes().size());

  auto job_id = JobID::FromInt(1);
  auto create_actor_request = Mocker::GenCreateActorRequest(job_id);
  auto actor = std::make_shared<gcs::GcsActor>(create_actor_request.task_spec(), "");

  // Schedule the actor with 1 available node, and the lease request should be send to the
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

TEST_F(RayletBasedActorSchedulerTest, TestLeasingCancelledWhenLeasing) {
  auto node = Mocker::GenNodeInfo();
  auto node_id = NodeID::FromBinary(node->node_id());
  gcs_node_manager_->AddNode(node);
  ASSERT_EQ(1, gcs_node_manager_->GetAllAliveNodes().size());

  auto job_id = JobID::FromInt(1);
  auto create_actor_request = Mocker::GenCreateActorRequest(job_id);
  auto actor = std::make_shared<gcs::GcsActor>(create_actor_request.task_spec(), "");

  // Schedule the actor with 1 available node, and the lease request should be send to the
  // node.
  gcs_actor_scheduler_->Schedule(actor);
  ASSERT_EQ(1, raylet_client_->num_workers_requested);
  ASSERT_EQ(1, raylet_client_->callbacks.size());

  // Cancel the lease request.
  const auto &task_id = TaskID::FromBinary(create_actor_request.task_spec().task_id());
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

TEST_F(RayletBasedActorSchedulerTest, TestNodeFailedWhenCreating) {
  auto node = Mocker::GenNodeInfo();
  auto node_id = NodeID::FromBinary(node->node_id());
  gcs_node_manager_->AddNode(node);
  ASSERT_EQ(1, gcs_node_manager_->GetAllAliveNodes().size());

  auto job_id = JobID::FromInt(1);
  auto create_actor_request = Mocker::GenCreateActorRequest(job_id);
  auto actor = std::make_shared<gcs::GcsActor>(create_actor_request.task_spec(), "");

  // Schedule the actor with 1 available node, and the lease request should be send to the
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

TEST_F(RayletBasedActorSchedulerTest, TestWorkerFailedWhenCreating) {
  auto node = Mocker::GenNodeInfo();
  auto node_id = NodeID::FromBinary(node->node_id());
  gcs_node_manager_->AddNode(node);
  ASSERT_EQ(1, gcs_node_manager_->GetAllAliveNodes().size());

  auto job_id = JobID::FromInt(1);
  auto create_actor_request = Mocker::GenCreateActorRequest(job_id);
  auto actor = std::make_shared<gcs::GcsActor>(create_actor_request.task_spec(), "");

  // Schedule the actor with 1 available node, and the lease request should be send to the
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

TEST_F(RayletBasedActorSchedulerTest, TestSpillback) {
  auto node1 = Mocker::GenNodeInfo();
  auto node_id_1 = NodeID::FromBinary(node1->node_id());
  gcs_node_manager_->AddNode(node1);
  ASSERT_EQ(1, gcs_node_manager_->GetAllAliveNodes().size());

  auto job_id = JobID::FromInt(1);
  auto create_actor_request = Mocker::GenCreateActorRequest(job_id);
  auto actor = std::make_shared<gcs::GcsActor>(create_actor_request.task_spec(), "");

  // Schedule the actor with 1 available node, and the lease request should be send to the
  // node.
  gcs_actor_scheduler_->Schedule(actor);
  ASSERT_EQ(1, raylet_client_->num_workers_requested);
  ASSERT_EQ(1, raylet_client_->callbacks.size());
  ASSERT_EQ(0, worker_client_->callbacks.size());

  // Add another node.
  auto node2 = Mocker::GenNodeInfo();
  auto node_id_2 = NodeID::FromBinary(node2->node_id());
  gcs_node_manager_->AddNode(node2);
  ASSERT_EQ(2, gcs_node_manager_->GetAllAliveNodes().size());

  // Grant with an invalid spillback node, and schedule again.
  auto invalid_node_id = NodeID::FromBinary(Mocker::GenNodeInfo()->node_id());
  ASSERT_TRUE(raylet_client_->GrantWorkerLease(
      node2->node_manager_address(), node2->node_manager_port(), WorkerID::Nil(),
      node_id_1, invalid_node_id));
  ASSERT_EQ(2, raylet_client_->num_workers_requested);
  ASSERT_EQ(1, raylet_client_->callbacks.size());
  ASSERT_EQ(0, worker_client_->callbacks.size());

  // Grant with a spillback node(node2), and the lease request should be send to the
  // node2.
  ASSERT_TRUE(raylet_client_->GrantWorkerLease(node2->node_manager_address(),
                                               node2->node_manager_port(),
                                               WorkerID::Nil(), node_id_1, node_id_2));
  ASSERT_EQ(3, raylet_client_->num_workers_requested);
  ASSERT_EQ(1, raylet_client_->callbacks.size());
  ASSERT_EQ(0, worker_client_->callbacks.size());

  // Grant a worker, then the actor creation request should be send to the worker.
  WorkerID worker_id = WorkerID::FromRandom();
  ASSERT_TRUE(raylet_client_->GrantWorkerLease(node2->node_manager_address(),
                                               node2->node_manager_port(), worker_id,
                                               node_id_2, NodeID::Nil()));
  ASSERT_EQ(0, raylet_client_->callbacks.size());
  ASSERT_EQ(1, worker_client_->callbacks.size());

  // Reply the actor creation request, then the actor should be scheduled successfully.
  ASSERT_TRUE(worker_client_->ReplyPushTask());
  ASSERT_EQ(0, worker_client_->callbacks.size());

  ASSERT_EQ(0, failure_actors_.size());
  ASSERT_EQ(1, success_actors_.size());
  ASSERT_EQ(actor, success_actors_.front());
  ASSERT_EQ(actor->GetNodeID(), node_id_2);
  ASSERT_EQ(actor->GetWorkerID(), worker_id);
}

TEST_F(RayletBasedActorSchedulerTest, TestReschedule) {
  auto node1 = Mocker::GenNodeInfo();
  auto node_id_1 = NodeID::FromBinary(node1->node_id());
  gcs_node_manager_->AddNode(node1);
  ASSERT_EQ(1, gcs_node_manager_->GetAllAliveNodes().size());

  // 1.Actor is already tied to a leased worker.
  auto job_id = JobID::FromInt(1);
  auto create_actor_request = Mocker::GenCreateActorRequest(job_id);
  auto actor = std::make_shared<gcs::GcsActor>(create_actor_request.task_spec(), "");
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

TEST_F(RayletBasedActorSchedulerTest, TestReleaseUnusedWorkers) {
  // Test the case that GCS won't send `RequestWorkerLease` request to the raylet,
  // if there is still a pending `ReleaseUnusedWorkers` request.

  // Add a node to the cluster.
  auto node = Mocker::GenNodeInfo();
  auto node_id = NodeID::FromBinary(node->node_id());
  gcs_node_manager_->AddNode(node);
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
  auto job_id = JobID::FromInt(1);
  auto request = Mocker::GenCreateActorRequest(job_id);
  auto actor = std::make_shared<gcs::GcsActor>(request.task_spec(), "");
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
