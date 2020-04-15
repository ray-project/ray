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

#include <ray/gcs/test/gcs_test_util.h>

#include <memory>
#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace ray {

class GcsActorSchedulerTest : public ::testing::Test {
 public:
  void SetUp() override {
    raylet_client_ = std::make_shared<Mocker::MockRayletClient>();
    worker_client_ = std::make_shared<Mocker::MockWorkerClient>();
    gcs_node_manager_ = std::make_shared<gcs::GcsNodeManager>(
        io_service_, node_info_accessor_, error_info_accessor_);
    gcs_actor_scheduler_ = std::make_shared<Mocker::MockedGcsActorScheduler>(
        io_service_, actor_info_accessor_, *gcs_node_manager_,
        /*schedule_failure_handler=*/
        [this](std::shared_ptr<gcs::GcsActor> actor) {
          failure_actors_.emplace_back(std::move(actor));
        },
        /*schedule_success_handler=*/
        [this](std::shared_ptr<gcs::GcsActor> actor) {
          success_actors_.emplace_back(std::move(actor));
        },
        /*lease_client_factory=*/
        [this](const rpc::Address &address) { return raylet_client_; },
        /*client_factory=*/
        [this](const rpc::Address &address) { return worker_client_; });
  }

 protected:
  boost::asio::io_service io_service_;
  Mocker::MockedActorInfoAccessor actor_info_accessor_;
  Mocker::MockedNodeInfoAccessor node_info_accessor_;
  Mocker::MockedErrorInfoAccessor error_info_accessor_;

  std::shared_ptr<Mocker::MockRayletClient> raylet_client_;
  std::shared_ptr<Mocker::MockWorkerClient> worker_client_;
  std::shared_ptr<gcs::GcsNodeManager> gcs_node_manager_;
  std::shared_ptr<Mocker::MockedGcsActorScheduler> gcs_actor_scheduler_;
  std::vector<std::shared_ptr<gcs::GcsActor>> success_actors_;
  std::vector<std::shared_ptr<gcs::GcsActor>> failure_actors_;
};

TEST_F(GcsActorSchedulerTest, TestScheduleFailedWithZeroNode) {
  ASSERT_EQ(0, gcs_node_manager_->GetAllAliveNodes().size());

  auto job_id = JobID::FromInt(1);
  auto create_actor_request = Mocker::GenCreateActorRequest(job_id);
  auto actor = std::make_shared<gcs::GcsActor>(create_actor_request);

  // Schedule the actor with zero node.
  gcs_actor_scheduler_->Schedule(actor);

  // The lease request should not be send and the scheduling of actor should fail as there
  // are no available nodes.
  ASSERT_EQ(raylet_client_->num_workers_requested, 0);
  ASSERT_EQ(0, success_actors_.size());
  ASSERT_EQ(1, failure_actors_.size());
  ASSERT_EQ(actor, failure_actors_.front());
}

TEST_F(GcsActorSchedulerTest, TestScheduleActorSuccess) {
  auto node = Mocker::GenNodeInfo();
  auto node_id = ClientID::FromBinary(node->node_id());
  gcs_node_manager_->AddNode(node);
  ASSERT_EQ(1, gcs_node_manager_->GetAllAliveNodes().size());

  auto job_id = JobID::FromInt(1);
  auto create_actor_request = Mocker::GenCreateActorRequest(job_id);
  auto actor = std::make_shared<gcs::GcsActor>(create_actor_request);

  // Schedule the actor with 1 available node, and the lease request should be send to the
  // node.
  gcs_actor_scheduler_->Schedule(actor);
  ASSERT_EQ(1, raylet_client_->num_workers_requested);
  ASSERT_EQ(1, raylet_client_->callbacks.size());
  ASSERT_EQ(0, worker_client_->callbacks.size());

  // Grant a worker, then the actor creation request should be send to the worker.
  ASSERT_TRUE(raylet_client_->GrantWorkerLease(
      node->node_manager_address(), node->node_manager_port(), WorkerID::FromRandom(),
      node_id, ClientID::Nil()));
  ASSERT_EQ(0, raylet_client_->callbacks.size());
  ASSERT_EQ(1, worker_client_->callbacks.size());

  // Reply the actor creation request, then the actor should be scheduled successfully.
  ASSERT_TRUE(worker_client_->ReplyPushTask());
  ASSERT_EQ(0, worker_client_->callbacks.size());
  ASSERT_EQ(0, failure_actors_.size());
  ASSERT_EQ(1, success_actors_.size());
  ASSERT_EQ(actor, success_actors_.front());
}

TEST_F(GcsActorSchedulerTest, TestScheduleRetryWhenLeasing) {
  auto node = Mocker::GenNodeInfo();
  auto node_id = ClientID::FromBinary(node->node_id());
  gcs_node_manager_->AddNode(node);
  ASSERT_EQ(1, gcs_node_manager_->GetAllAliveNodes().size());

  auto job_id = JobID::FromInt(1);
  auto create_actor_request = Mocker::GenCreateActorRequest(job_id);
  auto actor = std::make_shared<gcs::GcsActor>(create_actor_request);

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
      node_id, ClientID::Nil(), Status::IOError("")));
  ASSERT_EQ(1, gcs_actor_scheduler_->num_retry_leasing_count_);
  ASSERT_EQ(2, raylet_client_->num_workers_requested);
  ASSERT_EQ(1, raylet_client_->callbacks.size());
  ASSERT_EQ(0, worker_client_->callbacks.size());

  // Grant a worker, then the actor creation request should be send to the worker.
  ASSERT_TRUE(raylet_client_->GrantWorkerLease(
      node->node_manager_address(), node->node_manager_port(), WorkerID::FromRandom(),
      node_id, ClientID::Nil()));
  ASSERT_EQ(0, raylet_client_->callbacks.size());
  ASSERT_EQ(1, worker_client_->callbacks.size());

  // Reply the actor creation request, then the actor should be scheduled successfully.
  ASSERT_TRUE(worker_client_->ReplyPushTask());
  ASSERT_EQ(0, worker_client_->callbacks.size());
  ASSERT_EQ(0, failure_actors_.size());
  ASSERT_EQ(1, success_actors_.size());
  ASSERT_EQ(actor, success_actors_.front());
}

TEST_F(GcsActorSchedulerTest, TestScheduleRetryWhenCreating) {
  auto node = Mocker::GenNodeInfo();
  auto node_id = ClientID::FromBinary(node->node_id());
  gcs_node_manager_->AddNode(node);
  ASSERT_EQ(1, gcs_node_manager_->GetAllAliveNodes().size());

  auto job_id = JobID::FromInt(1);
  auto create_actor_request = Mocker::GenCreateActorRequest(job_id);
  auto actor = std::make_shared<gcs::GcsActor>(create_actor_request);

  // Schedule the actor with 1 available node, and the lease request should be send to the
  // node.
  gcs_actor_scheduler_->Schedule(actor);
  ASSERT_EQ(1, raylet_client_->num_workers_requested);
  ASSERT_EQ(1, raylet_client_->callbacks.size());
  ASSERT_EQ(0, worker_client_->callbacks.size());

  // Grant a worker, then the actor creation request should be send to the worker.
  ASSERT_TRUE(raylet_client_->GrantWorkerLease(
      node->node_manager_address(), node->node_manager_port(), WorkerID::FromRandom(),
      node_id, ClientID::Nil()));
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
}

TEST_F(GcsActorSchedulerTest, TestNodeFailedWhenLeasing) {
  auto node = Mocker::GenNodeInfo();
  auto node_id = ClientID::FromBinary(node->node_id());
  gcs_node_manager_->AddNode(node);
  ASSERT_EQ(1, gcs_node_manager_->GetAllAliveNodes().size());

  auto job_id = JobID::FromInt(1);
  auto create_actor_request = Mocker::GenCreateActorRequest(job_id);
  auto actor = std::make_shared<gcs::GcsActor>(create_actor_request);

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
      node_id, ClientID::Nil()));
  ASSERT_EQ(1, raylet_client_->num_workers_requested);
  ASSERT_EQ(0, raylet_client_->callbacks.size());
  ASSERT_EQ(0, gcs_actor_scheduler_->num_retry_leasing_count_);

  ASSERT_EQ(0, success_actors_.size());
  ASSERT_EQ(0, failure_actors_.size());
}

TEST_F(GcsActorSchedulerTest, TestNodeFailedWhenCreating) {
  auto node = Mocker::GenNodeInfo();
  auto node_id = ClientID::FromBinary(node->node_id());
  gcs_node_manager_->AddNode(node);
  ASSERT_EQ(1, gcs_node_manager_->GetAllAliveNodes().size());

  auto job_id = JobID::FromInt(1);
  auto create_actor_request = Mocker::GenCreateActorRequest(job_id);
  auto actor = std::make_shared<gcs::GcsActor>(create_actor_request);

  // Schedule the actor with 1 available node, and the lease request should be send to the
  // node.
  gcs_actor_scheduler_->Schedule(actor);
  ASSERT_EQ(1, raylet_client_->num_workers_requested);
  ASSERT_EQ(1, raylet_client_->callbacks.size());
  ASSERT_EQ(0, worker_client_->callbacks.size());

  // Grant a worker, then the actor creation request should be send to the worker.
  ASSERT_TRUE(raylet_client_->GrantWorkerLease(
      node->node_manager_address(), node->node_manager_port(), WorkerID::FromRandom(),
      node_id, ClientID::Nil()));
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

TEST_F(GcsActorSchedulerTest, TestWorkerFailedWhenCreating) {
  auto node = Mocker::GenNodeInfo();
  auto node_id = ClientID::FromBinary(node->node_id());
  gcs_node_manager_->AddNode(node);
  ASSERT_EQ(1, gcs_node_manager_->GetAllAliveNodes().size());

  auto job_id = JobID::FromInt(1);
  auto create_actor_request = Mocker::GenCreateActorRequest(job_id);
  auto actor = std::make_shared<gcs::GcsActor>(create_actor_request);

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
                                               node_id, ClientID::Nil()));
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

TEST_F(GcsActorSchedulerTest, TestSpillback) {
  auto node1 = Mocker::GenNodeInfo();
  auto node_id_1 = ClientID::FromBinary(node1->node_id());
  gcs_node_manager_->AddNode(node1);
  ASSERT_EQ(1, gcs_node_manager_->GetAllAliveNodes().size());

  auto job_id = JobID::FromInt(1);
  auto create_actor_request = Mocker::GenCreateActorRequest(job_id);
  auto actor = std::make_shared<gcs::GcsActor>(create_actor_request);

  // Schedule the actor with 1 available node, and the lease request should be send to the
  // node.
  gcs_actor_scheduler_->Schedule(actor);
  ASSERT_EQ(1, raylet_client_->num_workers_requested);
  ASSERT_EQ(1, raylet_client_->callbacks.size());
  ASSERT_EQ(0, worker_client_->callbacks.size());

  // Add another node.
  auto node2 = Mocker::GenNodeInfo();
  auto node_id_2 = ClientID::FromBinary(node2->node_id());
  gcs_node_manager_->AddNode(node2);
  ASSERT_EQ(2, gcs_node_manager_->GetAllAliveNodes().size());

  // Grant with a spillback node(node2), and the lease request should be send to the
  // node2.
  ASSERT_TRUE(raylet_client_->GrantWorkerLease(node2->node_manager_address(),
                                               node2->node_manager_port(),
                                               WorkerID::Nil(), node_id_1, node_id_2));
  ASSERT_EQ(2, raylet_client_->num_workers_requested);
  ASSERT_EQ(1, raylet_client_->callbacks.size());
  ASSERT_EQ(0, worker_client_->callbacks.size());

  // Grant a worker, then the actor creation request should be send to the worker.
  ASSERT_TRUE(raylet_client_->GrantWorkerLease(
      node2->node_manager_address(), node2->node_manager_port(), WorkerID::FromRandom(),
      node_id_2, ClientID::Nil()));
  ASSERT_EQ(0, raylet_client_->callbacks.size());
  ASSERT_EQ(1, worker_client_->callbacks.size());

  // Reply the actor creation request, then the actor should be scheduled successfully.
  ASSERT_TRUE(worker_client_->ReplyPushTask());
  ASSERT_EQ(0, worker_client_->callbacks.size());

  ASSERT_EQ(node_id_2, actor->GetNodeID());

  ASSERT_EQ(0, failure_actors_.size());
  ASSERT_EQ(1, success_actors_.size());
  ASSERT_EQ(actor, success_actors_.front());
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}