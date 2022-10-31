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

// clang-format off
#include "gtest/gtest.h"
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/gcs/gcs_server/gcs_actor_scheduler.h"
#include "ray/gcs/gcs_server/test/gcs_server_test_util.h"
#include "ray/gcs/test/gcs_test_util.h"
#include "mock/ray/pubsub/publisher.h"
// clang-format on

namespace ray {
using raylet::NoopLocalTaskManager;
namespace gcs {

class GcsActorSchedulerTest : public ::testing::Test {
 public:
  void SetUp() override {
    raylet_client_ = std::make_shared<GcsServerMocker::MockRayletClient>();
    raylet_client_pool_ = std::make_shared<rpc::NodeManagerClientPool>(
        [this](const rpc::Address &addr) { return raylet_client_; });
    worker_client_ = std::make_shared<GcsServerMocker::MockWorkerClient>();
    gcs_publisher_ = std::make_shared<gcs::GcsPublisher>(
        std::make_unique<ray::pubsub::MockPublisher>());
    store_client_ = std::make_shared<gcs::InMemoryStoreClient>(io_service_);
    gcs_table_storage_ = std::make_shared<gcs::InMemoryGcsTableStorage>(io_service_);
    gcs_node_manager_ = std::make_shared<gcs::GcsNodeManager>(
        gcs_publisher_, gcs_table_storage_, raylet_client_pool_);
    gcs_actor_table_ =
        std::make_shared<GcsServerMocker::MockedGcsActorTable>(store_client_);
    local_node_id_ = NodeID::FromRandom();
    auto cluster_resource_scheduler = std::make_shared<ClusterResourceScheduler>(
        scheduling::NodeID(local_node_id_.Binary()),
        NodeResources(),
        /*is_node_available_fn=*/
        [](auto) { return true; },
        /*is_local_node_with_raylet=*/false);
    counter.reset(
        new CounterMap<std::pair<rpc::ActorTableData::ActorState, std::string>>());
    cluster_task_manager_ = std::make_shared<ClusterTaskManager>(
        local_node_id_,
        cluster_resource_scheduler,
        /*get_node_info=*/
        [this](const NodeID &node_id) {
          auto node = gcs_node_manager_->GetAliveNode(node_id);
          return node.has_value() ? node.value().get() : nullptr;
        },
        /*announce_infeasible_task=*/
        nullptr,
        /*local_task_manager=*/
        std::make_shared<NoopLocalTaskManager>());
    auto gcs_resource_manager = std::make_shared<gcs::GcsResourceManager>(
        io_service_,
        cluster_resource_scheduler->GetClusterResourceManager(),
        local_node_id_);
    gcs_actor_scheduler_ = std::make_shared<GcsServerMocker::MockedGcsActorScheduler>(
        io_service_,
        *gcs_actor_table_,
        *gcs_node_manager_,
        cluster_task_manager_,
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
        raylet_client_pool_,
        /*client_factory=*/
        [this](const rpc::Address &address) { return worker_client_; },
        /*normal_task_resources_changed_callback=*/
        [gcs_resource_manager](const NodeID &node_id,
                               const rpc::ResourcesData &resources) {
          gcs_resource_manager->UpdateNodeNormalTaskResources(node_id, resources);
        });

    gcs_node_manager_->AddNodeAddedListener(
        [cluster_resource_scheduler](std::shared_ptr<rpc::GcsNodeInfo> node) {
          scheduling::NodeID node_id(node->node_id());
          auto &cluster_resource_manager =
              cluster_resource_scheduler->GetClusterResourceManager();
          auto resource_map = MapFromProtobuf(node->resources_total());
          auto node_resources = ResourceMapToNodeResources(resource_map, resource_map);
          cluster_resource_manager.AddOrUpdateNode(node_id, node_resources);
        });
  }

  std::shared_ptr<gcs::GcsActor> NewGcsActor(
      const std::unordered_map<std::string, double> &required_placement_resources) {
    rpc::Address owner_address;
    owner_address.set_raylet_id(NodeID::FromRandom().Binary());
    owner_address.set_ip_address("127.0.0.1");
    owner_address.set_port(5678);
    owner_address.set_worker_id(WorkerID::FromRandom().Binary());
    auto job_id = JobID::FromInt(1);

    std::unordered_map<std::string, double> required_resources;

    required_resources.insert(required_placement_resources.begin(),
                              required_placement_resources.end());
    auto actor_creating_task_spec =
        Mocker::GenActorCreationTask(job_id,
                                     /*max_restarts=*/1,
                                     /*detached=*/true,
                                     /*name=*/"",
                                     "",
                                     owner_address,
                                     required_resources,
                                     required_placement_resources);
    return std::make_shared<gcs::GcsActor>(actor_creating_task_spec.GetMessage(),
                                           /*ray_namespace=*/"",
                                           counter);
  }

  std::shared_ptr<rpc::GcsNodeInfo> AddNewNode(
      std::unordered_map<std::string, double> node_resources) {
    auto node_info = Mocker::GenNodeInfo();
    node_info->mutable_resources_total()->insert(node_resources.begin(),
                                                 node_resources.end());
    gcs_node_manager_->AddNode(node_info);
    return node_info;
  }

 protected:
  instrumented_io_context io_service_;
  std::shared_ptr<gcs::StoreClient> store_client_;
  std::shared_ptr<GcsServerMocker::MockedGcsActorTable> gcs_actor_table_;
  std::shared_ptr<GcsServerMocker::MockRayletClient> raylet_client_;
  std::shared_ptr<GcsServerMocker::MockWorkerClient> worker_client_;
  std::shared_ptr<gcs::GcsNodeManager> gcs_node_manager_;
  std::shared_ptr<ClusterTaskManager> cluster_task_manager_;
  std::shared_ptr<GcsServerMocker::MockedGcsActorScheduler> gcs_actor_scheduler_;
  std::shared_ptr<CounterMap<std::pair<rpc::ActorTableData::ActorState, std::string>>>
      counter;
  std::vector<std::shared_ptr<gcs::GcsActor>> failure_actors_;
  std::vector<std::shared_ptr<gcs::GcsActor>> success_actors_;
  std::shared_ptr<gcs::GcsPublisher> gcs_publisher_;
  std::shared_ptr<gcs::GcsTableStorage> gcs_table_storage_;
  std::shared_ptr<rpc::NodeManagerClientPool> raylet_client_pool_;
  NodeID local_node_id_;
};

/**************************************************************/
/************* TESTS WITH RAYLET SCHEDULING BELOW *************/
/**************************************************************/

TEST_F(GcsActorSchedulerTest, TestScheduleFailedWithZeroNode) {
  ASSERT_EQ(0, gcs_node_manager_->GetAllAliveNodes().size());

  auto job_id = JobID::FromInt(1);
  auto create_actor_request = Mocker::GenCreateActorRequest(job_id);
  auto actor =
      std::make_shared<gcs::GcsActor>(create_actor_request.task_spec(), "", counter);

  // Schedule the actor with zero node.
  gcs_actor_scheduler_->ScheduleByRaylet(actor);

  // The lease request should not be send and the scheduling of actor should fail as there
  // are no available nodes.
  ASSERT_EQ(raylet_client_->num_workers_requested, 0);
  ASSERT_EQ(0, success_actors_.size());
  ASSERT_EQ(1, failure_actors_.size());
  ASSERT_TRUE(actor->GetNodeID().IsNil());
}

TEST_F(GcsActorSchedulerTest, TestScheduleActorSuccess) {
  auto node = Mocker::GenNodeInfo();
  auto node_id = NodeID::FromBinary(node->node_id());
  gcs_node_manager_->AddNode(node);
  ASSERT_EQ(1, gcs_node_manager_->GetAllAliveNodes().size());

  auto job_id = JobID::FromInt(1);
  auto create_actor_request = Mocker::GenCreateActorRequest(job_id);
  auto actor =
      std::make_shared<gcs::GcsActor>(create_actor_request.task_spec(), "", counter);

  // Schedule the actor with 1 available node, and the lease request should be send to the
  // node.
  gcs_actor_scheduler_->ScheduleByRaylet(actor);
  ASSERT_EQ(1, raylet_client_->num_workers_requested);
  ASSERT_EQ(1, raylet_client_->callbacks.size());
  ASSERT_EQ(0, worker_client_->callbacks.size());

  // Grant a worker, then the actor creation request should be send to the worker.
  WorkerID worker_id = WorkerID::FromRandom();
  ASSERT_TRUE(raylet_client_->GrantWorkerLease(node->node_manager_address(),
                                               node->node_manager_port(),
                                               worker_id,
                                               node_id,
                                               NodeID::Nil()));
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

TEST_F(GcsActorSchedulerTest, TestScheduleRetryWhenLeasing) {
  auto node = Mocker::GenNodeInfo();
  auto node_id = NodeID::FromBinary(node->node_id());
  gcs_node_manager_->AddNode(node);
  ASSERT_EQ(1, gcs_node_manager_->GetAllAliveNodes().size());

  auto job_id = JobID::FromInt(1);
  auto create_actor_request = Mocker::GenCreateActorRequest(job_id);
  auto actor =
      std::make_shared<gcs::GcsActor>(create_actor_request.task_spec(), "", counter);

  // Schedule the actor with 1 available node, and the lease request should be send to the
  // node.
  gcs_actor_scheduler_->ScheduleByRaylet(actor);
  ASSERT_EQ(1, raylet_client_->num_workers_requested);
  ASSERT_EQ(1, raylet_client_->callbacks.size());
  ASSERT_EQ(0, worker_client_->callbacks.size());
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
  ASSERT_EQ(0, worker_client_->callbacks.size());

  // Grant a worker, then the actor creation request should be send to the worker.
  WorkerID worker_id = WorkerID::FromRandom();
  ASSERT_TRUE(raylet_client_->GrantWorkerLease(node->node_manager_address(),
                                               node->node_manager_port(),
                                               worker_id,
                                               node_id,
                                               NodeID::Nil()));
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

TEST_F(GcsActorSchedulerTest, TestScheduleRetryWhenCreating) {
  auto node = Mocker::GenNodeInfo();
  auto node_id = NodeID::FromBinary(node->node_id());
  gcs_node_manager_->AddNode(node);
  ASSERT_EQ(1, gcs_node_manager_->GetAllAliveNodes().size());

  auto job_id = JobID::FromInt(1);
  auto create_actor_request = Mocker::GenCreateActorRequest(job_id);
  auto actor =
      std::make_shared<gcs::GcsActor>(create_actor_request.task_spec(), "", counter);

  // Schedule the actor with 1 available node, and the lease request should be send to the
  // node.
  gcs_actor_scheduler_->ScheduleByRaylet(actor);
  ASSERT_EQ(1, raylet_client_->num_workers_requested);
  ASSERT_EQ(1, raylet_client_->callbacks.size());
  ASSERT_EQ(0, worker_client_->callbacks.size());

  // Grant a worker, then the actor creation request should be send to the worker.
  WorkerID worker_id = WorkerID::FromRandom();
  ASSERT_TRUE(raylet_client_->GrantWorkerLease(node->node_manager_address(),
                                               node->node_manager_port(),
                                               worker_id,
                                               node_id,
                                               NodeID::Nil()));
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

TEST_F(GcsActorSchedulerTest, TestNodeFailedWhenLeasing) {
  auto node = Mocker::GenNodeInfo();
  auto node_id = NodeID::FromBinary(node->node_id());
  gcs_node_manager_->AddNode(node);
  ASSERT_EQ(1, gcs_node_manager_->GetAllAliveNodes().size());

  auto job_id = JobID::FromInt(1);
  auto create_actor_request = Mocker::GenCreateActorRequest(job_id);
  auto actor =
      std::make_shared<gcs::GcsActor>(create_actor_request.task_spec(), "", counter);

  // Schedule the actor with 1 available node, and the lease request should be send to the
  // node.
  gcs_actor_scheduler_->ScheduleByRaylet(actor);
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
  auto node = Mocker::GenNodeInfo();
  auto node_id = NodeID::FromBinary(node->node_id());
  gcs_node_manager_->AddNode(node);
  ASSERT_EQ(1, gcs_node_manager_->GetAllAliveNodes().size());

  auto job_id = JobID::FromInt(1);
  auto create_actor_request = Mocker::GenCreateActorRequest(job_id);
  auto actor =
      std::make_shared<gcs::GcsActor>(create_actor_request.task_spec(), "", counter);

  // Schedule the actor with 1 available node, and the lease request should be send to the
  // node.
  gcs_actor_scheduler_->ScheduleByRaylet(actor);
  ASSERT_EQ(1, raylet_client_->num_workers_requested);
  ASSERT_EQ(1, raylet_client_->callbacks.size());

  // Cancel the lease request.
  const auto &task_id = TaskID::FromBinary(create_actor_request.task_spec().task_id());
  gcs_actor_scheduler_->CancelOnLeasing(node_id, actor->GetActorID(), task_id);
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
  auto node = Mocker::GenNodeInfo();
  auto node_id = NodeID::FromBinary(node->node_id());
  gcs_node_manager_->AddNode(node);
  ASSERT_EQ(1, gcs_node_manager_->GetAllAliveNodes().size());

  auto job_id = JobID::FromInt(1);
  auto create_actor_request = Mocker::GenCreateActorRequest(job_id);
  auto actor =
      std::make_shared<gcs::GcsActor>(create_actor_request.task_spec(), "", counter);

  // Schedule the actor with 1 available node, and the lease request should be send to the
  // node.
  gcs_actor_scheduler_->ScheduleByRaylet(actor);
  ASSERT_EQ(1, raylet_client_->num_workers_requested);
  ASSERT_EQ(1, raylet_client_->callbacks.size());
  ASSERT_EQ(0, worker_client_->callbacks.size());

  // Grant a worker, then the actor creation request should be send to the worker.
  ASSERT_TRUE(raylet_client_->GrantWorkerLease(node->node_manager_address(),
                                               node->node_manager_port(),
                                               WorkerID::FromRandom(),
                                               node_id,
                                               NodeID::Nil()));
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
  auto node_id = NodeID::FromBinary(node->node_id());
  gcs_node_manager_->AddNode(node);
  ASSERT_EQ(1, gcs_node_manager_->GetAllAliveNodes().size());

  auto job_id = JobID::FromInt(1);
  auto create_actor_request = Mocker::GenCreateActorRequest(job_id);
  auto actor =
      std::make_shared<gcs::GcsActor>(create_actor_request.task_spec(), "", counter);

  // Schedule the actor with 1 available node, and the lease request should be send to the
  // node.
  gcs_actor_scheduler_->ScheduleByRaylet(actor);
  ASSERT_EQ(1, raylet_client_->num_workers_requested);
  ASSERT_EQ(1, raylet_client_->callbacks.size());
  ASSERT_EQ(0, worker_client_->callbacks.size());

  // Grant a worker, then the actor creation request should be send to the worker.
  auto worker_id = WorkerID::FromRandom();
  ASSERT_TRUE(raylet_client_->GrantWorkerLease(node->node_manager_address(),
                                               node->node_manager_port(),
                                               worker_id,
                                               node_id,
                                               NodeID::Nil()));
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
  auto node_id_1 = NodeID::FromBinary(node1->node_id());
  gcs_node_manager_->AddNode(node1);
  ASSERT_EQ(1, gcs_node_manager_->GetAllAliveNodes().size());

  auto job_id = JobID::FromInt(1);
  auto create_actor_request = Mocker::GenCreateActorRequest(job_id);
  auto actor =
      std::make_shared<gcs::GcsActor>(create_actor_request.task_spec(), "", counter);

  // Schedule the actor with 1 available node, and the lease request should be send to the
  // node.
  gcs_actor_scheduler_->ScheduleByRaylet(actor);
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
  ASSERT_TRUE(raylet_client_->GrantWorkerLease(node2->node_manager_address(),
                                               node2->node_manager_port(),
                                               WorkerID::Nil(),
                                               node_id_1,
                                               invalid_node_id));
  ASSERT_EQ(2, raylet_client_->num_workers_requested);
  ASSERT_EQ(1, raylet_client_->callbacks.size());
  ASSERT_EQ(0, worker_client_->callbacks.size());

  // Grant with a spillback node(node2), and the lease request should be send to the
  // node2.
  ASSERT_TRUE(raylet_client_->GrantWorkerLease(node2->node_manager_address(),
                                               node2->node_manager_port(),
                                               WorkerID::Nil(),
                                               node_id_1,
                                               node_id_2));
  ASSERT_EQ(3, raylet_client_->num_workers_requested);
  ASSERT_EQ(1, raylet_client_->callbacks.size());
  ASSERT_EQ(0, worker_client_->callbacks.size());

  // Grant a worker, then the actor creation request should be send to the worker.
  WorkerID worker_id = WorkerID::FromRandom();
  ASSERT_TRUE(raylet_client_->GrantWorkerLease(node2->node_manager_address(),
                                               node2->node_manager_port(),
                                               worker_id,
                                               node_id_2,
                                               NodeID::Nil()));
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

TEST_F(GcsActorSchedulerTest, TestReschedule) {
  auto node1 = Mocker::GenNodeInfo();
  auto node_id_1 = NodeID::FromBinary(node1->node_id());
  gcs_node_manager_->AddNode(node1);
  ASSERT_EQ(1, gcs_node_manager_->GetAllAliveNodes().size());

  // 1.Actor is already tied to a leased worker.
  auto job_id = JobID::FromInt(1);
  auto create_actor_request = Mocker::GenCreateActorRequest(job_id);
  auto actor =
      std::make_shared<gcs::GcsActor>(create_actor_request.task_spec(), "", counter);
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
                                               node1->node_manager_port(),
                                               worker_id,
                                               node_id_1,
                                               NodeID::Nil()));
  ASSERT_EQ(0, raylet_client_->callbacks.size());
  ASSERT_EQ(1, worker_client_->callbacks.size());

  // Reply the actor creation request, then the actor should be scheduled successfully.
  ASSERT_TRUE(worker_client_->ReplyPushTask());
  ASSERT_EQ(0, worker_client_->callbacks.size());

  ASSERT_EQ(0, failure_actors_.size());
  ASSERT_EQ(2, success_actors_.size());
}

TEST_F(GcsActorSchedulerTest, TestReleaseUnusedWorkers) {
  // Test the case that GCS won't send `RequestWorkerLease` request to the raylet,
  // if there is still a pending `ReleaseUnusedWorkers` request.

  // Add a node to the cluster.
  auto node = Mocker::GenNodeInfo();
  auto node_id = NodeID::FromBinary(node->node_id());
  gcs_node_manager_->AddNode(node);
  ASSERT_EQ(1, gcs_node_manager_->GetAllAliveNodes().size());

  // Send a `ReleaseUnusedWorkers` request to the node.
  absl::flat_hash_map<NodeID, std::vector<WorkerID>> node_to_workers;
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
  auto actor = std::make_shared<gcs::GcsActor>(request.task_spec(), "", counter);
  gcs_actor_scheduler_->ScheduleByRaylet(actor);
  ASSERT_EQ(2, gcs_actor_scheduler_->num_retry_leasing_count_);
  ASSERT_EQ(raylet_client_->num_workers_requested, 0);

  // When `GcsActorScheduler` receives the `ReleaseUnusedWorkers` reply, it will send
  // out the `RequestWorkerLease` request.
  ASSERT_TRUE(raylet_client_->ReplyReleaseUnusedWorkers());
  gcs_actor_scheduler_->TryLeaseWorkerFromNodeAgain(actor, node);
  ASSERT_EQ(raylet_client_->num_workers_requested, 1);
}

/***********************************************************/
/************* TESTS WITH GCS SCHEDULING BELOW *************/
/***********************************************************/

TEST_F(GcsActorSchedulerTest, TestScheduleFailedWithZeroNodeByGcs) {
  // This feature flag is turned on for all of the following tests.
  RayConfig::instance().initialize(R"({"gcs_actor_scheduling_enabled": true})");

  ASSERT_EQ(0, gcs_node_manager_->GetAllAliveNodes().size());

  std::unordered_map<std::string, double> required_placement_resources = {
      {kMemory_ResourceLabel, 32}};
  auto actor = NewGcsActor(required_placement_resources);

  // Schedule with zero node.
  gcs_actor_scheduler_->ScheduleByGcs(actor);

  // The lease request should not be send and the scheduling of actor should fail as there
  // are no available nodes.
  ASSERT_EQ(raylet_client_->num_workers_requested, 0);
  ASSERT_EQ(0, success_actors_.size());
  ASSERT_EQ(1, cluster_task_manager_->GetInfeasibleQueueSize());
  ASSERT_TRUE(actor->GetNodeID().IsNil());
}

TEST_F(GcsActorSchedulerTest, TestNotEnoughClusterResources) {
  // Add a node with 64 memory units and 8 CPU.
  std::unordered_map<std::string, double> node_resources = {{kMemory_ResourceLabel, 64},
                                                            {kCPU_ResourceLabel, 8}};
  AddNewNode(node_resources);
  ASSERT_EQ(1, gcs_node_manager_->GetAllAliveNodes().size());

  // Schedule a actor (requiring 128 memory units and 4 CPU).
  std::unordered_map<std::string, double> required_placement_resources = {
      {kMemory_ResourceLabel, 128}, {kCPU_ResourceLabel, 4}};
  auto actor = NewGcsActor(required_placement_resources);

  gcs_actor_scheduler_->ScheduleByGcs(actor);

  // The lease request should not be sent and the scheduling of actor should fail as there
  // are not enough cluster resources.
  ASSERT_EQ(raylet_client_->num_workers_requested, 0);
  ASSERT_EQ(0, success_actors_.size());
  ASSERT_EQ(1, cluster_task_manager_->GetInfeasibleQueueSize());
  ASSERT_TRUE(actor->GetNodeID().IsNil());
}

TEST_F(GcsActorSchedulerTest, TestScheduleAndDestroyOneActor) {
  // Add a node with 64 memory units and 8 CPU.
  std::unordered_map<std::string, double> node_resources = {{kMemory_ResourceLabel, 64},
                                                            {kCPU_ResourceLabel, 8}};
  auto node = AddNewNode(node_resources);
  auto node_id = NodeID::FromBinary(node->node_id());
  scheduling::NodeID scheduling_node_id(node->node_id());
  ASSERT_EQ(1, gcs_node_manager_->GetAllAliveNodes().size());
  const auto &cluster_resource_manager =
      cluster_task_manager_->GetClusterResourceScheduler()->GetClusterResourceManager();
  auto resource_view_before_scheduling = cluster_resource_manager.GetResourceView();
  ASSERT_TRUE(resource_view_before_scheduling.contains(scheduling_node_id));

  // Schedule a actor (requiring 32 memory units and 4 CPU).
  std::unordered_map<std::string, double> required_placement_resources = {
      {kMemory_ResourceLabel, 32}, {kCPU_ResourceLabel, 4}};
  auto actor = NewGcsActor(required_placement_resources);

  gcs_actor_scheduler_->ScheduleByGcs(actor);

  ASSERT_EQ(1, raylet_client_->num_workers_requested);
  ASSERT_EQ(1, raylet_client_->callbacks.size());
  ASSERT_EQ(0, worker_client_->callbacks.size());

  // Grant a worker, then the actor creation request should be sent to the worker.
  WorkerID worker_id = WorkerID::FromRandom();
  ASSERT_TRUE(raylet_client_->GrantWorkerLease(node->node_manager_address(),
                                               node->node_manager_port(),
                                               worker_id,
                                               node_id,
                                               NodeID::Nil()));
  ASSERT_EQ(0, raylet_client_->callbacks.size());
  ASSERT_EQ(1, worker_client_->callbacks.size());

  // Reply the actor creation request, then the actor should be scheduled successfully.
  ASSERT_TRUE(worker_client_->ReplyPushTask());
  ASSERT_EQ(0, worker_client_->callbacks.size());
  ASSERT_EQ(0, cluster_task_manager_->GetInfeasibleQueueSize());
  ASSERT_EQ(0, cluster_task_manager_->GetPendingQueueSize());
  ASSERT_EQ(1, success_actors_.size());
  ASSERT_EQ(actor, success_actors_.front());
  ASSERT_EQ(actor->GetNodeID(), node_id);
  ASSERT_EQ(actor->GetWorkerID(), worker_id);

  auto resource_view_after_scheduling = cluster_resource_manager.GetResourceView();
  ASSERT_TRUE(resource_view_after_scheduling.contains(scheduling_node_id));
  ASSERT_NE(resource_view_before_scheduling.at(scheduling_node_id).GetLocalView(),
            resource_view_after_scheduling.at(scheduling_node_id).GetLocalView());

  // When destroying an actor, its acquired resources have to be returned.
  gcs_actor_scheduler_->OnActorDestruction(actor);
  auto resource_view_after_destruction = cluster_resource_manager.GetResourceView();
  ASSERT_TRUE(resource_view_after_destruction.contains(scheduling_node_id));
  ASSERT_TRUE(resource_view_after_destruction.at(scheduling_node_id).GetLocalView() ==
              resource_view_before_scheduling.at(scheduling_node_id).GetLocalView());
}

TEST_F(GcsActorSchedulerTest, TestBalancedSchedule) {
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

    gcs_actor_scheduler_->ScheduleByGcs(actor);

    ASSERT_FALSE(actor->GetNodeID().IsNil());
    sched_counts[actor->GetNodeID()]++;
  }

  // Make sure the 10 actors are balanced.
  for (const auto &entry : sched_counts) {
    ASSERT_EQ(5, entry.second);
  }
}

TEST_F(GcsActorSchedulerTest, TestRejectedRequestWorkerLeaseReply) {
  // Add two nodes, each with 32 memory units and 4 CPU.
  std::unordered_map<std::string, double> node_resources = {{kMemory_ResourceLabel, 32},
                                                            {kCPU_ResourceLabel, 4}};
  auto node1 = AddNewNode(node_resources);
  auto node2 = AddNewNode(node_resources);
  ASSERT_EQ(2, gcs_node_manager_->GetAllAliveNodes().size());

  // In the hybrid_policy, nodes are sorted in increasing order of scheduling::NodeID. So
  // we have to figure out which node is the first one in the sorted order.
  auto first_node =
      scheduling::NodeID(node1->node_id()) < scheduling::NodeID(node2->node_id()) ? node1
                                                                                  : node2;

  // Schedule a actor (requiring 32 memory units and 4 CPU).
  std::unordered_map<std::string, double> required_placement_resources = {
      {kMemory_ResourceLabel, 32}, {kCPU_ResourceLabel, 4}};
  auto actor = NewGcsActor(required_placement_resources);

  // Schedule the actor, and the lease request should be sent to the first node.
  gcs_actor_scheduler_->ScheduleByGcs(actor);
  ASSERT_EQ(NodeID::FromBinary(first_node->node_id()), actor->GetNodeID());
  ASSERT_EQ(1, raylet_client_->num_workers_requested);
  ASSERT_EQ(1, raylet_client_->callbacks.size());
  ASSERT_EQ(0, worker_client_->callbacks.size());

  // Mock a rejected reply, then the actor will be rescheduled.
  ASSERT_TRUE(raylet_client_->GrantWorkerLease(first_node->node_manager_address(),
                                               first_node->node_manager_port(),
                                               WorkerID::FromRandom(),
                                               NodeID::FromBinary(first_node->node_id()),
                                               NodeID::Nil(),
                                               Status::OK(),
                                               /*rejected=*/true));
  ASSERT_EQ(2, raylet_client_->num_workers_requested);
  ASSERT_EQ(1, raylet_client_->callbacks.size());
  ASSERT_EQ(0, worker_client_->callbacks.size());

  // The first node's resources have been preempted. The actor is rescheduled to the
  // second one.
  ASSERT_NE(NodeID::FromBinary(first_node->node_id()), actor->GetNodeID());
}

TEST_F(GcsActorSchedulerTest, TestScheduleRetryWhenLeasingByGcs) {
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
  gcs_actor_scheduler_->ScheduleByGcs(actor);
  ASSERT_EQ(1, raylet_client_->num_workers_requested);
  ASSERT_EQ(1, raylet_client_->callbacks.size());
  ASSERT_EQ(0, worker_client_->callbacks.size());
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
  ASSERT_EQ(0, worker_client_->callbacks.size());

  // Grant a worker, then the actor creation request should be sent to the worker.
  WorkerID worker_id = WorkerID::FromRandom();
  ASSERT_TRUE(raylet_client_->GrantWorkerLease(node->node_manager_address(),
                                               node->node_manager_port(),
                                               worker_id,
                                               node_id,
                                               NodeID::Nil()));
  ASSERT_EQ(0, raylet_client_->callbacks.size());
  ASSERT_EQ(1, worker_client_->callbacks.size());

  // Reply the actor creation request, then the actor should be scheduled successfully.
  ASSERT_TRUE(worker_client_->ReplyPushTask());
  ASSERT_EQ(0, worker_client_->callbacks.size());
  ASSERT_EQ(0, cluster_task_manager_->GetInfeasibleQueueSize());
  ASSERT_EQ(0, cluster_task_manager_->GetPendingQueueSize());
  ASSERT_EQ(1, success_actors_.size());
  ASSERT_EQ(actor, success_actors_.front());
  ASSERT_EQ(actor->GetNodeID(), node_id);
  ASSERT_EQ(actor->GetWorkerID(), worker_id);
}

TEST_F(GcsActorSchedulerTest, TestScheduleRetryWhenCreatingByGcs) {
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
  gcs_actor_scheduler_->ScheduleByGcs(actor);
  ASSERT_EQ(1, raylet_client_->num_workers_requested);
  ASSERT_EQ(1, raylet_client_->callbacks.size());
  ASSERT_EQ(0, worker_client_->callbacks.size());

  // Grant a worker, then the actor creation request should be sent to the worker.
  WorkerID worker_id = WorkerID::FromRandom();
  ASSERT_TRUE(raylet_client_->GrantWorkerLease(node->node_manager_address(),
                                               node->node_manager_port(),
                                               worker_id,
                                               node_id,
                                               NodeID::Nil()));
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
  ASSERT_EQ(0, cluster_task_manager_->GetInfeasibleQueueSize());
  ASSERT_EQ(0, cluster_task_manager_->GetPendingQueueSize());
  ASSERT_EQ(1, success_actors_.size());
  ASSERT_EQ(actor, success_actors_.front());
  ASSERT_EQ(actor->GetNodeID(), node_id);
  ASSERT_EQ(actor->GetWorkerID(), worker_id);
}

TEST_F(GcsActorSchedulerTest, TestNodeFailedWhenLeasingByGcs) {
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
  gcs_actor_scheduler_->ScheduleByGcs(actor);
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
  ASSERT_TRUE(raylet_client_->GrantWorkerLease(node->node_manager_address(),
                                               node->node_manager_port(),
                                               WorkerID::FromRandom(),
                                               node_id,
                                               NodeID::Nil()));
  ASSERT_EQ(1, raylet_client_->num_workers_requested);
  ASSERT_EQ(0, raylet_client_->callbacks.size());
  ASSERT_EQ(0, gcs_actor_scheduler_->num_retry_leasing_count_);

  ASSERT_EQ(0, success_actors_.size());
  ASSERT_EQ(0, cluster_task_manager_->GetInfeasibleQueueSize());
  ASSERT_EQ(0, cluster_task_manager_->GetPendingQueueSize());
}

TEST_F(GcsActorSchedulerTest, TestLeasingCancelledWhenLeasingByGcs) {
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
  gcs_actor_scheduler_->ScheduleByGcs(actor);
  ASSERT_EQ(1, raylet_client_->num_workers_requested);
  ASSERT_EQ(1, raylet_client_->callbacks.size());

  // Cancel the lease request.
  const auto &task_id = actor->GetCreationTaskSpecification().TaskId();
  gcs_actor_scheduler_->CancelOnLeasing(node_id, actor->GetActorID(), task_id);
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
  ASSERT_EQ(0, cluster_task_manager_->GetInfeasibleQueueSize());
  ASSERT_EQ(0, cluster_task_manager_->GetPendingQueueSize());
}

TEST_F(GcsActorSchedulerTest, TestNodeFailedWhenCreatingByGcs) {
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
  gcs_actor_scheduler_->ScheduleByGcs(actor);
  ASSERT_EQ(1, raylet_client_->num_workers_requested);
  ASSERT_EQ(1, raylet_client_->callbacks.size());
  ASSERT_EQ(0, worker_client_->callbacks.size());

  // Grant a worker, then the actor creation request should be send to the worker.
  ASSERT_TRUE(raylet_client_->GrantWorkerLease(node->node_manager_address(),
                                               node->node_manager_port(),
                                               WorkerID::FromRandom(),
                                               node_id,
                                               NodeID::Nil()));
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
  ASSERT_EQ(0, cluster_task_manager_->GetInfeasibleQueueSize());
  ASSERT_EQ(0, cluster_task_manager_->GetPendingQueueSize());
}

TEST_F(GcsActorSchedulerTest, TestWorkerFailedWhenCreatingByGcs) {
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
  gcs_actor_scheduler_->ScheduleByGcs(actor);
  ASSERT_EQ(1, raylet_client_->num_workers_requested);
  ASSERT_EQ(1, raylet_client_->callbacks.size());
  ASSERT_EQ(0, worker_client_->callbacks.size());

  // Grant a worker, then the actor creation request should be send to the worker.
  auto worker_id = WorkerID::FromRandom();
  ASSERT_TRUE(raylet_client_->GrantWorkerLease(node->node_manager_address(),
                                               node->node_manager_port(),
                                               worker_id,
                                               node_id,
                                               NodeID::Nil()));
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
  ASSERT_EQ(0, cluster_task_manager_->GetInfeasibleQueueSize());
  ASSERT_EQ(0, cluster_task_manager_->GetPendingQueueSize());
}

TEST_F(GcsActorSchedulerTest, TestRescheduleByGcs) {
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
                                               node1->node_manager_port(),
                                               worker_id,
                                               node_id_1,
                                               NodeID::Nil()));
  ASSERT_EQ(0, raylet_client_->callbacks.size());
  ASSERT_EQ(1, worker_client_->callbacks.size());

  // Reply the actor creation request, then the actor should be scheduled successfully.
  ASSERT_TRUE(worker_client_->ReplyPushTask());
  ASSERT_EQ(0, worker_client_->callbacks.size());

  ASSERT_EQ(0, cluster_task_manager_->GetInfeasibleQueueSize());
  ASSERT_EQ(0, cluster_task_manager_->GetPendingQueueSize());
  ASSERT_EQ(2, success_actors_.size());
}

TEST_F(GcsActorSchedulerTest, TestReleaseUnusedWorkersByGcs) {
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
  absl::flat_hash_map<NodeID, std::vector<WorkerID>> node_to_workers;
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
  gcs_actor_scheduler_->ScheduleByGcs(actor);
  ASSERT_EQ(2, gcs_actor_scheduler_->num_retry_leasing_count_);
  ASSERT_EQ(raylet_client_->num_workers_requested, 0);

  // When `GcsActorScheduler` receives the `ReleaseUnusedWorkers` reply, it will send
  // out the `RequestWorkerLease` request.
  ASSERT_TRUE(raylet_client_->ReplyReleaseUnusedWorkers());
  gcs_actor_scheduler_->TryLeaseWorkerFromNodeAgain(actor, node);
  ASSERT_EQ(raylet_client_->num_workers_requested, 1);
}

}  // namespace gcs
}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
