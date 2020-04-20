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
#include "gtest/gtest.h"

namespace ray {

class MockedGcsActorManager : public gcs::GcsActorManager {
 public:
  explicit MockedGcsActorManager(boost::asio::io_context &io_context,
                                 gcs::ActorInfoAccessor &actor_info_accessor,
                                 gcs::GcsNodeManager &gcs_node_manager,
                                 gcs::LeaseClientFactoryFn lease_client_factory = nullptr,
                                 rpc::ClientFactoryFn client_factory = nullptr)
      : gcs::GcsActorManager(io_context, actor_info_accessor, gcs_node_manager,
                             lease_client_factory, client_factory) {
    gcs_actor_scheduler_.reset(new Mocker::MockedGcsActorScheduler(
        io_context, actor_info_accessor, gcs_node_manager,
        /*schedule_failure_handler=*/
        [this](std::shared_ptr<gcs::GcsActor> actor) {
          // When there are no available nodes to schedule the actor the
          // gcs_actor_scheduler will treat it as failed and invoke this handler. In
          // this case, the actor should be appended to the `pending_actors_` and wait
          // for the registration of new node.
          pending_actors_.emplace_back(std::move(actor));
        },
        /*schedule_success_handler=*/
        [this](std::shared_ptr<gcs::GcsActor> actor) {
          OnActorCreateSuccess(std::move(actor));
        },
        std::move(lease_client_factory), std::move(client_factory)));
  }

 public:
  void ResetLeaseClientFactory(gcs::LeaseClientFactoryFn lease_client_factory) {
    auto gcs_actor_scheduler =
        dynamic_cast<Mocker::MockedGcsActorScheduler *>(gcs_actor_scheduler_.get());
    gcs_actor_scheduler->ResetLeaseClientFactory(std::move(lease_client_factory));
  }

  void ResetClientFactory(rpc::ClientFactoryFn client_factory) {
    auto gcs_actor_scheduler =
        dynamic_cast<Mocker::MockedGcsActorScheduler *>(gcs_actor_scheduler_.get());
    gcs_actor_scheduler->ResetClientFactory(std::move(client_factory));
  }

  const absl::flat_hash_map<ActorID, std::shared_ptr<gcs::GcsActor>>
      &GetAllRegisteredActors() const {
    return registered_actors_;
  }

  const std::vector<std::shared_ptr<gcs::GcsActor>> &GetAllPendingActors() const {
    return pending_actors_;
  }
};

class GcsActorManagerTest : public ::testing::Test {
 public:
  void SetUp() override {
    raylet_client_ = std::make_shared<Mocker::MockRayletClient>();
    worker_client_ = std::make_shared<Mocker::MockWorkerClient>();
    gcs_node_manager_ = std::make_shared<gcs::GcsNodeManager>(
        io_service_, node_info_accessor_, error_info_accessor_);
    gcs_actor_manager_ = std::make_shared<MockedGcsActorManager>(
        io_service_, actor_info_accessor_, *gcs_node_manager_,
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
  std::shared_ptr<MockedGcsActorManager> gcs_actor_manager_;
};

TEST_F(GcsActorManagerTest, TestNormalFlow) {
  gcs_actor_manager_->ResetLeaseClientFactory([this](const rpc::Address &address) {
    raylet_client_->auto_grant_node_id = ClientID::FromBinary(address.raylet_id());
    return raylet_client_;
  });
  gcs_actor_manager_->ResetClientFactory([this](const rpc::Address &address) {
    worker_client_->enable_auto_reply = true;
    return worker_client_;
  });

  auto job_id = JobID::FromInt(1);
  auto create_actor_request =
      Mocker::GenCreateActorRequest(job_id, /*max_reconstructions=*/2);
  std::vector<std::shared_ptr<gcs::GcsActor>> finished_actors;
  gcs_actor_manager_->RegisterActor(
      create_actor_request, [&finished_actors](std::shared_ptr<gcs::GcsActor> actor) {
        finished_actors.emplace_back(actor);
      });

  ASSERT_EQ(0, finished_actors.size());
  ASSERT_EQ(1, gcs_actor_manager_->GetAllRegisteredActors().size());
  ASSERT_EQ(1, gcs_actor_manager_->GetAllPendingActors().size());

  auto actor = gcs_actor_manager_->GetAllRegisteredActors().begin()->second;
  ASSERT_EQ(rpc::ActorTableData::PENDING, actor->GetState());

  // Add node_1 and then check if the actor is in state `ALIVE`
  auto node_1 = Mocker::GenNodeInfo();
  auto node_id_1 = ClientID::FromBinary(node_1->node_id());
  gcs_node_manager_->AddNode(node_1);
  ASSERT_EQ(1, finished_actors.size());
  ASSERT_EQ(1, gcs_node_manager_->GetAllAliveNodes().size());
  ASSERT_EQ(0, gcs_actor_manager_->GetAllPendingActors().size());
  ASSERT_EQ(rpc::ActorTableData::ALIVE, actor->GetState());
  ASSERT_EQ(node_id_1, actor->GetNodeID());

  // Remove node_1 and then check if the actor is in state `RECONSTRUCTING`
  gcs_node_manager_->RemoveNode(node_id_1);
  ASSERT_EQ(0, gcs_node_manager_->GetAllAliveNodes().size());
  ASSERT_EQ(1, gcs_actor_manager_->GetAllPendingActors().size());
  ASSERT_EQ(rpc::ActorTableData::RECONSTRUCTING, actor->GetState());

  // Add node_2 and then check if the actor is alive again.
  auto node_2 = Mocker::GenNodeInfo();
  auto node_id_2 = ClientID::FromBinary(node_2->node_id());
  gcs_node_manager_->AddNode(node_2);
  ASSERT_EQ(1, gcs_node_manager_->GetAllAliveNodes().size());
  ASSERT_EQ(0, gcs_actor_manager_->GetAllPendingActors().size());
  ASSERT_EQ(rpc::ActorTableData::ALIVE, actor->GetState());
  ASSERT_EQ(node_id_2, actor->GetNodeID());

  // Add node_3.
  auto node_3 = Mocker::GenNodeInfo();
  auto node_id_3 = ClientID::FromBinary(node_3->node_id());
  gcs_node_manager_->AddNode(node_3);
  ASSERT_EQ(2, gcs_node_manager_->GetAllAliveNodes().size());

  // Remove node_2 and then check if the actor drift to node_3.
  gcs_node_manager_->RemoveNode(node_id_2);
  ASSERT_EQ(1, gcs_node_manager_->GetAllAliveNodes().size());
  ASSERT_EQ(0, gcs_actor_manager_->GetAllPendingActors().size());
  ASSERT_EQ(rpc::ActorTableData::ALIVE, actor->GetState());
  ASSERT_EQ(node_id_3, actor->GetNodeID());

  // Remove node_3 and then check if the actor is dead.
  gcs_node_manager_->RemoveNode(node_id_3);
  ASSERT_EQ(0, gcs_node_manager_->GetAllAliveNodes().size());
  ASSERT_EQ(0, gcs_actor_manager_->GetAllPendingActors().size());
  ASSERT_EQ(rpc::ActorTableData::DEAD, actor->GetState());
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
