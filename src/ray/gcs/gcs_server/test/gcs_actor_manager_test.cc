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

#include <ray/gcs/gcs_server/test/gcs_server_test_util.h>
#include <ray/gcs/test/gcs_test_util.h>

#include <memory>
#include "gtest/gtest.h"

namespace ray {

using ::testing::_;

class MockActorScheduler : public gcs::GcsActorSchedulerInterface {
 public:
  MockActorScheduler() {}

  void Schedule(std::shared_ptr<gcs::GcsActor> actor) { actors.push_back(actor); }

  MOCK_METHOD1(CancelOnNode, std::vector<ActorID>(const ClientID &node_id));
  MOCK_METHOD2(CancelOnWorker,
               ActorID(const ClientID &node_id, const WorkerID &worker_id));

  std::vector<std::shared_ptr<gcs::GcsActor>> actors;
};

class GcsActorManagerTest : public ::testing::Test {
 public:
  GcsActorManagerTest()
      : mock_actor_scheduler_(new MockActorScheduler()),
        gcs_actor_manager_(mock_actor_scheduler_, actor_info_accessor_) {}

  GcsServerMocker::MockedActorInfoAccessor actor_info_accessor_;
  std::shared_ptr<MockActorScheduler> mock_actor_scheduler_;
  gcs::GcsActorManager gcs_actor_manager_;
};

TEST_F(GcsActorManagerTest, TestBasic) {
  auto job_id = JobID::FromInt(1);
  auto create_actor_request = Mocker::GenCreateActorRequest(job_id);
  std::vector<std::shared_ptr<gcs::GcsActor>> finished_actors;
  gcs_actor_manager_.RegisterActor(
      create_actor_request, [&finished_actors](std::shared_ptr<gcs::GcsActor> actor) {
        finished_actors.emplace_back(actor);
      });

  ASSERT_EQ(finished_actors.size(), 0);
  ASSERT_EQ(mock_actor_scheduler_->actors.size(), 1);
  auto actor = mock_actor_scheduler_->actors.back();
  mock_actor_scheduler_->actors.pop_back();

  gcs_actor_manager_.OnActorCreationFailed(actor);
  gcs_actor_manager_.SchedulePendingActors();
  ASSERT_EQ(mock_actor_scheduler_->actors.size(), 1);
  mock_actor_scheduler_->actors.clear();
  ASSERT_EQ(finished_actors.size(), 0);

  // Check that the actor is in state `ALIVE`.
  rpc::Address address;
  auto node_id = ClientID::FromRandom();
  auto worker_id = WorkerID::FromRandom();
  address.set_raylet_id(node_id.Binary());
  address.set_worker_id(worker_id.Binary());
  actor->UpdateAddress(address);
  gcs_actor_manager_.OnActorCreationSuccess(actor);
  ASSERT_EQ(finished_actors.size(), 1);

  // Killing another worker does not affect this actor.
  EXPECT_CALL(*mock_actor_scheduler_, CancelOnWorker(node_id, _));
  gcs_actor_manager_.ReconstructActorOnWorker(node_id, WorkerID::FromRandom());
  ASSERT_EQ(actor->GetState(), rpc::ActorTableData::ALIVE);

  // Remove worker and then check that the actor is dead.
  gcs_actor_manager_.ReconstructActorOnWorker(node_id, worker_id);
  ASSERT_EQ(actor->GetState(), rpc::ActorTableData::DEAD);
  // No more actors to schedule.
  gcs_actor_manager_.SchedulePendingActors();
  ASSERT_EQ(mock_actor_scheduler_->actors.size(), 0);
}

TEST_F(GcsActorManagerTest, TestBasicNodeFailure) {
  auto job_id = JobID::FromInt(1);
  auto create_actor_request = Mocker::GenCreateActorRequest(job_id);
  std::vector<std::shared_ptr<gcs::GcsActor>> finished_actors;
  gcs_actor_manager_.RegisterActor(
      create_actor_request, [&finished_actors](std::shared_ptr<gcs::GcsActor> actor) {
        finished_actors.emplace_back(actor);
      });

  ASSERT_EQ(finished_actors.size(), 0);
  ASSERT_EQ(mock_actor_scheduler_->actors.size(), 1);
  auto actor = mock_actor_scheduler_->actors.back();
  mock_actor_scheduler_->actors.pop_back();

  gcs_actor_manager_.OnActorCreationFailed(actor);
  gcs_actor_manager_.SchedulePendingActors();
  ASSERT_EQ(mock_actor_scheduler_->actors.size(), 1);
  mock_actor_scheduler_->actors.clear();
  ASSERT_EQ(finished_actors.size(), 0);

  // Check that the actor is in state `ALIVE`.
  rpc::Address address;
  auto node_id = ClientID::FromRandom();
  auto worker_id = WorkerID::FromRandom();
  address.set_raylet_id(node_id.Binary());
  address.set_worker_id(worker_id.Binary());
  actor->UpdateAddress(address);
  gcs_actor_manager_.OnActorCreationSuccess(actor);
  ASSERT_EQ(finished_actors.size(), 1);

  // Killing another worker does not affect this actor.
  EXPECT_CALL(*mock_actor_scheduler_, CancelOnNode(_));
  gcs_actor_manager_.ReconstructActorsOnNode(ClientID::FromRandom());
  ASSERT_EQ(actor->GetState(), rpc::ActorTableData::ALIVE);

  // Remove worker and then check that the actor is dead.
  EXPECT_CALL(*mock_actor_scheduler_, CancelOnNode(node_id));
  gcs_actor_manager_.ReconstructActorsOnNode(node_id);
  ASSERT_EQ(actor->GetState(), rpc::ActorTableData::DEAD);
  // No more actors to schedule.
  gcs_actor_manager_.SchedulePendingActors();
  ASSERT_EQ(mock_actor_scheduler_->actors.size(), 0);
}

TEST_F(GcsActorManagerTest, TestActorReconstruction) {
  auto job_id = JobID::FromInt(1);
  auto create_actor_request = Mocker::GenCreateActorRequest(
      job_id, /*max_reconstructions=*/1, /*detached=*/false);
  std::vector<std::shared_ptr<gcs::GcsActor>> finished_actors;
  gcs_actor_manager_.RegisterActor(
      create_actor_request, [&finished_actors](std::shared_ptr<gcs::GcsActor> actor) {
        finished_actors.emplace_back(actor);
      });

  ASSERT_EQ(finished_actors.size(), 0);
  ASSERT_EQ(mock_actor_scheduler_->actors.size(), 1);
  auto actor = mock_actor_scheduler_->actors.back();
  mock_actor_scheduler_->actors.pop_back();

  // Check that the actor is in state `ALIVE`.
  rpc::Address address;
  auto node_id = ClientID::FromRandom();
  auto worker_id = WorkerID::FromRandom();
  address.set_raylet_id(node_id.Binary());
  address.set_worker_id(worker_id.Binary());
  actor->UpdateAddress(address);
  gcs_actor_manager_.OnActorCreationSuccess(actor);
  ASSERT_EQ(finished_actors.size(), 1);

  // Remove worker and then check that the actor is being restarted.
  EXPECT_CALL(*mock_actor_scheduler_, CancelOnNode(node_id));
  gcs_actor_manager_.ReconstructActorsOnNode(node_id);
  ASSERT_EQ(actor->GetState(), rpc::ActorTableData::RECONSTRUCTING);

  // Add node and check that the actor is restarted.
  gcs_actor_manager_.SchedulePendingActors();
  ASSERT_EQ(mock_actor_scheduler_->actors.size(), 1);
  mock_actor_scheduler_->actors.clear();
  ASSERT_EQ(finished_actors.size(), 1);
  auto node_id2 = ClientID::FromRandom();
  address.set_raylet_id(node_id2.Binary());
  actor->UpdateAddress(address);
  gcs_actor_manager_.OnActorCreationSuccess(actor);
  ASSERT_EQ(finished_actors.size(), 1);
  ASSERT_EQ(actor->GetState(), rpc::ActorTableData::ALIVE);
  ASSERT_EQ(actor->GetNodeID(), node_id2);

  // Killing another worker does not affect this actor.
  EXPECT_CALL(*mock_actor_scheduler_, CancelOnNode(_));
  gcs_actor_manager_.ReconstructActorsOnNode(ClientID::FromRandom());
  ASSERT_EQ(actor->GetState(), rpc::ActorTableData::ALIVE);

  // Remove worker and then check that the actor is dead.
  EXPECT_CALL(*mock_actor_scheduler_, CancelOnNode(node_id2));
  gcs_actor_manager_.ReconstructActorsOnNode(node_id2);
  ASSERT_EQ(actor->GetState(), rpc::ActorTableData::DEAD);
  // No more actors to schedule.
  gcs_actor_manager_.SchedulePendingActors();
  ASSERT_EQ(mock_actor_scheduler_->actors.size(), 0);
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
