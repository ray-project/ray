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

class MockWorkerClient : public rpc::CoreWorkerClientInterface {
 public:
  ray::Status WaitForActorOutOfScope(
      const rpc::WaitForActorOutOfScopeRequest &request,
      const rpc::ClientCallback<rpc::WaitForActorOutOfScopeReply> &callback) override {
    callbacks.push_back(callback);
    return Status::OK();
  }

  ray::Status KillActor(
      const rpc::KillActorRequest &request,
      const rpc::ClientCallback<rpc::KillActorReply> &callback) override {
    killed_actors.push_back(ActorID::FromBinary(request.intended_actor_id()));
    return Status::OK();
  }

  bool Reply(Status status = Status::OK()) {
    if (callbacks.size() == 0) {
      return false;
    }
    auto callback = callbacks.front();
    auto reply = rpc::WaitForActorOutOfScopeReply();
    callback(status, reply);
    callbacks.pop_front();
    return true;
  }

  std::list<rpc::ClientCallback<rpc::WaitForActorOutOfScopeReply>> callbacks;
  std::vector<ActorID> killed_actors;
};

class GcsActorManagerTest : public ::testing::Test {
 public:
  GcsActorManagerTest()
      : mock_actor_scheduler_(new MockActorScheduler()),
        worker_client_(new MockWorkerClient()) {
    gcs_pub_sub_ = std::make_shared<GcsServerMocker::MockGcsPubSub>(redis_client_);
    store_client_ = std::make_shared<gcs::InMemoryStoreClient>(io_service_);
    gcs_table_storage_ = std::make_shared<gcs::InMemoryGcsTableStorage>(io_service_);
    gcs_actor_manager_.reset(new gcs::GcsActorManager(
        mock_actor_scheduler_, gcs_table_storage_, gcs_pub_sub_,
        [&](const rpc::Address &addr) { return worker_client_; }));
  }

  boost::asio::io_service io_service_;
  std::shared_ptr<gcs::StoreClient> store_client_;
  std::shared_ptr<gcs::GcsTableStorage> gcs_table_storage_;
  std::shared_ptr<MockActorScheduler> mock_actor_scheduler_;
  std::shared_ptr<MockWorkerClient> worker_client_;
  std::unique_ptr<gcs::GcsActorManager> gcs_actor_manager_;
  std::shared_ptr<GcsServerMocker::MockGcsPubSub> gcs_pub_sub_;
  std::shared_ptr<gcs::RedisClient> redis_client_;
};

TEST_F(GcsActorManagerTest, TestBasic) {
  auto job_id = JobID::FromInt(1);
  auto create_actor_request = Mocker::GenCreateActorRequest(job_id);
  std::vector<std::shared_ptr<gcs::GcsActor>> finished_actors;
  Status status = gcs_actor_manager_->RegisterActor(
      create_actor_request, [&finished_actors](std::shared_ptr<gcs::GcsActor> actor) {
        finished_actors.emplace_back(actor);
      });
  RAY_CHECK_OK(status);

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
  gcs_actor_manager_->OnActorCreationSuccess(actor);
  ASSERT_EQ(finished_actors.size(), 1);

  ASSERT_TRUE(worker_client_->Reply());
  ASSERT_EQ(actor->GetState(), rpc::ActorTableData::DEAD);
}

TEST_F(GcsActorManagerTest, TestSchedulingFailed) {
  auto job_id = JobID::FromInt(1);
  auto create_actor_request = Mocker::GenCreateActorRequest(job_id);
  std::vector<std::shared_ptr<gcs::GcsActor>> finished_actors;
  RAY_CHECK_OK(gcs_actor_manager_->RegisterActor(
      create_actor_request, [&finished_actors](std::shared_ptr<gcs::GcsActor> actor) {
        finished_actors.emplace_back(actor);
      }));

  ASSERT_EQ(finished_actors.size(), 0);
  ASSERT_EQ(mock_actor_scheduler_->actors.size(), 1);
  auto actor = mock_actor_scheduler_->actors.back();
  mock_actor_scheduler_->actors.clear();

  gcs_actor_manager_->OnActorCreationFailed(actor);
  gcs_actor_manager_->SchedulePendingActors();
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
  gcs_actor_manager_->OnActorCreationSuccess(actor);
  ASSERT_EQ(finished_actors.size(), 1);
}

TEST_F(GcsActorManagerTest, TestWorkerFailure) {
  auto job_id = JobID::FromInt(1);
  auto create_actor_request = Mocker::GenCreateActorRequest(job_id);
  std::vector<std::shared_ptr<gcs::GcsActor>> finished_actors;
  RAY_CHECK_OK(gcs_actor_manager_->RegisterActor(
      create_actor_request, [&finished_actors](std::shared_ptr<gcs::GcsActor> actor) {
        finished_actors.emplace_back(actor);
      }));

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
  gcs_actor_manager_->OnActorCreationSuccess(actor);
  ASSERT_EQ(finished_actors.size(), 1);

  // Killing another worker does not affect this actor.
  EXPECT_CALL(*mock_actor_scheduler_, CancelOnWorker(node_id, _));
  gcs_actor_manager_->OnWorkerDead(node_id, WorkerID::FromRandom());
  ASSERT_EQ(actor->GetState(), rpc::ActorTableData::ALIVE);

  // Remove worker and then check that the actor is dead.
  gcs_actor_manager_->OnWorkerDead(node_id, worker_id);
  ASSERT_EQ(actor->GetState(), rpc::ActorTableData::DEAD);
  // No more actors to schedule.
  gcs_actor_manager_->SchedulePendingActors();
  ASSERT_EQ(mock_actor_scheduler_->actors.size(), 0);

  ASSERT_TRUE(worker_client_->Reply());
}

TEST_F(GcsActorManagerTest, TestNodeFailure) {
  auto job_id = JobID::FromInt(1);
  auto create_actor_request = Mocker::GenCreateActorRequest(job_id);
  std::vector<std::shared_ptr<gcs::GcsActor>> finished_actors;
  Status status = gcs_actor_manager_->RegisterActor(
      create_actor_request, [&finished_actors](std::shared_ptr<gcs::GcsActor> actor) {
        finished_actors.emplace_back(actor);
      });
  RAY_CHECK_OK(status);

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
  gcs_actor_manager_->OnActorCreationSuccess(actor);
  ASSERT_EQ(finished_actors.size(), 1);

  // Killing another node does not affect this actor.
  EXPECT_CALL(*mock_actor_scheduler_, CancelOnNode(_));
  gcs_actor_manager_->OnNodeDead(ClientID::FromRandom());
  ASSERT_EQ(actor->GetState(), rpc::ActorTableData::ALIVE);

  // Remove node and then check that the actor is dead.
  EXPECT_CALL(*mock_actor_scheduler_, CancelOnNode(node_id));
  gcs_actor_manager_->OnNodeDead(node_id);
  ASSERT_EQ(actor->GetState(), rpc::ActorTableData::DEAD);
  // No more actors to schedule.
  gcs_actor_manager_->SchedulePendingActors();
  ASSERT_EQ(mock_actor_scheduler_->actors.size(), 0);

  ASSERT_TRUE(worker_client_->Reply());
}

TEST_F(GcsActorManagerTest, TestActorReconstruction) {
  auto job_id = JobID::FromInt(1);
  auto create_actor_request =
      Mocker::GenCreateActorRequest(job_id, /*max_restarts=*/1, /*detached=*/false);
  std::vector<std::shared_ptr<gcs::GcsActor>> finished_actors;
  Status status = gcs_actor_manager_->RegisterActor(
      create_actor_request, [&finished_actors](std::shared_ptr<gcs::GcsActor> actor) {
        finished_actors.emplace_back(actor);
      });
  RAY_CHECK_OK(status);

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
  gcs_actor_manager_->OnActorCreationSuccess(actor);
  ASSERT_EQ(finished_actors.size(), 1);

  // Remove worker and then check that the actor is being restarted.
  EXPECT_CALL(*mock_actor_scheduler_, CancelOnNode(node_id));
  gcs_actor_manager_->OnNodeDead(node_id);
  ASSERT_EQ(actor->GetState(), rpc::ActorTableData::RESTARTING);

  // Add node and check that the actor is restarted.
  gcs_actor_manager_->SchedulePendingActors();
  ASSERT_EQ(mock_actor_scheduler_->actors.size(), 1);
  mock_actor_scheduler_->actors.clear();
  ASSERT_EQ(finished_actors.size(), 1);
  auto node_id2 = ClientID::FromRandom();
  address.set_raylet_id(node_id2.Binary());
  actor->UpdateAddress(address);
  gcs_actor_manager_->OnActorCreationSuccess(actor);
  ASSERT_EQ(finished_actors.size(), 1);
  ASSERT_EQ(actor->GetState(), rpc::ActorTableData::ALIVE);
  ASSERT_EQ(actor->GetNodeID(), node_id2);

  // Killing another worker does not affect this actor.
  EXPECT_CALL(*mock_actor_scheduler_, CancelOnNode(_));
  gcs_actor_manager_->OnNodeDead(ClientID::FromRandom());
  ASSERT_EQ(actor->GetState(), rpc::ActorTableData::ALIVE);

  // Remove worker and then check that the actor is dead.
  EXPECT_CALL(*mock_actor_scheduler_, CancelOnNode(node_id2));
  gcs_actor_manager_->OnNodeDead(node_id2);
  ASSERT_EQ(actor->GetState(), rpc::ActorTableData::DEAD);
  // No more actors to schedule.
  gcs_actor_manager_->SchedulePendingActors();
  ASSERT_EQ(mock_actor_scheduler_->actors.size(), 0);

  ASSERT_TRUE(worker_client_->Reply());
}

TEST_F(GcsActorManagerTest, TestActorRestartWhenOwnerDead) {
  auto job_id = JobID::FromInt(1);
  auto create_actor_request =
      Mocker::GenCreateActorRequest(job_id, /*max_restarts=*/1, /*detached=*/false);
  std::vector<std::shared_ptr<gcs::GcsActor>> finished_actors;
  RAY_CHECK_OK(gcs_actor_manager_->RegisterActor(
      create_actor_request, [&finished_actors](std::shared_ptr<gcs::GcsActor> actor) {
        finished_actors.emplace_back(actor);
      }));

  ASSERT_EQ(finished_actors.size(), 0);
  ASSERT_EQ(mock_actor_scheduler_->actors.size(), 1);
  auto actor = mock_actor_scheduler_->actors.back();
  mock_actor_scheduler_->actors.pop_back();
  const auto owner_node_id = actor->GetOwnerNodeID();

  // Check that the actor is in state `ALIVE`.
  rpc::Address address;
  auto node_id = ClientID::FromRandom();
  auto worker_id = WorkerID::FromRandom();
  address.set_raylet_id(node_id.Binary());
  address.set_worker_id(worker_id.Binary());
  actor->UpdateAddress(address);
  gcs_actor_manager_->OnActorCreationSuccess(actor);
  ASSERT_EQ(finished_actors.size(), 1);

  // Remove the owner's node.
  EXPECT_CALL(*mock_actor_scheduler_, CancelOnNode(owner_node_id));
  gcs_actor_manager_->OnNodeDead(owner_node_id);
  // The child actor should be marked as dead.
  ASSERT_EQ(actor->GetState(), rpc::ActorTableData::DEAD);
  ASSERT_EQ(worker_client_->killed_actors.size(), 1);
  ASSERT_EQ(worker_client_->killed_actors.front(), actor->GetActorID());

  // Remove the actor's node and check that the actor is not restarted, since
  // its owner has died.
  EXPECT_CALL(*mock_actor_scheduler_, CancelOnNode(node_id));
  gcs_actor_manager_->OnNodeDead(node_id);
  ASSERT_EQ(actor->GetState(), rpc::ActorTableData::DEAD);
  gcs_actor_manager_->SchedulePendingActors();
  ASSERT_TRUE(mock_actor_scheduler_->actors.empty());
}

TEST_F(GcsActorManagerTest, TestDetachedActorRestartWhenCreatorDead) {
  auto job_id = JobID::FromInt(1);
  auto create_actor_request =
      Mocker::GenCreateActorRequest(job_id, /*max_restarts=*/1, /*detached=*/true);
  std::vector<std::shared_ptr<gcs::GcsActor>> finished_actors;
  RAY_CHECK_OK(gcs_actor_manager_->RegisterActor(
      create_actor_request, [&finished_actors](std::shared_ptr<gcs::GcsActor> actor) {
        finished_actors.emplace_back(actor);
      }));

  ASSERT_EQ(finished_actors.size(), 0);
  ASSERT_EQ(mock_actor_scheduler_->actors.size(), 1);
  auto actor = mock_actor_scheduler_->actors.back();
  mock_actor_scheduler_->actors.pop_back();
  const auto owner_node_id = actor->GetOwnerNodeID();

  // Check that the actor is in state `ALIVE`.
  rpc::Address address;
  auto node_id = ClientID::FromRandom();
  auto worker_id = WorkerID::FromRandom();
  address.set_raylet_id(node_id.Binary());
  address.set_worker_id(worker_id.Binary());
  actor->UpdateAddress(address);
  gcs_actor_manager_->OnActorCreationSuccess(actor);
  ASSERT_EQ(finished_actors.size(), 1);

  // Remove the owner's node.
  EXPECT_CALL(*mock_actor_scheduler_, CancelOnNode(owner_node_id));
  gcs_actor_manager_->OnNodeDead(owner_node_id);
  // The child actor should not be marked as dead.
  ASSERT_TRUE(worker_client_->killed_actors.empty());
  ASSERT_EQ(actor->GetState(), rpc::ActorTableData::ALIVE);
}

TEST_F(GcsActorManagerTest, TestNamedActors) {
  auto job_id_1 = JobID::FromInt(1);
  auto job_id_2 = JobID::FromInt(2);

  auto request1 =
      Mocker::GenCreateActorRequest(job_id_1, 0, /*is_detached=*/true, /*name=*/"actor1");
  Status status = gcs_actor_manager_->RegisterActor(
      request1, [](std::shared_ptr<gcs::GcsActor> actor) {});
  ASSERT_TRUE(status.ok());
  ASSERT_EQ(gcs_actor_manager_->GetActorIDByName("actor1").Binary(),
            request1.task_spec().actor_creation_task_spec().actor_id());

  auto request2 =
      Mocker::GenCreateActorRequest(job_id_1, 0, /*is_detached=*/true, /*name=*/"actor2");
  status = gcs_actor_manager_->RegisterActor(request2,
                                             [](std::shared_ptr<gcs::GcsActor> actor) {});
  ASSERT_TRUE(status.ok());
  ASSERT_EQ(gcs_actor_manager_->GetActorIDByName("actor2").Binary(),
            request2.task_spec().actor_creation_task_spec().actor_id());

  // Check that looking up a non-existent name returns ActorID::Nil();
  ASSERT_EQ(gcs_actor_manager_->GetActorIDByName("actor3"), ActorID::Nil());

  // Check that naming collisions return Status::Invalid.
  auto request3 =
      Mocker::GenCreateActorRequest(job_id_1, 0, /*is_detached=*/true, /*name=*/"actor2");
  status = gcs_actor_manager_->RegisterActor(request3,
                                             [](std::shared_ptr<gcs::GcsActor> actor) {});
  ASSERT_TRUE(status.IsInvalid());
  ASSERT_EQ(gcs_actor_manager_->GetActorIDByName("actor2").Binary(),
            request2.task_spec().actor_creation_task_spec().actor_id());

  // Check that naming collisions are enforced across JobIDs.
  auto request4 =
      Mocker::GenCreateActorRequest(job_id_2, 0, /*is_detached=*/true, /*name=*/"actor2");
  status = gcs_actor_manager_->RegisterActor(request4,
                                             [](std::shared_ptr<gcs::GcsActor> actor) {});
  ASSERT_TRUE(status.IsInvalid());
  ASSERT_EQ(gcs_actor_manager_->GetActorIDByName("actor2").Binary(),
            request2.task_spec().actor_creation_task_spec().actor_id());
}

TEST_F(GcsActorManagerTest, TestNamedActorDeletionWorkerFailure) {
  // Make sure named actor deletion succeeds when workers fail.
  const auto actor_name = "actor_to_delete";
  const auto job_id_1 = JobID::FromInt(1);
  const auto request1 = Mocker::GenCreateActorRequest(job_id_1, 0, /*is_detached=*/true,
                                                      /*name=*/actor_name);
  Status status = gcs_actor_manager_->RegisterActor(
      request1, [](std::shared_ptr<gcs::GcsActor> actor) {});
  ASSERT_TRUE(status.ok());
  ASSERT_EQ(gcs_actor_manager_->GetActorIDByName(actor_name).Binary(),
            request1.task_spec().actor_creation_task_spec().actor_id());

  auto actor = mock_actor_scheduler_->actors.back();
  mock_actor_scheduler_->actors.pop_back();

  // Check that the actor is in state `ALIVE`.
  rpc::Address address;
  auto node_id = ClientID::FromRandom();
  auto worker_id = WorkerID::FromRandom();
  address.set_raylet_id(node_id.Binary());
  address.set_worker_id(worker_id.Binary());
  actor->UpdateAddress(address);
  gcs_actor_manager_->OnActorCreationSuccess(actor);

  // Remove worker and then check that the actor is dead.
  gcs_actor_manager_->OnWorkerDead(node_id, worker_id);
  ASSERT_EQ(actor->GetState(), rpc::ActorTableData::DEAD);
  ASSERT_EQ(gcs_actor_manager_->GetActorIDByName(actor_name), ActorID::Nil());

  // Create an actor with the same name. This ensures that the name has been properly
  // deleted.
  const auto request2 = Mocker::GenCreateActorRequest(job_id_1, 0, /*is_detached=*/true,
                                                      /*name=*/actor_name);
  status = gcs_actor_manager_->RegisterActor(request2,
                                             [](std::shared_ptr<gcs::GcsActor> actor) {});
  ASSERT_TRUE(status.ok());
  ASSERT_EQ(gcs_actor_manager_->GetActorIDByName(actor_name).Binary(),
            request2.task_spec().actor_creation_task_spec().actor_id());
}

TEST_F(GcsActorManagerTest, TestNamedActorDeletionNodeFailure) {
  // Make sure named actor deletion succeeds when nodes fail.
  const auto job_id_1 = JobID::FromInt(1);
  const auto request1 =
      Mocker::GenCreateActorRequest(job_id_1, 0, /*is_detached=*/true, /*name=*/"actor");
  Status status = gcs_actor_manager_->RegisterActor(
      request1, [](std::shared_ptr<gcs::GcsActor> actor) {});
  ASSERT_TRUE(status.ok());
  ASSERT_EQ(gcs_actor_manager_->GetActorIDByName("actor").Binary(),
            request1.task_spec().actor_creation_task_spec().actor_id());

  auto actor = mock_actor_scheduler_->actors.back();
  mock_actor_scheduler_->actors.pop_back();

  // Check that the actor is in state `ALIVE`.
  rpc::Address address;
  auto node_id = ClientID::FromRandom();
  auto worker_id = WorkerID::FromRandom();
  address.set_raylet_id(node_id.Binary());
  address.set_worker_id(worker_id.Binary());
  actor->UpdateAddress(address);
  gcs_actor_manager_->OnActorCreationSuccess(actor);

  // Remove node and then check that the actor is dead.
  EXPECT_CALL(*mock_actor_scheduler_, CancelOnNode(node_id));
  gcs_actor_manager_->OnNodeDead(node_id);
  ASSERT_EQ(actor->GetState(), rpc::ActorTableData::DEAD);

  // Create an actor with the same name. This ensures that the name has been properly
  // deleted.
  const auto request2 =
      Mocker::GenCreateActorRequest(job_id_1, 0, /*is_detached=*/true, /*name=*/"actor");
  status = gcs_actor_manager_->RegisterActor(request2,
                                             [](std::shared_ptr<gcs::GcsActor> actor) {});
  ASSERT_TRUE(status.ok());
  ASSERT_EQ(gcs_actor_manager_->GetActorIDByName("actor").Binary(),
            request2.task_spec().actor_creation_task_spec().actor_id());
}

TEST_F(GcsActorManagerTest, TestNamedActorDeletionNotHappendWhenReconstructed) {
  // Make sure named actor deletion succeeds when nodes fail.
  const auto job_id_1 = JobID::FromInt(1);
  // The dead actor will be reconstructed.
  const auto request1 =
      Mocker::GenCreateActorRequest(job_id_1, 1, /*is_detached=*/true, /*name=*/"actor");
  Status status = gcs_actor_manager_->RegisterActor(
      request1, [](std::shared_ptr<gcs::GcsActor> actor) {});
  ASSERT_TRUE(status.ok());
  ASSERT_EQ(gcs_actor_manager_->GetActorIDByName("actor").Binary(),
            request1.task_spec().actor_creation_task_spec().actor_id());

  auto actor = mock_actor_scheduler_->actors.back();
  mock_actor_scheduler_->actors.pop_back();

  // Check that the actor is in state `ALIVE`.
  rpc::Address address;
  auto node_id = ClientID::FromRandom();
  auto worker_id = WorkerID::FromRandom();
  address.set_raylet_id(node_id.Binary());
  address.set_worker_id(worker_id.Binary());
  actor->UpdateAddress(address);
  gcs_actor_manager_->OnActorCreationSuccess(actor);

  // Remove worker and then check that the actor is dead. The actor should be
  // reconstructed.
  gcs_actor_manager_->OnWorkerDead(node_id, worker_id);
  ASSERT_EQ(actor->GetState(), rpc::ActorTableData::RESTARTING);

  // Create an actor with the same name.
  // It should fail because actor has been reconstructed, and names shouldn't have been
  // cleaned.
  const auto job_id_2 = JobID::FromInt(2);
  const auto request2 =
      Mocker::GenCreateActorRequest(job_id_2, 0, /*is_detached=*/true, /*name=*/"actor");
  status = gcs_actor_manager_->RegisterActor(request2,
                                             [](std::shared_ptr<gcs::GcsActor> actor) {});
  ASSERT_TRUE(status.IsInvalid());
  ASSERT_EQ(gcs_actor_manager_->GetActorIDByName("actor").Binary(),
            request1.task_spec().actor_creation_task_spec().actor_id());
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
