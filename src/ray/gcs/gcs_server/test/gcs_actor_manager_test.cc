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
#include "ray/common/test_util.h"
#include "ray/gcs/gcs_server/test/gcs_server_test_util.h"
#include "ray/gcs/test/gcs_test_util.h"
#include "ray/gcs/gcs_server/gcs_kv_manager.h"
#include "mock/ray/gcs/gcs_server/gcs_kv_manager.h"
#include "mock/ray/pubsub/publisher.h"
// clang-format on

namespace ray {

using ::testing::_;
using ::testing::Return;

class MockActorScheduler : public gcs::GcsActorSchedulerInterface {
 public:
  MockActorScheduler() {}

  void Schedule(std::shared_ptr<gcs::GcsActor> actor) { actors.push_back(actor); }
  void Reschedule(std::shared_ptr<gcs::GcsActor> actor) {}
  void ReleaseUnusedWorkers(
      const absl::flat_hash_map<NodeID, std::vector<WorkerID>> &node_to_workers) {}
  void OnActorDestruction(std::shared_ptr<gcs::GcsActor> actor) {}

  MOCK_CONST_METHOD0(DebugString, std::string());
  MOCK_METHOD1(CancelOnNode, std::vector<ActorID>(const NodeID &node_id));
  MOCK_METHOD2(CancelOnWorker, ActorID(const NodeID &node_id, const WorkerID &worker_id));
  MOCK_METHOD3(CancelOnLeasing,
               void(const NodeID &node_id,
                    const ActorID &actor_id,
                    const TaskID &task_id));

  std::vector<std::shared_ptr<gcs::GcsActor>> actors;
};

class MockWorkerClient : public rpc::CoreWorkerClientInterface {
 public:
  MockWorkerClient(instrumented_io_context &io_service) : io_service_(io_service) {}

  void WaitForActorOutOfScope(
      const rpc::WaitForActorOutOfScopeRequest &request,
      const rpc::ClientCallback<rpc::WaitForActorOutOfScopeReply> &callback) override {
    callbacks_.push_back(callback);
  }

  void KillActor(const rpc::KillActorRequest &request,
                 const rpc::ClientCallback<rpc::KillActorReply> &callback) override {
    killed_actors_.push_back(ActorID::FromBinary(request.intended_actor_id()));
  }

  bool Reply(Status status = Status::OK()) {
    if (callbacks_.size() == 0) {
      return false;
    }

    // The created_actors_ of gcs actor manager will be modified in io_service thread.
    // In order to avoid multithreading reading and writing created_actors_, we also
    // send the `WaitForActorOutOfScope` callback operation to io_service thread.
    std::promise<bool> promise;
    io_service_.post(
        [this, status, &promise]() {
          auto callback = callbacks_.front();
          auto reply = rpc::WaitForActorOutOfScopeReply();
          callback(status, reply);
          promise.set_value(false);
        },
        "test");
    promise.get_future().get();

    callbacks_.pop_front();
    return true;
  }

  std::list<rpc::ClientCallback<rpc::WaitForActorOutOfScopeReply>> callbacks_;
  std::vector<ActorID> killed_actors_;
  instrumented_io_context &io_service_;
};

class GcsActorManagerTest : public ::testing::Test {
 public:
  GcsActorManagerTest()
      : mock_actor_scheduler_(new MockActorScheduler()),
        delayed_to_run_(nullptr),
        delay_(0),
        skip_delay_(true),
        periodical_runner_(io_service_) {
    std::promise<bool> promise;
    thread_io_service_.reset(new std::thread([this, &promise] {
      std::unique_ptr<boost::asio::io_service::work> work(
          new boost::asio::io_service::work(io_service_));
      promise.set_value(true);
      io_service_.run();
    }));
    promise.get_future().get();
    worker_client_ = std::make_shared<MockWorkerClient>(io_service_);
    runtime_env_mgr_ =
        std::make_unique<ray::RuntimeEnvManager>([](auto, auto f) { f(true); });
    std::vector<rpc::ChannelType> channels = {rpc::ChannelType::GCS_ACTOR_CHANNEL};
    auto publisher = std::make_unique<ray::pubsub::Publisher>(
        std::vector<rpc::ChannelType>{
            rpc::ChannelType::GCS_ACTOR_CHANNEL,
        },
        /*periodic_runner=*/&periodical_runner_,
        /*get_time_ms=*/[]() -> double { return absl::ToUnixMicros(absl::Now()); },
        /*subscriber_timeout_ms=*/absl::ToInt64Microseconds(absl::Seconds(30)),
        /*batch_size=*/100);

    gcs_publisher_ = std::make_shared<gcs::GcsPublisher>(std::move(publisher));
    store_client_ = std::make_shared<gcs::InMemoryStoreClient>(io_service_);
    gcs_table_storage_ = std::make_shared<gcs::InMemoryGcsTableStorage>(io_service_);
    kv_ = std::make_unique<gcs::MockInternalKVInterface>();
    function_manager_ = std::make_unique<gcs::GcsFunctionManager>(*kv_);
    gcs_actor_manager_ = std::make_unique<gcs::GcsActorManager>(
        io_service_,
        mock_actor_scheduler_,
        gcs_table_storage_,
        gcs_publisher_,
        *runtime_env_mgr_,
        *function_manager_,
        [](const ActorID &actor_id) {},
        [this](const JobID &job_id) {
          auto job_config = std::make_shared<rpc::JobConfig>();
          job_config->set_ray_namespace(job_namespace_table_[job_id]);
          return job_config;
        },
        [this](std::function<void(void)> fn, boost::posix_time::milliseconds delay) {
          if (skip_delay_) {
            fn();
          } else {
            delay_ = delay;
            delayed_to_run_ = fn;
          }
        },
        [this](const rpc::Address &addr) { return worker_client_; });

    for (int i = 1; i <= 10; i++) {
      auto job_id = JobID::FromInt(i);
      job_namespace_table_[job_id] = "";
    }
  }

  virtual ~GcsActorManagerTest() {
    io_service_.stop();
    thread_io_service_->join();
  }

  void WaitActorCreated(const ActorID &actor_id) {
    auto condition = [this, actor_id]() {
      // The created_actors_ of gcs actor manager will be modified in io_service thread.
      // In order to avoid multithreading reading and writing created_actors_, we also
      // send the read operation to io_service thread.
      std::promise<bool> promise;
      io_service_.post(
          [this, actor_id, &promise]() {
            const auto &created_actors = gcs_actor_manager_->GetCreatedActors();
            for (auto &node_iter : created_actors) {
              for (auto &actor_iter : node_iter.second) {
                if (actor_iter.second == actor_id) {
                  promise.set_value(true);
                  return;
                }
              }
            }
            promise.set_value(false);
          },
          "test");
      return promise.get_future().get();
    };
    EXPECT_TRUE(WaitForCondition(condition, timeout_ms_.count()));
  }

  rpc::Address RandomAddress() const {
    rpc::Address address;
    auto node_id = NodeID::FromRandom();
    auto worker_id = WorkerID::FromRandom();
    address.set_raylet_id(node_id.Binary());
    address.set_worker_id(worker_id.Binary());
    return address;
  }

  std::shared_ptr<gcs::GcsActor> RegisterActor(const JobID &job_id,
                                               int max_restarts = 0,
                                               bool detached = false,
                                               const std::string &name = "",
                                               const std::string &ray_namespace = "") {
    std::promise<std::shared_ptr<gcs::GcsActor>> promise;
    auto request = Mocker::GenRegisterActorRequest(
        job_id, max_restarts, detached, name, ray_namespace);
    // `DestroyActor` triggers some asynchronous operations.
    // If we register an actor after destroying an actor, it may result in multithreading
    // reading and writing the same variable. In order to avoid the problem of
    // multithreading, we put `RegisterActor` to io_service thread.
    io_service_.post(
        [this, request, &promise]() {
          auto status = gcs_actor_manager_->RegisterActor(
              request, [&promise](std::shared_ptr<gcs::GcsActor> actor) {
                promise.set_value(std::move(actor));
              });
          if (!status.ok()) {
            promise.set_value(nullptr);
          }
        },
        "test");
    return promise.get_future().get();
  }

  void OnNodeDead(const NodeID &node_id) {
    std::promise<bool> promise;
    // `OnNodeDead` triggers some asynchronous operations. If we call `OnNodeDead` 2
    // times in succession, the second call may result in multithreading reading and
    // writing the same variable. In order to avoid the problem of multithreading, we put
    // `OnNodeDead` to io_service thread.
    io_service_.post(
        [this, node_id, &promise]() {
          gcs_actor_manager_->OnNodeDead(node_id, "127.0.0.1");
          promise.set_value(true);
        },
        "test");
    promise.get_future().get();
  }

  instrumented_io_context io_service_;
  std::unique_ptr<std::thread> thread_io_service_;
  std::shared_ptr<gcs::StoreClient> store_client_;
  std::shared_ptr<gcs::GcsTableStorage> gcs_table_storage_;
  std::shared_ptr<MockActorScheduler> mock_actor_scheduler_;
  std::shared_ptr<MockWorkerClient> worker_client_;
  absl::flat_hash_map<JobID, std::string> job_namespace_table_;
  std::unique_ptr<gcs::GcsActorManager> gcs_actor_manager_;
  std::shared_ptr<gcs::GcsPublisher> gcs_publisher_;
  std::unique_ptr<ray::RuntimeEnvManager> runtime_env_mgr_;
  const std::chrono::milliseconds timeout_ms_{2000};
  std::function<void(void)> delayed_to_run_;
  boost::posix_time::milliseconds delay_;
  std::unique_ptr<gcs::GcsFunctionManager> function_manager_;
  std::unique_ptr<gcs::MockInternalKVInterface> kv_;
  bool skip_delay_;
  PeriodicalRunner periodical_runner_;
};

TEST_F(GcsActorManagerTest, TestBasic) {
  auto job_id = JobID::FromInt(1);
  auto registered_actor = RegisterActor(job_id);
  rpc::CreateActorRequest create_actor_request;
  create_actor_request.mutable_task_spec()->CopyFrom(
      registered_actor->GetActorTableData().task_spec());

  std::vector<std::shared_ptr<gcs::GcsActor>> finished_actors;
  Status status = gcs_actor_manager_->CreateActor(
      create_actor_request,
      [&finished_actors](const std::shared_ptr<gcs::GcsActor> &actor,
                         const rpc::PushTaskReply &reply) {
        finished_actors.emplace_back(actor);
      });
  RAY_CHECK_OK(status);

  ASSERT_EQ(finished_actors.size(), 0);
  ASSERT_EQ(mock_actor_scheduler_->actors.size(), 1);
  auto actor = mock_actor_scheduler_->actors.back();
  mock_actor_scheduler_->actors.pop_back();

  // Check that the actor is in state `ALIVE`.
  actor->UpdateAddress(RandomAddress());
  gcs_actor_manager_->OnActorCreationSuccess(actor, rpc::PushTaskReply());
  WaitActorCreated(actor->GetActorID());
  ASSERT_EQ(finished_actors.size(), 1);

  ASSERT_TRUE(worker_client_->Reply());
  ASSERT_EQ(actor->GetState(), rpc::ActorTableData::DEAD);
}

TEST_F(GcsActorManagerTest, TestSchedulingFailed) {
  auto job_id = JobID::FromInt(1);
  auto registered_actor = RegisterActor(job_id);
  rpc::CreateActorRequest create_actor_request;
  create_actor_request.mutable_task_spec()->CopyFrom(
      registered_actor->GetActorTableData().task_spec());

  std::vector<std::shared_ptr<gcs::GcsActor>> finished_actors;
  RAY_CHECK_OK(gcs_actor_manager_->CreateActor(
      create_actor_request,
      [&finished_actors](std::shared_ptr<gcs::GcsActor> actor,
                         const rpc::PushTaskReply &reply) {
        finished_actors.emplace_back(actor);
      }));

  ASSERT_EQ(finished_actors.size(), 0);
  ASSERT_EQ(mock_actor_scheduler_->actors.size(), 1);
  auto actor = mock_actor_scheduler_->actors.back();
  mock_actor_scheduler_->actors.clear();

  gcs_actor_manager_->OnActorSchedulingFailed(
      actor, rpc::RequestWorkerLeaseReply::SCHEDULING_FAILED, "");
  gcs_actor_manager_->SchedulePendingActors();
  ASSERT_EQ(mock_actor_scheduler_->actors.size(), 1);
  mock_actor_scheduler_->actors.clear();
  ASSERT_EQ(finished_actors.size(), 0);

  // Check that the actor is in state `ALIVE`.
  actor->UpdateAddress(RandomAddress());
  gcs_actor_manager_->OnActorCreationSuccess(actor, rpc::PushTaskReply());
  WaitActorCreated(actor->GetActorID());
  ASSERT_EQ(finished_actors.size(), 1);
}

TEST_F(GcsActorManagerTest, TestWorkerFailure) {
  auto job_id = JobID::FromInt(1);
  auto registered_actor = RegisterActor(job_id);
  rpc::CreateActorRequest create_actor_request;
  create_actor_request.mutable_task_spec()->CopyFrom(
      registered_actor->GetActorTableData().task_spec());

  std::vector<std::shared_ptr<gcs::GcsActor>> finished_actors;
  RAY_CHECK_OK(gcs_actor_manager_->CreateActor(
      create_actor_request,
      [&finished_actors](std::shared_ptr<gcs::GcsActor> actor,
                         const rpc::PushTaskReply &reply) {
        finished_actors.emplace_back(actor);
      }));

  ASSERT_EQ(finished_actors.size(), 0);
  ASSERT_EQ(mock_actor_scheduler_->actors.size(), 1);
  auto actor = mock_actor_scheduler_->actors.back();
  mock_actor_scheduler_->actors.pop_back();

  // Check that the actor is in state `ALIVE`.
  auto address = RandomAddress();
  auto node_id = NodeID::FromBinary(address.raylet_id());
  auto worker_id = WorkerID::FromBinary(address.worker_id());
  actor->UpdateAddress(address);
  gcs_actor_manager_->OnActorCreationSuccess(actor, rpc::PushTaskReply());
  WaitActorCreated(actor->GetActorID());
  ASSERT_EQ(finished_actors.size(), 1);

  // Killing another worker does not affect this actor.
  EXPECT_CALL(*mock_actor_scheduler_, CancelOnWorker(node_id, _));
  gcs_actor_manager_->OnWorkerDead(node_id, WorkerID::FromRandom());
  ASSERT_EQ(actor->GetState(), rpc::ActorTableData::ALIVE);

  // Remove worker and then check that the actor is dead.
  gcs_actor_manager_->OnWorkerDead(node_id, worker_id);
  ASSERT_EQ(actor->GetState(), rpc::ActorTableData::DEAD);
  ASSERT_TRUE(actor->GetActorTableData().death_cause().has_actor_died_error_context());
  ASSERT_TRUE(absl::StrContains(
      actor->GetActorTableData().death_cause().actor_died_error_context().error_message(),
      "worker process has died."));
  // No more actors to schedule.
  gcs_actor_manager_->SchedulePendingActors();
  ASSERT_EQ(mock_actor_scheduler_->actors.size(), 0);

  ASSERT_TRUE(worker_client_->Reply());
}

TEST_F(GcsActorManagerTest, TestNodeFailure) {
  auto job_id = JobID::FromInt(1);
  auto registered_actor = RegisterActor(job_id);
  rpc::CreateActorRequest create_actor_request;
  create_actor_request.mutable_task_spec()->CopyFrom(
      registered_actor->GetActorTableData().task_spec());

  std::vector<std::shared_ptr<gcs::GcsActor>> finished_actors;
  Status status = gcs_actor_manager_->CreateActor(
      create_actor_request,
      [&finished_actors](std::shared_ptr<gcs::GcsActor> actor,
                         const rpc::PushTaskReply &reply) {
        finished_actors.emplace_back(actor);
      });
  RAY_CHECK_OK(status);

  ASSERT_EQ(finished_actors.size(), 0);
  ASSERT_EQ(mock_actor_scheduler_->actors.size(), 1);
  auto actor = mock_actor_scheduler_->actors.back();
  mock_actor_scheduler_->actors.pop_back();

  // Check that the actor is in state `ALIVE`.
  auto address = RandomAddress();
  actor->UpdateAddress(address);
  gcs_actor_manager_->OnActorCreationSuccess(actor, rpc::PushTaskReply());
  WaitActorCreated(actor->GetActorID());
  ASSERT_EQ(finished_actors.size(), 1);

  // Killing another node does not affect this actor.
  EXPECT_CALL(*mock_actor_scheduler_, CancelOnNode(_));
  OnNodeDead(NodeID::FromRandom());
  ASSERT_EQ(actor->GetState(), rpc::ActorTableData::ALIVE);

  // Remove node and then check that the actor is dead.
  auto node_id = NodeID::FromBinary(address.raylet_id());
  EXPECT_CALL(*mock_actor_scheduler_, CancelOnNode(node_id));

  OnNodeDead(node_id);
  ASSERT_EQ(actor->GetState(), rpc::ActorTableData::DEAD);
  ASSERT_TRUE(actor->GetActorTableData().death_cause().has_actor_died_error_context());
  ASSERT_TRUE(absl::StrContains(
      actor->GetActorTableData().death_cause().actor_died_error_context().error_message(),
      "node has died."));
  // No more actors to schedule.
  gcs_actor_manager_->SchedulePendingActors();
  ASSERT_EQ(mock_actor_scheduler_->actors.size(), 0);

  ASSERT_TRUE(worker_client_->Reply());
}

TEST_F(GcsActorManagerTest, TestActorReconstruction) {
  auto job_id = JobID::FromInt(1);
  auto registered_actor = RegisterActor(job_id,
                                        /*max_restarts=*/1,
                                        /*detached=*/false);
  rpc::CreateActorRequest create_actor_request;
  create_actor_request.mutable_task_spec()->CopyFrom(
      registered_actor->GetActorTableData().task_spec());

  std::vector<std::shared_ptr<gcs::GcsActor>> finished_actors;
  Status status = gcs_actor_manager_->CreateActor(
      create_actor_request,
      [&finished_actors](std::shared_ptr<gcs::GcsActor> actor,
                         const rpc::PushTaskReply &reply) {
        finished_actors.emplace_back(actor);
      });
  RAY_CHECK_OK(status);

  ASSERT_EQ(finished_actors.size(), 0);
  ASSERT_EQ(mock_actor_scheduler_->actors.size(), 1);
  auto actor = mock_actor_scheduler_->actors.back();
  mock_actor_scheduler_->actors.pop_back();

  // Check that the actor is in state `ALIVE`.
  auto address = RandomAddress();
  auto node_id = NodeID::FromBinary(address.raylet_id());
  actor->UpdateAddress(address);
  gcs_actor_manager_->OnActorCreationSuccess(actor, rpc::PushTaskReply());
  WaitActorCreated(actor->GetActorID());
  ASSERT_EQ(finished_actors.size(), 1);

  // Remove worker and then check that the actor is being restarted.
  EXPECT_CALL(*mock_actor_scheduler_, CancelOnNode(node_id));
  OnNodeDead(node_id);
  ASSERT_EQ(actor->GetState(), rpc::ActorTableData::RESTARTING);

  // Add node and check that the actor is restarted.
  gcs_actor_manager_->SchedulePendingActors();
  ASSERT_EQ(mock_actor_scheduler_->actors.size(), 1);
  mock_actor_scheduler_->actors.clear();
  ASSERT_EQ(finished_actors.size(), 1);
  auto node_id2 = NodeID::FromRandom();
  address.set_raylet_id(node_id2.Binary());
  actor->UpdateAddress(address);
  gcs_actor_manager_->OnActorCreationSuccess(actor, rpc::PushTaskReply());
  WaitActorCreated(actor->GetActorID());
  ASSERT_EQ(finished_actors.size(), 1);
  ASSERT_EQ(actor->GetState(), rpc::ActorTableData::ALIVE);
  ASSERT_EQ(actor->GetNodeID(), node_id2);

  // Killing another worker does not affect this actor.
  EXPECT_CALL(*mock_actor_scheduler_, CancelOnNode(_));
  OnNodeDead(NodeID::FromRandom());
  ASSERT_EQ(actor->GetState(), rpc::ActorTableData::ALIVE);

  // Remove worker and then check that the actor is dead.
  EXPECT_CALL(*mock_actor_scheduler_, CancelOnNode(node_id2));
  OnNodeDead(node_id2);
  ASSERT_EQ(actor->GetState(), rpc::ActorTableData::DEAD);
  ASSERT_TRUE(actor->GetActorTableData().death_cause().has_actor_died_error_context());
  ASSERT_TRUE(absl::StrContains(
      actor->GetActorTableData().death_cause().actor_died_error_context().error_message(),
      "node has died."));
  // No more actors to schedule.
  gcs_actor_manager_->SchedulePendingActors();
  ASSERT_EQ(mock_actor_scheduler_->actors.size(), 0);

  ASSERT_TRUE(worker_client_->Reply());
}

TEST_F(GcsActorManagerTest, TestActorRestartWhenOwnerDead) {
  auto job_id = JobID::FromInt(1);
  auto registered_actor = RegisterActor(job_id,
                                        /*max_restarts=*/1,
                                        /*detached=*/false);
  rpc::CreateActorRequest create_actor_request;
  create_actor_request.mutable_task_spec()->CopyFrom(
      registered_actor->GetActorTableData().task_spec());

  std::vector<std::shared_ptr<gcs::GcsActor>> finished_actors;
  RAY_CHECK_OK(gcs_actor_manager_->CreateActor(
      create_actor_request,
      [&finished_actors](std::shared_ptr<gcs::GcsActor> actor,
                         const rpc::PushTaskReply &reply) {
        finished_actors.emplace_back(actor);
      }));

  ASSERT_EQ(finished_actors.size(), 0);
  ASSERT_EQ(mock_actor_scheduler_->actors.size(), 1);
  auto actor = mock_actor_scheduler_->actors.back();
  mock_actor_scheduler_->actors.pop_back();
  const auto owner_node_id = actor->GetOwnerNodeID();

  // Check that the actor is in state `ALIVE`.
  auto address = RandomAddress();
  auto node_id = NodeID::FromBinary(address.raylet_id());
  actor->UpdateAddress(address);
  gcs_actor_manager_->OnActorCreationSuccess(actor, rpc::PushTaskReply());
  WaitActorCreated(actor->GetActorID());
  ASSERT_EQ(finished_actors.size(), 1);

  // Remove the owner's node.
  EXPECT_CALL(*mock_actor_scheduler_, CancelOnNode(owner_node_id));
  OnNodeDead(owner_node_id);
  // The child actor should be marked as dead.
  ASSERT_EQ(actor->GetState(), rpc::ActorTableData::DEAD);
  ASSERT_TRUE(actor->GetActorTableData().death_cause().has_actor_died_error_context());
  ASSERT_TRUE(absl::StrContains(
      actor->GetActorTableData().death_cause().actor_died_error_context().error_message(),
      "owner has died."));
  ASSERT_EQ(worker_client_->killed_actors_.size(), 1);
  ASSERT_EQ(worker_client_->killed_actors_.front(), actor->GetActorID());

  // Remove the actor's node and check that the actor is not restarted, since
  // its owner has died.
  EXPECT_CALL(*mock_actor_scheduler_, CancelOnNode(node_id));
  OnNodeDead(node_id);
  ASSERT_EQ(actor->GetState(), rpc::ActorTableData::DEAD);
  gcs_actor_manager_->SchedulePendingActors();
  ASSERT_TRUE(mock_actor_scheduler_->actors.empty());
}

TEST_F(GcsActorManagerTest, TestDetachedActorRestartWhenCreatorDead) {
  auto job_id = JobID::FromInt(1);
  auto registered_actor = RegisterActor(job_id,
                                        /*max_restarts=*/1,
                                        /*detached=*/true);
  rpc::CreateActorRequest create_actor_request;
  create_actor_request.mutable_task_spec()->CopyFrom(
      registered_actor->GetActorTableData().task_spec());

  std::vector<std::shared_ptr<gcs::GcsActor>> finished_actors;
  RAY_CHECK_OK(gcs_actor_manager_->CreateActor(
      create_actor_request,
      [&finished_actors](std::shared_ptr<gcs::GcsActor> actor,
                         const rpc::PushTaskReply &reply) {
        finished_actors.emplace_back(actor);
      }));

  ASSERT_EQ(finished_actors.size(), 0);
  ASSERT_EQ(mock_actor_scheduler_->actors.size(), 1);
  auto actor = mock_actor_scheduler_->actors.back();
  mock_actor_scheduler_->actors.pop_back();
  const auto owner_node_id = actor->GetOwnerNodeID();

  // Check that the actor is in state `ALIVE`.
  actor->UpdateAddress(RandomAddress());
  gcs_actor_manager_->OnActorCreationSuccess(actor, rpc::PushTaskReply());
  WaitActorCreated(actor->GetActorID());
  ASSERT_EQ(finished_actors.size(), 1);

  // Remove the owner's node.
  EXPECT_CALL(*mock_actor_scheduler_, CancelOnNode(owner_node_id));
  OnNodeDead(owner_node_id);
  // The child actor should not be marked as dead.
  ASSERT_TRUE(worker_client_->killed_actors_.empty());
  ASSERT_EQ(actor->GetState(), rpc::ActorTableData::ALIVE);
}

TEST_F(GcsActorManagerTest, TestActorWithEmptyName) {
  auto job_id = JobID::FromInt(1);

  // Gen `CreateActorRequest` with an empty name.
  // (name,actor_id) => ("", actor_id_1)
  auto request1 = Mocker::GenRegisterActorRequest(job_id,
                                                  /*max_restarts=*/0,
                                                  /*detached=*/true,
                                                  /*name=*/"");
  Status status = gcs_actor_manager_->RegisterActor(
      request1, [](std::shared_ptr<gcs::GcsActor> actor) {});
  // Ensure successful registration.
  ASSERT_TRUE(status.ok());
  // Make sure actor who empty name is not treated as a named actor.
  ASSERT_TRUE(gcs_actor_manager_->GetActorIDByName("", "").IsNil());

  // Gen another `CreateActorRequest` with an empty name.
  // (name,actor_id) => ("", actor_id_2)
  auto request2 = Mocker::GenRegisterActorRequest(job_id,
                                                  /*max_restarts=*/0,
                                                  /*detached=*/true,
                                                  /*name=*/"");
  status = gcs_actor_manager_->RegisterActor(request2,
                                             [](std::shared_ptr<gcs::GcsActor> actor) {});
  // Ensure successful registration.
  ASSERT_TRUE(status.ok());
}

TEST_F(GcsActorManagerTest, TestNamedActors) {
  auto job_id_1 = JobID::FromInt(1);
  auto job_id_2 = JobID::FromInt(2);

  auto request1 = Mocker::GenRegisterActorRequest(job_id_1,
                                                  /*max_restarts=*/0,
                                                  /*detached=*/true,
                                                  /*name=*/"actor1");
  Status status = gcs_actor_manager_->RegisterActor(
      request1, [](std::shared_ptr<gcs::GcsActor> actor) {});
  ASSERT_TRUE(status.ok());
  ASSERT_EQ(gcs_actor_manager_->GetActorIDByName("actor1", "").Binary(),
            request1.task_spec().actor_creation_task_spec().actor_id());

  auto request2 = Mocker::GenRegisterActorRequest(job_id_1,
                                                  /*max_restarts=*/0,
                                                  /*detached=*/true,
                                                  /*name=*/"actor2");
  status = gcs_actor_manager_->RegisterActor(request2,
                                             [](std::shared_ptr<gcs::GcsActor> actor) {});
  ASSERT_TRUE(status.ok());
  ASSERT_EQ(gcs_actor_manager_->GetActorIDByName("actor2", "").Binary(),
            request2.task_spec().actor_creation_task_spec().actor_id());

  // Check that looking up a non-existent name returns ActorID::Nil();
  ASSERT_EQ(gcs_actor_manager_->GetActorIDByName("actor3", ""), ActorID::Nil());

  // Check that naming collisions return Status::Invalid.
  auto request3 = Mocker::GenRegisterActorRequest(job_id_1,
                                                  /*max_restarts=*/0,
                                                  /*detached=*/true,
                                                  /*name=*/"actor2");
  status = gcs_actor_manager_->RegisterActor(request3,
                                             [](std::shared_ptr<gcs::GcsActor> actor) {});
  ASSERT_TRUE(status.IsNotFound());
  ASSERT_EQ(gcs_actor_manager_->GetActorIDByName("actor2", "").Binary(),
            request2.task_spec().actor_creation_task_spec().actor_id());

  // Check that naming collisions are enforced across JobIDs.
  auto request4 = Mocker::GenRegisterActorRequest(job_id_2,
                                                  /*max_restarts=*/0,
                                                  /*detached=*/true,
                                                  /*name=*/"actor2");
  status = gcs_actor_manager_->RegisterActor(request4,
                                             [](std::shared_ptr<gcs::GcsActor> actor) {});
  ASSERT_TRUE(status.IsNotFound());
  ASSERT_EQ(gcs_actor_manager_->GetActorIDByName("actor2", "").Binary(),
            request2.task_spec().actor_creation_task_spec().actor_id());
}

TEST_F(GcsActorManagerTest, TestNamedActorDeletionWorkerFailure) {
  // Make sure named actor deletion succeeds when workers fail.
  const auto actor_name = "actor_to_delete";
  const auto job_id_1 = JobID::FromInt(1);
  auto registered_actor_1 = RegisterActor(job_id_1,
                                          /*max_restarts=*/0,
                                          /*detached=*/true,
                                          /*name=*/actor_name);
  rpc::CreateActorRequest request1;
  request1.mutable_task_spec()->CopyFrom(
      registered_actor_1->GetActorTableData().task_spec());

  Status status = gcs_actor_manager_->CreateActor(
      request1,
      [](std::shared_ptr<gcs::GcsActor> actor, const rpc::PushTaskReply &reply) {});
  ASSERT_TRUE(status.ok());
  ASSERT_EQ(gcs_actor_manager_->GetActorIDByName(actor_name, "").Binary(),
            request1.task_spec().actor_creation_task_spec().actor_id());

  auto actor = mock_actor_scheduler_->actors.back();
  mock_actor_scheduler_->actors.pop_back();

  // Check that the actor is in state `ALIVE`.
  auto address = RandomAddress();
  auto node_id = NodeID::FromBinary(address.raylet_id());
  auto worker_id = WorkerID::FromBinary(address.worker_id());
  actor->UpdateAddress(address);
  gcs_actor_manager_->OnActorCreationSuccess(actor, rpc::PushTaskReply());
  WaitActorCreated(actor->GetActorID());

  // Remove worker and then check that the actor is dead.
  gcs_actor_manager_->OnWorkerDead(node_id, worker_id);
  ASSERT_EQ(actor->GetState(), rpc::ActorTableData::DEAD);
  ASSERT_TRUE(actor->GetActorTableData().death_cause().has_actor_died_error_context());
  ASSERT_TRUE(absl::StrContains(
      actor->GetActorTableData().death_cause().actor_died_error_context().error_message(),
      "worker process has died."));
  ASSERT_EQ(gcs_actor_manager_->GetActorIDByName(actor_name, ""), ActorID::Nil());

  // Create an actor with the same name. This ensures that the name has been properly
  // deleted.
  auto registered_actor_2 = RegisterActor(job_id_1,
                                          /*max_restarts=*/0,
                                          /*detached=*/true,
                                          /*name=*/actor_name);
  rpc::CreateActorRequest request2;
  request2.mutable_task_spec()->CopyFrom(
      registered_actor_2->GetActorTableData().task_spec());

  status = gcs_actor_manager_->CreateActor(
      request2,
      [](std::shared_ptr<gcs::GcsActor> actor, const rpc::PushTaskReply &reply) {});
  ASSERT_TRUE(status.ok());
  ASSERT_EQ(gcs_actor_manager_->GetActorIDByName(actor_name, "").Binary(),
            request2.task_spec().actor_creation_task_spec().actor_id());
}

TEST_F(GcsActorManagerTest, TestNamedActorDeletionNodeFailure) {
  // Make sure named actor deletion succeeds when nodes fail.
  const auto job_id_1 = JobID::FromInt(1);
  auto registered_actor_1 = RegisterActor(job_id_1,
                                          /*max_restarts=*/0,
                                          /*detached=*/true,
                                          /*name=*/"actor");
  rpc::CreateActorRequest request1;
  request1.mutable_task_spec()->CopyFrom(
      registered_actor_1->GetActorTableData().task_spec());

  Status status = gcs_actor_manager_->CreateActor(
      request1,
      [](std::shared_ptr<gcs::GcsActor> actor, const rpc::PushTaskReply &reply) {});
  ASSERT_TRUE(status.ok());
  ASSERT_EQ(gcs_actor_manager_->GetActorIDByName("actor", "").Binary(),
            request1.task_spec().actor_creation_task_spec().actor_id());

  auto actor = mock_actor_scheduler_->actors.back();
  mock_actor_scheduler_->actors.pop_back();

  // Check that the actor is in state `ALIVE`.
  auto address = RandomAddress();
  auto node_id = NodeID::FromBinary(address.raylet_id());
  actor->UpdateAddress(address);
  gcs_actor_manager_->OnActorCreationSuccess(actor, rpc::PushTaskReply());
  WaitActorCreated(actor->GetActorID());

  // Remove node and then check that the actor is dead.
  EXPECT_CALL(*mock_actor_scheduler_, CancelOnNode(node_id));
  OnNodeDead(node_id);
  ASSERT_EQ(actor->GetState(), rpc::ActorTableData::DEAD);
  ASSERT_TRUE(actor->GetActorTableData().death_cause().has_actor_died_error_context());
  ASSERT_TRUE(absl::StrContains(
      actor->GetActorTableData().death_cause().actor_died_error_context().error_message(),
      "node has died."));

  // Create an actor with the same name. This ensures that the name has been properly
  // deleted.
  auto registered_actor_2 = RegisterActor(job_id_1,
                                          /*max_restarts=*/0,
                                          /*detached=*/true,
                                          /*name=*/"actor");
  rpc::CreateActorRequest request2;
  request2.mutable_task_spec()->CopyFrom(
      registered_actor_2->GetActorTableData().task_spec());

  status = gcs_actor_manager_->CreateActor(
      request2,
      [](std::shared_ptr<gcs::GcsActor> actor, const rpc::PushTaskReply &reply) {});
  ASSERT_TRUE(status.ok());
  ASSERT_EQ(gcs_actor_manager_->GetActorIDByName("actor", "").Binary(),
            request2.task_spec().actor_creation_task_spec().actor_id());
}

TEST_F(GcsActorManagerTest, TestNamedActorDeletionNotHappendWhenReconstructed) {
  // Make sure named actor deletion succeeds when nodes fail.
  const auto job_id_1 = JobID::FromInt(1);
  // The dead actor will be reconstructed.
  auto registered_actor_1 = RegisterActor(job_id_1,
                                          /*max_restarts=*/1,
                                          /*detached=*/true,
                                          /*name=*/"actor");
  rpc::CreateActorRequest request1;
  request1.mutable_task_spec()->CopyFrom(
      registered_actor_1->GetActorTableData().task_spec());

  Status status = gcs_actor_manager_->CreateActor(
      request1,
      [](std::shared_ptr<gcs::GcsActor> actor, const rpc::PushTaskReply &reply) {});
  ASSERT_TRUE(status.ok());
  ASSERT_EQ(gcs_actor_manager_->GetActorIDByName("actor", "").Binary(),
            request1.task_spec().actor_creation_task_spec().actor_id());

  auto actor = mock_actor_scheduler_->actors.back();
  mock_actor_scheduler_->actors.pop_back();

  // Check that the actor is in state `ALIVE`.
  auto address = RandomAddress();
  auto node_id = NodeID::FromBinary(address.raylet_id());
  auto worker_id = WorkerID::FromBinary(address.worker_id());
  actor->UpdateAddress(address);
  gcs_actor_manager_->OnActorCreationSuccess(actor, rpc::PushTaskReply());
  WaitActorCreated(actor->GetActorID());

  // Remove worker and then check that the actor is dead. The actor should be
  // reconstructed.
  gcs_actor_manager_->OnWorkerDead(node_id, worker_id);
  ASSERT_EQ(actor->GetState(), rpc::ActorTableData::RESTARTING);

  // Create an actor with the same name.
  // It should fail because actor has been reconstructed, and names shouldn't have been
  // cleaned.
  const auto job_id_2 = JobID::FromInt(2);
  auto request2 = Mocker::GenRegisterActorRequest(job_id_2,
                                                  /*max_restarts=*/0,
                                                  /*detached=*/true,
                                                  /*name=*/"actor");
  status = gcs_actor_manager_->RegisterActor(request2,
                                             [](std::shared_ptr<gcs::GcsActor> actor) {});
  ASSERT_TRUE(status.IsNotFound());
  ASSERT_EQ(gcs_actor_manager_->GetActorIDByName("actor", "").Binary(),
            request1.task_spec().actor_creation_task_spec().actor_id());
}

TEST_F(GcsActorManagerTest, TestDestroyActorBeforeActorCreationCompletes) {
  auto job_id = JobID::FromInt(1);
  auto registered_actor = RegisterActor(job_id);
  rpc::CreateActorRequest create_actor_request;
  create_actor_request.mutable_task_spec()->CopyFrom(
      registered_actor->GetActorTableData().task_spec());

  std::vector<std::shared_ptr<gcs::GcsActor>> finished_actors;
  RAY_CHECK_OK(gcs_actor_manager_->CreateActor(
      create_actor_request,
      [&finished_actors](std::shared_ptr<gcs::GcsActor> actor,
                         const rpc::PushTaskReply &reply) {
        finished_actors.emplace_back(actor);
      }));

  ASSERT_EQ(finished_actors.size(), 0);
  ASSERT_EQ(mock_actor_scheduler_->actors.size(), 1);
  auto actor = mock_actor_scheduler_->actors.back();
  mock_actor_scheduler_->actors.clear();

  // Simulate the reply of WaitForActorOutOfScope request to trigger actor destruction.
  ASSERT_TRUE(worker_client_->Reply());

  // Check that the actor is in state `DEAD`.
  actor->UpdateAddress(RandomAddress());
  gcs_actor_manager_->OnActorCreationSuccess(actor, rpc::PushTaskReply());
  ASSERT_EQ(actor->GetState(), rpc::ActorTableData::DEAD);
  ASSERT_TRUE(actor->GetActorTableData().death_cause().has_actor_died_error_context());
  ASSERT_TRUE(absl::StrContains(
      actor->GetActorTableData().death_cause().actor_died_error_context().error_message(),
      "all references to the actor were removed"));
}

TEST_F(GcsActorManagerTest, TestRaceConditionCancelLease) {
  // Covers a scenario 1 in this PR https://github.com/ray-project/ray/pull/9215.
  auto job_id = JobID::FromInt(1);
  auto registered_actor = RegisterActor(job_id,
                                        /*max_restarts=*/1,
                                        /*detached=*/false);
  rpc::CreateActorRequest create_actor_request;
  create_actor_request.mutable_task_spec()->CopyFrom(
      registered_actor->GetActorTableData().task_spec());

  std::vector<std::shared_ptr<gcs::GcsActor>> finished_actors;
  RAY_CHECK_OK(gcs_actor_manager_->CreateActor(
      create_actor_request,
      [&finished_actors](std::shared_ptr<gcs::GcsActor> actor,
                         const rpc::PushTaskReply &reply) {
        finished_actors.emplace_back(actor);
      }));

  ASSERT_EQ(finished_actors.size(), 0);
  ASSERT_EQ(mock_actor_scheduler_->actors.size(), 1);
  auto actor = mock_actor_scheduler_->actors.back();
  mock_actor_scheduler_->actors.pop_back();
  const auto owner_node_id = actor->GetOwnerNodeID();
  const auto owner_worker_id = actor->GetOwnerID();

  // Check that the actor is in state `ALIVE`.
  rpc::Address address;
  auto node_id = NodeID::FromRandom();
  auto worker_id = WorkerID::FromRandom();
  address.set_raylet_id(node_id.Binary());
  address.set_worker_id(worker_id.Binary());
  actor->UpdateAddress(address);
  const auto &actor_id = actor->GetActorID();
  const auto &task_id =
      TaskID::FromBinary(registered_actor->GetActorTableData().task_spec().task_id());
  EXPECT_CALL(*mock_actor_scheduler_, CancelOnLeasing(node_id, actor_id, task_id));
  gcs_actor_manager_->OnWorkerDead(owner_node_id, owner_worker_id);
  ASSERT_TRUE(actor->GetActorTableData().death_cause().has_actor_died_error_context());
  ASSERT_TRUE(absl::StrContains(
      actor->GetActorTableData().death_cause().actor_died_error_context().error_message(),
      "owner has died."));
}

TEST_F(GcsActorManagerTest, TestRegisterActor) {
  auto job_id = JobID::FromInt(1);
  auto registered_actor = RegisterActor(job_id);
  // Make sure the actor state is `DEPENDENCIES_UNREADY`.
  ASSERT_EQ(registered_actor->GetState(), rpc::ActorTableData::DEPENDENCIES_UNREADY);
  // Make sure the actor has not been scheduled yet.
  ASSERT_TRUE(mock_actor_scheduler_->actors.empty());

  std::vector<std::shared_ptr<gcs::GcsActor>> finished_actors;
  rpc::CreateActorRequest request;
  request.mutable_task_spec()->CopyFrom(
      registered_actor->GetActorTableData().task_spec());
  RAY_CHECK_OK(gcs_actor_manager_->CreateActor(
      request,
      [&finished_actors](std::shared_ptr<gcs::GcsActor> actor,
                         const rpc::PushTaskReply &reply) {
        finished_actors.emplace_back(std::move(actor));
      }));
  // Make sure the actor is scheduling.
  ASSERT_EQ(mock_actor_scheduler_->actors.size(), 1);
  auto actor = mock_actor_scheduler_->actors.back();
  mock_actor_scheduler_->actors.pop_back();
  // Make sure the actor state is `PENDING`.
  ASSERT_EQ(actor->GetState(), rpc::ActorTableData::PENDING_CREATION);

  actor->UpdateAddress(RandomAddress());
  gcs_actor_manager_->OnActorCreationSuccess(actor, rpc::PushTaskReply());
  WaitActorCreated(actor->GetActorID());
  ASSERT_EQ(actor->GetState(), rpc::ActorTableData::ALIVE);
}

TEST_F(GcsActorManagerTest, TestOwnerWorkerDieBeforeActorDependenciesResolved) {
  auto job_id = JobID::FromInt(1);
  auto registered_actor = RegisterActor(job_id);
  const auto &owner_address = registered_actor->GetOwnerAddress();
  auto node_id = NodeID::FromBinary(owner_address.raylet_id());
  auto worker_id = WorkerID::FromBinary(owner_address.worker_id());
  gcs_actor_manager_->OnWorkerDead(node_id, worker_id);
  ASSERT_EQ(registered_actor->GetState(), rpc::ActorTableData::DEAD);
  ASSERT_TRUE(
      registered_actor->GetActorTableData().death_cause().has_actor_died_error_context());
  ASSERT_TRUE(absl::StrContains(registered_actor->GetActorTableData()
                                    .death_cause()
                                    .actor_died_error_context()
                                    .error_message(),
                                "owner has died."));

  // Make sure the actor gets cleaned up.
  const auto &registered_actors = gcs_actor_manager_->GetRegisteredActors();
  ASSERT_FALSE(registered_actors.count(registered_actor->GetActorID()));
  const auto &callbacks = gcs_actor_manager_->GetActorRegisterCallbacks();
  ASSERT_FALSE(callbacks.count(registered_actor->GetActorID()));
}

TEST_F(GcsActorManagerTest, TestOwnerWorkerDieBeforeDetachedActorDependenciesResolved) {
  auto job_id = JobID::FromInt(1);
  auto registered_actor = RegisterActor(job_id, /*max_restarts=*/1, /*detached=*/true);
  const auto &owner_address = registered_actor->GetOwnerAddress();
  auto node_id = NodeID::FromBinary(owner_address.raylet_id());
  auto worker_id = WorkerID::FromBinary(owner_address.worker_id());
  gcs_actor_manager_->OnWorkerDead(node_id, worker_id);
  ASSERT_EQ(registered_actor->GetState(), rpc::ActorTableData::DEAD);
  ASSERT_TRUE(
      registered_actor->GetActorTableData().death_cause().has_actor_died_error_context());
  ASSERT_TRUE(absl::StrContains(registered_actor->GetActorTableData()
                                    .death_cause()
                                    .actor_died_error_context()
                                    .error_message(),
                                "owner has died."));

  // Make sure the actor gets cleaned up.
  const auto &registered_actors = gcs_actor_manager_->GetRegisteredActors();
  ASSERT_FALSE(registered_actors.count(registered_actor->GetActorID()));
  const auto &callbacks = gcs_actor_manager_->GetActorRegisterCallbacks();
  ASSERT_FALSE(callbacks.count(registered_actor->GetActorID()));
}

TEST_F(GcsActorManagerTest, TestOwnerNodeDieBeforeActorDependenciesResolved) {
  auto job_id = JobID::FromInt(1);
  auto registered_actor = RegisterActor(job_id);
  const auto &owner_address = registered_actor->GetOwnerAddress();
  auto node_id = NodeID::FromBinary(owner_address.raylet_id());
  OnNodeDead(node_id);
  ASSERT_EQ(registered_actor->GetState(), rpc::ActorTableData::DEAD);
  ASSERT_TRUE(
      registered_actor->GetActorTableData().death_cause().has_actor_died_error_context());
  ASSERT_TRUE(absl::StrContains(registered_actor->GetActorTableData()
                                    .death_cause()
                                    .actor_died_error_context()
                                    .error_message(),
                                "owner has died."));

  // Make sure the actor gets cleaned up.
  const auto &registered_actors = gcs_actor_manager_->GetRegisteredActors();
  ASSERT_FALSE(registered_actors.count(registered_actor->GetActorID()));
  const auto &callbacks = gcs_actor_manager_->GetActorRegisterCallbacks();
  ASSERT_FALSE(callbacks.count(registered_actor->GetActorID()));
}

TEST_F(GcsActorManagerTest, TestOwnerNodeDieBeforeDetachedActorDependenciesResolved) {
  auto job_id = JobID::FromInt(1);
  auto registered_actor = RegisterActor(job_id, /*max_restarts=*/1, /*detached=*/true);
  const auto &owner_address = registered_actor->GetOwnerAddress();
  auto node_id = NodeID::FromBinary(owner_address.raylet_id());
  OnNodeDead(node_id);
  ASSERT_EQ(registered_actor->GetState(), rpc::ActorTableData::DEAD);
  ASSERT_TRUE(
      registered_actor->GetActorTableData().death_cause().has_actor_died_error_context());
  ASSERT_TRUE(absl::StrContains(registered_actor->GetActorTableData()
                                    .death_cause()
                                    .actor_died_error_context()
                                    .error_message(),
                                "owner has died."));

  // Make sure the actor gets cleaned up.
  const auto &registered_actors = gcs_actor_manager_->GetRegisteredActors();
  ASSERT_FALSE(registered_actors.count(registered_actor->GetActorID()));
  const auto &callbacks = gcs_actor_manager_->GetActorRegisterCallbacks();
  ASSERT_FALSE(callbacks.count(registered_actor->GetActorID()));
}

TEST_F(GcsActorManagerTest, TestOwnerAndChildDiedAtTheSameTimeRaceCondition) {
  // When owner and child die at the same time,
  auto job_id = JobID::FromInt(1);
  auto registered_actor = RegisterActor(job_id,
                                        /*max_restarts=*/1,
                                        /*detached=*/false);
  rpc::CreateActorRequest create_actor_request;
  create_actor_request.mutable_task_spec()->CopyFrom(
      registered_actor->GetActorTableData().task_spec());

  std::vector<std::shared_ptr<gcs::GcsActor>> finished_actors;
  RAY_CHECK_OK(gcs_actor_manager_->CreateActor(
      create_actor_request,
      [&finished_actors](std::shared_ptr<gcs::GcsActor> actor,
                         const rpc::PushTaskReply &reply) {
        finished_actors.emplace_back(actor);
      }));
  auto actor = mock_actor_scheduler_->actors.back();
  mock_actor_scheduler_->actors.pop_back();

  auto address = RandomAddress();
  actor->UpdateAddress(address);
  gcs_actor_manager_->OnActorCreationSuccess(actor, rpc::PushTaskReply());
  WaitActorCreated(actor->GetActorID());
  ASSERT_EQ(finished_actors.size(), 1);

  const auto owner_node_id = actor->GetOwnerNodeID();
  const auto owner_worker_id = actor->GetOwnerID();
  const auto child_node_id = actor->GetNodeID();
  const auto child_worker_id = actor->GetWorkerID();
  const auto actor_id = actor->GetActorID();
  // Make worker & owner fail at the same time, but owner's failure comes first.
  gcs_actor_manager_->OnWorkerDead(owner_node_id, owner_worker_id);
  EXPECT_CALL(*mock_actor_scheduler_, CancelOnWorker(child_node_id, child_worker_id))
      .WillOnce(Return(actor_id));
  gcs_actor_manager_->OnWorkerDead(child_node_id, child_worker_id);
}

TEST_F(GcsActorManagerTest, TestRayNamespace) {
  auto job_id_1 = JobID::FromInt(1);
  auto job_id_2 = JobID::FromInt(20);
  auto job_id_3 = JobID::FromInt(3);
  std::string second_namespace = "another_namespace";
  job_namespace_table_[job_id_2] = second_namespace;

  auto request1 = Mocker::GenRegisterActorRequest(job_id_1,
                                                  /*max_restarts=*/0,
                                                  /*detached=*/true,
                                                  /*name=*/"actor");
  {
    // Create an actor in the empty namespace
    Status status = gcs_actor_manager_->RegisterActor(
        request1, [](std::shared_ptr<gcs::GcsActor> actor) {});
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(gcs_actor_manager_->GetActorIDByName("actor", "").Binary(),
              request1.task_spec().actor_creation_task_spec().actor_id());
  }

  auto request2 = Mocker::GenRegisterActorRequest(job_id_2,
                                                  /*max_restarts=*/0,
                                                  /*detached=*/true,
                                                  /*name=*/"actor");
  {  // Create a second actor of the same name. Its job id belongs to a different
     // namespace though.
    Status status = gcs_actor_manager_->RegisterActor(
        request2, [](std::shared_ptr<gcs::GcsActor> actor) {});
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(gcs_actor_manager_->GetActorIDByName("actor", second_namespace).Binary(),
              request2.task_spec().actor_creation_task_spec().actor_id());
    // The actors may have the same name, but their ids are different.
    ASSERT_NE(gcs_actor_manager_->GetActorIDByName("actor", second_namespace).Binary(),
              request1.task_spec().actor_creation_task_spec().actor_id());
  }

  auto request3 = Mocker::GenRegisterActorRequest(job_id_3,
                                                  /*max_restarts=*/0,
                                                  /*detached=*/true,
                                                  /*name=*/"actor");
  {  // Actors from different jobs, in the same namespace should still collide.
    Status status = gcs_actor_manager_->RegisterActor(
        request3, [](std::shared_ptr<gcs::GcsActor> actor) {});
    ASSERT_TRUE(status.IsNotFound());
    ASSERT_EQ(gcs_actor_manager_->GetActorIDByName("actor", "").Binary(),
              request1.task_spec().actor_creation_task_spec().actor_id());
  }
}

TEST_F(GcsActorManagerTest, TestReuseActorNameInNamespace) {
  std::string actor_name = "actor";
  std::string ray_namespace = "actor_namespace";

  auto job_id_1 = JobID::FromInt(1);
  auto request_1 =
      Mocker::GenRegisterActorRequest(job_id_1, 0, true, actor_name, ray_namespace);
  auto actor_id_1 =
      ActorID::FromBinary(request_1.task_spec().actor_creation_task_spec().actor_id());
  {
    Status status = gcs_actor_manager_->RegisterActor(
        request_1, [](const std::shared_ptr<gcs::GcsActor> &actor) {});
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(gcs_actor_manager_->GetActorIDByName(actor_name, ray_namespace).Binary(),
              actor_id_1.Binary());
  }

  {
    auto owner_address = request_1.task_spec().caller_address();
    auto node_id = NodeID::FromBinary(owner_address.raylet_id());
    gcs_actor_manager_->OnNodeDead(node_id, "");
    ASSERT_EQ(gcs_actor_manager_->GetActorIDByName(actor_name, ray_namespace).Binary(),
              ActorID::Nil().Binary());
  }

  {
    auto job_id_2 = JobID::FromInt(2);
    auto request_2 =
        Mocker::GenRegisterActorRequest(job_id_2, 0, true, actor_name, ray_namespace);
    auto actor_id_2 =
        ActorID::FromBinary(request_2.task_spec().actor_creation_task_spec().actor_id());
    auto status = gcs_actor_manager_->RegisterActor(
        request_2, [](const std::shared_ptr<gcs::GcsActor> &actor) {});
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(gcs_actor_manager_->GetActorIDByName(actor_name, ray_namespace).Binary(),
              actor_id_2.Binary());
  }
}

TEST_F(GcsActorManagerTest, TestActorTableDataDelayedGC) {
  google::protobuf::Arena arena;
  skip_delay_ = false;
  auto job_id_1 = JobID::FromInt(1);
  auto request1 = Mocker::GenRegisterActorRequest(job_id_1,
                                                  /*max_restarts=*/0,
                                                  /*detached=*/false,
                                                  /*name=*/"actor");
  Status status = gcs_actor_manager_->RegisterActor(
      request1, [](std::shared_ptr<gcs::GcsActor> actor) {});
  ASSERT_TRUE(status.ok());
  ASSERT_EQ(gcs_actor_manager_->GetActorIDByName("actor", "").Binary(),
            request1.task_spec().actor_creation_task_spec().actor_id());

  // Simulate the reply of WaitForActorOutOfScope request to trigger actor destruction.
  ASSERT_TRUE(worker_client_->Reply());
  gcs_actor_manager_->OnJobFinished(job_id_1);
  // OnJobFinished work occurs on another thread.
  std::this_thread::sleep_for(std::chrono::seconds(1));

  {
    rpc::GetAllActorInfoRequest request;
    auto &reply =
        *google::protobuf::Arena::CreateMessage<rpc::GetAllActorInfoReply>(&arena);
    bool called = false;
    auto callback = [&called](Status status,
                              std::function<void()> success,
                              std::function<void()> failure) { called = true; };
    gcs_actor_manager_->HandleGetAllActorInfo(request, &reply, callback);

    ASSERT_EQ(reply.actor_table_data().size(), 0);
  }
  {
    rpc::GetAllActorInfoRequest request;
    auto &reply =
        *google::protobuf::Arena::CreateMessage<rpc::GetAllActorInfoReply>(&arena);
    request.set_show_dead_jobs(true);
    std::promise<void> promise;
    auto callback = [&promise](Status status,
                               std::function<void()> success,
                               std::function<void()> failure) { promise.set_value(); };
    gcs_actor_manager_->HandleGetAllActorInfo(request, &reply, callback);
    promise.get_future().get();
    ASSERT_EQ(reply.actor_table_data().size(), 1);
  }
  // Now the entry should be removed from "redis"
  delayed_to_run_();
  {
    rpc::GetAllActorInfoRequest request;
    auto &reply =
        *google::protobuf::Arena::CreateMessage<rpc::GetAllActorInfoReply>(&arena);
    request.set_show_dead_jobs(true);
    std::promise<void> promise;
    auto callback = [&promise](Status status,
                               std::function<void()> success,
                               std::function<void()> failure) { promise.set_value(); };
    gcs_actor_manager_->HandleGetAllActorInfo(request, &reply, callback);
    promise.get_future().get();
    ASSERT_EQ(reply.actor_table_data().size(), 0);
  }
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
