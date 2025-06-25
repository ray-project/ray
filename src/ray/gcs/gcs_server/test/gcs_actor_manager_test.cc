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

#include <list>
#include <memory>
#include <string>
#include <utility>
#include <vector>

// clang-format off
#include "gtest/gtest.h"
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/gcs/gcs_server/test/gcs_server_test_util.h"
#include "ray/gcs/test/gcs_test_util.h"
#include "ray/gcs/gcs_server/gcs_kv_manager.h"
#include "mock/ray/gcs/gcs_server/gcs_kv_manager.h"
#include "mock/ray/gcs/gcs_server/gcs_node_manager.h"
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
  void ReleaseUnusedActorWorkers(
      const absl::flat_hash_map<NodeID, std::vector<WorkerID>> &node_to_workers) {}
  void OnActorDestruction(std::shared_ptr<gcs::GcsActor> actor) {
    const auto &actor_id = actor->GetActorID();
    auto pending_it =
        std::find_if(actors.begin(),
                     actors.end(),
                     [actor_id](const std::shared_ptr<gcs::GcsActor> &actor) {
                       return actor->GetActorID() == actor_id;
                     });
    if (pending_it != actors.end()) {
      actors.erase(pending_it);
    }
  }

  size_t GetPendingActorsCount() const { return 0; }
  bool CancelInFlightActorScheduling(const std::shared_ptr<gcs::GcsActor> &actor) {
    return false;
  }

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
  explicit MockWorkerClient(instrumented_io_context &io_service)
      : io_service_(io_service) {}

  void WaitForActorRefDeleted(
      const rpc::WaitForActorRefDeletedRequest &request,
      const rpc::ClientCallback<rpc::WaitForActorRefDeletedReply> &callback) override {
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
    // send the `WaitForActorRefDeleted` callback operation to io_service thread.
    std::promise<bool> promise;
    io_service_.post(
        [this, status, &promise]() {
          auto callback = callbacks_.front();
          auto reply = rpc::WaitForActorRefDeletedReply();
          callback(status, std::move(reply));
          promise.set_value(false);
        },
        "test");
    promise.get_future().get();

    callbacks_.pop_front();
    return true;
  }

  std::list<rpc::ClientCallback<rpc::WaitForActorRefDeletedReply>> callbacks_;
  std::vector<ActorID> killed_actors_;
  instrumented_io_context &io_service_;
};

// Note: there are a lot of SyncPostAndWait calls in this test. This is because certain
// GcsActorManager methods require to be called on the main io_context. We can't simply
// put the whole test body in a SyncPostAndWait because that would deadlock (we need to
// debug why).
class GcsActorManagerTest : public ::testing::Test {
 public:
  GcsActorManagerTest() : periodical_runner_(PeriodicalRunner::Create(io_service_)) {
    RayConfig::instance().initialize(
        R"(
{
  "maximum_gcs_destroyed_actor_cached_count": 10
}
  )");
    std::promise<bool> promise;
    thread_io_service_.reset(new std::thread([this, &promise] {
      boost::asio::executor_work_guard<boost::asio::io_context::executor_type> work(
          io_service_.get_executor());
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
        /*periodical_runner=*/*periodical_runner_,
        /*get_time_ms=*/[]() -> double { return absl::ToUnixMicros(absl::Now()); },
        /*subscriber_timeout_ms=*/absl::ToInt64Microseconds(absl::Seconds(30)),
        /*batch_size=*/100);

    gcs_publisher_ = std::make_unique<gcs::GcsPublisher>(std::move(publisher));
    store_client_ = std::make_shared<gcs::InMemoryStoreClient>();
    gcs_table_storage_ = std::make_unique<gcs::InMemoryGcsTableStorage>();
    kv_ = std::make_unique<gcs::MockInternalKVInterface>();
    function_manager_ = std::make_unique<gcs::GCSFunctionManager>(*kv_, io_service_);
    auto scheduler = std::make_unique<MockActorScheduler>();
    mock_actor_scheduler_ = scheduler.get();
    worker_client_pool_ = std::make_unique<rpc::CoreWorkerClientPool>(
        [this](const rpc::Address &address) { return worker_client_; });
    gcs_actor_manager_ = std::make_unique<gcs::GcsActorManager>(
        std::move(scheduler),
        gcs_table_storage_.get(),
        io_service_,
        gcs_publisher_.get(),
        *runtime_env_mgr_,
        *function_manager_,
        [](const ActorID &actor_id) {},
        *worker_client_pool_);

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

  std::shared_ptr<gcs::GcsActor> RegisterActor(
      const JobID &job_id,
      int max_restarts = 0,
      bool detached = false,
      const std::string &name = "",
      const std::string &ray_namespace = "test") {
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
              request,
              [&promise](std::shared_ptr<gcs::GcsActor> actor, const Status &status) {
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
    auto node_info = std::make_shared<rpc::GcsNodeInfo>();
    node_info->set_node_id(node_id.Binary());
    io_service_.post(
        [this, node_info, &promise]() {
          gcs_actor_manager_->OnNodeDead(node_info, "127.0.0.1");
          promise.set_value(true);
        },
        "test");
    promise.get_future().get();
  }

  rpc::RestartActorForLineageReconstructionReply RestartActorForLineageReconstruction(
      const ActorID &actor_id, size_t num_restarts_due_to_lineage_reconstruction) {
    rpc::RestartActorForLineageReconstructionRequest request;
    request.set_actor_id(actor_id.Binary());
    request.set_num_restarts_due_to_lineage_reconstruction(
        num_restarts_due_to_lineage_reconstruction);
    rpc::RestartActorForLineageReconstructionReply reply;
    std::promise<bool> promise;
    io_service_.post(
        [this, &request, &reply, &promise]() {
          gcs_actor_manager_->HandleRestartActorForLineageReconstruction(
              request,
              &reply,
              [&promise](Status status,
                         std::function<void()> success,
                         std::function<void()> failure) { promise.set_value(true); });
        },
        "test");
    promise.get_future().get();
    return reply;
  }

  void ReportActorOutOfScope(const ActorID &actor_id,
                             size_t num_restarts_due_to_lineage_reconstrcution) {
    rpc::ReportActorOutOfScopeRequest request;
    request.set_actor_id(actor_id.Binary());
    request.set_num_restarts_due_to_lineage_reconstruction(
        num_restarts_due_to_lineage_reconstrcution);
    rpc::ReportActorOutOfScopeReply reply;
    std::promise<bool> promise;
    io_service_.post(
        [this, &request, &reply, &promise]() {
          gcs_actor_manager_->HandleReportActorOutOfScope(
              request,
              &reply,
              [&promise](Status status,
                         std::function<void()> success,
                         std::function<void()> failure) { promise.set_value(true); });
        },
        "test");
    promise.get_future().get();
  }

  std::shared_ptr<gcs::GcsActor> CreateActorAndWaitTilAlive(const JobID &job_id) {
    auto registered_actor = RegisterActor(job_id);
    rpc::CreateActorRequest create_actor_request;
    create_actor_request.mutable_task_spec()->CopyFrom(
        registered_actor->GetCreationTaskSpecification().GetMessage());
    std::vector<std::shared_ptr<gcs::GcsActor>> finished_actors;
    Status status = gcs_actor_manager_->CreateActor(
        create_actor_request,
        [&finished_actors](const std::shared_ptr<gcs::GcsActor> &actor,
                           const rpc::PushTaskReply &reply,
                           const Status &status) {
          finished_actors.emplace_back(actor);
        });

    auto actor = mock_actor_scheduler_->actors.back();
    mock_actor_scheduler_->actors.pop_back();

    // Check that the actor is in state `ALIVE`.
    actor->UpdateAddress(RandomAddress());
    gcs_actor_manager_->OnActorCreationSuccess(actor, rpc::PushTaskReply());
    WaitActorCreated(actor->GetActorID());
    RAY_CHECK_EQ(gcs_actor_manager_->CountFor(rpc::ActorTableData::ALIVE, ""), 1);
    RAY_CHECK_EQ(actor->GetState(), rpc::ActorTableData::ALIVE);
    return actor;
  }

  instrumented_io_context io_service_;
  std::unique_ptr<std::thread> thread_io_service_;
  std::shared_ptr<gcs::StoreClient> store_client_;
  std::shared_ptr<gcs::GcsTableStorage> gcs_table_storage_;
  // Actor scheduler's ownership lies in actor manager.
  MockActorScheduler *mock_actor_scheduler_ = nullptr;
  std::shared_ptr<MockWorkerClient> worker_client_;
  std::unique_ptr<rpc::CoreWorkerClientPool> worker_client_pool_;
  absl::flat_hash_map<JobID, std::string> job_namespace_table_;
  std::unique_ptr<gcs::GcsActorManager> gcs_actor_manager_;
  std::shared_ptr<gcs::GcsPublisher> gcs_publisher_;
  std::unique_ptr<ray::RuntimeEnvManager> runtime_env_mgr_;
  const std::chrono::milliseconds timeout_ms_{2000};
  absl::Mutex mutex_;
  std::unique_ptr<gcs::GCSFunctionManager> function_manager_;
  std::unique_ptr<gcs::MockInternalKVInterface> kv_;
  std::shared_ptr<PeriodicalRunner> periodical_runner_;
};

TEST_F(GcsActorManagerTest, TestBasic) {
  auto job_id = JobID::FromInt(1);
  auto registered_actor = RegisterActor(job_id);
  rpc::CreateActorRequest create_actor_request;
  create_actor_request.mutable_task_spec()->CopyFrom(
      registered_actor->GetCreationTaskSpecification().GetMessage());
  RAY_CHECK_EQ(
      gcs_actor_manager_->CountFor(rpc::ActorTableData::DEPENDENCIES_UNREADY, ""), 1);

  std::vector<std::shared_ptr<gcs::GcsActor>> finished_actors;
  Status status = gcs_actor_manager_->CreateActor(
      create_actor_request,
      [&finished_actors](const std::shared_ptr<gcs::GcsActor> &actor,
                         const rpc::PushTaskReply &reply,
                         const Status &status) { finished_actors.emplace_back(actor); });
  RAY_CHECK_OK(status);
  RAY_CHECK_EQ(gcs_actor_manager_->CountFor(rpc::ActorTableData::PENDING_CREATION, ""),
               1);

  ASSERT_EQ(finished_actors.size(), 0);
  ASSERT_EQ(mock_actor_scheduler_->actors.size(), 1);
  auto actor = mock_actor_scheduler_->actors.back();
  mock_actor_scheduler_->actors.pop_back();

  // Check that the actor is in state `ALIVE`.
  actor->UpdateAddress(RandomAddress());
  gcs_actor_manager_->OnActorCreationSuccess(actor, rpc::PushTaskReply());
  WaitActorCreated(actor->GetActorID());
  ASSERT_EQ(finished_actors.size(), 1);
  RAY_CHECK_EQ(gcs_actor_manager_->CountFor(rpc::ActorTableData::ALIVE, ""), 1);

  ASSERT_TRUE(worker_client_->Reply());
  ASSERT_EQ(actor->GetState(), rpc::ActorTableData::DEAD);
  RAY_CHECK_EQ(gcs_actor_manager_->CountFor(rpc::ActorTableData::ALIVE, ""), 0);
  RAY_CHECK_EQ(gcs_actor_manager_->CountFor(rpc::ActorTableData::DEAD, ""), 1);
}

TEST_F(GcsActorManagerTest, TestDeadCount) {
  ///
  /// Verify the DEAD count is correct after actors are GC'ed from the GCS.
  /// Actors are GC'ed from the GCS when there are more than
  /// maximum_gcs_destroyed_actor_cached_count dead actors.
  ///

  // Make sure we can cache only up to 10 dead actors.
  ASSERT_EQ(RayConfig::instance().maximum_gcs_destroyed_actor_cached_count(), 10);
  auto job_id = JobID::FromInt(1);

  // Create 20 actors.
  for (int i = 0; i < 20; i++) {
    auto registered_actor = RegisterActor(job_id);
    rpc::CreateActorRequest create_actor_request;
    create_actor_request.mutable_task_spec()->CopyFrom(
        registered_actor->GetCreationTaskSpecification().GetMessage());

    Status status =
        gcs_actor_manager_->CreateActor(create_actor_request,
                                        [](const std::shared_ptr<gcs::GcsActor> &actor,
                                           const rpc::PushTaskReply &reply,
                                           const Status &status) {});
    RAY_CHECK_OK(status);
    auto actor = mock_actor_scheduler_->actors.back();
    mock_actor_scheduler_->actors.pop_back();
    // Check that the actor is in state `ALIVE`.
    actor->UpdateAddress(RandomAddress());
    gcs_actor_manager_->OnActorCreationSuccess(actor, rpc::PushTaskReply());
    WaitActorCreated(actor->GetActorID());
    // Actor is killed.
    ASSERT_TRUE(worker_client_->Reply());
    ASSERT_EQ(actor->GetState(), rpc::ActorTableData::DEAD);
  }
  RAY_CHECK_EQ(gcs_actor_manager_->CountFor(rpc::ActorTableData::DEAD, ""), 20);
}

TEST_F(GcsActorManagerTest, TestSchedulingFailed) {
  auto job_id = JobID::FromInt(1);
  auto registered_actor = RegisterActor(job_id);
  rpc::CreateActorRequest create_actor_request;
  create_actor_request.mutable_task_spec()->CopyFrom(
      registered_actor->GetCreationTaskSpecification().GetMessage());

  std::vector<std::shared_ptr<gcs::GcsActor>> finished_actors;
  RAY_CHECK_OK(gcs_actor_manager_->CreateActor(
      create_actor_request,
      [&finished_actors](std::shared_ptr<gcs::GcsActor> actor,
                         const rpc::PushTaskReply &reply,
                         const Status &status) { finished_actors.emplace_back(actor); }));

  ASSERT_EQ(finished_actors.size(), 0);
  ASSERT_EQ(mock_actor_scheduler_->actors.size(), 1);
  auto actor = mock_actor_scheduler_->actors.back();
  mock_actor_scheduler_->actors.clear();

  SyncPostAndWait(io_service_, "TestSchedulingFailed", [&]() {
    gcs_actor_manager_->OnActorSchedulingFailed(
        actor,
        rpc::RequestWorkerLeaseReply::SCHEDULING_CANCELLED_RUNTIME_ENV_SETUP_FAILED,
        "");
  });
  ASSERT_EQ(mock_actor_scheduler_->actors.size(), 0);
}

TEST_F(GcsActorManagerTest, TestWorkerFailure) {
  auto job_id = JobID::FromInt(1);
  auto registered_actor = RegisterActor(job_id);
  rpc::CreateActorRequest create_actor_request;
  create_actor_request.mutable_task_spec()->CopyFrom(
      registered_actor->GetCreationTaskSpecification().GetMessage());

  std::vector<std::shared_ptr<gcs::GcsActor>> finished_actors;
  RAY_CHECK_OK(gcs_actor_manager_->CreateActor(
      create_actor_request,
      [&finished_actors](std::shared_ptr<gcs::GcsActor> actor,
                         const rpc::PushTaskReply &reply,
                         const Status &status) { finished_actors.emplace_back(actor); }));

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
  ASSERT_EQ(mock_actor_scheduler_->actors.size(), 0);

  ASSERT_TRUE(worker_client_->Reply());
}

TEST_F(GcsActorManagerTest, TestNodeFailure) {
  auto job_id = JobID::FromInt(1);
  auto registered_actor = RegisterActor(job_id);
  rpc::CreateActorRequest create_actor_request;
  create_actor_request.mutable_task_spec()->CopyFrom(
      registered_actor->GetCreationTaskSpecification().GetMessage());

  std::vector<std::shared_ptr<gcs::GcsActor>> finished_actors;
  Status status = gcs_actor_manager_->CreateActor(
      create_actor_request,
      [&finished_actors](std::shared_ptr<gcs::GcsActor> actor,
                         const rpc::PushTaskReply &reply,
                         const Status &status) { finished_actors.emplace_back(actor); });
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
      registered_actor->GetCreationTaskSpecification().GetMessage());

  std::vector<std::shared_ptr<gcs::GcsActor>> finished_actors;
  Status status = gcs_actor_manager_->CreateActor(
      create_actor_request,
      [&finished_actors](std::shared_ptr<gcs::GcsActor> actor,
                         const rpc::PushTaskReply &reply,
                         const Status &status) { finished_actors.emplace_back(actor); });
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
      registered_actor->GetCreationTaskSpecification().GetMessage());

  std::vector<std::shared_ptr<gcs::GcsActor>> finished_actors;
  RAY_CHECK_OK(gcs_actor_manager_->CreateActor(
      create_actor_request,
      [&finished_actors](std::shared_ptr<gcs::GcsActor> actor,
                         const rpc::PushTaskReply &reply,
                         const Status &status) { finished_actors.emplace_back(actor); }));

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
  ASSERT_TRUE(mock_actor_scheduler_->actors.empty());
}

TEST_F(GcsActorManagerTest, TestDetachedActorRestartWhenCreatorDead) {
  auto job_id = JobID::FromInt(1);
  auto registered_actor = RegisterActor(job_id,
                                        /*max_restarts=*/1,
                                        /*detached=*/true);
  rpc::CreateActorRequest create_actor_request;
  create_actor_request.mutable_task_spec()->CopyFrom(
      registered_actor->GetCreationTaskSpecification().GetMessage());

  std::vector<std::shared_ptr<gcs::GcsActor>> finished_actors;
  RAY_CHECK_OK(gcs_actor_manager_->CreateActor(
      create_actor_request,
      [&finished_actors](std::shared_ptr<gcs::GcsActor> actor,
                         const rpc::PushTaskReply &reply,
                         const Status &status) { finished_actors.emplace_back(actor); }));

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

  Status status = SyncPostAndWait(io_service_, "TestActorWithEmptyName", [&]() {
    return gcs_actor_manager_->RegisterActor(
        request1, [](std::shared_ptr<gcs::GcsActor> actor, const Status &status) {});
  });

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
  status = SyncPostAndWait(io_service_, "TestActorWithEmptyName", [&]() {
    return gcs_actor_manager_->RegisterActor(
        request2, [](std::shared_ptr<gcs::GcsActor> actor, const Status &status) {});
  });
  // Ensure successful registration.
  ASSERT_TRUE(status.ok());
}

TEST_F(GcsActorManagerTest, TestNamedActors) {
  auto job_id_1 = JobID::FromInt(1);
  auto job_id_2 = JobID::FromInt(2);

  auto request1 = Mocker::GenRegisterActorRequest(job_id_1,
                                                  /*max_restarts=*/0,
                                                  /*detached=*/true,
                                                  /*name=*/"actor1",
                                                  /*ray_namesapce=*/"test_named_actor");
  Status status = SyncPostAndWait(io_service_, "TestNamedActors", [&]() {
    return gcs_actor_manager_->RegisterActor(
        request1, [](std::shared_ptr<gcs::GcsActor> actor, const Status &status) {});
  });
  ASSERT_TRUE(status.ok());
  ASSERT_EQ(gcs_actor_manager_->GetActorIDByName("actor1", "test_named_actor").Binary(),
            request1.task_spec().actor_creation_task_spec().actor_id());

  auto request2 = Mocker::GenRegisterActorRequest(job_id_1,
                                                  /*max_restarts=*/0,
                                                  /*detached=*/true,
                                                  /*name=*/"actor2",
                                                  /*ray_namesapce=*/"test_named_actor");
  status = SyncPostAndWait(io_service_, "TestNamedActors", [&]() {
    return gcs_actor_manager_->RegisterActor(
        request2, [](std::shared_ptr<gcs::GcsActor> actor, const Status &status) {});
  });
  ASSERT_TRUE(status.ok());
  ASSERT_EQ(gcs_actor_manager_->GetActorIDByName("actor2", "test_named_actor").Binary(),
            request2.task_spec().actor_creation_task_spec().actor_id());

  // Check that looking up a non-existent name returns ActorID::Nil();
  ASSERT_EQ(gcs_actor_manager_->GetActorIDByName("actor3", "test_named_actor"),
            ActorID::Nil());

  // Check that naming collisions return Status::AlreadyExists.
  auto request3 = Mocker::GenRegisterActorRequest(job_id_1,
                                                  /*max_restarts=*/0,
                                                  /*detached=*/true,
                                                  /*name=*/"actor2",
                                                  /*ray_namesapce=*/"test_named_actor");
  status = SyncPostAndWait(io_service_, "TestNamedActors", [&]() {
    return gcs_actor_manager_->RegisterActor(
        request3, [](std::shared_ptr<gcs::GcsActor> actor, const Status &status) {});
  });
  ASSERT_TRUE(status.IsAlreadyExists());
  ASSERT_EQ(gcs_actor_manager_->GetActorIDByName("actor2", "test_named_actor").Binary(),
            request2.task_spec().actor_creation_task_spec().actor_id());

  // Check that naming collisions are enforced across JobIDs.
  auto request4 = Mocker::GenRegisterActorRequest(job_id_2,
                                                  /*max_restarts=*/0,
                                                  /*detached=*/true,
                                                  /*name=*/"actor2",
                                                  /*ray_namesapce=*/"test_named_actor");
  status = SyncPostAndWait(io_service_, "TestNamedActors", [&]() {
    return gcs_actor_manager_->RegisterActor(
        request4, [](std::shared_ptr<gcs::GcsActor> actor, const Status &status) {});
  });
  ASSERT_TRUE(status.IsAlreadyExists());
  ASSERT_EQ(gcs_actor_manager_->GetActorIDByName("actor2", "test_named_actor").Binary(),
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
      registered_actor_1->GetCreationTaskSpecification().GetMessage());

  Status status = gcs_actor_manager_->CreateActor(request1,
                                                  [](std::shared_ptr<gcs::GcsActor> actor,
                                                     const rpc::PushTaskReply &reply,
                                                     const Status &status) {});
  ASSERT_TRUE(status.ok());
  ASSERT_EQ(gcs_actor_manager_->GetActorIDByName(actor_name, "test").Binary(),
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
  ASSERT_EQ(gcs_actor_manager_->GetActorIDByName(actor_name, "test"), ActorID::Nil());

  // Create an actor with the same name. This ensures that the name has been properly
  // deleted.
  auto registered_actor_2 = RegisterActor(job_id_1,
                                          /*max_restarts=*/0,
                                          /*detached=*/true,
                                          /*name=*/actor_name);
  rpc::CreateActorRequest request2;
  request2.mutable_task_spec()->CopyFrom(
      registered_actor_2->GetCreationTaskSpecification().GetMessage());

  status = gcs_actor_manager_->CreateActor(request2,
                                           [](std::shared_ptr<gcs::GcsActor> actor,
                                              const rpc::PushTaskReply &reply,
                                              const Status &status) {});
  ASSERT_TRUE(status.ok());
  ASSERT_EQ(gcs_actor_manager_->GetActorIDByName(actor_name, "test").Binary(),
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
      registered_actor_1->GetCreationTaskSpecification().GetMessage());

  Status status = gcs_actor_manager_->CreateActor(request1,
                                                  [](std::shared_ptr<gcs::GcsActor> actor,
                                                     const rpc::PushTaskReply &reply,
                                                     const Status &status) {});
  ASSERT_TRUE(status.ok());
  ASSERT_EQ(gcs_actor_manager_->GetActorIDByName("actor", "test").Binary(),
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
      registered_actor_2->GetCreationTaskSpecification().GetMessage());

  status = gcs_actor_manager_->CreateActor(request2,
                                           [](std::shared_ptr<gcs::GcsActor> actor,
                                              const rpc::PushTaskReply &reply,
                                              const Status &status) {});
  ASSERT_TRUE(status.ok());
  ASSERT_EQ(gcs_actor_manager_->GetActorIDByName("actor", "test").Binary(),
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
      registered_actor_1->GetCreationTaskSpecification().GetMessage());

  Status status = gcs_actor_manager_->CreateActor(request1,
                                                  [](std::shared_ptr<gcs::GcsActor> actor,
                                                     const rpc::PushTaskReply &reply,
                                                     const Status &status) {});
  ASSERT_TRUE(status.ok());
  ASSERT_EQ(gcs_actor_manager_->GetActorIDByName("actor", "test").Binary(),
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
  status = SyncPostAndWait(io_service_, "TestNamedActors", [&]() {
    return gcs_actor_manager_->RegisterActor(
        request2, [](std::shared_ptr<gcs::GcsActor> actor, const Status &status) {});
  });
  ASSERT_TRUE(status.IsAlreadyExists());
  ASSERT_EQ(gcs_actor_manager_->GetActorIDByName("actor", "test").Binary(),
            request1.task_spec().actor_creation_task_spec().actor_id());
}

TEST_F(GcsActorManagerTest, TestDestroyActorBeforeActorCreationCompletes) {
  auto job_id = JobID::FromInt(1);
  auto registered_actor = RegisterActor(job_id);
  rpc::CreateActorRequest create_actor_request;
  create_actor_request.mutable_task_spec()->CopyFrom(
      registered_actor->GetCreationTaskSpecification().GetMessage());

  std::vector<std::shared_ptr<gcs::GcsActor>> finished_actors;
  RAY_CHECK_OK(gcs_actor_manager_->CreateActor(
      create_actor_request,
      [&finished_actors](std::shared_ptr<gcs::GcsActor> actor,
                         const rpc::PushTaskReply &reply,
                         const Status &status) { finished_actors.emplace_back(actor); }));

  ASSERT_EQ(finished_actors.size(), 0);
  ASSERT_EQ(mock_actor_scheduler_->actors.size(), 1);
  auto actor = mock_actor_scheduler_->actors.back();
  mock_actor_scheduler_->actors.clear();

  // Simulate the reply of WaitForActorRefDeleted request to trigger actor destruction.
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
      registered_actor->GetCreationTaskSpecification().GetMessage());

  std::vector<std::shared_ptr<gcs::GcsActor>> finished_actors;
  RAY_CHECK_OK(gcs_actor_manager_->CreateActor(
      create_actor_request,
      [&finished_actors](std::shared_ptr<gcs::GcsActor> actor,
                         const rpc::PushTaskReply &reply,
                         const Status &status) { finished_actors.emplace_back(actor); }));

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
  const auto &task_id = TaskID::FromBinary(
      registered_actor->GetCreationTaskSpecification().GetMessage().task_id());
  EXPECT_CALL(*mock_actor_scheduler_, CancelOnLeasing(node_id, actor_id, task_id));
  SyncPostAndWait(io_service_, "TestRaceConditionCancelLease", [&]() {
    gcs_actor_manager_->OnWorkerDead(owner_node_id, owner_worker_id);
  });
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
      registered_actor->GetCreationTaskSpecification().GetMessage());
  RAY_CHECK_OK(gcs_actor_manager_->CreateActor(
      request,
      [&finished_actors](std::shared_ptr<gcs::GcsActor> actor,
                         const rpc::PushTaskReply &reply,
                         const Status &status) {
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
  SyncPostAndWait(io_service_,
                  "TestOwnerWorkerDieBeforeActorDependenciesResolved",
                  [&]() { gcs_actor_manager_->OnWorkerDead(node_id, worker_id); });
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
  SyncPostAndWait(
      io_service_, "TestOwnerWorkerDieBeforeActorDependenciesResolved", [&]() {
        const auto &callbacks = gcs_actor_manager_->GetActorRegisterCallbacks();
        ASSERT_FALSE(callbacks.count(registered_actor->GetActorID()));
      });
}

TEST_F(GcsActorManagerTest, TestOwnerWorkerDieBeforeDetachedActorDependenciesResolved) {
  auto job_id = JobID::FromInt(1);
  auto registered_actor = RegisterActor(job_id, /*max_restarts=*/1, /*detached=*/true);
  const auto &owner_address = registered_actor->GetOwnerAddress();
  auto node_id = NodeID::FromBinary(owner_address.raylet_id());
  auto worker_id = WorkerID::FromBinary(owner_address.worker_id());
  SyncPostAndWait(io_service_,
                  "TestOwnerWorkerDieBeforeDetachedActorDependenciesResolved",
                  [&]() { gcs_actor_manager_->OnWorkerDead(node_id, worker_id); });
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
  SyncPostAndWait(
      io_service_, "TestOwnerWorkerDieBeforeDetachedActorDependenciesResolved", [&]() {
        const auto &callbacks = gcs_actor_manager_->GetActorRegisterCallbacks();
        ASSERT_FALSE(callbacks.count(registered_actor->GetActorID()));
      });
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
  SyncPostAndWait(io_service_, "TestOwnerNodeDieBeforeActorDependenciesResolved", [&]() {
    const auto &callbacks = gcs_actor_manager_->GetActorRegisterCallbacks();
    ASSERT_FALSE(callbacks.count(registered_actor->GetActorID()));
  });
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
  SyncPostAndWait(
      io_service_, "TestOwnerNodeDieBeforeDetachedActorDependenciesResolved", [&]() {
        const auto &callbacks = gcs_actor_manager_->GetActorRegisterCallbacks();
        ASSERT_FALSE(callbacks.count(registered_actor->GetActorID()));
      });
}

TEST_F(GcsActorManagerTest, TestOwnerAndChildDiedAtTheSameTimeRaceCondition) {
  // When owner and child die at the same time,
  auto job_id = JobID::FromInt(1);
  auto registered_actor = RegisterActor(job_id,
                                        /*max_restarts=*/1,
                                        /*detached=*/false);
  rpc::CreateActorRequest create_actor_request;
  create_actor_request.mutable_task_spec()->CopyFrom(
      registered_actor->GetCreationTaskSpecification().GetMessage());

  std::vector<std::shared_ptr<gcs::GcsActor>> finished_actors;
  RAY_CHECK_OK(gcs_actor_manager_->CreateActor(
      create_actor_request,
      [&finished_actors](std::shared_ptr<gcs::GcsActor> actor,
                         const rpc::PushTaskReply &reply,
                         const Status &status) { finished_actors.emplace_back(actor); }));
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
  SyncPostAndWait(io_service_, "TestOwnerAndChildDiedAtTheSameTimeRaceCondition", [&]() {
    gcs_actor_manager_->OnWorkerDead(owner_node_id, owner_worker_id);
    EXPECT_CALL(*mock_actor_scheduler_, CancelOnWorker(child_node_id, child_worker_id))
        .WillOnce(Return(actor_id));
    gcs_actor_manager_->OnWorkerDead(child_node_id, child_worker_id);
  });
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
  SyncPostAndWait(io_service_, "TestRayNamespace", [&]() {
    Status status = gcs_actor_manager_->RegisterActor(
        request1, [](std::shared_ptr<gcs::GcsActor> actor, const Status &status) {});
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(gcs_actor_manager_->GetActorIDByName("actor", "test").Binary(),
              request1.task_spec().actor_creation_task_spec().actor_id());
  });

  auto request2 = Mocker::GenRegisterActorRequest(job_id_2,
                                                  /*max_restarts=*/0,
                                                  /*detached=*/true,
                                                  /*name=*/"actor",
                                                  second_namespace);
  SyncPostAndWait(io_service_, "TestRayNamespace", [&]() {
    // Create a second actor of the same name. Its job id belongs to a different
    // namespace though.
    Status status = gcs_actor_manager_->RegisterActor(
        request2, [](std::shared_ptr<gcs::GcsActor> actor, const Status &status) {});
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(gcs_actor_manager_->GetActorIDByName("actor", second_namespace).Binary(),
              request2.task_spec().actor_creation_task_spec().actor_id());
    // The actors may have the same name, but their ids are different.
    ASSERT_NE(gcs_actor_manager_->GetActorIDByName("actor", second_namespace).Binary(),
              request1.task_spec().actor_creation_task_spec().actor_id());
  });

  auto request3 = Mocker::GenRegisterActorRequest(job_id_3,
                                                  /*max_restarts=*/0,
                                                  /*detached=*/true,
                                                  /*name=*/"actor",
                                                  /*ray_namespace=*/"test");
  SyncPostAndWait(io_service_, "TestRayNamespace", [&]() {
    Status status = gcs_actor_manager_->RegisterActor(
        request3, [](std::shared_ptr<gcs::GcsActor> actor, const Status &status) {});
    ASSERT_TRUE(status.IsAlreadyExists());
    ASSERT_EQ(gcs_actor_manager_->GetActorIDByName("actor", "test").Binary(),
              request1.task_spec().actor_creation_task_spec().actor_id());
  });
}

TEST_F(GcsActorManagerTest, TestReuseActorNameInNamespace) {
  std::string actor_name = "actor";
  std::string ray_namespace = "actor_namespace";

  auto job_id_1 = JobID::FromInt(1);
  auto request_1 =
      Mocker::GenRegisterActorRequest(job_id_1, 0, true, actor_name, ray_namespace);
  auto actor_id_1 =
      ActorID::FromBinary(request_1.task_spec().actor_creation_task_spec().actor_id());
  SyncPostAndWait(io_service_, "TestReuseActorNameInNamespace", [&]() {
    Status status = gcs_actor_manager_->RegisterActor(
        request_1,
        [](const std::shared_ptr<gcs::GcsActor> &actor, const Status &status) {});
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(gcs_actor_manager_->GetActorIDByName(actor_name, ray_namespace).Binary(),
              actor_id_1.Binary());
  });

  SyncPostAndWait(io_service_, "TestReuseActorNameInNamespace", [&]() {
    auto owner_address = request_1.task_spec().caller_address();
    auto node_info = std::make_shared<rpc::GcsNodeInfo>();
    node_info->set_node_id(owner_address.raylet_id());
    gcs_actor_manager_->OnNodeDead(node_info, "");
    ASSERT_EQ(gcs_actor_manager_->GetActorIDByName(actor_name, ray_namespace).Binary(),
              ActorID::Nil().Binary());
  });

  SyncPostAndWait(io_service_, "TestReuseActorNameInNamespace", [&]() {
    auto job_id_2 = JobID::FromInt(2);
    auto request_2 =
        Mocker::GenRegisterActorRequest(job_id_2, 0, true, actor_name, ray_namespace);
    auto actor_id_2 =
        ActorID::FromBinary(request_2.task_spec().actor_creation_task_spec().actor_id());
    auto status = gcs_actor_manager_->RegisterActor(
        request_2,
        [](const std::shared_ptr<gcs::GcsActor> &actor, const Status &status) {});
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(gcs_actor_manager_->GetActorIDByName(actor_name, ray_namespace).Binary(),
              actor_id_2.Binary());
  });
}

TEST_F(GcsActorManagerTest, TestGetAllActorInfoFilters) {
  google::protobuf::Arena arena;
  // The target filter actor.
  auto job_id = JobID::FromInt(1);
  auto actor = CreateActorAndWaitTilAlive(job_id);

  // Just register some other actors.
  auto job_id_other = JobID::FromInt(2);
  auto num_other_actors = 3;
  for (int i = 0; i < num_other_actors; i++) {
    auto request1 = Mocker::GenRegisterActorRequest(job_id_other,
                                                    /*max_restarts=*/0,
                                                    /*detached=*/false);
    SyncPostAndWait(io_service_, "TestGetAllActorInfoFilters", [&]() {
      Status status = gcs_actor_manager_->RegisterActor(
          request1, [](std::shared_ptr<gcs::GcsActor> actor, const Status &status) {});
      ASSERT_TRUE(status.ok());
    });
  }

  auto callback =
      [](Status status, std::function<void()> success, std::function<void()> failure) {};
  // Filter with actor id
  {
    rpc::GetAllActorInfoRequest request;
    request.mutable_filters()->set_actor_id(actor->GetActorID().Binary());

    auto &reply =
        *google::protobuf::Arena::CreateMessage<rpc::GetAllActorInfoReply>(&arena);
    gcs_actor_manager_->HandleGetAllActorInfo(request, &reply, callback);
    ASSERT_EQ(reply.actor_table_data().size(), 1);
    ASSERT_EQ(reply.total(), 1 + num_other_actors);
    ASSERT_EQ(reply.num_filtered(), num_other_actors);
  }

  // Filter with job id
  {
    rpc::GetAllActorInfoRequest request;
    request.mutable_filters()->set_job_id(job_id.Binary());

    auto &reply =
        *google::protobuf::Arena::CreateMessage<rpc::GetAllActorInfoReply>(&arena);
    gcs_actor_manager_->HandleGetAllActorInfo(request, &reply, callback);
    ASSERT_EQ(reply.actor_table_data().size(), 1);
    ASSERT_EQ(reply.num_filtered(), num_other_actors);
  }

  // Filter with states
  {
    rpc::GetAllActorInfoRequest request;
    request.mutable_filters()->set_state(rpc::ActorTableData::ALIVE);

    auto &reply =
        *google::protobuf::Arena::CreateMessage<rpc::GetAllActorInfoReply>(&arena);
    gcs_actor_manager_->HandleGetAllActorInfo(request, &reply, callback);
    ASSERT_EQ(reply.actor_table_data().size(), 1);
    ASSERT_EQ(reply.num_filtered(), num_other_actors);
  }

  // Simple test AND
  {
    rpc::GetAllActorInfoRequest request;
    request.mutable_filters()->set_state(rpc::ActorTableData::ALIVE);
    request.mutable_filters()->set_job_id(job_id.Binary());

    auto &reply =
        *google::protobuf::Arena::CreateMessage<rpc::GetAllActorInfoReply>(&arena);
    gcs_actor_manager_->HandleGetAllActorInfo(request, &reply, callback);
    ASSERT_EQ(reply.actor_table_data().size(), 1);
    ASSERT_EQ(reply.num_filtered(), num_other_actors);
  }
  {
    rpc::GetAllActorInfoRequest request;
    request.mutable_filters()->set_state(rpc::ActorTableData::DEAD);
    request.mutable_filters()->set_job_id(job_id.Binary());

    auto &reply =
        *google::protobuf::Arena::CreateMessage<rpc::GetAllActorInfoReply>(&arena);
    gcs_actor_manager_->HandleGetAllActorInfo(request, &reply, callback);
    ASSERT_EQ(reply.num_filtered(), num_other_actors + 1);
    ASSERT_EQ(reply.actor_table_data().size(), 0);
  }
}

TEST_F(GcsActorManagerTest, TestGetAllActorInfoLimit) {
  google::protobuf::Arena arena;
  auto job_id_1 = JobID::FromInt(1);
  auto num_actors = 3;
  for (int i = 0; i < num_actors; i++) {
    auto request1 = Mocker::GenRegisterActorRequest(job_id_1,
                                                    /*max_restarts=*/0,
                                                    /*detached=*/false);
    SyncPostAndWait(io_service_, "TestGetAllActorInfoLimit", [&]() {
      Status status = gcs_actor_manager_->RegisterActor(
          request1, [](std::shared_ptr<gcs::GcsActor> actor, const Status &status) {});
      ASSERT_TRUE(status.ok());
    });
  }

  {
    rpc::GetAllActorInfoRequest request;
    auto &reply =
        *google::protobuf::Arena::CreateMessage<rpc::GetAllActorInfoReply>(&arena);
    auto callback = [](Status status,
                       std::function<void()> success,
                       std::function<void()> failure) {};
    gcs_actor_manager_->HandleGetAllActorInfo(request, &reply, callback);
    ASSERT_EQ(reply.actor_table_data().size(), 3);

    request.set_limit(2);
    auto &reply_2 =
        *google::protobuf::Arena::CreateMessage<rpc::GetAllActorInfoReply>(&arena);
    gcs_actor_manager_->HandleGetAllActorInfo(request, &reply_2, callback);
    ASSERT_EQ(reply_2.actor_table_data().size(), 2);
    ASSERT_EQ(reply_2.total(), 3);
  }
}

namespace gcs {
TEST_F(GcsActorManagerTest, TestKillActorWhenActorIsCreating) {
  auto job_id = JobID::FromInt(1);
  auto registered_actor = RegisterActor(job_id, /*max_restarts*/ -1);
  rpc::CreateActorRequest create_actor_request;
  create_actor_request.mutable_task_spec()->CopyFrom(
      registered_actor->GetCreationTaskSpecification().GetMessage());

  std::vector<std::shared_ptr<gcs::GcsActor>> finished_actors;
  Status status = gcs_actor_manager_->CreateActor(
      create_actor_request,
      [&finished_actors](const std::shared_ptr<gcs::GcsActor> &actor,
                         const rpc::PushTaskReply &reply,
                         const Status &status) { finished_actors.emplace_back(actor); });
  RAY_CHECK_OK(status);

  ASSERT_EQ(finished_actors.size(), 0);
  ASSERT_EQ(mock_actor_scheduler_->actors.size(), 1);
  auto actor = mock_actor_scheduler_->actors.back();
  mock_actor_scheduler_->actors.pop_back();

  // Make sure actor in the phase of creating, that is the worker id is not nil and the
  // actor state is not alive yet.
  actor->UpdateAddress(RandomAddress());
  const auto &worker_id = actor->GetWorkerID();
  ASSERT_TRUE(!worker_id.IsNil());
  ASSERT_NE(actor->GetState(), rpc::ActorTableData::ALIVE);

  // Then handle the kill actor requst (restart).
  rpc::KillActorViaGcsReply reply;
  rpc::KillActorViaGcsRequest request;
  request.set_actor_id(actor->GetActorID().Binary());
  request.set_force_kill(true);
  // Set the `no_restart` flag to true so that the actor will restart again.
  request.set_no_restart(false);
  gcs_actor_manager_->HandleKillActorViaGcs(
      request,
      &reply,
      /*send_reply_callback*/
      [](Status status, std::function<void()> success, std::function<void()> failure) {});

  // Make sure the `KillActor` rpc is send.
  ASSERT_EQ(worker_client_->killed_actors_.size(), 1);
  ASSERT_EQ(worker_client_->killed_actors_.front(), actor->GetActorID());

  // Make sure the actor is restarting.
  ASSERT_EQ(actor->GetState(), rpc::ActorTableData::RESTARTING);
}

TEST_F(GcsActorManagerTest, TestRestartActorForLineageReconstruction) {
  auto job_id = JobID::FromInt(1);
  auto registered_actor = RegisterActor(job_id, /*max_restarts*/ -1);
  rpc::CreateActorRequest create_actor_request;
  create_actor_request.mutable_task_spec()->CopyFrom(
      registered_actor->GetCreationTaskSpecification().GetMessage());

  std::vector<std::shared_ptr<gcs::GcsActor>> created_actors;
  RAY_CHECK_OK(gcs_actor_manager_->CreateActor(
      create_actor_request,
      [&created_actors](std::shared_ptr<gcs::GcsActor> actor,
                        const rpc::PushTaskReply &reply,
                        const Status &status) { created_actors.emplace_back(actor); }));

  ASSERT_EQ(created_actors.size(), 0);
  ASSERT_EQ(mock_actor_scheduler_->actors.size(), 1);
  auto actor = mock_actor_scheduler_->actors.back();
  mock_actor_scheduler_->actors.pop_back();

  // Check that the actor is in state `ALIVE`.
  auto address = RandomAddress();
  auto node_id = NodeID::FromBinary(address.raylet_id());
  actor->UpdateAddress(address);
  gcs_actor_manager_->OnActorCreationSuccess(actor, rpc::PushTaskReply());
  WaitActorCreated(actor->GetActorID());
  ASSERT_EQ(created_actors.size(), 1);
  ASSERT_EQ(actor->GetState(), rpc::ActorTableData::ALIVE);

  // Remove node and then check that the actor is being restarted.
  EXPECT_CALL(*mock_actor_scheduler_, CancelOnNode(node_id));
  OnNodeDead(node_id);
  ASSERT_EQ(actor->GetState(), rpc::ActorTableData::RESTARTING);

  // Add node and check that the actor is restarted.
  ASSERT_EQ(mock_actor_scheduler_->actors.size(), 1);
  mock_actor_scheduler_->actors.clear();
  ASSERT_EQ(created_actors.size(), 1);
  auto node_id2 = NodeID::FromRandom();
  address.set_raylet_id(node_id2.Binary());
  actor->UpdateAddress(address);
  gcs_actor_manager_->OnActorCreationSuccess(actor, rpc::PushTaskReply());
  WaitActorCreated(actor->GetActorID());
  ASSERT_EQ(created_actors.size(), 1);
  ASSERT_EQ(actor->GetState(), rpc::ActorTableData::ALIVE);
  ASSERT_EQ(actor->GetNodeID(), node_id2);
  ASSERT_EQ(actor->GetActorTableData().num_restarts(), 1);
  ASSERT_EQ(actor->GetActorTableData().num_restarts_due_to_lineage_reconstruction(), 0);

  // The actor is out of scope and dead.
  ReportActorOutOfScope(actor->GetActorID(),
                        /*num_restarts_due_to_lineage_reconstruction=*/0);
  ASSERT_EQ(actor->GetState(), rpc::ActorTableData::DEAD);

  // Restart the actor due to linage reconstruction.
  RestartActorForLineageReconstruction(actor->GetActorID(),
                                       /*num_restarts_due_to_lineage_reconstruction=*/1);
  ASSERT_EQ(actor->GetState(), rpc::ActorTableData::RESTARTING);

  // Add node and check that the actor is restarted.
  ASSERT_EQ(mock_actor_scheduler_->actors.size(), 1);
  mock_actor_scheduler_->actors.clear();
  ASSERT_EQ(created_actors.size(), 1);
  auto node_id3 = NodeID::FromRandom();
  address.set_raylet_id(node_id3.Binary());
  actor->UpdateAddress(address);
  gcs_actor_manager_->OnActorCreationSuccess(actor, rpc::PushTaskReply());
  WaitActorCreated(actor->GetActorID());
  ASSERT_EQ(created_actors.size(), 1);
  ASSERT_EQ(actor->GetState(), rpc::ActorTableData::ALIVE);
  ASSERT_EQ(actor->GetNodeID(), node_id3);
  ASSERT_EQ(actor->GetActorTableData().num_restarts(), 2);
  ASSERT_EQ(actor->GetActorTableData().num_restarts_due_to_lineage_reconstruction(), 1);
}

TEST_F(GcsActorManagerTest, TestRestartPermanentlyDeadActorForLineageReconstruction) {
  auto job_id = JobID::FromInt(1);
  auto registered_actor = RegisterActor(job_id, /*max_restarts*/ 0);
  rpc::CreateActorRequest create_actor_request;
  create_actor_request.mutable_task_spec()->CopyFrom(
      registered_actor->GetCreationTaskSpecification().GetMessage());

  std::vector<std::shared_ptr<gcs::GcsActor>> created_actors;
  RAY_CHECK_OK(gcs_actor_manager_->CreateActor(
      create_actor_request,
      [&created_actors](std::shared_ptr<gcs::GcsActor> actor,
                        const rpc::PushTaskReply &reply,
                        const Status &status) { created_actors.emplace_back(actor); }));

  ASSERT_EQ(created_actors.size(), 0);
  ASSERT_EQ(mock_actor_scheduler_->actors.size(), 1);
  auto actor = mock_actor_scheduler_->actors.back();
  mock_actor_scheduler_->actors.pop_back();

  // Check that the actor is in state `ALIVE`.
  auto address = RandomAddress();
  actor->UpdateAddress(address);
  gcs_actor_manager_->OnActorCreationSuccess(actor, rpc::PushTaskReply());
  WaitActorCreated(actor->GetActorID());
  ASSERT_EQ(created_actors.size(), 1);
  ASSERT_EQ(actor->GetState(), rpc::ActorTableData::ALIVE);

  // Remove owner node and then check that the actor is dead.
  const auto owner_node_id = actor->GetOwnerNodeID();
  EXPECT_CALL(*mock_actor_scheduler_, CancelOnNode(owner_node_id));
  OnNodeDead(owner_node_id);
  ASSERT_EQ(actor->GetState(), rpc::ActorTableData::DEAD);

  // Restart on an invalid or permanently dead actor should fail.
  auto reply = RestartActorForLineageReconstruction(
      ActorID::Of(actor->GetActorID().JobId(), RandomTaskId(), 0),
      /*num_restarts_due_to_lineage_reconstruction=*/0);
  ASSERT_EQ(reply.status().code(), static_cast<int>(StatusCode::Invalid));

  reply = RestartActorForLineageReconstruction(
      actor->GetActorID(),
      /*num_restarts_due_to_lineage_reconstruction=*/0);
  ASSERT_EQ(reply.status().code(), static_cast<int>(StatusCode::Invalid));
}

TEST_F(GcsActorManagerTest, TestIdempotencyOfRestartActorForLineageReconstruction) {
  auto job_id = JobID::FromInt(1);
  auto registered_actor = RegisterActor(job_id, /*max_restarts*/ -1);
  rpc::CreateActorRequest create_actor_request;
  create_actor_request.mutable_task_spec()->CopyFrom(
      registered_actor->GetCreationTaskSpecification().GetMessage());

  std::vector<std::shared_ptr<gcs::GcsActor>> created_actors;
  RAY_CHECK_OK(gcs_actor_manager_->CreateActor(
      create_actor_request,
      [&created_actors](std::shared_ptr<gcs::GcsActor> actor,
                        const rpc::PushTaskReply &reply,
                        const Status &status) { created_actors.emplace_back(actor); }));

  ASSERT_EQ(created_actors.size(), 0);
  ASSERT_EQ(mock_actor_scheduler_->actors.size(), 1);
  auto actor = mock_actor_scheduler_->actors.back();
  mock_actor_scheduler_->actors.pop_back();

  // Check that the actor is in state `ALIVE`.
  auto address = RandomAddress();
  actor->UpdateAddress(address);
  gcs_actor_manager_->OnActorCreationSuccess(actor, rpc::PushTaskReply());
  WaitActorCreated(actor->GetActorID());
  ASSERT_EQ(created_actors.size(), 1);

  // The actor is out of scope and dead.
  ReportActorOutOfScope(actor->GetActorID(),
                        /*num_restarts_due_to_lineage_reconstruction=*/0);
  ASSERT_EQ(actor->GetState(), rpc::ActorTableData::DEAD);

  // Test the case where the RestartActorForLineageReconstruction rpc is received and
  // being handled and then the connection is lost and the caller resends the same
  // request. The second RestartActorForLineageReconstruction rpc should be deduplicated
  // and not be handled again, instead it should be replied with the same reply as the
  // first one.
  rpc::RestartActorForLineageReconstructionRequest request;
  request.set_actor_id(actor->GetActorID().Binary());
  request.set_num_restarts_due_to_lineage_reconstruction(1);
  rpc::RestartActorForLineageReconstructionReply reply1;
  rpc::RestartActorForLineageReconstructionReply reply2;
  std::promise<bool> promise1;
  std::promise<bool> promise2;
  io_service_.post(
      [this, &request, &reply1, &reply2, &promise1, &promise2]() {
        gcs_actor_manager_->HandleRestartActorForLineageReconstruction(
            request,
            &reply1,
            [&reply1, &promise1](Status status,
                                 std::function<void()> success,
                                 std::function<void()> failure) {
              ASSERT_EQ(reply1.status().code(), static_cast<int>(StatusCode::OK));
              promise1.set_value(true);
            });
        gcs_actor_manager_->HandleRestartActorForLineageReconstruction(
            request,
            &reply2,
            [&reply2, &promise2](Status status,
                                 std::function<void()> success,
                                 std::function<void()> failure) {
              ASSERT_EQ(reply2.status().code(), static_cast<int>(StatusCode::OK));
              promise2.set_value(true);
            });
      },
      "test");
  promise1.get_future().get();
  promise2.get_future().get();
  ASSERT_EQ(actor->GetState(), rpc::ActorTableData::RESTARTING);

  // Add node and check that the actor is restarted.
  ASSERT_EQ(mock_actor_scheduler_->actors.size(), 1);
  mock_actor_scheduler_->actors.clear();
  ASSERT_EQ(created_actors.size(), 1);
  auto node_id = NodeID::FromRandom();
  address.set_raylet_id(node_id.Binary());
  actor->UpdateAddress(address);
  gcs_actor_manager_->OnActorCreationSuccess(actor, rpc::PushTaskReply());
  WaitActorCreated(actor->GetActorID());
  ASSERT_EQ(created_actors.size(), 1);
  ASSERT_EQ(actor->GetState(), rpc::ActorTableData::ALIVE);
  ASSERT_EQ(actor->GetNodeID(), node_id);
  // Two duplicate RestartActorForLineageReconstruction rpcs should only trigger the
  // restart once.
  ASSERT_EQ(actor->GetActorTableData().num_restarts(), 1);
  ASSERT_EQ(actor->GetActorTableData().num_restarts_due_to_lineage_reconstruction(), 1);

  // Test the case where the RestartActorForLineageReconstruction rpc is replied but the
  // reply is lost and the caller resends the same request. The second
  // RestartActorForLineageReconstruction rpc should be directly replied without
  // triggering another restart of the actor.
  auto reply = RestartActorForLineageReconstruction(
      actor->GetActorID(),
      /*num_restarts_due_to_lineage_reconstruction=*/1);
  ASSERT_EQ(reply.status().code(), static_cast<int>(StatusCode::OK));
  // Make sure the actor is not restarted again.
  ASSERT_EQ(actor->GetState(), rpc::ActorTableData::ALIVE);
  ASSERT_EQ(actor->GetActorTableData().num_restarts(), 1);
  ASSERT_EQ(actor->GetActorTableData().num_restarts_due_to_lineage_reconstruction(), 1);
}

TEST_F(GcsActorManagerTest, TestDestroyActorWhenActorIsCreating) {
  auto job_id = JobID::FromInt(1);
  auto registered_actor = RegisterActor(job_id, /*max_restarts*/ -1);
  rpc::CreateActorRequest create_actor_request;
  create_actor_request.mutable_task_spec()->CopyFrom(
      registered_actor->GetCreationTaskSpecification().GetMessage());

  std::vector<std::shared_ptr<gcs::GcsActor>> finished_actors;
  Status status = gcs_actor_manager_->CreateActor(
      create_actor_request,
      [&finished_actors](const std::shared_ptr<gcs::GcsActor> &actor,
                         const rpc::PushTaskReply &reply,
                         const Status &status) { finished_actors.emplace_back(actor); });
  RAY_CHECK_OK(status);

  ASSERT_EQ(finished_actors.size(), 0);
  ASSERT_EQ(mock_actor_scheduler_->actors.size(), 1);
  auto actor = mock_actor_scheduler_->actors.back();
  mock_actor_scheduler_->actors.pop_back();

  // Make sure actor in the phase of creating, that is the worker id is not nil and the
  // actor state is not alive yet.
  actor->UpdateAddress(RandomAddress());
  const auto &worker_id = actor->GetWorkerID();
  ASSERT_TRUE(!worker_id.IsNil());
  ASSERT_NE(actor->GetState(), rpc::ActorTableData::ALIVE);

  // Then handle the kill actor requst (no restart).
  rpc::KillActorViaGcsReply reply;
  rpc::KillActorViaGcsRequest request;
  request.set_actor_id(actor->GetActorID().Binary());
  request.set_force_kill(true);
  // Set the `no_restart` flag to true so that the actor will be destoryed.
  request.set_no_restart(true);
  SyncPostAndWait(io_service_, "TestDestroyActorWhenActorIsCreating", [&]() {
    gcs_actor_manager_->HandleKillActorViaGcs(
        request,
        &reply,
        /*send_reply_callback*/
        [](Status status, std::function<void()> success, std::function<void()> failure) {
        });
  });

  // Make sure the `KillActor` rpc is send.
  ASSERT_EQ(worker_client_->killed_actors_.size(), 1);
  ASSERT_EQ(worker_client_->killed_actors_.front(), actor->GetActorID());

  // Make sure the actor is dead.
  ASSERT_EQ(actor->GetState(), rpc::ActorTableData::DEAD);
}

}  // namespace gcs
}  // namespace ray
