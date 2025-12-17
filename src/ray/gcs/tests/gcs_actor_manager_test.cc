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

#include "ray/gcs/gcs_actor_manager.h"

#include <gtest/gtest.h>

#include <list>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "mock/ray/gcs/gcs_kv_manager.h"
#include "mock/ray/gcs/gcs_node_manager.h"
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/runtime_env_manager.h"
#include "ray/common/test_utils.h"
#include "ray/core_worker_rpc_client/fake_core_worker_client.h"
#include "ray/gcs/gcs_actor.h"
#include "ray/gcs/gcs_actor_scheduler.h"
#include "ray/gcs/gcs_function_manager.h"
#include "ray/gcs/store_client/in_memory_store_client.h"
#include "ray/observability/fake_metric.h"
#include "ray/pubsub/publisher.h"
#include "ray/raylet_rpc_client/fake_raylet_client.h"

namespace ray {
namespace gcs {

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
                     [actor_id](const std::shared_ptr<gcs::GcsActor> &current_actor) {
                       return current_actor->GetActorID() == actor_id;
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
                    const LeaseID &lease_id));

  std::vector<std::shared_ptr<gcs::GcsActor>> actors;
};

class MockWorkerClient : public rpc::FakeCoreWorkerClient {
 public:
  explicit MockWorkerClient(instrumented_io_context &io_service)
      : io_service_(io_service) {}

  void WaitForActorRefDeleted(
      rpc::WaitForActorRefDeletedRequest &&request,
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
    auto callback = callbacks_.front();
    auto reply = rpc::WaitForActorRefDeletedReply();
    callback(status, std::move(reply));
    callbacks_.pop_front();
    return true;
  }

  std::list<rpc::ClientCallback<rpc::WaitForActorRefDeletedReply>> callbacks_;
  std::vector<ActorID> killed_actors_;
  instrumented_io_context &io_service_;
};

class GcsActorManagerTest : public ::testing::Test {
 public:
  GcsActorManagerTest() : periodical_runner_(PeriodicalRunner::Create(io_service_)) {
    RayConfig::instance().initialize(
        R"(
{
  "maximum_gcs_destroyed_actor_cached_count": 10
}
  )");
    worker_client_ = std::make_shared<MockWorkerClient>(io_service_);
    raylet_client_ = std::make_shared<rpc::FakeRayletClient>();
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

    gcs_publisher_ = std::make_unique<pubsub::GcsPublisher>(std::move(publisher));
    store_client_ = std::make_shared<gcs::InMemoryStoreClient>();
    gcs_table_storage_ =
        std::make_unique<gcs::GcsTableStorage>(std::make_unique<InMemoryStoreClient>());
    kv_ = std::make_unique<gcs::MockInternalKVInterface>();
    function_manager_ = std::make_unique<gcs::GCSFunctionManager>(*kv_, io_service_);
    auto scheduler = std::make_unique<MockActorScheduler>();
    mock_actor_scheduler_ = scheduler.get();
    raylet_client_pool_ = std::make_unique<rpc::RayletClientPool>(
        [this](const rpc::Address &address) { return raylet_client_; });
    worker_client_pool_ = std::make_unique<rpc::CoreWorkerClientPool>(
        [this](const rpc::Address &address) { return worker_client_; });
    fake_ray_event_recorder_ = std::make_unique<observability::FakeRayEventRecorder>();
    gcs_actor_manager_ = std::make_unique<gcs::GcsActorManager>(
        std::move(scheduler),
        gcs_table_storage_.get(),
        io_service_,
        gcs_publisher_.get(),
        *runtime_env_mgr_,
        *function_manager_,
        [](const ActorID &actor_id) {},
        *raylet_client_pool_,
        *worker_client_pool_,
        *fake_ray_event_recorder_,
        "test_session_name",
        fake_actor_by_state_gauge_,
        fake_gcs_actor_by_state_gauge_);

    for (int i = 1; i <= 10; i++) {
      auto job_id = JobID::FromInt(i);
      job_namespace_table_[job_id] = "";
    }
  }

  ~GcsActorManagerTest() { io_service_.stop(); }

  rpc::Address RandomAddress() const {
    rpc::Address address;
    auto node_id = NodeID::FromRandom();
    auto worker_id = WorkerID::FromRandom();
    address.set_node_id(node_id.Binary());
    address.set_worker_id(worker_id.Binary());
    return address;
  }

  std::shared_ptr<gcs::GcsActor> RegisterActor(
      const JobID &job_id,
      int max_restarts = 0,
      bool detached = false,
      const std::string &name = "",
      const std::string &ray_namespace = "test") {
    // The tests queue up operations and sometimes don't execute them through the
    // io_context themselves. This is a hack, and future tests shouldn't use this
    // RegisterActor function.
    while (io_service_.poll_one()) {
      continue;
    }
    auto request =
        GenRegisterActorRequest(job_id, max_restarts, detached, name, ray_namespace);
    auto status = gcs_actor_manager_->RegisterActor(request, [](const Status &) {});
    io_service_.run_one();
    io_service_.run_one();
    auto actor_id =
        ActorID::FromBinary(request.task_spec().actor_creation_task_spec().actor_id());
    return gcs_actor_manager_->registered_actors_.contains(actor_id)
               ? gcs_actor_manager_->registered_actors_[actor_id]
               : nullptr;
  }

  void OnNodeDead(const NodeID &node_id) {
    auto node_info = std::make_shared<rpc::GcsNodeInfo>();
    node_info->set_node_id(node_id.Binary());
    gcs_actor_manager_->OnNodeDead(node_info, "127.0.0.1");
  }

  void ReportActorOutOfScope(const ActorID &actor_id,
                             size_t num_restarts_due_to_lineage_reconstrcution) {
    rpc::ReportActorOutOfScopeRequest request;
    request.set_actor_id(actor_id.Binary());
    request.set_num_restarts_due_to_lineage_reconstruction(
        num_restarts_due_to_lineage_reconstrcution);
    rpc::ReportActorOutOfScopeReply reply;
    gcs_actor_manager_->HandleReportActorOutOfScope(
        request, &reply, [](auto status, auto success_callback, auto failure_callback) {
        });
    io_service_.run_one();
  }

  const absl::flat_hash_map<ActorID, std::vector<std::function<void(Status)>>>
      &GetActorRegisterCallbacks() const {
    return gcs_actor_manager_->actor_to_register_callbacks_;
  }

  instrumented_io_context io_service_;
  std::shared_ptr<gcs::StoreClient> store_client_;
  std::shared_ptr<gcs::GcsTableStorage> gcs_table_storage_;
  // Actor scheduler's ownership lies in actor manager.
  MockActorScheduler *mock_actor_scheduler_ = nullptr;
  std::shared_ptr<MockWorkerClient> worker_client_;
  std::shared_ptr<rpc::FakeRayletClient> raylet_client_;
  std::unique_ptr<rpc::RayletClientPool> raylet_client_pool_;
  std::unique_ptr<rpc::CoreWorkerClientPool> worker_client_pool_;
  absl::flat_hash_map<JobID, std::string> job_namespace_table_;
  std::unique_ptr<gcs::GcsActorManager> gcs_actor_manager_;
  std::shared_ptr<pubsub::GcsPublisher> gcs_publisher_;
  std::unique_ptr<ray::RuntimeEnvManager> runtime_env_mgr_;
  const std::chrono::milliseconds timeout_ms_{2000};
  absl::Mutex mutex_;
  std::unique_ptr<gcs::GCSFunctionManager> function_manager_;
  std::unique_ptr<gcs::MockInternalKVInterface> kv_;
  std::shared_ptr<PeriodicalRunner> periodical_runner_;
  std::unique_ptr<observability::FakeRayEventRecorder> fake_ray_event_recorder_;
  ray::observability::FakeGauge fake_actor_by_state_gauge_;
  ray::observability::FakeGauge fake_gcs_actor_by_state_gauge_;
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
                         const Status &) { finished_actors.emplace_back(actor); });
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
  io_service_.run_one();
  ASSERT_EQ(finished_actors.size(), 1);
  RAY_CHECK_EQ(gcs_actor_manager_->CountFor(rpc::ActorTableData::ALIVE, ""), 1);

  ASSERT_TRUE(worker_client_->Reply());
  ASSERT_EQ(actor->GetState(), rpc::ActorTableData::DEAD);
  RAY_CHECK_EQ(gcs_actor_manager_->CountFor(rpc::ActorTableData::ALIVE, ""), 0);
  RAY_CHECK_EQ(gcs_actor_manager_->CountFor(rpc::ActorTableData::DEAD, ""), 1);
}

TEST_F(GcsActorManagerTest, TestActorStateMetrics) {
  auto job_id = JobID::FromInt(1);
  auto registered_actor = RegisterActor(job_id);
  rpc::CreateActorRequest create_actor_request;
  create_actor_request.mutable_task_spec()->CopyFrom(
      registered_actor->GetCreationTaskSpecification().GetMessage());

  Status status =
      gcs_actor_manager_->CreateActor(create_actor_request,
                                      [](const std::shared_ptr<gcs::GcsActor> &actor,
                                         const rpc::PushTaskReply &reply,
                                         const Status &) {});
  RAY_CHECK_OK(status);
  auto actor = mock_actor_scheduler_->actors.back();
  mock_actor_scheduler_->actors.pop_back();
  actor->UpdateAddress(RandomAddress());
  gcs_actor_manager_->OnActorCreationSuccess(actor, rpc::PushTaskReply());
  io_service_.run_one();
  gcs_actor_manager_->RecordMetrics();
  auto gcs_actor_tag_to_value = fake_gcs_actor_by_state_gauge_.GetTagToValue();
  // 5 states: REGISTERED, CREATED, DESTROYED, UNRESOLVED, PENDING
  ASSERT_EQ(gcs_actor_tag_to_value.size(), 5);
  // 3 states: DEPENDENCIES_UNREADY, PENDING_CREATION, ALIVE
  auto tag_to_value = fake_actor_by_state_gauge_.GetTagToValue();
  ASSERT_EQ(tag_to_value.size(), 3);
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
                                           const Status &) {});
    RAY_CHECK_OK(status);
    auto actor = mock_actor_scheduler_->actors.back();
    mock_actor_scheduler_->actors.pop_back();
    // Check that the actor is in state `ALIVE`.
    actor->UpdateAddress(RandomAddress());
    gcs_actor_manager_->OnActorCreationSuccess(actor, rpc::PushTaskReply());
    io_service_.run_one();
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
      [&finished_actors](std::shared_ptr<gcs::GcsActor> result_actor,
                         const rpc::PushTaskReply &,
                         const Status &) {
        finished_actors.emplace_back(result_actor);
      }));

  ASSERT_EQ(finished_actors.size(), 0);
  ASSERT_EQ(mock_actor_scheduler_->actors.size(), 1);
  auto actor = mock_actor_scheduler_->actors.back();
  mock_actor_scheduler_->actors.clear();

  gcs_actor_manager_->OnActorSchedulingFailed(
      actor,
      rpc::RequestWorkerLeaseReply::SCHEDULING_CANCELLED_RUNTIME_ENV_SETUP_FAILED,
      "");
  io_service_.run_one();
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
      [&finished_actors](std::shared_ptr<gcs::GcsActor> result_actor,
                         const rpc::PushTaskReply &,
                         const Status &) {
        finished_actors.emplace_back(result_actor);
      }));

  ASSERT_EQ(finished_actors.size(), 0);
  ASSERT_EQ(mock_actor_scheduler_->actors.size(), 1);
  auto actor = mock_actor_scheduler_->actors.back();
  mock_actor_scheduler_->actors.pop_back();

  // Check that the actor is in state `ALIVE`.
  auto address = RandomAddress();
  auto node_id = NodeID::FromBinary(address.node_id());
  auto worker_id = WorkerID::FromBinary(address.worker_id());
  actor->UpdateAddress(address);
  gcs_actor_manager_->OnActorCreationSuccess(actor, rpc::PushTaskReply());
  io_service_.run_one();
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
                         const rpc::PushTaskReply &,
                         const Status &) { finished_actors.emplace_back(actor); });
  RAY_CHECK_OK(status);

  ASSERT_EQ(finished_actors.size(), 0);
  ASSERT_EQ(mock_actor_scheduler_->actors.size(), 1);
  auto actor = mock_actor_scheduler_->actors.back();
  mock_actor_scheduler_->actors.pop_back();

  // Check that the actor is in state `ALIVE`.
  auto address = RandomAddress();
  actor->UpdateAddress(address);
  gcs_actor_manager_->OnActorCreationSuccess(actor, rpc::PushTaskReply());
  io_service_.run_one();
  ASSERT_EQ(finished_actors.size(), 1);

  // Killing another node does not affect this actor.
  EXPECT_CALL(*mock_actor_scheduler_, CancelOnNode(_));
  OnNodeDead(NodeID::FromRandom());
  ASSERT_EQ(actor->GetState(), rpc::ActorTableData::ALIVE);

  // Remove node and then check that the actor is dead.
  auto node_id = NodeID::FromBinary(address.node_id());
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
                         const rpc::PushTaskReply &,
                         const Status &) { finished_actors.emplace_back(actor); });
  RAY_CHECK_OK(status);

  ASSERT_EQ(finished_actors.size(), 0);
  ASSERT_EQ(mock_actor_scheduler_->actors.size(), 1);
  auto actor = mock_actor_scheduler_->actors.back();
  mock_actor_scheduler_->actors.pop_back();

  // Check that the actor is in state `ALIVE`.
  auto address = RandomAddress();
  auto node_id = NodeID::FromBinary(address.node_id());
  actor->UpdateAddress(address);
  gcs_actor_manager_->OnActorCreationSuccess(actor, rpc::PushTaskReply());
  io_service_.run_one();
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
  address.set_node_id(node_id2.Binary());
  actor->UpdateAddress(address);
  gcs_actor_manager_->OnActorCreationSuccess(actor, rpc::PushTaskReply());
  io_service_.run_one();
  io_service_.run_one();
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
      [&finished_actors](std::shared_ptr<gcs::GcsActor> result_actor,
                         const rpc::PushTaskReply &,
                         const Status &) {
        finished_actors.emplace_back(result_actor);
      }));

  ASSERT_EQ(finished_actors.size(), 0);
  ASSERT_EQ(mock_actor_scheduler_->actors.size(), 1);
  auto actor = mock_actor_scheduler_->actors.back();
  mock_actor_scheduler_->actors.pop_back();
  const auto owner_node_id = actor->GetOwnerNodeID();

  // Check that the actor is in state `ALIVE`.
  auto address = RandomAddress();
  auto node_id = NodeID::FromBinary(address.node_id());
  actor->UpdateAddress(address);
  gcs_actor_manager_->OnActorCreationSuccess(actor, rpc::PushTaskReply());
  io_service_.run_one();
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
  // The worker has not yet been granted a lease, so no KillActor RPC should be sent.
  ASSERT_EQ(raylet_client_->killed_actors.size(), 0);

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
      [&finished_actors](std::shared_ptr<gcs::GcsActor> result_actor,
                         const rpc::PushTaskReply &,
                         const Status &) {
        finished_actors.emplace_back(result_actor);
      }));

  ASSERT_EQ(finished_actors.size(), 0);
  ASSERT_EQ(mock_actor_scheduler_->actors.size(), 1);
  auto actor = mock_actor_scheduler_->actors.back();
  mock_actor_scheduler_->actors.pop_back();
  const auto owner_node_id = actor->GetOwnerNodeID();

  // Check that the actor is in state `ALIVE`.
  actor->UpdateAddress(RandomAddress());
  gcs_actor_manager_->OnActorCreationSuccess(actor, rpc::PushTaskReply());
  io_service_.run_one();
  ASSERT_EQ(finished_actors.size(), 1);

  // Remove the owner's node.
  EXPECT_CALL(*mock_actor_scheduler_, CancelOnNode(owner_node_id));
  OnNodeDead(owner_node_id);
  // The child actor should not be marked as dead.
  ASSERT_TRUE(raylet_client_->killed_actors.empty());
  ASSERT_EQ(actor->GetState(), rpc::ActorTableData::ALIVE);
}

TEST_F(GcsActorManagerTest, TestActorWithEmptyName) {
  auto job_id = JobID::FromInt(1);

  // Gen `CreateActorRequest` with an empty name.
  // (name,actor_id) => ("", actor_id_1)
  auto request1 = GenRegisterActorRequest(job_id,
                                          /*max_restarts=*/0,
                                          /*detached=*/true,
                                          /*name=*/"");

  Status status = gcs_actor_manager_->RegisterActor(request1, [](const Status &) {});
  io_service_.run_one();

  // Ensure successful registration.
  ASSERT_TRUE(status.ok());
  // Make sure actor who empty name is not treated as a named actor.
  ASSERT_TRUE(gcs_actor_manager_->GetActorIDByName("", "").IsNil());

  // Gen another `CreateActorRequest` with an empty name.
  // (name,actor_id) => ("", actor_id_2)
  auto request2 = GenRegisterActorRequest(job_id,
                                          /*max_restarts=*/0,
                                          /*detached=*/true,
                                          /*name=*/"");
  status = gcs_actor_manager_->RegisterActor(request2, [](const Status &) {});
  io_service_.run_one();
  // Ensure successful registration.
  ASSERT_TRUE(status.ok());
}

TEST_F(GcsActorManagerTest, TestNamedActors) {
  auto job_id_1 = JobID::FromInt(1);
  auto job_id_2 = JobID::FromInt(2);

  auto request1 = GenRegisterActorRequest(job_id_1,
                                          /*max_restarts=*/0,
                                          /*detached=*/true,
                                          /*name=*/"actor1",
                                          /*ray_namespace=*/"test_named_actor");
  Status status = gcs_actor_manager_->RegisterActor(request1, [](const Status &) {});
  io_service_.run_one();
  ASSERT_TRUE(status.ok());
  ASSERT_EQ(gcs_actor_manager_->GetActorIDByName("actor1", "test_named_actor").Binary(),
            request1.task_spec().actor_creation_task_spec().actor_id());

  auto request2 = GenRegisterActorRequest(job_id_1,
                                          /*max_restarts=*/0,
                                          /*detached=*/true,
                                          /*name=*/"actor2",
                                          /*ray_namesapce=*/"test_named_actor");
  status = gcs_actor_manager_->RegisterActor(request2, [](const Status &) {});
  io_service_.run_one();
  ASSERT_TRUE(status.ok());
  ASSERT_EQ(gcs_actor_manager_->GetActorIDByName("actor2", "test_named_actor").Binary(),
            request2.task_spec().actor_creation_task_spec().actor_id());

  // Check that looking up a non-existent name returns ActorID::Nil();
  ASSERT_EQ(gcs_actor_manager_->GetActorIDByName("actor3", "test_named_actor"),
            ActorID::Nil());

  // Check that naming collisions return Status::AlreadyExists.
  auto request3 = GenRegisterActorRequest(job_id_1,
                                          /*max_restarts=*/0,
                                          /*detached=*/true,
                                          /*name=*/"actor2",
                                          /*ray_namesapce=*/"test_named_actor");
  status = gcs_actor_manager_->RegisterActor(request3, [](const Status &) {});
  io_service_.run_one();
  ASSERT_TRUE(status.IsAlreadyExists());
  ASSERT_EQ(gcs_actor_manager_->GetActorIDByName("actor2", "test_named_actor").Binary(),
            request2.task_spec().actor_creation_task_spec().actor_id());

  // Check that naming collisions are enforced across JobIDs.
  auto request4 = GenRegisterActorRequest(job_id_2,
                                          /*max_restarts=*/0,
                                          /*detached=*/true,
                                          /*name=*/"actor2",
                                          /*ray_namesapce=*/"test_named_actor");
  status = gcs_actor_manager_->RegisterActor(request4, [](const Status &) {});
  io_service_.run_one();
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

  Status status = gcs_actor_manager_->CreateActor(
      request1,
      [](std::shared_ptr<gcs::GcsActor>, const rpc::PushTaskReply &, const Status &) {});
  ASSERT_TRUE(status.ok());
  ASSERT_EQ(gcs_actor_manager_->GetActorIDByName(actor_name, "test").Binary(),
            request1.task_spec().actor_creation_task_spec().actor_id());

  auto actor = mock_actor_scheduler_->actors.back();
  mock_actor_scheduler_->actors.pop_back();

  // Check that the actor is in state `ALIVE`.
  auto address = RandomAddress();
  auto node_id = NodeID::FromBinary(address.node_id());
  auto worker_id = WorkerID::FromBinary(address.worker_id());
  actor->UpdateAddress(address);
  gcs_actor_manager_->OnActorCreationSuccess(actor, rpc::PushTaskReply());
  io_service_.run_one();

  // Remove worker and then check that the actor is dead.
  gcs_actor_manager_->OnWorkerDead(node_id, worker_id);
  ASSERT_EQ(actor->GetState(), rpc::ActorTableData::DEAD);
  ASSERT_TRUE(actor->GetActorTableData().death_cause().has_actor_died_error_context());
  ASSERT_TRUE(absl::StrContains(
      actor->GetActorTableData().death_cause().actor_died_error_context().error_message(),
      "worker process has died."));
  ASSERT_EQ(gcs_actor_manager_->GetActorIDByName(actor_name, "test"),
            actor->GetActorID());

  // Detached actor has no reply of WaitForActorRefDeleted request.
  ASSERT_FALSE(worker_client_->Reply());
  // Kill this detached actor
  rpc::KillActorViaGcsReply reply;
  rpc::KillActorViaGcsRequest request;
  request.set_actor_id(actor->GetActorID().Binary());
  request.set_force_kill(true);
  request.set_no_restart(true);
  gcs_actor_manager_->HandleKillActorViaGcs(
      request,
      &reply,
      /*send_reply_callback*/
      [](Status status, std::function<void()> success, std::function<void()> failure) {});
  io_service_.run_one();

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

  status = gcs_actor_manager_->CreateActor(
      request2,
      [](std::shared_ptr<gcs::GcsActor>, const rpc::PushTaskReply &, const Status &) {});
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

  Status status = gcs_actor_manager_->CreateActor(
      request1,
      [](std::shared_ptr<gcs::GcsActor>, const rpc::PushTaskReply &, const Status &) {});
  ASSERT_TRUE(status.ok());
  ASSERT_EQ(gcs_actor_manager_->GetActorIDByName("actor", "test").Binary(),
            request1.task_spec().actor_creation_task_spec().actor_id());

  auto actor = mock_actor_scheduler_->actors.back();
  mock_actor_scheduler_->actors.pop_back();

  // Check that the actor is in state `ALIVE`.
  auto address = RandomAddress();
  auto node_id = NodeID::FromBinary(address.node_id());
  actor->UpdateAddress(address);
  gcs_actor_manager_->OnActorCreationSuccess(actor, rpc::PushTaskReply());
  io_service_.run_one();

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

  status = gcs_actor_manager_->CreateActor(
      request2,
      [](std::shared_ptr<gcs::GcsActor>, const rpc::PushTaskReply &, const Status &) {});
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
                                                     const Status &) {});
  ASSERT_TRUE(status.ok());
  ASSERT_EQ(gcs_actor_manager_->GetActorIDByName("actor", "test").Binary(),
            request1.task_spec().actor_creation_task_spec().actor_id());

  auto actor = mock_actor_scheduler_->actors.back();
  mock_actor_scheduler_->actors.pop_back();

  // Check that the actor is in state `ALIVE`.
  auto address = RandomAddress();
  auto node_id = NodeID::FromBinary(address.node_id());
  auto worker_id = WorkerID::FromBinary(address.worker_id());
  actor->UpdateAddress(address);
  gcs_actor_manager_->OnActorCreationSuccess(actor, rpc::PushTaskReply());
  io_service_.run_one();

  // Remove worker and then check that the actor is dead. The actor should be
  // reconstructed.
  gcs_actor_manager_->OnWorkerDead(node_id, worker_id);
  ASSERT_EQ(actor->GetState(), rpc::ActorTableData::RESTARTING);

  // Create an actor with the same name.
  // It should fail because actor has been reconstructed, and names shouldn't have been
  // cleaned.
  const auto job_id_2 = JobID::FromInt(2);
  auto request2 = GenRegisterActorRequest(job_id_2,
                                          /*max_restarts=*/0,
                                          /*detached=*/true,
                                          /*name=*/"actor");
  status = gcs_actor_manager_->RegisterActor(request2, [](const Status &) {});
  io_service_.run_one();
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
      [&finished_actors](std::shared_ptr<gcs::GcsActor> result_actor,
                         const rpc::PushTaskReply &,
                         const Status &) {
        finished_actors.emplace_back(result_actor);
      }));

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
      [&finished_actors](std::shared_ptr<gcs::GcsActor> result_actor,
                         const rpc::PushTaskReply &,
                         const Status &) {
        finished_actors.emplace_back(result_actor);
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
  address.set_node_id(node_id.Binary());
  address.set_worker_id(worker_id.Binary());
  actor->UpdateAddress(address);
  const auto &actor_id = actor->GetActorID();
  // LeaseID is randomly generated, so we can't check for a specific lease ID.
  EXPECT_CALL(*mock_actor_scheduler_, CancelOnLeasing(node_id, actor_id, _));
  gcs_actor_manager_->OnWorkerDead(owner_node_id, owner_worker_id);
  io_service_.run_one();
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
      [&finished_actors](std::shared_ptr<gcs::GcsActor> result_actor,
                         const rpc::PushTaskReply &,
                         const Status &) {
        finished_actors.emplace_back(result_actor);
      }));
  // Make sure the actor is scheduling.
  ASSERT_EQ(mock_actor_scheduler_->actors.size(), 1);
  auto actor = mock_actor_scheduler_->actors.back();
  mock_actor_scheduler_->actors.pop_back();
  // Make sure the actor state is `PENDING`.
  ASSERT_EQ(actor->GetState(), rpc::ActorTableData::PENDING_CREATION);

  actor->UpdateAddress(RandomAddress());
  gcs_actor_manager_->OnActorCreationSuccess(actor, rpc::PushTaskReply());
  io_service_.run_one();
  ASSERT_EQ(actor->GetState(), rpc::ActorTableData::ALIVE);
}

TEST_F(GcsActorManagerTest, TestOwnerWorkerDieBeforeActorDependenciesResolved) {
  auto job_id = JobID::FromInt(1);
  auto registered_actor = RegisterActor(job_id);
  const auto &owner_address = registered_actor->GetOwnerAddress();
  auto node_id = NodeID::FromBinary(owner_address.node_id());
  auto worker_id = WorkerID::FromBinary(owner_address.worker_id());
  gcs_actor_manager_->OnWorkerDead(node_id, worker_id);
  io_service_.run_one();
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

  const auto &callbacks = GetActorRegisterCallbacks();
  ASSERT_FALSE(callbacks.count(registered_actor->GetActorID()));
  io_service_.run_one();
}

TEST_F(GcsActorManagerTest, TestOwnerWorkerDieBeforeDetachedActorDependenciesResolved) {
  auto job_id = JobID::FromInt(1);
  auto registered_actor = RegisterActor(job_id, /*max_restarts=*/1, /*detached=*/true);
  const auto &owner_address = registered_actor->GetOwnerAddress();
  auto node_id = NodeID::FromBinary(owner_address.node_id());
  auto worker_id = WorkerID::FromBinary(owner_address.worker_id());
  gcs_actor_manager_->OnWorkerDead(node_id, worker_id);
  io_service_.run_one();
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
  const auto &callbacks = GetActorRegisterCallbacks();
  ASSERT_FALSE(callbacks.count(registered_actor->GetActorID()));
  io_service_.run_one();
}

TEST_F(GcsActorManagerTest, TestOwnerNodeDieBeforeActorDependenciesResolved) {
  auto job_id = JobID::FromInt(1);
  auto registered_actor = RegisterActor(job_id);
  const auto &owner_address = registered_actor->GetOwnerAddress();
  auto node_id = NodeID::FromBinary(owner_address.node_id());
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
  const auto &callbacks = GetActorRegisterCallbacks();
  ASSERT_FALSE(callbacks.count(registered_actor->GetActorID()));
}

TEST_F(GcsActorManagerTest, TestOwnerNodeDieBeforeDetachedActorDependenciesResolved) {
  auto job_id = JobID::FromInt(1);
  auto registered_actor = RegisterActor(job_id, /*max_restarts=*/1, /*detached=*/true);
  const auto &owner_address = registered_actor->GetOwnerAddress();
  auto node_id = NodeID::FromBinary(owner_address.node_id());
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
  const auto &callbacks = GetActorRegisterCallbacks();
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
      registered_actor->GetCreationTaskSpecification().GetMessage());

  std::vector<std::shared_ptr<gcs::GcsActor>> finished_actors;
  RAY_CHECK_OK(gcs_actor_manager_->CreateActor(
      create_actor_request,
      [&finished_actors](std::shared_ptr<gcs::GcsActor> result_actor,
                         const rpc::PushTaskReply &,
                         const Status &) {
        finished_actors.emplace_back(result_actor);
      }));
  auto actor = mock_actor_scheduler_->actors.back();
  mock_actor_scheduler_->actors.pop_back();

  auto address = RandomAddress();
  actor->UpdateAddress(address);
  gcs_actor_manager_->OnActorCreationSuccess(actor, rpc::PushTaskReply());
  io_service_.run_one();
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
  io_service_.run_one();
}

TEST_F(GcsActorManagerTest, TestRayNamespace) {
  auto job_id_1 = JobID::FromInt(1);
  auto job_id_2 = JobID::FromInt(20);
  auto job_id_3 = JobID::FromInt(3);
  std::string second_namespace = "another_namespace";
  job_namespace_table_[job_id_2] = second_namespace;

  auto request1 = GenRegisterActorRequest(job_id_1,
                                          /*max_restarts=*/0,
                                          /*detached=*/true,
                                          /*name=*/"actor");
  Status status = gcs_actor_manager_->RegisterActor(request1, [](const Status &) {});
  ASSERT_TRUE(status.ok());
  ASSERT_EQ(gcs_actor_manager_->GetActorIDByName("actor", "test").Binary(),
            request1.task_spec().actor_creation_task_spec().actor_id());
  io_service_.run_one();

  auto request2 = GenRegisterActorRequest(job_id_2,
                                          /*max_restarts=*/0,
                                          /*detached=*/true,
                                          /*name=*/"actor",
                                          second_namespace);
  // Create a second actor of the same name. Its job id belongs to a different
  // namespace though.
  status = gcs_actor_manager_->RegisterActor(request2, [](const Status &) {});
  ASSERT_TRUE(status.ok());
  ASSERT_EQ(gcs_actor_manager_->GetActorIDByName("actor", second_namespace).Binary(),
            request2.task_spec().actor_creation_task_spec().actor_id());
  // The actors may have the same name, but their ids are different.
  ASSERT_NE(gcs_actor_manager_->GetActorIDByName("actor", second_namespace).Binary(),
            request1.task_spec().actor_creation_task_spec().actor_id());
  io_service_.run_one();

  auto request3 = GenRegisterActorRequest(job_id_3,
                                          /*max_restarts=*/0,
                                          /*detached=*/true,
                                          /*name=*/"actor",
                                          /*ray_namespace=*/"test");
  status = gcs_actor_manager_->RegisterActor(request3, [](const Status &) {});
  ASSERT_TRUE(status.IsAlreadyExists());
  ASSERT_EQ(gcs_actor_manager_->GetActorIDByName("actor", "test").Binary(),
            request1.task_spec().actor_creation_task_spec().actor_id());
  io_service_.run_one();
}

TEST_F(GcsActorManagerTest, TestReuseActorNameInNamespace) {
  std::string actor_name = "actor";
  std::string ray_namespace = "actor_namespace";

  auto job_id_1 = JobID::FromInt(1);
  auto request_1 = GenRegisterActorRequest(job_id_1, 0, true, actor_name, ray_namespace);
  auto actor_id_1 =
      ActorID::FromBinary(request_1.task_spec().actor_creation_task_spec().actor_id());
  Status status = gcs_actor_manager_->RegisterActor(request_1, [](const Status &) {});
  ASSERT_TRUE(status.ok());
  ASSERT_EQ(gcs_actor_manager_->GetActorIDByName(actor_name, ray_namespace).Binary(),
            actor_id_1.Binary());
  io_service_.run_one();

  auto owner_address = request_1.task_spec().caller_address();
  auto node_info = std::make_shared<rpc::GcsNodeInfo>();
  node_info->set_node_id(owner_address.node_id());
  gcs_actor_manager_->OnNodeDead(node_info, "");
  ASSERT_EQ(gcs_actor_manager_->GetActorIDByName(actor_name, ray_namespace).Binary(),
            ActorID::Nil().Binary());
  io_service_.run_one();

  auto job_id_2 = JobID::FromInt(2);
  auto request_2 = GenRegisterActorRequest(job_id_2, 0, true, actor_name, ray_namespace);
  auto actor_id_2 =
      ActorID::FromBinary(request_2.task_spec().actor_creation_task_spec().actor_id());
  status = gcs_actor_manager_->RegisterActor(request_2, [](const Status &) {});
  ASSERT_TRUE(status.ok());
  ASSERT_EQ(gcs_actor_manager_->GetActorIDByName(actor_name, ray_namespace).Binary(),
            actor_id_2.Binary());
  io_service_.run_one();
}

TEST_F(GcsActorManagerTest, TestGetAllActorInfoFilters) {
  google::protobuf::Arena arena;
  // The target filter actor.
  auto job_id = JobID::FromInt(1);
  auto registered_actor = RegisterActor(job_id);
  rpc::CreateActorRequest create_actor_request;
  create_actor_request.mutable_task_spec()->CopyFrom(
      registered_actor->GetCreationTaskSpecification().GetMessage());
  std::vector<std::shared_ptr<gcs::GcsActor>> finished_actors;
  Status create_status = gcs_actor_manager_->CreateActor(
      create_actor_request,
      [&finished_actors](const std::shared_ptr<gcs::GcsActor> &result_actor,
                         const rpc::PushTaskReply &,
                         const Status &) { finished_actors.emplace_back(result_actor); });

  ASSERT_TRUE(create_status.ok());
  auto actor = mock_actor_scheduler_->actors.back();
  mock_actor_scheduler_->actors.pop_back();

  // Check that the actor is in state `ALIVE`.
  actor->UpdateAddress(RandomAddress());
  gcs_actor_manager_->OnActorCreationSuccess(actor, rpc::PushTaskReply());
  io_service_.run_one();
  ASSERT_EQ(gcs_actor_manager_->CountFor(rpc::ActorTableData::ALIVE, ""), 1);
  ASSERT_EQ(actor->GetState(), rpc::ActorTableData::ALIVE);

  // Just register some other actors.
  auto job_id_other = JobID::FromInt(2);
  auto num_other_actors = 3;
  for (int i = 0; i < num_other_actors; i++) {
    auto request1 = GenRegisterActorRequest(job_id_other,
                                            /*max_restarts=*/0,
                                            /*detached=*/false);
    Status register_status =
        gcs_actor_manager_->RegisterActor(request1, [](const Status &) {});
    ASSERT_TRUE(register_status.ok());
    io_service_.run_one();
  }

  auto callback = [](Status, std::function<void()>, std::function<void()>) {};
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
    auto request1 = GenRegisterActorRequest(job_id_1,
                                            /*max_restarts=*/0,
                                            /*detached=*/false);
    Status status = gcs_actor_manager_->RegisterActor(request1, [](const Status &) {});
    ASSERT_TRUE(status.ok());
    io_service_.run_one();
  }

  {
    rpc::GetAllActorInfoRequest request;
    auto &reply =
        *google::protobuf::Arena::CreateMessage<rpc::GetAllActorInfoReply>(&arena);
    auto callback = [](Status, std::function<void()>, std::function<void()>) {};
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

TEST_F(GcsActorManagerTest, TestKillActorWhenActorIsCreating) {
  auto job_id = JobID::FromInt(1);
  auto registered_actor = RegisterActor(job_id, /*max_restarts*/ -1);
  rpc::CreateActorRequest create_actor_request;
  create_actor_request.mutable_task_spec()->CopyFrom(
      registered_actor->GetCreationTaskSpecification().GetMessage());

  std::vector<std::shared_ptr<gcs::GcsActor>> finished_actors;
  Status status = gcs_actor_manager_->CreateActor(
      create_actor_request,
      [&finished_actors](const std::shared_ptr<gcs::GcsActor> &result_actor,
                         const rpc::PushTaskReply &,
                         const Status &) { finished_actors.emplace_back(result_actor); });
  RAY_CHECK_OK(status);

  ASSERT_EQ(finished_actors.size(), 0);
  ASSERT_EQ(mock_actor_scheduler_->actors.size(), 1);
  auto actor = mock_actor_scheduler_->actors.back();
  mock_actor_scheduler_->actors.pop_back();

  // Make sure actor in the phase of creating, that is the worker id is not nil and the
  // actor state is not alive yet.
  actor->UpdateAddress(RandomAddress());
  actor->UpdateLocalRayletAddress(RandomAddress());
  const auto &worker_id = actor->GetWorkerID();
  ASSERT_TRUE(!worker_id.IsNil());
  const auto &local_raylet_address = actor->LocalRayletAddress();
  ASSERT_TRUE(local_raylet_address.has_value());
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
      [](Status, std::function<void()>, std::function<void()>) {});
  io_service_.run_one();

  // Make sure the `KillLocalActor` rpc is sent.
  ASSERT_EQ(raylet_client_->killed_actors.size(), 1);
  ASSERT_EQ(raylet_client_->killed_actors.front(), actor->GetActorID());

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
      [&created_actors](std::shared_ptr<gcs::GcsActor> result_actor,
                        const rpc::PushTaskReply &,
                        const Status &) { created_actors.emplace_back(result_actor); }));

  ASSERT_EQ(created_actors.size(), 0);
  ASSERT_EQ(mock_actor_scheduler_->actors.size(), 1);
  auto actor = mock_actor_scheduler_->actors.back();
  mock_actor_scheduler_->actors.pop_back();

  // Check that the actor is in state `ALIVE`.
  auto address = RandomAddress();
  auto node_id = NodeID::FromBinary(address.node_id());
  actor->UpdateAddress(address);
  gcs_actor_manager_->OnActorCreationSuccess(actor, rpc::PushTaskReply());
  io_service_.run_one();
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
  address.set_node_id(node_id2.Binary());
  actor->UpdateAddress(address);
  gcs_actor_manager_->OnActorCreationSuccess(actor, rpc::PushTaskReply());
  io_service_.run_one();
  io_service_.run_one();
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
  rpc::RestartActorForLineageReconstructionRequest request;
  request.set_actor_id(actor->GetActorID().Binary());
  request.set_num_restarts_due_to_lineage_reconstruction(
      /*num_restarts_due_to_lineage_reconstruction=*/1);
  rpc::RestartActorForLineageReconstructionReply reply;
  gcs_actor_manager_->HandleRestartActorForLineageReconstruction(
      request, &reply, [](auto, auto, auto) {});
  io_service_.run_one();
  ASSERT_EQ(actor->GetState(), rpc::ActorTableData::RESTARTING);

  // Add node and check that the actor is restarted.
  ASSERT_EQ(mock_actor_scheduler_->actors.size(), 1);
  mock_actor_scheduler_->actors.clear();
  ASSERT_EQ(created_actors.size(), 1);
  auto node_id3 = NodeID::FromRandom();
  address.set_node_id(node_id3.Binary());
  actor->UpdateAddress(address);
  gcs_actor_manager_->OnActorCreationSuccess(actor, rpc::PushTaskReply());
  io_service_.run_one();
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
      [&created_actors](std::shared_ptr<gcs::GcsActor> result_actor,
                        const rpc::PushTaskReply &,
                        const Status &) { created_actors.emplace_back(result_actor); }));

  ASSERT_EQ(created_actors.size(), 0);
  ASSERT_EQ(mock_actor_scheduler_->actors.size(), 1);
  auto actor = mock_actor_scheduler_->actors.back();
  mock_actor_scheduler_->actors.pop_back();

  // Check that the actor is in state `ALIVE`.
  auto address = RandomAddress();
  actor->UpdateAddress(address);
  gcs_actor_manager_->OnActorCreationSuccess(actor, rpc::PushTaskReply());
  io_service_.run_one();
  ASSERT_EQ(created_actors.size(), 1);
  ASSERT_EQ(actor->GetState(), rpc::ActorTableData::ALIVE);

  // Remove owner node and then check that the actor is dead.
  const auto owner_node_id = actor->GetOwnerNodeID();
  EXPECT_CALL(*mock_actor_scheduler_, CancelOnNode(owner_node_id));
  OnNodeDead(owner_node_id);
  ASSERT_EQ(actor->GetState(), rpc::ActorTableData::DEAD);

  // Restart on an invalid or permanently dead actor should fail.
  rpc::RestartActorForLineageReconstructionRequest request;
  request.set_actor_id(
      ActorID::Of(actor->GetActorID().JobId(), RandomTaskId(), 0).Binary());
  request.set_num_restarts_due_to_lineage_reconstruction(
      /*num_restarts_due_to_lineage_reconstruction=*/0);
  rpc::RestartActorForLineageReconstructionReply reply;
  gcs_actor_manager_->HandleRestartActorForLineageReconstruction(
      request, &reply, [](auto, auto, auto) {});
  io_service_.run_one();
  io_service_.run_one();
  ASSERT_EQ(reply.status().code(), static_cast<int>(StatusCode::Invalid));

  rpc::RestartActorForLineageReconstructionRequest request2;
  request2.set_actor_id(actor->GetActorID().Binary());
  request2.set_num_restarts_due_to_lineage_reconstruction(
      /*num_restarts_due_to_lineage_reconstruction=*/0);
  rpc::RestartActorForLineageReconstructionReply reply2;
  gcs_actor_manager_->HandleRestartActorForLineageReconstruction(
      request2, &reply2, [](auto, auto, auto) {});
  ASSERT_EQ(reply2.status().code(), static_cast<int>(StatusCode::Invalid));
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
      [&created_actors](std::shared_ptr<gcs::GcsActor> result_actor,
                        const rpc::PushTaskReply &,
                        const Status &) { created_actors.emplace_back(result_actor); }));

  ASSERT_EQ(created_actors.size(), 0);
  ASSERT_EQ(mock_actor_scheduler_->actors.size(), 1);
  auto actor = mock_actor_scheduler_->actors.back();
  mock_actor_scheduler_->actors.pop_back();

  // Check that the actor is in state `ALIVE`.
  auto address = RandomAddress();
  actor->UpdateAddress(address);
  gcs_actor_manager_->OnActorCreationSuccess(actor, rpc::PushTaskReply());
  io_service_.run_one();
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

  gcs_actor_manager_->HandleRestartActorForLineageReconstruction(
      request, &reply1, [&reply1](Status, std::function<void()>, std::function<void()>) {
        ASSERT_EQ(reply1.status().code(), static_cast<int>(StatusCode::OK));
      });
  gcs_actor_manager_->HandleRestartActorForLineageReconstruction(
      request, &reply2, [&reply2](Status, std::function<void()>, std::function<void()>) {
        ASSERT_EQ(reply2.status().code(), static_cast<int>(StatusCode::OK));
      });
  io_service_.run_one();
  ASSERT_EQ(actor->GetState(), rpc::ActorTableData::RESTARTING);

  // Add node and check that the actor is restarted.
  ASSERT_EQ(mock_actor_scheduler_->actors.size(), 1);
  mock_actor_scheduler_->actors.clear();
  ASSERT_EQ(created_actors.size(), 1);
  auto node_id = NodeID::FromRandom();
  address.set_node_id(node_id.Binary());
  actor->UpdateAddress(address);
  gcs_actor_manager_->OnActorCreationSuccess(actor, rpc::PushTaskReply());
  io_service_.run_one();
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
  rpc::RestartActorForLineageReconstructionRequest request3;
  request3.set_actor_id(actor->GetActorID().Binary());
  request3.set_num_restarts_due_to_lineage_reconstruction(1);
  rpc::RestartActorForLineageReconstructionReply reply3;
  gcs_actor_manager_->HandleRestartActorForLineageReconstruction(
      request3, &reply3, [](auto, auto, auto) {});
  ASSERT_EQ(reply3.status().code(), static_cast<int>(StatusCode::OK));
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
      [&finished_actors](const std::shared_ptr<gcs::GcsActor> &result_actor,
                         const rpc::PushTaskReply &,
                         const Status &) { finished_actors.emplace_back(result_actor); });
  RAY_CHECK_OK(status);

  ASSERT_EQ(finished_actors.size(), 0);
  ASSERT_EQ(mock_actor_scheduler_->actors.size(), 1);
  auto actor = mock_actor_scheduler_->actors.back();
  mock_actor_scheduler_->actors.pop_back();

  // Make sure actor in the phase of creating, that is the worker id is not nil and the
  // actor state is not alive yet.
  actor->UpdateAddress(RandomAddress());
  actor->UpdateLocalRayletAddress(RandomAddress());
  const auto &worker_id = actor->GetWorkerID();
  ASSERT_TRUE(!worker_id.IsNil());
  const auto &local_raylet_address = actor->LocalRayletAddress();
  ASSERT_TRUE(local_raylet_address.has_value());
  ASSERT_NE(actor->GetState(), rpc::ActorTableData::ALIVE);

  // Then handle the kill actor requst (no restart).
  rpc::KillActorViaGcsReply reply;
  rpc::KillActorViaGcsRequest request;
  request.set_actor_id(actor->GetActorID().Binary());
  request.set_force_kill(true);
  // Set the `no_restart` flag to true so that the actor will be destoryed.
  request.set_no_restart(true);
  gcs_actor_manager_->HandleKillActorViaGcs(
      request,
      &reply,
      /*send_reply_callback*/
      [](Status, std::function<void()>, std::function<void()>) {});
  io_service_.run_one();
  io_service_.run_one();

  // Make sure the `KillLocalActor` rpc is sent.
  ASSERT_EQ(raylet_client_->killed_actors.size(), 1);
  ASSERT_EQ(raylet_client_->killed_actors.front(), actor->GetActorID());

  // Make sure the actor is dead.
  ASSERT_EQ(actor->GetState(), rpc::ActorTableData::DEAD);
}

TEST_F(GcsActorManagerTest, TestDestroyWhileRegistering) {
  // Register comes in -> Kill comes in -> Run all kv operations and callbacks
  auto register_request = GenRegisterActorRequest(
      JobID::FromInt(1), /*max_restarts=*/0, /*detached=*/false, "", "test");
  rpc::RegisterActorReply register_reply;
  gcs_actor_manager_->HandleRegisterActor(
      register_request, &register_reply, [](auto, auto, auto) {});
  rpc::KillActorViaGcsRequest kill_request;
  kill_request.set_actor_id(
      register_request.task_spec().actor_creation_task_spec().actor_id());
  kill_request.set_force_kill(false);
  kill_request.set_no_restart(true);
  rpc::KillActorViaGcsReply kill_reply;
  gcs_actor_manager_->HandleKillActorViaGcs(
      kill_request, &kill_reply, [](auto, auto, auto) {});
  // Run all kv operations and callbacks
  for (int i = 0; i < 5; i++) {
    io_service_.run_one();
  }
  ASSERT_EQ(register_reply.status().code(),
            static_cast<int>(StatusCode::SchedulingCancelled));
  ASSERT_EQ(kill_reply.status().code(), static_cast<int>(StatusCode::OK));
  ASSERT_EQ(raylet_client_->killed_actors.size(), 0);
  ASSERT_TRUE(gcs_actor_manager_->GetRegisteredActors().empty());
}

TEST_F(GcsActorManagerTest, TestRestartPreemptedActor) {
  // This test verifies that when an actor is preempted, calling OnWorkerDead
  // does not increment the num_restarts counter and still restarts the actor.
  auto job_id = JobID::FromInt(1);
  auto registered_actor = RegisterActor(job_id,
                                        /*max_restarts=*/1,
                                        /*detached=*/false);
  rpc::CreateActorRequest create_actor_request;
  create_actor_request.mutable_task_spec()->CopyFrom(
      registered_actor->GetCreationTaskSpecification().GetMessage());

  Status status =
      gcs_actor_manager_->CreateActor(create_actor_request,
                                      [](const std::shared_ptr<gcs::GcsActor> &,
                                         const rpc::PushTaskReply &,
                                         const Status &) {});
  RAY_CHECK_OK(status);

  ASSERT_EQ(mock_actor_scheduler_->actors.size(), 1);
  auto actor = mock_actor_scheduler_->actors.back();
  mock_actor_scheduler_->actors.pop_back();

  // Make the actor alive on a specific node
  auto address = RandomAddress();
  auto node_id = NodeID::FromBinary(address.node_id());
  auto worker_id = WorkerID::FromBinary(address.worker_id());
  actor->UpdateAddress(address);
  gcs_actor_manager_->OnActorCreationSuccess(actor, rpc::PushTaskReply());
  io_service_.run_one();
  ASSERT_EQ(actor->GetState(), rpc::ActorTableData::ALIVE);

  // Initially num_restarts should be 0
  ASSERT_EQ(actor->GetActorTableData().num_restarts(), 0);
  ASSERT_FALSE(actor->GetActorTableData().preempted());

  // First restart: actor is NOT preempted, so num_restarts should increment
  gcs_actor_manager_->OnWorkerDead(node_id, worker_id);
  ASSERT_EQ(actor->GetState(), rpc::ActorTableData::RESTARTING);
  ASSERT_EQ(actor->GetActorTableData().num_restarts(), 1);  // Should increment
  ASSERT_FALSE(actor->GetActorTableData().preempted());

  // Make the actor alive on a specific node again.
  auto new_address = RandomAddress();
  auto new_node_id = NodeID::FromBinary(new_address.node_id());
  auto new_worker_id = WorkerID::FromBinary(new_address.worker_id());
  actor->UpdateAddress(new_address);
  gcs_actor_manager_->OnActorCreationSuccess(actor, rpc::PushTaskReply());
  io_service_.run_one();
  ASSERT_EQ(actor->GetState(), rpc::ActorTableData::ALIVE);
  ASSERT_EQ(actor->GetActorTableData().num_restarts(), 1);
  ASSERT_EQ(actor->GetActorTableData().num_restarts_due_to_node_preemption(), 0);

  // Now set the actor as preempted using SetPreemptedAndPublish
  gcs_actor_manager_->SetPreemptedAndPublish(new_node_id);
  io_service_.run_one();
  ASSERT_TRUE(actor->GetActorTableData().preempted());

  // Second restart: actor is preempted, so num_restarts and
  // num_restarts_due_to_node_preemption should increment
  gcs_actor_manager_->OnWorkerDead(new_node_id, new_worker_id);
  ASSERT_EQ(actor->GetState(), rpc::ActorTableData::RESTARTING);
  ASSERT_EQ(actor->GetActorTableData().num_restarts(), 2);  // Should increment
  ASSERT_EQ(actor->GetActorTableData().num_restarts_due_to_node_preemption(), 1);

  // Make the actor alive on another node again
  auto new_address_2 = RandomAddress();
  auto new_node_id_2 = NodeID::FromBinary(new_address_2.node_id());
  auto new_worker_id_2 = WorkerID::FromBinary(new_address_2.worker_id());
  actor->UpdateAddress(new_address_2);
  gcs_actor_manager_->OnActorCreationSuccess(actor, rpc::PushTaskReply());
  io_service_.run_one();
  ASSERT_EQ(actor->GetState(), rpc::ActorTableData::ALIVE);
  ASSERT_EQ(actor->GetActorTableData().num_restarts(), 2);
  ASSERT_EQ(actor->GetActorTableData().num_restarts_due_to_node_preemption(), 1);
  ASSERT_FALSE(actor->GetActorTableData().preempted());  // Turn preempted back

  // Third restart: actor reaches max_restarts, so num_restarts and
  // num_restarts_due_to_node_preemption should not increment
  gcs_actor_manager_->OnWorkerDead(new_node_id_2, new_worker_id_2);
  ASSERT_EQ(actor->GetState(), rpc::ActorTableData::DEAD);
  ASSERT_EQ(actor->GetActorTableData().num_restarts(), 2);
  ASSERT_EQ(actor->GetActorTableData().num_restarts_due_to_node_preemption(), 1);
  ASSERT_FALSE(actor->GetActorTableData().preempted());
}

}  // namespace gcs

}  // namespace ray
