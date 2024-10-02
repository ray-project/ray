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

#include <chrono>
#include <memory>
#include <thread>

// clang-format off
#include "gtest/gtest.h"
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/test_util.h"
#include "ray/gcs/gcs_server/test/gcs_server_test_util.h"
#include "ray/gcs/test/gcs_test_util.h"
#include "ray/gcs/gcs_server/gcs_kv_manager.h"
#include "mock/ray/gcs/gcs_server/gcs_kv_manager.h"
#include "mock/ray/gcs/gcs_server/gcs_node_manager.h"
#include "mock/ray/pubsub/publisher.h"
#include "ray/util/event.h"
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
  MockWorkerClient(instrumented_io_context &io_service) : io_service_(io_service) {}

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

class GcsActorManagerTest : public ::testing::Test {
 public:
  GcsActorManagerTest()
      : mock_actor_scheduler_(new MockActorScheduler()), periodical_runner_(io_service_) {
    RayConfig::instance().initialize(
        R"(
{
  "maximum_gcs_destroyed_actor_cached_count": 10,
  "enable_export_api_write": true
}
  )");
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
        mock_actor_scheduler_,
        gcs_table_storage_,
        gcs_publisher_,
        *runtime_env_mgr_,
        *function_manager_,
        [](const ActorID &actor_id) {},
        [this](const rpc::Address &addr) { return worker_client_; });

    for (int i = 1; i <= 10; i++) {
      auto job_id = JobID::FromInt(i);
      job_namespace_table_[job_id] = "";
    }
    log_dir_ = "event_123";
  }

  virtual ~GcsActorManagerTest() {
    io_service_.stop();
    thread_io_service_->join();
    std::filesystem::remove_all(log_dir_.c_str());
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
  absl::Mutex mutex_;
  std::unique_ptr<gcs::GcsFunctionManager> function_manager_;
  std::unique_ptr<gcs::MockInternalKVInterface> kv_;
  PeriodicalRunner periodical_runner_;
  std::string log_dir_;
};

TEST_F(GcsActorManagerTest, TestBasic) {
  std::vector<SourceTypeVariant> source_types = {
      rpc::ExportEvent_SourceType::ExportEvent_SourceType_EXPORT_ACTOR};
  RayEventInit(source_types, absl::flat_hash_map<std::string, std::string>(), log_dir_);
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

  // Check correct export events are written for each of the 4 state transitions
  int num_retry = 5;
  int num_export_events = 4;
  std::vector<std::string> expected_states = {
      "DEPENDENCIES_UNREADY", "PENDING_CREATION", "ALIVE", "DEAD"};
  std::vector<std::string> vc;
  for (int i = 0; i < num_retry; i++) {
    Mocker::ReadContentFromFile(vc, log_dir_ + "/export_events/event_EXPORT_ACTOR.log");
    if ((int)vc.size() == num_export_events) {
      for (int event_idx = 0; event_idx < num_export_events; event_idx++) {
        json export_event_as_json = json::parse(vc[event_idx]);
        json event_data = export_event_as_json["event_data"].get<json>();
        ASSERT_EQ(event_data["state"], expected_states[event_idx]);
        if (event_idx == num_export_events - 1) {
          // Verify death cause for last actor DEAD event
          ASSERT_EQ(
              event_data["death_cause"]["actor_died_error_context"]["error_message"],
              "The actor is dead because all references to the actor were removed "
              "including lineage ref count.");
        }
      }
      return;
    } else {
      // Sleep and retry
      std::this_thread::sleep_for(std::chrono::seconds(1));
      vc.clear();
    }
  }
  Mocker::ReadContentFromFile(vc, log_dir_ + "/export_events/event_EXPORT_ACTOR.log");
  std::ostringstream lines;
  for (auto line : vc) {
    lines << line << "\n";
  }
  ASSERT_TRUE(false) << "Export API wrote " << (int)vc.size() << " lines, but expecting "
                     << num_export_events << ".\nLines:\n"
                     << lines.str();
}

}  // namespace ray
