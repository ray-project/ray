// Copyright 2025 The Ray Authors.
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

#include "ray/raylet/node_manager.h"

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "gmock/gmock.h"
#include "mock/ray/core_worker/experimental_mutable_object_provider.h"
#include "mock/ray/gcs/gcs_client/gcs_client.h"
#include "mock/ray/object_manager/object_directory.h"
#include "mock/ray/object_manager/object_manager.h"
#include "mock/ray/object_manager/plasma/client.h"
#include "mock/ray/pubsub/subscriber.h"
#include "mock/ray/raylet/local_task_manager.h"
#include "mock/ray/raylet/worker_pool.h"
#include "mock/ray/rpc/worker/core_worker_client.h"
#include "ray/raylet/scheduling/cluster_task_manager.h"
#include "ray/raylet/test/util.h"

namespace ray::raylet {
using ::testing::_;
using ::testing::Return;

namespace {

TaskSpecification BuildTaskSpec(
    const std::unordered_map<std::string, double> &resources) {
  TaskSpecBuilder builder;
  rpc::Address empty_address;
  rpc::JobConfig config;
  FunctionDescriptor function_descriptor =
      FunctionDescriptorBuilder::BuildPython("x", "", "", "");
  builder.SetCommonTaskSpec(TaskID::FromRandom(JobID::Nil()),
                            "dummy_task",
                            Language::PYTHON,
                            function_descriptor,
                            JobID::Nil(),
                            config,
                            TaskID::Nil(),
                            0,
                            TaskID::Nil(),
                            empty_address,
                            1,
                            false,
                            false,
                            -1,
                            resources,
                            resources,
                            "",
                            0,
                            TaskID::Nil(),
                            "");
  return std::move(builder).ConsumeAndBuild();
}

}  // namespace

TEST(NodeManagerStaticTest, TestHandleReportWorkerBacklog) {
  {
    // Worker backlog report from a disconnected worker should be ignored.
    MockWorkerPool worker_pool;
    MockLocalTaskManager local_task_manager;

    WorkerID worker_id = WorkerID::FromRandom();
    EXPECT_CALL(worker_pool, GetRegisteredWorker(worker_id))
        .Times(1)
        .WillOnce(Return(nullptr));
    EXPECT_CALL(worker_pool, GetRegisteredDriver(worker_id))
        .Times(1)
        .WillOnce(Return(nullptr));
    EXPECT_CALL(local_task_manager, ClearWorkerBacklog(_)).Times(0);
    EXPECT_CALL(local_task_manager, SetWorkerBacklog(_, _, _)).Times(0);

    rpc::ReportWorkerBacklogRequest request;
    request.set_worker_id(worker_id.Binary());
    rpc::ReportWorkerBacklogReply reply;
    NodeManager::HandleReportWorkerBacklog(
        request,
        &reply,
        [](Status status, std::function<void()> success, std::function<void()> failure) {
        },
        worker_pool,
        local_task_manager);
  }

  {
    // Worker backlog report from a connected driver should be recorded.
    MockWorkerPool worker_pool;
    MockLocalTaskManager local_task_manager;

    WorkerID worker_id = WorkerID::FromRandom();
    std::shared_ptr<MockWorker> driver = std::make_shared<MockWorker>(worker_id, 10);

    rpc::ReportWorkerBacklogRequest request;
    request.set_worker_id(worker_id.Binary());
    auto backlog_report_1 = request.add_backlog_reports();
    auto task_spec_1 = BuildTaskSpec({{"CPU", 1}});
    backlog_report_1->mutable_resource_spec()->CopyFrom(task_spec_1.GetMessage());
    backlog_report_1->set_backlog_size(1);

    auto backlog_report_2 = request.add_backlog_reports();
    auto task_spec_2 = BuildTaskSpec({{"GPU", 2}});
    backlog_report_2->mutable_resource_spec()->CopyFrom(task_spec_2.GetMessage());
    backlog_report_2->set_backlog_size(3);
    rpc::ReportWorkerBacklogReply reply;

    EXPECT_CALL(worker_pool, GetRegisteredWorker(worker_id))
        .Times(1)
        .WillOnce(Return(nullptr));
    EXPECT_CALL(worker_pool, GetRegisteredDriver(worker_id))
        .Times(1)
        .WillOnce(Return(driver));
    EXPECT_CALL(local_task_manager, ClearWorkerBacklog(worker_id)).Times(1);
    EXPECT_CALL(local_task_manager,
                SetWorkerBacklog(task_spec_1.GetSchedulingClass(), worker_id, 1))
        .Times(1);
    EXPECT_CALL(local_task_manager,
                SetWorkerBacklog(task_spec_2.GetSchedulingClass(), worker_id, 3))
        .Times(1);

    NodeManager::HandleReportWorkerBacklog(
        request,
        &reply,
        [](Status status, std::function<void()> success, std::function<void()> failure) {
        },
        worker_pool,
        local_task_manager);
  }

  {
    // Worker backlog report from a connected worker should be recorded.
    MockWorkerPool worker_pool;
    MockLocalTaskManager local_task_manager;

    WorkerID worker_id = WorkerID::FromRandom();
    std::shared_ptr<MockWorker> worker = std::make_shared<MockWorker>(worker_id, 10);

    rpc::ReportWorkerBacklogRequest request;
    request.set_worker_id(worker_id.Binary());
    auto backlog_report_1 = request.add_backlog_reports();
    auto task_spec_1 = BuildTaskSpec({{"CPU", 1}});
    backlog_report_1->mutable_resource_spec()->CopyFrom(task_spec_1.GetMessage());
    backlog_report_1->set_backlog_size(1);

    auto backlog_report_2 = request.add_backlog_reports();
    auto task_spec_2 = BuildTaskSpec({{"GPU", 2}});
    backlog_report_2->mutable_resource_spec()->CopyFrom(task_spec_2.GetMessage());
    backlog_report_2->set_backlog_size(3);
    rpc::ReportWorkerBacklogReply reply;

    EXPECT_CALL(worker_pool, GetRegisteredWorker(worker_id))
        .Times(1)
        .WillOnce(Return(worker));
    EXPECT_CALL(worker_pool, GetRegisteredDriver(worker_id)).Times(0);

    EXPECT_CALL(local_task_manager, ClearWorkerBacklog(worker_id)).Times(1);
    EXPECT_CALL(local_task_manager,
                SetWorkerBacklog(task_spec_1.GetSchedulingClass(), worker_id, 1))
        .Times(1);
    EXPECT_CALL(local_task_manager,
                SetWorkerBacklog(task_spec_2.GetSchedulingClass(), worker_id, 3))
        .Times(1);

    NodeManager::HandleReportWorkerBacklog(
        request,
        &reply,
        [](Status status, std::function<void()> success, std::function<void()> failure) {
        },
        worker_pool,
        local_task_manager);
  }
}

class NodeManagerTest : public ::testing::Test {
 public:
  NodeManagerTest()
      : client_call_manager_(io_service_, /*record_stats=*/false),
        worker_rpc_pool_([](const auto &) {
          return std::make_shared<rpc::MockCoreWorkerClientInterface>();
        }) {
    RayConfig::instance().initialize(R"({
      "raylet_liveness_self_check_interval_ms": 100
    })");

    NodeManagerConfig node_manager_config{};
    node_manager_config.maximum_startup_concurrency = 1;
    node_manager_config.store_socket_name = "test_store_socket";

    auto core_worker_subscriber = std::make_unique<pubsub::MockSubscriber>();
    auto object_directory = std::make_unique<MockObjectDirectory>();
    mock_object_directory_ = object_directory.get();
    auto object_manager = std::make_unique<MockObjectManager>();
    mock_object_manager_ = object_manager.get();

    EXPECT_CALL(*mock_object_manager_, GetMemoryCapacity()).WillRepeatedly(Return(0));

    EXPECT_CALL(mock_store_client_, Connect(node_manager_config.store_socket_name, _, _))
        .WillOnce(Return(Status::OK()));

    auto mutable_object_provider =
        std::make_unique<core::experimental::MockMutableObjectProvider>();
    mock_mutable_object_provider_ = mutable_object_provider.get();

    EXPECT_CALL(mock_worker_pool_, SetNodeManagerPort(_)).Times(1);
    EXPECT_CALL(mock_worker_pool_, SetRuntimeEnvAgentClient(_)).Times(1);
    EXPECT_CALL(mock_worker_pool_, Start()).Times(1);

    EXPECT_CALL(mock_worker_pool_, DebugString()).WillRepeatedly(Return(""));
    EXPECT_CALL(*mock_gcs_client_, DebugString()).WillRepeatedly(Return(""));
    EXPECT_CALL(*mock_object_manager_, DebugString()).WillRepeatedly(Return(""));
    EXPECT_CALL(*mock_object_directory_, DebugString()).WillRepeatedly(Return(""));
    EXPECT_CALL(*core_worker_subscriber, DebugString()).WillRepeatedly(Return(""));

    raylet_node_id_ = NodeID::FromRandom();

    local_object_manager_ = std::make_shared<LocalObjectManager>(
        raylet_node_id_,
        node_manager_config.node_manager_address,
        node_manager_config.node_manager_port,
        io_service_,
        RayConfig::instance().free_objects_batch_size(),
        RayConfig::instance().free_objects_period_milliseconds(),
        mock_worker_pool_,
        worker_rpc_pool_,
        /*max_io_workers*/ node_manager_config.max_io_workers,
        /*is_external_storage_type_fs*/
        RayConfig::instance().is_external_storage_type_fs(),
        /*max_fused_object_count*/ RayConfig::instance().max_fused_object_count(),
        /*on_objects_freed*/
        [&](const std::vector<ObjectID> &object_ids) {
          mock_object_manager_->FreeObjects(object_ids,
                                            /*local_only=*/false);
        },
        /*is_plasma_object_spillable*/
        [&](const ObjectID &object_id) {
          return mock_object_manager_->IsPlasmaObjectSpillable(object_id);
        },
        /*core_worker_subscriber_=*/core_worker_subscriber.get(),
        mock_object_directory_);

    dependency_manager_ = std::make_shared<DependencyManager>(*mock_object_manager_);

    cluster_resource_scheduler_ = std::make_shared<ClusterResourceScheduler>(
        io_service_,
        ray::scheduling::NodeID(raylet_node_id_.Binary()),
        node_manager_config.resource_config.GetResourceMap(),
        /*is_node_available_fn*/
        [&](ray::scheduling::NodeID node_id) {
          return mock_gcs_client_->Nodes().Get(NodeID::FromBinary(node_id.Binary())) !=
                 nullptr;
        },
        /*get_used_object_store_memory*/
        [&]() {
          if (RayConfig::instance().scheduler_report_pinned_bytes_only()) {
            // Get the current bytes used by local primary object copies.  This
            // is used to help node scale down decisions. A node can only be
            // safely drained when this function reports zero.
            int64_t bytes_used = local_object_manager_->GetPrimaryBytes();
            // Report nonzero if we have objects spilled to the local filesystem.
            if (bytes_used == 0 && local_object_manager_->HasLocallySpilledObjects()) {
              bytes_used = 1;
            }
            return bytes_used;
          }
          return mock_object_manager_->GetUsedMemory();
        },
        /*get_pull_manager_at_capacity*/
        [&]() { return mock_object_manager_->PullManagerHasPullsQueued(); },
        [](const ray::rpc::NodeDeathInfo &node_death_info) {},
        /*labels*/
        node_manager_config.labels);

    auto get_node_info_func = [&](const NodeID &node_id) {
      return mock_gcs_client_->Nodes().Get(node_id);
    };

    auto max_task_args_memory = static_cast<int64_t>(
        static_cast<float>(mock_object_manager_->GetMemoryCapacity()) *
        RayConfig::instance().max_task_args_memory_fraction());

    local_task_manager_ = std::make_shared<LocalTaskManager>(
        raylet_node_id_,
        *std::dynamic_pointer_cast<ClusterResourceScheduler>(cluster_resource_scheduler_),
        *dependency_manager_,
        get_node_info_func,
        mock_worker_pool_,
        leased_workers_,
        [&](const std::vector<ObjectID> &object_ids,
            std::vector<std::unique_ptr<RayObject>> *results) {
          return node_manager_->GetObjectsFromPlasma(object_ids, results);
        },
        max_task_args_memory);

    cluster_task_manager_ = std::make_shared<ClusterTaskManager>(
        raylet_node_id_,
        *std::dynamic_pointer_cast<ClusterResourceScheduler>(cluster_resource_scheduler_),
        get_node_info_func,
        [](const ray::RayTask &task) {},
        *local_task_manager_);

    node_manager_ = std::make_unique<NodeManager>(io_service_,
                                                  raylet_node_id_,
                                                  "test_node_name",
                                                  node_manager_config,
                                                  mock_gcs_client_,
                                                  client_call_manager_,
                                                  worker_rpc_pool_,
                                                  std::move(core_worker_subscriber),
                                                  cluster_resource_scheduler_,
                                                  local_task_manager_,
                                                  cluster_task_manager_,
                                                  std::move(object_directory),
                                                  std::move(object_manager),
                                                  *local_object_manager_,
                                                  *dependency_manager_,
                                                  mock_worker_pool_,
                                                  leased_workers_,
                                                  mock_store_client_,
                                                  std::move(mutable_object_provider),
                                                  /*shutdown_raylet_gracefully=*/
                                                  [](const auto &) {});
  }

  instrumented_io_context io_service_;
  rpc::ClientCallManager client_call_manager_;
  rpc::CoreWorkerClientPool worker_rpc_pool_;

  NodeID raylet_node_id_;
  std::shared_ptr<ClusterResourceScheduler> cluster_resource_scheduler_;
  std::shared_ptr<LocalTaskManager> local_task_manager_;
  std::shared_ptr<ClusterTaskManagerInterface> cluster_task_manager_;
  std::shared_ptr<LocalObjectManager> local_object_manager_;
  std::shared_ptr<DependencyManager> dependency_manager_;
  std::shared_ptr<gcs::MockGcsClient> mock_gcs_client_ =
      std::make_shared<gcs::MockGcsClient>();
  MockObjectDirectory *mock_object_directory_;
  MockObjectManager *mock_object_manager_;
  core::experimental::MockMutableObjectProvider *mock_mutable_object_provider_;
  plasma::MockPlasmaClient mock_store_client_;

  std::unique_ptr<NodeManager> node_manager_;
  MockWorkerPool mock_worker_pool_;
  absl::flat_hash_map<WorkerID, std::shared_ptr<WorkerInterface>> leased_workers_;
};

TEST_F(NodeManagerTest, TestRegisterGcsAndCheckSelfAlive) {
  EXPECT_CALL(*mock_gcs_client_->mock_node_accessor, AsyncSubscribeToNodeChange(_, _))
      .WillOnce(Return(Status::OK()));
  EXPECT_CALL(*mock_gcs_client_->mock_worker_accessor,
              AsyncSubscribeToWorkerFailures(_, _))
      .WillOnce(Return(Status::OK()));
  EXPECT_CALL(*mock_gcs_client_->mock_job_accessor, AsyncSubscribeAll(_, _))
      .WillOnce(Return(Status::OK()));
  EXPECT_CALL(mock_worker_pool_, GetAllRegisteredWorkers(_, _))
      .WillRepeatedly(Return(std::vector<std::shared_ptr<WorkerInterface>>{}));
  EXPECT_CALL(mock_worker_pool_, GetAllRegisteredDrivers(_))
      .WillRepeatedly(Return(std::vector<std::shared_ptr<WorkerInterface>>{}));
  EXPECT_CALL(mock_worker_pool_, IsWorkerAvailableForScheduling())
      .WillRepeatedly(Return(false));
  std::promise<void> promise;
  EXPECT_CALL(*mock_gcs_client_->mock_node_accessor, AsyncCheckSelfAlive(_, _))
      .WillOnce([&promise](const auto &, const auto &) {
        promise.set_value();
        return Status::OK();
      });
  RAY_CHECK_OK(node_manager_->RegisterGcs());
  std::thread thread{[this] {
    // Run the io_service in a separate thread to avoid blocking the main thread.
    auto work_guard = boost::asio::make_work_guard(io_service_);
    io_service_.run();
  }};
  auto future = promise.get_future();
  EXPECT_EQ(future.wait_for(std::chrono::seconds(1)), std::future_status::ready);
  io_service_.stop();
  thread.join();
}

}  // namespace ray::raylet

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
