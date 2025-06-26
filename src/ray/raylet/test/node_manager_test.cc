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

TaskSpecBuilder DetachedActorCreationTaskBuilder(const rpc::Address &owner_address,
                                                 const ActorID &actor_id) {
  rpc::JobConfig config;
  const FunctionDescriptor function_descriptor =
      FunctionDescriptorBuilder::BuildPython("x", "", "", "");
  TaskSpecBuilder task_spec_builder;
  task_spec_builder.SetCommonTaskSpec(TaskID::FromRandom(JobID::Nil()),
                                      "dummy_task",
                                      Language::PYTHON,
                                      function_descriptor,
                                      JobID::Nil(),
                                      config,
                                      TaskID::Nil(),
                                      0,
                                      TaskID::Nil(),
                                      owner_address,
                                      1,
                                      false,
                                      false,
                                      -1,
                                      {{"CPU", 0}},
                                      {{"CPU", 0}},
                                      "",
                                      0,
                                      TaskID::Nil(),
                                      "");
  task_spec_builder.SetActorCreationTaskSpec(actor_id,
                                             /*serialized_actor_handle=*/"",
                                             rpc::SchedulingStrategy(),
                                             /*max_restarts=*/0,
                                             /*max_task_retries=*/0,
                                             /*dynamic_worker_options=*/{},
                                             /*max_concurrency=*/1,
                                             /*is_detached=*/true,
                                             /*name=*/"",
                                             /*ray_namespace=*/"",
                                             /*is_asyncio=*/false,
                                             /*concurrency_groups=*/{},
                                             /*extension_data=*/"",
                                             /*execute_out_of_order=*/false,
                                             /*root_detached_actor_id=*/actor_id);
  return task_spec_builder;
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
      "raylet_liveness_self_check_interval_ms": 100,
      "kill_worker_timeout_milliseconds": 10
    })");

    NodeManagerConfig node_manager_config{};
    node_manager_config.maximum_startup_concurrency = 1;
    node_manager_config.store_socket_name = "test_store_socket";

    core_worker_subscriber_ = std::make_unique<pubsub::MockSubscriber>();
    mock_object_directory_ = std::make_unique<MockObjectDirectory>();
    mock_object_manager_ = std::make_unique<MockObjectManager>();

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
    EXPECT_CALL(*core_worker_subscriber_, DebugString()).WillRepeatedly(Return(""));

    raylet_node_id_ = NodeID::FromRandom();

    local_object_manager_ = std::make_unique<LocalObjectManager>(
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
        core_worker_subscriber_.get(),
        mock_object_directory_.get());

    dependency_manager_ = std::make_unique<DependencyManager>(*mock_object_manager_);

    cluster_resource_scheduler_ = std::make_unique<ClusterResourceScheduler>(
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

    local_task_manager_ = std::make_unique<LocalTaskManager>(
        raylet_node_id_,
        *cluster_resource_scheduler_,
        *dependency_manager_,
        get_node_info_func,
        mock_worker_pool_,
        leased_workers_,
        [&](const std::vector<ObjectID> &object_ids,
            std::vector<std::unique_ptr<RayObject>> *results) {
          return node_manager_->GetObjectsFromPlasma(object_ids, results);
        },
        max_task_args_memory);

    cluster_task_manager_ = std::make_unique<ClusterTaskManager>(
        raylet_node_id_,
        *cluster_resource_scheduler_,
        get_node_info_func,
        [](const ray::RayTask &task) {},
        *local_task_manager_);

    node_manager_ = std::make_unique<NodeManager>(io_service_,
                                                  raylet_node_id_,
                                                  "test_node_name",
                                                  node_manager_config,
                                                  *mock_gcs_client_,
                                                  client_call_manager_,
                                                  worker_rpc_pool_,
                                                  *core_worker_subscriber_,
                                                  *cluster_resource_scheduler_,
                                                  *local_task_manager_,
                                                  *cluster_task_manager_,
                                                  *mock_object_directory_,
                                                  *mock_object_manager_,
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
  std::unique_ptr<pubsub::MockSubscriber> core_worker_subscriber_;
  std::unique_ptr<ClusterResourceScheduler> cluster_resource_scheduler_;
  std::unique_ptr<LocalTaskManager> local_task_manager_;
  std::unique_ptr<ClusterTaskManagerInterface> cluster_task_manager_;
  std::unique_ptr<LocalObjectManager> local_object_manager_;
  std::unique_ptr<DependencyManager> dependency_manager_;
  std::unique_ptr<gcs::MockGcsClient> mock_gcs_client_ =
      std::make_unique<gcs::MockGcsClient>();
  std::unique_ptr<MockObjectDirectory> mock_object_directory_;
  std::unique_ptr<MockObjectManager> mock_object_manager_;
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

TEST_F(NodeManagerTest, TestDetachedWorkerIsKilledByFailedWorker) {
  EXPECT_CALL(*mock_gcs_client_->mock_node_accessor, AsyncSubscribeToNodeChange(_, _))
      .WillOnce(Return(Status::OK()));
  EXPECT_CALL(*mock_gcs_client_->mock_job_accessor, AsyncSubscribeAll(_, _))
      .WillOnce(Return(Status::OK()));
  EXPECT_CALL(*mock_gcs_client_->mock_node_accessor, AsyncCheckSelfAlive(_, _))
      .WillRepeatedly(Return(Status::OK()));
  EXPECT_CALL(mock_worker_pool_, GetAllRegisteredWorkers(_, _))
      .WillRepeatedly(Return(std::vector<std::shared_ptr<WorkerInterface>>{}));
  EXPECT_CALL(mock_worker_pool_, GetAllRegisteredDrivers(_))
      .WillRepeatedly(Return(std::vector<std::shared_ptr<WorkerInterface>>{}));
  EXPECT_CALL(mock_worker_pool_, IsWorkerAvailableForScheduling())
      .WillRepeatedly(Return(false));
  EXPECT_CALL(mock_worker_pool_, PrestartWorkers(_, _)).Times(1);

  std::promise<void> pop_worker_callback_promise;
  PopWorkerCallback pop_worker_callback;
  gcs::ItemCallback<rpc::WorkerDeltaData> publish_worker_failure_callback;

  // Save the publish_worker_failure_callback for publishing a worker failure event later.
  EXPECT_CALL(*mock_gcs_client_->mock_worker_accessor,
              AsyncSubscribeToWorkerFailures(_, _))
      .WillOnce([&](const gcs::ItemCallback<rpc::WorkerDeltaData> &subscribe,
                    const gcs::StatusCallback &done) {
        publish_worker_failure_callback = subscribe;
        return Status::OK();
      });

  // Save the pop_worker_callback for providing a mock worker later.
  EXPECT_CALL(mock_worker_pool_, PopWorker(_, _))
      .WillOnce(
          [&](const TaskSpecification &task_spec, const PopWorkerCallback &callback) {
            pop_worker_callback = callback;
            pop_worker_callback_promise.set_value();
          });

  // Invoke RegisterGcs and io_service_.run() so that the above EXPECT_CALLs can be
  // triggered.
  RAY_CHECK_OK(node_manager_->RegisterGcs());
  std::thread io_thread{[&] {
    // Run the io_service in a separate thread to avoid blocking the main thread.
    auto work_guard = boost::asio::make_work_guard(io_service_);
    io_service_.run();
  }};

  std::thread grpc_client_thread{[&] {
    // Run the grpc client in a separate thread to avoid blocking the main thread.

    // Preparing a detached actor creation task spec for the later RequestWorkerLease rpc.
    const auto owner_worker_id = WorkerID::FromRandom();
    rpc::Address owner_address;
    owner_address.set_worker_id(owner_worker_id.Binary());
    const auto actor_id =
        ActorID::Of(JobID::FromInt(1), TaskID::FromRandom(JobID::FromInt(1)), 0);
    const auto task_spec_builder =
        DetachedActorCreationTaskBuilder(owner_address, actor_id);

    // Invoke RequestWorkerLease to request a leased worker for the task in the
    // NodeManager.
    grpc::ClientContext context;
    rpc::RequestWorkerLeaseReply reply;
    rpc::RequestWorkerLeaseRequest request;
    request.mutable_resource_spec()->CopyFrom(task_spec_builder.GetMessage());
    auto channel =
        grpc::CreateChannel("localhost:" + std::to_string(node_manager_->GetServerPort()),
                            grpc::InsecureChannelCredentials());
    auto stub = rpc::NodeManagerService::NewStub(channel);
    auto status = stub->RequestWorkerLease(&context, request, &reply);
    EXPECT_TRUE(status.ok());

    // After RequestWorkerLease, a leased worker is ready in the NodeManager.
    // Then use publish_worker_failure_callback to say owner_worker_id is dead.
    // The leased worker should not be killed by this because it is a detached actor.
    rpc::WorkerDeltaData delta_data;
    delta_data.set_worker_id(owner_worker_id.Binary());
    publish_worker_failure_callback(std::move(delta_data));
  }};

  // Prepare a mock worker with a real process so that we can check if the process is
  // alive later.
  const auto worker = std::make_shared<MockWorker>(WorkerID::FromRandom(), 10);
  auto [proc, spawn_error] =
      Process::Spawn(std::vector<std::string>{"sleep", "1000"}, true);
  EXPECT_FALSE(spawn_error);
  worker->SetProcess(proc);
  // Complete the RequestWorkerLease rpc with the mock worker.
  pop_worker_callback_promise.get_future().wait();
  io_service_.post([&] { pop_worker_callback(worker, PopWorkerStatus::OK, ""); },
                   "pop_worker_callback");

  // Wait for the client thead to complete. This waits for the RequestWorkerLease call
  // and publish_worker_failure_callback to finish.
  grpc_client_thread.join();
  // Wait for more than kill_worker_timeout_milliseconds.
  std::this_thread::sleep_for(std::chrono::seconds(1));
  // The process should still be alive because it should not be killed by
  // publish_worker_failure_callback.
  EXPECT_TRUE(proc.IsAlive());
  // clean up.
  proc.Kill();
  io_service_.stop();
  io_thread.join();
}

TEST_F(NodeManagerTest, TestDetachedWorkerIsKilledByFailedNode) {
  EXPECT_CALL(*mock_object_directory_, HandleNodeRemoved(_)).Times(1);
  EXPECT_CALL(*mock_object_manager_, HandleNodeRemoved(_)).Times(1);
  EXPECT_CALL(*mock_gcs_client_->mock_worker_accessor,
              AsyncSubscribeToWorkerFailures(_, _))
      .WillOnce(Return(Status::OK()));
  EXPECT_CALL(*mock_gcs_client_->mock_job_accessor, AsyncSubscribeAll(_, _))
      .WillOnce(Return(Status::OK()));
  EXPECT_CALL(*mock_gcs_client_->mock_node_accessor, AsyncCheckSelfAlive(_, _))
      .WillRepeatedly(Return(Status::OK()));
  EXPECT_CALL(mock_worker_pool_, GetAllRegisteredWorkers(_, _))
      .WillRepeatedly(Return(std::vector<std::shared_ptr<WorkerInterface>>{}));
  EXPECT_CALL(mock_worker_pool_, GetAllRegisteredDrivers(_))
      .WillRepeatedly(Return(std::vector<std::shared_ptr<WorkerInterface>>{}));
  EXPECT_CALL(mock_worker_pool_, IsWorkerAvailableForScheduling())
      .WillRepeatedly(Return(false));
  EXPECT_CALL(mock_worker_pool_, PrestartWorkers(_, _)).Times(1);

  std::promise<void> pop_worker_callback_promise;
  PopWorkerCallback pop_worker_callback;
  std::function<void(const NodeID &id, rpc::GcsNodeInfo &&node_info)>
      publish_node_change_callback;

  // Save the publish_node_change_callback for publishing a node failure event later.
  EXPECT_CALL(*mock_gcs_client_->mock_node_accessor, AsyncSubscribeToNodeChange(_, _))
      .WillOnce([&](const gcs::SubscribeCallback<NodeID, rpc::GcsNodeInfo> &subscribe,
                    const gcs::StatusCallback &done) {
        publish_node_change_callback = subscribe;
        return Status::OK();
      });

  // Save the pop_worker_callback for providing a mock worker later.
  EXPECT_CALL(mock_worker_pool_, PopWorker(_, _))
      .WillOnce(
          [&](const TaskSpecification &task_spec, const PopWorkerCallback &callback) {
            pop_worker_callback = callback;
            pop_worker_callback_promise.set_value();
          });

  // Invoke RegisterGcs and io_service_.run() so that the above EXPECT_CALLs can be
  // triggered.
  RAY_CHECK_OK(node_manager_->RegisterGcs());
  std::thread io_thread{[&] {
    // Run the io_service in a separate thread to avoid blocking the main thread.
    auto work_guard = boost::asio::make_work_guard(io_service_);
    io_service_.run();
  }};

  std::thread grpc_client_thread{[&] {
    // Run the grpc client in a separate thread to avoid blocking the main thread.

    // Preparing a detached actor creation task spec for the later RequestWorkerLease rpc.
    const auto owner_node_id = NodeID::FromRandom();
    rpc::Address owner_address;
    owner_address.set_raylet_id(owner_node_id.Binary());
    const auto actor_id =
        ActorID::Of(JobID::FromInt(1), TaskID::FromRandom(JobID::FromInt(1)), 0);
    const auto task_spec_builder =
        DetachedActorCreationTaskBuilder(owner_address, actor_id);

    // Invoke RequestWorkerLease to request a leased worker for the task in the
    // NodeManager.
    grpc::ClientContext context;
    rpc::RequestWorkerLeaseReply reply;
    rpc::RequestWorkerLeaseRequest request;
    request.mutable_resource_spec()->CopyFrom(task_spec_builder.GetMessage());
    auto channel =
        grpc::CreateChannel("localhost:" + std::to_string(node_manager_->GetServerPort()),
                            grpc::InsecureChannelCredentials());
    auto stub = rpc::NodeManagerService::NewStub(channel);
    auto status = stub->RequestWorkerLease(&context, request, &reply);
    EXPECT_TRUE(status.ok());

    // After RequestWorkerLease, a leased worker is ready in the NodeManager.
    // Then use publish_node_change_callback to say owner_node_id is dead.
    // The leased worker should not be killed by this because it is a detached actor.
    GcsNodeInfo node_info;
    node_info.set_state(GcsNodeInfo::DEAD);
    publish_node_change_callback(owner_node_id, std::move(node_info));
  }};

  // Prepare a mock worker with a real process so that we can check if the process is
  // alive later.
  const auto worker = std::make_shared<MockWorker>(WorkerID::FromRandom(), 10);
  auto [proc, spawn_error] =
      Process::Spawn(std::vector<std::string>{"sleep", "1000"}, true);
  EXPECT_FALSE(spawn_error);
  worker->SetProcess(proc);
  // Complete the RequestWorkerLease rpc with the mock worker.
  pop_worker_callback_promise.get_future().wait();
  io_service_.post([&] { pop_worker_callback(worker, PopWorkerStatus::OK, ""); },
                   "pop_worker_callback");

  // Wait for the client thead to complete. This waits for the RequestWorkerLease call
  // and publish_worker_failure_callback to finish.
  grpc_client_thread.join();
  // Wait for more than kill_worker_timeout_milliseconds.
  std::this_thread::sleep_for(std::chrono::seconds(1));
  // The process should still be alive because it should not be killed by
  // publish_worker_failure_callback.
  EXPECT_TRUE(proc.IsAlive());
  // clean up.
  proc.Kill();
  io_service_.stop();
  io_thread.join();
}

}  // namespace ray::raylet

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
