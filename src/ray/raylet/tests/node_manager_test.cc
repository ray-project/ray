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

#include <atomic>
#include <cstdint>
#include <memory>
#include <queue>
#include <string>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>

#include "gmock/gmock.h"
#include "mock/ray/core_worker/experimental_mutable_object_provider.h"
#include "mock/ray/gcs_client/gcs_client.h"
#include "mock/ray/object_manager/object_directory.h"
#include "mock/ray/object_manager/object_manager.h"
#include "mock/ray/raylet/local_lease_manager.h"
#include "mock/ray/raylet/worker_pool.h"
#include "mock/ray/rpc/worker/core_worker_client.h"
#include "ray/common/buffer.h"
#include "ray/common/bundle_spec.h"
#include "ray/common/flatbuf_utils.h"
#include "ray/common/scheduling/cluster_resource_data.h"
#include "ray/common/scheduling/resource_set.h"
#include "ray/core_worker_rpc_client/core_worker_client_pool.h"
#include "ray/core_worker_rpc_client/fake_core_worker_client.h"
#include "ray/object_manager/plasma/fake_plasma_client.h"
#include "ray/observability/fake_metric.h"
#include "ray/pubsub/fake_subscriber.h"
#include "ray/raylet/fake_worker.h"
#include "ray/raylet/local_object_manager_interface.h"
#include "ray/raylet/scheduling/cluster_lease_manager.h"
#include "ray/raylet/tests/util.h"
#include "ray/raylet_rpc_client/fake_raylet_client.h"
#include "ray/rpc/utils.h"

namespace ray::raylet {
using ::testing::_;
using ::testing::Return;

namespace {

constexpr double kTestTotalCpuResource = 10.0;

class FakeLocalObjectManager : public LocalObjectManagerInterface {
 public:
  FakeLocalObjectManager(
      std::shared_ptr<absl::flat_hash_set<ObjectID>> objects_pending_deletion)
      : objects_pending_deletion_(objects_pending_deletion) {}

  void PinObjectsAndWaitForFree(const std::vector<ObjectID> &object_ids,
                                std::vector<std::unique_ptr<RayObject>> &&objects,
                                const rpc::Address &owner_address,
                                const ObjectID &generator_id = ObjectID::Nil()) override {
  }

  // NOOP
  void SpillObjectUptoMaxThroughput() override {}

  void SpillObjects(const std::vector<ObjectID> &objects_ids,
                    std::function<void(const ray::Status &)> callback) override {}

  void AsyncRestoreSpilledObject(
      const ObjectID &object_id,
      int64_t object_size,
      const std::string &object_url,
      std::function<void(const ray::Status &)> callback) override {}

  void FlushFreeObjects() override{};

  bool ObjectPendingDeletion(const ObjectID &object_id) override {
    return objects_pending_deletion_->find(object_id) != objects_pending_deletion_->end();
  }

  void ProcessSpilledObjectsDeleteQueue(uint32_t max_batch_size) override {}

  bool IsSpillingInProgress() override { return false; }

  void FillObjectStoreStats(rpc::GetNodeStatsReply *reply) const override {}

  void RecordMetrics() const override {}

  std::string GetLocalSpilledObjectURL(const ObjectID &object_id) override { return ""; }

  int64_t GetPrimaryBytes() const override { return 0; }

  bool HasLocallySpilledObjects() const override { return false; }

  std::string DebugString() const override { return ""; }

 private:
  std::shared_ptr<absl::flat_hash_set<ObjectID>> objects_pending_deletion_;
};

LeaseSpecification BuildLeaseSpec(
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
  return LeaseSpecification(std::move(builder).ConsumeAndBuild().GetMessage());
}

LeaseSpecification DetachedActorCreationLeaseSpec(const rpc::Address &owner_address,
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
                                             /*allow_out_of_order_execution=*/false,
                                             /*root_detached_actor_id=*/actor_id);
  return LeaseSpecification(std::move(task_spec_builder).ConsumeAndBuild().GetMessage());
}

}  // namespace

TEST(NodeManagerStaticTest, TestHandleReportWorkerBacklog) {
  {
    // Worker backlog report from a disconnected worker should be ignored.
    MockWorkerPool worker_pool;
    MockLocalLeaseManager local_lease_manager;

    WorkerID worker_id = WorkerID::FromRandom();
    EXPECT_CALL(worker_pool, GetRegisteredWorker(worker_id))
        .Times(1)
        .WillOnce(Return(nullptr));
    EXPECT_CALL(worker_pool, GetRegisteredDriver(worker_id))
        .Times(1)
        .WillOnce(Return(nullptr));
    EXPECT_CALL(local_lease_manager, ClearWorkerBacklog(_)).Times(0);
    EXPECT_CALL(local_lease_manager, SetWorkerBacklog(_, _, _)).Times(0);

    rpc::ReportWorkerBacklogRequest request;
    request.set_worker_id(worker_id.Binary());
    rpc::ReportWorkerBacklogReply reply;
    NodeManager::HandleReportWorkerBacklog(
        request,
        &reply,
        [](Status status, std::function<void()> success, std::function<void()> failure) {
        },
        worker_pool,
        local_lease_manager);
  }

  {
    // Worker backlog report from a connected driver should be recorded.
    MockWorkerPool worker_pool;
    MockLocalLeaseManager local_lease_manager;

    WorkerID worker_id = WorkerID::FromRandom();
    std::shared_ptr<MockWorker> driver = std::make_shared<MockWorker>(worker_id, 10);

    rpc::ReportWorkerBacklogRequest request;
    request.set_worker_id(worker_id.Binary());
    auto backlog_report_1 = request.add_backlog_reports();
    auto lease_spec_1 = BuildLeaseSpec({{"CPU", 1}});
    backlog_report_1->mutable_lease_spec()->CopyFrom(lease_spec_1.GetMessage());
    backlog_report_1->set_backlog_size(1);

    auto backlog_report_2 = request.add_backlog_reports();
    auto lease_spec_2 = BuildLeaseSpec({{"GPU", 2}});
    backlog_report_2->mutable_lease_spec()->CopyFrom(lease_spec_2.GetMessage());
    backlog_report_2->set_backlog_size(3);
    rpc::ReportWorkerBacklogReply reply;

    EXPECT_CALL(worker_pool, GetRegisteredWorker(worker_id))
        .Times(1)
        .WillOnce(Return(nullptr));
    EXPECT_CALL(worker_pool, GetRegisteredDriver(worker_id))
        .Times(1)
        .WillOnce(Return(driver));
    EXPECT_CALL(local_lease_manager, ClearWorkerBacklog(worker_id)).Times(1);
    EXPECT_CALL(local_lease_manager,
                SetWorkerBacklog(lease_spec_1.GetSchedulingClass(), worker_id, 1))
        .Times(1);
    EXPECT_CALL(local_lease_manager,
                SetWorkerBacklog(lease_spec_2.GetSchedulingClass(), worker_id, 3))
        .Times(1);

    NodeManager::HandleReportWorkerBacklog(
        request,
        &reply,
        [](Status status, std::function<void()> success, std::function<void()> failure) {
        },
        worker_pool,
        local_lease_manager);
  }

  {
    // Worker backlog report from a connected worker should be recorded.
    MockWorkerPool worker_pool;
    MockLocalLeaseManager local_lease_manager;

    WorkerID worker_id = WorkerID::FromRandom();
    std::shared_ptr<MockWorker> worker = std::make_shared<MockWorker>(worker_id, 10);

    rpc::ReportWorkerBacklogRequest request;
    request.set_worker_id(worker_id.Binary());
    auto backlog_report_1 = request.add_backlog_reports();
    auto lease_spec_1 = BuildLeaseSpec({{"CPU", 1}});
    backlog_report_1->mutable_lease_spec()->CopyFrom(lease_spec_1.GetMessage());
    backlog_report_1->set_backlog_size(1);

    auto backlog_report_2 = request.add_backlog_reports();
    auto lease_spec_2 = BuildLeaseSpec({{"GPU", 2}});
    backlog_report_2->mutable_lease_spec()->CopyFrom(lease_spec_2.GetMessage());
    backlog_report_2->set_backlog_size(3);
    rpc::ReportWorkerBacklogReply reply;

    EXPECT_CALL(worker_pool, GetRegisteredWorker(worker_id))
        .Times(1)
        .WillOnce(Return(worker));
    EXPECT_CALL(worker_pool, GetRegisteredDriver(worker_id)).Times(0);

    EXPECT_CALL(local_lease_manager, ClearWorkerBacklog(worker_id)).Times(1);
    EXPECT_CALL(local_lease_manager,
                SetWorkerBacklog(lease_spec_1.GetSchedulingClass(), worker_id, 1))
        .Times(1);
    EXPECT_CALL(local_lease_manager,
                SetWorkerBacklog(lease_spec_2.GetSchedulingClass(), worker_id, 3))
        .Times(1);

    NodeManager::HandleReportWorkerBacklog(
        request,
        &reply,
        [](Status status, std::function<void()> success, std::function<void()> failure) {
        },
        worker_pool,
        local_lease_manager);
  }
}

class NodeManagerTest : public ::testing::Test {
 public:
  NodeManagerTest()
      : client_call_manager_(io_service_, /*record_stats=*/false, /*local_address=*/""),
        worker_rpc_pool_([](const auto &) {
          return std::make_shared<rpc::MockCoreWorkerClientInterface>();
        }),
        raylet_client_pool_(
            [](const auto &) { return std::make_shared<rpc::FakeRayletClient>(); }),
        fake_task_by_state_counter_() {
    RayConfig::instance().initialize(R"({
      "raylet_liveness_self_check_interval_ms": 100
    })");

    NodeManagerConfig node_manager_config{};
    node_manager_config.maximum_startup_concurrency = 1;
    node_manager_config.store_socket_name = "test_store_socket";
    node_manager_config.resource_config = ResourceSet(
        absl::flat_hash_map<std::string, double>{{"CPU", kTestTotalCpuResource}});

    core_worker_subscriber_ = std::make_unique<pubsub::FakeSubscriber>();
    mock_object_directory_ = std::make_unique<MockObjectDirectory>();
    mock_object_manager_ = std::make_unique<MockObjectManager>();

    EXPECT_CALL(*mock_object_manager_, GetMemoryCapacity()).WillRepeatedly(Return(0));

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

    raylet_node_id_ = NodeID::FromRandom();

    objects_pending_deletion_ = std::make_shared<absl::flat_hash_set<ObjectID>>();

    local_object_manager_ =
        std::make_unique<FakeLocalObjectManager>(objects_pending_deletion_);

    lease_dependency_manager_ = std::make_unique<LeaseDependencyManager>(
        *mock_object_manager_, fake_task_by_state_counter_);

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
      auto ptr = mock_gcs_client_->Nodes().GetNodeAddressAndLiveness(node_id);
      return ptr ? std::optional(*ptr) : std::nullopt;
    };

    auto max_task_args_memory = static_cast<int64_t>(
        static_cast<float>(mock_object_manager_->GetMemoryCapacity()) *
        RayConfig::instance().max_task_args_memory_fraction());

    local_lease_manager_ = std::make_unique<LocalLeaseManager>(
        raylet_node_id_,
        *cluster_resource_scheduler_,
        *lease_dependency_manager_,
        get_node_info_func,
        mock_worker_pool_,
        leased_workers_,
        [&](const std::vector<ObjectID> &object_ids,
            std::vector<std::unique_ptr<RayObject>> *results) {
          return node_manager_->GetObjectsFromPlasma(object_ids, results);
        },
        max_task_args_memory);

    cluster_lease_manager_ = std::make_unique<ClusterLeaseManager>(
        raylet_node_id_,
        *cluster_resource_scheduler_,
        get_node_info_func,
        [](const ray::RayLease &lease) {},
        *local_lease_manager_);

    placement_group_resource_manager_ =
        std::make_unique<NewPlacementGroupResourceManager>(*cluster_resource_scheduler_);

    node_manager_ = std::make_unique<NodeManager>(
        io_service_,
        raylet_node_id_,
        "test_node_name",
        node_manager_config,
        *mock_gcs_client_,
        client_call_manager_,
        worker_rpc_pool_,
        raylet_client_pool_,
        *core_worker_subscriber_,
        *cluster_resource_scheduler_,
        *local_lease_manager_,
        *cluster_lease_manager_,
        *mock_object_directory_,
        *mock_object_manager_,
        *local_object_manager_,
        *lease_dependency_manager_,
        mock_worker_pool_,
        leased_workers_,
        mock_store_client_,
        std::move(mutable_object_provider),
        /*shutdown_raylet_gracefully=*/
        [](const auto &) {},
        [](const std::string &) {},
        nullptr,
        shutting_down_,
        *placement_group_resource_manager_,
        boost::asio::basic_socket_acceptor<local_stream_protocol>(io_service_),
        boost::asio::basic_stream_socket<local_stream_protocol>(io_service_));
  }

  instrumented_io_context io_service_;
  rpc::ClientCallManager client_call_manager_;
  rpc::CoreWorkerClientPool worker_rpc_pool_;
  rpc::RayletClientPool raylet_client_pool_;

  NodeID raylet_node_id_;
  std::unique_ptr<pubsub::FakeSubscriber> core_worker_subscriber_;
  std::unique_ptr<ClusterResourceScheduler> cluster_resource_scheduler_;
  std::unique_ptr<LocalLeaseManager> local_lease_manager_;
  std::unique_ptr<ClusterLeaseManager> cluster_lease_manager_;
  std::unique_ptr<PlacementGroupResourceManager> placement_group_resource_manager_;
  std::shared_ptr<LocalObjectManagerInterface> local_object_manager_;
  std::unique_ptr<LeaseDependencyManager> lease_dependency_manager_;
  std::unique_ptr<gcs::MockGcsClient> mock_gcs_client_ =
      std::make_unique<gcs::MockGcsClient>();
  std::unique_ptr<MockObjectDirectory> mock_object_directory_;
  std::unique_ptr<MockObjectManager> mock_object_manager_;
  core::experimental::MockMutableObjectProvider *mock_mutable_object_provider_;
  std::shared_ptr<plasma::PlasmaClientInterface> mock_store_client_ =
      std::make_shared<plasma::FakePlasmaClient>();

  std::unique_ptr<NodeManager> node_manager_;
  MockWorkerPool mock_worker_pool_;
  absl::flat_hash_map<LeaseID, std::shared_ptr<WorkerInterface>> leased_workers_;
  std::shared_ptr<absl::flat_hash_set<ObjectID>> objects_pending_deletion_;
  ray::observability::FakeGauge fake_task_by_state_counter_;

  std::atomic_bool shutting_down_ = RayletShutdownState::ALIVE;
};

TEST_F(NodeManagerTest, TestRegisterGcsAndCheckSelfAlive) {
  EXPECT_CALL(*mock_gcs_client_->mock_node_accessor,
              AsyncSubscribeToNodeAddressAndLivenessChange(_, _))
      .Times(1);
  EXPECT_CALL(*mock_gcs_client_->mock_worker_accessor,
              AsyncSubscribeToWorkerFailures(_, _))
      .WillOnce(Return(Status::OK()));
  EXPECT_CALL(*mock_gcs_client_->mock_job_accessor, AsyncSubscribeAll(_, _))
      .WillOnce(Return(Status::OK()));
  EXPECT_CALL(mock_worker_pool_, GetAllRegisteredWorkers(_, _))
      .WillRepeatedly(Return(std::vector<std::shared_ptr<WorkerInterface>>{}));
  EXPECT_CALL(mock_worker_pool_, GetAllRegisteredDrivers(_, _))
      .WillRepeatedly(Return(std::vector<std::shared_ptr<WorkerInterface>>{}));
  EXPECT_CALL(mock_worker_pool_, IsWorkerAvailableForScheduling())
      .WillRepeatedly(Return(false));
  std::promise<void> promise;
  EXPECT_CALL(*mock_gcs_client_->mock_node_accessor, AsyncCheckAlive(_, _, _))
      .WillOnce(
          [&promise](const auto &, const auto &, const auto &) { promise.set_value(); });
  node_manager_->RegisterGcs();
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
  EXPECT_CALL(*mock_gcs_client_->mock_node_accessor,
              AsyncSubscribeToNodeAddressAndLivenessChange(_, _))
      .Times(1);
  EXPECT_CALL(*mock_gcs_client_->mock_job_accessor, AsyncSubscribeAll(_, _))
      .WillOnce(Return(Status::OK()));
  EXPECT_CALL(mock_worker_pool_, GetAllRegisteredWorkers(_, _))
      .WillRepeatedly(Return(std::vector<std::shared_ptr<WorkerInterface>>{}));
  EXPECT_CALL(mock_worker_pool_, GetAllRegisteredDrivers(_, _))
      .WillRepeatedly(Return(std::vector<std::shared_ptr<WorkerInterface>>{}));
  EXPECT_CALL(mock_worker_pool_, IsWorkerAvailableForScheduling())
      .WillRepeatedly(Return(false));
  EXPECT_CALL(mock_worker_pool_, PrestartWorkers(_, _)).Times(1);

  // Save the pop_worker_callback for providing a mock worker later.
  PopWorkerCallback pop_worker_callback;
  EXPECT_CALL(mock_worker_pool_, PopWorker(_, _))
      .WillOnce(
          [&](const LeaseSpecification &lease_spec, const PopWorkerCallback &callback) {
            pop_worker_callback = callback;
          });

  // Save the publish_worker_failure_callback for publishing a worker failure event later.
  gcs::ItemCallback<rpc::WorkerDeltaData> publish_worker_failure_callback;
  EXPECT_CALL(*mock_gcs_client_->mock_worker_accessor,
              AsyncSubscribeToWorkerFailures(_, _))
      .WillOnce([&](const gcs::ItemCallback<rpc::WorkerDeltaData> &subscribe,
                    const gcs::StatusCallback &done) {
        publish_worker_failure_callback = subscribe;
        return Status::OK();
      });

  // Invoke RegisterGcs and wait until publish_worker_failure_callback is set.
  node_manager_->RegisterGcs();
  while (!publish_worker_failure_callback) {
    io_service_.run_one();
  }

  // Preparing a detached actor creation task spec for the later RequestWorkerLease rpc.
  const auto owner_worker_id = WorkerID::FromRandom();
  rpc::Address owner_address;
  owner_address.set_worker_id(owner_worker_id.Binary());
  const auto actor_id =
      ActorID::Of(JobID::FromInt(1), TaskID::FromRandom(JobID::FromInt(1)), 0);
  const auto lease_spec = DetachedActorCreationLeaseSpec(owner_address, actor_id);

  // Invoke RequestWorkerLease to request a leased worker for the task in the
  // NodeManager.
  std::promise<Status> promise;
  rpc::RequestWorkerLeaseReply reply;
  rpc::RequestWorkerLeaseRequest request;
  request.mutable_lease_spec()->CopyFrom(lease_spec.GetMessage());
  node_manager_->HandleRequestWorkerLease(
      request,
      &reply,
      [&](Status status, std::function<void()> success, std::function<void()> failure) {
        promise.set_value(status);
      });

  // Prepare a mock worker and check if it is not killed later.
  const auto worker = std::make_shared<MockWorker>(WorkerID::FromRandom(), 10);
  // Complete the RequestWorkerLease rpc with the mock worker.
  pop_worker_callback(worker, PopWorkerStatus::OK, "");
  EXPECT_TRUE(promise.get_future().get().ok());

  // After RequestWorkerLease, a leased worker is ready in the NodeManager.
  // Then use publish_worker_failure_callback to say owner_worker_id is dead.
  // The leased worker should not be killed by this because it is a detached actor.
  rpc::WorkerDeltaData delta_data;
  delta_data.set_worker_id(owner_worker_id.Binary());
  publish_worker_failure_callback(std::move(delta_data));
  // The worker should still be alive because it should not be killed by
  // publish_worker_failure_callback.
  EXPECT_FALSE(worker->IsKilled());
}

TEST_F(NodeManagerTest, TestDetachedWorkerIsKilledByFailedNode) {
  EXPECT_CALL(*mock_object_directory_, HandleNodeRemoved(_)).Times(1);
  EXPECT_CALL(*mock_object_manager_, HandleNodeRemoved(_)).Times(1);
  EXPECT_CALL(*mock_gcs_client_->mock_worker_accessor,
              AsyncSubscribeToWorkerFailures(_, _))
      .WillOnce(Return(Status::OK()));
  EXPECT_CALL(*mock_gcs_client_->mock_job_accessor, AsyncSubscribeAll(_, _))
      .WillOnce(Return(Status::OK()));
  EXPECT_CALL(mock_worker_pool_, GetAllRegisteredWorkers(_, _))
      .WillRepeatedly(Return(std::vector<std::shared_ptr<WorkerInterface>>{}));
  EXPECT_CALL(mock_worker_pool_, GetAllRegisteredDrivers(_, _))
      .WillRepeatedly(Return(std::vector<std::shared_ptr<WorkerInterface>>{}));
  EXPECT_CALL(mock_worker_pool_, IsWorkerAvailableForScheduling())
      .WillRepeatedly(Return(false));
  EXPECT_CALL(mock_worker_pool_, PrestartWorkers(_, _)).Times(1);

  // Save the pop_worker_callback for providing a mock worker later.
  PopWorkerCallback pop_worker_callback;
  EXPECT_CALL(mock_worker_pool_, PopWorker(_, _))
      .WillOnce(
          [&](const LeaseSpecification &lease_spec, const PopWorkerCallback &callback) {
            pop_worker_callback = callback;
          });

  // Save the publish_node_change_callback for publishing a node failure event later.
  std::function<void(const NodeID &id, rpc::GcsNodeAddressAndLiveness &&node_info)>
      publish_node_change_callback;
  EXPECT_CALL(*mock_gcs_client_->mock_node_accessor,
              AsyncSubscribeToNodeAddressAndLivenessChange(_, _))
      .WillOnce([&](const gcs::SubscribeCallback<NodeID, rpc::GcsNodeAddressAndLiveness>
                        &subscribe,
                    const gcs::StatusCallback &done) {
        publish_node_change_callback = subscribe;
      });
  node_manager_->RegisterGcs();

  // Preparing a detached actor creation task spec for the later RequestWorkerLease rpc.
  const auto owner_node_id = NodeID::FromRandom();
  rpc::Address owner_address;
  owner_address.set_node_id(owner_node_id.Binary());
  const auto actor_id =
      ActorID::Of(JobID::FromInt(1), TaskID::FromRandom(JobID::FromInt(1)), 0);
  const auto lease_spec = DetachedActorCreationLeaseSpec(owner_address, actor_id);

  // Invoke RequestWorkerLease to request a leased worker for the task in the
  // NodeManager.
  std::promise<Status> promise;
  rpc::RequestWorkerLeaseReply reply;
  rpc::RequestWorkerLeaseRequest request;
  request.mutable_lease_spec()->CopyFrom(lease_spec.GetMessage());
  node_manager_->HandleRequestWorkerLease(
      request,
      &reply,
      [&](Status status, std::function<void()> success, std::function<void()> failure) {
        promise.set_value(status);
      });

  // Prepare a mock worker and check if it is not killed later.
  const auto worker = std::make_shared<MockWorker>(WorkerID::FromRandom(), 10);
  // Complete the RequestWorkerLease rpc with the mock worker.
  pop_worker_callback(worker, PopWorkerStatus::OK, "");
  EXPECT_TRUE(promise.get_future().get().ok());

  // After RequestWorkerLease, a leased worker is ready in the NodeManager.
  // Then use publish_node_change_callback to say owner_node_id is dead.
  // The leased worker should not be killed by this because it is a detached actor.
  rpc::GcsNodeAddressAndLiveness node_info;
  node_info.set_state(GcsNodeInfo::DEAD);
  publish_node_change_callback(owner_node_id, std::move(node_info));
  // The worker should still be alive because it should not be killed by
  // publish_node_change_callback.
  EXPECT_FALSE(worker->IsKilled());
}

TEST_F(NodeManagerTest, TestPinningAnObjectPendingDeletionFails) {
  // Object needs to be created in plasma before it can be pinned.
  rpc::Address owner_addr;
  plasma::flatbuf::ObjectSource source = plasma::flatbuf::ObjectSource::CreatedByWorker;
  ObjectID id = ObjectID::FromRandom();

  RAY_UNUSED(mock_store_client_->TryCreateImmediately(
      id, owner_addr, 1024, nullptr, 1024, nullptr, source, 0));

  rpc::PinObjectIDsRequest pin_request;
  pin_request.add_object_ids(id.Binary());
  rpc::PinObjectIDsReply successful_pin_reply;

  node_manager_->HandlePinObjectIDs(
      pin_request,
      &successful_pin_reply,
      [](Status s, std::function<void()> success, std::function<void()> failure) {});

  EXPECT_EQ(successful_pin_reply.successes_size(), 1);
  EXPECT_TRUE(successful_pin_reply.successes(0));

  // TODO(irabbani): This is a hack to mark object for pending deletion in the
  // FakeLocalObjectManager. Follow up in CORE-1677 to remove this and
  // integrate with a Fake SubscriberInterface.
  objects_pending_deletion_->emplace(id);

  rpc::PinObjectIDsReply failed_pin_reply;
  node_manager_->HandlePinObjectIDs(
      pin_request,
      &failed_pin_reply,
      [](Status s, std::function<void()> success, std::function<void()> failure) {});

  EXPECT_EQ(failed_pin_reply.successes_size(), 1);
  EXPECT_FALSE(failed_pin_reply.successes(0));
}

TEST_F(NodeManagerTest, TestConsumeSyncMessage) {
  // Create and wrap a mock resource view sync message.
  syncer::ResourceViewSyncMessage payload;
  payload.mutable_resources_total()->insert({"CPU", kTestTotalCpuResource});
  payload.mutable_resources_available()->insert({"CPU", kTestTotalCpuResource});
  payload.mutable_labels()->insert({"label1", "value1"});

  std::string serialized;
  ASSERT_TRUE(payload.SerializeToString(&serialized));

  auto node_id = NodeID::FromRandom();
  syncer::RaySyncMessage msg;
  msg.set_node_id(node_id.Binary());
  msg.set_message_type(syncer::MessageType::RESOURCE_VIEW);
  msg.set_sync_message(serialized);

  node_manager_->ConsumeSyncMessage(std::make_shared<syncer::RaySyncMessage>(msg));

  // Verify node resources and labels were updated.
  const auto &node_resources =
      cluster_resource_scheduler_->GetClusterResourceManager().GetNodeResources(
          scheduling::NodeID(node_id.Binary()));
  EXPECT_EQ(node_resources.labels.at("label1"), "value1");
  EXPECT_EQ(node_resources.total.Get(scheduling::ResourceID("CPU")).Double(),
            kTestTotalCpuResource);
  EXPECT_EQ(node_resources.available.Get(scheduling::ResourceID("CPU")).Double(),
            kTestTotalCpuResource);
}

TEST_F(NodeManagerTest, TestResizeLocalResourceInstancesSuccessful) {
  // Test 1: Up scaling (increasing resource capacity)
  rpc::ResizeLocalResourceInstancesRequest request;
  rpc::ResizeLocalResourceInstancesReply reply;

  (*request.mutable_resources())["CPU"] = 8.0;
  (*request.mutable_resources())["memory"] = 16000000.0;

  bool callback_called = false;

  node_manager_->HandleResizeLocalResourceInstances(
      request,
      &reply,
      [&callback_called](
          Status s, std::function<void()> success, std::function<void()> failure) {
        callback_called = true;
        EXPECT_TRUE(s.ok());
      });
  EXPECT_TRUE(callback_called);

  // Check that reply contains the updated resources
  EXPECT_EQ(reply.total_resources().at("CPU"), 8.0);
  EXPECT_EQ(reply.total_resources().at("memory"), 16000000.0);

  // Test 2: Down scaling (decreasing resources)
  (*request.mutable_resources())["CPU"] = 4.0;
  (*request.mutable_resources())["memory"] = 8000000.0;

  reply.Clear();
  callback_called = false;
  node_manager_->HandleResizeLocalResourceInstances(
      request,
      &reply,
      [&callback_called](
          Status s, std::function<void()> success, std::function<void()> failure) {
        callback_called = true;
        EXPECT_TRUE(s.ok());
      });
  EXPECT_TRUE(callback_called);

  // Check that reply contains the updated (reduced) resources
  EXPECT_EQ(reply.total_resources().at("CPU"), 4.0);
  EXPECT_EQ(reply.total_resources().at("memory"), 8000000.0);

  // Test 3: No changes (same values)
  reply.Clear();
  callback_called = false;
  node_manager_->HandleResizeLocalResourceInstances(
      request,
      &reply,
      [&callback_called](
          Status s, std::function<void()> success, std::function<void()> failure) {
        callback_called = true;
        EXPECT_TRUE(s.ok());
      });
  EXPECT_TRUE(callback_called);

  // Should still succeed and return current state
  EXPECT_EQ(reply.total_resources().at("CPU"), 4.0);
  EXPECT_EQ(reply.total_resources().at("memory"), 8000000.0);

  // Test 4: Now update only CPU, leaving memory unchanged
  request.mutable_resources()->clear();
  (*request.mutable_resources())["CPU"] = 8.0;  // Double the CPU

  reply.Clear();
  callback_called = false;
  node_manager_->HandleResizeLocalResourceInstances(
      request,
      &reply,
      [&callback_called](
          Status s, std::function<void()> success, std::function<void()> failure) {
        callback_called = true;
        EXPECT_TRUE(s.ok());
      });
  EXPECT_TRUE(callback_called);

  // Check that CPU was updated, and memory was unchanged
  EXPECT_EQ(reply.total_resources().at("CPU"), 8.0);
  EXPECT_EQ(reply.total_resources().at("memory"), 8000000.0);
}

TEST_F(NodeManagerTest, TestResizeLocalResourceInstancesInvalidArgument) {
  // Test trying to resize unit instance resources (GPU, etc.)
  rpc::ResizeLocalResourceInstancesRequest request;
  rpc::ResizeLocalResourceInstancesReply reply;

  (*request.mutable_resources())["GPU"] = 4.0;  // GPU is a unit instance resource

  bool callback_called = false;

  node_manager_->HandleResizeLocalResourceInstances(
      request,
      &reply,
      [&callback_called](
          Status s, std::function<void()> success, std::function<void()> failure) {
        callback_called = true;
        EXPECT_FALSE(s.ok());
        EXPECT_TRUE(s.IsInvalidArgument());
        // Check the error message contains expected details
        std::string error_msg = s.message();
        EXPECT_TRUE(error_msg.find("Cannot resize unit instance resource 'GPU'") !=
                    std::string::npos);
        EXPECT_TRUE(error_msg.find("Unit instance resources") != std::string::npos);
        EXPECT_TRUE(error_msg.find("cannot be resized dynamically") != std::string::npos);
      });

  // The callback should have been called with an InvalidArgument status
  EXPECT_TRUE(callback_called);
}

TEST_F(NodeManagerTest, TestResizeLocalResourceInstancesClamps) {
  // Test 1: Best effort downsizing
  rpc::ResizeLocalResourceInstancesRequest request;
  rpc::ResizeLocalResourceInstancesReply reply;

  // Initialize resources to a known state
  (*request.mutable_resources())["CPU"] = 8.0;
  (*request.mutable_resources())["memory"] = 16000000.0;

  bool callback_called = false;
  node_manager_->HandleResizeLocalResourceInstances(
      request,
      &reply,
      [&callback_called](
          Status s, std::function<void()> success, std::function<void()> failure) {
        callback_called = true;
        EXPECT_TRUE(s.ok());
      });
  EXPECT_TRUE(callback_called);

  // Simulate resource usage by allocating task resources through the local resource
  // manager: Use 6 out of 8 CPUs and 2 are free.
  const absl::flat_hash_map<std::string, double> task_resources = {{"CPU", 6.0}};
  std::shared_ptr<TaskResourceInstances> task_allocation =
      std::make_shared<TaskResourceInstances>();
  bool allocation_success =
      cluster_resource_scheduler_->GetLocalResourceManager().AllocateLocalTaskResources(
          task_resources, task_allocation);
  EXPECT_TRUE(allocation_success);

  // Now request to downsize CPU to 4. Should clamp to 6.
  callback_called = false;
  (*request.mutable_resources())["CPU"] = 4.0;
  reply.Clear();
  node_manager_->HandleResizeLocalResourceInstances(
      request,
      &reply,
      [&callback_called](
          Status s, std::function<void()> success, std::function<void()> failure) {
        callback_called = true;
        EXPECT_TRUE(s.ok());
      });
  EXPECT_TRUE(callback_called);
  // Total CPU should be clamped to 6 because there are only 2 CPUs available.
  // It should resize from 8 to 6 instead of resizing to 4.
  EXPECT_EQ(reply.total_resources().at("CPU"), 6.0);

  // Test 2: Extreme request (e.g., 0). Should clamp to current usage.
  callback_called = false;
  (*request.mutable_resources())["CPU"] = 0.0;
  reply.Clear();
  node_manager_->HandleResizeLocalResourceInstances(
      request,
      &reply,
      [&callback_called](
          Status s, std::function<void()> success, std::function<void()> failure) {
        callback_called = true;
        EXPECT_TRUE(s.ok());
      });
  EXPECT_TRUE(callback_called);
  // With 6 used, total should remain 6
  EXPECT_EQ(reply.total_resources().at("CPU"), 6.0);
}

class NodeManagerReturnWorkerLeaseIdempotentTest
    : public NodeManagerTest,
      public testing::WithParamInterface<std::tuple<bool, bool>> {};

TEST_P(NodeManagerReturnWorkerLeaseIdempotentTest, TestDifferentRequestArgs) {
  const auto &params = GetParam();
  bool disconnect_worker = std::get<0>(params);
  bool worker_exiting = std::get<1>(params);

  LeaseID lease_id = LeaseID::FromRandom();
  leased_workers_[lease_id] = std::make_shared<MockWorker>(WorkerID::FromRandom(), 10);
  rpc::ReturnWorkerLeaseRequest request;
  rpc::ReturnWorkerLeaseReply reply1;
  rpc::ReturnWorkerLeaseReply reply2;
  request.set_lease_id(lease_id.Binary());
  request.set_disconnect_worker(disconnect_worker);
  request.set_disconnect_worker_error_detail("test");
  request.set_worker_exiting(worker_exiting);

  if (disconnect_worker) {
    EXPECT_CALL(
        mock_worker_pool_,
        GetRegisteredWorker(testing::A<const std::shared_ptr<ClientConnection> &>()))
        .Times(1)
        .WillOnce(Return(nullptr));
    EXPECT_CALL(
        mock_worker_pool_,
        GetRegisteredDriver(testing::A<const std::shared_ptr<ClientConnection> &>()))
        .Times(1)
        .WillOnce(Return(nullptr));
  }
  node_manager_->HandleReturnWorkerLease(
      request,
      &reply1,
      [](Status s, std::function<void()> success, std::function<void()> failure) {
        ASSERT_TRUE(s.ok());
      });
  ASSERT_EQ(leased_workers_.size(), 0);
  node_manager_->HandleReturnWorkerLease(
      request,
      &reply2,
      [](Status s, std::function<void()> success, std::function<void()> failure) {
        ASSERT_TRUE(s.ok());
      });
  ASSERT_EQ(leased_workers_.size(), 0);
}

INSTANTIATE_TEST_SUITE_P(NodeManagerReturnWorkerLeaseIdempotentVariations,
                         NodeManagerReturnWorkerLeaseIdempotentTest,
                         testing::Combine(testing::Bool(), testing::Bool()));

TEST_F(NodeManagerTest, TestHandleRequestWorkerLeaseGrantedLeaseIdempotent) {
  auto lease_spec = BuildLeaseSpec({});
  rpc::RequestWorkerLeaseRequest request;
  rpc::RequestWorkerLeaseReply reply1;
  rpc::RequestWorkerLeaseReply reply2;
  LeaseID lease_id = LeaseID::FromRandom();
  lease_spec.GetMutableMessage().set_lease_id(lease_id.Binary());
  request.mutable_lease_spec()->CopyFrom(lease_spec.GetMessage());
  request.set_backlog_size(1);
  request.set_grant_or_reject(true);
  request.set_is_selected_based_on_locality(true);
  auto worker = std::make_shared<MockWorker>(WorkerID::FromRandom(), 10);
  PopWorkerCallback pop_worker_callback;
  EXPECT_CALL(mock_worker_pool_, PopWorker(_, _))
      .Times(1)
      .WillOnce([&](const LeaseSpecification &ls, const PopWorkerCallback &callback) {
        pop_worker_callback = callback;
      });
  node_manager_->HandleRequestWorkerLease(
      request,
      &reply1,
      [](Status s, std::function<void()> success, std::function<void()> failure) {
        ASSERT_TRUE(s.ok());
      });
  pop_worker_callback(worker, PopWorkerStatus::OK, "");
  ASSERT_EQ(leased_workers_.size(), 1);
  ASSERT_EQ(leased_workers_[lease_id]->GetGrantedLeaseId(), lease_id);
  request.mutable_lease_spec()->CopyFrom(lease_spec.GetMessage());
  node_manager_->HandleRequestWorkerLease(
      request,
      &reply2,
      [](Status s, std::function<void()> success, std::function<void()> failure) {
        ASSERT_TRUE(s.ok());
      });
  ASSERT_EQ(leased_workers_.size(), 1);
  ASSERT_EQ(leased_workers_[lease_id]->GetGrantedLeaseId(), lease_id);
  ASSERT_EQ(leased_workers_[lease_id]->WorkerId(),
            WorkerID::FromBinary(reply1.worker_address().worker_id()));
  ASSERT_EQ(reply1.worker_address(), reply2.worker_address());
}

TEST_F(NodeManagerTest, TestHandleRequestWorkerLeaseScheduledLeaseIdempotent) {
  auto lease_spec = BuildLeaseSpec({});

  // Create a task dependency to test that lease dependencies are requested/pulled only
  // once for a lease even if HandleRequestWorkerLease is called multiple times.
  ObjectID object_dep = ObjectID::FromRandom();
  auto *object_ref_dep = lease_spec.GetMutableMessage().add_dependencies();
  object_ref_dep->set_object_id(object_dep.Binary());

  rpc::Address owner_addr;
  plasma::flatbuf::ObjectSource source = plasma::flatbuf::ObjectSource::CreatedByWorker;
  RAY_UNUSED(mock_store_client_->TryCreateImmediately(
      object_dep, owner_addr, 1024, nullptr, 1024, nullptr, source, 0));

  rpc::RequestWorkerLeaseRequest request;
  rpc::RequestWorkerLeaseReply reply1;
  rpc::RequestWorkerLeaseReply reply2;
  LeaseID lease_id = LeaseID::FromRandom();
  lease_spec.GetMutableMessage().set_lease_id(lease_id.Binary());
  request.mutable_lease_spec()->CopyFrom(lease_spec.GetMessage());
  request.set_backlog_size(1);
  request.set_grant_or_reject(true);
  request.set_is_selected_based_on_locality(true);

  EXPECT_CALL(*mock_object_manager_, Pull(_, _, _)).Times(1).WillOnce(Return(1));

  auto worker = std::make_shared<MockWorker>(WorkerID::FromRandom(), 10);
  PopWorkerCallback pop_worker_callback;
  EXPECT_CALL(mock_worker_pool_, PopWorker(_, _))
      .Times(1)
      .WillOnce([&](const LeaseSpecification &ls, const PopWorkerCallback &callback) {
        pop_worker_callback = callback;
      });
  uint32_t callback_count = 0;
  node_manager_->HandleRequestWorkerLease(
      request,
      &reply1,
      [&callback_count](
          Status s, std::function<void()> success, std::function<void()> failure) {
        callback_count++;
        ASSERT_TRUE(s.ok());
      });
  ASSERT_EQ(leased_workers_.size(), 0);
  auto scheduling_class = lease_spec.GetSchedulingClass();
  ASSERT_TRUE(local_lease_manager_->IsLeaseQueued(scheduling_class, lease_id));

  // Test HandleRequestWorkerLease idempotency for leases that aren't yet granted
  node_manager_->HandleRequestWorkerLease(
      request,
      &reply2,
      [&callback_count](
          Status s, std::function<void()> success, std::function<void()> failure) {
        callback_count++;
        ASSERT_TRUE(s.ok());
      });
  ASSERT_EQ(leased_workers_.size(), 0);
  ASSERT_TRUE(local_lease_manager_->IsLeaseQueued(scheduling_class, lease_id));

  // Make the dependency available and notify the local lease manager that leases are
  // unblocked so the lease can be granted
  auto ready_lease_ids = lease_dependency_manager_->HandleObjectLocal(object_dep);
  ASSERT_EQ(ready_lease_ids.size(), 1);
  ASSERT_EQ(ready_lease_ids[0], lease_id);
  local_lease_manager_->LeasesUnblocked(ready_lease_ids);

  // Grant the lease, both callbacks should be triggered
  ASSERT_TRUE(pop_worker_callback);
  pop_worker_callback(worker, PopWorkerStatus::OK, "");
  ASSERT_EQ(leased_workers_.size(), 1);
  ASSERT_EQ(leased_workers_[lease_id]->GetGrantedLeaseId(), lease_id);
  ASSERT_EQ(leased_workers_[lease_id]->WorkerId(),
            WorkerID::FromBinary(reply1.worker_address().worker_id()));
  ASSERT_EQ(reply1.worker_address(), reply2.worker_address());
  ASSERT_EQ(callback_count, 2);
}

TEST_F(NodeManagerTest, TestHandleRequestWorkerLeaseInfeasibleIdempotent) {
  auto lease_spec = BuildLeaseSpec({{"CPU", kTestTotalCpuResource + 1}});
  lease_spec.GetMutableMessage()
      .mutable_scheduling_strategy()
      ->mutable_node_affinity_scheduling_strategy()
      ->set_soft(false);  // Hard constraint

  rpc::RequestWorkerLeaseRequest request;
  rpc::RequestWorkerLeaseReply reply1;
  rpc::RequestWorkerLeaseReply reply2;
  LeaseID lease_id = LeaseID::FromRandom();
  lease_spec.GetMutableMessage().set_lease_id(lease_id.Binary());
  request.mutable_lease_spec()->CopyFrom(lease_spec.GetMessage());
  request.set_backlog_size(1);
  request.set_grant_or_reject(true);
  request.set_is_selected_based_on_locality(true);
  node_manager_->HandleRequestWorkerLease(
      request,
      &reply1,
      [](Status s, std::function<void()> success, std::function<void()> failure) {
        ASSERT_TRUE(s.ok());
      });
  ASSERT_EQ(leased_workers_.size(), 0);
  ASSERT_EQ(reply1.canceled(), true);
  ASSERT_EQ(reply1.failure_type(),
            rpc::RequestWorkerLeaseReply::SCHEDULING_CANCELLED_UNSCHEDULABLE);
  request.mutable_lease_spec()->CopyFrom(lease_spec.GetMessage());
  node_manager_->HandleRequestWorkerLease(
      request,
      &reply2,
      [](Status s, std::function<void()> success, std::function<void()> failure) {
        ASSERT_TRUE(s.ok());
      });
  ASSERT_EQ(leased_workers_.size(), 0);
  ASSERT_EQ(reply1.canceled(), reply2.canceled());
  ASSERT_EQ(reply1.failure_type(), reply2.failure_type());
  ASSERT_EQ(reply1.scheduling_failure_message(), reply2.scheduling_failure_message());
}

size_t GetPendingLeaseWorkerCount(const LocalLeaseManager &local_lease_manager) {
  return local_lease_manager.waiting_lease_queue_.size() +
         local_lease_manager.leases_to_grant_.size();
}

TEST_F(NodeManagerTest, TestReschedulingLeasesDuringHandleDrainRaylet) {
  // Test that when the node is being drained, leases inside local lease manager
  // will be cancelled and re-added to the cluster lease manager for rescheduling.
  auto lease_spec = BuildLeaseSpec({});
  rpc::RequestWorkerLeaseRequest request_worker_lease_request;
  rpc::RequestWorkerLeaseReply request_worker_lease_reply;
  LeaseID lease_id = LeaseID::FromRandom();
  lease_spec.GetMutableMessage().set_lease_id(lease_id.Binary());
  request_worker_lease_request.mutable_lease_spec()->CopyFrom(lease_spec.GetMessage());
  request_worker_lease_request.set_backlog_size(1);
  request_worker_lease_request.set_grant_or_reject(true);
  request_worker_lease_request.set_is_selected_based_on_locality(true);
  node_manager_->HandleRequestWorkerLease(
      request_worker_lease_request,
      &request_worker_lease_reply,
      [](Status s, std::function<void()> success, std::function<void()> failure) {
        ASSERT_FALSE(true) << "This callback should not be called.";
      });
  ASSERT_EQ(GetPendingLeaseWorkerCount(*local_lease_manager_), 1);
  rpc::DrainRayletRequest drain_raylet_request;
  rpc::DrainRayletReply drain_raylet_reply;
  drain_raylet_request.set_reason(
      rpc::autoscaler::DrainNodeReason::DRAIN_NODE_REASON_PREEMPTION);
  node_manager_->HandleDrainRaylet(
      drain_raylet_request,
      &drain_raylet_reply,
      [](Status s, std::function<void()> success, std::function<void()> failure) {
        ASSERT_TRUE(s.ok());
      });
  ASSERT_EQ(GetPendingLeaseWorkerCount(*local_lease_manager_), 0);
  // The lease is infeasible now since the local node is draining.
  ASSERT_EQ(cluster_lease_manager_->GetInfeasibleQueueSize(), 1);
}

TEST_F(NodeManagerTest, RetryHandleCancelWorkerLeaseWhenHasLeaseRequest) {
  auto lease_spec = BuildLeaseSpec({});
  rpc::RequestWorkerLeaseRequest request_worker_lease_request;
  rpc::RequestWorkerLeaseReply request_worker_lease_reply;
  LeaseID lease_id = LeaseID::FromRandom();
  lease_spec.GetMutableMessage().set_lease_id(lease_id.Binary());
  request_worker_lease_request.mutable_lease_spec()->CopyFrom(lease_spec.GetMessage());
  request_worker_lease_request.set_backlog_size(1);
  request_worker_lease_request.set_grant_or_reject(true);
  request_worker_lease_request.set_is_selected_based_on_locality(true);
  node_manager_->HandleRequestWorkerLease(
      request_worker_lease_request,
      &request_worker_lease_reply,
      [](Status s, std::function<void()> success, std::function<void()> failure) {
        ASSERT_TRUE(s.ok());
      });
  ASSERT_EQ(GetPendingLeaseWorkerCount(*local_lease_manager_), 1);
  rpc::CancelWorkerLeaseRequest cancel_worker_lease_request;
  cancel_worker_lease_request.set_lease_id(lease_id.Binary());
  rpc::CancelWorkerLeaseReply cancel_worker_lease_reply1;
  rpc::CancelWorkerLeaseReply cancel_worker_lease_reply2;
  node_manager_->HandleCancelWorkerLease(
      cancel_worker_lease_request,
      &cancel_worker_lease_reply1,
      [](Status s, std::function<void()> success, std::function<void()> failure) {
        ASSERT_TRUE(s.ok());
      });
  ASSERT_EQ(GetPendingLeaseWorkerCount(*local_lease_manager_), 0);
  node_manager_->HandleCancelWorkerLease(
      cancel_worker_lease_request,
      &cancel_worker_lease_reply2,
      [](Status s, std::function<void()> success, std::function<void()> failure) {
        ASSERT_TRUE(s.ok());
      });
  ASSERT_EQ(GetPendingLeaseWorkerCount(*local_lease_manager_), 0);
  ASSERT_EQ(cancel_worker_lease_reply1.success(), true);
  // Due to the message reordering case where the cancel worker lease request
  // arrives at the raylet before the worker lease request has been received, we
  // cannot return true on the retry since from the raylet perspective both situations are
  // equivalent. Even if this returns false, the first request to HandleCancelWorkerLease
  // will trigger the callback for HandleRequestWorkerLease and remove the pending lease
  // request which prevents the CancelWorkerLease loop.
  ASSERT_EQ(cancel_worker_lease_reply2.success(), false);
}

TEST_F(NodeManagerTest, TestHandleCancelWorkerLeaseNoLeaseIdempotent) {
  LeaseID lease_id = LeaseID::FromRandom();
  rpc::CancelWorkerLeaseRequest request;
  request.set_lease_id(lease_id.Binary());
  rpc::CancelWorkerLeaseReply reply1;
  rpc::CancelWorkerLeaseReply reply2;
  node_manager_->HandleCancelWorkerLease(
      request,
      &reply1,
      [](Status s, std::function<void()> success, std::function<void()> failure) {
        ASSERT_TRUE(s.ok());
      });
  ASSERT_EQ(GetPendingLeaseWorkerCount(*local_lease_manager_), 0);
  node_manager_->HandleCancelWorkerLease(
      request,
      &reply2,
      [](Status s, std::function<void()> success, std::function<void()> failure) {
        ASSERT_TRUE(s.ok());
      });
  ASSERT_EQ(GetPendingLeaseWorkerCount(*local_lease_manager_), 0);
  ASSERT_EQ(reply1.success(), false);
  ASSERT_EQ(reply2.success(), false);
}

class PinObjectIDsIdempotencyTest : public NodeManagerTest,
                                    public ::testing::WithParamInterface<bool> {};

TEST_P(PinObjectIDsIdempotencyTest, TestHandlePinObjectIDsIdempotency) {
  // object_exists: determines whether we add an object to the plasma store which is used
  // for pinning.
  // object_exists == true: an object is added to the plasma store and PinObjectIDs is
  // expected to succeed. A true boolean value is inserted at the index of the object
  // in reply.successes.
  // object_exists == false: an object is not added to the plasma store. PinObjectIDs will
  // still succeed and not return an error when trying to pin a non-existent object, but
  // will instead at the index of the object in reply.successes insert a false
  // boolean value.
  const bool object_exists = GetParam();
  ObjectID id = ObjectID::FromRandom();

  if (object_exists) {
    rpc::Address owner_addr;
    plasma::flatbuf::ObjectSource source = plasma::flatbuf::ObjectSource::CreatedByWorker;
    RAY_UNUSED(mock_store_client_->TryCreateImmediately(
        id, owner_addr, 1024, nullptr, 1024, nullptr, source, 0));
  }

  rpc::PinObjectIDsRequest pin_request;
  pin_request.add_object_ids(id.Binary());

  rpc::PinObjectIDsReply reply1;
  node_manager_->HandlePinObjectIDs(
      pin_request,
      &reply1,
      [](Status s, std::function<void()> success, std::function<void()> failure) {});

  int64_t primary_bytes = local_object_manager_->GetPrimaryBytes();
  rpc::PinObjectIDsReply reply2;
  node_manager_->HandlePinObjectIDs(
      pin_request,
      &reply2,
      [](Status s, std::function<void()> success, std::function<void()> failure) {});

  // For each invocation of HandlePinObjectIDs, we expect the size of reply.successes and
  // the boolean values it contains to not change.
  EXPECT_EQ(reply1.successes_size(), 1);
  EXPECT_EQ(reply1.successes(0), object_exists);
  EXPECT_EQ(reply2.successes_size(), 1);
  EXPECT_EQ(reply2.successes(0), object_exists);
  EXPECT_EQ(local_object_manager_->GetPrimaryBytes(), primary_bytes);
}

INSTANTIATE_TEST_SUITE_P(PinObjectIDsIdempotencyVariations,
                         PinObjectIDsIdempotencyTest,
                         testing::Bool());

class NodeManagerKillActorTest : public NodeManagerTest,
                                 public ::testing::WithParamInterface<bool> {};

TEST_P(NodeManagerKillActorTest, TestHandleKillLocalActorIdempotency) {
  // worker_is_alive: determines whether the worker is alive and whether KillActor RPC
  // should be sent. worker_is_alive == true: Worker is alive and KillActor RPC should be
  // sent twice. worker_is_alive == false: Worker is dead and KillActor RPC should not be
  // sent.

  bool worker_is_alive = GetParam();

  WorkerID worker_id = WorkerID::FromRandom();
  JobID job_id = JobID::FromInt(1);
  ActorID actor_id = ActorID::Of(job_id, TaskID::ForDriverTask(job_id), 1);

  auto fake_rpc_client = std::make_shared<rpc::FakeCoreWorkerClient>();
  std::shared_ptr<raylet::MockWorker> worker;

  if (worker_is_alive) {
    worker = std::make_shared<raylet::MockWorker>(worker_id, 10);
    worker->Connect(fake_rpc_client);
    EXPECT_CALL(mock_worker_pool_, GetRegisteredWorker(worker_id))
        .Times(2)
        .WillRepeatedly(Return(worker));
  } else {
    EXPECT_CALL(mock_worker_pool_, GetRegisteredWorker(worker_id))
        .Times(2)
        .WillRepeatedly(Return(nullptr));
  }

  rpc::KillLocalActorRequest request;
  request.set_worker_id(worker_id.Binary());
  request.set_intended_actor_id(actor_id.Binary());
  request.set_force_kill(false);
  auto actor_died_ctx = request.mutable_death_cause()->mutable_actor_died_error_context();
  actor_died_ctx->set_reason(rpc::ActorDiedErrorContext::RAY_KILL);
  actor_died_ctx->set_error_message("Test kill");

  rpc::KillLocalActorReply reply1;
  node_manager_->HandleKillLocalActor(
      request,
      &reply1,
      [](Status s, std::function<void()> success, std::function<void()> failure) {
        ASSERT_TRUE(s.ok());
      });

  rpc::KillLocalActorReply reply2;
  node_manager_->HandleKillLocalActor(
      request,
      &reply2,
      [](Status s, std::function<void()> success, std::function<void()> failure) {
        ASSERT_TRUE(s.ok());
      });

  size_t expected_rpc_calls = worker_is_alive ? 2 : 0;
  ASSERT_EQ(fake_rpc_client->num_kill_actor_requests, expected_rpc_calls)
      << "Expected " << expected_rpc_calls
      << " KillActor RPC calls for worker_is_alive==" << worker_is_alive;
}

INSTANTIATE_TEST_SUITE_P(WorkerState, NodeManagerKillActorTest, ::testing::Bool());

class NodeManagerDeathTest : public NodeManagerTest,
                             public ::testing::WithParamInterface<bool> {};

TEST_P(NodeManagerDeathTest, TestGcsPublishesSelfDead) {
  // When the GCS publishes the node's death,
  // 1. The raylet should kill itself immediately if it's not shutting down.
  // 2. The raylet should ignore the death publish if the shutdown process has already
  //    started
  const bool shutting_down_during_death_publish = GetParam();

  gcs::SubscribeCallback<NodeID, rpc::GcsNodeAddressAndLiveness>
      publish_node_change_callback;
  EXPECT_CALL(*mock_gcs_client_->mock_node_accessor,
              AsyncSubscribeToNodeAddressAndLivenessChange(_, _))
      .WillOnce([&](const gcs::SubscribeCallback<NodeID, rpc::GcsNodeAddressAndLiveness>
                        &subscribe,
                    const gcs::StatusCallback &done) {
        publish_node_change_callback = subscribe;
      });
  node_manager_->RegisterGcs();

  shutting_down_ = shutting_down_during_death_publish;

  rpc::GcsNodeAddressAndLiveness dead_node_info;
  dead_node_info.set_node_id(raylet_node_id_.Binary());
  dead_node_info.set_state(rpc::GcsNodeInfo::DEAD);

  if (shutting_down_during_death_publish) {
    publish_node_change_callback(raylet_node_id_, std::move(dead_node_info));
  } else {
    ASSERT_DEATH(publish_node_change_callback(raylet_node_id_, std::move(dead_node_info)),
                 ".*Exiting because this node manager has.*");
  }
}

INSTANTIATE_TEST_SUITE_P(NodeManagerDeathVariations,
                         NodeManagerDeathTest,
                         testing::Bool());

class DrainRayletIdempotencyTest
    : public NodeManagerTest,
      public ::testing::WithParamInterface<
          std::tuple<rpc::autoscaler::DrainNodeReason, bool>> {};

TEST_P(DrainRayletIdempotencyTest, TestHandleDrainRayletIdempotency) {
  // drain_reason: the reason for the drain request (PREEMPTION or IDLE_TERMINATION).
  // is_node_idle: determines whether the node is idle.
  // is_node_idle == true: the node is idle.
  //    - drain_reason == PREEMPTION: DrainRaylet is expected to accept the request.
  //    - drain_reason == IDLE_TERMINATION: DrainRaylet is expected to accept the request.
  // is_node_idle == false: the node is not idle.
  //    - drain_reason == PREEMPTION: DrainRaylet is expected to accept the request.
  //    - drain_reason == IDLE_TERMINATION: DrainRaylet is expected to reject the request.

  auto [drain_reason, is_node_idle] = GetParam();
  if (!is_node_idle) {
    cluster_resource_scheduler_->GetLocalResourceManager().SetBusyFootprint(
        WorkFootprint::NODE_WORKERS);
  }

  // Whether the drain request is expected to be accepted. Note that for preemption we
  // must always accept the request regardless of the node's idle state.
  bool drain_request_accepted = false;
  if (drain_reason == rpc::autoscaler::DrainNodeReason::DRAIN_NODE_REASON_PREEMPTION) {
    drain_request_accepted = true;
  } else {
    drain_request_accepted = is_node_idle;
  }

  rpc::DrainRayletRequest request;
  request.set_reason(drain_reason);
  request.set_reason_message("Test drain");
  request.set_deadline_timestamp_ms(std::numeric_limits<int64_t>::max());

  rpc::DrainRayletReply reply1;
  node_manager_->HandleDrainRaylet(
      request, &reply1, [](Status s, std::function<void()>, std::function<void()>) {
        ASSERT_TRUE(s.ok());
      });

  ASSERT_EQ(reply1.is_accepted(), drain_request_accepted);
  ASSERT_EQ(cluster_resource_scheduler_->GetLocalResourceManager().IsLocalNodeDraining(),
            drain_request_accepted);

  rpc::DrainRayletReply reply2;
  node_manager_->HandleDrainRaylet(
      request, &reply2, [&](Status s, std::function<void()>, std::function<void()>) {
        ASSERT_TRUE(s.ok());
      });

  ASSERT_EQ(reply2.is_accepted(), drain_request_accepted);
  ASSERT_EQ(cluster_resource_scheduler_->GetLocalResourceManager().IsLocalNodeDraining(),
            drain_request_accepted);
}

INSTANTIATE_TEST_SUITE_P(
    DrainRayletIdempotencyVariations,
    DrainRayletIdempotencyTest,
    ::testing::Combine(
        ::testing::Values(
            rpc::autoscaler::DrainNodeReason::DRAIN_NODE_REASON_IDLE_TERMINATION,
            rpc::autoscaler::DrainNodeReason::DRAIN_NODE_REASON_PREEMPTION),
        ::testing::Bool()));

bool IsBundleRegistered(const PlacementGroupResourceManager &manager,
                        const BundleID &bundle_id) {
  return manager.bundle_spec_map_.contains(bundle_id);
}

class ReleaseUnusedBundlesRetriesTest : public NodeManagerTest,
                                        public ::testing::WithParamInterface<bool> {};

TEST_P(ReleaseUnusedBundlesRetriesTest, TestHandleReleaseUnusedBundlesRetries) {
  // bundle_in_use: determines whether we mark a bundle as in use and it is released by
  // the placement group resource manager.
  // bundle_in_use == true: a bundle is marked as in use in the placement group resource
  // manager. ReleaseUnusedBundles is expected to not release the bundle.
  // bundle_in_use == false: a bundle is not marked as in use in the placement group
  // resource manager. ReleaseUnusedBundles is expected to release the bundle.
  bool bundle_in_use = GetParam();

  auto group_id = PlacementGroupID::Of(JobID::FromInt(1));
  absl::flat_hash_map<std::string, double> unit_resource = {{"CPU", 1.0}};

  auto bundle_id = BundleID(group_id, 1);
  rpc::Bundle bundle;
  auto *bundle_id_msg = bundle.mutable_bundle_id();
  bundle_id_msg->set_placement_group_id(group_id.Binary());
  bundle_id_msg->set_bundle_index(1);
  auto unit_resources = bundle.mutable_unit_resources();
  for (const auto &[key, value] : unit_resource) {
    unit_resources->insert({key, value});
  }
  auto bundle_spec = std::make_shared<BundleSpecification>(bundle);
  ASSERT_TRUE(placement_group_resource_manager_->PrepareBundles({bundle_spec}));
  placement_group_resource_manager_->CommitBundles({bundle_spec});

  EXPECT_TRUE(IsBundleRegistered(*placement_group_resource_manager_, bundle_id));

  WorkerID worker_id = WorkerID::FromRandom();
  LeaseID lease_id = LeaseID::FromRandom();
  auto worker = std::make_shared<raylet::FakeWorker>(worker_id, 0, io_service_);
  worker->SetBundleId(bundle_id);
  worker->GrantLeaseId(lease_id);
  leased_workers_.emplace(lease_id, worker);

  rpc::ReleaseUnusedBundlesRequest request;
  if (bundle_in_use) {
    auto *bundle_entry = request.add_bundles_in_use();
    bundle_entry->mutable_bundle_id()->set_placement_group_id(group_id.Binary());
    bundle_entry->mutable_bundle_id()->set_bundle_index(1);
  } else {
    // When the bundle is not in use, the worker associated with that bundle is destroyed
    // hence need to mock the GetRegisteredWorker call to return the worker.
    EXPECT_CALL(
        mock_worker_pool_,
        GetRegisteredWorker(testing::An<const std::shared_ptr<ClientConnection> &>()))
        .WillOnce(Return(worker));
  }

  rpc::ReleaseUnusedBundlesReply reply1;
  node_manager_->HandleReleaseUnusedBundles(
      request, &reply1, [](Status s, std::function<void()>, std::function<void()>) {
        EXPECT_TRUE(s.ok());
      });

  if (bundle_in_use) {
    EXPECT_TRUE(leased_workers_.contains(lease_id));
    EXPECT_EQ(leased_workers_.size(), 1);
    EXPECT_TRUE(IsBundleRegistered(*placement_group_resource_manager_, bundle_id));
  } else {
    EXPECT_FALSE(leased_workers_.contains(lease_id));
    EXPECT_EQ(leased_workers_.size(), 0);
    EXPECT_FALSE(IsBundleRegistered(*placement_group_resource_manager_, bundle_id));
  }

  rpc::ReleaseUnusedBundlesReply reply2;
  node_manager_->HandleReleaseUnusedBundles(
      request, &reply2, [](Status s, std::function<void()>, std::function<void()>) {
        EXPECT_TRUE(s.ok());
      });

  if (bundle_in_use) {
    EXPECT_TRUE(leased_workers_.contains(lease_id));
    EXPECT_EQ(leased_workers_.size(), 1);
    EXPECT_TRUE(IsBundleRegistered(*placement_group_resource_manager_, bundle_id));
  } else {
    EXPECT_FALSE(leased_workers_.contains(lease_id));
    EXPECT_EQ(leased_workers_.size(), 0);
    EXPECT_FALSE(IsBundleRegistered(*placement_group_resource_manager_, bundle_id));
  }
}

INSTANTIATE_TEST_SUITE_P(ReleaseUnusedBundlesRetriesVariations,
                         ReleaseUnusedBundlesRetriesTest,
                         ::testing::Bool());

}  // namespace ray::raylet

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
