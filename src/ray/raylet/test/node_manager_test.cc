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

#include <cstdint>
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
#include "ray/common/buffer.h"
#include "ray/object_manager/plasma/client.h"
#include "ray/raylet/local_object_manager_interface.h"
#include "ray/raylet/scheduling/cluster_task_manager.h"
#include "ray/raylet/test/util.h"

namespace ray::raylet {
using ::testing::_;
using ::testing::Return;

namespace {

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

class FakePlasmaClient : public plasma::PlasmaClientInterface {
 public:
  Status Connect(const std::string &store_socket_name,
                 const std::string &manager_socket_name = "",
                 int num_retries = -1) override {
    return Status::OK();
  };

  Status CreateAndSpillIfNeeded(const ObjectID &object_id,
                                const ray::rpc::Address &owner_address,
                                bool is_mutable,
                                int64_t data_size,
                                const uint8_t *metadata,
                                int64_t metadata_size,
                                std::shared_ptr<Buffer> *data,
                                plasma::flatbuf::ObjectSource source,
                                int device_num = 0) override {
    return TryCreateImmediately(
        object_id, owner_address, data_size, metadata, metadata_size, data, source);
  }

  Status TryCreateImmediately(const ObjectID &object_id,
                              const ray::rpc::Address &owner_address,
                              int64_t data_size,
                              const uint8_t *metadata,
                              int64_t metadata_size,
                              std::shared_ptr<Buffer> *data,
                              plasma::flatbuf::ObjectSource source,
                              int device_num = 0) override {
    objects_ids_in_plasma_.emplace(object_id);
    objects_in_plasma_.emplace(
        object_id, std::make_pair(std::vector<uint8_t>{}, std::vector<uint8_t>{}));
    return Status::OK();
  }

  Status Get(const std::vector<ObjectID> &object_ids,
             int64_t timeout_ms,
             std::vector<plasma::ObjectBuffer> *object_buffers,
             bool is_from_worker) override {
    for (const auto &id : object_ids) {
      auto &buffers = objects_in_plasma_[id];
      plasma::ObjectBuffer shm_buffer{std::make_shared<SharedMemoryBuffer>(
                                          buffers.first.data(), buffers.first.size()),
                                      std::make_shared<SharedMemoryBuffer>(
                                          buffers.second.data(), buffers.second.size())};
      object_buffers->emplace_back(shm_buffer);
    }
    return Status::OK();
  }

  Status ExperimentalMutableObjectRegisterWriter(const ObjectID &object_id) override {
    return Status::OK();
  }

  Status GetExperimentalMutableObject(
      const ObjectID &object_id,
      std::unique_ptr<plasma::MutableObject> *mutable_object) override {
    return Status::OK();
  }

  Status Release(const ObjectID &object_id) override {
    objects_ids_in_plasma_.erase(object_id);
    return Status::OK();
  }

  Status Contains(const ObjectID &object_id, bool *has_object) override {
    *has_object = objects_ids_in_plasma_.find(object_id) != objects_ids_in_plasma_.end();
    return Status::OK();
  }

  Status Abort(const ObjectID &object_id) override { return Status::OK(); }

  Status Seal(const ObjectID &object_id) override { return Status::OK(); }

  Status Delete(const std::vector<ObjectID> &object_ids) override {
    for (const auto &id : object_ids) {
      objects_ids_in_plasma_.erase(id);
    }
    return Status::OK();
  }

  Status Disconnect() override { return Status::OK(); };

  std::string DebugString() { return ""; }

  int64_t store_capacity() { return 1; }

 private:
  absl::flat_hash_set<ObjectID> objects_ids_in_plasma_;
  absl::flat_hash_map<ObjectID, std::pair<std::vector<uint8_t>, std::vector<uint8_t>>>
      objects_in_plasma_;
};

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
      "raylet_liveness_self_check_interval_ms": 100
    })");

    NodeManagerConfig node_manager_config{};
    node_manager_config.maximum_startup_concurrency = 1;
    node_manager_config.store_socket_name = "test_store_socket";

    core_worker_subscriber_ = std::make_unique<pubsub::MockSubscriber>();
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
    EXPECT_CALL(*core_worker_subscriber_, DebugString()).WillRepeatedly(Return(""));

    raylet_node_id_ = NodeID::FromRandom();

    objects_pending_deletion_ = std::make_shared<absl::flat_hash_set<ObjectID>>();

    local_object_manager_ =
        std::make_unique<FakeLocalObjectManager>(objects_pending_deletion_);

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
                                                  *mock_store_client_,
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
  std::shared_ptr<LocalObjectManagerInterface> local_object_manager_;
  std::unique_ptr<DependencyManager> dependency_manager_;
  std::unique_ptr<gcs::MockGcsClient> mock_gcs_client_ =
      std::make_unique<gcs::MockGcsClient>();
  std::unique_ptr<MockObjectDirectory> mock_object_directory_;
  std::unique_ptr<MockObjectManager> mock_object_manager_;
  core::experimental::MockMutableObjectProvider *mock_mutable_object_provider_;
  std::shared_ptr<plasma::PlasmaClientInterface> mock_store_client_ =
      std::make_shared<FakePlasmaClient>();

  std::unique_ptr<NodeManager> node_manager_;
  MockWorkerPool mock_worker_pool_;
  absl::flat_hash_map<WorkerID, std::shared_ptr<WorkerInterface>> leased_workers_;
  std::shared_ptr<absl::flat_hash_set<ObjectID>> objects_pending_deletion_;
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

  // Save the pop_worker_callback for providing a mock worker later.
  PopWorkerCallback pop_worker_callback;
  EXPECT_CALL(mock_worker_pool_, PopWorker(_, _))
      .WillOnce(
          [&](const TaskSpecification &task_spec, const PopWorkerCallback &callback) {
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
  RAY_CHECK_OK(node_manager_->RegisterGcs());
  while (!publish_worker_failure_callback) {
    io_service_.run_one();
  }

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
  std::promise<Status> promise;
  rpc::RequestWorkerLeaseReply reply;
  rpc::RequestWorkerLeaseRequest request;
  request.mutable_resource_spec()->CopyFrom(task_spec_builder.GetMessage());
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
  EXPECT_CALL(*mock_gcs_client_->mock_node_accessor, AsyncCheckSelfAlive(_, _))
      .WillRepeatedly(Return(Status::OK()));
  EXPECT_CALL(mock_worker_pool_, GetAllRegisteredWorkers(_, _))
      .WillRepeatedly(Return(std::vector<std::shared_ptr<WorkerInterface>>{}));
  EXPECT_CALL(mock_worker_pool_, GetAllRegisteredDrivers(_))
      .WillRepeatedly(Return(std::vector<std::shared_ptr<WorkerInterface>>{}));
  EXPECT_CALL(mock_worker_pool_, IsWorkerAvailableForScheduling())
      .WillRepeatedly(Return(false));
  EXPECT_CALL(mock_worker_pool_, PrestartWorkers(_, _)).Times(1);

  // Save the pop_worker_callback for providing a mock worker later.
  PopWorkerCallback pop_worker_callback;
  EXPECT_CALL(mock_worker_pool_, PopWorker(_, _))
      .WillOnce(
          [&](const TaskSpecification &task_spec, const PopWorkerCallback &callback) {
            pop_worker_callback = callback;
          });

  // Save the publish_node_change_callback for publishing a node failure event later.
  std::function<void(const NodeID &id, rpc::GcsNodeInfo &&node_info)>
      publish_node_change_callback;
  EXPECT_CALL(*mock_gcs_client_->mock_node_accessor, AsyncSubscribeToNodeChange(_, _))
      .WillOnce([&](const gcs::SubscribeCallback<NodeID, rpc::GcsNodeInfo> &subscribe,
                    const gcs::StatusCallback &done) {
        publish_node_change_callback = subscribe;
        return Status::OK();
      });

  // Invoke RegisterGcs and wait until publish_node_change_callback is set.
  RAY_CHECK_OK(node_manager_->RegisterGcs());
  while (!publish_node_change_callback) {
    io_service_.run_one();
  }

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
  std::promise<Status> promise;
  rpc::RequestWorkerLeaseReply reply;
  rpc::RequestWorkerLeaseRequest request;
  request.mutable_resource_spec()->CopyFrom(task_spec_builder.GetMessage());
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
  GcsNodeInfo node_info;
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

}  // namespace ray::raylet

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
