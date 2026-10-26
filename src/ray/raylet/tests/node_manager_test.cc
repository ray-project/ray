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

#include "gtest/gtest.h"
#include "ray/asio/periodical_runner.h"
#include "ray/common/buffer.h"
#include "ray/common/bundle_spec.h"
#include "ray/common/cgroup2/noop_cgroup_manager.h"
#include "ray/common/flatbuf_utils.h"
#include "ray/common/scheduling/cluster_resource_data.h"
#include "ray/common/scheduling/resource_set.h"
#include "ray/core_worker/fake_mutable_object_provider.h"
#include "ray/core_worker_rpc_client/core_worker_client_pool.h"
#include "ray/core_worker_rpc_client/fake_core_worker_client.h"
#include "ray/gcs_rpc_client/fake_gcs_client.h"
#include "ray/object_manager/fake_object_directory.h"
#include "ray/object_manager/fake_object_manager.h"
#include "ray/object_manager/plasma/fake_plasma_client.h"
#include "ray/observability/fake_metric.h"
#include "ray/pubsub/fake_subscriber.h"
#include "ray/raylet/fake_local_lease_manager.h"
#include "ray/raylet/fake_worker.h"
#include "ray/raylet/fake_worker_pool.h"
#include "ray/raylet/local_object_manager_interface.h"
#include "ray/raylet/scheduling/cluster_lease_manager.h"
#include "ray/raylet/tests/util.h"
#include "ray/raylet_rpc_client/fake_raylet_client.h"
#include "ray/rpc/utils.h"
#include "ray/util/clock.h"

namespace ray::raylet {

namespace {

constexpr double kTestTotalCpuResource = 10.0;
constexpr double kTestObjectStoreMemory = 1024 * 1024 * 1024;  // 1 GiB

// Counts scheduling passes, including the one QueueAndScheduleLease runs internally.
class CountingClusterLeaseManager : public ClusterLeaseManager {
 public:
  using ClusterLeaseManager::ClusterLeaseManager;

  void ScheduleAndGrantLeases() override {
    ++num_schedule_and_grant_leases;
    ClusterLeaseManager::ScheduleAndGrantLeases();
  }

  int num_schedule_and_grant_leases = 0;
};

class FakeLocalObjectManager : public LocalObjectManagerInterface {
 public:
  FakeLocalObjectManager(
      std::shared_ptr<absl::flat_hash_set<ObjectID>> objects_pending_deletion)
      : objects_pending_deletion_(objects_pending_deletion) {}

  void PinObjectsAndWaitForFree(
      const std::vector<ObjectID> &object_ids,
      std::vector<std::unique_ptr<RayObject>> &&objects,
      const rpc::Address &owner_address,
      const ObjectID &generator_id = ObjectID::Nil(),
      const WorkerID &producer_worker_id = WorkerID::Nil()) override {
    pinned_objects.insert(pinned_objects.end(), object_ids.begin(), object_ids.end());
    for (const ObjectID &object_id : object_ids) {
      producer_worker_ids.emplace(object_id, producer_worker_id);
    }
  }

  std::vector<ObjectID> pinned_objects;
  absl::flat_hash_map<ObjectID, WorkerID> producer_worker_ids;

  WorkerID GetProducerWorkerId(const ObjectID &object_id) const override {
    auto it = producer_worker_ids.find(object_id);
    return it == producer_worker_ids.end() ? WorkerID::Nil() : it->second;
  }

  bool HasDiskCopy(const ObjectID &object_id) const override {
    return disk_copies.contains(object_id);
  }

  // Settable by tests: objects spilled or being spilled.
  absl::flat_hash_set<ObjectID> disk_copies;

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

  bool HoldsPrimaryCopy(const ObjectID &object_id) const override {
    return primary_copies.contains(object_id);
  }

  // Settable by tests: objects whose primary copy this node holds.
  absl::flat_hash_set<ObjectID> primary_copies;

  void ProcessSpilledObjectsDeleteQueue(uint32_t max_batch_size) override {}

  bool IsSpillingInProgress() override { return false; }

  void FillObjectStoreStats(rpc::GetNodeStatsReply *reply) const override {}

  void RecordMetrics() const override {}

  std::string GetLocalSpilledObjectURL(const ObjectID &object_id) override { return ""; }

  int64_t GetPrimaryBytes() const override { return primary_bytes_; }

  // Settable by tests to simulate pinned primary object copies on the node.
  int64_t primary_bytes_ = 0;

  bool HasLocallySpilledObjects() const override { return false; }

  void ReleaseFreedLocalObject(const ObjectID &object_id) override {}

  std::vector<ObjectID> GetLocalObjectsOwnedBy(const WorkerID &worker_id) const override {
    return {};
  }

  std::vector<ObjectID> GetLocalObjectsOwnedByOwnersOn(
      const NodeID &node_id) const override {
    return {};
  }

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
  ray::Clock clock;
  {
    // Worker backlog report from a disconnected worker should be ignored.
    FakeWorkerPool worker_pool;
    FakeLocalLeaseManager local_lease_manager;

    WorkerID worker_id = WorkerID::FromRandom();

    // Both driver and worker lookups return nullptr (the fake's defaults),
    // simulating a disconnected worker.

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

    // SetWorkerBacklog must not be called for a disconnected worker.
    EXPECT_EQ(local_lease_manager.set_worker_backlog_call_count, 0);
  }

  {
    // Worker backlog report from a connected driver should be recorded.
    FakeWorkerPool worker_pool;
    FakeLocalLeaseManager local_lease_manager;

    WorkerID worker_id = WorkerID::FromRandom();
    std::shared_ptr<MockWorker> driver =
        std::make_shared<MockWorker>(worker_id, 10, clock);

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

    worker_pool.registered_driver_by_id = driver;

    NodeManager::HandleReportWorkerBacklog(
        request,
        &reply,
        [](Status status, std::function<void()> success, std::function<void()> failure) {
        },
        worker_pool,
        local_lease_manager);

    EXPECT_EQ(local_lease_manager.set_worker_backlog_call_count, 1);
  }

  {
    // Worker backlog report from a connected worker should be recorded.
    FakeWorkerPool worker_pool;
    FakeLocalLeaseManager local_lease_manager;

    WorkerID worker_id = WorkerID::FromRandom();
    std::shared_ptr<MockWorker> worker =
        std::make_shared<MockWorker>(worker_id, 10, clock);

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

    // Driver lookup returns nullptr (default); worker lookup returns the worker.
    worker_pool.registered_worker_by_id = worker;

    NodeManager::HandleReportWorkerBacklog(
        request,
        &reply,
        [](Status status, std::function<void()> success, std::function<void()> failure) {
        },
        worker_pool,
        local_lease_manager);

    EXPECT_EQ(local_lease_manager.set_worker_backlog_call_count, 1);
  }
}

class NodeManagerTest : public ::testing::Test {
 public:
  NodeManagerTest()
      : client_call_manager_(io_service_, /*record_stats=*/false, /*local_address=*/""),
        worker_rpc_pool_(
            [](const auto &) { return std::make_shared<rpc::FakeCoreWorkerClient>(); }),
        raylet_client_pool_(
            [](const auto &) { return std::make_shared<rpc::FakeRayletClient>(); }),
        fake_task_by_state_counter_() {
    RayConfig::instance().initialize(R"({
      "raylet_liveness_self_check_interval_ms": 100
    })");

    NodeManagerConfig node_manager_config{};
    node_manager_config.maximum_startup_concurrency = 1;
    node_manager_config.store_socket_name = "test_store_socket";
    node_manager_config.resource_config =
        ResourceSet(absl::flat_hash_map<std::string, double>{
            {"CPU", kTestTotalCpuResource},
            {"object_store_memory", kTestObjectStoreMemory}});
    // Set non-zero ports to skip waiting for agents reporting ports back.
    // we don't actually start the agents in this test.
    node_manager_config.metrics_agent_port = 44386;
    node_manager_config.metrics_export_port = 63802;
    node_manager_config.dashboard_agent_listen_port = 52365;
    node_manager_config.runtime_env_agent_port = 37429;

    core_worker_subscriber_ = std::make_unique<pubsub::FakeSubscriber>();
    fake_object_directory_ = std::make_unique<FakeObjectDirectory>();
    fake_object_manager_ = std::make_unique<FakeObjectManager>();

    auto mutable_object_provider =
        std::make_unique<core::experimental::FakeMutableObjectProvider>();
    fake_mutable_object_provider_ = mutable_object_provider.get();

    raylet_node_id_ = NodeID::FromRandom();

    objects_pending_deletion_ = std::make_shared<absl::flat_hash_set<ObjectID>>();

    local_object_manager_ =
        std::make_shared<FakeLocalObjectManager>(objects_pending_deletion_);

    lease_dependency_manager_ = std::make_unique<LeaseDependencyManager>(
        *fake_object_manager_, fake_task_by_state_counter_);

    cluster_resource_scheduler_ = std::make_unique<ClusterResourceScheduler>(
        ray::PeriodicalRunner::Create(io_service_),
        ray::scheduling::NodeID(raylet_node_id_.Binary()),
        node_manager_config.resource_config.GetResourceMap(),
        /*is_node_available_fn*/
        [&](ray::scheduling::NodeID node_id) {
          return fake_gcs_client_->Nodes().IsNodeAlive(
              NodeID::FromBinary(node_id.Binary()));
        },
        fake_resource_usage_gauge_,
        clock_,
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
          return fake_object_manager_->GetUsedMemory();
        },
        /*get_pull_manager_at_capacity*/
        [&]() { return fake_object_manager_->PullManagerHasPullsQueued(); },
        [](const ray::rpc::NodeDeathInfo &node_death_info) {},
        /*labels*/
        node_manager_config.labels);

    auto get_node_info_func = [&](const NodeID &node_id) {
      return fake_gcs_client_->Nodes().GetNodeAddressAndLiveness(node_id);
    };

    auto max_task_args_memory = static_cast<int64_t>(
        static_cast<float>(fake_object_manager_->GetMemoryCapacity()) *
        RayConfig::instance().max_task_args_memory_fraction());

    ray::raylet::SchedulerMetrics scheduler_metrics{
        fake_scheduler_tasks_gauge_,
        fake_scheduler_unscheduleable_tasks_gauge_,
        fake_scheduler_failed_worker_startup_total_gauge_,
        fake_internal_num_spilled_tasks_gauge_,
        fake_internal_num_infeasible_scheduling_classes_gauge_};

    local_lease_manager_ = std::make_unique<LocalLeaseManager>(
        raylet_node_id_,
        *cluster_resource_scheduler_,
        *lease_dependency_manager_,
        get_node_info_func,
        fake_worker_pool_,
        leased_workers_,
        [&](const std::vector<ObjectID> &object_ids,
            std::vector<std::unique_ptr<RayObject>> *results) {
          return node_manager_->GetObjectsFromPlasma(object_ids, results);
        },
        max_task_args_memory,
        scheduler_metrics,
        clock_);

    cluster_lease_manager_ = std::make_unique<CountingClusterLeaseManager>(
        raylet_node_id_,
        *cluster_resource_scheduler_,
        get_node_info_func,
        [](const ray::RayLease &lease) {},
        *local_lease_manager_);

    placement_group_resource_manager_ =
        std::make_unique<NewPlacementGroupResourceManager>(*cluster_resource_scheduler_);

    node_manager_ = std::make_unique<NodeManager>(
        io_service_,
        ray::PeriodicalRunner::Create(io_service_),
        raylet_node_id_,
        "test_node_name",
        node_manager_config,
        *fake_gcs_client_,
        client_call_manager_,
        worker_rpc_pool_,
        raylet_client_pool_,
        *core_worker_subscriber_,
        *cluster_resource_scheduler_,
        *local_lease_manager_,
        *cluster_lease_manager_,
        *fake_object_directory_,
        *fake_object_manager_,
        *local_object_manager_,
        *lease_dependency_manager_,
        fake_worker_pool_,
        leased_workers_,
        fake_store_client_,
        std::move(mutable_object_provider),
        /*shutdown_raylet_gracefully=*/
        [](const auto &) {},
        [](const std::string &) {},
        std::make_unique<NoopCgroupManager>(),
        shutting_down_,
        *placement_group_resource_manager_,
        boost::asio::basic_socket_acceptor<local_stream_protocol>(io_service_),
        boost::asio::basic_stream_socket<local_stream_protocol>(io_service_),
        fake_memory_manager_worker_eviction_total_count_,
        fake_node_manager_unexpected_worker_failure_total_count_,
        fake_clock_);

    // The NodeManager constructor wires up the worker pool exactly once.
    EXPECT_EQ(fake_worker_pool_.set_node_manager_port_calls, 1);
    EXPECT_EQ(fake_worker_pool_.set_runtime_env_agent_client_calls, 1);
    EXPECT_EQ(fake_worker_pool_.start_calls, 1);
  }

  // Grant a lease that carries one plasma dependency of `arg_bytes` so the
  // LocalLeaseManager pins it as a lease argument. Returns (lease_id, worker).
  std::pair<LeaseID, std::shared_ptr<MockWorker>> GrantLeaseWithPinnedArg(
      size_t arg_bytes) {
    auto lease_spec = BuildLeaseSpec({});
    ObjectID object_dep = ObjectID::FromRandom();
    lease_spec.GetMutableMessage().add_dependencies()->set_object_id(object_dep.Binary());

    rpc::Address owner_addr;
    RAY_UNUSED(fake_store_client_->TryCreateImmediately(
        object_dep,
        owner_addr,
        arg_bytes,
        nullptr,
        arg_bytes,
        nullptr,
        plasma::flatbuf::ObjectSource::CreatedByWorker,
        0));

    LeaseID lease_id = LeaseID::FromRandom();
    lease_spec.GetMutableMessage().set_lease_id(lease_id.Binary());
    rpc::RequestWorkerLeaseRequest request;
    rpc::RequestWorkerLeaseReply reply;
    request.mutable_lease_spec()->CopyFrom(lease_spec.GetMessage());
    request.set_backlog_size(1);
    request.set_grant_or_reject(true);
    request.set_is_selected_based_on_locality(true);

    std::shared_ptr<MockWorker> worker =
        std::make_shared<MockWorker>(WorkerID::FromRandom(), 10, clock_);
    PopWorkerCallback pop_cb;
    fake_worker_pool_.pop_worker_hook = [&](const LeaseSpecification &,
                                            const PopWorkerCallback &cb) { pop_cb = cb; };
    node_manager_->HandleRequestWorkerLease(
        request, &reply, [](Status s, std::function<void()>, std::function<void()>) {
          ASSERT_TRUE(s.ok());
        });
    std::vector<LeaseID> ready = lease_dependency_manager_->HandleObjectLocal(object_dep);
    local_lease_manager_->LeasesUnblocked(ready);
    RAY_CHECK(pop_cb);
    pop_cb(worker, PopWorkerStatus::OK, "");
    fake_worker_pool_.pop_worker_hook = nullptr;
    return {lease_id, worker};
  }

  instrumented_io_context io_service_;
  rpc::ClientCallManager client_call_manager_;
  rpc::CoreWorkerClientPool worker_rpc_pool_;
  rpc::RayletClientPool raylet_client_pool_;

  NodeID raylet_node_id_;
  std::unique_ptr<pubsub::FakeSubscriber> core_worker_subscriber_;
  FakeClock fake_clock_;
  ray::Clock clock_;
  std::unique_ptr<ClusterResourceScheduler> cluster_resource_scheduler_;
  std::unique_ptr<LocalLeaseManager> local_lease_manager_;
  std::unique_ptr<CountingClusterLeaseManager> cluster_lease_manager_;
  std::unique_ptr<PlacementGroupResourceManager> placement_group_resource_manager_;
  std::shared_ptr<FakeLocalObjectManager> local_object_manager_;
  std::unique_ptr<LeaseDependencyManager> lease_dependency_manager_;
  std::unique_ptr<gcs::FakeGcsClient> fake_gcs_client_ =
      std::make_unique<gcs::FakeGcsClient>();
  std::unique_ptr<FakeObjectDirectory> fake_object_directory_;
  std::unique_ptr<FakeObjectManager> fake_object_manager_;
  core::experimental::FakeMutableObjectProvider *fake_mutable_object_provider_;
  std::shared_ptr<plasma::PlasmaClientInterface> fake_store_client_ =
      std::make_shared<plasma::FakePlasmaClient>();

  std::unique_ptr<NodeManager> node_manager_;
  FakeWorkerPool fake_worker_pool_;
  absl::flat_hash_map<LeaseID, std::shared_ptr<WorkerInterface>> leased_workers_;
  std::shared_ptr<absl::flat_hash_set<ObjectID>> objects_pending_deletion_;
  ray::observability::FakeGauge fake_task_by_state_counter_;

  std::atomic_bool shutting_down_ = RayletShutdownState::ALIVE;
  ray::observability::FakeGauge fake_resource_usage_gauge_;
  ray::observability::FakeGauge fake_scheduler_tasks_gauge_;
  ray::observability::FakeGauge fake_scheduler_unscheduleable_tasks_gauge_;
  ray::observability::FakeGauge fake_scheduler_failed_worker_startup_total_gauge_;
  ray::observability::FakeGauge fake_internal_num_spilled_tasks_gauge_;
  ray::observability::FakeGauge fake_internal_num_infeasible_scheduling_classes_gauge_;
  ray::observability::FakeCounter fake_memory_manager_worker_eviction_total_count_;
  ray::observability::FakeCounter
      fake_node_manager_unexpected_worker_failure_total_count_;
};

TEST_F(NodeManagerTest, HandleIsLocalWorkerDeadUnknownWorker) {
  WorkerID worker_id = WorkerID::FromRandom();
  // Both worker and driver lookups return nullptr (the fake's defaults).
  rpc::IsLocalWorkerDeadRequest request;
  request.set_worker_id(worker_id.Binary());
  rpc::IsLocalWorkerDeadReply reply;
  bool replied = false;
  node_manager_->HandleIsLocalWorkerDead(
      request,
      &reply,
      [&](Status, std::function<void()> success, std::function<void()> failure) {
        replied = true;
      });
  EXPECT_TRUE(replied);
  EXPECT_TRUE(reply.is_dead());
}

TEST_F(NodeManagerTest, HandleIsLocalWorkerDeadRegisteredDriver) {
  WorkerID worker_id = WorkerID::FromRandom();
  auto driver = std::make_shared<MockWorker>(worker_id, 10, clock_);
  // Worker lookup returns nullptr (default); driver lookup returns the driver.
  fake_worker_pool_.registered_driver_by_id = driver;
  rpc::IsLocalWorkerDeadRequest request;
  request.set_worker_id(worker_id.Binary());
  rpc::IsLocalWorkerDeadReply reply;
  bool replied = false;
  node_manager_->HandleIsLocalWorkerDead(
      request,
      &reply,
      [&](Status, std::function<void()> success, std::function<void()> failure) {
        replied = true;
      });
  EXPECT_TRUE(replied);
  EXPECT_FALSE(reply.is_dead());
}

TEST_F(NodeManagerTest, HandleIsLocalWorkerDeadRegisteredWorker) {
  WorkerID worker_id = WorkerID::FromRandom();
  auto worker = std::make_shared<MockWorker>(worker_id, 10, clock_);
  // || short-circuits: GetRegisteredDriver is never called when worker is found.
  fake_worker_pool_.registered_worker_by_id = worker;
  rpc::IsLocalWorkerDeadRequest request;
  request.set_worker_id(worker_id.Binary());
  rpc::IsLocalWorkerDeadReply reply;
  bool replied = false;
  node_manager_->HandleIsLocalWorkerDead(
      request,
      &reply,
      [&](Status, std::function<void()> success, std::function<void()> failure) {
        replied = true;
      });
  EXPECT_TRUE(replied);
  EXPECT_FALSE(reply.is_dead());
}

TEST_F(NodeManagerTest, TestRegisterGcsAndCheckSelfAlive) {
  // GetAllRegisteredWorkers/Drivers return empty and AllAliveWorkersAreActors returns
  // false by the fake worker pool's defaults.
  std::promise<void> promise;
  // The liveness self-check fires AsyncCheckAlive; set the promise on the first call
  // (guarded so a subsequent timer tick doesn't set an already-satisfied promise).
  fake_gcs_client_->fake_node_accessor->async_check_alive_hook =
      [&promise, called = false](const std::vector<NodeID> &,
                                 int64_t,
                                 const rpc::MultiItemCallback<bool> &) mutable {
        if (!called) {
          called = true;
          promise.set_value();
        }
      };
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

  // RegisterGcs subscribes to node, worker, and job changes exactly once, and the
  // liveness self-check invoked AsyncCheckAlive at least once.
  EXPECT_EQ(fake_gcs_client_->fake_node_accessor
                ->async_subscribe_to_node_address_and_liveness_change_call_count,
            1);
  EXPECT_EQ(fake_gcs_client_->fake_worker_accessor
                ->async_subscribe_to_worker_failures_call_count,
            1);
  EXPECT_EQ(fake_gcs_client_->fake_job_accessor->async_subscribe_all_call_count, 1);
  EXPECT_GE(fake_gcs_client_->fake_node_accessor->async_check_alive_calls.size(), 1u);
}

TEST_F(NodeManagerTest, TestDetachedWorkerIsKilledByFailedWorker) {
  // GetAllRegisteredWorkers/Drivers return empty and AllAliveWorkersAreActors returns
  // false by the fake worker pool's defaults.

  // Save the pop_worker_callback for providing a mock worker later.
  PopWorkerCallback pop_worker_callback;
  fake_worker_pool_.pop_worker_hook = [&](const LeaseSpecification &lease_spec,
                                          const PopWorkerCallback &callback) {
    pop_worker_callback = callback;
  };

  // The worker-failure subscribe callback is captured by the fake worker accessor and
  // used to publish a worker failure event later.
  auto &publish_worker_failure_callback =
      fake_gcs_client_->fake_worker_accessor
          ->async_subscribe_to_worker_failures_subscribe;

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
  const auto worker = std::make_shared<MockWorker>(WorkerID::FromRandom(), 10, clock_);
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

  EXPECT_EQ(fake_worker_pool_.prestart_workers_calls, 1);
  EXPECT_EQ(fake_worker_pool_.pop_worker_calls, 1);
}

TEST_F(NodeManagerTest, TestDetachedWorkerIsKilledByFailedNode) {
  // GetAllRegisteredWorkers/Drivers return empty and AllAliveWorkersAreActors returns
  // false by the fake worker pool's defaults.

  // Save the pop_worker_callback for providing a mock worker later.
  PopWorkerCallback pop_worker_callback;
  fake_worker_pool_.pop_worker_hook = [&](const LeaseSpecification &lease_spec,
                                          const PopWorkerCallback &callback) {
    pop_worker_callback = callback;
  };

  node_manager_->RegisterGcs();

  // The node-change subscribe callback is captured by the fake node accessor and used
  // to publish a node failure event later.
  auto &publish_node_change_callback =
      fake_gcs_client_->fake_node_accessor->node_address_and_liveness_subscribe;
  ASSERT_TRUE(publish_node_change_callback);

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
  const auto worker = std::make_shared<MockWorker>(WorkerID::FromRandom(), 10, clock_);
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

  // The node removal is propagated to the object directory and object manager exactly
  // once.
  EXPECT_EQ(fake_object_directory_->handle_node_removed_calls.size(), 1u);
  EXPECT_EQ(fake_object_manager_->handle_node_removed_calls.size(), 1u);
  EXPECT_EQ(fake_worker_pool_.prestart_workers_calls, 1);
  EXPECT_EQ(fake_worker_pool_.pop_worker_calls, 1);
}

TEST_F(NodeManagerTest, TestPinningAnObjectPendingDeletionFails) {
  // Object needs to be created in plasma before it can be pinned.
  rpc::Address owner_addr;
  plasma::flatbuf::ObjectSource source = plasma::flatbuf::ObjectSource::CreatedByWorker;
  ObjectID id = ObjectID::FromRandom();

  RAY_UNUSED(fake_store_client_->TryCreateImmediately(
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

TEST_F(NodeManagerTest, TestPinObjectIDsRecordsTheProducer) {
  rpc::Address owner_addr;
  plasma::flatbuf::ObjectSource source = plasma::flatbuf::ObjectSource::CreatedByWorker;
  const WorkerID producer_worker_id = WorkerID::FromRandom();
  const ObjectID with_producer = ObjectID::FromRandom();
  const ObjectID without_producer = ObjectID::FromRandom();
  for (const ObjectID &id : {with_producer, without_producer}) {
    RAY_UNUSED(fake_store_client_->TryCreateImmediately(
        id, owner_addr, 1024, nullptr, 1024, nullptr, source, 0));
  }

  rpc::PinObjectIDsRequest request_with_producer;
  request_with_producer.add_object_ids(with_producer.Binary());
  request_with_producer.set_producer_worker_id(producer_worker_id.Binary());
  rpc::PinObjectIDsRequest request_without_producer;
  request_without_producer.add_object_ids(without_producer.Binary());
  for (const rpc::PinObjectIDsRequest &request :
       {request_with_producer, request_without_producer}) {
    rpc::PinObjectIDsReply reply;
    node_manager_->HandlePinObjectIDs(
        request,
        &reply,
        [](Status s, std::function<void()> success, std::function<void()> failure) {});
    ASSERT_TRUE(reply.successes(0));
  }

  EXPECT_EQ(local_object_manager_->GetProducerWorkerId(with_producer),
            producer_worker_id);
  EXPECT_TRUE(local_object_manager_->GetProducerWorkerId(without_producer).IsNil());
}

TEST_F(NodeManagerTest, TestPinAndReportMovedObject) {
  rpc::Address owner_addr;
  owner_addr.set_worker_id(WorkerID::FromRandom().Binary());
  owner_addr.set_node_id(NodeID::FromRandom().Binary());
  plasma::flatbuf::ObjectSource source = plasma::flatbuf::ObjectSource::CreatedByWorker;
  ObjectID id = ObjectID::FromRandom();

  using PinResult = NodeManager::MovedObjectPinResult;

  // This node already holds the primary copy: nothing to do.
  local_object_manager_->primary_copies.insert(id);
  EXPECT_EQ(node_manager_->PinAndReportMovedObject(id, owner_addr), PinResult::kPinned);
  EXPECT_TRUE(local_object_manager_->pinned_objects.empty());
  EXPECT_TRUE(fake_object_directory_->primary_moved_objects.empty());
  local_object_manager_->primary_copies.erase(id);

  // The object is not in the store: nothing is pinned or reported.
  EXPECT_EQ(node_manager_->PinAndReportMovedObject(id, owner_addr),
            PinResult::kNotInStore);
  EXPECT_TRUE(local_object_manager_->pinned_objects.empty());
  EXPECT_TRUE(fake_object_directory_->primary_moved_objects.empty());

  RAY_UNUSED(fake_store_client_->TryCreateImmediately(
      id, owner_addr, 1024, nullptr, 1024, nullptr, source, 0));
  EXPECT_EQ(node_manager_->PinAndReportMovedObject(id, owner_addr), PinResult::kPinned);
  EXPECT_EQ(local_object_manager_->pinned_objects, std::vector<ObjectID>{id});
  EXPECT_EQ(fake_object_directory_->primary_moved_objects, std::vector<ObjectID>{id});
  // The consumer's pin doesn't record a producer.
  EXPECT_TRUE(local_object_manager_->GetProducerWorkerId(id).IsNil());

  // The object is pending deletion: the pin is refused.
  objects_pending_deletion_->emplace(id);
  EXPECT_EQ(node_manager_->PinAndReportMovedObject(id, owner_addr),
            PinResult::kNotPinnable);
  EXPECT_EQ(local_object_manager_->pinned_objects.size(), 1u);
  EXPECT_EQ(fake_object_directory_->primary_moved_objects.size(), 1u);

  // The owner is dead: the pin is refused although the copy is in the store.
  const ObjectID orphan = ObjectID::FromRandom();
  RAY_UNUSED(fake_store_client_->TryCreateImmediately(
      orphan, owner_addr, 1024, nullptr, 1024, nullptr, source, 0));
  node_manager_->failed_workers_cache_.insert(
      WorkerID::FromBinary(owner_addr.worker_id()));
  EXPECT_EQ(node_manager_->PinAndReportMovedObject(orphan, owner_addr),
            PinResult::kNotPinnable);
  EXPECT_EQ(local_object_manager_->pinned_objects.size(), 1u);
  EXPECT_EQ(fake_object_directory_->primary_moved_objects.size(), 1u);
}

// The producer's hook runs once, when the object's last copy on the node is deleted.
class NodeManagerOnObjectFreedTest : public NodeManagerTest {
 protected:
  NodeManagerOnObjectFreedTest()
      : producer_(std::make_shared<raylet::MockWorker>(producer_id_, 10, clock_)) {
    producer_->Connect(producer_client_);
    fake_worker_pool_.registered_worker_by_id = producer_;
  }

  void AddRamCopy(const WorkerID &producer_worker_id) {
    fake_object_manager_->local_object_producer_worker_ids[object_id_] =
        producer_worker_id;
    ObjectInfo object_info;
    object_info.object_id = object_id_;
    object_info.producer_worker_id = producer_worker_id;
    node_manager_->HandleObjectLocal(object_info);
  }

  // Reads the copy's producer before deleting it, as the raylet does.
  void DeleteRamCopy() {
    const WorkerID producer_worker_id =
        fake_object_manager_->GetLocalObjectProducerWorkerId(object_id_);
    fake_object_manager_->local_object_producer_worker_ids.erase(object_id_);
    node_manager_->HandleObjectMissing(object_id_, producer_worker_id);
  }

  void AddDiskCopy() { local_object_manager_->disk_copies.insert(object_id_); }

  void DeleteDiskCopy() {
    local_object_manager_->disk_copies.erase(object_id_);
    node_manager_->HandleSpilledCopyDeleted(object_id_, producer_id_);
  }

  // The producer's pin, which this node records as the primary copy.
  void Pin(const WorkerID &producer_worker_id) {
    local_object_manager_->primary_copies.insert(object_id_);
    local_object_manager_->producer_worker_ids[object_id_] = producer_worker_id;
  }

  // An actor task on this node takes the object as an arg.
  void Consume(bool is_move = true) {
    rpc::ObjectReference ref;
    ref.set_object_id(object_id_.Binary());
    node_manager_->HandleMovedActorCallArgs(WorkerID::FromRandom(), {ref}, {is_move});
  }

  bool FiredAtConsume() const {
    return node_manager_->objects_freed_at_consume_.contains(object_id_);
  }

  // The objects the producer was told about, in order.
  std::vector<ObjectID> FreedObjects() const {
    std::vector<ObjectID> freed;
    for (const rpc::OnObjectFreedRequest &request :
         producer_client_->on_object_freed_requests) {
      EXPECT_EQ(WorkerID::FromBinary(request.intended_worker_id()), producer_id_);
      freed.push_back(ObjectID::FromBinary(request.object_id()));
    }
    return freed;
  }

  const ObjectID object_id_ = ObjectID::FromRandom();
  const WorkerID producer_id_ = WorkerID::FromRandom();
  std::shared_ptr<rpc::FakeCoreWorkerClient> producer_client_ =
      std::make_shared<rpc::FakeCoreWorkerClient>();
  std::shared_ptr<raylet::MockWorker> producer_;
};

TEST_F(NodeManagerOnObjectFreedTest, NeverSpilledFiresWhenTheCopyIsDeleted) {
  AddRamCopy(producer_id_);
  DeleteRamCopy();
  EXPECT_EQ(FreedObjects(), std::vector<ObjectID>{object_id_});
}

TEST_F(NodeManagerOnObjectFreedTest, SpilledFiresWhenTheDiskCopyIsDeletedLast) {
  // The in-memory copy is deleted first, by the free or by eviction.
  AddRamCopy(producer_id_);
  AddDiskCopy();
  DeleteRamCopy();
  EXPECT_TRUE(FreedObjects().empty());
  DeleteDiskCopy();
  EXPECT_EQ(FreedObjects(), std::vector<ObjectID>{object_id_});
}

TEST_F(NodeManagerOnObjectFreedTest, SpilledFiresWhenTheRamCopyIsDeletedLast) {
  AddRamCopy(producer_id_);
  AddDiskCopy();
  DeleteDiskCopy();
  EXPECT_TRUE(FreedObjects().empty());
  DeleteRamCopy();
  EXPECT_EQ(FreedObjects(), std::vector<ObjectID>{object_id_});
}

TEST_F(NodeManagerOnObjectFreedTest, RestoredCopyFiresWhenTheDiskCopyIsDeletedLast) {
  AddRamCopy(producer_id_);
  AddDiskCopy();
  DeleteRamCopy();
  // A restored or received copy records no producer.
  AddRamCopy(WorkerID::Nil());
  DeleteRamCopy();
  EXPECT_TRUE(FreedObjects().empty());
  DeleteDiskCopy();
  EXPECT_EQ(FreedObjects(), std::vector<ObjectID>{object_id_});
}

TEST_F(NodeManagerOnObjectFreedTest, RestoredCopyFiresWhenTheRamCopyIsDeletedLast) {
  AddRamCopy(producer_id_);
  AddDiskCopy();
  DeleteRamCopy();
  AddRamCopy(WorkerID::Nil());
  DeleteDiskCopy();
  EXPECT_TRUE(FreedObjects().empty());
  DeleteRamCopy();
  EXPECT_EQ(FreedObjects(), std::vector<ObjectID>{object_id_});
}

TEST_F(NodeManagerOnObjectFreedTest, NoProducerDoesNotFire) {
  AddRamCopy(WorkerID::Nil());
  DeleteRamCopy();
  EXPECT_TRUE(FreedObjects().empty());
}

TEST_F(NodeManagerOnObjectFreedTest, MoveFiresOnceAndTheFreeDoesNotFireAgain) {
  AddRamCopy(producer_id_);
  Pin(producer_id_);
  Consume();
  EXPECT_EQ(FreedObjects(), std::vector<ObjectID>{object_id_});
  // A retry of the consuming task, or the same arg moved twice.
  Consume();
  EXPECT_EQ(FreedObjects(), std::vector<ObjectID>{object_id_});
  DeleteRamCopy();
  EXPECT_EQ(FreedObjects(), std::vector<ObjectID>{object_id_});
  EXPECT_FALSE(FiredAtConsume());
}

TEST_F(NodeManagerOnObjectFreedTest, MoveBeforeThePinFiresFromTheCopyList) {
  AddRamCopy(producer_id_);
  Consume();
  EXPECT_EQ(FreedObjects(), std::vector<ObjectID>{object_id_});
  DeleteRamCopy();
  EXPECT_EQ(FreedObjects(), std::vector<ObjectID>{object_id_});
  EXPECT_FALSE(FiredAtConsume());
}

TEST_F(NodeManagerOnObjectFreedTest,
       MovedSpilledObjectDoesNotFireAgainWhenRamCopyIsLast) {
  AddRamCopy(producer_id_);
  Pin(producer_id_);
  AddDiskCopy();
  Consume();
  DeleteDiskCopy();
  DeleteRamCopy();
  EXPECT_EQ(FreedObjects(), std::vector<ObjectID>{object_id_});
  EXPECT_FALSE(FiredAtConsume());
}

TEST_F(NodeManagerOnObjectFreedTest,
       MovedSpilledObjectDoesNotFireAgainWhenDiskCopyIsLast) {
  AddRamCopy(producer_id_);
  Pin(producer_id_);
  AddDiskCopy();
  Consume();
  DeleteRamCopy();
  EXPECT_TRUE(FiredAtConsume());
  DeleteDiskCopy();
  EXPECT_EQ(FreedObjects(), std::vector<ObjectID>{object_id_});
  EXPECT_FALSE(FiredAtConsume());
}

TEST_F(NodeManagerOnObjectFreedTest, MoveAfterEvictionFiresFromThePinRecord) {
  AddRamCopy(producer_id_);
  Pin(producer_id_);
  AddDiskCopy();
  DeleteRamCopy();
  Consume();
  EXPECT_EQ(FreedObjects(), std::vector<ObjectID>{object_id_});
  // Restored for the consumer, then freed with the disk copy deleted first.
  AddRamCopy(WorkerID::Nil());
  DeleteDiskCopy();
  DeleteRamCopy();
  EXPECT_EQ(FreedObjects(), std::vector<ObjectID>{object_id_});
  EXPECT_FALSE(FiredAtConsume());
}

TEST_F(NodeManagerOnObjectFreedTest, MovedRestoredObjectDoesNotFireAgainWhenDiskIsLast) {
  AddRamCopy(producer_id_);
  Pin(producer_id_);
  AddDiskCopy();
  DeleteRamCopy();
  Consume();
  AddRamCopy(WorkerID::Nil());
  DeleteRamCopy();
  DeleteDiskCopy();
  EXPECT_EQ(FreedObjects(), std::vector<ObjectID>{object_id_});
  EXPECT_FALSE(FiredAtConsume());
}

TEST_F(NodeManagerOnObjectFreedTest, FreeDuringTheRestoreDoesNotFireAgain) {
  AddRamCopy(producer_id_);
  Pin(producer_id_);
  AddDiskCopy();
  DeleteRamCopy();
  Consume();
  // The free deletes the disk copy before the restored copy is sealed.
  DeleteDiskCopy();
  EXPECT_FALSE(FiredAtConsume());
  AddRamCopy(WorkerID::Nil());
  DeleteRamCopy();
  EXPECT_EQ(FreedObjects(), std::vector<ObjectID>{object_id_});
}

TEST_F(NodeManagerOnObjectFreedTest, ArgThatIsNotMovedFiresAtTheFree) {
  AddRamCopy(producer_id_);
  Pin(producer_id_);
  Consume(/*is_move=*/false);
  EXPECT_TRUE(FreedObjects().empty());
  DeleteRamCopy();
  EXPECT_EQ(FreedObjects(), std::vector<ObjectID>{object_id_});
}

TEST_F(NodeManagerOnObjectFreedTest, MoveOfACopyFromAnotherNodeDoesNotFire) {
  // A received copy, pinned here by the move with no producer.
  AddRamCopy(WorkerID::Nil());
  Pin(WorkerID::Nil());
  Consume();
  DeleteRamCopy();
  EXPECT_TRUE(FreedObjects().empty());
  EXPECT_FALSE(FiredAtConsume());
}

TEST_F(NodeManagerOnObjectFreedTest, DeadProducerDoesNotFire) {
  fake_worker_pool_.registered_worker_by_id = nullptr;
  AddRamCopy(producer_id_);
  AddDiskCopy();
  DeleteRamCopy();
  DeleteDiskCopy();
  EXPECT_TRUE(FreedObjects().empty());
}

namespace {

ObjectInfo MakeObjectInfo(const ObjectID &object_id, const rpc::Address &owner_address) {
  ObjectInfo info;
  info.object_id = object_id;
  info.owner_node_id = NodeID::FromBinary(owner_address.node_id());
  info.owner_ip_address = owner_address.ip_address();
  info.owner_port = owner_address.port();
  info.owner_worker_id = WorkerID::FromBinary(owner_address.worker_id());
  return info;
}

rpc::Address MakeOwnerAddress() {
  rpc::Address owner_address;
  owner_address.set_worker_id(WorkerID::FromRandom().Binary());
  owner_address.set_node_id(NodeID::FromRandom().Binary());
  owner_address.set_ip_address("127.0.0.1");
  owner_address.set_port(1234);
  return owner_address;
}

}  // namespace

/// A moved object is pinned as the primary copy when it becomes local, with
/// the owner from the object info. An object nobody moves is left alone.
TEST_F(NodeManagerTest, TestHandleObjectLocalPinsMovedObject) {
  const rpc::Address owner_addr = MakeOwnerAddress();
  const ObjectID moved = ObjectID::FromRandom();
  const ObjectID plain = ObjectID::FromRandom();
  plasma::flatbuf::ObjectSource source = plasma::flatbuf::ObjectSource::CreatedByWorker;
  for (const ObjectID &id : {moved, plain}) {
    RAY_UNUSED(fake_store_client_->TryCreateImmediately(
        id, owner_addr, 1024, nullptr, 1024, nullptr, source, 0));
  }
  fake_object_manager_->MarkMovePull(moved, WorkerID::FromRandom());

  node_manager_->HandleObjectLocal(MakeObjectInfo(plain, owner_addr));
  EXPECT_TRUE(local_object_manager_->pinned_objects.empty());
  EXPECT_TRUE(fake_object_directory_->primary_moved_objects.empty());

  node_manager_->HandleObjectLocal(MakeObjectInfo(moved, owner_addr));
  EXPECT_EQ(local_object_manager_->pinned_objects, std::vector<ObjectID>{moved});
  EXPECT_EQ(fake_object_directory_->primary_moved_objects, std::vector<ObjectID>{moved});
}

/// When the copy left the store before it could be pinned, the record survives
/// the cancel of the wait pull, so the fetch of the object by the worker that moves it
/// pulls it as a move. A pinned copy, or one that must not be pinned because it
/// is being freed, leaves no record behind.
TEST_F(NodeManagerTest, TestHandleObjectLocalKeepsRecordAfterPinMiss) {
  const rpc::Address owner_addr = MakeOwnerAddress();
  const WorkerID consumer = WorkerID::FromRandom();
  const ObjectID evicted = ObjectID::FromRandom();
  const ObjectID present = ObjectID::FromRandom();
  const ObjectID freed = ObjectID::FromRandom();
  plasma::flatbuf::ObjectSource source = plasma::flatbuf::ObjectSource::CreatedByWorker;
  for (const ObjectID &id : {present, freed}) {
    RAY_UNUSED(fake_store_client_->TryCreateImmediately(
        id, owner_addr, 1024, nullptr, 1024, nullptr, source, 0));
  }
  objects_pending_deletion_->emplace(freed);
  std::vector<rpc::ObjectReference> refs;
  for (const ObjectID &id : {evicted, present, freed}) {
    rpc::ObjectReference ref;
    ref.set_object_id(id.Binary());
    *ref.mutable_owner_address() = owner_addr;
    refs.push_back(ref);
  }
  lease_dependency_manager_->StartOrUpdateWaitRequest(consumer, refs);
  ASSERT_EQ(fake_object_manager_->pull_calls.size(), 3u);
  for (const ObjectID &id : {evicted, present, freed}) {
    fake_object_manager_->MarkMovePull(id, consumer);
  }

  node_manager_->HandleObjectLocal(MakeObjectInfo(evicted, owner_addr));
  EXPECT_TRUE(local_object_manager_->pinned_objects.empty());
  EXPECT_EQ(fake_object_manager_->cancel_pull_calls.size(), 1u);
  EXPECT_EQ(fake_object_manager_->GetMovePullRequester(evicted), consumer);

  node_manager_->HandleObjectLocal(MakeObjectInfo(present, owner_addr));
  EXPECT_EQ(local_object_manager_->pinned_objects, std::vector<ObjectID>{present});
  EXPECT_EQ(fake_object_manager_->cancel_pull_calls.size(), 2u);
  EXPECT_FALSE(fake_object_manager_->IsMovePull(present));

  node_manager_->HandleObjectLocal(MakeObjectInfo(freed, owner_addr));
  EXPECT_EQ(local_object_manager_->pinned_objects, std::vector<ObjectID>{present});
  EXPECT_EQ(fake_object_manager_->cancel_pull_calls.size(), 3u);
  EXPECT_FALSE(fake_object_manager_->IsMovePull(freed));
}

/// Each moved arg of an actor task is handled once, before the wait that
/// fetches the args: skipped if this node holds its primary copy, pinned now if
/// a copy is here, recorded as a move otherwise or if the copy is gone, and
/// left alone if the copy here is being freed.
TEST_F(NodeManagerTest, TestHandleMovedActorCallArgs) {
  const rpc::Address owner_addr = MakeOwnerAddress();
  const WorkerID consumer = WorkerID::FromRandom();
  const ObjectID primary_here = ObjectID::FromRandom();
  const ObjectID copy_here = ObjectID::FromRandom();
  const ObjectID copy_gone = ObjectID::FromRandom();
  const ObjectID copy_freed = ObjectID::FromRandom();
  const ObjectID remote = ObjectID::FromRandom();
  const ObjectID not_moved = ObjectID::FromRandom();
  plasma::flatbuf::ObjectSource source = plasma::flatbuf::ObjectSource::CreatedByWorker;
  for (const ObjectID &id : {copy_here, copy_freed}) {
    RAY_UNUSED(fake_store_client_->TryCreateImmediately(
        id, owner_addr, 1024, nullptr, 1024, nullptr, source, 0));
  }
  objects_pending_deletion_->emplace(copy_freed);
  local_object_manager_->primary_copies.insert(primary_here);
  // The raylet believes all three copies are local; copy_gone no longer is.
  for (const ObjectID &id : {copy_here, copy_gone, copy_freed}) {
    RAY_UNUSED(lease_dependency_manager_->HandleObjectLocal(id));
  }

  std::vector<rpc::ObjectReference> refs;
  for (const ObjectID &id :
       {primary_here, copy_here, copy_gone, copy_freed, remote, remote, not_moved}) {
    rpc::ObjectReference ref;
    ref.set_object_id(id.Binary());
    *ref.mutable_owner_address() = owner_addr;
    refs.push_back(ref);
  }
  node_manager_->HandleMovedActorCallArgs(
      consumer, refs, {true, true, true, true, true, true, false});

  EXPECT_EQ(local_object_manager_->pinned_objects, std::vector<ObjectID>{copy_here});
  EXPECT_EQ(fake_object_directory_->primary_moved_objects,
            std::vector<ObjectID>{copy_here});
  EXPECT_FALSE(fake_object_manager_->IsMovePull(primary_here));
  EXPECT_FALSE(fake_object_manager_->IsMovePull(copy_here));
  EXPECT_FALSE(fake_object_manager_->IsMovePull(copy_freed));
  EXPECT_FALSE(fake_object_manager_->IsMovePull(not_moved));
  EXPECT_EQ(fake_object_manager_->GetMovePullRequester(copy_gone), consumer);
  EXPECT_EQ(fake_object_manager_->GetMovePullRequester(remote), consumer);
}

TEST_F(NodeManagerTest, TestConsumeSyncMessages) {
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

  node_manager_->ConsumeSyncMessages(syncer::MessageType::RESOURCE_VIEW,
                                     {std::make_shared<syncer::RaySyncMessage>(msg)});

  // Verify node resources and labels were updated.
  const auto &node_resources =
      cluster_resource_scheduler_->GetClusterResourceManager().GetNodeResources(
          scheduling::NodeID(node_id.Binary()));
  EXPECT_EQ(node_resources.labels.at("label1"), "value1");
  EXPECT_EQ(node_resources.total.Get(scheduling::ResourceID("CPU")).Double(),
            kTestTotalCpuResource);
  EXPECT_EQ(node_resources.GetAvailableSum(scheduling::ResourceID("CPU")).Double(),
            kTestTotalCpuResource);
}

// Tests that a batch of resource views is applied in full with one scheduling pass.
TEST_F(NodeManagerTest, TestConsumeSyncMessagesSchedulesOncePerBatch) {
  auto make_resource_view = [](const NodeID &node_id) {
    syncer::ResourceViewSyncMessage payload;
    payload.mutable_resources_total()->insert({"CPU", kTestTotalCpuResource});
    payload.mutable_resources_available()->insert({"CPU", kTestTotalCpuResource});
    payload.mutable_labels()->insert({"label1", "value1"});
    std::string serialized;
    RAY_CHECK(payload.SerializeToString(&serialized));
    auto msg = std::make_shared<syncer::RaySyncMessage>();
    msg->set_node_id(node_id.Binary());
    msg->set_message_type(syncer::MessageType::RESOURCE_VIEW);
    msg->set_version(1);
    msg->set_sync_message(serialized);
    return msg;
  };
  const std::vector<NodeID> node_ids = {
      NodeID::FromRandom(), NodeID::FromRandom(), NodeID::FromRandom()};

  const int passes_before = cluster_lease_manager_->num_schedule_and_grant_leases;
  node_manager_->ConsumeSyncMessages(syncer::MessageType::RESOURCE_VIEW,
                                     {make_resource_view(node_ids[0]),
                                      make_resource_view(node_ids[1]),
                                      make_resource_view(node_ids[2])});

  for (const auto &node_id : node_ids) {
    const auto &cluster_resource_manager =
        cluster_resource_scheduler_->GetClusterResourceManager();
    ASSERT_TRUE(cluster_resource_manager.HasNode(scheduling::NodeID(node_id.Binary())));
    const auto &node_resources =
        cluster_resource_manager.GetNodeResources(scheduling::NodeID(node_id.Binary()));
    EXPECT_EQ(node_resources.total.Get(scheduling::ResourceID("CPU")).Double(),
              kTestTotalCpuResource);
    EXPECT_EQ(node_resources.GetAvailableSum(scheduling::ResourceID("CPU")).Double(),
              kTestTotalCpuResource);
    EXPECT_EQ(node_resources.labels.at("label1"), "value1");
  }
  EXPECT_EQ(passes_before + 1, cluster_lease_manager_->num_schedule_and_grant_leases);
}

class NodeManagerDeadResourceSyncTest : public NodeManagerTest,
                                        public ::testing::WithParamInterface<bool> {};

TEST_P(NodeManagerDeadResourceSyncTest, IgnoreResourceViewsAfterNodeDeath) {
  node_manager_->RegisterGcs();
  auto &on_node_change =
      fake_gcs_client_->fake_node_accessor->node_address_and_liveness_subscribe;
  ASSERT_TRUE(on_node_change);
  EXPECT_EQ(fake_gcs_client_->fake_node_accessor
                ->async_subscribe_to_node_address_and_liveness_change_call_count,
            1);
  EXPECT_EQ(fake_gcs_client_->fake_worker_accessor
                ->async_subscribe_to_worker_failures_call_count,
            1);
  EXPECT_EQ(fake_gcs_client_->fake_job_accessor->async_subscribe_all_call_count, 1);

  auto &resources = cluster_resource_scheduler_->GetClusterResourceManager();
  const auto initial_node_count = resources.NumNodes();
  const auto node_id = NodeID::FromRandom();
  const scheduling::NodeID scheduling_id(node_id.Binary());
  syncer::ResourceViewSyncMessage payload;
  (*payload.mutable_resources_total())["CPU"] = kTestTotalCpuResource;
  (*payload.mutable_resources_available())["CPU"] = kTestTotalCpuResource;
  (*payload.mutable_resources_total())["GPU"] = 1;
  (*payload.mutable_resources_available())["GPU"] = 1;
  (*payload.mutable_labels())["ray.io/accelerator-type"] = "H20";
  auto msg = std::make_shared<syncer::RaySyncMessage>();
  msg->set_node_id(node_id.Binary());
  msg->set_message_type(syncer::MessageType::RESOURCE_VIEW);
  ASSERT_TRUE(payload.SerializeToString(msg->mutable_sync_message()));

  // Cover both an existing resource view and DEAD arriving before the first view.
  if (GetParam()) {
    node_manager_->ConsumeSyncMessages(syncer::MessageType::RESOURCE_VIEW, {msg});
    ASSERT_EQ(resources.NumNodes(), initial_node_count + 1);
  }
  rpc::GcsNodeAddressAndLiveness dead_node;
  dead_node.set_node_id(node_id.Binary());
  dead_node.set_state(GcsNodeInfo::DEAD);
  on_node_change(node_id, dead_node);
  ASSERT_EQ(fake_object_directory_->handle_node_removed_calls.size(), 1);
  EXPECT_EQ(fake_object_directory_->handle_node_removed_calls.front(), node_id);
  ASSERT_EQ(fake_object_manager_->handle_node_removed_calls.size(), 1);
  EXPECT_EQ(fake_object_manager_->handle_node_removed_calls.front(), node_id);
  ASSERT_EQ(resources.NumNodes(), initial_node_count);

  for (int64_t version = 1; version <= 3; ++version) {
    msg->set_version(version);
    node_manager_->ConsumeSyncMessages(syncer::MessageType::RESOURCE_VIEW, {msg});
    EXPECT_EQ(resources.NumNodes(), initial_node_count);
    EXPECT_FALSE(resources.GetResourceView().contains(scheduling_id));
  }

  // A restarted/new node has a new NodeID. Even without an ALIVE notification,
  // its resource view must still be accepted (unlike the old dead NodeID).
  const auto new_node_id = NodeID::FromRandom();
  msg->set_node_id(new_node_id.Binary());
  node_manager_->ConsumeSyncMessages(syncer::MessageType::RESOURCE_VIEW, {msg});
  EXPECT_EQ(resources.NumNodes(), initial_node_count + 1);
  const scheduling::NodeID new_scheduling_id(new_node_id.Binary());
  ASSERT_TRUE(resources.GetResourceView().contains(new_scheduling_id));
  const auto &node_resources = resources.GetNodeResources(new_scheduling_id);
  EXPECT_EQ(node_resources.GetAvailableSum(scheduling::ResourceID("GPU")).Double(), 1);
  EXPECT_EQ(node_resources.labels.at("ray.io/accelerator-type"), "H20");
}

INSTANTIATE_TEST_SUITE_P(ResourceViewOrdering,
                         NodeManagerDeadResourceSyncTest,
                         ::testing::Bool());

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
  std::shared_ptr<MockWorker> worker =
      std::make_shared<MockWorker>(WorkerID::FromRandom(), 10, clock_);
  // A worker in leased_workers_ always carries its granted lease (see
  // LocalLeaseManager::Grant), and HandleReturnWorkerLease checks for it.
  rpc::LeaseSpec lease_spec_msg;
  lease_spec_msg.set_lease_id(lease_id.Binary());
  lease_spec_msg.set_type(rpc::TaskType::NORMAL_TASK);
  worker->GrantLease(RayLease(std::move(lease_spec_msg)));
  leased_workers_[lease_id] = worker;
  rpc::ReturnWorkerLeaseRequest request;
  rpc::ReturnWorkerLeaseReply reply1;
  rpc::ReturnWorkerLeaseReply reply2;
  request.set_lease_id(lease_id.Binary());
  request.set_disconnect_worker(disconnect_worker);
  request.set_disconnect_worker_error_detail("test");
  request.set_worker_exiting(worker_exiting);

  // When disconnect_worker is set, the by-connection worker/driver lookups return
  // nullptr (the fake's defaults), simulating an already-disconnected worker.
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
  auto worker = std::make_shared<MockWorker>(WorkerID::FromRandom(), 10, clock_);
  PopWorkerCallback pop_worker_callback;
  fake_worker_pool_.pop_worker_hook = [&](const LeaseSpecification &ls,
                                          const PopWorkerCallback &callback) {
    pop_worker_callback = callback;
  };
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
  // The second (idempotent) request must not pop a second worker.
  EXPECT_EQ(fake_worker_pool_.pop_worker_calls, 1);
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
  RAY_UNUSED(fake_store_client_->TryCreateImmediately(
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

  // The fake object manager returns pull request id 1 for the first Pull (its default
  // next_pull_request_id) and records the call.
  auto worker = std::make_shared<MockWorker>(WorkerID::FromRandom(), 10, clock_);
  PopWorkerCallback pop_worker_callback;
  fake_worker_pool_.pop_worker_hook = [&](const LeaseSpecification &ls,
                                          const PopWorkerCallback &callback) {
    pop_worker_callback = callback;
  };
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
  // The dependency is pulled exactly once and only one worker is popped across the two
  // idempotent requests.
  EXPECT_EQ(fake_object_manager_->pull_calls.size(), 1u);
  EXPECT_EQ(fake_worker_pool_.pop_worker_calls, 1);
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

TEST_F(NodeManagerTest, TestHandleCancelWorkerLeaseWhenHasLeaseRequest) {
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

  // A RequestWorkerLease that arrives after cancellation (message reordering)
  // is rejected via the tombstone and must not re-queue the lease.
  rpc::RequestWorkerLeaseReply reorder_reply;
  node_manager_->HandleRequestWorkerLease(
      request_worker_lease_request,
      &reorder_reply,
      [](Status s, std::function<void()> success, std::function<void()> failure) {
        ASSERT_TRUE(s.ok());
      });
  ASSERT_TRUE(reorder_reply.canceled());
  ASSERT_EQ(reorder_reply.failure_type(),
            rpc::RequestWorkerLeaseReply::SCHEDULING_CANCELLED_INTENDED);
  ASSERT_EQ(GetPendingLeaseWorkerCount(*local_lease_manager_), 0);
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

  // A RequestWorkerLease for a lease cancelled before it was ever seen must
  // be rejected and must not queue.
  auto lease_spec = BuildLeaseSpec({});
  lease_spec.GetMutableMessage().set_lease_id(lease_id.Binary());
  rpc::RequestWorkerLeaseRequest request_worker_lease_request;
  request_worker_lease_request.mutable_lease_spec()->CopyFrom(lease_spec.GetMessage());
  request_worker_lease_request.set_backlog_size(1);
  request_worker_lease_request.set_grant_or_reject(true);
  request_worker_lease_request.set_is_selected_based_on_locality(true);
  rpc::RequestWorkerLeaseReply request_worker_lease_reply;
  node_manager_->HandleRequestWorkerLease(
      request_worker_lease_request,
      &request_worker_lease_reply,
      [](Status s, std::function<void()> success, std::function<void()> failure) {
        ASSERT_TRUE(s.ok());
      });
  ASSERT_TRUE(request_worker_lease_reply.canceled());
  ASSERT_EQ(request_worker_lease_reply.failure_type(),
            rpc::RequestWorkerLeaseReply::SCHEDULING_CANCELLED_INTENDED);
  ASSERT_EQ(GetPendingLeaseWorkerCount(*local_lease_manager_), 0);
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
    RAY_UNUSED(fake_store_client_->TryCreateImmediately(
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
    worker = std::make_shared<raylet::MockWorker>(worker_id, 10, clock_);
    worker->Connect(fake_rpc_client);
    fake_worker_pool_.registered_worker_by_id = worker;
  }
  // When worker_is_alive is false, the worker lookup returns nullptr (the fake's
  // default).

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

  node_manager_->RegisterGcs();

  // The node-change subscribe callback is captured by the fake node accessor.
  auto &publish_node_change_callback =
      fake_gcs_client_->fake_node_accessor->node_address_and_liveness_subscribe;
  ASSERT_TRUE(publish_node_change_callback);

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

TEST_F(NodeManagerTest, TestHandleDrainRayletRejectsIdleDrainWithPinnedObject) {
  // Tests that a node that holds a primary copy rejects a drain.
  local_object_manager_->primary_bytes_ = 1;

  rpc::DrainRayletRequest request;
  request.set_reason(
      rpc::autoscaler::DrainNodeReason::DRAIN_NODE_REASON_IDLE_TERMINATION);
  request.set_reason_message("idle for long enough");
  request.set_deadline_timestamp_ms(std::numeric_limits<int64_t>::max());

  rpc::DrainRayletReply reply;
  node_manager_->HandleDrainRaylet(
      request, &reply, [](Status s, std::function<void()>, std::function<void()>) {
        ASSERT_TRUE(s.ok());
      });
  ASSERT_FALSE(reply.is_accepted());
  ASSERT_FALSE(
      cluster_resource_scheduler_->GetLocalResourceManager().IsLocalNodeDraining());

  // With no pinned primary copies, the same idle drain is accepted.
  local_object_manager_->primary_bytes_ = 0;
  rpc::DrainRayletReply reply2;
  node_manager_->HandleDrainRaylet(
      request, &reply2, [](Status s, std::function<void()>, std::function<void()>) {
        ASSERT_TRUE(s.ok());
      });
  ASSERT_TRUE(reply2.is_accepted());
  ASSERT_TRUE(
      cluster_resource_scheduler_->GetLocalResourceManager().IsLocalNodeDraining());
}

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
    cluster_resource_scheduler_->GetLocalResourceManager().MarkFootprintAsBusy(
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
  rpc::LeaseSpec lease_spec_msg;
  lease_spec_msg.set_lease_id(lease_id.Binary());
  lease_spec_msg.set_type(rpc::TaskType::NORMAL_TASK);
  worker->GrantLease(RayLease(std::move(lease_spec_msg)));
  worker->GrantLeaseId(lease_id);
  leased_workers_.emplace(lease_id, worker);

  rpc::ReleaseUnusedBundlesRequest request;
  if (bundle_in_use) {
    auto *bundle_entry = request.add_bundles_in_use();
    bundle_entry->mutable_bundle_id()->set_placement_group_id(group_id.Binary());
    bundle_entry->mutable_bundle_id()->set_bundle_index(1);
  } else {
    // When the bundle is not in use, the worker associated with that bundle is destroyed
    // so the by-connection worker lookup returns the worker.
    fake_worker_pool_.registered_worker_by_connection = worker;
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

// Regression: HandleReturnWorkerLease with worker_exiting=true must release
// pinned lease args without waiting on DisconnectClient.
TEST_F(NodeManagerTest, TestExitingWorkerReleasesPinnedLeaseArgs) {
  auto [lease_id, worker] = GrantLeaseWithPinnedArg(/*arg_bytes=*/1024);
  ASSERT_GT(local_lease_manager_->GetPinnedLeaseArgumentsBytes(), 0);
  ASSERT_EQ(leased_workers_.size(), 1u);

  rpc::ReturnWorkerLeaseRequest request;
  rpc::ReturnWorkerLeaseReply reply;
  request.set_lease_id(lease_id.Binary());
  request.set_worker_exiting(true);
  request.set_disconnect_worker(false);
  node_manager_->HandleReturnWorkerLease(
      request, &reply, [](Status s, std::function<void()>, std::function<void()>) {
        ASSERT_TRUE(s.ok());
      });

  EXPECT_EQ(leased_workers_.size(), 0u);
  EXPECT_EQ(local_lease_manager_->GetPinnedLeaseArgumentsBytes(), 0);
}

}  // namespace ray::raylet
