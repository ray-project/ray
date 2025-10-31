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

// clang-format off
#include "ray/raylet/scheduling/cluster_lease_manager.h"

#include <memory>
#include <string>
#include <list>
#include <utility>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "ray/common/id.h"
#include "ray/common/scheduling/resource_set.h"
#include "ray/common/scheduling/scheduling_ids.h"
#include "ray/common/lease/lease.h"
#include "ray/common/task/task_util.h"
#include "ray/common/test_utils.h"
#include "ray/raylet/local_lease_manager.h"
#include "ray/raylet/scheduling/cluster_resource_scheduler.h"
#include "ray/raylet/tests/util.h"
#include "mock/ray/gcs_client/gcs_client.h"
// clang-format on

namespace ray {

namespace raylet {

using ::testing::_;

class MockWorkerPool : public WorkerPoolInterface {
 public:
  MockWorkerPool() : num_pops(0) {}

  void PopWorker(const LeaseSpecification &lease_spec,
                 const PopWorkerCallback &callback) override {
    num_pops++;
    const int runtime_env_hash = lease_spec.GetRuntimeEnvHash();
    callbacks[runtime_env_hash].push_back(callback);
  }

  void PushWorker(const std::shared_ptr<WorkerInterface> &worker) override {
    workers.push_front(worker);
  }

  std::vector<std::shared_ptr<WorkerInterface>> GetAllRegisteredWorkers(
      bool filter_dead_workers, bool filter_io_workers) const override {
    RAY_CHECK(false) << "Not used.";
    return {};
  }

  bool IsWorkerAvailableForScheduling() const override {
    RAY_CHECK(false) << "Not used.";
    return false;
  }

  std::shared_ptr<WorkerInterface> GetRegisteredWorker(
      const WorkerID &worker_id) const override {
    RAY_CHECK(false) << "Not used.";
    return nullptr;
  };

  std::shared_ptr<WorkerInterface> GetRegisteredDriver(
      const WorkerID &worker_id) const override {
    RAY_CHECK(false) << "Not used.";
    return nullptr;
  }

  void TriggerCallbacksWithNotOKStatus(
      PopWorkerStatus status, const std::string &runtime_env_setup_error_msg = "") {
    RAY_CHECK(status != PopWorkerStatus::OK);
    for (const auto &pair : callbacks) {
      for (const auto &callback : pair.second) {
        // No lease should be dispatched.
        ASSERT_FALSE(
            callback(nullptr,
                     status,
                     /*runtime_env_setup_error_msg*/ runtime_env_setup_error_msg));
      }
    }
    callbacks.clear();
  }

  void TriggerCallbacks() {
    for (auto it = workers.begin(); it != workers.end();) {
      std::shared_ptr<WorkerInterface> worker = *it;
      auto runtime_env_hash = worker->GetRuntimeEnvHash();
      bool dispatched = false;
      auto cb_it = callbacks.find(runtime_env_hash);
      if (cb_it != callbacks.end()) {
        auto &list = cb_it->second;
        RAY_CHECK(!list.empty());
        for (auto list_it = list.begin(); list_it != list.end();) {
          auto &callback = *list_it;
          dispatched = callback(worker, PopWorkerStatus::OK, "");
          list_it = list.erase(list_it);
          if (dispatched) {
            break;
          }
        }
        if (list.empty()) {
          callbacks.erase(cb_it);
        }
        if (dispatched) {
          it = workers.erase(it);
          continue;
        }
      }
      it++;
    }
  }

  std::shared_ptr<WorkerInterface> GetRegisteredWorker(
      const std::shared_ptr<ClientConnection> &connection) const override {
    RAY_CHECK(false) << "Not used.";
    return nullptr;
  }

  std::shared_ptr<WorkerInterface> GetRegisteredDriver(
      const std::shared_ptr<ClientConnection> &connection) const override {
    RAY_CHECK(false) << "Not used.";
    return nullptr;
  }

  void HandleJobStarted(const JobID &job_id, const rpc::JobConfig &job_config) override {
    RAY_CHECK(false) << "Not used.";
  }

  void HandleJobFinished(const JobID &job_id) override {
    RAY_CHECK(false) << "Not used.";
  }

  void Start() override { RAY_CHECK(false) << "Not used."; }

  void SetNodeManagerPort(int node_manager_port) override {
    RAY_CHECK(false) << "Not used.";
  }

  void SetRuntimeEnvAgentClient(
      std::unique_ptr<RuntimeEnvAgentClient> runtime_env_agent_client) override {
    RAY_CHECK(false) << "Not used.";
  }

  std::vector<std::shared_ptr<WorkerInterface>> GetAllRegisteredDrivers(
      bool filter_dead_drivers, bool filter_system_drivers) const override {
    RAY_CHECK(false) << "Not used.";
    return {};
  }

  Status RegisterDriver(const std::shared_ptr<WorkerInterface> &worker,
                        const rpc::JobConfig &job_config,
                        std::function<void(Status, int)> send_reply_callback) override {
    RAY_CHECK(false) << "Not used.";
    return Status::Invalid("Not used.");
  }

  Status RegisterWorker(const std::shared_ptr<WorkerInterface> &worker,
                        pid_t pid,
                        StartupToken worker_startup_token,
                        std::function<void(Status, int)> send_reply_callback) override {
    RAY_CHECK(false) << "Not used.";
    return Status::Invalid("Not used.");
  }

  boost::optional<const rpc::JobConfig &> GetJobConfig(
      const JobID &job_id) const override {
    RAY_CHECK(false) << "Not used.";
    return boost::none;
  }

  void OnWorkerStarted(const std::shared_ptr<WorkerInterface> &worker) override {
    RAY_CHECK(false) << "Not used.";
  }

  void PushSpillWorker(const std::shared_ptr<WorkerInterface> &worker) override {
    RAY_CHECK(false) << "Not used.";
  }

  void PushRestoreWorker(const std::shared_ptr<WorkerInterface> &worker) override {
    RAY_CHECK(false) << "Not used.";
  }

  void DisconnectWorker(const std::shared_ptr<WorkerInterface> &worker,
                        rpc::WorkerExitType disconnect_type) override {
    RAY_CHECK(false) << "Not used.";
  }

  void DisconnectDriver(const std::shared_ptr<WorkerInterface> &driver) override {
    RAY_CHECK(false) << "Not used.";
  }

  void PrestartWorkers(const LeaseSpecification &lease_spec,
                       int64_t backlog_size) override {
    RAY_CHECK(false) << "Not used.";
  }

  void StartNewWorker(
      const std::shared_ptr<PopWorkerRequest> &pop_worker_request) override {
    RAY_CHECK(false) << "Not used.";
  }

  std::string DebugString() const override {
    RAY_CHECK(false) << "Not used.";
    return "";
  }

  void PopSpillWorker(
      std::function<void(std::shared_ptr<WorkerInterface>)> callback) override {
    RAY_CHECK(false) << "Not used.";
  }

  void PopRestoreWorker(
      std::function<void(std::shared_ptr<WorkerInterface>)> callback) override {
    RAY_CHECK(false) << "Not used.";
  }

  void PushDeleteWorker(const std::shared_ptr<WorkerInterface> &worker) override {
    RAY_CHECK(false) << "Not used.";
  }

  void PopDeleteWorker(
      std::function<void(std::shared_ptr<WorkerInterface>)> callback) override {
    RAY_CHECK(false) << "Not used.";
  }

  size_t CallbackSize(int runtime_env_hash) {
    auto cb_it = callbacks.find(runtime_env_hash);
    if (cb_it != callbacks.end()) {
      auto &list = cb_it->second;
      return list.size();
    }
    return 0;
  }

  std::list<std::shared_ptr<WorkerInterface>> workers;
  absl::flat_hash_map<int, std::list<PopWorkerCallback>> callbacks;
  int num_pops;
};

std::shared_ptr<ClusterResourceScheduler> CreateSingleNodeScheduler(
    const std::string &id, double num_cpus, double num_gpus, gcs::GcsClient &gcs_client) {
  absl::flat_hash_map<std::string, double> local_node_resources;
  local_node_resources[ray::kCPU_ResourceLabel] = num_cpus;
  local_node_resources[ray::kGPU_ResourceLabel] = num_gpus;
  local_node_resources[ray::kMemory_ResourceLabel] = 128;
  static instrumented_io_context io_context;
  auto scheduler = std::make_shared<ClusterResourceScheduler>(
      io_context,
      scheduling::NodeID(id),
      local_node_resources,
      /*is_node_available_fn*/ [&gcs_client](scheduling::NodeID node_id) {
        return gcs_client.Nodes().GetNodeAddressAndLiveness(
                   NodeID::FromBinary(node_id.Binary())) != nullptr;
      });

  return scheduler;
}

RayLease CreateLease(
    const std::unordered_map<std::string, double> &required_resources,
    int num_args = 0,
    std::vector<ObjectID> args = {},
    const std::shared_ptr<rpc::RuntimeEnvInfo> runtime_env_info = nullptr,
    rpc::SchedulingStrategy scheduling_strategy = rpc::SchedulingStrategy(),
    const LeaseID &lease_id = LeaseID::FromRandom()) {
  TaskSpecBuilder spec_builder;
  TaskID id = RandomTaskId();
  JobID job_id = RandomJobId();
  rpc::Address address;
  address.set_node_id(NodeID::FromRandom().Binary());
  address.set_worker_id(WorkerID::FromRandom().Binary());
  spec_builder.SetCommonTaskSpec(id,
                                 "dummy_task",
                                 Language::PYTHON,
                                 FunctionDescriptorBuilder::BuildPython("", "", "", ""),
                                 job_id,
                                 rpc::JobConfig(),
                                 TaskID::Nil(),
                                 0,
                                 TaskID::Nil(),
                                 address,
                                 0,
                                 /*returns_dynamic=*/false,
                                 /*is_streaming_generator*/ false,
                                 /*generator_backpressure_num_objects*/ -1,
                                 required_resources,
                                 {},
                                 "",
                                 0,
                                 TaskID::Nil(),
                                 "",
                                 runtime_env_info);

  if (!args.empty()) {
    for (auto &arg : args) {
      spec_builder.AddArg(TaskArgByReference(arg, rpc::Address(), ""));
    }
  } else {
    for (int i = 0; i < num_args; i++) {
      ObjectID put_id = ObjectID::FromIndex(RandomTaskId(), /*index=*/i + 1);
      spec_builder.AddArg(TaskArgByReference(put_id, rpc::Address(), ""));
    }
  }

  spec_builder.SetNormalTaskSpec(0, false, "", scheduling_strategy, ActorID::Nil());
  TaskSpecification spec = std::move(spec_builder).ConsumeAndBuild();
  LeaseSpecification lease_spec(spec.GetMessage());
  lease_spec.GetMutableMessage().set_lease_id(lease_id.Binary());
  return RayLease(std::move(lease_spec));
}

class MockLeaseDependencyManager : public LeaseDependencyManagerInterface {
 public:
  explicit MockLeaseDependencyManager(std::unordered_set<ObjectID> &missing_objects)
      : missing_objects_(missing_objects) {}

  bool RequestLeaseDependencies(const LeaseID &lease_id,
                                const std::vector<rpc::ObjectReference> &required_objects,
                                const TaskMetricsKey &task_key) {
    RAY_CHECK(subscribed_leases.insert(lease_id).second);
    for (auto &obj_ref : required_objects) {
      if (missing_objects_.find(ObjectRefToId(obj_ref)) != missing_objects_.end()) {
        return false;
      }
    }
    return true;
  }

  void RemoveLeaseDependencies(const LeaseID &lease_id) {
    RAY_CHECK(subscribed_leases.erase(lease_id));
  }

  bool LeaseDependenciesBlocked(const LeaseID &lease_id) const {
    return blocked_leases.count(lease_id);
  }

  bool CheckObjectLocal(const ObjectID &object_id) const { return true; }

  std::unordered_set<ObjectID> &missing_objects_;
  std::unordered_set<LeaseID> subscribed_leases;
  std::unordered_set<LeaseID> blocked_leases;
};

class FeatureFlagEnvironment : public ::testing::Environment {
  /// We should run these tests with feature flags on to ensure we are testing the flagged
  /// behavior.
 public:
  ~FeatureFlagEnvironment() override {}

  // Override this to define how to set up the environment.
  void SetUp() override { RayConfig::instance().worker_cap_enabled() = true; }

  // Override this to define how to tear down the environment.
  void TearDown() override {}
};

testing::Environment *const env =
    ::testing::AddGlobalTestEnvironment(new FeatureFlagEnvironment);

class ClusterLeaseManagerTest : public ::testing::Test {
 public:
  explicit ClusterLeaseManagerTest(double num_cpus_at_head = 8.0,
                                   double num_gpus_at_head = 0.0)
      : gcs_client_(std::make_unique<gcs::MockGcsClient>()),
        id_(NodeID::FromRandom()),
        scheduler_(CreateSingleNodeScheduler(
            id_.Binary(), num_cpus_at_head, num_gpus_at_head, *gcs_client_)),
        lease_dependency_manager_(missing_objects_),
        local_lease_manager_(std::make_unique<LocalLeaseManager>(
            id_,
            *scheduler_,
            lease_dependency_manager_,
            /* get_node_info= */
            [this](
                const NodeID &node_id) -> std::optional<rpc::GcsNodeAddressAndLiveness> {
              node_info_calls_++;
              if (node_info_.count(node_id) != 0) {
                return std::optional((node_info_[node_id]));
              }
              return std::nullopt;
            },
            pool_,
            leased_workers_,
            /* get_lease_args= */
            [this](const std::vector<ObjectID> &object_ids,
                   std::vector<std::unique_ptr<RayObject>> *results) {
              for (auto &obj_id : object_ids) {
                if (missing_objects_.count(obj_id) == 0) {
                  results->emplace_back(MakeDummyArg());
                } else {
                  results->emplace_back(nullptr);
                }
              }
              return true;
            },
            /*max_pinned_lease_args_bytes=*/1000,
            /*get_time=*/[this]() { return current_time_ms_; })),
        lease_manager_(
            id_,
            *scheduler_,
            /* get_node_info= */
            [this](
                const NodeID &node_id) -> std::optional<rpc::GcsNodeAddressAndLiveness> {
              node_info_calls_++;
              if (node_info_.count(node_id) != 0) {
                return std::optional((node_info_[node_id]));
              }
              return std::nullopt;
            },
            /* announce_infeasible_lease= */
            [this](const RayLease &lease) { announce_infeasible_lease_calls_++; },
            *local_lease_manager_,
            /*get_time=*/[this]() { return current_time_ms_; }) {
    RayConfig::instance().initialize("{\"scheduler_top_k_absolute\": 1}");
  }

  void SetUp() {
    static rpc::GcsNodeAddressAndLiveness node_info;
    ON_CALL(*gcs_client_->mock_node_accessor,
            GetNodeAddressAndLiveness(::testing::_, ::testing::_))
        .WillByDefault(::testing::Return(&node_info));
  }

  RayObject *MakeDummyArg() {
    std::vector<uint8_t> data;
    data.resize(default_arg_size_);
    auto buffer = std::make_shared<LocalMemoryBuffer>(data.data(), data.size());
    return new RayObject(buffer, nullptr, {});
  }

  void Shutdown() {}

  void AddNode(const NodeID &id,
               double num_cpus,
               double num_gpus = 0,
               double memory = 0) {
    absl::flat_hash_map<std::string, double> node_resources;
    node_resources[ray::kCPU_ResourceLabel] = num_cpus;
    node_resources[ray::kGPU_ResourceLabel] = num_gpus;
    node_resources[ray::kMemory_ResourceLabel] = memory;
    scheduler_->GetClusterResourceManager().AddOrUpdateNode(
        scheduling::NodeID(id.Binary()), node_resources, node_resources);

    rpc::GcsNodeAddressAndLiveness info;
    node_info_[id] = info;
  }

  void AssertNoLeaks() {
    ASSERT_TRUE(lease_manager_.leases_to_schedule_.empty());
    ASSERT_TRUE(local_lease_manager_->leases_to_grant_.empty());
    ASSERT_TRUE(local_lease_manager_->waiting_leases_index_.empty());
    ASSERT_TRUE(local_lease_manager_->waiting_lease_queue_.empty());
    ASSERT_TRUE(lease_manager_.infeasible_leases_.empty());
    ASSERT_TRUE(local_lease_manager_->granted_lease_args_.empty());
    ASSERT_TRUE(local_lease_manager_->pinned_lease_arguments_.empty());
    ASSERT_TRUE(local_lease_manager_->info_by_sched_cls_.empty());
    ASSERT_EQ(local_lease_manager_->pinned_lease_arguments_bytes_, 0);
    ASSERT_TRUE(lease_dependency_manager_.subscribed_leases.empty());
  }

  void AssertPinnedLeaseArgumentsPresent(const RayLease &lease) {
    const auto &expected_deps = lease.GetLeaseSpecification().GetDependencyIds();
    ASSERT_EQ(local_lease_manager_
                  ->granted_lease_args_[lease.GetLeaseSpecification().LeaseId()],
              expected_deps);
    for (auto &arg : expected_deps) {
      ASSERT_TRUE(local_lease_manager_->pinned_lease_arguments_.count(arg));
    }
  }

  int NumLeasesToDispatchWithStatus(internal::WorkStatus status) {
    int count = 0;
    for (const auto &pair : local_lease_manager_->leases_to_grant_) {
      for (const auto &work : pair.second) {
        if (work->GetState() == status) {
          count++;
        }
      }
    }
    return count;
  }

  int NumRunningLeases() {
    int count = 0;
    for (const auto &pair : local_lease_manager_->info_by_sched_cls_) {
      count += (pair.second.granted_leases.size());
    }

    return count;
  }

  std::unique_ptr<gcs::MockGcsClient> gcs_client_;
  NodeID id_;
  std::shared_ptr<ClusterResourceScheduler> scheduler_;
  MockWorkerPool pool_;
  absl::flat_hash_map<LeaseID, std::shared_ptr<WorkerInterface>> leased_workers_;
  std::unordered_set<ObjectID> missing_objects_;

  int default_arg_size_ = 10;

  int node_info_calls_ = 0;
  int announce_infeasible_lease_calls_ = 0;
  absl::flat_hash_map<NodeID, rpc::GcsNodeAddressAndLiveness> node_info_;
  int64_t current_time_ms_ = 0;

  MockLeaseDependencyManager lease_dependency_manager_;
  std::unique_ptr<LocalLeaseManager> local_lease_manager_;
  ClusterLeaseManager lease_manager_;
};

// Same as ClusterLeaseManagerTest, but the head node starts with 4.0 num gpus.
class ClusterLeaseManagerTestWithGPUsAtHead : public ClusterLeaseManagerTest {
 public:
  ClusterLeaseManagerTestWithGPUsAtHead()
      : ClusterLeaseManagerTest(/*num_cpus_at_head=*/8.0, /*num_gpus_at_head=*/4.0) {}
};

// Same as ClusterLeaseManagerTest, but the head node starts with 0.0 num cpus.
class ClusterLeaseManagerTestWithoutCPUsAtHead : public ClusterLeaseManagerTest {
 public:
  ClusterLeaseManagerTestWithoutCPUsAtHead()
      : ClusterLeaseManagerTest(/*num_cpus_at_head=*/0.0) {}
};

TEST_F(ClusterLeaseManagerTest, BasicTest) {
  /*
    Test basic scheduler functionality:
    1. Queue and attempt to schedule/dispatch atest with no workers available
    2. A worker becomes available, dispatch again.
   */
  RayLease lease = CreateLease({{ray::kCPU_ResourceLabel, 4}});
  rpc::RequestWorkerLeaseReply reply;
  bool callback_occurred = false;
  bool *callback_occurred_ptr = &callback_occurred;
  auto callback = [callback_occurred_ptr](
                      Status, std::function<void()>, std::function<void()>) {
    *callback_occurred_ptr = true;
  };

  lease_manager_.QueueAndScheduleLease(
      lease,
      false,
      false,
      std::vector<internal::ReplyCallback>{internal::ReplyCallback(callback, &reply)});
  pool_.TriggerCallbacks();
  ASSERT_FALSE(callback_occurred);
  ASSERT_EQ(leased_workers_.size(), 0);
  ASSERT_EQ(pool_.workers.size(), 0);

  std::shared_ptr<MockWorker> worker =
      std::make_shared<MockWorker>(WorkerID::FromRandom(), 1234);
  pool_.PushWorker(std::static_pointer_cast<WorkerInterface>(worker));
  pool_.TriggerCallbacks();

  ASSERT_TRUE(callback_occurred);
  ASSERT_EQ(leased_workers_.size(), 1);
  ASSERT_EQ(pool_.workers.size(), 0);
  ASSERT_EQ(node_info_calls_, 0);

  RayLease finished_lease;
  local_lease_manager_->CleanupLease(leased_workers_.begin()->second, &finished_lease);
  ASSERT_EQ(finished_lease.GetLeaseSpecification().LeaseId(),
            lease.GetLeaseSpecification().LeaseId());
  AssertNoLeaks();
}

TEST_F(ClusterLeaseManagerTest, IdempotencyTest) {
  /*
    A few lease manager methods are meant to be idempotent.
    * `CleanupLease`
    * `ReleaseCpuResourcesFromBlockedWorker`
    * `ReturnCpuResourcesToUnblockedWorker`
   */
  RayLease lease = CreateLease({{ray::kCPU_ResourceLabel, 4}});
  rpc::RequestWorkerLeaseReply reply;
  bool callback_occurred = false;
  bool *callback_occurred_ptr = &callback_occurred;
  auto callback = [callback_occurred_ptr](
                      Status, std::function<void()>, std::function<void()>) {
    *callback_occurred_ptr = true;
  };

  lease_manager_.QueueAndScheduleLease(
      lease,
      false,
      false,
      std::vector<internal::ReplyCallback>{internal::ReplyCallback(callback, &reply)});
  pool_.TriggerCallbacks();
  ASSERT_FALSE(callback_occurred);
  ASSERT_EQ(leased_workers_.size(), 0);
  ASSERT_EQ(pool_.workers.size(), 0);

  std::shared_ptr<MockWorker> worker =
      std::make_shared<MockWorker>(WorkerID::FromRandom(), 1234);
  pool_.PushWorker(std::static_pointer_cast<WorkerInterface>(worker));
  pool_.TriggerCallbacks();

  ASSERT_TRUE(callback_occurred);
  ASSERT_EQ(leased_workers_.size(), 1);
  ASSERT_EQ(pool_.workers.size(), 0);
  ASSERT_EQ(node_info_calls_, 0);

  ASSERT_EQ(scheduler_->GetLocalResourceManager().GetLocalAvailableCpus(), 4.0);

  local_lease_manager_->ReleaseCpuResourcesFromBlockedWorker(worker);
  local_lease_manager_->ReleaseCpuResourcesFromBlockedWorker(worker);

  ASSERT_EQ(scheduler_->GetLocalResourceManager().GetLocalAvailableCpus(), 8.0);

  local_lease_manager_->ReturnCpuResourcesToUnblockedWorker(worker);
  local_lease_manager_->ReturnCpuResourcesToUnblockedWorker(worker);

  ASSERT_EQ(scheduler_->GetLocalResourceManager().GetLocalAvailableCpus(), 4.0);

  RayLease finished_lease;
  local_lease_manager_->CleanupLease(leased_workers_.begin()->second, &finished_lease);
  local_lease_manager_->CleanupLease(leased_workers_.begin()->second, &finished_lease);
  ASSERT_EQ(finished_lease.GetLeaseSpecification().LeaseId(),
            lease.GetLeaseSpecification().LeaseId());
  ASSERT_EQ(scheduler_->GetLocalResourceManager().GetLocalAvailableCpus(), 8.0);
  AssertNoLeaks();
}

TEST_F(ClusterLeaseManagerTest, GrantQueueNonBlockingTest) {
  /*
    Test that if no worker is available for the first lease in a leases to grant
    queue (because the runtime env in the lease spec doesn't match any
    available worker), other leases in the grant queue can still be scheduled.
    https://github.com/ray-project/ray/issues/16226
   */

  // Use the same required_resources for all tasks so they end up in the same queue.
  const std::unordered_map<std::string, double> required_resources = {
      {ray::kCPU_ResourceLabel, 4}};

  std::string serialized_runtime_env_A = "mock_env_A";
  std::shared_ptr<rpc::RuntimeEnvInfo> runtime_env_info_A = nullptr;
  runtime_env_info_A.reset(new rpc::RuntimeEnvInfo());
  runtime_env_info_A->set_serialized_runtime_env(serialized_runtime_env_A);

  RayLease lease_A =
      CreateLease(required_resources, /*num_args=*/0, /*args=*/{}, runtime_env_info_A);
  rpc::RequestWorkerLeaseReply reply_A;
  bool callback_occurred = false;
  bool *callback_occurred_ptr = &callback_occurred;
  auto callback = [callback_occurred_ptr](
                      Status, std::function<void()>, std::function<void()>) {
    *callback_occurred_ptr = true;
  };

  std::string serialized_runtime_env_B = "mock_env_B";
  std::shared_ptr<rpc::RuntimeEnvInfo> runtime_env_info_B = nullptr;
  runtime_env_info_B.reset(new rpc::RuntimeEnvInfo());
  runtime_env_info_B->set_serialized_runtime_env(serialized_runtime_env_B);

  RayLease lease_B_1 =
      CreateLease(required_resources, /*num_args=*/0, /*args=*/{}, runtime_env_info_B);
  RayLease lease_B_2 =
      CreateLease(required_resources, /*num_args=*/0, /*args=*/{}, runtime_env_info_B);
  rpc::RequestWorkerLeaseReply reply_B_1;
  rpc::RequestWorkerLeaseReply reply_B_2;
  auto empty_callback = [](Status, std::function<void()>, std::function<void()>) {};

  // Ensure task_A is not at the front of the queue.
  lease_manager_.QueueAndScheduleLease(
      lease_B_1,
      false,
      false,
      std::vector<internal::ReplyCallback>{
          internal::ReplyCallback(empty_callback, &reply_B_1)});
  lease_manager_.QueueAndScheduleLease(
      lease_A,
      false,
      false,
      std::vector<internal::ReplyCallback>{internal::ReplyCallback(callback, &reply_A)});
  lease_manager_.QueueAndScheduleLease(
      lease_B_2,
      false,
      false,
      std::vector<internal::ReplyCallback>{
          internal::ReplyCallback(empty_callback, &reply_B_2)});
  pool_.TriggerCallbacks();

  // Push a worker that can only run task A.
  std::shared_ptr<MockWorker> worker_A = std::make_shared<MockWorker>(
      WorkerID::FromRandom(), 1234, CalculateRuntimeEnvHash(serialized_runtime_env_A));
  pool_.PushWorker(std::static_pointer_cast<WorkerInterface>(worker_A));
  pool_.TriggerCallbacks();

  ASSERT_TRUE(callback_occurred);
  ASSERT_EQ(leased_workers_.size(), 1);
  ASSERT_EQ(pool_.workers.size(), 0);
  ASSERT_EQ(node_info_calls_, 0);

  RayLease finished_lease;
  local_lease_manager_->CleanupLease(leased_workers_.begin()->second, &finished_lease);
  ASSERT_EQ(finished_lease.GetLeaseSpecification().LeaseId(),
            lease_A.GetLeaseSpecification().LeaseId());

  // task_B_1 and task_B_2 remain in the dispatch queue, so don't call AssertNoLeaks().
  // AssertNoLeaks();
}

TEST_F(ClusterLeaseManagerTest, BlockedWorkerDiesTest) {
  /*
   Tests the edge case in which a worker crashes while it's blocked. In this case, its CPU
   resources should not be double freed.
   */

  // Add PG CPU and GPU resources.
  scheduler_->GetLocalResourceManager().AddLocalResourceInstances(
      scheduling::ResourceID("CPU_group_aaa"), std::vector<FixedPoint>{FixedPoint(1)});
  scheduler_->GetLocalResourceManager().AddLocalResourceInstances(
      scheduling::ResourceID("CPU_group_0_aaa"), std::vector<FixedPoint>{FixedPoint(1)});

  WorkerID worker_id1 = WorkerID::FromRandom();
  WorkerID worker_id2 = WorkerID::FromRandom();
  LeaseID lease_id1 = LeaseID::FromWorker(worker_id1, 1);
  LeaseID lease_id2 = LeaseID::FromWorker(worker_id2, 1);
  RayLease lease1 = CreateLease({{ray::kCPU_ResourceLabel, 4}},
                                0,
                                {},
                                nullptr,
                                rpc::SchedulingStrategy(),
                                lease_id1);
  rpc::RequestWorkerLeaseReply reply1;
  RayLease lease2 = CreateLease({{"CPU_group_aaa", 1}, {"CPU_group_0_aaa", 1}},
                                0,
                                {},
                                nullptr,
                                rpc::SchedulingStrategy(),
                                lease_id2);
  rpc::RequestWorkerLeaseReply reply2;

  bool callback_occurred = false;
  bool *callback_occurred_ptr = &callback_occurred;
  auto callback = [callback_occurred_ptr](
                      Status, std::function<void()>, std::function<void()>) {
    *callback_occurred_ptr = true;
  };

  lease_manager_.QueueAndScheduleLease(
      lease1,
      false,
      false,
      std::vector<internal::ReplyCallback>{internal::ReplyCallback(callback, &reply1)});
  pool_.TriggerCallbacks();

  ASSERT_FALSE(callback_occurred);
  ASSERT_EQ(leased_workers_.size(), 0);
  ASSERT_EQ(pool_.workers.size(), 0);

  std::shared_ptr<MockWorker> worker1 = std::make_shared<MockWorker>(worker_id1, 1234);
  std::shared_ptr<MockWorker> worker2 = std::make_shared<MockWorker>(worker_id2, 5678);
  pool_.PushWorker(std::static_pointer_cast<WorkerInterface>(worker1));

  lease_manager_.ScheduleAndGrantLeases();
  pool_.TriggerCallbacks();

  lease_manager_.QueueAndScheduleLease(
      lease2,
      false,
      false,
      std::vector<internal::ReplyCallback>{internal::ReplyCallback(callback, &reply2)});
  pool_.PushWorker(std::static_pointer_cast<WorkerInterface>(worker2));
  lease_manager_.ScheduleAndGrantLeases();
  pool_.TriggerCallbacks();

  ASSERT_TRUE(callback_occurred);
  ASSERT_EQ(leased_workers_.size(), 2);
  ASSERT_EQ(pool_.workers.size(), 0);
  ASSERT_EQ(node_info_calls_, 0);

  // Block the worker. Which releases only the CPU resource.
  local_lease_manager_->ReleaseCpuResourcesFromBlockedWorker(worker1);
  local_lease_manager_->ReleaseCpuResourcesFromBlockedWorker(worker2);

  RayLease finished_lease1;
  RayLease finished_lease2;
  // If a resource was double-freed, we will crash in this call.
  local_lease_manager_->CleanupLease(leased_workers_[lease_id1], &finished_lease1);
  local_lease_manager_->CleanupLease(leased_workers_[lease_id2], &finished_lease2);
  ASSERT_EQ(finished_lease1.GetLeaseSpecification().LeaseId(),
            lease1.GetLeaseSpecification().LeaseId());
  ASSERT_EQ(finished_lease2.GetLeaseSpecification().LeaseId(),
            lease2.GetLeaseSpecification().LeaseId());

  AssertNoLeaks();
}

TEST_F(ClusterLeaseManagerTest, BlockedWorkerDies2Test) {
  /*
    Same edge case as the previous test, but this time the block and finish requests
    happen in the opposite order.
   */
  RayLease lease = CreateLease({{ray::kCPU_ResourceLabel, 4}});
  rpc::RequestWorkerLeaseReply reply;
  bool callback_occurred = false;
  bool *callback_occurred_ptr = &callback_occurred;
  auto callback = [callback_occurred_ptr](
                      Status, std::function<void()>, std::function<void()>) {
    *callback_occurred_ptr = true;
  };

  lease_manager_.QueueAndScheduleLease(
      lease,
      false,
      false,
      std::vector<internal::ReplyCallback>{internal::ReplyCallback(callback, &reply)});
  pool_.TriggerCallbacks();

  ASSERT_FALSE(callback_occurred);
  ASSERT_EQ(leased_workers_.size(), 0);
  ASSERT_EQ(pool_.workers.size(), 0);

  std::shared_ptr<MockWorker> worker =
      std::make_shared<MockWorker>(WorkerID::FromRandom(), 1234);
  pool_.PushWorker(std::static_pointer_cast<WorkerInterface>(worker));

  lease_manager_.ScheduleAndGrantLeases();
  pool_.TriggerCallbacks();

  ASSERT_TRUE(callback_occurred);
  ASSERT_EQ(leased_workers_.size(), 1);
  ASSERT_EQ(pool_.workers.size(), 0);
  ASSERT_EQ(node_info_calls_, 0);

  RayLease finished_lease;
  local_lease_manager_->CleanupLease(leased_workers_.begin()->second, &finished_lease);
  ASSERT_EQ(finished_lease.GetLeaseSpecification().LeaseId(),
            lease.GetLeaseSpecification().LeaseId());

  // Block the worker. Which releases only the CPU resource.
  local_lease_manager_->ReleaseCpuResourcesFromBlockedWorker(worker);

  AssertNoLeaks();
}

TEST_F(ClusterLeaseManagerTest, NoFeasibleNodeTest) {
  std::shared_ptr<MockWorker> worker =
      std::make_shared<MockWorker>(WorkerID::FromRandom(), 1234);
  pool_.PushWorker(std::dynamic_pointer_cast<WorkerInterface>(worker));

  RayLease lease = CreateLease({{ray::kCPU_ResourceLabel, 999}});
  rpc::RequestWorkerLeaseReply reply;

  bool callback_called = false;
  bool *callback_called_ptr = &callback_called;
  auto callback = [callback_called_ptr](
                      Status, std::function<void()>, std::function<void()>) {
    *callback_called_ptr = true;
  };

  lease_manager_.QueueAndScheduleLease(
      lease,
      false,
      false,
      std::vector<internal::ReplyCallback>{internal::ReplyCallback(callback, &reply)});
  pool_.TriggerCallbacks();

  ASSERT_FALSE(callback_called);
  ASSERT_EQ(leased_workers_.size(), 0);
  // Worker is unused.
  ASSERT_EQ(pool_.workers.size(), 1);
  ASSERT_EQ(node_info_calls_, 0);
}

TEST_F(ClusterLeaseManagerTest, DrainingWhileResolving) {
  /*
    Test the race condition in which a lease is assigned to a node, but cannot
    run because its dependencies are unresolved. Once its dependencies are
    resolved, the node is being drained.
  */
  RayLease lease = CreateLease({{ray::kCPU_ResourceLabel, 1}});
  rpc::RequestWorkerLeaseReply reply;
  bool callback_occurred = false;
  bool *callback_occurred_ptr = &callback_occurred;
  auto callback = [callback_occurred_ptr](
                      Status, std::function<void()>, std::function<void()>) {
    *callback_occurred_ptr = true;
  };
  lease_manager_.QueueAndScheduleLease(
      lease,
      false,
      false,
      std::vector<internal::ReplyCallback>{internal::ReplyCallback(callback, &reply)});
  std::shared_ptr<MockWorker> worker =
      std::make_shared<MockWorker>(WorkerID::FromRandom(), 1234);
  std::shared_ptr<MockWorker> worker2 =
      std::make_shared<MockWorker>(WorkerID::FromRandom(), 12345);
  pool_.PushWorker(std::static_pointer_cast<WorkerInterface>(worker));
  pool_.PushWorker(std::static_pointer_cast<WorkerInterface>(worker2));
  pool_.TriggerCallbacks();
  ASSERT_TRUE(callback_occurred);
  ASSERT_EQ(leased_workers_.size(), 1);
  ASSERT_EQ(pool_.workers.size(), 1);

  auto remote_node_id = NodeID::FromRandom();
  AddNode(remote_node_id, 5);

  RayLease resolving_args_lease = CreateLease({{ray::kCPU_ResourceLabel, 1}}, 1);
  auto missing_arg = resolving_args_lease.GetLeaseSpecification().GetDependencyIds()[0];
  missing_objects_.insert(missing_arg);
  rpc::RequestWorkerLeaseReply spillback_reply;
  lease_manager_.QueueAndScheduleLease(
      resolving_args_lease,
      false,
      false,
      std::vector<internal::ReplyCallback>{
          internal::ReplyCallback(callback, &spillback_reply)});
  pool_.TriggerCallbacks();
  ASSERT_EQ(leased_workers_.size(), 1);
  ASSERT_EQ(pool_.workers.size(), 1);

  // Drain the local node.
  rpc::DrainRayletRequest drain_request;
  drain_request.set_deadline_timestamp_ms(std::numeric_limits<int64_t>::max());
  scheduler_->GetLocalResourceManager().SetLocalNodeDraining(drain_request);

  // Arg is resolved.
  missing_objects_.erase(missing_arg);
  std::vector<LeaseID> unblocked = {
      resolving_args_lease.GetLeaseSpecification().LeaseId()};
  local_lease_manager_->LeasesUnblocked(unblocked);
  ASSERT_EQ(spillback_reply.retry_at_raylet_address().node_id(), remote_node_id.Binary());
}

TEST_F(ClusterLeaseManagerTest, ResourceTakenWhileResolving) {
  /*
    Test the race condition in which a lease is assigned to a node, but cannot
    run because its dependencies are unresolved. Once its dependencies are
    resolved, the node no longer has available resources.
  */
  std::shared_ptr<MockWorker> worker =
      std::make_shared<MockWorker>(WorkerID::FromRandom(), 1234);
  std::shared_ptr<MockWorker> worker2 =
      std::make_shared<MockWorker>(WorkerID::FromRandom(), 12345);
  pool_.PushWorker(std::static_pointer_cast<WorkerInterface>(worker2));
  pool_.PushWorker(std::static_pointer_cast<WorkerInterface>(worker));

  rpc::RequestWorkerLeaseReply reply;
  int num_callbacks = 0;
  int *num_callbacks_ptr = &num_callbacks;
  auto callback = [num_callbacks_ptr](
                      Status, std::function<void()>, std::function<void()>) {
    (*num_callbacks_ptr) = *num_callbacks_ptr + 1;
  };

  /* Blocked on dependencies */
  auto lease = CreateLease({{ray::kCPU_ResourceLabel, 5}}, 2);
  auto missing_arg = lease.GetLeaseSpecification().GetDependencyIds()[0];
  missing_objects_.insert(missing_arg);
  std::unordered_set<LeaseID> expected_subscribed_leases = {
      lease.GetLeaseSpecification().LeaseId()};
  lease_manager_.QueueAndScheduleLease(
      lease,
      false,
      false,
      std::vector<internal::ReplyCallback>{internal::ReplyCallback(callback, &reply)});
  pool_.TriggerCallbacks();
  ASSERT_EQ(lease_dependency_manager_.subscribed_leases, expected_subscribed_leases);

  ASSERT_EQ(num_callbacks, 0);
  ASSERT_EQ(leased_workers_.size(), 0);
  ASSERT_EQ(pool_.workers.size(), 2);
  // It's important that we don't pop the worker until we need to. See
  // https://github.com/ray-project/ray/issues/13725.
  ASSERT_EQ(pool_.num_pops, 0);

  /* This lease can run */
  auto lease2 = CreateLease({{ray::kCPU_ResourceLabel, 5}}, 1);
  lease_manager_.QueueAndScheduleLease(
      lease2,
      false,
      false,
      std::vector<internal::ReplyCallback>{internal::ReplyCallback(callback, &reply)});
  pool_.TriggerCallbacks();
  ASSERT_EQ(lease_dependency_manager_.subscribed_leases, expected_subscribed_leases);

  AssertPinnedLeaseArgumentsPresent(lease2);
  ASSERT_EQ(num_callbacks, 1);
  ASSERT_EQ(leased_workers_.size(), 1);
  ASSERT_EQ(pool_.workers.size(), 1);
  ASSERT_EQ(pool_.num_pops, 1);

  /* First lease is unblocked now, but resources are no longer available */
  missing_objects_.erase(missing_arg);
  auto id = lease.GetLeaseSpecification().LeaseId();
  std::vector<LeaseID> unblocked = {id};
  local_lease_manager_->LeasesUnblocked(unblocked);
  ASSERT_EQ(lease_dependency_manager_.subscribed_leases, expected_subscribed_leases);

  AssertPinnedLeaseArgumentsPresent(lease2);
  ASSERT_EQ(num_callbacks, 1);
  ASSERT_EQ(leased_workers_.size(), 1);
  ASSERT_EQ(pool_.workers.size(), 1);
  ASSERT_EQ(pool_.num_pops, 1);

  /* Second lease finishes, making space for the original lease */
  RayLease finished_lease;
  local_lease_manager_->CleanupLease(leased_workers_.begin()->second, &finished_lease);
  leased_workers_.clear();

  lease_manager_.ScheduleAndGrantLeases();
  pool_.TriggerCallbacks();
  ASSERT_TRUE(lease_dependency_manager_.subscribed_leases.empty());

  // Lease2 is now done so lease can run.
  AssertPinnedLeaseArgumentsPresent(lease);
  ASSERT_EQ(num_callbacks, 2);
  ASSERT_EQ(leased_workers_.size(), 1);
  ASSERT_EQ(pool_.workers.size(), 0);
  ASSERT_EQ(pool_.num_pops, 2);

  local_lease_manager_->CleanupLease(leased_workers_.begin()->second, &finished_lease);
  AssertNoLeaks();
}

TEST_F(ClusterLeaseManagerTest, TestIsSelectedBasedOnLocality) {
  std::shared_ptr<MockWorker> worker1 =
      std::make_shared<MockWorker>(WorkerID::FromRandom(), 1234);
  std::shared_ptr<MockWorker> worker2 =
      std::make_shared<MockWorker>(WorkerID::FromRandom(), 1235);
  pool_.PushWorker(std::dynamic_pointer_cast<WorkerInterface>(worker1));
  pool_.PushWorker(std::dynamic_pointer_cast<WorkerInterface>(worker2));

  int num_callbacks = 0;
  auto callback = [&](Status, std::function<void()>, std::function<void()>) {
    num_callbacks++;
  };

  auto remote_node_id = NodeID::FromRandom();
  AddNode(remote_node_id, 8);

  auto lease1 = CreateLease({{ray::kCPU_ResourceLabel, 5}});
  rpc::RequestWorkerLeaseReply local_reply;
  lease_manager_.QueueAndScheduleLease(
      lease1,
      false,
      /*is_selected_based_on_locality=*/false,
      std::vector<internal::ReplyCallback>{
          internal::ReplyCallback(callback, &local_reply)});
  pool_.TriggerCallbacks();
  ASSERT_EQ(num_callbacks, 1);
  // The first lease was dispatched.
  ASSERT_EQ(leased_workers_.size(), 1);
  ASSERT_EQ(pool_.workers.size(), 1);

  auto lease2 = CreateLease({{ray::kCPU_ResourceLabel, 1}});
  rpc::RequestWorkerLeaseReply spillback_reply;
  lease_manager_.QueueAndScheduleLease(
      lease2,
      false,
      /*is_selected_based_on_locality=*/false,
      std::vector<internal::ReplyCallback>{
          internal::ReplyCallback(callback, &spillback_reply)});
  pool_.TriggerCallbacks();
  // The second lease was spilled.
  ASSERT_EQ(num_callbacks, 2);
  ASSERT_EQ(spillback_reply.retry_at_raylet_address().node_id(), remote_node_id.Binary());
  ASSERT_EQ(leased_workers_.size(), 1);
  ASSERT_EQ(pool_.workers.size(), 1);

  auto lease3 = CreateLease({{ray::kCPU_ResourceLabel, 1}});
  lease_manager_.QueueAndScheduleLease(
      lease3,
      false,
      /*is_selected_based_on_locality=*/true,
      std::vector<internal::ReplyCallback>{
          internal::ReplyCallback(callback, &local_reply)});
  pool_.TriggerCallbacks();
  ASSERT_EQ(num_callbacks, 3);
  // The third lease was dispatched.
  ASSERT_EQ(leased_workers_.size(), 2);
  ASSERT_EQ(pool_.workers.size(), 0);

  while (!leased_workers_.empty()) {
    RayLease finished_lease;
    local_lease_manager_->CleanupLease(leased_workers_.begin()->second, &finished_lease);
    leased_workers_.erase(leased_workers_.begin());
  }
  AssertNoLeaks();
}

TEST_F(ClusterLeaseManagerTest, TestGrantOrReject) {
  std::shared_ptr<MockWorker> worker1 =
      std::make_shared<MockWorker>(WorkerID::FromRandom(), 1234);
  std::shared_ptr<MockWorker> worker2 =
      std::make_shared<MockWorker>(WorkerID::FromRandom(), 1235);
  pool_.PushWorker(std::dynamic_pointer_cast<WorkerInterface>(worker1));
  pool_.PushWorker(std::dynamic_pointer_cast<WorkerInterface>(worker2));

  int num_callbacks = 0;
  auto callback = [&](Status, std::function<void()>, std::function<void()>) {
    num_callbacks++;
  };

  auto remote_node_id = NodeID::FromRandom();
  AddNode(remote_node_id, 8);

  auto lease1 = CreateLease({{ray::kCPU_ResourceLabel, 5}});
  rpc::RequestWorkerLeaseReply local_reply;
  lease_manager_.QueueAndScheduleLease(
      lease1,
      /*grant_or_reject=*/false,
      false,
      std::vector<internal::ReplyCallback>{
          internal::ReplyCallback(callback, &local_reply)});
  pool_.TriggerCallbacks();
  ASSERT_EQ(num_callbacks, 1);
  // The first lease was dispatched.
  ASSERT_EQ(leased_workers_.size(), 1);
  ASSERT_EQ(pool_.workers.size(), 1);

  auto lease2 = CreateLease({{ray::kCPU_ResourceLabel, 1}});
  rpc::RequestWorkerLeaseReply spillback_reply;
  lease_manager_.QueueAndScheduleLease(
      lease2,
      /*grant_or_reject=*/false,
      false,
      std::vector<internal::ReplyCallback>{
          internal::ReplyCallback(callback, &spillback_reply)});
  pool_.TriggerCallbacks();
  // The second lease was spilled.
  ASSERT_EQ(num_callbacks, 2);
  ASSERT_EQ(spillback_reply.retry_at_raylet_address().node_id(), remote_node_id.Binary());
  ASSERT_EQ(leased_workers_.size(), 1);
  ASSERT_EQ(pool_.workers.size(), 1);

  auto lease3 = CreateLease({{ray::kCPU_ResourceLabel, 1}});
  lease_manager_.QueueAndScheduleLease(
      lease3,
      /*grant_or_reject=*/true,
      false,
      std::vector<internal::ReplyCallback>{
          internal::ReplyCallback(callback, &local_reply)});
  pool_.TriggerCallbacks();
  ASSERT_EQ(num_callbacks, 3);
  // The third lease was dispatched.
  ASSERT_EQ(leased_workers_.size(), 2);
  ASSERT_EQ(pool_.workers.size(), 0);

  while (!leased_workers_.empty()) {
    RayLease finished_lease;
    local_lease_manager_->CleanupLease(leased_workers_.begin()->second, &finished_lease);
    leased_workers_.erase(leased_workers_.begin());
  }
  AssertNoLeaks();
}

TEST_F(ClusterLeaseManagerTest, TestSpillAfterAssigned) {
  /*
    Test the race condition in which a lease is assigned to the local node, but
    it cannot be run because a different lease gets assigned the resources
    first. The un-runnable lease should eventually get spilled back to another
    node.
  */
  std::shared_ptr<MockWorker> worker =
      std::make_shared<MockWorker>(WorkerID::FromRandom(), 1234);
  auto remote_node_id = NodeID::FromRandom();
  AddNode(remote_node_id, 5);

  int num_callbacks = 0;
  auto callback = [&](Status, std::function<void()>, std::function<void()>) {
    num_callbacks++;
  };

  /* Blocked on starting a worker. */
  auto lease = CreateLease({{ray::kCPU_ResourceLabel, 5}});
  rpc::RequestWorkerLeaseReply local_reply;
  lease_manager_.QueueAndScheduleLease(
      lease,
      false,
      false,
      std::vector<internal::ReplyCallback>{
          internal::ReplyCallback(callback, &local_reply)});
  pool_.TriggerCallbacks();

  ASSERT_EQ(num_callbacks, 0);
  ASSERT_EQ(leased_workers_.size(), 0);

  // Resources are no longer available for the second.
  auto lease2 = CreateLease({{ray::kCPU_ResourceLabel, 5}});
  rpc::RequestWorkerLeaseReply reject_reply;
  lease_manager_.QueueAndScheduleLease(
      lease2,
      /*grant_or_reject=*/true,
      false,
      std::vector<internal::ReplyCallback>{
          internal::ReplyCallback(callback, &reject_reply)});
  pool_.TriggerCallbacks();

  // The second lease was rejected.
  ASSERT_EQ(num_callbacks, 1);
  ASSERT_TRUE(reject_reply.rejected());
  ASSERT_EQ(leased_workers_.size(), 0);

  // Resources are no longer available for the third.
  auto lease3 = CreateLease({{ray::kCPU_ResourceLabel, 5}});
  rpc::RequestWorkerLeaseReply spillback_reply;
  lease_manager_.QueueAndScheduleLease(
      lease3,
      false,
      false,
      std::vector<internal::ReplyCallback>{
          internal::ReplyCallback(callback, &spillback_reply)});
  pool_.TriggerCallbacks();

  // The third lease was spilled.
  ASSERT_EQ(num_callbacks, 2);
  ASSERT_EQ(spillback_reply.retry_at_raylet_address().node_id(), remote_node_id.Binary());
  ASSERT_EQ(leased_workers_.size(), 0);

  // Two workers start. First lease was dispatched now.
  pool_.PushWorker(std::static_pointer_cast<WorkerInterface>(worker));
  pool_.PushWorker(std::static_pointer_cast<WorkerInterface>(worker));
  lease_manager_.ScheduleAndGrantLeases();
  pool_.TriggerCallbacks();
  // Check that all leases got removed from the queue.
  ASSERT_EQ(num_callbacks, 3);
  // The first lease was dispatched.
  ASSERT_EQ(leased_workers_.size(), 1);
  // Leave one alive worker.
  ASSERT_EQ(pool_.workers.size(), 1);

  RayLease finished_lease;
  local_lease_manager_->CleanupLease(leased_workers_.begin()->second, &finished_lease);
  ASSERT_EQ(finished_lease.GetLeaseSpecification().LeaseId(),
            lease.GetLeaseSpecification().LeaseId());

  AssertNoLeaks();
}

TEST_F(ClusterLeaseManagerTest, TestIdleNode) {
  RayLease lease = CreateLease({{}});
  rpc::RequestWorkerLeaseReply reply;
  bool callback_occurred = false;
  bool *callback_occurred_ptr = &callback_occurred;
  auto callback = [callback_occurred_ptr](
                      Status, std::function<void()>, std::function<void()>) {
    *callback_occurred_ptr = true;
  };

  lease_manager_.QueueAndScheduleLease(
      lease,
      false,
      false,
      std::vector<internal::ReplyCallback>{internal::ReplyCallback(callback, &reply)});
  pool_.TriggerCallbacks();
  ASSERT_TRUE(scheduler_->GetLocalResourceManager().IsLocalNodeIdle());
  ASSERT_FALSE(callback_occurred);
  ASSERT_EQ(leased_workers_.size(), 0);

  std::shared_ptr<MockWorker> worker =
      std::make_shared<MockWorker>(WorkerID::FromRandom(), 1234);
  pool_.PushWorker(std::static_pointer_cast<WorkerInterface>(worker));
  pool_.TriggerCallbacks();

  ASSERT_TRUE(callback_occurred);
  ASSERT_EQ(leased_workers_.size(), 1);
  ASSERT_FALSE(scheduler_->GetLocalResourceManager().IsLocalNodeIdle());
  ASSERT_EQ(node_info_calls_, 0);
}

TEST_F(ClusterLeaseManagerTest, NotOKPopWorkerAfterDrainingTest) {
  /*
    Test cases where the node is being drained after PopWorker is called
    and PopWorker fails.
  */

  // Make the node non-idle so that the node won't be drained and terminated immediately.
  {
    std::shared_ptr<TaskResourceInstances> task_allocation =
        std::make_shared<TaskResourceInstances>();
    ResourceRequest resource_request =
        ResourceMapToResourceRequest({{ResourceID::CPU(), 1.0}}, false);
    scheduler_->GetLocalResourceManager().AllocateLocalTaskResources(resource_request,
                                                                     task_allocation);
  }

  RayLease lease1 = CreateLease({{ray::kCPU_ResourceLabel, 1}});
  RayLease lease2 = CreateLease({{ray::kCPU_ResourceLabel, 1}});
  rpc::RequestWorkerLeaseReply reply1;
  rpc::RequestWorkerLeaseReply reply2;
  bool callback_called = false;
  bool *callback_called_ptr = &callback_called;
  auto callback = [callback_called_ptr](
                      Status, std::function<void()>, std::function<void()>) {
    *callback_called_ptr = true;
  };
  lease_manager_.QueueAndScheduleLease(
      lease1,
      false,
      false,
      std::vector<internal::ReplyCallback>{internal::ReplyCallback(callback, &reply1)});
  lease_manager_.QueueAndScheduleLease(
      lease2,
      false,
      false,
      std::vector<internal::ReplyCallback>{internal::ReplyCallback(callback, &reply2)});

  auto remote_node_id = NodeID::FromRandom();
  AddNode(remote_node_id, 5);

  // Drain the local node.
  rpc::DrainRayletRequest drain_request;
  drain_request.set_deadline_timestamp_ms(std::numeric_limits<int64_t>::max());
  scheduler_->GetLocalResourceManager().SetLocalNodeDraining(drain_request);

  pool_.callbacks[lease1.GetLeaseSpecification().GetRuntimeEnvHash()].front()(
      nullptr, PopWorkerStatus::WorkerPendingRegistration, "");
  pool_.callbacks[lease1.GetLeaseSpecification().GetRuntimeEnvHash()].back()(
      nullptr, PopWorkerStatus::RuntimeEnvCreationFailed, "runtime env setup error");
  pool_.callbacks.clear();
  lease_manager_.ScheduleAndGrantLeases();
  // lease1 is spilled and lease2 is cancelled.
  ASSERT_EQ(reply1.retry_at_raylet_address().node_id(), remote_node_id.Binary());
  ASSERT_TRUE(reply2.canceled());
  ASSERT_EQ(reply2.scheduling_failure_message(), "runtime env setup error");
}

TEST_F(ClusterLeaseManagerTest, NotOKPopWorkerTest) {
  RayLease lease1 = CreateLease({{ray::kCPU_ResourceLabel, 1}});
  rpc::RequestWorkerLeaseReply reply;
  bool callback_called = false;
  bool *callback_called_ptr = &callback_called;
  auto callback = [callback_called_ptr](
                      Status, std::function<void()>, std::function<void()>) {
    *callback_called_ptr = true;
  };
  lease_manager_.QueueAndScheduleLease(
      lease1,
      false,
      false,
      std::vector<internal::ReplyCallback>{internal::ReplyCallback(callback, &reply)});
  ASSERT_EQ(NumLeasesToDispatchWithStatus(internal::WorkStatus::WAITING_FOR_WORKER), 1);
  ASSERT_EQ(NumLeasesToDispatchWithStatus(internal::WorkStatus::WAITING), 0);
  ASSERT_EQ(NumRunningLeases(), 1);
  pool_.TriggerCallbacksWithNotOKStatus(PopWorkerStatus::WorkerPendingRegistration);
  ASSERT_FALSE(callback_called);
  ASSERT_EQ(NumLeasesToDispatchWithStatus(internal::WorkStatus::WAITING_FOR_WORKER), 0);
  ASSERT_EQ(NumLeasesToDispatchWithStatus(internal::WorkStatus::WAITING), 1);
  ASSERT_EQ(NumRunningLeases(), 0);
  ASSERT_TRUE(lease_manager_.CancelLease(lease1.GetLeaseSpecification().LeaseId()));

  callback_called = false;
  reply.Clear();
  RayLease lease2 = CreateLease({{ray::kCPU_ResourceLabel, 1}});
  lease_manager_.QueueAndScheduleLease(
      lease2,
      false,
      false,
      std::vector<internal::ReplyCallback>{internal::ReplyCallback(callback, &reply)});
  ASSERT_EQ(NumLeasesToDispatchWithStatus(internal::WorkStatus::WAITING_FOR_WORKER), 1);
  ASSERT_EQ(NumLeasesToDispatchWithStatus(internal::WorkStatus::WAITING), 0);
  ASSERT_EQ(NumRunningLeases(), 1);
  // The lease should be cancelled.
  const auto runtime_env_error_msg = "Runtime env error message";
  pool_.TriggerCallbacksWithNotOKStatus(PopWorkerStatus::RuntimeEnvCreationFailed,
                                        runtime_env_error_msg);
  ASSERT_TRUE(callback_called);
  ASSERT_EQ(NumLeasesToDispatchWithStatus(internal::WorkStatus::WAITING_FOR_WORKER), 0);
  ASSERT_EQ(NumLeasesToDispatchWithStatus(internal::WorkStatus::WAITING), 0);
  ASSERT_EQ(NumRunningLeases(), 0);
  ASSERT_TRUE(reply.canceled());
  ASSERT_EQ(reply.scheduling_failure_message(), runtime_env_error_msg);

  // Test that local lease manager handles PopWorkerStatus::JobFinished correctly.
  callback_called = false;
  reply.Clear();
  RayLease lease3 = CreateLease({{ray::kCPU_ResourceLabel, 1}});
  lease_manager_.QueueAndScheduleLease(
      lease3,
      false,
      false,
      std::vector<internal::ReplyCallback>{internal::ReplyCallback(callback, &reply)});
  ASSERT_EQ(NumLeasesToDispatchWithStatus(internal::WorkStatus::WAITING_FOR_WORKER), 1);
  ASSERT_EQ(NumLeasesToDispatchWithStatus(internal::WorkStatus::WAITING), 0);
  ASSERT_EQ(NumRunningLeases(), 1);
  pool_.TriggerCallbacksWithNotOKStatus(PopWorkerStatus::JobFinished);
  // The lease should be removed from the leases_to_grant queue.
  ASSERT_FALSE(callback_called);
  ASSERT_EQ(NumLeasesToDispatchWithStatus(internal::WorkStatus::WAITING_FOR_WORKER), 0);
  ASSERT_EQ(NumLeasesToDispatchWithStatus(internal::WorkStatus::WAITING), 0);
  ASSERT_EQ(NumRunningLeases(), 0);

  AssertNoLeaks();
}

TEST_F(ClusterLeaseManagerTest, TaskUnschedulableTest) {
  LeaseSpecification lease_spec =
      CreateLease({{ray::kCPU_ResourceLabel, 1}}).GetLeaseSpecification();
  lease_spec.GetMutableMessage()
      .mutable_scheduling_strategy()
      ->mutable_node_affinity_scheduling_strategy()
      ->set_node_id(NodeID::FromRandom().Binary());
  lease_spec.GetMutableMessage()
      .mutable_scheduling_strategy()
      ->mutable_node_affinity_scheduling_strategy()
      ->set_soft(false);
  rpc::RequestWorkerLeaseReply reply;

  bool callback_called = false;
  bool *callback_called_ptr = &callback_called;
  auto callback = [callback_called_ptr](
                      Status, std::function<void()>, std::function<void()>) {
    *callback_called_ptr = true;
  };

  lease_manager_.QueueAndScheduleLease(
      RayLease(lease_spec),
      false,
      false,
      std::vector<internal::ReplyCallback>{internal::ReplyCallback(callback, &reply)});
  ASSERT_TRUE(callback_called);
  ASSERT_TRUE(reply.canceled());
  ASSERT_EQ(reply.failure_type(),
            rpc::RequestWorkerLeaseReply::SCHEDULING_CANCELLED_UNSCHEDULABLE);

  AssertNoLeaks();
}

TEST_F(ClusterLeaseManagerTest, TaskCancellationTest) {
  std::shared_ptr<MockWorker> worker =
      std::make_shared<MockWorker>(WorkerID::FromRandom(), 1234);
  RayLease lease1 = CreateLease({{ray::kCPU_ResourceLabel, 1}});
  rpc::RequestWorkerLeaseReply reply;

  bool callback_called = false;
  bool *callback_called_ptr = &callback_called;
  auto callback = [callback_called_ptr](
                      Status, std::function<void()>, std::function<void()>) {
    *callback_called_ptr = true;
  };

  // Lease1 not queued so we can't cancel it.
  ASSERT_FALSE(lease_manager_.CancelLease(lease1.GetLeaseSpecification().LeaseId()));

  lease_manager_.QueueAndScheduleLease(
      lease1,
      false,
      false,
      std::vector<internal::ReplyCallback>{internal::ReplyCallback(callback, &reply)});
  pool_.TriggerCallbacks();

  // Lease1 is now in dispatch queue.
  callback_called = false;
  reply.Clear();
  ASSERT_TRUE(lease_manager_.CancelLease(lease1.GetLeaseSpecification().LeaseId()));
  pool_.PushWorker(std::static_pointer_cast<WorkerInterface>(worker));
  lease_manager_.ScheduleAndGrantLeases();
  pool_.TriggerCallbacks();
  // Lease1 will not be granted.
  ASSERT_TRUE(callback_called);
  ASSERT_TRUE(reply.canceled());
  ASSERT_EQ(leased_workers_.size(), 0);

  RayLease lease2 = CreateLease({{ray::kCPU_ResourceLabel, 1}});
  lease_manager_.QueueAndScheduleLease(
      lease2,
      false,
      false,
      std::vector<internal::ReplyCallback>{internal::ReplyCallback(callback, &reply)});
  pool_.TriggerCallbacks();

  // Lease2 is now granted so we can't cancel it.
  callback_called = false;
  reply.Clear();
  ASSERT_FALSE(lease_manager_.CancelLease(lease2.GetLeaseSpecification().LeaseId()));
  ASSERT_FALSE(reply.canceled());
  ASSERT_FALSE(callback_called);
  ASSERT_EQ(pool_.workers.size(), 0);
  ASSERT_EQ(leased_workers_.size(), 1);

  RayLease finished_lease;
  local_lease_manager_->CleanupLease(leased_workers_.begin()->second, &finished_lease);
  ASSERT_EQ(finished_lease.GetLeaseSpecification().LeaseId(),
            lease2.GetLeaseSpecification().LeaseId());

  RayLease lease3 = CreateLease({{ray::kCPU_ResourceLabel, 2}});
  rpc::RequestWorkerLeaseReply reply3;
  RayLease lease4 = CreateLease({{ray::kCPU_ResourceLabel, 200}});
  rpc::RequestWorkerLeaseReply reply4;
  // Lease 3 should be popping worker
  lease_manager_.QueueAndScheduleLease(
      lease3,
      false,
      false,
      std::vector<internal::ReplyCallback>{internal::ReplyCallback(callback, &reply3)});
  // Lease 4 is infeasible
  lease_manager_.QueueAndScheduleLease(
      lease4,
      false,
      false,
      std::vector<internal::ReplyCallback>{internal::ReplyCallback(callback, &reply4)});
  pool_.TriggerCallbacks();
  ASSERT_TRUE(lease_manager_.CancelLeases(
      [](const std::shared_ptr<internal::Work> &work) { return true; },
      rpc::RequestWorkerLeaseReply::SCHEDULING_CANCELLED_INTENDED,
      ""));
  ASSERT_TRUE(reply3.canceled());
  ASSERT_TRUE(reply4.canceled());

  AssertNoLeaks();
}

TEST_F(ClusterLeaseManagerTest, TaskCancelInfeasibleTask) {
  /* Make sure cancelLease works for infeasible leases */
  std::shared_ptr<MockWorker> worker =
      std::make_shared<MockWorker>(WorkerID::FromRandom(), 1234);
  pool_.PushWorker(std::static_pointer_cast<WorkerInterface>(worker));

  RayLease lease = CreateLease({{ray::kCPU_ResourceLabel, 12}});
  rpc::RequestWorkerLeaseReply reply;

  bool callback_called = false;
  bool *callback_called_ptr = &callback_called;
  auto callback = [callback_called_ptr](
                      Status, std::function<void()>, std::function<void()>) {
    *callback_called_ptr = true;
  };

  lease_manager_.QueueAndScheduleLease(
      lease,
      false,
      false,
      std::vector<internal::ReplyCallback>{internal::ReplyCallback(callback, &reply)});
  pool_.TriggerCallbacks();

  // RayLease is now queued so cancellation works.
  ASSERT_TRUE(lease_manager_.CancelLease(lease.GetLeaseSpecification().LeaseId()));
  lease_manager_.ScheduleAndGrantLeases();
  pool_.TriggerCallbacks();
  // Lease will not be granted.
  ASSERT_TRUE(callback_called);
  ASSERT_TRUE(reply.canceled());
  ASSERT_EQ(leased_workers_.size(), 0);
  ASSERT_EQ(pool_.workers.size(), 1);

  // Although the feasible node is added, lease shouldn't be granted because it is
  // cancelled.
  auto remote_node_id = NodeID::FromRandom();
  AddNode(remote_node_id, 12);
  lease_manager_.ScheduleAndGrantLeases();
  pool_.TriggerCallbacks();
  ASSERT_TRUE(callback_called);
  ASSERT_TRUE(reply.canceled());
  ASSERT_EQ(leased_workers_.size(), 0);
  ASSERT_EQ(pool_.workers.size(), 1);
  AssertNoLeaks();
}

TEST_F(ClusterLeaseManagerTest, TaskCancelWithResourceShape) {
  // lease1 doesn't match the resource shape so shouldn't be cancelled
  // lease2 matches the resource shape and should be cancelled
  std::shared_ptr<MockWorker> worker =
      std::make_shared<MockWorker>(WorkerID::FromRandom(), 1234);
  RayLease lease1 = CreateLease({{ray::kCPU_ResourceLabel, 1}});
  RayLease lease2 = CreateLease({{ray::kCPU_ResourceLabel, 10}});
  absl::flat_hash_map<std::string, double> resource_shape_1 = {
      {ray::kCPU_ResourceLabel, 10}};
  absl::flat_hash_map<std::string, double> resource_shape_2 = {
      {ray::kCPU_ResourceLabel, 11}};
  std::vector<ResourceSet> target_resource_shapes = {ResourceSet(resource_shape_1),
                                                     ResourceSet(resource_shape_2)};
  rpc::RequestWorkerLeaseReply reply1;
  rpc::RequestWorkerLeaseReply reply2;

  bool callback_called_1 = false;
  bool callback_called_2 = false;
  bool *callback_called_ptr_1 = &callback_called_1;
  bool *callback_called_ptr_2 = &callback_called_2;
  auto callback1 = [callback_called_ptr_1](
                       Status, std::function<void()>, std::function<void()>) {
    *callback_called_ptr_1 = true;
  };
  auto callback2 = [callback_called_ptr_2](
                       Status, std::function<void()>, std::function<void()>) {
    *callback_called_ptr_2 = true;
  };

  lease_manager_.QueueAndScheduleLease(
      lease1,
      false,
      false,
      std::vector<internal::ReplyCallback>{internal::ReplyCallback(callback1, &reply1)});
  pool_.TriggerCallbacks();
  lease_manager_.QueueAndScheduleLease(
      lease2,
      false,
      false,
      std::vector<internal::ReplyCallback>{internal::ReplyCallback(callback2, &reply2)});
  pool_.TriggerCallbacks();

  callback_called_1 = false;
  callback_called_2 = false;
  reply1.Clear();
  reply2.Clear();
  ASSERT_TRUE(lease_manager_.CancelLeasesWithResourceShapes(target_resource_shapes));
  ASSERT_FALSE(reply1.canceled());
  ASSERT_FALSE(callback_called_1);
  ASSERT_TRUE(reply2.canceled());
  ASSERT_TRUE(callback_called_2);

  pool_.PushWorker(std::static_pointer_cast<WorkerInterface>(worker));
  lease_manager_.ScheduleAndGrantLeases();
  pool_.TriggerCallbacks();
  ASSERT_EQ(pool_.workers.size(), 0);
  ASSERT_EQ(leased_workers_.size(), 1);

  RayLease finished_lease;
  local_lease_manager_->CleanupLease(leased_workers_.begin()->second, &finished_lease);
  ASSERT_EQ(finished_lease.GetLeaseSpecification().LeaseId(),
            lease1.GetLeaseSpecification().LeaseId());

  AssertNoLeaks();
}

TEST_F(ClusterLeaseManagerTest, HeartbeatTest) {
  std::shared_ptr<MockWorker> worker =
      std::make_shared<MockWorker>(WorkerID::FromRandom(), 1234);
  pool_.PushWorker(std::static_pointer_cast<WorkerInterface>(worker));

  {
    RayLease lease = CreateLease({{ray::kCPU_ResourceLabel, 1}});
    rpc::RequestWorkerLeaseReply reply;

    bool callback_called = false;
    bool *callback_called_ptr = &callback_called;
    auto callback = [callback_called_ptr](
                        Status, std::function<void()>, std::function<void()>) {
      *callback_called_ptr = true;
    };

    lease_manager_.QueueAndScheduleLease(
        lease,
        false,
        false,
        std::vector<internal::ReplyCallback>{internal::ReplyCallback(callback, &reply)});
    pool_.TriggerCallbacks();
    ASSERT_TRUE(callback_called);
    // Now {CPU: 7, GPU: 4, MEM:128}
  }

  {
    RayLease lease = CreateLease({{ray::kCPU_ResourceLabel, 1}});
    rpc::RequestWorkerLeaseReply reply;

    bool callback_called = false;
    bool *callback_called_ptr = &callback_called;
    auto callback = [callback_called_ptr](
                        Status, std::function<void()>, std::function<void()>) {
      *callback_called_ptr = true;
    };

    lease_manager_.QueueAndScheduleLease(
        lease,
        false,
        false,
        std::vector<internal::ReplyCallback>{internal::ReplyCallback(callback, &reply)});
    pool_.TriggerCallbacks();
    ASSERT_FALSE(callback_called);  // No worker available.
    // Now {CPU: 7, GPU: 4, MEM:128} with 1 queued lease.
  }

  {
    RayLease lease =
        CreateLease({{ray::kCPU_ResourceLabel, 9}, {ray::kGPU_ResourceLabel, 5}});
    rpc::RequestWorkerLeaseReply reply;

    bool callback_called = false;
    bool *callback_called_ptr = &callback_called;
    auto callback = [callback_called_ptr](
                        Status, std::function<void()>, std::function<void()>) {
      *callback_called_ptr = true;
    };

    lease_manager_.QueueAndScheduleLease(
        lease,
        false,
        false,
        std::vector<internal::ReplyCallback>{internal::ReplyCallback(callback, &reply)});
    pool_.TriggerCallbacks();
    ASSERT_FALSE(callback_called);  // Infeasible.
    // Now there is also an infeasible lease {CPU: 9}.
  }

  {
    RayLease lease =
        CreateLease({{ray::kCPU_ResourceLabel, 10}, {ray::kGPU_ResourceLabel, 1}});
    rpc::RequestWorkerLeaseReply reply;

    bool callback_called = false;
    bool *callback_called_ptr = &callback_called;
    auto callback = [callback_called_ptr](
                        Status, std::function<void()>, std::function<void()>) {
      *callback_called_ptr = true;
    };

    lease_manager_.QueueAndScheduleLease(
        lease,
        false,
        false,
        std::vector<internal::ReplyCallback>{internal::ReplyCallback(callback, &reply)});
    pool_.TriggerCallbacks();
    ASSERT_FALSE(callback_called);  // Infeasible.
    // Now there is also an infeasible lease {CPU: 10}.
  }

  {
    rpc::ResourcesData data;
    lease_manager_.FillResourceUsage(data);

    auto load_by_shape =
        data.mutable_resource_load_by_shape()->mutable_resource_demands();
    ASSERT_EQ(load_by_shape->size(), 3);

    std::vector<std::vector<unsigned int>> expected = {
        // infeasible, ready, CPU, GPU, size
        {1, 0, 10, 1, 2},
        {1, 0, 9, 5, 2},
        {0, 0, 1, 0, 1}};

    for (auto &load : *load_by_shape) {
      bool found = false;
      for (unsigned int i = 0; i < expected.size(); i++) {
        auto expected_load = expected[i];
        auto shape = *load.mutable_shape();
        bool match =
            (expected_load[0] == load.num_infeasible_requests_queued() &&
             expected_load[1] == load.num_ready_requests_queued() &&
             expected_load[2] == shape["CPU"] && expected_load[4] == shape.size());
        if (expected_load[3]) {
          match = match && shape["GPU"];
        }
        // These logs are very useful for debugging.
        // RAY_LOG(ERROR) << "==========================";
        // RAY_LOG(ERROR) << expected_load[0] << "\t"
        //                << load.num_infeasible_requests_queued();
        // RAY_LOG(ERROR) << expected_load[1] << "\t" << load.num_ready_requests_queued();
        // RAY_LOG(ERROR) << expected_load[2] << "\t" << shape["CPU"];
        // RAY_LOG(ERROR) << expected_load[3] << "\t" << shape["GPU"];
        // RAY_LOG(ERROR) << expected_load[4] << "\t" << shape.size();
        // RAY_LOG(ERROR) << "==========================";
        // RAY_LOG(ERROR) << load.DebugString();
        // RAY_LOG(ERROR) << "-----------------------------------";
        found = found || match;
      }
      ASSERT_TRUE(found);
    }
  }
}

TEST_F(ClusterLeaseManagerTest, ResourceReportForNodeAffinitySchedulingStrategyTasks) {
  rpc::RequestWorkerLeaseReply reply;
  bool callback_occurred = false;
  bool *callback_occurred_ptr = &callback_occurred;
  auto callback = [callback_occurred_ptr](
                      Status, std::function<void()>, std::function<void()>) {
    *callback_occurred_ptr = true;
  };

  // Feasible strict lease won't be reported.
  rpc::SchedulingStrategy scheduling_strategy;
  scheduling_strategy.mutable_node_affinity_scheduling_strategy()->set_node_id(
      id_.Binary());
  scheduling_strategy.mutable_node_affinity_scheduling_strategy()->set_soft(false);
  RayLease lease1 =
      CreateLease({{ray::kCPU_ResourceLabel, 1}}, 0, {}, nullptr, scheduling_strategy);
  lease_manager_.QueueAndScheduleLease(
      lease1,
      false,
      false,
      std::vector<internal::ReplyCallback>{internal::ReplyCallback(callback, &reply)});

  // Feasible soft lease won't be reported.
  scheduling_strategy.mutable_node_affinity_scheduling_strategy()->set_node_id(
      id_.Binary());
  scheduling_strategy.mutable_node_affinity_scheduling_strategy()->set_soft(true);
  RayLease task2 =
      CreateLease({{ray::kCPU_ResourceLabel, 2}}, 0, {}, nullptr, scheduling_strategy);
  lease_manager_.QueueAndScheduleLease(
      task2,
      false,
      false,
      std::vector<internal::ReplyCallback>{internal::ReplyCallback(callback, &reply)});

  // Infeasible soft lease will be reported.
  scheduling_strategy.mutable_node_affinity_scheduling_strategy()->set_node_id(
      id_.Binary());
  scheduling_strategy.mutable_node_affinity_scheduling_strategy()->set_soft(true);
  RayLease task3 =
      CreateLease({{ray::kGPU_ResourceLabel, 1}}, 0, {}, nullptr, scheduling_strategy);
  lease_manager_.QueueAndScheduleLease(
      task3,
      false,
      false,
      std::vector<internal::ReplyCallback>{internal::ReplyCallback(callback, &reply)});
  ASSERT_FALSE(callback_occurred);

  // Infeasible strict lease won't be reported (will fail immediately).
  scheduling_strategy.mutable_node_affinity_scheduling_strategy()->set_node_id(
      id_.Binary());
  scheduling_strategy.mutable_node_affinity_scheduling_strategy()->set_soft(false);
  RayLease task4 =
      CreateLease({{ray::kGPU_ResourceLabel, 2}}, 0, {}, nullptr, scheduling_strategy);
  lease_manager_.QueueAndScheduleLease(
      task4,
      false,
      false,
      std::vector<internal::ReplyCallback>{internal::ReplyCallback(callback, &reply)});
  ASSERT_TRUE(callback_occurred);
  ASSERT_TRUE(reply.canceled());
  ASSERT_EQ(reply.failure_type(),
            rpc::RequestWorkerLeaseReply::SCHEDULING_CANCELLED_UNSCHEDULABLE);

  ASSERT_EQ(leased_workers_.size(), 0);
  ASSERT_EQ(pool_.workers.size(), 0);

  rpc::ResourcesData data;
  lease_manager_.FillResourceUsage(data);
  auto resource_load_by_shape = data.resource_load_by_shape();
  ASSERT_EQ(resource_load_by_shape.resource_demands().size(), 1);
  auto demand = resource_load_by_shape.resource_demands()[0];
  ASSERT_EQ(demand.num_infeasible_requests_queued(), 1);
  ASSERT_EQ(demand.num_ready_requests_queued(), 0);
  ASSERT_EQ(demand.shape().at("GPU"), 1);
}

TEST_F(ClusterLeaseManagerTest, BacklogReportTest) {
  /*
    Test basic scheduler functionality:
    1. Queue and attempt to schedule/dispatch a test with no workers available
    2. A worker becomes available, dispatch again.
   */
  rpc::RequestWorkerLeaseReply reply;
  bool callback_occurred = false;
  bool *callback_occurred_ptr = &callback_occurred;
  auto callback = [callback_occurred_ptr](
                      Status, std::function<void()>, std::function<void()>) {
    *callback_occurred_ptr = true;
  };

  std::vector<LeaseID> to_cancel;
  std::vector<WorkerID> worker_ids;
  for (int i = 0; i < 10; i++) {
    RayLease lease = CreateLease({{ray::kCPU_ResourceLabel, 8}});
    lease_manager_.QueueAndScheduleLease(
        lease,
        false,
        false,
        std::vector<internal::ReplyCallback>{internal::ReplyCallback(callback, &reply)});
    worker_ids.push_back(WorkerID::FromRandom());
    local_lease_manager_->SetWorkerBacklog(
        lease.GetLeaseSpecification().GetSchedulingClass(), worker_ids.back(), 10 - i);
    pool_.TriggerCallbacks();
    // Don't add the first lease to `to_cancel`.
    if (i != 0) {
      to_cancel.push_back(lease.GetLeaseSpecification().LeaseId());
    }
  }

  ASSERT_FALSE(callback_occurred);
  ASSERT_EQ(leased_workers_.size(), 0);
  ASSERT_EQ(pool_.workers.size(), 0);
  ASSERT_EQ(node_info_calls_, 0);

  {  // 1 lease has resources allocated, while remaining 9 are stuck.
    rpc::ResourcesData data;
    lease_manager_.FillResourceUsage(data);
    auto resource_load_by_shape = data.resource_load_by_shape();
    auto shape1 = resource_load_by_shape.resource_demands()[0];

    ASSERT_EQ(shape1.backlog_size(), 55);
    ASSERT_EQ(shape1.num_infeasible_requests_queued(), 0);
    ASSERT_EQ(shape1.num_ready_requests_queued(), 9);
  }

  // Push a worker so the first lease can be granted.
  std::shared_ptr<MockWorker> worker =
      std::make_shared<MockWorker>(WorkerID::FromRandom(), 1234);
  pool_.PushWorker(worker);
  lease_manager_.ScheduleAndGrantLeases();
  local_lease_manager_->ClearWorkerBacklog(worker_ids[0]);
  pool_.TriggerCallbacks();

  {
    rpc::ResourcesData data;
    lease_manager_.FillResourceUsage(data);
    auto resource_load_by_shape = data.resource_load_by_shape();
    auto shape1 = resource_load_by_shape.resource_demands()[0];

    ASSERT_TRUE(callback_occurred);
    ASSERT_EQ(shape1.backlog_size(), 45);
    ASSERT_EQ(shape1.num_infeasible_requests_queued(), 0);
    ASSERT_EQ(shape1.num_ready_requests_queued(), 9);
  }

  // Cancel the rest.
  for (auto &lease_id : to_cancel) {
    ASSERT_TRUE(lease_manager_.CancelLease(lease_id));
  }

  for (size_t i = 1; i < worker_ids.size(); ++i) {
    local_lease_manager_->ClearWorkerBacklog(worker_ids[i]);
  }

  {
    rpc::ResourcesData data;
    lease_manager_.FillResourceUsage(data);
    auto resource_load_by_shape = data.resource_load_by_shape();
    ASSERT_EQ(resource_load_by_shape.resource_demands().size(), 0);

    while (!leased_workers_.empty()) {
      RayLease finished_lease;
      local_lease_manager_->CleanupLease(leased_workers_.begin()->second,
                                         &finished_lease);
      leased_workers_.erase(leased_workers_.begin());
    }
    AssertNoLeaks();
  }
}

TEST_F(ClusterLeaseManagerTest, OwnerDeadTest) {
  // Test the case when the lease owner (worker or node) dies, the lease is cancelled.
  RayLease lease = CreateLease({{ray::kCPU_ResourceLabel, 4}});
  rpc::RequestWorkerLeaseReply reply;
  bool callback_occurred = false;
  bool *callback_occurred_ptr = &callback_occurred;
  auto callback = [callback_occurred_ptr](
                      Status, std::function<void()>, std::function<void()>) {
    *callback_occurred_ptr = true;
  };

  lease_manager_.QueueAndScheduleLease(
      lease,
      false,
      false,
      std::vector<internal::ReplyCallback>{internal::ReplyCallback(callback, &reply)});
  pool_.TriggerCallbacks();

  ASSERT_FALSE(callback_occurred);

  lease_manager_.CancelAllLeasesOwnedBy(lease.GetLeaseSpecification().CallerWorkerId());

  AssertNoLeaks();

  callback_occurred = false;
  lease_manager_.QueueAndScheduleLease(
      lease,
      false,
      false,
      std::vector<internal::ReplyCallback>{internal::ReplyCallback(callback, &reply)});
  pool_.TriggerCallbacks();

  ASSERT_FALSE(callback_occurred);

  lease_manager_.CancelAllLeasesOwnedBy(lease.GetLeaseSpecification().CallerNodeId());

  AssertNoLeaks();
}

TEST_F(ClusterLeaseManagerTest, TestInfeasibleLeaseWarning) {
  /*
    Test if infeasible leases warnings are printed.
   */
  // Create an infeasible lease.
  RayLease lease = CreateLease({{ray::kCPU_ResourceLabel, 12}});
  rpc::RequestWorkerLeaseReply reply;
  std::shared_ptr<bool> callback_occurred = std::make_shared<bool>(false);
  auto callback = [callback_occurred](
                      Status, std::function<void()>, std::function<void()>) {
    *callback_occurred = true;
  };
  lease_manager_.QueueAndScheduleLease(
      lease,
      false,
      false,
      std::vector<internal::ReplyCallback>{internal::ReplyCallback(callback, &reply)});
  pool_.TriggerCallbacks();
  ASSERT_EQ(announce_infeasible_lease_calls_, 1);

  // Infeasible warning shouldn't be reprinted when the previous lease is still infeasible
  // after adding a new node.
  AddNode(NodeID::FromRandom(), 8);
  std::shared_ptr<MockWorker> worker =
      std::make_shared<MockWorker>(WorkerID::FromRandom(), 1234);
  pool_.PushWorker(std::static_pointer_cast<WorkerInterface>(worker));
  lease_manager_.ScheduleAndGrantLeases();
  pool_.TriggerCallbacks();
  // Lease shouldn't be scheduled yet.
  ASSERT_EQ(announce_infeasible_lease_calls_, 1);
  ASSERT_FALSE(*callback_occurred);
  ASSERT_EQ(leased_workers_.size(), 0);
  ASSERT_EQ(pool_.workers.size(), 1);

  // Now we have a node that is feasible to schedule the lease. Make sure the infeasible
  // lease is spillbacked properly.
  auto remote_node_id = NodeID::FromRandom();
  AddNode(remote_node_id, 12);
  lease_manager_.ScheduleAndGrantLeases();
  pool_.TriggerCallbacks();
  // Make sure nothing happens locally.
  ASSERT_EQ(announce_infeasible_lease_calls_, 1);
  ASSERT_TRUE(*callback_occurred);
  ASSERT_EQ(leased_workers_.size(), 0);
  ASSERT_EQ(pool_.workers.size(), 1);
  // Make sure the spillback callback is called.
  ASSERT_EQ(reply.retry_at_raylet_address().node_id(), remote_node_id.Binary());
  AssertNoLeaks();
}

TEST_F(ClusterLeaseManagerTest, TestMultipleInfeasibleLeasesWarnOnce) {
  /*
    Test infeasible warning is printed only once when the same shape is queued again.
   */

  // Make sure the first infeasible lease announces warning.
  RayLease lease = CreateLease({{ray::kCPU_ResourceLabel, 12}});
  rpc::RequestWorkerLeaseReply reply;
  std::shared_ptr<bool> callback_occurred = std::make_shared<bool>(false);
  auto callback = [callback_occurred](
                      Status, std::function<void()>, std::function<void()>) {
    *callback_occurred = true;
  };
  lease_manager_.QueueAndScheduleLease(
      lease,
      false,
      false,
      std::vector<internal::ReplyCallback>{internal::ReplyCallback(callback, &reply)});
  pool_.TriggerCallbacks();
  ASSERT_EQ(announce_infeasible_lease_calls_, 1);

  // Make sure the same shape infeasible lease won't be announced.
  RayLease lease2 = CreateLease({{ray::kCPU_ResourceLabel, 12}});
  rpc::RequestWorkerLeaseReply reply2;
  std::shared_ptr<bool> callback_occurred2 = std::make_shared<bool>(false);
  auto callback2 = [callback_occurred2](
                       Status, std::function<void()>, std::function<void()>) {
    *callback_occurred2 = true;
  };
  lease_manager_.QueueAndScheduleLease(
      lease2,
      false,
      false,
      std::vector<internal::ReplyCallback>{internal::ReplyCallback(callback2, &reply2)});
  pool_.TriggerCallbacks();
  ASSERT_EQ(announce_infeasible_lease_calls_, 1);
}

TEST_F(ClusterLeaseManagerTest, TestAnyPendingLeasesForResourceAcquisition) {
  /*
    Check if the manager can correctly identify pending leases.
   */
  std::shared_ptr<MockWorker> worker =
      std::make_shared<MockWorker>(WorkerID::FromRandom(), 1234);
  pool_.PushWorker(std::static_pointer_cast<WorkerInterface>(worker));

  // lease1: running.
  RayLease lease = CreateLease({{ray::kCPU_ResourceLabel, 6}});
  rpc::RequestWorkerLeaseReply reply;
  std::shared_ptr<bool> callback_occurred = std::make_shared<bool>(false);
  auto callback = [callback_occurred](
                      Status, std::function<void()>, std::function<void()>) {
    *callback_occurred = true;
  };
  lease_manager_.QueueAndScheduleLease(
      lease,
      false,
      false,
      std::vector<internal::ReplyCallback>{internal::ReplyCallback(callback, &reply)});
  pool_.TriggerCallbacks();
  ASSERT_TRUE(*callback_occurred);
  ASSERT_EQ(leased_workers_.size(), 1);
  ASSERT_EQ(pool_.workers.size(), 0);

  // lease1: running. Progress is made, and there's no deadlock.
  int pending_lease_creations = 0;
  int pending_leases = 0;
  ASSERT_EQ(lease_manager_.AnyPendingLeasesForResourceAcquisition(
                &pending_lease_creations, &pending_leases),
            nullptr);
  ASSERT_EQ(pending_lease_creations, 0);
  ASSERT_EQ(pending_leases, 0);

  // lease1: running, lease2: queued.
  RayLease lease2 = CreateLease({{ray::kCPU_ResourceLabel, 6}});
  rpc::RequestWorkerLeaseReply reply2;
  std::shared_ptr<bool> callback_occurred2 = std::make_shared<bool>(false);
  auto callback2 = [callback_occurred2](
                       Status, std::function<void()>, std::function<void()>) {
    *callback_occurred2 = true;
  };
  lease_manager_.QueueAndScheduleLease(
      lease2,
      false,
      false,
      std::vector<internal::ReplyCallback>{internal::ReplyCallback(callback2, &reply2)});
  pool_.TriggerCallbacks();
  ASSERT_FALSE(*callback_occurred2);
  auto pending_lease = lease_manager_.AnyPendingLeasesForResourceAcquisition(
      &pending_lease_creations, &pending_leases);
  ASSERT_EQ(pending_lease->GetLeaseSpecification().LeaseId(),
            lease2.GetLeaseSpecification().LeaseId());
  ASSERT_EQ(pending_lease_creations, 0);
  ASSERT_EQ(pending_leases, 1);
}

TEST_F(ClusterLeaseManagerTest, ArgumentEvicted) {
  /*
    Test the lease's dependencies becoming local, then one of the arguments is
    evicted. The lease should go from waiting -> dispatch -> waiting.
  */
  std::shared_ptr<MockWorker> worker =
      std::make_shared<MockWorker>(WorkerID::FromRandom(), 1234);
  pool_.PushWorker(std::static_pointer_cast<WorkerInterface>(worker));

  rpc::RequestWorkerLeaseReply reply;
  int num_callbacks = 0;
  int *num_callbacks_ptr = &num_callbacks;
  auto callback = [num_callbacks_ptr](
                      Status, std::function<void()>, std::function<void()>) {
    (*num_callbacks_ptr) = *num_callbacks_ptr + 1;
  };

  /* Blocked on dependencies */
  auto lease = CreateLease({{ray::kCPU_ResourceLabel, 5}}, 2);
  auto missing_arg = lease.GetLeaseSpecification().GetDependencyIds()[0];
  missing_objects_.insert(missing_arg);
  std::unordered_set<LeaseID> expected_subscribed_leases = {
      lease.GetLeaseSpecification().LeaseId()};
  lease_manager_.QueueAndScheduleLease(
      lease,
      false,
      false,
      std::vector<internal::ReplyCallback>{internal::ReplyCallback(callback, &reply)});
  pool_.TriggerCallbacks();
  ASSERT_EQ(lease_dependency_manager_.subscribed_leases, expected_subscribed_leases);
  ASSERT_EQ(num_callbacks, 0);
  ASSERT_EQ(leased_workers_.size(), 0);

  /* RayLease is unblocked now */
  missing_objects_.erase(missing_arg);
  pool_.workers.clear();
  auto id = lease.GetLeaseSpecification().LeaseId();
  local_lease_manager_->LeasesUnblocked({id});
  ASSERT_EQ(lease_dependency_manager_.subscribed_leases, expected_subscribed_leases);
  ASSERT_EQ(num_callbacks, 0);
  ASSERT_EQ(leased_workers_.size(), 0);

  /* Worker available and arguments available */
  pool_.PushWorker(std::static_pointer_cast<WorkerInterface>(worker));
  lease_manager_.ScheduleAndGrantLeases();
  pool_.TriggerCallbacks();
  ASSERT_EQ(num_callbacks, 1);
  ASSERT_EQ(leased_workers_.size(), 1);

  RayLease finished_lease;
  local_lease_manager_->CleanupLease(leased_workers_.begin()->second, &finished_lease);
  ASSERT_EQ(finished_lease.GetLeaseSpecification().LeaseId(),
            lease.GetLeaseSpecification().LeaseId());

  AssertNoLeaks();
}

TEST_F(ClusterLeaseManagerTest, FeasibleToNonFeasible) {
  // Test the case, when resources changes in local node, the feasible lease should
  // able to transfer to infeasible lease
  std::shared_ptr<MockWorker> worker =
      std::make_shared<MockWorker>(WorkerID::FromRandom(), 1234);
  pool_.PushWorker(std::static_pointer_cast<WorkerInterface>(worker));
  RayLease lease1 = CreateLease({{ray::kCPU_ResourceLabel, 4}});
  rpc::RequestWorkerLeaseReply reply1;
  bool callback_occurred1 = false;
  auto callback1 = [&callback_occurred1](
                       Status, std::function<void()>, std::function<void()>) {
    callback_occurred1 = true;
  };
  lease_manager_.QueueAndScheduleLease(
      lease1,
      false,
      false,
      std::vector<internal::ReplyCallback>{internal::ReplyCallback(callback1, &reply1)});
  pool_.TriggerCallbacks();
  ASSERT_EQ(leased_workers_.size(), 1);
  ASSERT_TRUE(callback_occurred1);
  ASSERT_EQ(pool_.workers.size(), 0);
  ASSERT_EQ(lease_manager_.leases_to_schedule_.size(), 0);
  ASSERT_EQ(local_lease_manager_->leases_to_grant_.size(), 0);
  ASSERT_EQ(lease_manager_.infeasible_leases_.size(), 0);

  // Delete cpu resource of local node, then lease 2 should be turned into
  // infeasible.
  scheduler_->GetLocalResourceManager().DeleteLocalResource(
      scheduling::ResourceID(ray::kCPU_ResourceLabel));

  RayLease lease2 = CreateLease({{ray::kCPU_ResourceLabel, 4}});
  rpc::RequestWorkerLeaseReply reply2;
  bool callback_occurred2 = false;
  auto callback2 = [&callback_occurred2](
                       Status, std::function<void()>, std::function<void()>) {
    callback_occurred2 = true;
  };
  lease_manager_.QueueAndScheduleLease(
      lease2,
      false,
      false,
      std::vector<internal::ReplyCallback>{internal::ReplyCallback(callback2, &reply2)});
  pool_.TriggerCallbacks();
  ASSERT_EQ(leased_workers_.size(), 1);
  ASSERT_FALSE(callback_occurred2);
  ASSERT_EQ(pool_.workers.size(), 0);
  ASSERT_EQ(lease_manager_.leases_to_schedule_.size(), 0);
  ASSERT_EQ(local_lease_manager_->leases_to_grant_.size(), 0);
  ASSERT_EQ(local_lease_manager_->waiting_lease_queue_.size(), 0);
  ASSERT_EQ(lease_manager_.infeasible_leases_.size(), 1);

  RayLease finished_lease;
  local_lease_manager_->CleanupLease(leased_workers_.begin()->second, &finished_lease);
  ASSERT_EQ(finished_lease.GetLeaseSpecification().LeaseId(),
            lease1.GetLeaseSpecification().LeaseId());
}

TEST_F(ClusterLeaseManagerTest, NegativePlacementGroupCpuResources) {
  // Add PG CPU resources.
  scheduler_->GetLocalResourceManager().AddLocalResourceInstances(
      scheduling::ResourceID("CPU_group_aaa"), std::vector<FixedPoint>{FixedPoint(2)});
  scheduler_->GetLocalResourceManager().AddLocalResourceInstances(
      scheduling::ResourceID("CPU_group_0_aaa"), std::vector<FixedPoint>{FixedPoint(1)});
  scheduler_->GetLocalResourceManager().AddLocalResourceInstances(
      scheduling::ResourceID("CPU_group_1_aaa"), std::vector<FixedPoint>{FixedPoint(1)});

  const NodeResources &node_resources =
      scheduler_->GetClusterResourceManager().GetNodeResources(
          scheduling::NodeID(id_.Binary()));

  auto worker1 = std::make_shared<MockWorker>(WorkerID::FromRandom(), 1234);
  auto allocated_instances = std::make_shared<TaskResourceInstances>();
  ASSERT_TRUE(scheduler_->GetLocalResourceManager().AllocateLocalTaskResources(
      {{"CPU_group_aaa", 1.}, {"CPU_group_0_aaa", 1.}}, allocated_instances));
  worker1->SetAllocatedInstances(allocated_instances);
  // worker1 calls ray.get() and release the CPU resource
  ASSERT_TRUE(local_lease_manager_->ReleaseCpuResourcesFromBlockedWorker(worker1));

  // the released CPU resource is acquired by worker2
  auto worker2 = std::make_shared<MockWorker>(WorkerID::FromRandom(), 5678);
  allocated_instances = std::make_shared<TaskResourceInstances>();
  ASSERT_TRUE(scheduler_->GetLocalResourceManager().AllocateLocalTaskResources(
      {{"CPU_group_aaa", 1.}, {"CPU_group_0_aaa", 1.}}, allocated_instances));
  worker2->SetAllocatedInstances(allocated_instances);

  // ray.get() returns and worker1 acquires the CPU resource again
  ASSERT_TRUE(local_lease_manager_->ReturnCpuResourcesToUnblockedWorker(worker1));
  ASSERT_EQ(node_resources.available.Get(scheduling::ResourceID("CPU_group_aaa")), 0);
  ASSERT_EQ(node_resources.available.Get(scheduling::ResourceID("CPU_group_0_aaa")), -1);
  ASSERT_EQ(node_resources.available.Get(scheduling::ResourceID("CPU_group_1_aaa")), 1);

  auto worker3 = std::make_shared<MockWorker>(WorkerID::FromRandom(), 7678);
  allocated_instances = std::make_shared<TaskResourceInstances>();
  ASSERT_TRUE(scheduler_->GetLocalResourceManager().AllocateLocalTaskResources(
      {{"CPU_group_aaa", 1.}, {"CPU_group_1_aaa", 1.}}, allocated_instances));
  worker3->SetAllocatedInstances(allocated_instances);
  ASSERT_EQ(node_resources.available.Get(scheduling::ResourceID("CPU_group_aaa")), -1);
  ASSERT_EQ(node_resources.available.Get(scheduling::ResourceID("CPU_group_0_aaa")), -1);
  ASSERT_EQ(node_resources.available.Get(scheduling::ResourceID("CPU_group_1_aaa")), 0);
}

TEST_F(ClusterLeaseManagerTestWithGPUsAtHead, ReleaseAndReturnWorkerCpuResources) {
  // Add PG CPU and GPU resources.
  scheduler_->GetLocalResourceManager().AddLocalResourceInstances(
      scheduling::ResourceID("CPU_group_aaa"), std::vector<FixedPoint>{FixedPoint(1)});
  scheduler_->GetLocalResourceManager().AddLocalResourceInstances(
      scheduling::ResourceID("CPU_group_0_aaa"), std::vector<FixedPoint>{FixedPoint(1)});
  scheduler_->GetLocalResourceManager().AddLocalResourceInstances(
      scheduling::ResourceID("GPU_group_aaa"), std::vector<FixedPoint>{FixedPoint(1)});
  scheduler_->GetLocalResourceManager().AddLocalResourceInstances(
      scheduling::ResourceID("GPU_group_0_aaa"), std::vector<FixedPoint>{FixedPoint(1)});

  const NodeResources &node_resources =
      scheduler_->GetClusterResourceManager().GetNodeResources(
          scheduling::NodeID(id_.Binary()));
  ASSERT_EQ(node_resources.available.Get(ResourceID::CPU()), 8);
  ASSERT_EQ(node_resources.available.Get(ResourceID::GPU()), 4);

  auto worker1 = std::make_shared<MockWorker>(WorkerID::FromRandom(), 1234);
  auto worker2 = std::make_shared<MockWorker>(WorkerID::FromRandom(), 5678);

  // Check failed as the worker has no allocated resource instances.
  ASSERT_FALSE(local_lease_manager_->ReleaseCpuResourcesFromBlockedWorker(worker1));
  ASSERT_FALSE(local_lease_manager_->ReleaseCpuResourcesFromBlockedWorker(worker2));

  auto node_resource_instances =
      scheduler_->GetLocalResourceManager().GetLocalResources();
  auto available_resource_instances =
      node_resource_instances.GetAvailableResourceInstances();

  auto allocated_instances = std::make_shared<TaskResourceInstances>();
  absl::flat_hash_map<std::string, double> lease_spec = {{"CPU", 1.}, {"GPU", 1.}};
  ASSERT_TRUE(scheduler_->GetLocalResourceManager().AllocateLocalTaskResources(
      lease_spec, allocated_instances));
  worker1->SetAllocatedInstances(allocated_instances);

  allocated_instances = std::make_shared<TaskResourceInstances>();
  lease_spec = {{"CPU_group_aaa", 1.},
                {"CPU_group_0_aaa", 1.},
                {"GPU_group_aaa", 1.},
                {"GPU_group_0_aaa", 1.}};
  ASSERT_TRUE(scheduler_->GetLocalResourceManager().AllocateLocalTaskResources(
      lease_spec, allocated_instances));
  worker2->SetAllocatedInstances(allocated_instances);

  // Check that the resources are allocated successfully.
  ASSERT_EQ(node_resources.available.Get(ResourceID::CPU()), 7);
  ASSERT_EQ(node_resources.available.Get(ResourceID::GPU()), 3);
  ASSERT_EQ(node_resources.available.Get(scheduling::ResourceID("CPU_group_aaa")), 0);
  ASSERT_EQ(node_resources.available.Get(scheduling::ResourceID("CPU_group_0_aaa")), 0);
  ASSERT_EQ(node_resources.available.Get(scheduling::ResourceID("GPU_group_aaa")), 0);
  ASSERT_EQ(node_resources.available.Get(scheduling::ResourceID("GPU_group_0_aaa")), 0);

  // Check that the cpu resources are released successfully.
  ASSERT_TRUE(local_lease_manager_->ReleaseCpuResourcesFromBlockedWorker(worker1));
  ASSERT_TRUE(local_lease_manager_->ReleaseCpuResourcesFromBlockedWorker(worker2));

  // Check that only cpu resources are released.
  ASSERT_EQ(node_resources.available.Get(ResourceID::CPU()), 8);
  ASSERT_EQ(node_resources.available.Get(ResourceID::GPU()), 3);
  ASSERT_EQ(node_resources.available.Get(scheduling::ResourceID("CPU_group_aaa")), 1);
  ASSERT_EQ(node_resources.available.Get(scheduling::ResourceID("CPU_group_0_aaa")), 1);
  ASSERT_EQ(node_resources.available.Get(scheduling::ResourceID("GPU_group_aaa")), 0);
  ASSERT_EQ(node_resources.available.Get(scheduling::ResourceID("GPU_group_0_aaa")), 0);

  // Mark worker as blocked.
  worker1->MarkBlocked();
  worker2->MarkBlocked();
  // Check failed as the worker is blocked.
  ASSERT_FALSE(local_lease_manager_->ReleaseCpuResourcesFromBlockedWorker(worker1));
  ASSERT_FALSE(local_lease_manager_->ReleaseCpuResourcesFromBlockedWorker(worker2));
  // Check nothing will be changed.
  ASSERT_EQ(node_resources.available.Get(ResourceID::CPU()), 8);
  ASSERT_EQ(node_resources.available.Get(ResourceID::GPU()), 3);
  ASSERT_EQ(node_resources.available.Get(scheduling::ResourceID("CPU_group_aaa")), 1);
  ASSERT_EQ(node_resources.available.Get(scheduling::ResourceID("CPU_group_0_aaa")), 1);
  ASSERT_EQ(node_resources.available.Get(scheduling::ResourceID("GPU_group_aaa")), 0);
  ASSERT_EQ(node_resources.available.Get(scheduling::ResourceID("GPU_group_0_aaa")), 0);

  // Check that the cpu resources are returned back to worker successfully.
  ASSERT_TRUE(local_lease_manager_->ReturnCpuResourcesToUnblockedWorker(worker1));
  ASSERT_TRUE(local_lease_manager_->ReturnCpuResourcesToUnblockedWorker(worker2));

  // Check that only cpu resources are returned back to the worker.
  ASSERT_EQ(node_resources.available.Get(ResourceID::CPU()), 7);
  ASSERT_EQ(node_resources.available.Get(ResourceID::GPU()), 3);
  ASSERT_EQ(node_resources.available.Get(scheduling::ResourceID("CPU_group_aaa")), 0);
  ASSERT_EQ(node_resources.available.Get(scheduling::ResourceID("CPU_group_0_aaa")), 0);
  ASSERT_EQ(node_resources.available.Get(scheduling::ResourceID("GPU_group_aaa")), 0);
  ASSERT_EQ(node_resources.available.Get(scheduling::ResourceID("GPU_group_0_aaa")), 0);

  // Mark worker as unblocked.
  worker1->MarkUnblocked();
  worker2->MarkUnblocked();
  ASSERT_FALSE(local_lease_manager_->ReturnCpuResourcesToUnblockedWorker(worker1));
  ASSERT_FALSE(local_lease_manager_->ReturnCpuResourcesToUnblockedWorker(worker2));
  // Check nothing will be changed.
  ASSERT_EQ(node_resources.available.Get(ResourceID::CPU()), 7);
  ASSERT_EQ(node_resources.available.Get(ResourceID::GPU()), 3);
  ASSERT_EQ(node_resources.available.Get(scheduling::ResourceID("CPU_group_aaa")), 0);
  ASSERT_EQ(node_resources.available.Get(scheduling::ResourceID("CPU_group_0_aaa")), 0);
  ASSERT_EQ(node_resources.available.Get(scheduling::ResourceID("GPU_group_aaa")), 0);
  ASSERT_EQ(node_resources.available.Get(scheduling::ResourceID("GPU_group_0_aaa")), 0);
}

TEST_F(ClusterLeaseManagerTest, TestSpillWaitingLeases) {
  // Cases to check:
  // - resources available locally, lease dependencies being fetched -> do not spill.
  // - resources available locally, lease dependencies blocked -> spill.
  // - resources not available locally -> spill.
  std::vector<RayLease> leases;
  std::vector<std::unique_ptr<rpc::RequestWorkerLeaseReply>> replies;
  int num_callbacks = 0;
  auto callback = [&](Status, std::function<void()>, std::function<void()>) {
    num_callbacks++;
  };
  for (int i = 0; i < 5; i++) {
    RayLease lease = CreateLease({{ray::kCPU_ResourceLabel, 8}}, /*num_args=*/1);
    leases.push_back(lease);
    replies.push_back(std::make_unique<rpc::RequestWorkerLeaseReply>());
    // All leases except the last one added are waiting for dependencies.
    if (i < 4) {
      auto missing_arg = lease.GetLeaseSpecification().GetDependencyIds()[0];
      missing_objects_.insert(missing_arg);
    }
    if (i == 0) {
      const_cast<LeaseSpecification &>(lease.GetLeaseSpecification())
          .GetMutableMessage()
          .mutable_scheduling_strategy()
          ->mutable_spread_scheduling_strategy();
    }
    lease_manager_.QueueAndScheduleLease(
        lease,
        false,
        false,
        std::vector<internal::ReplyCallback>{
            internal::ReplyCallback(callback, replies[i].get())});
    pool_.TriggerCallbacks();
  }
  ASSERT_EQ(num_callbacks, 0);
  // Local resources could only dispatch one lease.
  ASSERT_EQ(NumLeasesToDispatchWithStatus(internal::WorkStatus::WAITING_FOR_WORKER), 1);

  auto remote_node_id = NodeID::FromRandom();
  AddNode(remote_node_id, 16);
  // We are fetching dependencies for all waiting leases but we have no enough
  // resources available locally to schedule leases except the first.
  // We should only spill up to the remote node's resource availability.
  lease_manager_.ScheduleAndGrantLeases();
  ASSERT_EQ(num_callbacks, 2);
  // Spill from the back of the waiting queue.
  ASSERT_EQ(replies[0]->retry_at_raylet_address().node_id(), "");
  ASSERT_EQ(replies[1]->retry_at_raylet_address().node_id(), "");
  ASSERT_EQ(replies[2]->retry_at_raylet_address().node_id(), remote_node_id.Binary());
  ASSERT_EQ(replies[3]->retry_at_raylet_address().node_id(), remote_node_id.Binary());
  ASSERT_FALSE(lease_manager_.CancelLease(leases[2].GetLeaseSpecification().LeaseId()));
  ASSERT_FALSE(lease_manager_.CancelLease(leases[3].GetLeaseSpecification().LeaseId()));
  // Do not spill back leases ready to dispatch.
  ASSERT_EQ(replies[4]->retry_at_raylet_address().node_id(), "");

  AddNode(remote_node_id, 8);
  // Dispatch the ready lease.
  std::shared_ptr<MockWorker> worker =
      std::make_shared<MockWorker>(WorkerID::FromRandom(), 1234);
  pool_.PushWorker(std::dynamic_pointer_cast<WorkerInterface>(worker));
  lease_manager_.ScheduleAndGrantLeases();
  pool_.TriggerCallbacks();
  ASSERT_EQ(num_callbacks, 4);
  // One waiting lease spilled.
  ASSERT_EQ(replies[0]->retry_at_raylet_address().node_id(), "");
  ASSERT_EQ(replies[1]->retry_at_raylet_address().node_id(), remote_node_id.Binary());
  ASSERT_FALSE(lease_manager_.CancelLease(leases[1].GetLeaseSpecification().LeaseId()));
  // One lease dispatched.
  ASSERT_EQ(replies[4]->worker_address().port(), 1234);

  // Spillback is idempotent.
  lease_manager_.ScheduleAndGrantLeases();
  pool_.TriggerCallbacks();
  ASSERT_EQ(num_callbacks, 4);
  // One waiting lease spilled.
  ASSERT_EQ(replies[0]->retry_at_raylet_address().node_id(), "");
  ASSERT_EQ(replies[1]->retry_at_raylet_address().node_id(), remote_node_id.Binary());
  ASSERT_FALSE(lease_manager_.CancelLease(leases[1].GetLeaseSpecification().LeaseId()));
  // One lease dispatched.
  ASSERT_EQ(replies[4]->worker_address().port(), 1234);

  // Spread lease won't be spilled due to waiting for dependencies.
  AddNode(remote_node_id, 8);
  lease_manager_.ScheduleAndGrantLeases();
  ASSERT_EQ(num_callbacks, 4);
  ASSERT_EQ(replies[0]->retry_at_raylet_address().node_id(), "");

  RayLease finished_lease;
  local_lease_manager_->CleanupLease(leased_workers_.begin()->second, &finished_lease);
  leased_workers_.clear();
  ASSERT_TRUE(lease_manager_.CancelLease(leases[0].GetLeaseSpecification().LeaseId()));
  AssertNoLeaks();
}

TEST_F(ClusterLeaseManagerTest, PinnedArgsMemoryTest) {
  /*
    Total memory required by granted lease args stays under the specified
    threshold.
  */
  auto worker_id1 = WorkerID::FromRandom();
  auto worker_id2 = WorkerID::FromRandom();
  std::shared_ptr<MockWorker> worker = std::make_shared<MockWorker>(worker_id1, 1234);
  std::shared_ptr<MockWorker> worker2 = std::make_shared<MockWorker>(worker_id2, 12345);
  pool_.PushWorker(std::static_pointer_cast<WorkerInterface>(worker2));
  pool_.PushWorker(std::static_pointer_cast<WorkerInterface>(worker));

  rpc::RequestWorkerLeaseReply reply;
  int num_callbacks = 0;
  int *num_callbacks_ptr = &num_callbacks;
  auto callback = [num_callbacks_ptr](
                      Status, std::function<void()>, std::function<void()>) {
    (*num_callbacks_ptr) = *num_callbacks_ptr + 1;
  };

  // This lease can run.
  auto lease_id1 = LeaseID::FromWorker(worker_id1, 1);
  default_arg_size_ = 600;
  auto lease1 = CreateLease({{ray::kCPU_ResourceLabel, 1}},
                            1,
                            {},
                            nullptr,
                            rpc::SchedulingStrategy(),
                            lease_id1);
  lease_manager_.QueueAndScheduleLease(
      lease1,
      false,
      false,
      std::vector<internal::ReplyCallback>{internal::ReplyCallback(callback, &reply)});
  pool_.TriggerCallbacks();
  ASSERT_EQ(num_callbacks, 1);
  ASSERT_EQ(leased_workers_.size(), 1);
  ASSERT_EQ(pool_.workers.size(), 1);
  AssertPinnedLeaseArgumentsPresent(lease1);

  // This lease cannot run because it would put us over the memory threshold.
  auto lease_id2 = LeaseID::FromWorker(worker_id2, 1);
  auto lease2 = CreateLease({{ray::kCPU_ResourceLabel, 1}},
                            1,
                            {},
                            nullptr,
                            rpc::SchedulingStrategy(),
                            lease_id2);
  lease_manager_.QueueAndScheduleLease(
      lease2,
      false,
      false,
      std::vector<internal::ReplyCallback>{internal::ReplyCallback(callback, &reply)});
  pool_.TriggerCallbacks();
  ASSERT_EQ(num_callbacks, 1);
  ASSERT_EQ(leased_workers_.size(), 1);
  ASSERT_EQ(pool_.workers.size(), 1);

  /* First lease finishes, freeing memory for the second lease */
  RayLease finished_lease;
  local_lease_manager_->CleanupLease(leased_workers_.begin()->second, &finished_lease);
  leased_workers_.clear();

  lease_manager_.ScheduleAndGrantLeases();
  pool_.TriggerCallbacks();
  AssertPinnedLeaseArgumentsPresent(lease2);
  ASSERT_EQ(num_callbacks, 2);
  ASSERT_EQ(leased_workers_.size(), 1);
  ASSERT_EQ(pool_.workers.size(), 0);

  local_lease_manager_->CleanupLease(leased_workers_.begin()->second, &finished_lease);
  leased_workers_.clear();
  AssertNoLeaks();
}

TEST_F(ClusterLeaseManagerTest, PinnedArgsSameMemoryTest) {
  /*
   * Two leases that depend on the same object can run concurrently.
   */
  std::shared_ptr<MockWorker> worker =
      std::make_shared<MockWorker>(WorkerID::FromRandom(), 1234);
  std::shared_ptr<MockWorker> worker2 =
      std::make_shared<MockWorker>(WorkerID::FromRandom(), 12345);
  pool_.PushWorker(std::static_pointer_cast<WorkerInterface>(worker2));
  pool_.PushWorker(std::static_pointer_cast<WorkerInterface>(worker));

  rpc::RequestWorkerLeaseReply reply;
  int num_callbacks = 0;
  int *num_callbacks_ptr = &num_callbacks;
  auto callback = [num_callbacks_ptr](
                      Status, std::function<void()>, std::function<void()>) {
    (*num_callbacks_ptr) = *num_callbacks_ptr + 1;
  };

  // This lease can run.
  default_arg_size_ = 600;
  auto lease = CreateLease({{ray::kCPU_ResourceLabel, 1}}, 1);
  lease_manager_.QueueAndScheduleLease(
      lease,
      false,
      false,
      std::vector<internal::ReplyCallback>{internal::ReplyCallback(callback, &reply)});
  pool_.TriggerCallbacks();
  ASSERT_EQ(num_callbacks, 1);
  ASSERT_EQ(leased_workers_.size(), 1);
  ASSERT_EQ(pool_.workers.size(), 1);
  AssertPinnedLeaseArgumentsPresent(lease);

  // This lease can run because it depends on the same object as the first lease.
  auto lease2 = CreateLease({{ray::kCPU_ResourceLabel, 1}},
                            1,
                            lease.GetLeaseSpecification().GetDependencyIds());
  lease_manager_.QueueAndScheduleLease(
      lease2,
      false,
      false,
      std::vector<internal::ReplyCallback>{internal::ReplyCallback(callback, &reply)});
  pool_.TriggerCallbacks();
  ASSERT_EQ(num_callbacks, 2);
  ASSERT_EQ(leased_workers_.size(), 2);
  ASSERT_EQ(pool_.workers.size(), 0);

  RayLease finished_lease;
  for (auto &cur_worker : leased_workers_) {
    local_lease_manager_->CleanupLease(cur_worker.second, &finished_lease);
  }
  AssertNoLeaks();
}

TEST_F(ClusterLeaseManagerTest, LargeArgsNoStarvationTest) {
  std::shared_ptr<MockWorker> worker =
      std::make_shared<MockWorker>(WorkerID::FromRandom(), 1234);
  pool_.PushWorker(std::static_pointer_cast<WorkerInterface>(worker));

  rpc::RequestWorkerLeaseReply reply;
  int num_callbacks = 0;
  int *num_callbacks_ptr = &num_callbacks;
  auto callback = [num_callbacks_ptr](
                      Status, std::function<void()>, std::function<void()>) {
    (*num_callbacks_ptr) = *num_callbacks_ptr + 1;
  };

  default_arg_size_ = 2000;
  auto lease = CreateLease({{ray::kCPU_ResourceLabel, 1}}, 1);
  pool_.PushWorker(std::static_pointer_cast<WorkerInterface>(worker));
  lease_manager_.QueueAndScheduleLease(
      lease,
      false,
      false,
      std::vector<internal::ReplyCallback>{internal::ReplyCallback(callback, &reply)});
  pool_.TriggerCallbacks();
  ASSERT_EQ(num_callbacks, 1);
  ASSERT_EQ(leased_workers_.size(), 1);
  AssertPinnedLeaseArgumentsPresent(lease);

  RayLease finished_lease;
  local_lease_manager_->CleanupLease(leased_workers_.begin()->second, &finished_lease);
  AssertNoLeaks();
}

TEST_F(ClusterLeaseManagerTest, PopWorkerExactlyOnce) {
  // Create and queue one lease.
  std::string serialized_runtime_env = "mock_env";
  std::shared_ptr<rpc::RuntimeEnvInfo> runtime_env_info = nullptr;
  runtime_env_info.reset(new rpc::RuntimeEnvInfo());
  runtime_env_info->set_serialized_runtime_env(serialized_runtime_env);

  RayLease lease = CreateLease(
      {{ray::kCPU_ResourceLabel, 4}}, /*num_args=*/0, /*args=*/{}, runtime_env_info);
  auto runtime_env_hash = lease.GetLeaseSpecification().GetRuntimeEnvHash();
  rpc::RequestWorkerLeaseReply reply;
  bool callback_occurred = false;
  bool *callback_occurred_ptr = &callback_occurred;
  auto callback = [callback_occurred_ptr](
                      Status, std::function<void()>, std::function<void()>) {
    *callback_occurred_ptr = true;
  };

  lease_manager_.QueueAndScheduleLease(
      lease,
      false,
      false,
      std::vector<internal::ReplyCallback>{internal::ReplyCallback(callback, &reply)});

  // Make sure callback doesn't occurred.
  ASSERT_FALSE(callback_occurred);
  ASSERT_EQ(leased_workers_.size(), 0);
  ASSERT_EQ(pool_.workers.size(), 0);
  // Popworker was called once.
  ASSERT_EQ(pool_.CallbackSize(runtime_env_hash), 1);
  // Try to schedule and dispatch leases.
  lease_manager_.ScheduleAndGrantLeases();
  // Popworker has been called once, don't call it repeatedly.
  ASSERT_EQ(pool_.CallbackSize(runtime_env_hash), 1);
  // Push a worker and try to call back.
  std::shared_ptr<MockWorker> worker =
      std::make_shared<MockWorker>(WorkerID::FromRandom(), 1234, runtime_env_hash);
  pool_.PushWorker(std::static_pointer_cast<WorkerInterface>(worker));
  pool_.TriggerCallbacks();
  // Make sure callback has occurred.
  ASSERT_TRUE(callback_occurred);
  ASSERT_EQ(leased_workers_.size(), 1);
  ASSERT_EQ(pool_.workers.size(), 0);
  // Try to schedule and dispatch leases.
  lease_manager_.ScheduleAndGrantLeases();
  // Worker has been popped. Don't call `PopWorker` repeatedly.
  ASSERT_EQ(pool_.CallbackSize(runtime_env_hash), 0);

  RayLease finished_lease;
  local_lease_manager_->CleanupLease(leased_workers_.begin()->second, &finished_lease);
  ASSERT_EQ(finished_lease.GetLeaseSpecification().LeaseId(),
            lease.GetLeaseSpecification().LeaseId());
  AssertNoLeaks();
}

TEST_F(ClusterLeaseManagerTest, CapRunningOnDispatchQueue) {
  scheduler_->GetLocalResourceManager().AddLocalResourceInstances(
      scheduling::ResourceID(ray::kGPU_ResourceLabel), {1, 1, 1});
  RayLease lease =
      CreateLease({{ray::kCPU_ResourceLabel, 4}, {ray::kGPU_ResourceLabel, 1}},
                  /*num_args=*/0,
                  /*args=*/{});
  RayLease lease2 =
      CreateLease({{ray::kCPU_ResourceLabel, 4}, {ray::kGPU_ResourceLabel, 1}},
                  /*num_args=*/0,
                  /*args=*/{});
  RayLease lease3 =
      CreateLease({{ray::kCPU_ResourceLabel, 4}, {ray::kGPU_ResourceLabel, 1}},
                  /*num_args=*/0,
                  /*args=*/{});
  auto runtime_env_hash = lease.GetLeaseSpecification().GetRuntimeEnvHash();
  std::vector<std::shared_ptr<MockWorker>> workers;
  for (int i = 0; i < 3; i++) {
    std::shared_ptr<MockWorker> worker =
        std::make_shared<MockWorker>(WorkerID::FromRandom(), 1234, runtime_env_hash);
    pool_.PushWorker(std::static_pointer_cast<WorkerInterface>(worker));
    pool_.TriggerCallbacks();
    workers.push_back(worker);
  }
  rpc::RequestWorkerLeaseReply reply;
  int num_callbacks = 0;
  auto callback = [&num_callbacks](Status, std::function<void()>, std::function<void()>) {
    num_callbacks++;
  };
  lease_manager_.QueueAndScheduleLease(
      lease,
      false,
      false,
      std::vector<internal::ReplyCallback>{internal::ReplyCallback(callback, &reply)});
  lease_manager_.QueueAndScheduleLease(
      lease2,
      false,
      false,
      std::vector<internal::ReplyCallback>{internal::ReplyCallback(callback, &reply)});
  lease_manager_.QueueAndScheduleLease(
      lease3,
      false,
      false,
      std::vector<internal::ReplyCallback>{internal::ReplyCallback(callback, &reply)});
  pool_.TriggerCallbacks();

  ASSERT_EQ(num_callbacks, 2);

  local_lease_manager_->ReleaseCpuResourcesFromBlockedWorker(workers[0]);
  local_lease_manager_->ReleaseCpuResourcesFromBlockedWorker(workers[1]);

  lease_manager_.ScheduleAndGrantLeases();

  // Even though there are free resources, we've hit our cap of (8/4=)2 workers
  // of the given scheduling class so we shouldn't dispatch the remaining lease.
  ASSERT_EQ(num_callbacks, 2);

  RayLease buf;
  local_lease_manager_->CleanupLease(workers[1], &buf);

  lease_manager_.ScheduleAndGrantLeases();
  pool_.TriggerCallbacks();
  ASSERT_EQ(num_callbacks, 3);

  local_lease_manager_->CleanupLease(workers[0], &buf);
  local_lease_manager_->CleanupLease(workers[2], &buf);

  AssertNoLeaks();
}

TEST_F(ClusterLeaseManagerTest, ZeroCPULeases) {
  scheduler_->GetLocalResourceManager().AddLocalResourceInstances(
      scheduling::ResourceID(ray::kGPU_ResourceLabel), {1, 1, 1});
  RayLease lease = CreateLease({{"GPU", 1}}, /*num_args=*/0, /*args=*/{});
  RayLease lease2 = CreateLease({{"GPU", 1}}, /*num_args=*/0, /*args=*/{});
  RayLease lease3 = CreateLease({{"GPU", 1}}, /*num_args=*/0, /*args=*/{});
  auto runtime_env_hash = lease.GetLeaseSpecification().GetRuntimeEnvHash();
  std::vector<std::shared_ptr<MockWorker>> workers;
  for (int i = 0; i < 3; i++) {
    std::shared_ptr<MockWorker> worker =
        std::make_shared<MockWorker>(WorkerID::FromRandom(), 1234, runtime_env_hash);
    pool_.PushWorker(std::static_pointer_cast<WorkerInterface>(worker));
    pool_.TriggerCallbacks();
    workers.push_back(worker);
  }
  rpc::RequestWorkerLeaseReply reply;
  int num_callbacks = 0;
  auto callback = [&num_callbacks](Status, std::function<void()>, std::function<void()>) {
    num_callbacks++;
  };
  lease_manager_.QueueAndScheduleLease(
      lease,
      false,
      false,
      std::vector<internal::ReplyCallback>{internal::ReplyCallback(callback, &reply)});
  lease_manager_.QueueAndScheduleLease(
      lease2,
      false,
      false,
      std::vector<internal::ReplyCallback>{internal::ReplyCallback(callback, &reply)});
  lease_manager_.QueueAndScheduleLease(
      lease3,
      false,
      false,
      std::vector<internal::ReplyCallback>{internal::ReplyCallback(callback, &reply)});
  pool_.TriggerCallbacks();

  // We shouldn't cap anything for zero cpu leases (and shouldn't crash before
  // this point).
  ASSERT_EQ(num_callbacks, 3);

  for (auto &worker : workers) {
    RayLease buf;
    local_lease_manager_->CleanupLease(worker, &buf);
  }

  AssertNoLeaks();
}

TEST_F(ClusterLeaseManagerTestWithoutCPUsAtHead, ZeroCPUNode) {
  RayLease lease = CreateLease({}, /*num_args=*/0, /*args=*/{});
  RayLease lease2 = CreateLease({}, /*num_args=*/0, /*args=*/{});
  RayLease lease3 = CreateLease({}, /*num_args=*/0, /*args=*/{});
  auto runtime_env_hash = lease.GetLeaseSpecification().GetRuntimeEnvHash();
  std::vector<std::shared_ptr<MockWorker>> workers;
  for (int i = 0; i < 3; i++) {
    std::shared_ptr<MockWorker> worker =
        std::make_shared<MockWorker>(WorkerID::FromRandom(), 1234, runtime_env_hash);
    pool_.PushWorker(std::static_pointer_cast<WorkerInterface>(worker));
    pool_.TriggerCallbacks();
    workers.push_back(worker);
  }
  rpc::RequestWorkerLeaseReply reply;
  int num_callbacks = 0;
  auto callback = [&num_callbacks](Status, std::function<void()>, std::function<void()>) {
    num_callbacks++;
  };
  lease_manager_.QueueAndScheduleLease(
      lease,
      false,
      false,
      std::vector<internal::ReplyCallback>{internal::ReplyCallback(callback, &reply)});
  lease_manager_.QueueAndScheduleLease(
      lease2,
      false,
      false,
      std::vector<internal::ReplyCallback>{internal::ReplyCallback(callback, &reply)});
  lease_manager_.QueueAndScheduleLease(
      lease3,
      false,
      false,
      std::vector<internal::ReplyCallback>{internal::ReplyCallback(callback, &reply)});
  pool_.TriggerCallbacks();

  // We shouldn't cap anything for zero cpu leases (and shouldn't crash before
  // this point).
  ASSERT_EQ(num_callbacks, 3);

  for (auto &worker : workers) {
    RayLease buf;
    local_lease_manager_->CleanupLease(worker, &buf);
  }
  AssertNoLeaks();
}

/// Test that we are able to spillback leases
/// while hitting the scheduling class cap.
TEST_F(ClusterLeaseManagerTest, SchedulingClassCapSpillback) {
  std::shared_ptr<MockWorker> worker =
      std::make_shared<MockWorker>(WorkerID::FromRandom(), 1234);
  pool_.PushWorker(std::dynamic_pointer_cast<WorkerInterface>(worker));

  std::vector<RayLease> leases;
  std::vector<std::unique_ptr<rpc::RequestWorkerLeaseReply>> replies;
  int num_callbacks = 0;
  auto callback = [&](Status, std::function<void()>, std::function<void()>) {
    num_callbacks++;
  };
  // The first lease will be dispatched right away,
  // and the second lease will hit the scheduling class cap.
  for (int i = 0; i < 2; ++i) {
    RayLease lease = CreateLease({{ray::kCPU_ResourceLabel, 8}});
    leases.push_back(lease);
    replies.push_back(std::make_unique<rpc::RequestWorkerLeaseReply>());
    lease_manager_.QueueAndScheduleLease(
        lease,
        false,
        false,
        std::vector<internal::ReplyCallback>{
            internal::ReplyCallback(callback, replies[i].get())});
    pool_.TriggerCallbacks();
  }

  ASSERT_EQ(replies[0]->worker_address().port(), 1234);
  ASSERT_EQ(num_callbacks, 1);
  ASSERT_EQ(NumLeasesToDispatchWithStatus(internal::WorkStatus::WAITING), 1);

  // A new node is added so we should be able to spillback to it.
  auto remote_node_id = NodeID::FromRandom();
  AddNode(remote_node_id, 8);
  lease_manager_.ScheduleAndGrantLeases();
  ASSERT_EQ(num_callbacks, 2);
  ASSERT_EQ(replies[1]->retry_at_raylet_address().node_id(), remote_node_id.Binary());
}

/// Test that we exponentially increase the amount of time it takes to increase
/// the dispatch cap for a scheduling class.
TEST_F(ClusterLeaseManagerTest, SchedulingClassCapIncrease) {
  auto get_unblocked_worker = [](std::vector<std::shared_ptr<MockWorker>> &workers)
      -> std::shared_ptr<MockWorker> {
    for (auto &worker : workers) {
      if (worker->GetAllocatedInstances() != nullptr && !worker->IsBlocked()) {
        return worker;
      }
    }
    return nullptr;
  };

  int64_t UNIT = RayConfig::instance().worker_cap_initial_backoff_delay_ms();
  std::vector<RayLease> leases;
  for (int i = 0; i < 3; i++) {
    RayLease lease = CreateLease({{ray::kCPU_ResourceLabel, 8}},
                                 /*num_args=*/0,
                                 /*args=*/{});
    leases.emplace_back(lease);
  }

  rpc::RequestWorkerLeaseReply reply;
  int num_callbacks = 0;
  auto callback = [&num_callbacks](Status, std::function<void()>, std::function<void()>) {
    num_callbacks++;
  };
  for (const auto &lease : leases) {
    lease_manager_.QueueAndScheduleLease(
        lease,
        false,
        false,
        std::vector<internal::ReplyCallback>{internal::ReplyCallback(callback, &reply)});
  }

  auto runtime_env_hash = leases[0].GetLeaseSpecification().GetRuntimeEnvHash();
  std::vector<std::shared_ptr<MockWorker>> workers;
  for (int i = 0; i < 3; i++) {
    std::shared_ptr<MockWorker> worker =
        std::make_shared<MockWorker>(WorkerID::FromRandom(), 1234, runtime_env_hash);
    pool_.PushWorker(std::static_pointer_cast<WorkerInterface>(worker));
    pool_.TriggerCallbacks();
    workers.push_back(worker);
  }
  lease_manager_.ScheduleAndGrantLeases();

  ASSERT_EQ(num_callbacks, 1);

  current_time_ms_ += UNIT;
  ASSERT_FALSE(workers.back()->IsBlocked());
  ASSERT_TRUE(local_lease_manager_->ReleaseCpuResourcesFromBlockedWorker(
      get_unblocked_worker(workers)));
  lease_manager_.ScheduleAndGrantLeases();
  pool_.TriggerCallbacks();
  lease_manager_.ScheduleAndGrantLeases();
  ASSERT_EQ(num_callbacks, 2);

  // Since we're increasing exponentially, increasing by a unit show no longer be enough.
  current_time_ms_ += UNIT;
  ASSERT_TRUE(local_lease_manager_->ReleaseCpuResourcesFromBlockedWorker(
      get_unblocked_worker(workers)));
  lease_manager_.ScheduleAndGrantLeases();
  pool_.TriggerCallbacks();
  lease_manager_.ScheduleAndGrantLeases();
  ASSERT_EQ(num_callbacks, 2);

  // Now it should run
  current_time_ms_ += UNIT;
  lease_manager_.ScheduleAndGrantLeases();
  pool_.TriggerCallbacks();
  lease_manager_.ScheduleAndGrantLeases();
  ASSERT_EQ(num_callbacks, 3);

  // Let just one lease finish.
  for (auto it = workers.begin(); it != workers.end(); it++) {
    if (!(*it)->IsBlocked()) {
      RayLease buf;
      local_lease_manager_->CleanupLease(*it, &buf);
      workers.erase(it);
      break;
    }
  }

  current_time_ms_ += UNIT;

  // Now schedule another lease of the same scheduling class.
  RayLease lease = CreateLease({{ray::kCPU_ResourceLabel, 8}},
                               /*num_args=*/0,
                               /*args=*/{});
  lease_manager_.QueueAndScheduleLease(
      lease,
      false,
      false,
      std::vector<internal::ReplyCallback>{internal::ReplyCallback(callback, &reply)});

  std::shared_ptr<MockWorker> new_worker =
      std::make_shared<MockWorker>(WorkerID::FromRandom(), 1234, runtime_env_hash);
  pool_.PushWorker(std::static_pointer_cast<WorkerInterface>(new_worker));
  pool_.TriggerCallbacks();
  workers.push_back(new_worker);

  // It can't run for another 2 units (doesn't increase to 4, because one of
  // the leases finished).
  ASSERT_EQ(num_callbacks, 3);

  current_time_ms_ += 2 * UNIT;
  lease_manager_.ScheduleAndGrantLeases();
  pool_.TriggerCallbacks();
  ASSERT_EQ(num_callbacks, 4);

  for (auto &worker : workers) {
    RayLease buf;
    local_lease_manager_->CleanupLease(worker, &buf);
  }

  AssertNoLeaks();
}

/// Ensure we reset the cap after we've granted all leases in the queue.
TEST_F(ClusterLeaseManagerTest, SchedulingClassCapResetTest) {
  int64_t UNIT = RayConfig::instance().worker_cap_initial_backoff_delay_ms();
  std::vector<RayLease> leases;
  for (int i = 0; i < 2; i++) {
    RayLease lease = CreateLease({{ray::kCPU_ResourceLabel, 8}},
                                 /*num_args=*/0,
                                 /*args=*/{});
    leases.emplace_back(lease);
  }

  rpc::RequestWorkerLeaseReply reply;
  int num_callbacks = 0;
  auto callback = [&num_callbacks](Status, std::function<void()>, std::function<void()>) {
    num_callbacks++;
  };
  for (const auto &lease : leases) {
    lease_manager_.QueueAndScheduleLease(
        lease,
        false,
        false,
        std::vector<internal::ReplyCallback>{internal::ReplyCallback(callback, &reply)});
  }

  auto runtime_env_hash = leases[0].GetLeaseSpecification().GetRuntimeEnvHash();

  std::shared_ptr<MockWorker> worker1 =
      std::make_shared<MockWorker>(WorkerID::FromRandom(), 1234, runtime_env_hash);
  pool_.PushWorker(std::static_pointer_cast<WorkerInterface>(worker1));
  pool_.TriggerCallbacks();
  lease_manager_.ScheduleAndGrantLeases();

  ASSERT_TRUE(local_lease_manager_->ReleaseCpuResourcesFromBlockedWorker(worker1));
  current_time_ms_ += UNIT;

  std::shared_ptr<MockWorker> worker2 =
      std::make_shared<MockWorker>(WorkerID::FromRandom(), 1234, runtime_env_hash);
  pool_.PushWorker(std::static_pointer_cast<WorkerInterface>(worker2));
  lease_manager_.ScheduleAndGrantLeases();
  pool_.TriggerCallbacks();

  ASSERT_EQ(num_callbacks, 2);

  RayLease buf;
  local_lease_manager_->CleanupLease(worker1, &buf);
  local_lease_manager_->CleanupLease(worker2, &buf);

  AssertNoLeaks();

  for (int i = 0; i < 2; i++) {
    RayLease lease = CreateLease({{ray::kCPU_ResourceLabel, 8}},
                                 /*num_args=*/0,
                                 /*args=*/{});
    lease_manager_.QueueAndScheduleLease(
        lease,
        false,
        false,
        std::vector<internal::ReplyCallback>{internal::ReplyCallback(callback, &reply)});
  }

  std::shared_ptr<MockWorker> worker3 =
      std::make_shared<MockWorker>(WorkerID::FromRandom(), 1234, runtime_env_hash);
  pool_.PushWorker(std::static_pointer_cast<WorkerInterface>(worker3));
  pool_.TriggerCallbacks();
  lease_manager_.ScheduleAndGrantLeases();
  ASSERT_EQ(num_callbacks, 3);

  ASSERT_TRUE(local_lease_manager_->ReleaseCpuResourcesFromBlockedWorker(worker3));
  current_time_ms_ += UNIT;

  std::shared_ptr<MockWorker> worker4 =
      std::make_shared<MockWorker>(WorkerID::FromRandom(), 1234, runtime_env_hash);
  pool_.PushWorker(std::static_pointer_cast<WorkerInterface>(worker4));
  lease_manager_.ScheduleAndGrantLeases();
  pool_.TriggerCallbacks();

  ASSERT_EQ(num_callbacks, 4);

  {
    // Ensure a class of a different scheduling class can still be scheduled.
    RayLease lease5 = CreateLease({},
                                  /*num_args=*/0,
                                  /*args=*/{});
    lease_manager_.QueueAndScheduleLease(
        lease5,
        false,
        false,
        std::vector<internal::ReplyCallback>{internal::ReplyCallback(callback, &reply)});
    std::shared_ptr<MockWorker> worker5 =
        std::make_shared<MockWorker>(WorkerID::FromRandom(), 1234, runtime_env_hash);
    pool_.PushWorker(std::static_pointer_cast<WorkerInterface>(worker5));
    lease_manager_.ScheduleAndGrantLeases();
    pool_.TriggerCallbacks();
    ASSERT_EQ(num_callbacks, 5);
    local_lease_manager_->CleanupLease(worker5, &buf);
  }

  local_lease_manager_->CleanupLease(worker3, &buf);
  local_lease_manager_->CleanupLease(worker4, &buf);

  AssertNoLeaks();
}

/// Test that scheduling classes which have reached their running cap start
/// their timer after the new lease is submitted, not before.
TEST_F(ClusterLeaseManagerTest, DispatchTimerAfterRequestTest) {
  int64_t UNIT = RayConfig::instance().worker_cap_initial_backoff_delay_ms();
  RayLease first_lease = CreateLease({{ray::kCPU_ResourceLabel, 8}},
                                     /*num_args=*/0,
                                     /*args=*/{});

  rpc::RequestWorkerLeaseReply reply;
  int num_callbacks = 0;
  auto callback = [&num_callbacks](Status, std::function<void()>, std::function<void()>) {
    num_callbacks++;
  };
  lease_manager_.QueueAndScheduleLease(
      first_lease,
      false,
      false,
      std::vector<internal::ReplyCallback>{internal::ReplyCallback(callback, &reply)});

  auto runtime_env_hash = first_lease.GetLeaseSpecification().GetRuntimeEnvHash();
  std::vector<std::shared_ptr<MockWorker>> workers;
  for (int i = 0; i < 3; i++) {
    std::shared_ptr<MockWorker> worker =
        std::make_shared<MockWorker>(WorkerID::FromRandom(), 1234, runtime_env_hash);
    pool_.PushWorker(std::static_pointer_cast<WorkerInterface>(worker));
    pool_.TriggerCallbacks();
    workers.push_back(worker);
  }
  lease_manager_.ScheduleAndGrantLeases();

  ASSERT_EQ(num_callbacks, 1);

  RayLease second_lease = CreateLease({{ray::kCPU_ResourceLabel, 8}},
                                      /*num_args=*/0,
                                      /*args=*/{});
  lease_manager_.QueueAndScheduleLease(
      second_lease,
      false,
      false,
      std::vector<internal::ReplyCallback>{internal::ReplyCallback(callback, &reply)});
  pool_.TriggerCallbacks();

  /// Can't schedule yet due to the cap.
  ASSERT_EQ(num_callbacks, 1);
  for (auto &worker : workers) {
    if (worker->GetAllocatedInstances() && !worker->IsBlocked()) {
      local_lease_manager_->ReleaseCpuResourcesFromBlockedWorker(worker);
    }
  }

  current_time_ms_ += UNIT;
  lease_manager_.ScheduleAndGrantLeases();
  pool_.TriggerCallbacks();

  ASSERT_EQ(num_callbacks, 2);
  for (auto &worker : workers) {
    if (worker->GetAllocatedInstances() && !worker->IsBlocked()) {
      local_lease_manager_->ReleaseCpuResourcesFromBlockedWorker(worker);
    }
  }

  /// A lot of time passes, definitely more than the timeout.
  current_time_ms_ += 100000 * UNIT;

  RayLease third_lease = CreateLease({{ray::kCPU_ResourceLabel, 8}},
                                     /*num_args=*/0,
                                     /*args=*/{});
  lease_manager_.QueueAndScheduleLease(
      third_lease,
      false,
      false,
      std::vector<internal::ReplyCallback>{internal::ReplyCallback(callback, &reply)});
  pool_.TriggerCallbacks();

  /// We still can't schedule the third lease since the timer doesn't start
  /// until after the lease is queued.
  ASSERT_EQ(num_callbacks, 2);

  current_time_ms_ += 2 * UNIT;
  lease_manager_.ScheduleAndGrantLeases();
  pool_.TriggerCallbacks();

  ASSERT_EQ(num_callbacks, 3);

  for (auto &worker : workers) {
    RayLease buf;
    local_lease_manager_->CleanupLease(worker, &buf);
  }

  AssertNoLeaks();
}

TEST_F(ClusterLeaseManagerTest, PopWorkerBeforeDraining) {
  /*
    Test that if PopWorker happens before draining,
    the lease request can still succeed.
  */
  RayLease lease = CreateLease({{ray::kCPU_ResourceLabel, 1}});
  rpc::RequestWorkerLeaseReply reply;
  bool callback_occurred = false;
  bool *callback_occurred_ptr = &callback_occurred;
  auto callback = [callback_occurred_ptr](
                      Status, std::function<void()>, std::function<void()>) {
    *callback_occurred_ptr = true;
  };
  lease_manager_.QueueAndScheduleLease(
      lease,
      false,
      false,
      std::vector<internal::ReplyCallback>{internal::ReplyCallback(callback, &reply)});

  // Drain the local node.
  rpc::DrainRayletRequest drain_request;
  drain_request.set_deadline_timestamp_ms(std::numeric_limits<int64_t>::max());
  scheduler_->GetLocalResourceManager().SetLocalNodeDraining(drain_request);

  std::shared_ptr<MockWorker> worker =
      std::make_shared<MockWorker>(WorkerID::FromRandom(), 1234);
  pool_.PushWorker(std::static_pointer_cast<WorkerInterface>(worker));
  pool_.TriggerCallbacks();
  ASSERT_TRUE(callback_occurred);
  ASSERT_EQ(leased_workers_.size(), 1);
}

TEST_F(ClusterLeaseManagerTest, UnscheduleableWhileDraining) {
  /*
    Test that new leases are not scheduled onto draining nodes.
  */
  RayLease lease = CreateLease({{ray::kCPU_ResourceLabel, 1}});
  rpc::RequestWorkerLeaseReply reply;
  bool callback_occurred = false;
  bool *callback_occurred_ptr = &callback_occurred;
  auto callback = [callback_occurred_ptr](
                      Status, std::function<void()>, std::function<void()>) {
    *callback_occurred_ptr = true;
  };
  lease_manager_.QueueAndScheduleLease(
      lease,
      false,
      false,
      std::vector<internal::ReplyCallback>{internal::ReplyCallback(callback, &reply)});
  std::shared_ptr<MockWorker> worker =
      std::make_shared<MockWorker>(WorkerID::FromRandom(), 1234);
  std::shared_ptr<MockWorker> worker2 =
      std::make_shared<MockWorker>(WorkerID::FromRandom(), 12345);
  pool_.PushWorker(std::static_pointer_cast<WorkerInterface>(worker));
  pool_.PushWorker(std::static_pointer_cast<WorkerInterface>(worker2));
  pool_.TriggerCallbacks();
  ASSERT_TRUE(callback_occurred);
  ASSERT_EQ(leased_workers_.size(), 1);
  ASSERT_EQ(pool_.workers.size(), 1);

  auto remote_node_id = NodeID::FromRandom();
  AddNode(remote_node_id, 5);

  // Drain the local node.
  rpc::DrainRayletRequest drain_request;
  drain_request.set_deadline_timestamp_ms(std::numeric_limits<int64_t>::max());
  scheduler_->GetLocalResourceManager().SetLocalNodeDraining(drain_request);

  RayLease spillback_lease = CreateLease({{ray::kCPU_ResourceLabel, 1}});
  rpc::RequestWorkerLeaseReply spillback_reply;
  lease_manager_.QueueAndScheduleLease(
      spillback_lease,
      false,
      false,
      std::vector<internal::ReplyCallback>{
          internal::ReplyCallback(callback, &spillback_reply)});
  pool_.TriggerCallbacks();
  ASSERT_EQ(leased_workers_.size(), 1);
  ASSERT_EQ(pool_.workers.size(), 1);
  ASSERT_EQ(spillback_reply.retry_at_raylet_address().node_id(), remote_node_id.Binary());
}

// Regression test for https://github.com/ray-project/ray/issues/16935:
// When a lease requires 1 CPU and is infeasible because head node has 0 CPU,
// make sure the lease's resource demand is reported.
TEST_F(ClusterLeaseManagerTestWithoutCPUsAtHead, OneCpuInfeasibleLease) {
  rpc::RequestWorkerLeaseReply reply;
  bool callback_occurred = false;
  bool *callback_occurred_ptr = &callback_occurred;
  auto callback = [callback_occurred_ptr](const Status &,
                                          const std::function<void()> &,
                                          const std::function<void()> &) {
    *callback_occurred_ptr = true;
  };

  constexpr int num_cases = 5;
  // Create 5 leases with different CPU requests.
  const std::array<int, num_cases> cpu_request = {1, 2, 1, 3, 1};
  // Each type of CPU request corresponds to a types of resource demand.
  const std::array<int, num_cases> demand_types = {1, 2, 2, 3, 3};
  // Number of infeasible 1 CPU requests..
  const std::array<int, num_cases> num_infeasible_1cpu = {1, 1, 2, 2, 3};

  for (int i = 0; i < num_cases; ++i) {
    RayLease lease = CreateLease({{ray::kCPU_ResourceLabel, cpu_request[i]}});
    lease_manager_.QueueAndScheduleLease(
        lease,
        false,
        false,
        std::vector<internal::ReplyCallback>{internal::ReplyCallback(callback, &reply)});
    pool_.TriggerCallbacks();

    // The lease cannot run because there is only 1 node (head) with 0 CPU.
    ASSERT_FALSE(callback_occurred);
    ASSERT_EQ(leased_workers_.size(), 0);
    ASSERT_EQ(pool_.workers.size(), 0);
    ASSERT_EQ(node_info_calls_, 0);

    rpc::ResourcesData data;
    lease_manager_.FillResourceUsage(data);
    const auto &resource_load_by_shape = data.resource_load_by_shape();
    ASSERT_EQ(resource_load_by_shape.resource_demands().size(), demand_types[i]);

    // Assert that the one-cpu fields are correct.
    bool one_cpu_found = false;
    for (const auto &demand : resource_load_by_shape.resource_demands()) {
      if (demand.shape().at("CPU") == 1) {
        ASSERT_FALSE(one_cpu_found);
        one_cpu_found = true;
        EXPECT_EQ(demand.num_infeasible_requests_queued(), num_infeasible_1cpu[i]);
        ASSERT_EQ(demand.shape().size(), 1);
      }
    }
    ASSERT_TRUE(one_cpu_found);
  }
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

}  // namespace raylet

}  // namespace ray
