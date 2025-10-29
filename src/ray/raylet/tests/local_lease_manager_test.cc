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

#include "ray/raylet/local_lease_manager.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <list>
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "mock/ray/gcs_client/gcs_client.h"
#include "mock/ray/object_manager/object_manager.h"
#include "ray/common/id.h"
#include "ray/common/lease/lease.h"
#include "ray/common/task/task_util.h"
#include "ray/common/test_utils.h"
#include "ray/observability/fake_metric.h"
#include "ray/raylet/scheduling/cluster_resource_scheduler.h"
#include "ray/raylet/tests/util.h"

namespace ray::raylet {

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
        // No lease should be granted.
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
      bool granted = false;
      auto cb_it = callbacks.find(runtime_env_hash);
      if (cb_it != callbacks.end()) {
        auto &list = cb_it->second;
        RAY_CHECK(!list.empty());
        for (auto list_it = list.begin(); list_it != list.end();) {
          auto &callback = *list_it;
          granted = callback(worker, PopWorkerStatus::OK, "");
          list_it = list.erase(list_it);
          if (granted) {
            break;
          }
        }
        if (list.empty()) {
          callbacks.erase(cb_it);
        }
        if (granted) {
          it = workers.erase(it);
          continue;
        }
      }
      it++;
    }
  }

  size_t CallbackSize(int runtime_env_hash) {
    auto cb_it = callbacks.find(runtime_env_hash);
    if (cb_it != callbacks.end()) {
      auto &list = cb_it->second;
      return list.size();
    }
    return 0;
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

  std::list<std::shared_ptr<WorkerInterface>> workers;
  absl::flat_hash_map<int, std::list<PopWorkerCallback>> callbacks;
  int num_pops;
};

namespace {

std::shared_ptr<ClusterResourceScheduler> CreateSingleNodeScheduler(
    const std::string &id, double num_cpus, gcs::GcsClient &gcs_client) {
  absl::flat_hash_map<std::string, double> local_node_resources;
  local_node_resources[ray::kCPU_ResourceLabel] = num_cpus;
  static instrumented_io_context io_context;
  auto scheduler = std::make_shared<ClusterResourceScheduler>(
      io_context,
      scheduling::NodeID(id),
      local_node_resources,
      /*is_node_available_fn*/ [&gcs_client](scheduling::NodeID node_id) {
        return gcs_client.Nodes().Get(NodeID::FromBinary(node_id.Binary())) != nullptr;
      });

  return scheduler;
}

RayLease CreateLease(const std::unordered_map<std::string, double> &required_resources,
                     const std::string &task_name = "default",
                     const std::vector<std::unique_ptr<TaskArg>> &args = {}) {
  TaskSpecBuilder spec_builder;
  TaskID id = RandomTaskId();
  JobID job_id = RandomJobId();
  rpc::Address address;
  spec_builder.SetCommonTaskSpec(
      id,
      task_name,
      Language::PYTHON,
      FunctionDescriptorBuilder::BuildPython(task_name, "", "", ""),
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
      nullptr);

  spec_builder.SetNormalTaskSpec(0, false, "", rpc::SchedulingStrategy(), ActorID::Nil());

  for (const auto &arg : args) {
    spec_builder.AddArg(*arg);
  }

  TaskSpecification spec = std::move(spec_builder).ConsumeAndBuild();
  LeaseSpecification lease_spec(spec.GetMessage());
  lease_spec.GetMutableMessage().set_lease_id(LeaseID::FromRandom().Binary());
  return RayLease(std::move(lease_spec));
}

}  // namespace

class LocalLeaseManagerTest : public ::testing::Test {
 public:
  explicit LocalLeaseManagerTest(double num_cpus = 3.0)
      : gcs_client_(std::make_unique<gcs::MockGcsClient>()),
        id_(NodeID::FromRandom()),
        scheduler_(CreateSingleNodeScheduler(id_.Binary(), num_cpus, *gcs_client_)),
        object_manager_(),
        fake_task_by_state_counter_(),
        lease_dependency_manager_(object_manager_, fake_task_by_state_counter_),
        local_lease_manager_(std::make_shared<LocalLeaseManager>(
            id_,
            *scheduler_,
            lease_dependency_manager_,
            /* get_node_info= */
            [this](
                const NodeID &node_id) -> std::optional<rpc::GcsNodeAddressAndLiveness> {
              if (node_info_.count(node_id) != 0) {
                return std::optional((node_info_[node_id]));
              }
              return std::nullopt;
            },
            pool_,
            leased_workers_,
            /* get_lease_arguments= */
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
            /*max_pinned_lease_arguments_bytes=*/1000,
            /*get_time=*/[this]() { return current_time_ms_; })) {}

  void SetUp() override {
    static rpc::GcsNodeInfo node_info;
    ON_CALL(*gcs_client_->mock_node_accessor, Get(::testing::_, ::testing::_))
        .WillByDefault(::testing::Return(&node_info));
  }

  RayObject *MakeDummyArg() {
    std::vector<uint8_t> data;
    data.resize(default_arg_size_);
    auto buffer = std::make_shared<LocalMemoryBuffer>(data.data(), data.size());
    return new RayObject(buffer, nullptr, {});
  }

  void Shutdown() {}

  std::unique_ptr<gcs::MockGcsClient> gcs_client_;
  NodeID id_;
  std::shared_ptr<ClusterResourceScheduler> scheduler_;
  MockWorkerPool pool_;
  absl::flat_hash_map<LeaseID, std::shared_ptr<WorkerInterface>> leased_workers_;
  std::unordered_set<ObjectID> missing_objects_;

  int default_arg_size_ = 10;
  int64_t current_time_ms_ = 0;

  absl::flat_hash_map<NodeID, rpc::GcsNodeAddressAndLiveness> node_info_;

  MockObjectManager object_manager_;
  ray::observability::FakeGauge fake_task_by_state_counter_;
  LeaseDependencyManager lease_dependency_manager_;
  std::shared_ptr<LocalLeaseManager> local_lease_manager_;
};

TEST_F(LocalLeaseManagerTest, TestCancelLeasesWithoutReply) {
  int num_callbacks_called = 0;
  auto callback = [&num_callbacks_called](Status status,
                                          std::function<void()> success,
                                          std::function<void()> failure) {
    ++num_callbacks_called;
  };

  auto lease1 = CreateLease({{ray::kCPU_ResourceLabel, 1}}, "f");
  rpc::RequestWorkerLeaseReply reply1;
  // lease1 is waiting for a worker
  local_lease_manager_->QueueAndScheduleLease(std::make_shared<internal::Work>(
      lease1,
      false,
      false,
      std::vector<internal::ReplyCallback>{internal::ReplyCallback(callback, &reply1)},
      internal::WorkStatus::WAITING));

  auto arg_id = ObjectID::FromRandom();
  std::vector<std::unique_ptr<TaskArg>> args;
  args.push_back(
      std::make_unique<TaskArgByReference>(arg_id, rpc::Address{}, "call_site"));
  auto lease2 = CreateLease({{kCPU_ResourceLabel, 1}}, "f", args);
  EXPECT_CALL(object_manager_, Pull(_, _, _)).WillOnce(::testing::Return(1));
  rpc::RequestWorkerLeaseReply reply2;
  // lease2 is waiting for args
  local_lease_manager_->QueueAndScheduleLease(std::make_shared<internal::Work>(
      lease2,
      false,
      false,
      std::vector<internal::ReplyCallback>{internal::ReplyCallback(callback, &reply2)},
      internal::WorkStatus::WAITING));

  auto cancelled_works = local_lease_manager_->CancelLeasesWithoutReply(
      [](const std::shared_ptr<internal::Work> &work) { return true; });
  ASSERT_EQ(cancelled_works.size(), 2);
  // Make sure the reply is not sent.
  ASSERT_EQ(num_callbacks_called, 0);
}

TEST_F(LocalLeaseManagerTest, TestLeaseGrantingOrder) {
  // Initial setup: 3 CPUs available.
  std::shared_ptr<MockWorker> worker1 =
      std::make_shared<MockWorker>(WorkerID::FromRandom(), 0);
  std::shared_ptr<MockWorker> worker2 =
      std::make_shared<MockWorker>(WorkerID::FromRandom(), 0);
  std::shared_ptr<MockWorker> worker3 =
      std::make_shared<MockWorker>(WorkerID::FromRandom(), 0);
  pool_.PushWorker(std::static_pointer_cast<WorkerInterface>(worker1));
  pool_.PushWorker(std::static_pointer_cast<WorkerInterface>(worker2));
  pool_.PushWorker(std::static_pointer_cast<WorkerInterface>(worker3));

  // First batch of leases: [f, f]
  auto lease_f1 = CreateLease({{ray::kCPU_ResourceLabel, 1}}, "f");
  auto lease_f2 = CreateLease({{ray::kCPU_ResourceLabel, 1}}, "f");
  rpc::RequestWorkerLeaseReply reply;
  auto empty_callback =
      [](Status status, std::function<void()> success, std::function<void()> failure) {};
  local_lease_manager_->WaitForLeaseArgsRequests(std::make_shared<internal::Work>(
      lease_f1,
      false,
      false,
      std::vector<internal::ReplyCallback>{
          internal::ReplyCallback(empty_callback, &reply)},
      internal::WorkStatus::WAITING));
  local_lease_manager_->ScheduleAndGrantLeases();
  pool_.TriggerCallbacks();
  local_lease_manager_->WaitForLeaseArgsRequests(std::make_shared<internal::Work>(
      lease_f2,
      false,
      false,
      std::vector<internal::ReplyCallback>{
          internal::ReplyCallback(empty_callback, &reply)},
      internal::WorkStatus::WAITING));
  local_lease_manager_->ScheduleAndGrantLeases();
  pool_.TriggerCallbacks();

  // Second batch of leases: [f, f, f, g]
  auto lease_f3 = CreateLease({{ray::kCPU_ResourceLabel, 1}}, "f");
  auto lease_f4 = CreateLease({{ray::kCPU_ResourceLabel, 1}}, "f");
  auto lease_f5 = CreateLease({{ray::kCPU_ResourceLabel, 1}}, "f");
  auto lease_g1 = CreateLease({{ray::kCPU_ResourceLabel, 1}}, "g");
  local_lease_manager_->WaitForLeaseArgsRequests(std::make_shared<internal::Work>(
      lease_f3,
      false,
      false,
      std::vector<internal::ReplyCallback>{
          internal::ReplyCallback(empty_callback, &reply)},
      internal::WorkStatus::WAITING));
  local_lease_manager_->WaitForLeaseArgsRequests(std::make_shared<internal::Work>(
      lease_f4,
      false,
      false,
      std::vector<internal::ReplyCallback>{
          internal::ReplyCallback(empty_callback, &reply)},
      internal::WorkStatus::WAITING));
  local_lease_manager_->WaitForLeaseArgsRequests(std::make_shared<internal::Work>(
      lease_f5,
      false,
      false,
      std::vector<internal::ReplyCallback>{
          internal::ReplyCallback(empty_callback, &reply)},
      internal::WorkStatus::WAITING));
  local_lease_manager_->WaitForLeaseArgsRequests(std::make_shared<internal::Work>(
      lease_g1,
      false,
      false,
      std::vector<internal::ReplyCallback>{
          internal::ReplyCallback(empty_callback, &reply)},
      internal::WorkStatus::WAITING));
  local_lease_manager_->ScheduleAndGrantLeases();
  pool_.TriggerCallbacks();
  auto leases_to_grant_ = local_lease_manager_->GetLeasesToGrant();
  // Out of the leases in the second batch, only lease g is granted due to fair scheduling
  ASSERT_EQ(leases_to_grant_.size(), 1);
}

TEST_F(LocalLeaseManagerTest, TestNoLeakOnImpossibleInfeasibleLease) {
  // Note that ideally it shouldn't be possible for an infeasible lease to
  // be in the local lease manager when ScheduleAndGrantLeases happens.
  // See https://github.com/ray-project/ray/pull/52295 for reasons why added this.

  std::shared_ptr<MockWorker> worker1 =
      std::make_shared<MockWorker>(WorkerID::FromRandom(), 0);
  std::shared_ptr<MockWorker> worker2 =
      std::make_shared<MockWorker>(WorkerID::FromRandom(), 0);
  pool_.PushWorker(std::static_pointer_cast<WorkerInterface>(worker1));

  // Create 2 leases that requires 3 CPU's each and are waiting on an arg.
  auto arg_id = ObjectID::FromRandom();
  std::vector<std::unique_ptr<TaskArg>> args;
  args.push_back(
      std::make_unique<TaskArgByReference>(arg_id, rpc::Address{}, "call_site"));
  auto lease1 = CreateLease({{kCPU_ResourceLabel, 3}}, "f", args);
  auto lease2 = CreateLease({{kCPU_ResourceLabel, 3}}, "f2", args);

  EXPECT_CALL(object_manager_, Pull(_, _, _))
      .WillOnce(::testing::Return(1))
      .WillOnce(::testing::Return(2));

  // Submit the leases to the local lease manager.
  int num_callbacks_called = 0;
  auto callback = [&num_callbacks_called](Status status,
                                          std::function<void()> success,
                                          std::function<void()> failure) {
    ++num_callbacks_called;
  };
  rpc::RequestWorkerLeaseReply reply1;
  local_lease_manager_->QueueAndScheduleLease(std::make_shared<internal::Work>(
      lease1,
      false,
      false,
      std::vector<internal::ReplyCallback>{internal::ReplyCallback(callback, &reply1)},
      internal::WorkStatus::WAITING));
  rpc::RequestWorkerLeaseReply reply2;
  local_lease_manager_->QueueAndScheduleLease(std::make_shared<internal::Work>(
      lease2,
      false,
      false,
      std::vector<internal::ReplyCallback>{internal::ReplyCallback(callback, &reply2)},
      internal::WorkStatus::WAITING));

  // Node no longer has cpu.
  scheduler_->GetLocalResourceManager().DeleteLocalResource(
      scheduling::ResourceID::CPU());

  // Simulate arg becoming local.
  local_lease_manager_->LeasesUnblocked({lease1.GetLeaseSpecification().LeaseId(),
                                         lease2.GetLeaseSpecification().LeaseId()});

  // Assert that the the correct rpc replies were sent back and the grant map is empty.
  ASSERT_EQ(reply1.failure_type(),
            rpc::RequestWorkerLeaseReply::SCHEDULING_CANCELLED_UNSCHEDULABLE);
  ASSERT_EQ(reply2.failure_type(),
            rpc::RequestWorkerLeaseReply::SCHEDULING_CANCELLED_UNSCHEDULABLE);
  ASSERT_EQ(num_callbacks_called, 2);
  ASSERT_EQ(local_lease_manager_->GetLeasesToGrant().size(), 0);
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

}  // namespace ray::raylet
