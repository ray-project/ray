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

#include "ray/raylet/local_task_manager.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <memory>
#include <string>

#include "mock/ray/gcs/gcs_client/gcs_client.h"
#include "ray/common/id.h"
#include "ray/common/task/task.h"
#include "ray/common/task/task_util.h"
#include "ray/common/test_util.h"
#include "ray/raylet/scheduling/cluster_resource_scheduler.h"
#include "ray/raylet/test/util.h"

namespace ray {
namespace raylet {

using ::testing::_;

class MockWorkerPool : public WorkerPoolInterface {
 public:
  MockWorkerPool() : num_pops(0) {}

  void PopWorker(const TaskSpecification &task_spec,
                 const PopWorkerCallback &callback,
                 const std::string &allocated_instances_serialized_json) {
    num_pops++;
    const int runtime_env_hash = task_spec.GetRuntimeEnvHash();
    callbacks[runtime_env_hash].push_back(callback);
  }

  void PushWorker(const std::shared_ptr<WorkerInterface> &worker) {
    workers.push_front(worker);
  }

  const std::vector<std::shared_ptr<WorkerInterface>> GetAllRegisteredWorkers(
      bool filter_dead_workers, bool filter_io_workers) const {
    RAY_CHECK(false) << "Not used.";
    return {};
  }

  void TriggerCallbacksWithNotOKStatus(
      PopWorkerStatus status, const std::string &runtime_env_setup_error_msg = "") {
    RAY_CHECK(status != PopWorkerStatus::OK);
    for (const auto &pair : callbacks) {
      for (const auto &callback : pair.second) {
        // No task should be dispatched.
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

RayTask CreateTask(const std::unordered_map<std::string, double> &required_resources,
                   const std::string &task_name = "default") {
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
      nullptr);

  spec_builder.SetNormalTaskSpec(0, false, "", rpc::SchedulingStrategy(), ActorID::Nil());

  return RayTask(spec_builder.Build());
}

class MockObjectManager : public ObjectManagerInterface {
 public:
  MockObjectManager() {}
  uint64_t Pull(const std::vector<rpc::ObjectReference> &object_refs,
                BundlePriority priority,
                const TaskMetricsKey &metrics_key) override {
    return 0;
  }
  void CancelPull(uint64_t request_id) override {}
  bool PullRequestActiveOrWaitingForMetadata(uint64_t request_id) const override {
    return false;
  }
  int64_t PullManagerNumInactivePullsByTaskName(
      const TaskMetricsKey &metrics_key) const override {
    return 0;
  }
};

class LocalTaskManagerTest : public ::testing::Test {
 public:
  LocalTaskManagerTest(double num_cpus = 3.0)
      : gcs_client_(std::make_unique<gcs::MockGcsClient>()),
        id_(NodeID::FromRandom()),
        scheduler_(CreateSingleNodeScheduler(id_.Binary(), num_cpus, *gcs_client_)),
        object_manager_(),
        dependency_manager_(object_manager_),
        local_task_manager_(std::make_shared<LocalTaskManager>(
            id_,
            scheduler_,
            dependency_manager_, /* is_owner_alive= */
            [](const WorkerID &worker_id, const NodeID &node_id) { return true; },
            /* get_node_info= */
            [this](const NodeID &node_id) -> const rpc::GcsNodeInfo * {
              if (node_info_.count(node_id) != 0) {
                return &node_info_[node_id];
              }
              return nullptr;
            },
            pool_,
            leased_workers_,
            /* get_task_arguments= */
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
            /*max_pinned_task_arguments_bytes=*/1000,
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
  absl::flat_hash_map<WorkerID, std::shared_ptr<WorkerInterface>> leased_workers_;
  std::unordered_set<ObjectID> missing_objects_;

  int default_arg_size_ = 10;
  int64_t current_time_ms_ = 0;

  absl::flat_hash_map<NodeID, rpc::GcsNodeInfo> node_info_;

  MockObjectManager object_manager_;
  DependencyManager dependency_manager_;
  std::shared_ptr<LocalTaskManager> local_task_manager_;
};

TEST_F(LocalTaskManagerTest, TestTaskDispatchingOrder) {
  RAY_LOG(INFO) << "Starting TestTaskDispatchingOrder";

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

  // First batch of tasks: 2 'f' tasks
  auto task_f1 = CreateTask({{ray::kCPU_ResourceLabel, 1}}, "f");
  auto task_f2 = CreateTask({{ray::kCPU_ResourceLabel, 1}}, "f");
  rpc::RequestWorkerLeaseReply reply;
  bool callback_occurred = false;
  bool *callback_occurred_ptr = &callback_occurred;
  auto callback = [callback_occurred_ptr](
                      Status, std::function<void()>, std::function<void()>) {
    *callback_occurred_ptr = true;
  };
  local_task_manager_->WaitForTaskArgsRequests(std::make_shared<internal::Work>(
      task_f1,
      false,
      false,
      &reply,
      [callback] { callback(Status::OK(), nullptr, nullptr); },
      internal::WorkStatus::WAITING));
  local_task_manager_->ScheduleAndDispatchTasks();
  pool_.TriggerCallbacks();
  local_task_manager_->WaitForTaskArgsRequests(std::make_shared<internal::Work>(
      task_f2,
      false,
      false,
      &reply,
      [callback] { callback(Status::OK(), nullptr, nullptr); },
      internal::WorkStatus::WAITING));
  local_task_manager_->ScheduleAndDispatchTasks();
  pool_.TriggerCallbacks();

  // Second batch of tasks: [f, f, f, g]
  auto task_f3 = CreateTask({{ray::kCPU_ResourceLabel, 1}}, "f");
  auto task_f4 = CreateTask({{ray::kCPU_ResourceLabel, 1}}, "f");
  auto task_f5 = CreateTask({{ray::kCPU_ResourceLabel, 1}}, "f");
  auto task_g1 = CreateTask({{ray::kCPU_ResourceLabel, 1}}, "g");
  local_task_manager_->WaitForTaskArgsRequests(std::make_shared<internal::Work>(
      task_f3,
      false,
      false,
      &reply,
      [callback] { callback(Status::OK(), nullptr, nullptr); },
      internal::WorkStatus::WAITING));
  local_task_manager_->WaitForTaskArgsRequests(std::make_shared<internal::Work>(
      task_f4,
      false,
      false,
      &reply,
      [callback] { callback(Status::OK(), nullptr, nullptr); },
      internal::WorkStatus::WAITING));
  local_task_manager_->WaitForTaskArgsRequests(std::make_shared<internal::Work>(
      task_f5,
      false,
      false,
      &reply,
      [callback] { callback(Status::OK(), nullptr, nullptr); },
      internal::WorkStatus::WAITING));
  local_task_manager_->WaitForTaskArgsRequests(std::make_shared<internal::Work>(
      task_g1,
      false,
      false,
      &reply,
      [callback] { callback(Status::OK(), nullptr, nullptr); },
      internal::WorkStatus::WAITING));
  local_task_manager_->ScheduleAndDispatchTasks();
  pool_.TriggerCallbacks();
  auto tasks_to_dispatch_ = local_task_manager_->GetTaskToDispatch();
  // Only task f in queue now as g is dispatched.
  ASSERT_EQ(tasks_to_dispatch_.size(), 1);
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

}  // namespace raylet
}  // namespace ray
