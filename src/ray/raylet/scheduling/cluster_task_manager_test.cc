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

#include "ray/raylet/scheduling/cluster_task_manager.h"

#include <memory>
#include <string>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "ray/common/id.h"
#include "ray/common/task/scheduling_resources.h"
#include "ray/common/task/task.h"
#include "ray/common/task/task_util.h"
#include "ray/common/test_util.h"
#include "ray/raylet/scheduling/cluster_resource_scheduler.h"
#include "ray/raylet/scheduling/scheduling_ids.h"
#include "ray/raylet/test/util.h"

#ifdef UNORDERED_VS_ABSL_MAPS_EVALUATION
#include <chrono>

#include "absl/container/flat_hash_map.h"
#endif  // UNORDERED_VS_ABSL_MAPS_EVALUATION

namespace ray {

namespace raylet {

using ::testing::_;

class MockWorkerPool : public WorkerPoolInterface {
 public:
  MockWorkerPool() : num_pops(0) {}

  void PopWorker(const TaskSpecification &task_spec, const PopWorkerCallback &callback,
                 const std::string &allocated_instances_serialized_json) {
    num_pops++;
    const WorkerCacheKey env = {
        task_spec.OverrideEnvironmentVariables(), task_spec.SerializedRuntimeEnv(), {}};
    const int runtime_env_hash = env.IntHash();
    callbacks[runtime_env_hash].push_back(callback);
  }

  void PushWorker(const std::shared_ptr<WorkerInterface> &worker) {
    workers.push_front(worker);
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
          dispatched = callback(worker, PopWorkerStatus::OK);
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
  std::unordered_map<int, std::list<PopWorkerCallback>> callbacks;
  int num_pops;
};

std::shared_ptr<ClusterResourceScheduler> CreateSingleNodeScheduler(
    const std::string &id) {
  std::unordered_map<std::string, double> local_node_resources;
  local_node_resources[ray::kCPU_ResourceLabel] = 8;
  local_node_resources[ray::kGPU_ResourceLabel] = 4;
  local_node_resources[ray::kMemory_ResourceLabel] = 128;

  auto scheduler = std::make_shared<ClusterResourceScheduler>(
      ClusterResourceScheduler(id, local_node_resources));

  return scheduler;
}

RayTask CreateTask(const std::unordered_map<std::string, double> &required_resources,
                   int num_args = 0, std::vector<ObjectID> args = {},
                   std::string serialized_runtime_env = "{}") {
  TaskSpecBuilder spec_builder;
  TaskID id = RandomTaskId();
  JobID job_id = RandomJobId();
  rpc::Address address;
  spec_builder.SetCommonTaskSpec(
      id, "dummy_task", Language::PYTHON,
      FunctionDescriptorBuilder::BuildPython("", "", "", ""), job_id, TaskID::Nil(), 0,
      TaskID::Nil(), address, 0, required_resources, {},
      std::make_pair(PlacementGroupID::Nil(), -1), true, "", serialized_runtime_env);

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

  rpc::TaskExecutionSpec execution_spec_message;
  execution_spec_message.set_num_forwards(1);
  return RayTask(spec_builder.Build(),
                 TaskExecutionSpecification(execution_spec_message));
}

class MockTaskDependencyManager : public TaskDependencyManagerInterface {
 public:
  MockTaskDependencyManager(std::unordered_set<ObjectID> &missing_objects)
      : missing_objects_(missing_objects) {}

  bool RequestTaskDependencies(
      const TaskID &task_id, const std::vector<rpc::ObjectReference> &required_objects) {
    RAY_CHECK(subscribed_tasks.insert(task_id).second);
    for (auto &obj_ref : required_objects) {
      if (missing_objects_.count(ObjectRefToId(obj_ref))) {
        return false;
      }
    }
    return true;
  }

  void RemoveTaskDependencies(const TaskID &task_id) {
    RAY_CHECK(subscribed_tasks.erase(task_id));
  }

  bool TaskDependenciesBlocked(const TaskID &task_id) const {
    return blocked_tasks.count(task_id);
  }

  bool CheckObjectLocal(const ObjectID &object_id) const { return true; }

  std::unordered_set<ObjectID> &missing_objects_;
  std::unordered_set<TaskID> subscribed_tasks;
  std::unordered_set<TaskID> blocked_tasks;
};

class ClusterTaskManagerTest : public ::testing::Test {
 public:
  ClusterTaskManagerTest()
      : id_(NodeID::FromRandom()),
        scheduler_(CreateSingleNodeScheduler(id_.Binary())),
        is_owner_alive_(true),
        node_info_calls_(0),
        announce_infeasible_task_calls_(0),
        dependency_manager_(missing_objects_),
        task_manager_(id_, scheduler_, dependency_manager_,
                      /* is_owner_alive= */
                      [this](const WorkerID &worker_id, const NodeID &node_id) {
                        return is_owner_alive_;
                      },
                      /* get_node_info= */
                      [this](const NodeID &node_id) {
                        node_info_calls_++;
                        return node_info_[node_id];
                      },
                      /* announce_infeasible_task= */
                      [this](const RayTask &task) { announce_infeasible_task_calls_++; },
                      pool_, leased_workers_,
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
                      /*max_pinned_task_arguments_bytes=*/1000) {}

  RayObject *MakeDummyArg() {
    std::vector<uint8_t> data;
    data.resize(default_arg_size_);
    auto buffer = std::make_shared<LocalMemoryBuffer>(data.data(), data.size());
    return new RayObject(buffer, nullptr, {});
  }

  void SetUp() {}

  void Shutdown() {}

  void AddNode(const NodeID &id, double num_cpus, double num_gpus = 0,
               double memory = 0) {
    std::unordered_map<std::string, double> node_resources;
    node_resources[ray::kCPU_ResourceLabel] = num_cpus;
    node_resources[ray::kGPU_ResourceLabel] = num_gpus;
    node_resources[ray::kMemory_ResourceLabel] = memory;
    scheduler_->AddOrUpdateNode(id.Binary(), node_resources, node_resources);

    rpc::GcsNodeInfo info;
    node_info_[id] = info;
  }

  void AssertNoLeaks() {
    ASSERT_TRUE(task_manager_.tasks_to_schedule_.empty());
    ASSERT_TRUE(task_manager_.tasks_to_dispatch_.empty());
    ASSERT_TRUE(task_manager_.waiting_tasks_index_.empty());
    ASSERT_TRUE(task_manager_.waiting_task_queue_.empty());
    ASSERT_TRUE(task_manager_.infeasible_tasks_.empty());
    ASSERT_TRUE(task_manager_.executing_task_args_.empty());
    ASSERT_TRUE(task_manager_.pinned_task_arguments_.empty());
    ASSERT_EQ(task_manager_.pinned_task_arguments_bytes_, 0);
    ASSERT_TRUE(dependency_manager_.subscribed_tasks.empty());
  }

  void AssertPinnedTaskArgumentsPresent(const RayTask &task) {
    const auto &expected_deps = task.GetTaskSpecification().GetDependencyIds();
    ASSERT_EQ(task_manager_.executing_task_args_[task.GetTaskSpecification().TaskId()],
              expected_deps);
    for (auto &arg : expected_deps) {
      ASSERT_TRUE(task_manager_.pinned_task_arguments_.count(arg));
    }
  }

  int NumTasksWaitingForWorker() {
    int count = 0;
    for (const auto &pair : task_manager_.tasks_to_dispatch_) {
      for (const auto &work : pair.second) {
        if (work->status == WorkStatus::WAITING_FOR_WORKER) {
          count++;
        }
      }
    }
    return count;
  }

  NodeID id_;
  std::shared_ptr<ClusterResourceScheduler> scheduler_;
  MockWorkerPool pool_;
  std::unordered_map<WorkerID, std::shared_ptr<WorkerInterface>> leased_workers_;
  std::unordered_set<ObjectID> missing_objects_;

  bool is_owner_alive_;
  int default_arg_size_ = 10;

  int node_info_calls_;
  int announce_infeasible_task_calls_;
  std::unordered_map<NodeID, boost::optional<rpc::GcsNodeInfo>> node_info_;

  MockTaskDependencyManager dependency_manager_;
  ClusterTaskManager task_manager_;
};

TEST_F(ClusterTaskManagerTest, BasicTest) {
  /*
    Test basic scheduler functionality:
    1. Queue and attempt to schedule/dispatch atest with no workers available
    2. A worker becomes available, dispatch again.
   */
  RayTask task = CreateTask({{ray::kCPU_ResourceLabel, 4}});
  rpc::RequestWorkerLeaseReply reply;
  bool callback_occurred = false;
  bool *callback_occurred_ptr = &callback_occurred;
  auto callback = [callback_occurred_ptr](Status, std::function<void()>,
                                          std::function<void()>) {
    *callback_occurred_ptr = true;
  };

  task_manager_.QueueAndScheduleTask(task, &reply, callback);
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

  RayTask finished_task;
  task_manager_.TaskFinished(leased_workers_.begin()->second, &finished_task);
  ASSERT_EQ(finished_task.GetTaskSpecification().TaskId(),
            task.GetTaskSpecification().TaskId());
  AssertNoLeaks();
}

TEST_F(ClusterTaskManagerTest, DispatchQueueNonBlockingTest) {
  /*
    Test that if no worker is available for the first task in a dispatch
    queue (because the runtime env in the task spec doesn't match any
    available worker), other tasks in the dispatch queue can still be scheduled.
    https://github.com/ray-project/ray/issues/16226
   */

  // Use the same required_resources for all tasks so they end up in the same queue.
  const std::unordered_map<std::string, double> required_resources = {
      {ray::kCPU_ResourceLabel, 4}};

  std::string serialized_runtime_env_A = "mock_env_A";
  RayTask task_A = CreateTask(required_resources, /*num_args=*/0, /*args=*/{},
                              serialized_runtime_env_A);
  rpc::RequestWorkerLeaseReply reply_A;
  bool callback_occurred = false;
  bool *callback_occurred_ptr = &callback_occurred;
  auto callback = [callback_occurred_ptr](Status, std::function<void()>,
                                          std::function<void()>) {
    *callback_occurred_ptr = true;
  };

  std::string serialized_runtime_env_B = "mock_env_B";
  RayTask task_B_1 = CreateTask(required_resources, /*num_args=*/0, /*args=*/{},
                                serialized_runtime_env_B);
  RayTask task_B_2 = CreateTask(required_resources, /*num_args=*/0, /*args=*/{},
                                serialized_runtime_env_B);
  rpc::RequestWorkerLeaseReply reply_B_1;
  rpc::RequestWorkerLeaseReply reply_B_2;
  auto empty_callback = [](Status, std::function<void()>, std::function<void()>) {};

  // Ensure task_A is not at the front of the queue.
  task_manager_.QueueAndScheduleTask(task_B_1, &reply_B_1, empty_callback);
  task_manager_.QueueAndScheduleTask(task_A, &reply_A, callback);
  task_manager_.QueueAndScheduleTask(task_B_2, &reply_B_2, empty_callback);
  pool_.TriggerCallbacks();

  // Push a worker that can only run task A.
  const WorkerCacheKey env_A = {
      /*override_environment_variables=*/{}, serialized_runtime_env_A, {}};
  const int runtime_env_hash_A = env_A.IntHash();
  std::shared_ptr<MockWorker> worker_A =
      std::make_shared<MockWorker>(WorkerID::FromRandom(), 1234, runtime_env_hash_A);
  pool_.PushWorker(std::static_pointer_cast<WorkerInterface>(worker_A));
  pool_.TriggerCallbacks();

  ASSERT_TRUE(callback_occurred);
  ASSERT_EQ(leased_workers_.size(), 1);
  ASSERT_EQ(pool_.workers.size(), 0);
  ASSERT_EQ(node_info_calls_, 0);

  RayTask finished_task;
  task_manager_.TaskFinished(leased_workers_.begin()->second, &finished_task);
  ASSERT_EQ(finished_task.GetTaskSpecification().TaskId(),
            task_A.GetTaskSpecification().TaskId());

  // task_B_1 and task_B_2 remain in the dispatch queue, so don't call AssertNoLeaks().
  // AssertNoLeaks();
}

TEST_F(ClusterTaskManagerTest, BlockedWorkerDiesTest) {
  /*
   Tests the edge case in which a worker crashes while it's blocked. In this case, its CPU
   resources should not be double freed.
   */
  RayTask task = CreateTask({{ray::kCPU_ResourceLabel, 4}});
  rpc::RequestWorkerLeaseReply reply;
  bool callback_occurred = false;
  bool *callback_occurred_ptr = &callback_occurred;
  auto callback = [callback_occurred_ptr](Status, std::function<void()>,
                                          std::function<void()>) {
    *callback_occurred_ptr = true;
  };

  task_manager_.QueueAndScheduleTask(task, &reply, callback);
  pool_.TriggerCallbacks();

  ASSERT_FALSE(callback_occurred);
  ASSERT_EQ(leased_workers_.size(), 0);
  ASSERT_EQ(pool_.workers.size(), 0);

  std::shared_ptr<MockWorker> worker =
      std::make_shared<MockWorker>(WorkerID::FromRandom(), 1234);
  pool_.PushWorker(std::static_pointer_cast<WorkerInterface>(worker));

  task_manager_.ScheduleAndDispatchTasks();
  pool_.TriggerCallbacks();

  ASSERT_TRUE(callback_occurred);
  ASSERT_EQ(leased_workers_.size(), 1);
  ASSERT_EQ(pool_.workers.size(), 0);
  ASSERT_EQ(node_info_calls_, 0);

  // Block the worker. Which releases only the CPU resource.
  task_manager_.ReleaseCpuResourcesFromUnblockedWorker(worker);

  RayTask finished_task;
  // If a resource was double-freed, we will crash in this call.
  task_manager_.TaskFinished(leased_workers_.begin()->second, &finished_task);
  ASSERT_EQ(finished_task.GetTaskSpecification().TaskId(),
            task.GetTaskSpecification().TaskId());

  AssertNoLeaks();
}

TEST_F(ClusterTaskManagerTest, BlockedWorkerDies2Test) {
  /*
    Same edge case as the previous test, but this time the block and finish requests
    happen in the opposite order.
   */
  RayTask task = CreateTask({{ray::kCPU_ResourceLabel, 4}});
  rpc::RequestWorkerLeaseReply reply;
  bool callback_occurred = false;
  bool *callback_occurred_ptr = &callback_occurred;
  auto callback = [callback_occurred_ptr](Status, std::function<void()>,
                                          std::function<void()>) {
    *callback_occurred_ptr = true;
  };

  task_manager_.QueueAndScheduleTask(task, &reply, callback);
  pool_.TriggerCallbacks();

  ASSERT_FALSE(callback_occurred);
  ASSERT_EQ(leased_workers_.size(), 0);
  ASSERT_EQ(pool_.workers.size(), 0);

  std::shared_ptr<MockWorker> worker =
      std::make_shared<MockWorker>(WorkerID::FromRandom(), 1234);
  pool_.PushWorker(std::static_pointer_cast<WorkerInterface>(worker));

  task_manager_.ScheduleAndDispatchTasks();
  pool_.TriggerCallbacks();

  ASSERT_TRUE(callback_occurred);
  ASSERT_EQ(leased_workers_.size(), 1);
  ASSERT_EQ(pool_.workers.size(), 0);
  ASSERT_EQ(node_info_calls_, 0);

  RayTask finished_task;
  task_manager_.TaskFinished(leased_workers_.begin()->second, &finished_task);
  ASSERT_EQ(finished_task.GetTaskSpecification().TaskId(),
            task.GetTaskSpecification().TaskId());

  // Block the worker. Which releases only the CPU resource.
  task_manager_.ReleaseCpuResourcesFromUnblockedWorker(worker);

  AssertNoLeaks();
}

TEST_F(ClusterTaskManagerTest, NoFeasibleNodeTest) {
  std::shared_ptr<MockWorker> worker =
      std::make_shared<MockWorker>(WorkerID::FromRandom(), 1234);
  pool_.PushWorker(std::dynamic_pointer_cast<WorkerInterface>(worker));

  RayTask task = CreateTask({{ray::kCPU_ResourceLabel, 999}});
  rpc::RequestWorkerLeaseReply reply;

  bool callback_called = false;
  bool *callback_called_ptr = &callback_called;
  auto callback = [callback_called_ptr](Status, std::function<void()>,
                                        std::function<void()>) {
    *callback_called_ptr = true;
  };

  task_manager_.QueueAndScheduleTask(task, &reply, callback);
  pool_.TriggerCallbacks();

  ASSERT_FALSE(callback_called);
  ASSERT_EQ(leased_workers_.size(), 0);
  // Worker is unused.
  ASSERT_EQ(pool_.workers.size(), 1);
  ASSERT_EQ(node_info_calls_, 0);
}

TEST_F(ClusterTaskManagerTest, ResourceTakenWhileResolving) {
  /*
    Test the race condition in which a task is assigned to a node, but cannot
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
  auto callback = [num_callbacks_ptr](Status, std::function<void()>,
                                      std::function<void()>) {
    (*num_callbacks_ptr) = *num_callbacks_ptr + 1;
  };

  /* Blocked on dependencies */
  auto task = CreateTask({{ray::kCPU_ResourceLabel, 5}}, 2);
  auto missing_arg = task.GetTaskSpecification().GetDependencyIds()[0];
  missing_objects_.insert(missing_arg);
  std::unordered_set<TaskID> expected_subscribed_tasks = {
      task.GetTaskSpecification().TaskId()};
  task_manager_.QueueAndScheduleTask(task, &reply, callback);
  pool_.TriggerCallbacks();
  ASSERT_EQ(dependency_manager_.subscribed_tasks, expected_subscribed_tasks);

  ASSERT_EQ(num_callbacks, 0);
  ASSERT_EQ(leased_workers_.size(), 0);
  ASSERT_EQ(pool_.workers.size(), 2);
  // It's important that we don't pop the worker until we need to. See
  // https://github.com/ray-project/ray/issues/13725.
  ASSERT_EQ(pool_.num_pops, 0);

  /* This task can run */
  auto task2 = CreateTask({{ray::kCPU_ResourceLabel, 5}}, 1);
  task_manager_.QueueAndScheduleTask(task2, &reply, callback);
  pool_.TriggerCallbacks();
  ASSERT_EQ(dependency_manager_.subscribed_tasks, expected_subscribed_tasks);

  AssertPinnedTaskArgumentsPresent(task2);
  ASSERT_EQ(num_callbacks, 1);
  ASSERT_EQ(leased_workers_.size(), 1);
  ASSERT_EQ(pool_.workers.size(), 1);
  ASSERT_EQ(pool_.num_pops, 1);

  /* First task is unblocked now, but resources are no longer available */
  missing_objects_.erase(missing_arg);
  auto id = task.GetTaskSpecification().TaskId();
  std::vector<TaskID> unblocked = {id};
  task_manager_.TasksUnblocked(unblocked);
  ASSERT_EQ(dependency_manager_.subscribed_tasks, expected_subscribed_tasks);

  AssertPinnedTaskArgumentsPresent(task2);
  ASSERT_EQ(num_callbacks, 1);
  ASSERT_EQ(leased_workers_.size(), 1);
  ASSERT_EQ(pool_.workers.size(), 1);
  ASSERT_EQ(pool_.num_pops, 1);

  /* Second task finishes, making space for the original task */
  RayTask finished_task;
  task_manager_.TaskFinished(leased_workers_.begin()->second, &finished_task);
  leased_workers_.clear();

  task_manager_.ScheduleAndDispatchTasks();
  pool_.TriggerCallbacks();
  ASSERT_TRUE(dependency_manager_.subscribed_tasks.empty());

  // Task2 is now done so task can run.
  AssertPinnedTaskArgumentsPresent(task);
  ASSERT_EQ(num_callbacks, 2);
  ASSERT_EQ(leased_workers_.size(), 1);
  ASSERT_EQ(pool_.workers.size(), 0);
  ASSERT_EQ(pool_.num_pops, 2);

  task_manager_.TaskFinished(leased_workers_.begin()->second, &finished_task);
  AssertNoLeaks();
}

TEST_F(ClusterTaskManagerTest, TestSpillAfterAssigned) {
  /*
    Test the race condition in which a task is assigned to the local node, but
    it cannot be run because a different task gets assigned the resources
    first. The un-runnable task should eventually get spilled back to another
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
  auto task = CreateTask({{ray::kCPU_ResourceLabel, 5}});
  rpc::RequestWorkerLeaseReply local_reply;
  task_manager_.QueueAndScheduleTask(task, &local_reply, callback);
  pool_.TriggerCallbacks();

  ASSERT_EQ(num_callbacks, 0);
  ASSERT_EQ(leased_workers_.size(), 0);

  // Resources are no longer available for the second.
  auto task2 = CreateTask({{ray::kCPU_ResourceLabel, 5}});
  rpc::RequestWorkerLeaseReply spillback_reply;
  task_manager_.QueueAndScheduleTask(task2, &spillback_reply, callback);
  pool_.TriggerCallbacks();

  // The second task was spilled.
  ASSERT_EQ(num_callbacks, 1);
  ASSERT_EQ(spillback_reply.retry_at_raylet_address().raylet_id(),
            remote_node_id.Binary());
  ASSERT_EQ(leased_workers_.size(), 0);

  // Two workers start. First task was dispatched now.
  pool_.PushWorker(std::static_pointer_cast<WorkerInterface>(worker));
  pool_.PushWorker(std::static_pointer_cast<WorkerInterface>(worker));
  task_manager_.ScheduleAndDispatchTasks();
  pool_.TriggerCallbacks();
  // Check that both tasks got removed from the queue.
  ASSERT_EQ(num_callbacks, 2);
  // The first task was dispatched.
  ASSERT_EQ(leased_workers_.size(), 1);
  // Leave one alive worker.
  ASSERT_EQ(pool_.workers.size(), 1);

  RayTask finished_task;
  task_manager_.TaskFinished(leased_workers_.begin()->second, &finished_task);
  ASSERT_EQ(finished_task.GetTaskSpecification().TaskId(),
            task.GetTaskSpecification().TaskId());

  AssertNoLeaks();
}

TEST_F(ClusterTaskManagerTest, TaskCancellationTest) {
  std::shared_ptr<MockWorker> worker =
      std::make_shared<MockWorker>(WorkerID::FromRandom(), 1234);
  RayTask task = CreateTask({{ray::kCPU_ResourceLabel, 1}});
  rpc::RequestWorkerLeaseReply reply;

  bool callback_called = false;
  bool *callback_called_ptr = &callback_called;
  auto callback = [callback_called_ptr](Status, std::function<void()>,
                                        std::function<void()>) {
    *callback_called_ptr = true;
  };

  // RayTask not queued so we can't cancel it.
  ASSERT_FALSE(task_manager_.CancelTask(task.GetTaskSpecification().TaskId()));

  task_manager_.QueueAndScheduleTask(task, &reply, callback);
  pool_.TriggerCallbacks();

  // RayTask is now in dispatch queue.
  callback_called = false;
  reply.Clear();
  ASSERT_TRUE(task_manager_.CancelTask(task.GetTaskSpecification().TaskId()));
  task_manager_.ScheduleAndDispatchTasks();
  pool_.TriggerCallbacks();
  // Task will not execute.
  ASSERT_TRUE(callback_called);
  ASSERT_TRUE(reply.canceled());
  ASSERT_EQ(leased_workers_.size(), 0);

  pool_.PushWorker(std::static_pointer_cast<WorkerInterface>(worker));
  task_manager_.QueueAndScheduleTask(task, &reply, callback);
  pool_.TriggerCallbacks();

  // RayTask is now running so we can't cancel it.
  callback_called = false;
  reply.Clear();
  ASSERT_FALSE(task_manager_.CancelTask(task.GetTaskSpecification().TaskId()));
  // RayTask will not execute.
  ASSERT_FALSE(reply.canceled());
  ASSERT_FALSE(callback_called);
  ASSERT_EQ(pool_.workers.size(), 0);
  ASSERT_EQ(leased_workers_.size(), 1);

  RayTask finished_task;
  task_manager_.TaskFinished(leased_workers_.begin()->second, &finished_task);
  ASSERT_EQ(finished_task.GetTaskSpecification().TaskId(),
            task.GetTaskSpecification().TaskId());

  AssertNoLeaks();
}

TEST_F(ClusterTaskManagerTest, TaskCancelInfeasibleTask) {
  /* Make sure cancelTask works for infeasible tasks */
  std::shared_ptr<MockWorker> worker =
      std::make_shared<MockWorker>(WorkerID::FromRandom(), 1234);
  pool_.PushWorker(std::static_pointer_cast<WorkerInterface>(worker));

  RayTask task = CreateTask({{ray::kCPU_ResourceLabel, 12}});
  rpc::RequestWorkerLeaseReply reply;

  bool callback_called = false;
  bool *callback_called_ptr = &callback_called;
  auto callback = [callback_called_ptr](Status, std::function<void()>,
                                        std::function<void()>) {
    *callback_called_ptr = true;
  };

  task_manager_.QueueAndScheduleTask(task, &reply, callback);
  pool_.TriggerCallbacks();

  // RayTask is now queued so cancellation works.
  ASSERT_TRUE(task_manager_.CancelTask(task.GetTaskSpecification().TaskId()));
  task_manager_.ScheduleAndDispatchTasks();
  pool_.TriggerCallbacks();
  // Task will not execute.
  ASSERT_TRUE(callback_called);
  ASSERT_TRUE(reply.canceled());
  ASSERT_EQ(leased_workers_.size(), 0);
  ASSERT_EQ(pool_.workers.size(), 1);

  // Althoug the feasible node is added, task shouldn't be executed because it is
  // cancelled.
  auto remote_node_id = NodeID::FromRandom();
  AddNode(remote_node_id, 12);
  task_manager_.ScheduleAndDispatchTasks();
  pool_.TriggerCallbacks();
  ASSERT_TRUE(callback_called);
  ASSERT_TRUE(reply.canceled());
  ASSERT_EQ(leased_workers_.size(), 0);
  ASSERT_EQ(pool_.workers.size(), 1);
  AssertNoLeaks();
}

TEST_F(ClusterTaskManagerTest, HeartbeatTest) {
  std::shared_ptr<MockWorker> worker =
      std::make_shared<MockWorker>(WorkerID::FromRandom(), 1234);
  pool_.PushWorker(std::static_pointer_cast<WorkerInterface>(worker));

  {
    RayTask task = CreateTask({{ray::kCPU_ResourceLabel, 1}});
    rpc::RequestWorkerLeaseReply reply;

    bool callback_called = false;
    bool *callback_called_ptr = &callback_called;
    auto callback = [callback_called_ptr](Status, std::function<void()>,
                                          std::function<void()>) {
      *callback_called_ptr = true;
    };

    task_manager_.QueueAndScheduleTask(task, &reply, callback);
    pool_.TriggerCallbacks();
    ASSERT_TRUE(callback_called);
    // Now {CPU: 7, GPU: 4, MEM:128}
  }

  {
    RayTask task = CreateTask({{ray::kCPU_ResourceLabel, 1}});
    rpc::RequestWorkerLeaseReply reply;

    bool callback_called = false;
    bool *callback_called_ptr = &callback_called;
    auto callback = [callback_called_ptr](Status, std::function<void()>,
                                          std::function<void()>) {
      *callback_called_ptr = true;
    };

    task_manager_.QueueAndScheduleTask(task, &reply, callback);
    pool_.TriggerCallbacks();
    ASSERT_FALSE(callback_called);  // No worker available.
    // Now {CPU: 7, GPU: 4, MEM:128} with 1 queued task.
  }

  {
    RayTask task =
        CreateTask({{ray::kCPU_ResourceLabel, 9}, {ray::kGPU_ResourceLabel, 5}});
    rpc::RequestWorkerLeaseReply reply;

    bool callback_called = false;
    bool *callback_called_ptr = &callback_called;
    auto callback = [callback_called_ptr](Status, std::function<void()>,
                                          std::function<void()>) {
      *callback_called_ptr = true;
    };

    task_manager_.QueueAndScheduleTask(task, &reply, callback);
    pool_.TriggerCallbacks();
    ASSERT_FALSE(callback_called);  // Infeasible.
    // Now there is also an infeasible task {CPU: 9}.
  }

  {
    RayTask task =
        CreateTask({{ray::kCPU_ResourceLabel, 10}, {ray::kGPU_ResourceLabel, 1}});
    rpc::RequestWorkerLeaseReply reply;

    bool callback_called = false;
    bool *callback_called_ptr = &callback_called;
    auto callback = [callback_called_ptr](Status, std::function<void()>,
                                          std::function<void()>) {
      *callback_called_ptr = true;
    };

    task_manager_.QueueAndScheduleTask(task, &reply, callback);
    pool_.TriggerCallbacks();
    ASSERT_FALSE(callback_called);  // Infeasible.
    // Now there is also an infeasible task {CPU: 10}.
  }

  {
    rpc::ResourcesData data;
    task_manager_.FillResourceUsage(data);

    auto load_by_shape =
        data.mutable_resource_load_by_shape()->mutable_resource_demands();
    ASSERT_EQ(load_by_shape->size(), 3);

    std::vector<std::vector<unsigned int>> expected = {
        // infeasible, ready, CPU, GPU, size
        {1, 0, 10, 1, 2},
        {1, 0, 9, 5, 2},
        {0, 1, 1, 0, 1}};

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

TEST_F(ClusterTaskManagerTest, BacklogReportTest) {
  /*
    Test basic scheduler functionality:
    1. Queue and attempt to schedule/dispatch atest with no workers available
    2. A worker becomes available, dispatch again.
   */
  rpc::RequestWorkerLeaseReply reply;
  bool callback_occurred = false;
  bool *callback_occurred_ptr = &callback_occurred;
  auto callback = [callback_occurred_ptr](Status, std::function<void()>,
                                          std::function<void()>) {
    *callback_occurred_ptr = true;
  };

  std::vector<TaskID> to_cancel;

  // Don't add these fist 2 tasks to `to_cancel`.
  for (int i = 0; i < 1; i++) {
    RayTask task = CreateTask({{ray::kCPU_ResourceLabel, 8}});
    task.SetBacklogSize(10 - i);
    task_manager_.QueueAndScheduleTask(task, &reply, callback);
    pool_.TriggerCallbacks();
  }

  for (int i = 1; i < 10; i++) {
    RayTask task = CreateTask({{ray::kCPU_ResourceLabel, 8}});
    task.SetBacklogSize(10 - i);
    task_manager_.QueueAndScheduleTask(task, &reply, callback);
    pool_.TriggerCallbacks();
    to_cancel.push_back(task.GetTaskSpecification().TaskId());
  }

  ASSERT_FALSE(callback_occurred);
  ASSERT_EQ(leased_workers_.size(), 0);
  ASSERT_EQ(pool_.workers.size(), 0);
  ASSERT_EQ(node_info_calls_, 0);

  {  // No tasks can run because the worker pool is empty.
    rpc::ResourcesData data;
    task_manager_.FillResourceUsage(data);
    auto resource_load_by_shape = data.resource_load_by_shape();
    auto shape1 = resource_load_by_shape.resource_demands()[0];

    ASSERT_EQ(shape1.backlog_size(), 55);
    ASSERT_EQ(shape1.num_infeasible_requests_queued(), 0);
    ASSERT_EQ(shape1.num_ready_requests_queued(), 10);
  }

  // Push a worker so the first task can run.
  std::shared_ptr<MockWorker> worker =
      std::make_shared<MockWorker>(WorkerID::FromRandom(), 1234);
  pool_.PushWorker(worker);
  task_manager_.ScheduleAndDispatchTasks();
  pool_.TriggerCallbacks();

  {
    rpc::ResourcesData data;
    task_manager_.FillResourceUsage(data);
    auto resource_load_by_shape = data.resource_load_by_shape();
    auto shape1 = resource_load_by_shape.resource_demands()[0];

    ASSERT_TRUE(callback_occurred);
    ASSERT_EQ(shape1.backlog_size(), 45);
    ASSERT_EQ(shape1.num_infeasible_requests_queued(), 0);
    ASSERT_EQ(shape1.num_ready_requests_queued(), 9);
  }

  // Cancel the rest.
  for (auto &task_id : to_cancel) {
    ASSERT_TRUE(task_manager_.CancelTask(task_id));
  }

  {
    rpc::ResourcesData data;
    task_manager_.FillResourceUsage(data);
    auto resource_load_by_shape = data.resource_load_by_shape();
    ASSERT_EQ(resource_load_by_shape.resource_demands().size(), 0);

    while (!leased_workers_.empty()) {
      RayTask finished_task;
      task_manager_.TaskFinished(leased_workers_.begin()->second, &finished_task);
      leased_workers_.erase(leased_workers_.begin());
    }
    AssertNoLeaks();
  }
}

TEST_F(ClusterTaskManagerTest, OwnerDeadTest) {
  /*
    Test the race condition in which the owner of a task dies while the task is pending.
    This is the essence of test_actor_advanced.py::test_pending_actor_removed_by_owner
   */
  RayTask task = CreateTask({{ray::kCPU_ResourceLabel, 4}});
  rpc::RequestWorkerLeaseReply reply;
  bool callback_occurred = false;
  bool *callback_occurred_ptr = &callback_occurred;
  auto callback = [callback_occurred_ptr](Status, std::function<void()>,
                                          std::function<void()>) {
    *callback_occurred_ptr = true;
  };

  std::shared_ptr<MockWorker> worker =
      std::make_shared<MockWorker>(WorkerID::FromRandom(), 1234);
  pool_.PushWorker(std::static_pointer_cast<WorkerInterface>(worker));

  is_owner_alive_ = false;
  task_manager_.QueueAndScheduleTask(task, &reply, callback);
  pool_.TriggerCallbacks();

  ASSERT_FALSE(callback_occurred);
  ASSERT_EQ(leased_workers_.size(), 0);
  ASSERT_EQ(pool_.workers.size(), 1);

  is_owner_alive_ = true;
  task_manager_.ScheduleAndDispatchTasks();
  pool_.TriggerCallbacks();

  ASSERT_FALSE(callback_occurred);
  ASSERT_EQ(leased_workers_.size(), 0);
  ASSERT_EQ(pool_.workers.size(), 1);
  AssertNoLeaks();
}

TEST_F(ClusterTaskManagerTest, TestInfeasibleTaskWarning) {
  /*
    Test if infeasible tasks warnings are printed.
   */
  // Create an infeasible task.
  RayTask task = CreateTask({{ray::kCPU_ResourceLabel, 12}});
  rpc::RequestWorkerLeaseReply reply;
  std::shared_ptr<bool> callback_occurred = std::make_shared<bool>(false);
  auto callback = [callback_occurred](Status, std::function<void()>,
                                      std::function<void()>) {
    *callback_occurred = true;
  };
  task_manager_.QueueAndScheduleTask(task, &reply, callback);
  pool_.TriggerCallbacks();
  ASSERT_EQ(announce_infeasible_task_calls_, 1);

  // Infeasible warning shouldn't be reprinted when the previous task is still infeasible
  // after adding a new node.
  AddNode(NodeID::FromRandom(), 8);
  std::shared_ptr<MockWorker> worker =
      std::make_shared<MockWorker>(WorkerID::FromRandom(), 1234);
  pool_.PushWorker(std::static_pointer_cast<WorkerInterface>(worker));
  task_manager_.ScheduleAndDispatchTasks();
  pool_.TriggerCallbacks();
  // Task shouldn't be scheduled yet.
  ASSERT_EQ(announce_infeasible_task_calls_, 1);
  ASSERT_FALSE(*callback_occurred);
  ASSERT_EQ(leased_workers_.size(), 0);
  ASSERT_EQ(pool_.workers.size(), 1);

  // Now we have a node that is feasible to schedule the task. Make sure the infeasible
  // task is spillbacked properly.
  auto remote_node_id = NodeID::FromRandom();
  AddNode(remote_node_id, 12);
  task_manager_.ScheduleAndDispatchTasks();
  pool_.TriggerCallbacks();
  // Make sure nothing happens locally.
  ASSERT_EQ(announce_infeasible_task_calls_, 1);
  ASSERT_TRUE(*callback_occurred);
  ASSERT_EQ(leased_workers_.size(), 0);
  ASSERT_EQ(pool_.workers.size(), 1);
  // Make sure the spillback callback is called.
  ASSERT_EQ(reply.retry_at_raylet_address().raylet_id(), remote_node_id.Binary());
  AssertNoLeaks();
}

TEST_F(ClusterTaskManagerTest, TestMultipleInfeasibleTasksWarnOnce) {
  /*
    Test infeasible warning is printed only once when the same shape is queued again.
   */

  // Make sure the first infeasible task announces warning.
  RayTask task = CreateTask({{ray::kCPU_ResourceLabel, 12}});
  rpc::RequestWorkerLeaseReply reply;
  std::shared_ptr<bool> callback_occurred = std::make_shared<bool>(false);
  auto callback = [callback_occurred](Status, std::function<void()>,
                                      std::function<void()>) {
    *callback_occurred = true;
  };
  task_manager_.QueueAndScheduleTask(task, &reply, callback);
  pool_.TriggerCallbacks();
  ASSERT_EQ(announce_infeasible_task_calls_, 1);

  // Make sure the same shape infeasible task won't be announced.
  RayTask task2 = CreateTask({{ray::kCPU_ResourceLabel, 12}});
  rpc::RequestWorkerLeaseReply reply2;
  std::shared_ptr<bool> callback_occurred2 = std::make_shared<bool>(false);
  auto callback2 = [callback_occurred2](Status, std::function<void()>,
                                        std::function<void()>) {
    *callback_occurred2 = true;
  };
  task_manager_.QueueAndScheduleTask(task2, &reply2, callback2);
  pool_.TriggerCallbacks();
  ASSERT_EQ(announce_infeasible_task_calls_, 1);
}

TEST_F(ClusterTaskManagerTest, TestAnyPendingTasks) {
  /*
    Check if the manager can correctly identify pending tasks.
   */
  std::shared_ptr<MockWorker> worker =
      std::make_shared<MockWorker>(WorkerID::FromRandom(), 1234);
  pool_.PushWorker(std::static_pointer_cast<WorkerInterface>(worker));

  // task1: running
  RayTask task = CreateTask({{ray::kCPU_ResourceLabel, 6}});
  rpc::RequestWorkerLeaseReply reply;
  std::shared_ptr<bool> callback_occurred = std::make_shared<bool>(false);
  auto callback = [callback_occurred](Status, std::function<void()>,
                                      std::function<void()>) {
    *callback_occurred = true;
  };
  task_manager_.QueueAndScheduleTask(task, &reply, callback);
  pool_.TriggerCallbacks();
  ASSERT_TRUE(*callback_occurred);
  ASSERT_EQ(leased_workers_.size(), 1);
  ASSERT_EQ(pool_.workers.size(), 0);

  // task1: running. Progress is made, and there's no deadlock.
  ray::RayTask exemplar;
  bool any_pending = false;
  int pending_actor_creations = 0;
  int pending_tasks = 0;
  ASSERT_FALSE(task_manager_.AnyPendingTasks(&exemplar, &any_pending,
                                             &pending_actor_creations, &pending_tasks));

  // task1: running, task2: queued.
  RayTask task2 = CreateTask({{ray::kCPU_ResourceLabel, 6}});
  rpc::RequestWorkerLeaseReply reply2;
  std::shared_ptr<bool> callback_occurred2 = std::make_shared<bool>(false);
  auto callback2 = [callback_occurred2](Status, std::function<void()>,
                                        std::function<void()>) {
    *callback_occurred2 = true;
  };
  task_manager_.QueueAndScheduleTask(task2, &reply2, callback2);
  pool_.TriggerCallbacks();
  ASSERT_FALSE(*callback_occurred2);
  ASSERT_TRUE(task_manager_.AnyPendingTasks(&exemplar, &any_pending,
                                            &pending_actor_creations, &pending_tasks));
}

TEST_F(ClusterTaskManagerTest, ArgumentEvicted) {
  /*
    Test the task's dependencies becoming local, then one of the arguments is
    evicted. The task should go from waiting -> dispatch -> waiting.
  */
  std::shared_ptr<MockWorker> worker =
      std::make_shared<MockWorker>(WorkerID::FromRandom(), 1234);
  pool_.PushWorker(std::static_pointer_cast<WorkerInterface>(worker));

  rpc::RequestWorkerLeaseReply reply;
  int num_callbacks = 0;
  int *num_callbacks_ptr = &num_callbacks;
  auto callback = [num_callbacks_ptr](Status, std::function<void()>,
                                      std::function<void()>) {
    (*num_callbacks_ptr) = *num_callbacks_ptr + 1;
  };

  /* Blocked on dependencies */
  auto task = CreateTask({{ray::kCPU_ResourceLabel, 5}}, 2);
  auto missing_arg = task.GetTaskSpecification().GetDependencyIds()[0];
  missing_objects_.insert(missing_arg);
  std::unordered_set<TaskID> expected_subscribed_tasks = {
      task.GetTaskSpecification().TaskId()};
  task_manager_.QueueAndScheduleTask(task, &reply, callback);
  pool_.TriggerCallbacks();
  ASSERT_EQ(dependency_manager_.subscribed_tasks, expected_subscribed_tasks);
  ASSERT_EQ(num_callbacks, 0);
  ASSERT_EQ(leased_workers_.size(), 0);

  /* RayTask is unblocked now */
  missing_objects_.erase(missing_arg);
  pool_.workers.clear();
  auto id = task.GetTaskSpecification().TaskId();
  task_manager_.TasksUnblocked({id});
  ASSERT_EQ(dependency_manager_.subscribed_tasks, expected_subscribed_tasks);
  ASSERT_EQ(num_callbacks, 0);
  ASSERT_EQ(leased_workers_.size(), 0);

  /* Worker available and arguments available */
  pool_.PushWorker(std::static_pointer_cast<WorkerInterface>(worker));
  task_manager_.ScheduleAndDispatchTasks();
  pool_.TriggerCallbacks();
  ASSERT_EQ(num_callbacks, 1);
  ASSERT_EQ(leased_workers_.size(), 1);

  RayTask finished_task;
  task_manager_.TaskFinished(leased_workers_.begin()->second, &finished_task);
  ASSERT_EQ(finished_task.GetTaskSpecification().TaskId(),
            task.GetTaskSpecification().TaskId());

  AssertNoLeaks();
}

TEST_F(ClusterTaskManagerTest, FeasibleToNonFeasible) {
  // Test the case, when resources changes in local node, the feasible task should
  // able to transfer to infeasible task
  std::shared_ptr<MockWorker> worker =
      std::make_shared<MockWorker>(WorkerID::FromRandom(), 1234);
  pool_.PushWorker(std::static_pointer_cast<WorkerInterface>(worker));
  RayTask task1 = CreateTask({{ray::kCPU_ResourceLabel, 4}});
  rpc::RequestWorkerLeaseReply reply1;
  bool callback_occurred1 = false;
  task_manager_.QueueAndScheduleTask(
      task1, &reply1,
      [&callback_occurred1](Status, std::function<void()>, std::function<void()>) {
        callback_occurred1 = true;
      });
  pool_.TriggerCallbacks();
  ASSERT_EQ(leased_workers_.size(), 1);
  ASSERT_TRUE(callback_occurred1);
  ASSERT_EQ(pool_.workers.size(), 0);
  ASSERT_EQ(task_manager_.tasks_to_schedule_.size(), 0);
  ASSERT_EQ(task_manager_.tasks_to_dispatch_.size(), 0);
  ASSERT_EQ(task_manager_.infeasible_tasks_.size(), 0);

  // Delete cpu resource of local node, then task 2 should be turned into
  // infeasible.
  scheduler_->DeleteLocalResource(ray::kCPU_ResourceLabel);

  RayTask task2 = CreateTask({{ray::kCPU_ResourceLabel, 4}});
  rpc::RequestWorkerLeaseReply reply2;
  bool callback_occurred2 = false;
  task_manager_.QueueAndScheduleTask(
      task2, &reply2,
      [&callback_occurred2](Status, std::function<void()>, std::function<void()>) {
        callback_occurred2 = true;
      });
  pool_.TriggerCallbacks();
  ASSERT_EQ(leased_workers_.size(), 1);
  ASSERT_FALSE(callback_occurred2);
  ASSERT_EQ(pool_.workers.size(), 0);
  ASSERT_EQ(task_manager_.tasks_to_schedule_.size(), 0);
  ASSERT_EQ(task_manager_.tasks_to_dispatch_.size(), 0);
  ASSERT_EQ(task_manager_.infeasible_tasks_.size(), 1);

  RayTask finished_task;
  task_manager_.TaskFinished(leased_workers_.begin()->second, &finished_task);
  ASSERT_EQ(finished_task.GetTaskSpecification().TaskId(),
            task1.GetTaskSpecification().TaskId());
}

TEST_F(ClusterTaskManagerTest, RleaseAndReturnWorkerCpuResources) {
  const NodeResources &node_resources = scheduler_->GetLocalNodeResources();
  ASSERT_EQ(node_resources.predefined_resources[PredefinedResources::CPU].available, 8);
  ASSERT_EQ(node_resources.predefined_resources[PredefinedResources::GPU].available, 4);

  auto worker = std::make_shared<MockWorker>(WorkerID::FromRandom(), 1234);

  // Check failed as the worker has no allocated resource instances.
  ASSERT_FALSE(task_manager_.ReleaseCpuResourcesFromUnblockedWorker(worker));

  auto node_resource_instances = scheduler_->GetLocalResources();
  auto available_resource_instances =
      node_resource_instances.GetAvailableResourceInstances();

  auto allocated_instances = std::make_shared<TaskResourceInstances>();
  const std::unordered_map<std::string, double> task_spec = {{"CPU", 1.}, {"GPU", 1.}};
  ASSERT_TRUE(scheduler_->AllocateLocalTaskResources(task_spec, allocated_instances));
  worker->SetAllocatedInstances(allocated_instances);

  // Check that the resoruces are allocated successfully.
  ASSERT_EQ(node_resources.predefined_resources[PredefinedResources::CPU].available, 7);
  ASSERT_EQ(node_resources.predefined_resources[PredefinedResources::GPU].available, 3);

  // Check that the cpu resources are released successfully.
  ASSERT_TRUE(task_manager_.ReleaseCpuResourcesFromUnblockedWorker(worker));

  // Check that only cpu resources are released.
  ASSERT_EQ(node_resources.predefined_resources[PredefinedResources::CPU].available, 8);
  ASSERT_EQ(node_resources.predefined_resources[PredefinedResources::GPU].available, 3);

  // Mark worker as blocked.
  worker->MarkBlocked();
  // Check failed as the worker is blocked.
  ASSERT_FALSE(task_manager_.ReleaseCpuResourcesFromUnblockedWorker(worker));
  // Check nothing will be changed.
  ASSERT_EQ(node_resources.predefined_resources[PredefinedResources::CPU].available, 8);
  ASSERT_EQ(node_resources.predefined_resources[PredefinedResources::GPU].available, 3);

  // Check that the cpu resources are returned back to worker successfully.
  ASSERT_TRUE(task_manager_.ReturnCpuResourcesToBlockedWorker(worker));

  // Check that only cpu resources are returned back to the worker.
  ASSERT_EQ(node_resources.predefined_resources[PredefinedResources::CPU].available, 7);
  ASSERT_EQ(node_resources.predefined_resources[PredefinedResources::GPU].available, 3);

  // Mark worker as unblocked.
  worker->MarkUnblocked();
  ASSERT_FALSE(task_manager_.ReturnCpuResourcesToBlockedWorker(worker));
  // Check nothing will be changed.
  ASSERT_EQ(node_resources.predefined_resources[PredefinedResources::CPU].available, 7);
  ASSERT_EQ(node_resources.predefined_resources[PredefinedResources::GPU].available, 3);
}

TEST_F(ClusterTaskManagerTest, TestSpillWaitingTasks) {
  // Cases to check:
  // - resources available locally, task dependencies being fetched -> do not spill.
  // - resources available locally, task dependencies blocked -> spill.
  // - resources not available locally -> spill.
  std::vector<RayTask> tasks;
  std::vector<std::unique_ptr<rpc::RequestWorkerLeaseReply>> replies;
  int num_callbacks = 0;
  auto callback = [&](Status, std::function<void()>, std::function<void()>) {
    num_callbacks++;
  };
  for (int i = 0; i < 5; i++) {
    RayTask task = CreateTask({{ray::kCPU_ResourceLabel, 8}}, /*num_args=*/1);
    tasks.push_back(task);
    replies.push_back(std::make_unique<rpc::RequestWorkerLeaseReply>());
    // All tasks except the last one added are waiting for dependencies.
    if (i < 4) {
      auto missing_arg = task.GetTaskSpecification().GetDependencyIds()[0];
      missing_objects_.insert(missing_arg);
    }
    task_manager_.QueueAndScheduleTask(task, replies[i].get(), callback);
    pool_.TriggerCallbacks();
  }
  ASSERT_EQ(num_callbacks, 0);
  // Local resources could only dispatch one task.
  ASSERT_EQ(NumTasksWaitingForWorker(), 1);

  auto remote_node_id = NodeID::FromRandom();
  AddNode(remote_node_id, 16);
  // We are fetching dependencies for all waiting tasks but we have no enough
  // resources available locally to schedule tasks except the first.
  // We should only spill up to the remote node's resource availability.
  task_manager_.ScheduleAndDispatchTasks();
  ASSERT_EQ(num_callbacks, 2);
  // Spill from the back of the waiting queue.
  ASSERT_EQ(replies[0]->retry_at_raylet_address().raylet_id(), "");
  ASSERT_EQ(replies[1]->retry_at_raylet_address().raylet_id(), "");
  ASSERT_EQ(replies[2]->retry_at_raylet_address().raylet_id(), remote_node_id.Binary());
  ASSERT_EQ(replies[3]->retry_at_raylet_address().raylet_id(), remote_node_id.Binary());
  ASSERT_FALSE(task_manager_.CancelTask(tasks[2].GetTaskSpecification().TaskId()));
  ASSERT_FALSE(task_manager_.CancelTask(tasks[3].GetTaskSpecification().TaskId()));
  // Do not spill back tasks ready to dispatch.
  ASSERT_EQ(replies[4]->retry_at_raylet_address().raylet_id(), "");

  AddNode(remote_node_id, 8);
  // Dispatch the ready task.
  std::shared_ptr<MockWorker> worker =
      std::make_shared<MockWorker>(WorkerID::FromRandom(), 1234);
  pool_.PushWorker(std::dynamic_pointer_cast<WorkerInterface>(worker));
  task_manager_.ScheduleAndDispatchTasks();
  pool_.TriggerCallbacks();
  ASSERT_EQ(num_callbacks, 4);
  // One waiting task spilled.
  ASSERT_EQ(replies[0]->retry_at_raylet_address().raylet_id(), "");
  ASSERT_EQ(replies[1]->retry_at_raylet_address().raylet_id(), remote_node_id.Binary());
  ASSERT_FALSE(task_manager_.CancelTask(tasks[1].GetTaskSpecification().TaskId()));
  // One task dispatched.
  ASSERT_EQ(replies[4]->worker_address().port(), 1234);

  // Spillback is idempotent.
  task_manager_.ScheduleAndDispatchTasks();
  pool_.TriggerCallbacks();
  ASSERT_EQ(num_callbacks, 4);
  // One waiting task spilled.
  ASSERT_EQ(replies[0]->retry_at_raylet_address().raylet_id(), "");
  ASSERT_EQ(replies[1]->retry_at_raylet_address().raylet_id(), remote_node_id.Binary());
  ASSERT_FALSE(task_manager_.CancelTask(tasks[1].GetTaskSpecification().TaskId()));
  // One task dispatched.
  ASSERT_EQ(replies[4]->worker_address().port(), 1234);

  RayTask finished_task;
  task_manager_.TaskFinished(leased_workers_.begin()->second, &finished_task);
  leased_workers_.clear();
  ASSERT_TRUE(task_manager_.CancelTask(tasks[0].GetTaskSpecification().TaskId()));
  AssertNoLeaks();
}

TEST_F(ClusterTaskManagerTest, PinnedArgsMemoryTest) {
  /*
    Total memory required by executing tasks' args stays under the specified
    threshold.
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
  auto callback = [num_callbacks_ptr](Status, std::function<void()>,
                                      std::function<void()>) {
    (*num_callbacks_ptr) = *num_callbacks_ptr + 1;
  };

  // This task can run.
  default_arg_size_ = 600;
  auto task = CreateTask({{ray::kCPU_ResourceLabel, 1}}, 1);
  task_manager_.QueueAndScheduleTask(task, &reply, callback);
  pool_.TriggerCallbacks();
  ASSERT_EQ(num_callbacks, 1);
  ASSERT_EQ(leased_workers_.size(), 1);
  ASSERT_EQ(pool_.workers.size(), 1);
  AssertPinnedTaskArgumentsPresent(task);

  // This task cannot run because it would put us over the memory threshold.
  auto task2 = CreateTask({{ray::kCPU_ResourceLabel, 1}}, 1);
  task_manager_.QueueAndScheduleTask(task2, &reply, callback);
  pool_.TriggerCallbacks();
  ASSERT_EQ(num_callbacks, 1);
  ASSERT_EQ(leased_workers_.size(), 1);
  ASSERT_EQ(pool_.workers.size(), 1);

  /* First task finishes, freeing memory for the second task */
  RayTask finished_task;
  task_manager_.TaskFinished(leased_workers_.begin()->second, &finished_task);
  leased_workers_.clear();

  task_manager_.ScheduleAndDispatchTasks();
  pool_.TriggerCallbacks();
  AssertPinnedTaskArgumentsPresent(task2);
  ASSERT_EQ(num_callbacks, 2);
  ASSERT_EQ(leased_workers_.size(), 1);
  ASSERT_EQ(pool_.workers.size(), 0);

  task_manager_.TaskFinished(leased_workers_.begin()->second, &finished_task);
  leased_workers_.clear();
  AssertNoLeaks();
}

TEST_F(ClusterTaskManagerTest, PinnedArgsSameMemoryTest) {
  /*
   * Two tasks that depend on the same object can run concurrently.
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
  auto callback = [num_callbacks_ptr](Status, std::function<void()>,
                                      std::function<void()>) {
    (*num_callbacks_ptr) = *num_callbacks_ptr + 1;
  };

  // This task can run.
  default_arg_size_ = 600;
  auto task = CreateTask({{ray::kCPU_ResourceLabel, 1}}, 1);
  task_manager_.QueueAndScheduleTask(task, &reply, callback);
  pool_.TriggerCallbacks();
  ASSERT_EQ(num_callbacks, 1);
  ASSERT_EQ(leased_workers_.size(), 1);
  ASSERT_EQ(pool_.workers.size(), 1);
  AssertPinnedTaskArgumentsPresent(task);

  // This task can run because it depends on the same object as the first task.
  auto task2 = CreateTask({{ray::kCPU_ResourceLabel, 1}}, 1,
                          task.GetTaskSpecification().GetDependencyIds());
  task_manager_.QueueAndScheduleTask(task2, &reply, callback);
  pool_.TriggerCallbacks();
  ASSERT_EQ(num_callbacks, 2);
  ASSERT_EQ(leased_workers_.size(), 2);
  ASSERT_EQ(pool_.workers.size(), 0);

  RayTask finished_task;
  for (auto &worker : leased_workers_) {
    task_manager_.TaskFinished(worker.second, &finished_task);
  }
  AssertNoLeaks();
}

TEST_F(ClusterTaskManagerTest, LargeArgsNoStarvationTest) {
  std::shared_ptr<MockWorker> worker =
      std::make_shared<MockWorker>(WorkerID::FromRandom(), 1234);
  pool_.PushWorker(std::static_pointer_cast<WorkerInterface>(worker));

  rpc::RequestWorkerLeaseReply reply;
  int num_callbacks = 0;
  int *num_callbacks_ptr = &num_callbacks;
  auto callback = [num_callbacks_ptr](Status, std::function<void()>,
                                      std::function<void()>) {
    (*num_callbacks_ptr) = *num_callbacks_ptr + 1;
  };

  default_arg_size_ = 2000;
  auto task = CreateTask({{ray::kCPU_ResourceLabel, 1}}, 1);
  pool_.PushWorker(std::static_pointer_cast<WorkerInterface>(worker));
  task_manager_.QueueAndScheduleTask(task, &reply, callback);
  pool_.TriggerCallbacks();
  ASSERT_EQ(num_callbacks, 1);
  ASSERT_EQ(leased_workers_.size(), 1);
  AssertPinnedTaskArgumentsPresent(task);

  RayTask finished_task;
  task_manager_.TaskFinished(leased_workers_.begin()->second, &finished_task);
  AssertNoLeaks();
}

TEST_F(ClusterTaskManagerTest, TestResourceDiff) {
  // When scheduling_resources is null, resource is always marked as changed
  rpc::ResourcesData resource_data;
  task_manager_.FillResourceUsage(resource_data, nullptr);
  ASSERT_TRUE(resource_data.resource_load_changed());
  auto scheduling_resources = std::make_shared<SchedulingResources>();
  // Same resources(empty), not changed.
  resource_data.set_resource_load_changed(false);
  task_manager_.FillResourceUsage(resource_data, scheduling_resources);
  ASSERT_FALSE(resource_data.resource_load_changed());
  // Resource changed.
  resource_data.set_resource_load_changed(false);
  ResourceSet res;
  res.AddOrUpdateResource("CPU", 100);
  scheduling_resources->SetLoadResources(std::move(res));
  task_manager_.FillResourceUsage(resource_data, scheduling_resources);
  ASSERT_TRUE(resource_data.resource_load_changed());
}

TEST_F(ClusterTaskManagerTest, PopWorkerExactlyOnce) {
  // Create and queue one task.
  std::string serialized_runtime_env = "mock_env";
  RayTask task = CreateTask({{ray::kCPU_ResourceLabel, 4}}, /*num_args=*/0, /*args=*/{},
                            serialized_runtime_env);
  auto runtime_env_hash = task.GetTaskSpecification().GetRuntimeEnvHash();
  rpc::RequestWorkerLeaseReply reply;
  bool callback_occurred = false;
  bool *callback_occurred_ptr = &callback_occurred;
  auto callback = [callback_occurred_ptr](Status, std::function<void()>,
                                          std::function<void()>) {
    *callback_occurred_ptr = true;
  };

  task_manager_.QueueAndScheduleTask(task, &reply, callback);

  // Make sure callback doesn't occurred.
  ASSERT_FALSE(callback_occurred);
  ASSERT_EQ(leased_workers_.size(), 0);
  ASSERT_EQ(pool_.workers.size(), 0);
  // Popworker was called once.
  ASSERT_EQ(pool_.CallbackSize(runtime_env_hash), 1);
  // Try to schedule and dispatch tasks.
  task_manager_.ScheduleAndDispatchTasks();
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
  // Try to schedule and dispatch tasks.
  task_manager_.ScheduleAndDispatchTasks();
  // Worker has been popped. Don't call `PopWorker` repeatedly.
  ASSERT_EQ(pool_.CallbackSize(runtime_env_hash), 0);

  RayTask finished_task;
  task_manager_.TaskFinished(leased_workers_.begin()->second, &finished_task);
  ASSERT_EQ(finished_task.GetTaskSpecification().TaskId(),
            task.GetTaskSpecification().TaskId());
  AssertNoLeaks();
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

}  // namespace raylet

}  // namespace ray
