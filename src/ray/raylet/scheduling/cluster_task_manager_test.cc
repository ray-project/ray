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
  std::shared_ptr<WorkerInterface> PopWorker(const TaskSpecification &task_spec) {
    if (workers.empty()) {
      return nullptr;
    }
    auto worker_ptr = workers.front();
    workers.pop_front();
    return worker_ptr;
  }

  void PushWorker(const std::shared_ptr<WorkerInterface> &worker) {
    workers.push_front(worker);
  }

  std::list<std::shared_ptr<WorkerInterface>> workers;
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

Task CreateTask(const std::unordered_map<std::string, double> &required_resources,
                int num_args = 0) {
  TaskSpecBuilder spec_builder;
  TaskID id = RandomTaskId();
  JobID job_id = RandomJobId();
  rpc::Address address;
  spec_builder.SetCommonTaskSpec(id, "dummy_task", Language::PYTHON,
                                 FunctionDescriptorBuilder::BuildPython("", "", "", ""),
                                 job_id, TaskID::Nil(), 0, TaskID::Nil(), address, 0,
                                 required_resources, {},
                                 std::make_pair(PlacementGroupID::Nil(), -1), true, "");

  for (int i = 0; i < num_args; i++) {
    ObjectID put_id = ObjectID::FromIndex(TaskID::Nil(), /*index=*/i + 1);
    spec_builder.AddArg(TaskArgByReference(put_id, rpc::Address()));
  }

  rpc::TaskExecutionSpec execution_spec_message;
  execution_spec_message.set_num_forwards(1);
  return Task(spec_builder.Build(), TaskExecutionSpecification(execution_spec_message));
}

class MockTaskDependencyManager : public TaskDependencyManagerInterface {
 public:
  bool RequestTaskDependencies(
      const TaskID &task_id, const std::vector<rpc::ObjectReference> &required_objects) {
    RAY_CHECK(subscribed_tasks.insert(task_id).second);
    return task_ready_;
  }

  void RemoveTaskDependencies(const TaskID &task_id) {
    RAY_CHECK(subscribed_tasks.erase(task_id));
  }

  bool IsTaskReady(const TaskID &task_id) const { return task_ready_; }

  bool task_ready_ = true;

  std::unordered_set<TaskID> subscribed_tasks;
};

class ClusterTaskManagerTest : public ::testing::Test {
 public:
  ClusterTaskManagerTest()
      : id_(NodeID::FromRandom()),
        scheduler_(CreateSingleNodeScheduler(id_.Binary())),
        is_owner_alive_(true),
        node_info_calls_(0),
        announce_infeasible_task_calls_(0),
        task_manager_(id_, scheduler_, dependency_manager_,
                      [this](const WorkerID &worker_id, const NodeID &node_id) {
                        return is_owner_alive_;
                      },
                      [this](const NodeID &node_id) {
                        node_info_calls_++;
                        return node_info_[node_id];
                      },
                      [this](const Task &task) { announce_infeasible_task_calls_++; },
                      pool_, leased_workers_) {}

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
    ASSERT_TRUE(task_manager_.waiting_tasks_.empty());
    ASSERT_TRUE(task_manager_.infeasible_tasks_.empty());
    ASSERT_TRUE(dependency_manager_.subscribed_tasks.empty());
  }

  NodeID id_;
  std::shared_ptr<ClusterResourceScheduler> scheduler_;
  MockWorkerPool pool_;
  std::unordered_map<WorkerID, std::shared_ptr<WorkerInterface>> leased_workers_;

  bool is_owner_alive_;

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
  Task task = CreateTask({{ray::kCPU_ResourceLabel, 4}});
  rpc::RequestWorkerLeaseReply reply;
  bool callback_occurred = false;
  bool *callback_occurred_ptr = &callback_occurred;
  auto callback = [callback_occurred_ptr](Status, std::function<void()>,
                                          std::function<void()>) {
    *callback_occurred_ptr = true;
  };

  task_manager_.QueueAndScheduleTask(task, &reply, callback);

  ASSERT_FALSE(callback_occurred);
  ASSERT_EQ(leased_workers_.size(), 0);
  ASSERT_EQ(pool_.workers.size(), 0);

  std::shared_ptr<MockWorker> worker =
      std::make_shared<MockWorker>(WorkerID::FromRandom(), 1234);
  pool_.PushWorker(std::dynamic_pointer_cast<WorkerInterface>(worker));

  task_manager_.ScheduleAndDispatchTasks();

  ASSERT_TRUE(callback_occurred);
  ASSERT_EQ(leased_workers_.size(), 1);
  ASSERT_EQ(pool_.workers.size(), 0);
  ASSERT_EQ(node_info_calls_, 0);

  AssertNoLeaks();
}

TEST_F(ClusterTaskManagerTest, NoFeasibleNodeTest) {
  std::shared_ptr<MockWorker> worker =
      std::make_shared<MockWorker>(WorkerID::FromRandom(), 1234);
  pool_.PushWorker(std::dynamic_pointer_cast<WorkerInterface>(worker));

  Task task = CreateTask({{ray::kCPU_ResourceLabel, 999}});
  rpc::RequestWorkerLeaseReply reply;

  bool callback_called = false;
  bool *callback_called_ptr = &callback_called;
  auto callback = [callback_called_ptr](Status, std::function<void()>,
                                        std::function<void()>) {
    *callback_called_ptr = true;
  };

  task_manager_.QueueAndScheduleTask(task, &reply, callback);

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
  pool_.PushWorker(std::dynamic_pointer_cast<WorkerInterface>(worker2));
  pool_.PushWorker(std::dynamic_pointer_cast<WorkerInterface>(worker));

  rpc::RequestWorkerLeaseReply reply;
  int num_callbacks = 0;
  int *num_callbacks_ptr = &num_callbacks;
  auto callback = [num_callbacks_ptr](Status, std::function<void()>,
                                      std::function<void()>) {
    (*num_callbacks_ptr) = *num_callbacks_ptr + 1;
  };

  /* Blocked on dependencies */
  dependency_manager_.task_ready_ = false;
  auto task = CreateTask({{ray::kCPU_ResourceLabel, 5}}, 1);
  std::unordered_set<TaskID> expected_subscribed_tasks = {
      task.GetTaskSpecification().TaskId()};
  task_manager_.QueueAndScheduleTask(task, &reply, callback);
  ASSERT_EQ(dependency_manager_.subscribed_tasks, expected_subscribed_tasks);

  ASSERT_EQ(num_callbacks, 0);
  ASSERT_EQ(leased_workers_.size(), 0);
  ASSERT_EQ(pool_.workers.size(), 2);

  /* This task can run */
  auto task2 = CreateTask({{ray::kCPU_ResourceLabel, 5}});
  task_manager_.QueueAndScheduleTask(task2, &reply, callback);
  ASSERT_EQ(dependency_manager_.subscribed_tasks, expected_subscribed_tasks);

  ASSERT_EQ(num_callbacks, 1);
  ASSERT_EQ(leased_workers_.size(), 1);
  ASSERT_EQ(pool_.workers.size(), 1);

  /* First task is unblocked now, but resources are no longer available */
  dependency_manager_.task_ready_ = true;
  auto id = task.GetTaskSpecification().TaskId();
  std::vector<TaskID> unblocked = {id};
  task_manager_.TasksUnblocked(unblocked);
  ASSERT_EQ(dependency_manager_.subscribed_tasks, expected_subscribed_tasks);

  ASSERT_EQ(num_callbacks, 1);
  ASSERT_EQ(leased_workers_.size(), 1);
  ASSERT_EQ(pool_.workers.size(), 1);

  /* Second task finishes, making space for the original task */
  leased_workers_.clear();
  task_manager_.ReleaseWorkerResources(worker);

  task_manager_.ScheduleAndDispatchTasks();
  ASSERT_TRUE(dependency_manager_.subscribed_tasks.empty());

  // Task2 is now done so task can run.
  ASSERT_EQ(num_callbacks, 2);
  ASSERT_EQ(leased_workers_.size(), 1);
  ASSERT_EQ(pool_.workers.size(), 0);
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

  ASSERT_EQ(num_callbacks, 0);
  ASSERT_EQ(leased_workers_.size(), 0);

  /* This task can run but not at the same time as the first */
  auto task2 = CreateTask({{ray::kCPU_ResourceLabel, 5}});
  rpc::RequestWorkerLeaseReply spillback_reply;
  task_manager_.QueueAndScheduleTask(task2, &spillback_reply, callback);

  ASSERT_EQ(num_callbacks, 0);
  ASSERT_EQ(leased_workers_.size(), 0);

  // Two workers start. First task is dispatched now, but resources are no
  // longer available for the second.
  pool_.PushWorker(std::dynamic_pointer_cast<WorkerInterface>(worker));
  pool_.PushWorker(std::dynamic_pointer_cast<WorkerInterface>(worker));
  task_manager_.ScheduleAndDispatchTasks();
  // Check that both tasks got removed from the queue.
  ASSERT_EQ(num_callbacks, 2);
  // The first task was dispatched.
  ASSERT_EQ(leased_workers_.size(), 1);
  // The second task was spilled.
  ASSERT_EQ(spillback_reply.retry_at_raylet_address().raylet_id(),
            remote_node_id.Binary());
  AssertNoLeaks();
}

TEST_F(ClusterTaskManagerTest, TaskCancellationTest) {
  std::shared_ptr<MockWorker> worker =
      std::make_shared<MockWorker>(WorkerID::FromRandom(), 1234);
  Task task = CreateTask({{ray::kCPU_ResourceLabel, 1}});
  rpc::RequestWorkerLeaseReply reply;

  bool callback_called = false;
  bool *callback_called_ptr = &callback_called;
  auto callback = [callback_called_ptr](Status, std::function<void()>,
                                        std::function<void()>) {
    *callback_called_ptr = true;
  };

  // Task not queued so we can't cancel it.
  ASSERT_FALSE(task_manager_.CancelTask(task.GetTaskSpecification().TaskId()));

  task_manager_.QueueAndScheduleTask(task, &reply, callback);

  // Task is now in dispatch queue.
  callback_called = false;
  reply.Clear();
  ASSERT_TRUE(task_manager_.CancelTask(task.GetTaskSpecification().TaskId()));
  task_manager_.ScheduleAndDispatchTasks();
  // Task will not execute.
  ASSERT_TRUE(callback_called);
  ASSERT_TRUE(reply.canceled());
  ASSERT_EQ(leased_workers_.size(), 0);

  pool_.PushWorker(std::dynamic_pointer_cast<WorkerInterface>(worker));
  task_manager_.QueueAndScheduleTask(task, &reply, callback);

  // Task is now running so we can't cancel it.
  callback_called = false;
  reply.Clear();
  ASSERT_FALSE(task_manager_.CancelTask(task.GetTaskSpecification().TaskId()));
  // Task will not execute.
  ASSERT_FALSE(reply.canceled());
  ASSERT_FALSE(callback_called);
  ASSERT_EQ(pool_.workers.size(), 0);
  ASSERT_EQ(leased_workers_.size(), 1);
  AssertNoLeaks();
}

TEST_F(ClusterTaskManagerTest, TaskCancelInfeasibleTask) {
  /* Make sure cancelTask works for infeasible tasks */
  std::shared_ptr<MockWorker> worker =
      std::make_shared<MockWorker>(WorkerID::FromRandom(), 1234);
  pool_.PushWorker(std::dynamic_pointer_cast<WorkerInterface>(worker));

  Task task = CreateTask({{ray::kCPU_ResourceLabel, 12}});
  rpc::RequestWorkerLeaseReply reply;

  bool callback_called = false;
  bool *callback_called_ptr = &callback_called;
  auto callback = [callback_called_ptr](Status, std::function<void()>,
                                        std::function<void()>) {
    *callback_called_ptr = true;
  };

  task_manager_.QueueAndScheduleTask(task, &reply, callback);

  // Task is now queued so cancellation works.
  ASSERT_TRUE(task_manager_.CancelTask(task.GetTaskSpecification().TaskId()));
  task_manager_.ScheduleAndDispatchTasks();
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
  ASSERT_TRUE(callback_called);
  ASSERT_TRUE(reply.canceled());
  ASSERT_EQ(leased_workers_.size(), 0);
  ASSERT_EQ(pool_.workers.size(), 1);
  AssertNoLeaks();
}

TEST_F(ClusterTaskManagerTest, HeartbeatTest) {
  std::shared_ptr<MockWorker> worker =
      std::make_shared<MockWorker>(WorkerID::FromRandom(), 1234);
  pool_.PushWorker(std::dynamic_pointer_cast<WorkerInterface>(worker));

  {
    Task task = CreateTask({{ray::kCPU_ResourceLabel, 1}});
    rpc::RequestWorkerLeaseReply reply;

    bool callback_called = false;
    bool *callback_called_ptr = &callback_called;
    auto callback = [callback_called_ptr](Status, std::function<void()>,
                                          std::function<void()>) {
      *callback_called_ptr = true;
    };

    task_manager_.QueueAndScheduleTask(task, &reply, callback);
    ASSERT_TRUE(callback_called);
    // Now {CPU: 7, GPU: 4, MEM:128}
  }

  {
    Task task = CreateTask({{ray::kCPU_ResourceLabel, 1}});
    rpc::RequestWorkerLeaseReply reply;

    bool callback_called = false;
    bool *callback_called_ptr = &callback_called;
    auto callback = [callback_called_ptr](Status, std::function<void()>,
                                          std::function<void()>) {
      *callback_called_ptr = true;
    };

    task_manager_.QueueAndScheduleTask(task, &reply, callback);
    ASSERT_FALSE(callback_called);  // No worker available.
    // Now {CPU: 7, GPU: 4, MEM:128} with 1 queued task.
  }

  {
    Task task = CreateTask({{ray::kCPU_ResourceLabel, 9}, {ray::kGPU_ResourceLabel, 5}});
    rpc::RequestWorkerLeaseReply reply;

    bool callback_called = false;
    bool *callback_called_ptr = &callback_called;
    auto callback = [callback_called_ptr](Status, std::function<void()>,
                                          std::function<void()>) {
      *callback_called_ptr = true;
    };

    task_manager_.QueueAndScheduleTask(task, &reply, callback);
    ASSERT_FALSE(callback_called);  // Infeasible.
    // Now there is also an infeasible task {CPU: 9}.
  }

  {
    Task task = CreateTask({{ray::kCPU_ResourceLabel, 10}, {ray::kGPU_ResourceLabel, 1}});
    rpc::RequestWorkerLeaseReply reply;

    bool callback_called = false;
    bool *callback_called_ptr = &callback_called;
    auto callback = [callback_called_ptr](Status, std::function<void()>,
                                          std::function<void()>) {
      *callback_called_ptr = true;
    };

    task_manager_.QueueAndScheduleTask(task, &reply, callback);
    ASSERT_FALSE(callback_called);  // Infeasible.
    // Now there is also an infeasible task {CPU: 10}.
  }

  {
    auto data = std::make_shared<rpc::ResourcesData>();
    task_manager_.FillResourceUsage(data);

    auto load_by_shape =
        data->mutable_resource_load_by_shape()->mutable_resource_demands();
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
        /* These logs are very useful for debugging.
        RAY_LOG(ERROR) << "==========================";
        RAY_LOG(ERROR) << expected_load[0] << "\t" <<
        load.num_infeasible_requests_queued(); RAY_LOG(ERROR) << expected_load[1] << "\t"
        << load.num_ready_requests_queued(); RAY_LOG(ERROR) << expected_load[2] << "\t" <<
        shape["CPU"]; RAY_LOG(ERROR) << expected_load[3] << "\t" << shape["GPU"];
        RAY_LOG(ERROR) << expected_load[4] << "\t" << shape.size();
        RAY_LOG(ERROR) << "==========================";
        RAY_LOG(ERROR) << load.DebugString();
        RAY_LOG(ERROR) << "-----------------------------------";
        */
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

  std::shared_ptr<MockWorker> worker =
      std::make_shared<MockWorker>(WorkerID::FromRandom(), 1234);
  pool_.PushWorker(std::dynamic_pointer_cast<WorkerInterface>(worker));

  std::vector<TaskID> to_cancel;

  for (int i = 0; i < 10; i++) {
    Task task = CreateTask({{ray::kCPU_ResourceLabel, 100}});
    task.SetBacklogSize(i);
    task_manager_.QueueAndScheduleTask(task, &reply, callback);
    to_cancel.push_back(task.GetTaskSpecification().TaskId());
  }

  ASSERT_FALSE(callback_occurred);
  ASSERT_EQ(leased_workers_.size(), 0);
  ASSERT_EQ(pool_.workers.size(), 1);
  ASSERT_EQ(node_info_calls_, 0);

  auto data = std::make_shared<rpc::ResourcesData>();
  task_manager_.FillResourceUsage(data);

  auto resource_load_by_shape = data->resource_load_by_shape();
  auto shape1 = resource_load_by_shape.resource_demands()[0];

  ASSERT_EQ(shape1.backlog_size(), 45);
  ASSERT_EQ(shape1.num_infeasible_requests_queued(), 10);
  ASSERT_EQ(shape1.num_ready_requests_queued(), 0);

  for (auto &task_id : to_cancel) {
    ASSERT_TRUE(task_manager_.CancelTask(task_id));
  }

  data = std::make_shared<rpc::ResourcesData>();
  task_manager_.FillResourceUsage(data);

  resource_load_by_shape = data->resource_load_by_shape();
  shape1 = resource_load_by_shape.resource_demands()[0];

  ASSERT_EQ(shape1.backlog_size(), 0);
  ASSERT_EQ(shape1.num_infeasible_requests_queued(), 0);
  ASSERT_EQ(shape1.num_ready_requests_queued(), 0);
  AssertNoLeaks();
}

TEST_F(ClusterTaskManagerTest, OwnerDeadTest) {
  /*
    Test the race condition in which the owner of a task dies while the task is pending.
    This is the essence of test_actor_advanced.py::test_pending_actor_removed_by_owner
   */
  Task task = CreateTask({{ray::kCPU_ResourceLabel, 4}});
  rpc::RequestWorkerLeaseReply reply;
  bool callback_occurred = false;
  bool *callback_occurred_ptr = &callback_occurred;
  auto callback = [callback_occurred_ptr](Status, std::function<void()>,
                                          std::function<void()>) {
    *callback_occurred_ptr = true;
  };

  std::shared_ptr<MockWorker> worker =
      std::make_shared<MockWorker>(WorkerID::FromRandom(), 1234);
  pool_.PushWorker(std::dynamic_pointer_cast<WorkerInterface>(worker));

  is_owner_alive_ = false;
  task_manager_.QueueAndScheduleTask(task, &reply, callback);

  ASSERT_FALSE(callback_occurred);
  ASSERT_EQ(leased_workers_.size(), 0);
  ASSERT_EQ(pool_.workers.size(), 1);

  is_owner_alive_ = true;
  task_manager_.ScheduleAndDispatchTasks();

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
  Task task = CreateTask({{ray::kCPU_ResourceLabel, 12}});
  rpc::RequestWorkerLeaseReply reply;
  std::shared_ptr<bool> callback_occurred = std::make_shared<bool>(false);
  auto callback = [callback_occurred](Status, std::function<void()>,
                                      std::function<void()>) {
    *callback_occurred = true;
  };
  task_manager_.QueueAndScheduleTask(task, &reply, callback);
  ASSERT_EQ(announce_infeasible_task_calls_, 1);

  // Infeasible warning shouldn't be reprinted when the previous task is still infeasible
  // after adding a new node.
  AddNode(NodeID::FromRandom(), 8);
  std::shared_ptr<MockWorker> worker =
      std::make_shared<MockWorker>(WorkerID::FromRandom(), 1234);
  pool_.PushWorker(std::dynamic_pointer_cast<WorkerInterface>(worker));
  task_manager_.ScheduleAndDispatchTasks();
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
  Task task = CreateTask({{ray::kCPU_ResourceLabel, 12}});
  rpc::RequestWorkerLeaseReply reply;
  std::shared_ptr<bool> callback_occurred = std::make_shared<bool>(false);
  auto callback = [callback_occurred](Status, std::function<void()>,
                                      std::function<void()>) {
    *callback_occurred = true;
  };
  task_manager_.QueueAndScheduleTask(task, &reply, callback);
  ASSERT_EQ(announce_infeasible_task_calls_, 1);

  // Make sure the same shape infeasible task won't be announced.
  Task task2 = CreateTask({{ray::kCPU_ResourceLabel, 12}});
  rpc::RequestWorkerLeaseReply reply2;
  std::shared_ptr<bool> callback_occurred2 = std::make_shared<bool>(false);
  auto callback2 = [callback_occurred2](Status, std::function<void()>,
                                        std::function<void()>) {
    *callback_occurred2 = true;
  };
  task_manager_.QueueAndScheduleTask(task2, &reply2, callback2);
  ASSERT_EQ(announce_infeasible_task_calls_, 1);
}

TEST_F(ClusterTaskManagerTest, TestAnyPendingTasks) {
  /*
    Check if the manager can correctly identify pending tasks.
   */
  std::shared_ptr<MockWorker> worker =
      std::make_shared<MockWorker>(WorkerID::FromRandom(), 1234);
  pool_.PushWorker(std::dynamic_pointer_cast<WorkerInterface>(worker));

  // task1: running
  Task task = CreateTask({{ray::kCPU_ResourceLabel, 6}});
  rpc::RequestWorkerLeaseReply reply;
  std::shared_ptr<bool> callback_occurred = std::make_shared<bool>(false);
  auto callback = [callback_occurred](Status, std::function<void()>,
                                      std::function<void()>) {
    *callback_occurred = true;
  };
  task_manager_.QueueAndScheduleTask(task, &reply, callback);
  ASSERT_TRUE(*callback_occurred);
  ASSERT_EQ(leased_workers_.size(), 1);
  ASSERT_EQ(pool_.workers.size(), 0);

  // task1: running. Progress is made, and there's no deadlock.
  ray::Task exemplar;
  bool any_pending = false;
  int pending_actor_creations = 0;
  int pending_tasks = 0;
  ASSERT_FALSE(task_manager_.AnyPendingTasks(&exemplar, &any_pending,
                                             &pending_actor_creations, &pending_tasks));

  // task1: running, task2: queued.
  Task task2 = CreateTask({{ray::kCPU_ResourceLabel, 6}});
  rpc::RequestWorkerLeaseReply reply2;
  std::shared_ptr<bool> callback_occurred2 = std::make_shared<bool>(false);
  auto callback2 = [callback_occurred2](Status, std::function<void()>,
                                        std::function<void()>) {
    *callback_occurred2 = true;
  };
  task_manager_.QueueAndScheduleTask(task2, &reply2, callback2);
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
  pool_.PushWorker(std::dynamic_pointer_cast<WorkerInterface>(worker));

  rpc::RequestWorkerLeaseReply reply;
  int num_callbacks = 0;
  int *num_callbacks_ptr = &num_callbacks;
  auto callback = [num_callbacks_ptr](Status, std::function<void()>,
                                      std::function<void()>) {
    (*num_callbacks_ptr) = *num_callbacks_ptr + 1;
  };

  /* Blocked on dependencies */
  dependency_manager_.task_ready_ = false;
  auto task = CreateTask({{ray::kCPU_ResourceLabel, 5}}, 2);
  std::unordered_set<TaskID> expected_subscribed_tasks = {
      task.GetTaskSpecification().TaskId()};
  task_manager_.QueueAndScheduleTask(task, &reply, callback);
  ASSERT_EQ(dependency_manager_.subscribed_tasks, expected_subscribed_tasks);
  ASSERT_EQ(num_callbacks, 0);
  ASSERT_EQ(leased_workers_.size(), 0);

  /* Task is unblocked now */
  dependency_manager_.task_ready_ = true;
  pool_.workers.clear();
  auto id = task.GetTaskSpecification().TaskId();
  task_manager_.TasksUnblocked({id});
  ASSERT_EQ(dependency_manager_.subscribed_tasks, expected_subscribed_tasks);
  ASSERT_EQ(num_callbacks, 0);
  ASSERT_EQ(leased_workers_.size(), 0);

  /* Task argument gets evicted */
  dependency_manager_.task_ready_ = false;
  pool_.PushWorker(std::dynamic_pointer_cast<WorkerInterface>(worker));
  task_manager_.ScheduleAndDispatchTasks();
  ASSERT_EQ(dependency_manager_.subscribed_tasks, expected_subscribed_tasks);
  ASSERT_EQ(num_callbacks, 0);
  ASSERT_EQ(leased_workers_.size(), 0);

  /* Worker available and arguments available */
  dependency_manager_.task_ready_ = true;
  task_manager_.TasksUnblocked({id});
  ASSERT_EQ(num_callbacks, 1);
  ASSERT_EQ(leased_workers_.size(), 1);
  AssertNoLeaks();
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

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

}  // namespace raylet

}  // namespace ray
