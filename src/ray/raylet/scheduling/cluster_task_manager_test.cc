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

#ifdef UNORDERED_VS_ABSL_MAPS_EVALUATION
#include <chrono>

#include "absl/container/flat_hash_map.h"
#endif  // UNORDERED_VS_ABSL_MAPS_EVALUATION

namespace ray {

namespace raylet {

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

class MockWorker : public WorkerInterface {
 public:
  MockWorker(WorkerID worker_id, int port) : worker_id_(worker_id), port_(port) {}

  WorkerID WorkerId() const { return worker_id_; }

  rpc::WorkerType GetWorkerType() const { return rpc::WorkerType::WORKER; }

  int Port() const { return port_; }

  void SetOwnerAddress(const rpc::Address &address) { address_ = address; }

  void AssignTaskId(const TaskID &task_id) {}

  void AssignJobId(const JobID &job_id) {}

  void SetAssignedTask(Task &assigned_task) {}

  const std::string IpAddress() const { return address_.ip_address(); }

  void SetAllocatedInstances(
      std::shared_ptr<TaskResourceInstances> &allocated_instances) {
    allocated_instances_ = allocated_instances;
  }

  void SetLifetimeAllocatedInstances(
      std::shared_ptr<TaskResourceInstances> &allocated_instances) {
    lifetime_allocated_instances_ = allocated_instances;
  }

  std::shared_ptr<TaskResourceInstances> GetAllocatedInstances() {
    return allocated_instances_;
  }
  std::shared_ptr<TaskResourceInstances> GetLifetimeAllocatedInstances() {
    return lifetime_allocated_instances_;
  }

  void MarkDead() { RAY_CHECK(false) << "Method unused"; }
  bool IsDead() const {
    RAY_CHECK(false) << "Method unused";
    return false;
  }
  void MarkBlocked() { RAY_CHECK(false) << "Method unused"; }
  void MarkUnblocked() { RAY_CHECK(false) << "Method unused"; }
  bool IsBlocked() const {
    RAY_CHECK(false) << "Method unused";
    return false;
  }

  Process GetProcess() const {
    RAY_CHECK(false) << "Method unused";
    return Process::CreateNewDummy();
  }
  void SetProcess(Process proc) { RAY_CHECK(false) << "Method unused"; }
  Language GetLanguage() const {
    RAY_CHECK(false) << "Method unused";
    return Language::PYTHON;
  }

  void Connect(int port) { RAY_CHECK(false) << "Method unused"; }

  int AssignedPort() const {
    RAY_CHECK(false) << "Method unused";
    return -1;
  }
  void SetAssignedPort(int port) { RAY_CHECK(false) << "Method unused"; }
  const TaskID &GetAssignedTaskId() const {
    RAY_CHECK(false) << "Method unused";
    return TaskID::Nil();
  }
  bool AddBlockedTaskId(const TaskID &task_id) {
    RAY_CHECK(false) << "Method unused";
    return false;
  }
  bool RemoveBlockedTaskId(const TaskID &task_id) {
    RAY_CHECK(false) << "Method unused";
    return false;
  }
  const std::unordered_set<TaskID> &GetBlockedTaskIds() const {
    RAY_CHECK(false) << "Method unused";
    auto *t = new std::unordered_set<TaskID>();
    return *t;
  }
  const JobID &GetAssignedJobId() const {
    RAY_CHECK(false) << "Method unused";
    return JobID::Nil();
  }
  void AssignActorId(const ActorID &actor_id) { RAY_CHECK(false) << "Method unused"; }
  const ActorID &GetActorId() const {
    RAY_CHECK(false) << "Method unused";
    return ActorID::Nil();
  }
  void MarkDetachedActor() { RAY_CHECK(false) << "Method unused"; }
  bool IsDetachedActor() const {
    RAY_CHECK(false) << "Method unused";
    return false;
  }
  const std::shared_ptr<ClientConnection> Connection() const {
    RAY_CHECK(false) << "Method unused";
    return nullptr;
  }
  const rpc::Address &GetOwnerAddress() const {
    RAY_CHECK(false) << "Method unused";
    return address_;
  }

  const ResourceIdSet &GetLifetimeResourceIds() const {
    RAY_CHECK(false) << "Method unused";
    auto *t = new ResourceIdSet();
    return *t;
  }
  void SetLifetimeResourceIds(ResourceIdSet &resource_ids) {
    RAY_CHECK(false) << "Method unused";
  }
  void ResetLifetimeResourceIds() { RAY_CHECK(false) << "Method unused"; }

  const ResourceIdSet &GetTaskResourceIds() const {
    RAY_CHECK(false) << "Method unused";
    auto *t = new ResourceIdSet();
    return *t;
  }
  void SetTaskResourceIds(ResourceIdSet &resource_ids) {
    RAY_CHECK(false) << "Method unused";
  }
  void ResetTaskResourceIds() { RAY_CHECK(false) << "Method unused"; }
  ResourceIdSet ReleaseTaskCpuResources() {
    RAY_CHECK(false) << "Method unused";
    auto *t = new ResourceIdSet();
    return *t;
  }
  void AcquireTaskCpuResources(const ResourceIdSet &cpu_resources) {
    RAY_CHECK(false) << "Method unused";
  }

  Status AssignTask(const Task &task, const ResourceIdSet &resource_id_set) {
    RAY_CHECK(false) << "Method unused";
    Status s;
    return s;
  }
  void DirectActorCallArgWaitComplete(int64_t tag) {
    RAY_CHECK(false) << "Method unused";
  }

  void ClearAllocatedInstances() { allocated_instances_ = nullptr; }

  void ClearLifetimeAllocatedInstances() { RAY_CHECK(false) << "Method unused"; }

  void SetBorrowedCPUInstances(std::vector<double> &cpu_instances) {
    borrowed_cpu_instances_ = cpu_instances;
  }

  const PlacementGroupID &GetPlacementGroupId() const {
    RAY_CHECK(false) << "Method unused";
    return PlacementGroupID::Nil();
  }

  void SetPlacementGroupId(const PlacementGroupID &placement_group_id) {
    RAY_CHECK(false) << "Method unused";
  }

  std::vector<double> &GetBorrowedCPUInstances() { return borrowed_cpu_instances_; }

  void ClearBorrowedCPUInstances() { RAY_CHECK(false) << "Method unused"; }

  Task &GetAssignedTask() {
    RAY_CHECK(false) << "Method unused";
    auto *t = new Task();
    return *t;
  }

  bool IsRegistered() {
    RAY_CHECK(false) << "Method unused";
    return false;
  }

  rpc::CoreWorkerClient *rpc_client() {
    RAY_CHECK(false) << "Method unused";
    return nullptr;
  }

 private:
  WorkerID worker_id_;
  int port_;
  rpc::Address address_;
  std::shared_ptr<TaskResourceInstances> allocated_instances_;
  std::shared_ptr<TaskResourceInstances> lifetime_allocated_instances_;
  std::vector<double> borrowed_cpu_instances_;
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
                                 required_resources, {}, PlacementGroupID::Nil());

  for (int i = 0; i < num_args; i++) {
    ObjectID put_id = ObjectID::FromIndex(TaskID::Nil(), /*index=*/i + 1);
    spec_builder.AddArg(TaskArgByReference(put_id, rpc::Address()));
  }

  rpc::TaskExecutionSpec execution_spec_message;
  execution_spec_message.set_num_forwards(1);
  return Task(spec_builder.Build(), TaskExecutionSpecification(execution_spec_message));
}

class ClusterTaskManagerTest : public ::testing::Test {
 public:
  ClusterTaskManagerTest()
      : id_(ClientID::FromRandom()),
        single_node_resource_scheduler_(CreateSingleNodeScheduler(id_.Binary())),
        fulfills_dependencies_calls_(0),
        dependencies_fulfilled_(true),
        node_info_calls_(0),
        node_info_(boost::optional<rpc::GcsNodeInfo>{}),
        task_manager_(id_, single_node_resource_scheduler_,
                      [this](const Task &_task) {
                        fulfills_dependencies_calls_++;
                        return dependencies_fulfilled_;
                      },
                      [this](const ClientID &node_id) {
                        node_info_calls_++;
                        return node_info_;
                      }) {}

  void SetUp() {}

  void Shutdown() {}

  ClientID id_;
  std::shared_ptr<ClusterResourceScheduler> single_node_resource_scheduler_;
  MockWorkerPool pool_;
  std::unordered_map<WorkerID, std::shared_ptr<WorkerInterface>> leased_workers_;

  int fulfills_dependencies_calls_;
  bool dependencies_fulfilled_;

  int node_info_calls_;
  boost::optional<rpc::GcsNodeInfo> node_info_;

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
  auto callback = [callback_occurred_ptr]() { *callback_occurred_ptr = true; };

  task_manager_.QueueTask(task, &reply, callback);
  task_manager_.SchedulePendingTasks();
  task_manager_.DispatchScheduledTasksToWorkers(pool_, leased_workers_);

  ASSERT_FALSE(callback_occurred);
  ASSERT_EQ(leased_workers_.size(), 0);
  ASSERT_EQ(pool_.workers.size(), 0);

  std::shared_ptr<MockWorker> worker =
      std::make_shared<MockWorker>(WorkerID::FromRandom(), 1234);
  pool_.PushWorker(std::dynamic_pointer_cast<WorkerInterface>(worker));

  task_manager_.DispatchScheduledTasksToWorkers(pool_, leased_workers_);

  ASSERT_TRUE(callback_occurred);
  ASSERT_EQ(leased_workers_.size(), 1);
  ASSERT_EQ(pool_.workers.size(), 0);
  ASSERT_EQ(fulfills_dependencies_calls_, 0);
  ASSERT_EQ(node_info_calls_, 0);
}

TEST_F(ClusterTaskManagerTest, NoFeasibleNodeTest) {
  std::shared_ptr<MockWorker> worker =
      std::make_shared<MockWorker>(WorkerID::FromRandom(), 1234);
  pool_.PushWorker(std::dynamic_pointer_cast<WorkerInterface>(worker));

  Task task = CreateTask({{ray::kCPU_ResourceLabel, 999}});
  rpc::RequestWorkerLeaseReply reply;

  bool callback_called = false;
  bool *callback_called_ptr = &callback_called;
  auto callback = [callback_called_ptr]() { *callback_called_ptr = true; };

  task_manager_.QueueTask(task, &reply, callback);
  task_manager_.SchedulePendingTasks();
  task_manager_.DispatchScheduledTasksToWorkers(pool_, leased_workers_);

  ASSERT_FALSE(callback_called);
  ASSERT_EQ(leased_workers_.size(), 0);
  // Worker is unused.
  ASSERT_EQ(pool_.workers.size(), 1);
  ASSERT_EQ(fulfills_dependencies_calls_, 0);
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
  auto callback = [num_callbacks_ptr]() {
    (*num_callbacks_ptr) = *num_callbacks_ptr + 1;
  };

  /* Blocked on dependencies */
  auto task = CreateTask({{ray::kCPU_ResourceLabel, 5}}, 1);
  dependencies_fulfilled_ = false;
  task_manager_.QueueTask(task, &reply, callback);
  task_manager_.SchedulePendingTasks();
  task_manager_.DispatchScheduledTasksToWorkers(pool_, leased_workers_);

  ASSERT_EQ(num_callbacks, 0);
  ASSERT_EQ(leased_workers_.size(), 0);
  ASSERT_EQ(pool_.workers.size(), 2);

  /* This task can run */
  auto task2 = CreateTask({{ray::kCPU_ResourceLabel, 5}});
  task_manager_.QueueTask(task2, &reply, callback);
  task_manager_.SchedulePendingTasks();
  task_manager_.DispatchScheduledTasksToWorkers(pool_, leased_workers_);

  ASSERT_EQ(num_callbacks, 1);
  ASSERT_EQ(leased_workers_.size(), 1);
  ASSERT_EQ(pool_.workers.size(), 1);

  /* First task is unblocked now, but resources are no longer available */
  auto id = task.GetTaskSpecification().TaskId();
  std::vector<TaskID> unblocked = {id};
  task_manager_.TasksUnblocked(unblocked);
  task_manager_.DispatchScheduledTasksToWorkers(pool_, leased_workers_);

  ASSERT_EQ(num_callbacks, 1);
  ASSERT_EQ(leased_workers_.size(), 1);
  ASSERT_EQ(pool_.workers.size(), 1);

  /* Second task finishes, making space for the original task */
  leased_workers_.clear();
  task_manager_.HandleTaskFinished(worker);

  task_manager_.DispatchScheduledTasksToWorkers(pool_, leased_workers_);

  // Task2 is now done so task can run.
  ASSERT_EQ(num_callbacks, 2);
  ASSERT_EQ(leased_workers_.size(), 1);
  ASSERT_EQ(pool_.workers.size(), 0);
}

TEST_F(ClusterTaskManagerTest, TaskCancellationTest) {
  std::shared_ptr<MockWorker> worker =
      std::make_shared<MockWorker>(WorkerID::FromRandom(), 1234);
  pool_.PushWorker(std::dynamic_pointer_cast<WorkerInterface>(worker));

  Task task = CreateTask({{ray::kCPU_ResourceLabel, 1}});
  rpc::RequestWorkerLeaseReply reply;

  bool callback_called = false;
  bool *callback_called_ptr = &callback_called;
  auto callback = [callback_called_ptr]() { *callback_called_ptr = true; };

  // Task not queued so we can't cancel it.
  ASSERT_FALSE(task_manager_.CancelTask(task.GetTaskSpecification().TaskId()));

  task_manager_.QueueTask(task, &reply, callback);

  // Task is now queued so cancellation works.
  ASSERT_TRUE(task_manager_.CancelTask(task.GetTaskSpecification().TaskId()));
  task_manager_.DispatchScheduledTasksToWorkers(pool_, leased_workers_);
  // Task will not execute.
  ASSERT_FALSE(callback_called);
  ASSERT_EQ(leased_workers_.size(), 0);
  ASSERT_EQ(pool_.workers.size(), 1);

  task_manager_.QueueTask(task, &reply, callback);
  task_manager_.SchedulePendingTasks();

  // We can still cancel the task if it's on the dispatch queue.
  ASSERT_TRUE(task_manager_.CancelTask(task.GetTaskSpecification().TaskId()));
  // Task will not execute.
  ASSERT_FALSE(callback_called);
  ASSERT_EQ(leased_workers_.size(), 0);
  ASSERT_EQ(pool_.workers.size(), 1);

  task_manager_.QueueTask(task, &reply, callback);
  task_manager_.SchedulePendingTasks();
  task_manager_.DispatchScheduledTasksToWorkers(pool_, leased_workers_);

  // Task is now running so we can't cancel it.
  ASSERT_FALSE(task_manager_.CancelTask(task.GetTaskSpecification().TaskId()));
  // Task will not execute.
  ASSERT_TRUE(callback_called);
  ASSERT_EQ(pool_.workers.size(), 0);
  ASSERT_EQ(leased_workers_.size(), 1);
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

}  // namespace raylet
}  // namespace ray
