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

// using namespace std;

namespace ray {

namespace raylet {

class MockWorkerPool : public WorkerPoolInterface {
 public:
  /// Pop an idle worker from the pool. The caller is responsible for pushing
  /// the worker back onto the pool once the worker has completed its work.
  ///
  /// \param task_spec The returned worker must be able to execute this task.
  /// \return An idle worker with the requested task spec. Returns nullptr if no
  /// such worker exists.
  std::shared_ptr<WorkerInterface> PopWorker(const TaskSpecification &task_spec) {
    if (workers.empty()) {
      return nullptr;
    }
    auto worker_ptr = workers.front();
    workers.pop_front();
    return worker_ptr;
  }
  /// Add an idle worker to the pool.
  ///
  /// \param The idle worker to add.
  void PushWorker(const std::shared_ptr<WorkerInterface> &worker) {
    workers.push_front(worker);
  }

  std::list<std::shared_ptr<WorkerInterface>> workers;
};

class MockWorker : public WorkerInterface {
 public:
  MockWorker(WorkerID worker_id, int port) : worker_id_(worker_id), port_(port) {}

  WorkerID WorkerId() const { return worker_id_; }

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
  /// Return the worker process.
  Process GetProcess() const {
    RAY_CHECK(false) << "Method unused";
    return Process::CreateNewDummy();
  }
  void SetProcess(Process proc) { RAY_CHECK(false) << "Method unused"; }
  Language GetLanguage() const {
    RAY_CHECK(false) << "Method unused";
    return Language::PYTHON;
  }
  /// Connect this worker's gRPC client.
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

  const std::unordered_set<ObjectID> &GetActiveObjectIds() const {
    RAY_CHECK(false) << "Method unused";
    auto *t = new std::unordered_set<ObjectID>();
    return *t;
  }
  void SetActiveObjectIds(const std::unordered_set<ObjectID> &&object_ids) {
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
  void WorkerLeaseGranted(const std::string &address, int port) {
    RAY_CHECK(false) << "Method unused";
  }

  // Setter, geter, and clear methods  for allocated_instances_.

  void ClearAllocatedInstances() { RAY_CHECK(false) << "Method unused"; }

  void ClearLifetimeAllocatedInstances() { RAY_CHECK(false) << "Method unused"; }

  void SetBorrowedCPUInstances(std::vector<double> &cpu_instances) {
    RAY_CHECK(false) << "Method unused";
  }

  std::vector<double> &GetBorrowedCPUInstances() {
    RAY_CHECK(false) << "Method unused";
    auto *t = new std::vector<double>();
    return *t;
  }

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

Task CreateTask(const std::unordered_map<std::string, double> &required_resources) {
  TaskSpecBuilder spec_builder;
  TaskID id = RandomTaskId();
  JobID job_id = RandomJobId();
  rpc::Address address;
  spec_builder.SetCommonTaskSpec(
      id, Language::PYTHON, FunctionDescriptorBuilder::BuildPython("", "", "", ""),
      job_id, TaskID::Nil(), 0, TaskID::Nil(), address, 0, required_resources, {});
  rpc::TaskExecutionSpec execution_spec_message;
  execution_spec_message.set_num_forwards(1);
  return Task(spec_builder.Build(), TaskExecutionSpecification(execution_spec_message));
}

class ClusterTaskManagerTest : public ::testing::Test {
 public:
  void SetUp() {}

  void Shutdown() {}
};

TEST_F(ClusterTaskManagerTest, SampleTest) {
  ClientID id = ClientID::FromRandom();
  // Task has no dependencies and shouldn't spill so these functions should never be
  // called.
  std::function<bool(const Task &)> fulfills_dependencies_func = nullptr;
  std::function<boost::optional<rpc::GcsNodeInfo>(const ClientID &node_id)>
      get_node_info_func = nullptr;
  auto task_manager = ClusterTaskManager(id, CreateSingleNodeScheduler(id.Binary()),
                                         fulfills_dependencies_func, get_node_info_func);

  Task task = CreateTask({{ray::kCPU_ResourceLabel, 4}});

  MockWorkerPool pool;

  rpc::RequestWorkerLeaseReply reply;

  bool callback_occurred = false;
  bool *callback_occurred_ptr = &callback_occurred;
  auto callback = [callback_occurred_ptr]() { *callback_occurred_ptr = true; };

  std::unordered_map<WorkerID, std::shared_ptr<WorkerInterface>> leased_workers;

  task_manager.QueueTask(task, &reply, callback);
  task_manager.SchedulePendingTasks();
  task_manager.DispatchScheduledTasksToWorkers(pool, leased_workers);

  ASSERT_FALSE(callback_occurred);

  rpc::Address address;
  std::shared_ptr<MockWorker> worker =
      std::make_shared<MockWorker>(WorkerID::FromRandom(), 1234);
  // WorkerInterface worker = MockWorker();
  pool.PushWorker(std::dynamic_pointer_cast<WorkerInterface>(worker));

  task_manager.DispatchScheduledTasksToWorkers(pool, leased_workers);
}

}  // namespace raylet

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
