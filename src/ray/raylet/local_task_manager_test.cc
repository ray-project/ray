#include <gtest/gtest.h>
#include "ray/raylet/local_task_manager.h"
#include "ray/raylet/task.h"
#include "ray/raylet/task_dependency_manager.h"
#include "ray/raylet/scheduling/scheduling_ids.h"
#include "ray/raylet/cluster_resource_scheduler.h"

namespace ray {
namespace raylet {

class MockWorkerPool : public WorkerPoolInterface {
 public:
  MockWorkerPool() {}
  void PopWorker(
      const TaskSpecification &task_spec,
      std::function<void(std::shared_ptr<WorkerInterface>, PopWorkerStatus, const std::string &)> callback,
      const std::string &allocated_instances_serialized_json = "{}") override {
    callback(nullptr, PopWorkerStatus::OK, "");
  }
  void PushWorker(const std::shared_ptr<WorkerInterface> &worker) override {}
  void PrestartWorkers(const TaskSpecification &task_spec, int64_t backlog_size) override {}
  void DisconnectWorker(const std::shared_ptr<WorkerInterface> &worker,
                        rpc::WorkerExitType disconnect_type) override {}
  void GetAllRegisteredWorkers(bool filter_dead_workers,
                               std::vector<std::shared_ptr<WorkerInterface>> *out) const override {}
};

class LocalTaskManagerTest : public ::testing::Test {
 protected:
  LocalTaskManagerTest()
      : node_id_(NodeID::FromRandom()),
        cluster_resource_scheduler_(
            std::make_shared<ClusterResourceScheduler>(node_id_.Binary(), false)),
        task_dependency_manager_(),
        is_owner_alive_([](const WorkerID &, const NodeID &) { return true; }),
        get_node_info_([](const NodeID &) { return std::make_shared<rpc::GcsNodeInfo>(); }),
        worker_pool_(std::make_shared<MockWorkerPool>()),
        get_task_arguments_([](const std::vector<ObjectID> &, std::vector<std::unique_ptr<RayObject>> *) { return true; }),
        local_task_manager_(node_id_,
                            cluster_resource_scheduler_,
                            task_dependency_manager_,
                            is_owner_alive_,
                            get_node_info_,
                            *worker_pool_,
                            leased_workers_,
                            get_task_arguments_,
                            1000000,
                            []() { return absl::GetCurrentTimeNanos() / 1e6; },
                            1000) {}

  NodeID node_id_;
  std::shared_ptr<ClusterResourceScheduler> cluster_resource_scheduler_;
  TaskDependencyManager task_dependency_manager_;
  std::function<bool(const WorkerID &, const NodeID &)> is_owner_alive_;
  internal::NodeInfoGetter get_node_info_;
  std::shared_ptr<MockWorkerPool> worker_pool_;
  absl::flat_hash_map<WorkerID, std::shared_ptr<WorkerInterface>> leased_workers_;
  std::function<bool(const std::vector<ObjectID> &,
                     std::vector<std::unique_ptr<RayObject>> *)>
      get_task_arguments_;
  LocalTaskManager local_task_manager_;
};

TEST_F(LocalTaskManagerTest, TestFairDispatching) {
  TaskSpecification task_spec_f, task_spec_g;

  // Set the cluster resource scheduler to have 3 CPUs
  cluster_resource_scheduler_->GetLocalResourceManager().UpdateResourceCapacity(
      ResourceID::CPU(), 3);

  // Create task f with 1 CPU requirement
  ResourceSet resources_f;
  resources_f.AddResource(ResourceID::CPU(), 1);
  task_spec_f = TaskSpecification(
      TaskID::FromRandom(),
      TaskID::ForDriverTask(JobID::FromInt(1)),
      TaskID::Nil(),
      0,
      Language::PYTHON,
      FunctionDescriptorBuilder::BuildPython("module", "function_f", "", ""),
      resources_f,
      {},
      {},
      {},
      {},
      PlacementGroupID::Nil(),
      true,
      false,
      0,
      {},
      {},
      TaskOptions(),
      rpc::Address(),
      {},
      {},
      {},
      {},
      {});

  // Create task g with 1 CPU requirement
  ResourceSet resources_g;
  resources_g.AddResource(ResourceID::CPU(), 1);
  task_spec_g = TaskSpecification(
      TaskID::FromRandom(),
      TaskID::ForDriverTask(JobID::FromInt(1)),
      TaskID::Nil(),
      0,
      Language::PYTHON,
      FunctionDescriptorBuilder::BuildPython("module", "function_g", "", ""),
      resources_g,
      {},
      {},
      {},
      {},
      PlacementGroupID::Nil(),
      true,
      false,
      0,
      {},
      {},
      TaskOptions(),
      rpc::Address(),
      {},
      {},
      {},
      {},
      {});

  auto work_f1 = std::make_shared<internal::Work>(task_spec_f, nullptr, nullptr, nullptr);
  auto work_f2 = std::make_shared<internal::Work>(task_spec_f, nullptr, nullptr, nullptr);
  auto work_f3 = std::make_shared<internal::Work>(task_spec_f, nullptr, nullptr, nullptr);
  auto work_f4 = std::make_shared<internal::Work>(task_spec_f, nullptr, nullptr, nullptr);
  auto work_g1 = std::make_shared<internal::Work>(task_spec_g, nullptr, nullptr, nullptr);
  auto work_g2 = std::make_shared<internal::Work>(task_spec_g, nullptr, nullptr, nullptr);

  // Enqueue initial tasks f
  local_task_manager_.QueueAndScheduleTask(work_f1);
  local_task_manager_.QueueAndScheduleTask(work_f2);

  // Run scheduling to allocate resources for the initial tasks
  local_task_manager_.ScheduleAndDispatchTasks();

  // Check the task states
  ASSERT_TRUE(work_f1->GetState() == internal::WorkStatus::WAITING_FOR_WORKER);
  ASSERT_TRUE(work_f2->GetState() == internal::WorkStatus::WAITING_FOR_WORKER);

  // Enqueue new tasks [f, f, g, g]
  local_task_manager_.QueueAndScheduleTask(work_f3);
  local_task_manager_.QueueAndScheduleTask(work_f4);
  local_task_manager_.QueueAndScheduleTask(work_g1);
  local_task_manager_.QueueAndScheduleTask(work_g2);

  // Run scheduling to test fair dispatching
  local_task_manager_.ScheduleAndDispatchTasks();

  // Expect that task g1 gets dispatched before task f3
  ASSERT_TRUE(work_g1->GetState() == internal::WorkStatus::WAITING_FOR_WORKER);
  ASSERT_TRUE(work_g2->GetState() == internal::WorkStatus::WAITING_FOR_WORKER || work_g2->GetState() == internal::WorkStatus::WAITING);
  ASSERT_TRUE(work_f3->GetState() == internal::WorkStatus::WAITING);
  ASSERT_TRUE(work_f4->GetState() == internal::WorkStatus::WAITING);
}

}  // namespace raylet
}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
