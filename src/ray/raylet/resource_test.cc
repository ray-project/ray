#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "ray/raylet/format/node_manager_generated.h"
#include "ray/raylet/lineage_cache.h"
#include "ray/raylet/task.h"
#include "ray/raylet/task_execution_spec.h"
#include "ray/raylet/task_spec.h"
#include "ray/raylet/scheduling_queue.h"


namespace ray {

namespace raylet {

Task CreateTask(std::unordered_map<std::string, double> required_resources) {
  std::vector<std::shared_ptr<TaskArgument>> arguments;
  std::vector<ObjectID> references = {};
  arguments.emplace_back(std::make_shared<TaskArgumentByReference>(references));

  auto spec = TaskSpecification(UniqueID::from_random(), TaskID::from_random(), 0,
                                FunctionID::nil(), arguments, 0,
                                required_resources, Language::PYTHON);
  auto execution_spec = TaskExecutionSpecification(std::vector<ObjectID>());
  Task task = Task(execution_spec, spec);
  return task;
}

TEST(ResourceTest, TestSchedulingQueue) {
  SchedulingQueue queues;

  std::unordered_map<std::string, double> requirements = {{"CPU", 2}, {"GPU", 2}};
  auto rs = ResourceSet(requirements);
  auto task = CreateTask(requirements);
  auto task_id = task.GetTaskSpecification().TaskId();
  queues.QueueReadyTasks({task});
  auto rq = queues.GetReadyQueue();
  ASSERT_EQ(rq.GetMinTaskCount(), 1);
  ASSERT_EQ(rs, rq.GetMinTaskResources());

  auto task1 = CreateTask(requirements);
  auto task_id1 = task1.GetTaskSpecification().TaskId();
  queues.QueueReadyTasks({task1});
  rq = queues.GetReadyQueue();
  ASSERT_EQ(rq.GetMinTaskCount(), 2);
  ASSERT_EQ(rs, rq.GetMinTaskResources());

  std::unordered_map<std::string, double> requirements2 = {{"CPU", 2}};
  auto rs2 = ResourceSet(requirements2);
  auto task2 = CreateTask(requirements2);
  const auto task_id2 = task2.GetTaskSpecification().TaskId();
  queues.QueueReadyTasks({task2});
  rq = queues.GetReadyQueue();
  ASSERT_EQ(rq.GetMinTaskCount(), 1);
  ASSERT_EQ(rs2, rq.GetMinTaskResources());

  std::unordered_map<std::string, double> available = {{"CPU", 2}, {"GPU", 1}};
  auto rs_available = ResourceSet(available);
  auto schedule_flag = queues.GetReadyQueue().CanScheduleMinTask(rs_available);
  ASSERT_EQ(schedule_flag, true);

  std::unordered_set<TaskID> removed_tasks = {};
  removed_tasks.insert(task_id2);
  queues.RemoveTasks(removed_tasks);
  rq = queues.GetReadyQueue();
  ASSERT_EQ(rq.GetMinTaskCount(), 2);
  ASSERT_EQ(rs, rq.GetMinTaskResources());

  schedule_flag = queues.GetReadyQueue().CanScheduleMinTask(rs_available);
  ASSERT_EQ(schedule_flag, false);

  removed_tasks = {};
  removed_tasks.insert(task_id);
  queues.RemoveTasks(removed_tasks);
  rq = queues.GetReadyQueue();
  ASSERT_EQ(rq.GetMinTaskCount(), 1);
  ASSERT_EQ(rs, rq.GetMinTaskResources());

  removed_tasks = {};
  removed_tasks.insert(task_id1);
  queues.RemoveTasks(removed_tasks);
  rq = queues.GetReadyQueue();
  ASSERT_EQ(rq.GetMinTaskCount(), 0);
}


TEST(ResourceTest, TestReadyQueueMetadata) {
  ReadyQueue rq;

  auto rs = ResourceSet({{"CPU", 2}, {"GPU", 2}});
  rq.UpdateMinOnAdd(rs);

  rq.UpdateMinOnAdd(rs);
  ASSERT_EQ(rq.GetMinTaskCount(), 2);
  ASSERT_EQ(rs, rq.GetMinTaskResources());

  auto rs1 = ResourceSet({{"CPU", 2}, {"GPU", 1}});
  rq.UpdateMinOnAdd(rs1);
  ASSERT_EQ(rq.GetMinTaskCount(), 1);
  ASSERT_EQ(rs1, rq.GetMinTaskResources());

  auto rs2 = ResourceSet({{"CPU", 2}});
  rq.UpdateMinOnAdd(rs2);
  ASSERT_EQ(rq.GetMinTaskCount(), 1);
  ASSERT_EQ(rs2, rq.GetMinTaskResources());

  rq.UpdateMinOnRemove(rs);
  ASSERT_EQ(rq.GetMinTaskCount(), 1);
  ASSERT_EQ(rs2, rq.GetMinTaskResources());

  rq.UpdateMinOnRemove(rs1);
  ASSERT_EQ(rq.GetMinTaskCount(), 1);

  rq.UpdateMinOnRemove(rs2);
  ASSERT_EQ(rq.GetMinTaskCount(), 0);
}


}  // namespace raylet

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
