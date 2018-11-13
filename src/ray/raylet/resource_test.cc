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

void TestTaskReturnId(const TaskID &task_id, int64_t return_index) {
  // Round trip test for computing the object ID for a task's return value,
  // then computing the task ID that created the object.
  ObjectID return_id = ComputeReturnId(task_id, return_index);
  ASSERT_EQ(ComputeTaskId(return_id), task_id);
  ASSERT_EQ(ComputeObjectIndex(return_id), return_index);
}

void TestTaskPutId(const TaskID &task_id, int64_t put_index) {
  // Round trip test for computing the object ID for a task's put value, then
  // computing the task ID that created the object.
  ObjectID put_id = ComputePutId(task_id, put_index);
  ASSERT_EQ(ComputeTaskId(put_id), task_id);
  ASSERT_EQ(ComputeObjectIndex(put_id), -1 * put_index);
}


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
  auto qrm = queues.GetReadyQueue().GetQueueReadyMetadata();
  ASSERT_EQ(qrm.GetMinTaskCount(), 1);
  ASSERT_EQ(rs, qrm.GetMinTaskResources());

  auto task1 = CreateTask(requirements);
  auto task_id1 = task1.GetTaskSpecification().TaskId();
  queues.QueueReadyTasks({task1});
  qrm = queues.GetReadyQueue().GetQueueReadyMetadata();
  ASSERT_EQ(qrm.GetMinTaskCount(), 2);
  ASSERT_EQ(rs, qrm.GetMinTaskResources());

  std::unordered_map<std::string, double> requirements2 = {{"CPU", 2}};
  auto rs2 = ResourceSet(requirements2);
  auto task2 = CreateTask(requirements2);
  const auto task_id2 = task2.GetTaskSpecification().TaskId();
  queues.QueueReadyTasks({task2});
  qrm = queues.GetReadyQueue().GetQueueReadyMetadata();
  ASSERT_EQ(qrm.GetMinTaskCount(), 1);
  ASSERT_EQ(rs2, qrm.GetMinTaskResources());

  std::unordered_map<std::string, double> available = {{"CPU", 2}, {"GPU", 1}};
  auto rs_available = ResourceSet(available);
  auto schedule_flag = queues.GetReadyQueue().CanScheduleMinTask(rs_available);
  ASSERT_EQ(schedule_flag, true);

  std::unordered_set<TaskID> removed_tasks = {};
  removed_tasks.insert(task_id2);
  queues.RemoveTasks(removed_tasks);
  qrm = queues.GetReadyQueue().GetQueueReadyMetadata();
  ASSERT_EQ(qrm.GetMinTaskCount(), 2);
  ASSERT_EQ(rs, qrm.GetMinTaskResources());

  schedule_flag = queues.GetReadyQueue().CanScheduleMinTask(rs_available);
  ASSERT_EQ(schedule_flag, false);

  removed_tasks = {};
  removed_tasks.insert(task_id);
  queues.RemoveTasks(removed_tasks);
  qrm = queues.GetReadyQueue().GetQueueReadyMetadata();
  ASSERT_EQ(qrm.GetMinTaskCount(), 1);
  ASSERT_EQ(rs, qrm.GetMinTaskResources());

  removed_tasks = {};
  removed_tasks.insert(task_id1);
  queues.RemoveTasks(removed_tasks);
  qrm = queues.GetReadyQueue().GetQueueReadyMetadata();
  ASSERT_EQ(qrm.GetMinTaskCount(), 0);
}

TEST(ResourceTest, TestTaskQueue) {
  SchedulingQueue::TaskQueue task_queue;

  std::unordered_map<std::string, double> requirements = {{"CPU", 2}, {"GPU", 2}};
  auto rs = ResourceSet(requirements);

  auto task = CreateTask(requirements);
  auto task_id = task.GetTaskSpecification().TaskId();
  task_queue.AppendTask(task_id, task, true);
  auto qrm = task_queue.GetQueueReadyMetadata();
  ASSERT_EQ(qrm.GetMinTaskCount(), 1);
  ASSERT_EQ(rs, qrm.GetMinTaskResources());

  auto task1 = CreateTask(requirements);
  auto task_id1 = task1.GetTaskSpecification().TaskId();
  task_queue.AppendTask(task_id1, task1, true);
  qrm = task_queue.GetQueueReadyMetadata();
  ASSERT_EQ(qrm.GetMinTaskCount(), 2);
  ASSERT_EQ(rs, qrm.GetMinTaskResources());

  std::unordered_map<std::string, double> requirements2 = {{"CPU", 2}};
  auto rs2 = ResourceSet(requirements2);
  auto task2 = CreateTask(requirements2);
  auto task_id2 = task2.GetTaskSpecification().TaskId();
  task_queue.AppendTask(task_id2, task2, true);
  qrm = task_queue.GetQueueReadyMetadata();
  ASSERT_EQ(qrm.GetMinTaskCount(), 1);
  ASSERT_EQ(rs2, qrm.GetMinTaskResources());

  std::vector<Task> removed_tasks;
  bool flag = task_queue.RemoveTask(task_id2, &removed_tasks, true);
  ASSERT_EQ(flag, true);
  ASSERT_EQ(removed_tasks.size(), 1);
  qrm = task_queue.GetQueueReadyMetadata();
  ASSERT_EQ(qrm.GetMinTaskCount(), 2);
  ASSERT_EQ(rs, qrm.GetMinTaskResources());

  task_queue.RemoveTask(task_id, &removed_tasks, true);
  qrm = task_queue.GetQueueReadyMetadata();
  ASSERT_EQ(qrm.GetMinTaskCount(), 1);
  ASSERT_EQ(rs, qrm.GetMinTaskResources());

  task_queue.RemoveTask(task_id1, &removed_tasks, true);
  qrm = task_queue.GetQueueReadyMetadata();
  ASSERT_EQ(qrm.GetMinTaskCount(), 0);
}


TEST(ResourceTest, TestQueueReadyMetadata) {
  QueueReadyMetadata qrm;

  auto rs = ResourceSet({{"CPU", 2}, {"GPU", 2}});
  qrm.UpdateMinOnAdd(rs);

  qrm.UpdateMinOnAdd(rs);
  ASSERT_EQ(qrm.GetMinTaskCount(), 2);
  ASSERT_EQ(rs, qrm.GetMinTaskResources());

  auto rs1 = ResourceSet({{"CPU", 2}, {"GPU", 1}});
  qrm.UpdateMinOnAdd(rs1);
  ASSERT_EQ(qrm.GetMinTaskCount(), 1);
  ASSERT_EQ(rs1, qrm.GetMinTaskResources());

  auto rs2 = ResourceSet({{"CPU", 2}});
  qrm.UpdateMinOnAdd(rs2);
  ASSERT_EQ(qrm.GetMinTaskCount(), 1);
  ASSERT_EQ(rs2, qrm.GetMinTaskResources());

  qrm.UpdateMinOnRemove(rs);
  ASSERT_EQ(qrm.GetMinTaskCount(), 1);
  ASSERT_EQ(rs2, qrm.GetMinTaskResources());

  qrm.UpdateMinOnRemove(rs1);
  ASSERT_EQ(qrm.GetMinTaskCount(), 1);

  qrm.UpdateMinOnRemove(rs2);
  ASSERT_EQ(qrm.GetMinTaskCount(), 0);
}


}  // namespace raylet

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
