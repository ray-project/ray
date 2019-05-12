#include "gtest/gtest.h"

#include "ray/raylet/task_spec.h"

namespace ray {

namespace raylet {

void TestTaskReturnId(const TaskID &task_id, int64_t return_index) {
  // Round trip test for computing the object ID for a task's return value,
  // then computing the task ID that created the object.
  ObjectID return_id = ComputeReturnId(task_id, return_index);
  RAY_LOG(ERROR) << "TestTaskReturnId:" << return_id << ", with return index:" << return_index;
  RAY_LOG(ERROR) << "ComputeTaskId(return_id):" << ComputeTaskId(return_id);
  RAY_LOG(ERROR) << "ComputeObjectIndex(return_id):" << ComputeObjectIndex(return_id);
  ASSERT_EQ(ComputeTaskId(return_id), task_id);
  ASSERT_EQ(ComputeObjectIndex(return_id), return_index);
}

void TestTaskPutId(const TaskID &task_id, int64_t put_index) {
  // Round trip test for computing the object ID for a task's put value, then
  // computing the task ID that created the object.
  ObjectID put_id = ComputePutId(task_id, put_index);
  RAY_LOG(ERROR) << "TestTaskPutId:" << put_id << ", with return index:" << put_index;
  RAY_LOG(ERROR) << "ComputeTaskId(return_id):" << ComputeTaskId(put_id);
  RAY_LOG(ERROR) << "ComputeObjectIndex(return_id):" << ComputeObjectIndex(put_id);
  ASSERT_EQ(ComputeTaskId(put_id), task_id);
  ASSERT_EQ(ComputeObjectIndex(put_id), -1 * put_index);
}

TEST(TaskSpecTest, TestTaskReturnIds) {
  TaskID task_id = TaskID::from_random();

  // Check that we can compute between a task ID and the object IDs of its
  // return values and puts.
  TestTaskReturnId(task_id, 1);
  TestTaskReturnId(task_id, 2);
  TestTaskReturnId(task_id, kMaxTaskReturns);
  TestTaskPutId(task_id, 1);
  TestTaskPutId(task_id, 2);
  TestTaskPutId(task_id, kMaxTaskPuts);
}

}  // namespace raylet

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
