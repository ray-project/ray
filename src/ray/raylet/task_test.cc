#include "gtest/gtest.h"

#include "ray/raylet/task_spec.h"

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

TEST(TaskSpecTest, TestTaskReturnIds) {
  TaskID task_id = FinishTaskId(TaskID::from_random());

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
