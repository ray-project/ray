#include "gtest/gtest.h"

#include "ray/raylet/task_spec.h"

namespace ray {

namespace raylet {

void TestTaskReturnId(const TaskId &task_id, int64_t return_index) {
  // Round trip test for computing the object ID for a task's return value,
  // then computing the task ID that created the object.
  ObjectId return_id = ObjectId::for_task_return(task_id, return_index);
  ASSERT_EQ(return_id.task_id(), task_id);
  ASSERT_EQ(return_id.object_index(), return_index);
}

void TestTaskPutId(const TaskId &task_id, int64_t put_index) {
  // Round trip test for computing the object ID for a task's put value, then
  // computing the task ID that created the object.
  ObjectId put_id = ObjectId::for_put(task_id, put_index);
  ASSERT_EQ(put_id.task_id(), task_id);
  ASSERT_EQ(put_id.object_index(), -1 * put_index);
}

TEST(TaskSpecTest, TestTaskReturnIds) {
  TaskId task_id = TaskId::from_random();

  // Check that we can compute between a task ID and the object IDs of its
  // return values and puts.
  TestTaskReturnId(task_id, 1);
  TestTaskReturnId(task_id, 2);
  TestTaskReturnId(task_id, kMaxTaskReturns);
  TestTaskPutId(task_id, 1);
  TestTaskPutId(task_id, 2);
  TestTaskPutId(task_id, kMaxTaskPuts);
}

TEST(IdPropertyTest, TestIdProperty) {
  TaskId task_id = TaskId::from_random();
  ASSERT_EQ(task_id, TaskId::from_binary(task_id.binary()));
  ObjectId object_id = ObjectId::from_random();
  ASSERT_EQ(object_id, ObjectId::from_binary(object_id.binary()));

  ASSERT_TRUE(TaskId().is_nil());
  ASSERT_TRUE(TaskId::nil().is_nil());
  ASSERT_TRUE(ObjectId().is_nil());
  ASSERT_TRUE(ObjectId::nil().is_nil());
}

}  // namespace raylet

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
