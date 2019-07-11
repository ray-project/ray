#include "gtest/gtest.h"

#include "ray/common/common_protocol.h"
#include "ray/common/task/task_spec.h"

namespace ray {

void TestTaskReturnId(const TaskID &task_id, int64_t return_index) {
  // Round trip test for computing the object ID for a task's return value,
  // then computing the task ID that created the object.
  ObjectID return_id = ObjectID::ForTaskReturn(task_id, return_index);
  ASSERT_EQ(return_id.TaskId(), task_id);
  ASSERT_EQ(return_id.ObjectIndex(), return_index);
}

void TestTaskPutId(const TaskID &task_id, int64_t put_index) {
  // Round trip test for computing the object ID for a task's put value, then
  // computing the task ID that created the object.
  ObjectID put_id = ObjectID::ForPut(task_id, put_index);
  ASSERT_EQ(put_id.TaskId(), task_id);
  ASSERT_EQ(put_id.ObjectIndex(), -1 * put_index);
}

TEST(TaskSpecTest, TestTaskReturnIds) {
  TaskID task_id = TaskID::FromRandom();

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
  TaskID task_id = TaskID::FromRandom();
  ASSERT_EQ(task_id, TaskID::FromBinary(task_id.Binary()));
  ObjectID object_id = ObjectID::FromRandom();
  ASSERT_EQ(object_id, ObjectID::FromBinary(object_id.Binary()));

  ASSERT_TRUE(TaskID().IsNil());
  ASSERT_TRUE(TaskID::Nil().IsNil());
  ASSERT_TRUE(ObjectID().IsNil());
  ASSERT_TRUE(ObjectID::Nil().IsNil());
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
