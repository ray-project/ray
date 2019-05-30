#include "gtest/gtest.h"

#include "ray/common/common_protocol.h"
#include "ray/raylet/task_spec.h"

namespace ray {

namespace raylet {

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

TEST(TaskSpecTest, TaskInfoSize) {
  std::vector<ObjectID> references = {ObjectID::FromRandom(), ObjectID::FromRandom()};
  auto arguments_1 = std::make_shared<TaskArgumentByReference>(references);
  std::string one_arg("This is an value argument.");
  auto arguments_2 = std::make_shared<TaskArgumentByValue>(
      reinterpret_cast<const uint8_t *>(one_arg.c_str()), one_arg.size());
  std::vector<std::shared_ptr<TaskArgument>> task_arguments({arguments_1, arguments_2});
  auto task_id = TaskID::FromRandom();
  {
    flatbuffers::FlatBufferBuilder fbb;
    std::vector<flatbuffers::Offset<Arg>> arguments;
    for (auto &argument : task_arguments) {
      arguments.push_back(argument->ToFlatbuffer(fbb));
    }
    // General task.
    auto spec = CreateTaskInfo(
        fbb, to_flatbuf(fbb, DriverID::FromRandom()), to_flatbuf(fbb, task_id),
        to_flatbuf(fbb, TaskID::FromRandom()), 0, to_flatbuf(fbb, ActorID::Nil()),
        to_flatbuf(fbb, ObjectID::Nil()), 0, to_flatbuf(fbb, ActorID::Nil()),
        to_flatbuf(fbb, ActorHandleID::Nil()), 0,
        ids_to_flatbuf(fbb, std::vector<ObjectID>()), fbb.CreateVector(arguments), 1,
        map_to_flatbuf(fbb, {}), map_to_flatbuf(fbb, {}), Language::PYTHON,
        string_vec_to_flatbuf(fbb, {"PackageName", "ClassName", "FunctionName"}));
    fbb.Finish(spec);
    RAY_LOG(ERROR) << "Ordinary task info size: " << fbb.GetSize();
  }

  {
    flatbuffers::FlatBufferBuilder fbb;
    std::vector<flatbuffers::Offset<Arg>> arguments;
    for (auto &argument : task_arguments) {
      arguments.push_back(argument->ToFlatbuffer(fbb));
    }
    // General task.
    auto spec = CreateTaskInfo(
        fbb, to_flatbuf(fbb, DriverID::FromRandom()), to_flatbuf(fbb, task_id),
        to_flatbuf(fbb, TaskID::FromRandom()), 10, to_flatbuf(fbb, ActorID::FromRandom()),
        to_flatbuf(fbb, ObjectID::FromRandom()), 10000000,
        to_flatbuf(fbb, ActorID::FromRandom()),
        to_flatbuf(fbb, ActorHandleID::FromRandom()), 20,
        ids_to_flatbuf(
            fbb, std::vector<ObjectID>({ObjectID::FromRandom(), ObjectID::FromRandom()})),
        fbb.CreateVector(arguments), 2, map_to_flatbuf(fbb, {}), map_to_flatbuf(fbb, {}),
        Language::PYTHON,
        string_vec_to_flatbuf(fbb, {"PackageName", "ClassName", "FunctionName"}));
    fbb.Finish(spec);
    RAY_LOG(ERROR) << "Actor task info size: " << fbb.GetSize();
  }
}

}  // namespace raylet

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
