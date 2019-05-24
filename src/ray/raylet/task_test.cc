#include "gtest/gtest.h"

#include "ray/common/common_protocol.h"
#include "ray/raylet/task_spec.h"

namespace ray {

namespace raylet {

void TestTaskReturnId(const TaskID &task_id, int64_t return_index) {
  // Round trip test for computing the object ID for a task's return value,
  // then computing the task ID that created the object.
  ObjectID return_id = ObjectID::for_task_return(task_id, return_index);
  ASSERT_EQ(return_id.task_id(), task_id);
  ASSERT_EQ(return_id.object_index(), return_index);
}

void TestTaskPutId(const TaskID &task_id, int64_t put_index) {
  // Round trip test for computing the object ID for a task's put value, then
  // computing the task ID that created the object.
  ObjectID put_id = ObjectID::for_put(task_id, put_index);
  ASSERT_EQ(put_id.task_id(), task_id);
  ASSERT_EQ(put_id.object_index(), -1 * put_index);
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

TEST(IdPropertyTest, TestIdProperty) {
  TaskID task_id = TaskID::from_random();
  ASSERT_EQ(task_id, TaskID::from_binary(task_id.binary()));
  ObjectID object_id = ObjectID::from_random();
  ASSERT_EQ(object_id, ObjectID::from_binary(object_id.binary()));

  ASSERT_TRUE(TaskID().is_nil());
  ASSERT_TRUE(TaskID::nil().is_nil());
  ASSERT_TRUE(ObjectID().is_nil());
  ASSERT_TRUE(ObjectID::nil().is_nil());
}

TEST(TaskSpecTest, TaskInfoSize) {
  std::vector<ObjectID> references = {ObjectID::from_random(), ObjectID::from_random()};
  auto arguments_1 = std::make_shared<TaskArgumentByReference>(references);
  std::string one_arg("This is an value argument.");
  auto arguments_2 = std::make_shared<TaskArgumentByValue>(
      reinterpret_cast<const uint8_t *>(one_arg.c_str()), one_arg.size());
  std::vector<std::shared_ptr<TaskArgument>> task_arguments({arguments_1, arguments_2});
  auto task_id = TaskID::from_random();
  {
    flatbuffers::FlatBufferBuilder fbb;
    std::vector<flatbuffers::Offset<Arg>> arguments;
    for (auto &argument : task_arguments) {
      arguments.push_back(argument->ToFlatbuffer(fbb));
    }
    // General task.
    auto spec = CreateTaskInfo(
        fbb, to_flatbuf(fbb, DriverID::from_random()), to_flatbuf(fbb, task_id),
        to_flatbuf(fbb, TaskID::from_random()), 0, to_flatbuf(fbb, ActorID::nil()),
        to_flatbuf(fbb, ObjectID::nil()), 0, to_flatbuf(fbb, ActorID::nil()),
        to_flatbuf(fbb, ActorHandleID::nil()), 0,
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
        fbb, to_flatbuf(fbb, DriverID::from_random()), to_flatbuf(fbb, task_id),
        to_flatbuf(fbb, TaskID::from_random()), 10,
        to_flatbuf(fbb, ActorID::from_random()), to_flatbuf(fbb, ObjectID::from_random()),
        10000000, to_flatbuf(fbb, ActorID::from_random()),
        to_flatbuf(fbb, ActorHandleID::from_random()), 20,
        ids_to_flatbuf(fbb, std::vector<ObjectID>(
                                {ObjectID::from_random(), ObjectID::from_random()})),
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
