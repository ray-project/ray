#include "gtest/gtest.h"

#include "ray/common/common_protocol.h"
#include "ray/common/task/task_spec.h"

namespace ray {


void TestReturnObjectId(const TaskID &task_id, int64_t return_index) {
  // Round trip test for computing the object ID for a task's return value,
  // then computing the task ID that created the object.
  ObjectID return_id = ObjectID::ForTaskReturn(task_id, return_index);
  ASSERT_TRUE(return_id.IsTask());
  ASSERT_TRUE(return_id.IsReturnObject());
  ASSERT_FALSE(return_id.IsPutObject());
  ASSERT_EQ(return_id.TaskId(), task_id);
  ASSERT_TRUE(TransportType::STANDARD == return_id.GetTransportType());
  ASSERT_EQ(return_id.ObjectIndex(), return_index);
}

void TestPutObjectId(const TaskID &task_id, int64_t put_index) {
  // Round trip test for computing the object ID for a task's put value, then
  // computing the task ID that created the object.
  ObjectID put_id = ObjectID::ForPut(task_id, put_index);
  ASSERT_TRUE(put_id.IsTask());
  ASSERT_FALSE(put_id.IsReturnObject());
  ASSERT_TRUE(put_id.IsPutObject());
  ASSERT_EQ(put_id.TaskId(), task_id);
  ASSERT_TRUE(TransportType::STANDARD == put_id.GetTransportType());
  ASSERT_EQ(put_id.ObjectIndex(), put_index);
}

void TestRandomObjectId(TransportType transport_type) {
  // Round trip test for computing the object ID from random.
  const ObjectID random_object_id = ObjectID::FromRandom(transport_type);
  ASSERT_FALSE(random_object_id.IsTask());
  ASSERT_TRUE(transport_type == random_object_id.GetTransportType());
}

const static JobID DEFAULT_JOB_ID = JobID::FromInt(199);

TEST(ActorIDTest, TestActorID) {
  {
    // test from binary
    const ActorID actor_id_1 = ActorID::FromRandom(DEFAULT_JOB_ID);
    const auto actor_id_1_binary = actor_id_1.Binary();
    const auto actor_id_2 = ActorID::FromBinary(actor_id_1_binary);
    ASSERT_EQ(actor_id_1, actor_id_2);
  }

  {
    // test get job id
    const ActorID actor_id = ActorID::FromRandom(DEFAULT_JOB_ID);
    ASSERT_EQ(DEFAULT_JOB_ID, actor_id.JobId());
  }
}

TEST(ObjectIDTest, TestObjectID) {
  const static ActorID default_actor_id = ActorID::FromRandom(DEFAULT_JOB_ID);
  const static TaskID default_task_id = TaskID::FromRandom(default_actor_id);

  {
    // test for put
    TestPutObjectId(default_task_id, 1);
    TestPutObjectId(default_task_id, 2);
    TestPutObjectId(default_task_id, ObjectID::MAX_TASK_PUTS);
  }

  {
    // test for return
    TestReturnObjectId(default_task_id, 1);
    TestReturnObjectId(default_task_id, 2);
    TestReturnObjectId(default_task_id, ObjectID::MAX_TASK_RETURNS);
  }

  {
    // test random object id
    TestRandomObjectId(TransportType::STANDARD);
    TestRandomObjectId(TransportType::DIRECT_ACTOR_CALL);
  }
}

void TestObjectIdFlags(bool is_task, ObjectType object_type, TransportType transport_type) {
  using namespace object_id_helper;
  uint16_t flags = 0;
  SetIsTaskFlag(&flags, is_task);
  SetObjectTypeFlag(&flags, object_type);
  SetTransportTypeFlag(&flags, transport_type);
  ASSERT_EQ(is_task, IsTask(flags));
  ASSERT_EQ(object_type, GetObjectType(flags));
  ASSERT_EQ(transport_type, GetTransportType(flags));
}

TEST(HelperTest, TestHelper) {
  TestObjectIdFlags(true, ObjectType::PUT_OBJECT, TransportType::STANDARD);
  TestObjectIdFlags(true, ObjectType::PUT_OBJECT, TransportType::DIRECT_ACTOR_CALL);
  TestObjectIdFlags(true, ObjectType::RETURN_OBJECT, TransportType::STANDARD);
  TestObjectIdFlags(true, ObjectType::RETURN_OBJECT, TransportType::DIRECT_ACTOR_CALL);
  TestObjectIdFlags(false, ObjectType::PUT_OBJECT, TransportType::STANDARD);
  TestObjectIdFlags(false, ObjectType::PUT_OBJECT, TransportType::DIRECT_ACTOR_CALL);
  TestObjectIdFlags(false, ObjectType::RETURN_OBJECT, TransportType::STANDARD);
  TestObjectIdFlags(false, ObjectType::RETURN_OBJECT, TransportType::DIRECT_ACTOR_CALL);
}

TEST(NilTest, TestIsNil) {
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
