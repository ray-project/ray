#include "gtest/gtest.h"

#include "ray/common/common_protocol.h"
#include "ray/common/task/task_spec.h"

namespace ray {

void TestReturnObjectId(const TaskID &task_id, int64_t return_index,
                        uint8_t transport_type) {
  // Round trip test for computing the object ID for a task's return value,
  // then computing the task ID that created the object.
  ObjectID return_id = ObjectID::ForTaskReturn(task_id, return_index, transport_type);
  ASSERT_TRUE(return_id.CreatedByTask());
  ASSERT_TRUE(return_id.IsReturnObject());
  ASSERT_FALSE(return_id.IsPutObject());
  ASSERT_EQ(return_id.TaskId(), task_id);
  ASSERT_TRUE(transport_type == return_id.GetTransportType());
  ASSERT_EQ(return_id.ObjectIndex(), return_index);
}

void TestPutObjectId(const TaskID &task_id, int64_t put_index) {
  // Round trip test for computing the object ID for a task's put value, then
  // computing the task ID that created the object.
  ObjectID put_id = ObjectID::ForPut(task_id, put_index, 1);
  ASSERT_TRUE(put_id.CreatedByTask());
  ASSERT_FALSE(put_id.IsReturnObject());
  ASSERT_TRUE(put_id.IsPutObject());
  ASSERT_EQ(put_id.TaskId(), task_id);
  ASSERT_TRUE(1 == put_id.GetTransportType());
  ASSERT_EQ(put_id.ObjectIndex(), put_index);
}

void TestRandomObjectId() {
  // Round trip test for computing the object ID from random.
  const ObjectID random_object_id = ObjectID::FromRandom();
  ASSERT_FALSE(random_object_id.CreatedByTask());
}

const static JobID kDefaultJobId = JobID::FromInt(199);

const static TaskID kDefaultDriverTaskId = TaskID::ForDriverTask(kDefaultJobId);

TEST(ActorIDTest, TestActorID) {
  {
    // test from binary
    const ActorID actor_id_1 = ActorID::Of(kDefaultJobId, kDefaultDriverTaskId, 1);
    const auto actor_id_1_binary = actor_id_1.Binary();
    const auto actor_id_2 = ActorID::FromBinary(actor_id_1_binary);
    ASSERT_EQ(actor_id_1, actor_id_2);
  }

  {
    // test get job id
    const ActorID actor_id = ActorID::Of(kDefaultJobId, kDefaultDriverTaskId, 1);
    ASSERT_EQ(kDefaultJobId, actor_id.JobId());
  }
}

TEST(TaskIDTest, TestTaskID) {
  // Round trip test for task ID.
  {
    const ActorID actor_id = ActorID::Of(kDefaultJobId, kDefaultDriverTaskId, 1);
    const TaskID task_id_1 =
        TaskID::ForActorTask(kDefaultJobId, kDefaultDriverTaskId, 1, actor_id);
    ASSERT_EQ(actor_id, task_id_1.ActorId());
  }
}

TEST(ObjectIDTest, TestObjectID) {
  const static ActorID default_actor_id =
      ActorID::Of(kDefaultJobId, kDefaultDriverTaskId, 1);
  const static TaskID default_task_id =
      TaskID::ForActorTask(kDefaultJobId, kDefaultDriverTaskId, 1, default_actor_id);

  {
    // test for put
    TestPutObjectId(default_task_id, 1);
    TestPutObjectId(default_task_id, 2);
    TestPutObjectId(default_task_id, ObjectID::kMaxObjectIndex);
  }

  {
    // test for return
    TestReturnObjectId(default_task_id, 1, 2);
    TestReturnObjectId(default_task_id, 2, 3);
    TestReturnObjectId(default_task_id, ObjectID::kMaxObjectIndex, 4);
  }

  {
    // test random object id
    TestRandomObjectId();
  }
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
