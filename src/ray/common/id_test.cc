#include "gtest/gtest.h"

#include "ray/common/common_protocol.h"
#include "ray/common/task/task_spec.h"

namespace ray {


void TestReturnObjectId(const TaskID &task_id, int64_t return_index) {
  // Round trip test for computing the object ID for a task's return value,
  // then computing the task ID that created the object.
  ObjectID return_id = ObjectID::ForTaskReturn(task_id, return_index);
  ASSERT_TRUE(return_id.CreatedByTask());
  ASSERT_TRUE(return_id.IsReturnObject());
  ASSERT_FALSE(return_id.IsPutObject());
  ASSERT_EQ(return_id.TaskId(), task_id);
  ASSERT_TRUE(0 == return_id.GetTransportType());
  ASSERT_EQ(return_id.ObjectIndex(), return_index);
}

void TestPutObjectId(const TaskID &task_id, int64_t put_index) {
  // Round trip test for computing the object ID for a task's put value, then
  // computing the task ID that created the object.
  ObjectID put_id = ObjectID::ForPut(task_id, put_index);
  ASSERT_TRUE(put_id.CreatedByTask());
  ASSERT_FALSE(put_id.IsReturnObject());
  ASSERT_TRUE(put_id.IsPutObject());
  ASSERT_EQ(put_id.TaskId(), task_id);
  ASSERT_TRUE(0 == put_id.GetTransportType());
  ASSERT_EQ(put_id.ObjectIndex(), put_index);
}

void TestRandomObjectId() {
  // Round trip test for computing the object ID from random.
  const ObjectID random_object_id = ObjectID::FromRandom();
  ASSERT_FALSE(random_object_id.CreatedByTask());
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

// TODO(qwang): test task id.

TEST(ObjectIDTest, TestObjectID) {
  const static ActorID default_actor_id = ActorID::FromRandom(DEFAULT_JOB_ID);
  const static TaskID default_task_id = TaskID::FromRandom(default_actor_id);

  {
    // test for put
    TestPutObjectId(default_task_id, 1);
    TestPutObjectId(default_task_id, 2);
    TestPutObjectId(default_task_id, ObjectID::MAX_OBJECT_INDEX);
  }

  {
    // test for return
    TestReturnObjectId(default_task_id, 1);
    TestReturnObjectId(default_task_id, 2);
    TestReturnObjectId(default_task_id, ObjectID::MAX_OBJECT_INDEX);
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
