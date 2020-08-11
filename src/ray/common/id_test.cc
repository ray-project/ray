// Copyright 2017 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
    TestReturnObjectId(default_task_id, 1);
    TestReturnObjectId(default_task_id, 2);
    TestReturnObjectId(default_task_id, ObjectID::kMaxObjectIndex);
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

TEST(HashTest, TestNilHash) {
  // Manually trigger the hash calculation of the static global nil ID.
  auto nil_hash = ObjectID::Nil().Hash();
  ObjectID id1 = ObjectID::FromRandom();
  ASSERT_NE(nil_hash, id1.Hash());
  ObjectID id2 = ObjectID::FromBinary(ObjectID::FromRandom().Binary());
  ASSERT_NE(nil_hash, id2.Hash());
  ASSERT_NE(id1.Hash(), id2.Hash());
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
