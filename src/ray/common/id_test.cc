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

void TestFromIndexObjectId(const TaskID &task_id, int64_t index) {
  // Round trip test for computing the object ID for an object created by a task, either
  // via an object put or by being a return object for the task.
  ObjectID obj_id = ObjectID::FromIndex(task_id, index);
  ASSERT_EQ(obj_id.TaskId(), task_id);
  ASSERT_EQ(obj_id.ObjectIndex(), index);
}

void TestRandomObjectId() {
  // Round trip test for computing the object ID from random.
  const ObjectID random_object_id = ObjectID::FromRandom();
  ASSERT_FALSE(random_object_id.TaskId().IsNil());
  ASSERT_EQ(random_object_id.ObjectIndex(), 0);
}

const static JobID kDefaultJobId = JobID::FromInt(199);

const static TaskID kDefaultDriverTaskId = TaskID::ForDriverTask(kDefaultJobId);

TEST(JobIDTest, TestJobID) {
  uint32_t id = 100;
  JobID job_id = JobID::FromInt(id);
  ASSERT_EQ(job_id.ToInt(), id);
}

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
    // test from index
    TestFromIndexObjectId(default_task_id, 1);
    TestFromIndexObjectId(default_task_id, 2);
    TestFromIndexObjectId(default_task_id, ObjectID::kMaxObjectIndex);
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
