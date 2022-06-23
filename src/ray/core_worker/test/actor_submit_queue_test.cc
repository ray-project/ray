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

#include <thread>

#include "gtest/gtest.h"
#include "ray/common/test_util.h"
#include "ray/core_worker/transport/out_of_order_actor_submit_queue.h"

namespace ray {
namespace core {
namespace {
TaskSpecification BuildTaskSpec(uint64_t seq) {
  TaskSpecification spec;
  spec.GetMutableMessage().set_task_id(TaskID::FromRandom(JobID()).Binary());
  spec.GetMutableMessage().set_type(ray::rpc::TaskType::ACTOR_TASK);
  spec.GetMutableMessage().mutable_actor_task_spec()->set_actor_counter(seq);
  return spec;
}
}  // namespace

TEST(OutofOrderActorSubmitQueueTest, PassThroughTest) {
  OutofOrderActorSubmitQueue queue(ActorID{});
  // insert request 0 1 2 3 4
  for (uint64_t i = 0; i < 5; i++) {
    EXPECT_TRUE(queue.Emplace(i, BuildTaskSpec(i)));
  }
  // insert request 0 again fails
  EXPECT_FALSE(queue.Emplace(0, BuildTaskSpec(0)));
  // contains and gets
  for (uint64_t i = 0; i < 5; i++) {
    EXPECT_TRUE(queue.Contains(i));
    EXPECT_FALSE(queue.Get(i).second);
  }
  // dependency failure remove request 4
  queue.MarkDependencyFailed(4);
  for (uint64_t i = 0; i < 5; i++) {
    if (i != 4) {
      EXPECT_TRUE(queue.Contains(i));
      EXPECT_FALSE(queue.Get(i).second);
    } else {
      EXPECT_FALSE(queue.Contains(i));
    }
  }

  // nothing is resolved.
  EXPECT_FALSE(queue.PopNextTaskToSend().has_value());

  // dependency resolved for request 1 and 3
  queue.MarkDependencyResolved(1);
  queue.MarkDependencyResolved(3);
  for (uint64_t i = 0; i < 4; i++) {
    EXPECT_TRUE(queue.Contains(i));
    if (i == 1 || i == 3) {
      EXPECT_TRUE(queue.Get(i).second);
    } else {
      EXPECT_FALSE(queue.Get(i).second);
    }
  }

  // task 1 and task 3 is ready to send.
  EXPECT_EQ(queue.PopNextTaskToSend()->first.ActorCounter(), 1);
  EXPECT_EQ(queue.PopNextTaskToSend()->first.ActorCounter(), 3);
  EXPECT_FALSE(queue.PopNextTaskToSend().has_value());

  EXPECT_TRUE(queue.PopAllOutOfOrderCompletedTasks().empty());

  // only contains task 2 and 4.
  for (uint64_t i = 0; i < 5; i++) {
    if (i == 0 || i == 2) {
      EXPECT_TRUE(queue.Contains(i));
      EXPECT_FALSE(queue.Get(i).second);
    } else {
      EXPECT_FALSE(queue.Contains(i));
    }
  }

  queue.MarkDependencyResolved(2);
  std::vector<TaskID> task_ids = {queue.Get(0).first.TaskId(),
                                  queue.Get(2).first.TaskId()};
  // clear all tasks.
  auto ret = queue.ClearAllTasks();
  EXPECT_EQ(ret, task_ids);
  for (uint64_t i = 0; i < 5; i++) {
    EXPECT_FALSE(queue.Contains(i));
  }
}

}  // namespace core
}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
