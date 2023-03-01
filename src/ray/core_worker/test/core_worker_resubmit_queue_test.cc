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
#include "ray/core_worker/core_worker.h"

namespace ray {
namespace core {

TEST(CoreWorkerResubmitQueueTest, EarlierTaskInsertedAheadOfExisting) {
  TaskSpecification task_spec;
  std::priority_queue<TaskToRetry, std::deque<TaskToRetry>, TaskToRetryDescComparator>
      to_resubmit_;
  to_resubmit_.push({1, task_spec});
  to_resubmit_.push({3, task_spec});
  to_resubmit_.push({0, task_spec});

  ASSERT_EQ(to_resubmit_.top().execution_time_ms, 0);
  to_resubmit_.pop();

  to_resubmit_.push({0, task_spec});
  to_resubmit_.push({2, task_spec});

  ASSERT_EQ(to_resubmit_.top().execution_time_ms, 0);
  to_resubmit_.pop();

  ASSERT_EQ(to_resubmit_.top().execution_time_ms, 1);
  to_resubmit_.pop();

  ASSERT_EQ(to_resubmit_.top().execution_time_ms, 2);
  to_resubmit_.pop();

  ASSERT_EQ(to_resubmit_.top().execution_time_ms, 3);
  to_resubmit_.pop();

  ASSERT_TRUE(to_resubmit_.empty());
}

}  // namespace core
}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
