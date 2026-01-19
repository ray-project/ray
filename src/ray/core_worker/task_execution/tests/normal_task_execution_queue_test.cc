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
#include "ray/core_worker/task_execution/normal_task_execution_queue.h"

#include <atomic>
#include <memory>

#include "gtest/gtest.h"
#include "ray/common/status.h"
#include "ray/common/task/task_spec.h"

namespace ray {
namespace core {

TEST(NormalTaskExecutionQueueTest, TestCancelQueuedTask) {
  std::unique_ptr<NormalTaskExecutionQueue> queue =
      std::make_unique<NormalTaskExecutionQueue>();
  int n_ok = 0;
  int n_rej = 0;
  auto fn_ok = [&n_ok](const TaskSpecification &task_spec,
                       rpc::SendReplyCallback callback) { n_ok++; };
  auto fn_rej = [&n_rej](const TaskSpecification &task_spec,
                         const Status &status,
                         rpc::SendReplyCallback callback) { n_rej++; };
  TaskSpecification task_spec;
  task_spec.GetMutableMessage().set_type(TaskType::NORMAL_TASK);
  queue->Add(-1, -1, fn_ok, fn_rej, nullptr, task_spec);
  queue->Add(-1, -1, fn_ok, fn_rej, nullptr, task_spec);
  queue->Add(-1, -1, fn_ok, fn_rej, nullptr, task_spec);
  queue->Add(-1, -1, fn_ok, fn_rej, nullptr, task_spec);
  queue->Add(-1, -1, fn_ok, fn_rej, nullptr, task_spec);
  ASSERT_TRUE(queue->CancelTaskIfFound(TaskID::Nil()));
  queue->ExecuteQueuedTasks();
  ASSERT_EQ(n_ok, 4);
  ASSERT_EQ(n_rej, 1);

  queue->Stop();
}

TEST(NormalTaskExecutionQueueTest, StopCancelsQueuedTasks) {
  std::unique_ptr<NormalTaskExecutionQueue> queue =
      std::make_unique<NormalTaskExecutionQueue>();
  int n_ok = 0;
  std::atomic<int> n_rej{0};
  auto fn_ok = [&n_ok](const TaskSpecification &task_spec,
                       rpc::SendReplyCallback callback) { n_ok++; };
  auto fn_rej = [&n_rej](const TaskSpecification &task_spec,
                         const Status &status,
                         rpc::SendReplyCallback callback) {
    ASSERT_TRUE(status.IsSchedulingCancelled());
    n_rej.fetch_add(1);
  };
  TaskSpecification task_spec;
  task_spec.GetMutableMessage().set_type(TaskType::NORMAL_TASK);

  // Enqueue several normal tasks but do not schedule them.
  queue->Add(-1, -1, fn_ok, fn_rej, nullptr, task_spec);
  queue->Add(-1, -1, fn_ok, fn_rej, nullptr, task_spec);
  queue->Add(-1, -1, fn_ok, fn_rej, nullptr, task_spec);

  // Stopping should cancel all queued tasks without running them.
  queue->Stop();

  ASSERT_EQ(n_ok, 0);
  ASSERT_EQ(n_rej.load(), 3);
}

}  // namespace core
}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
