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
#include <optional>

#include "gtest/gtest.h"
#include "ray/common/status.h"
#include "ray/common/task/task_spec.h"
#include "ray/core_worker/task_execution/common.h"

namespace ray {
namespace core {

namespace {

// Construct a TaskToExecute container for tests. The per-task execute/cancel behavior is
// supplied to the queue at construction time (not per task), so the container only needs
// to carry the task spec. The reply and send_reply_callback are dummy implementations
// that are never inspected by the tests' queue-level callbacks.
TaskToExecute MakeTaskToExecute(const TaskSpecification &task_spec) {
  static rpc::PushTaskReply dummy_reply;
  return TaskToExecute(
      task_spec,
      /*resource_ids=*/std::nullopt,
      &dummy_reply,
      [](const Status &, std::function<void()>, std::function<void()>) {});
}

}  // namespace

TEST(NormalTaskExecutionQueueTest, TestCancelQueuedTask) {
  int n_executed = 0;
  int n_canceled = 0;

  std::unique_ptr<NormalTaskExecutionQueue> queue =
      std::make_unique<NormalTaskExecutionQueue>(
          [&n_executed](TaskToExecute &task) { n_executed++; },
          [&n_canceled](const TaskToExecute &task, const Status &status) {
            n_canceled++;
          });

  TaskSpecification task_spec;
  task_spec.GetMutableMessage().set_type(TaskType::NORMAL_TASK);
  TaskToExecute task = MakeTaskToExecute(task_spec);

  queue->EnqueueTask(task);
  queue->EnqueueTask(task);
  queue->EnqueueTask(task);
  queue->EnqueueTask(task);
  queue->EnqueueTask(task);
  ASSERT_TRUE(queue->CancelTaskIfFound(TaskID::Nil()));
  queue->ExecuteQueuedTasks();
  ASSERT_EQ(n_executed, 4);
  ASSERT_EQ(n_canceled, 1);

  queue->Stop();
}

TEST(NormalTaskExecutionQueueTest, StopCancelsQueuedTasks) {
  int n_executed = 0;
  std::atomic<int> n_canceled{0};

  std::unique_ptr<NormalTaskExecutionQueue> queue =
      std::make_unique<NormalTaskExecutionQueue>(
          [&n_executed](TaskToExecute &task) { n_executed++; },
          [&n_canceled](const TaskToExecute &task, const Status &status) {
            ASSERT_TRUE(status.IsSchedulingCancelled());
            n_canceled.fetch_add(1);
          });

  TaskSpecification task_spec;
  task_spec.GetMutableMessage().set_type(TaskType::NORMAL_TASK);
  TaskToExecute task = MakeTaskToExecute(task_spec);

  // Enqueue several normal tasks but do not schedule them.
  queue->EnqueueTask(task);
  queue->EnqueueTask(task);
  queue->EnqueueTask(task);

  // Stopping should cancel all queued tasks without running them.
  queue->Stop();

  ASSERT_EQ(n_executed, 0);
  ASSERT_EQ(n_canceled.load(), 3);
}

}  // namespace core
}  // namespace ray
