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

#include <deque>
#include <string>
#include <utility>

namespace ray {
namespace core {

NormalTaskExecutionQueue::NormalTaskExecutionQueue(){};

void NormalTaskExecutionQueue::CancelAllQueuedTasks(const std::string &msg) {
  absl::MutexLock lock(&mu_);
  Status status = Status::SchedulingCancelled(msg);

  while (!pending_normal_tasks_.empty()) {
    auto it = pending_normal_tasks_.begin();
    it->Cancel(status);
    pending_normal_tasks_.erase(it);
  }
}

void NormalTaskExecutionQueue::Stop() {
  CancelAllQueuedTasks(
      "Normal task execution queue stopped; canceling all queued tasks.");
}

/// Add a new task's callbacks to the worker queue.
void NormalTaskExecutionQueue::Add(
    int64_t seq_no,
    int64_t client_processed_up_to,
    std::function<void(const TaskSpecification &, rpc::SendReplyCallback)> accept_request,
    std::function<void(const TaskSpecification &, const Status &, rpc::SendReplyCallback)>
        reject_request,
    rpc::SendReplyCallback send_reply_callback,
    TaskSpecification task_spec) {
  absl::MutexLock lock(&mu_);
  // Normal tasks should not have ordering constraints.
  RAY_CHECK(seq_no == -1);
  // Create a TaskToExecute object for the new task, and add it to the queue.

  pending_normal_tasks_.push_back(TaskToExecute(std::move(accept_request),
                                                std::move(reject_request),
                                                std::move(send_reply_callback),
                                                std::move(task_spec)));
}

bool NormalTaskExecutionQueue::CancelTaskIfFound(TaskID task_id) {
  absl::MutexLock lock(&mu_);
  for (std::deque<TaskToExecute>::reverse_iterator it = pending_normal_tasks_.rbegin();
       it != pending_normal_tasks_.rend();
       ++it) {
    if (it->TaskID() == task_id) {
      it->Cancel(Status::OK());
      pending_normal_tasks_.erase(std::next(it).base());
      return true;
    }
  }
  return false;
}

std::optional<TaskToExecute> NormalTaskExecutionQueue::TryPopQueuedTask() {
  absl::MutexLock lock(&mu_);
  if (pending_normal_tasks_.empty()) {
    return std::nullopt;
  }

  TaskToExecute task = std::move(pending_normal_tasks_.front());
  pending_normal_tasks_.pop_front();
  return task;
}

void NormalTaskExecutionQueue::ExecuteQueuedTasks() {
  while (auto task = TryPopQueuedTask()) {
    task->Accept();
  }
}

}  // namespace core
}  // namespace ray
