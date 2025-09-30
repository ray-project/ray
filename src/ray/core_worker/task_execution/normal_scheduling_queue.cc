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

#include "ray/core_worker/task_execution/normal_scheduling_queue.h"

#include <deque>
#include <utility>

namespace ray {
namespace core {

NormalSchedulingQueue::NormalSchedulingQueue(){};

void NormalSchedulingQueue::Stop() {
  CancelAllPending(Status::SchedulingCancelled(
      "Normal scheduling queue stopped; canceling pending tasks"));
}

bool NormalSchedulingQueue::TaskQueueEmpty() const {
  absl::MutexLock lock(&mu_);
  return pending_normal_tasks_.empty();
}

// Returns the current size of the task queue.
size_t NormalSchedulingQueue::Size() const {
  absl::MutexLock lock(&mu_);
  return pending_normal_tasks_.size();
}

/// Add a new task's callbacks to the worker queue.
void NormalSchedulingQueue::Add(
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
  // Create a InboundRequest object for the new task, and add it to the queue.

  pending_normal_tasks_.push_back(InboundRequest(std::move(accept_request),
                                                 std::move(reject_request),
                                                 std::move(send_reply_callback),
                                                 std::move(task_spec)));
}

// Search for an InboundRequest associated with the task that we are trying to cancel.
// If found, remove the InboundRequest from the queue and return true. Otherwise,
// return false.
bool NormalSchedulingQueue::CancelTaskIfFound(TaskID task_id) {
  absl::MutexLock lock(&mu_);
  for (std::deque<InboundRequest>::reverse_iterator it = pending_normal_tasks_.rbegin();
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

/// Schedules as many requests as possible in sequence.
void NormalSchedulingQueue::ScheduleRequests() {
  while (true) {
    InboundRequest head;
    {
      absl::MutexLock lock(&mu_);
      if (!pending_normal_tasks_.empty()) {
        head = std::move(pending_normal_tasks_.front());
        pending_normal_tasks_.pop_front();
      } else {
        return;
      }
    }
    head.Accept();
  }
}

void NormalSchedulingQueue::CancelAllPending(const Status &status) {
  absl::MutexLock lock(&mu_);
  while (!pending_normal_tasks_.empty()) {
    auto it = pending_normal_tasks_.begin();
    it->Cancel(status);
    pending_normal_tasks_.erase(it);
  }
}

}  // namespace core
}  // namespace ray
