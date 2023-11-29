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

#include "ray/core_worker/transport/sequential_actor_submit_queue.h"

namespace ray {
namespace core {
SequentialActorSubmitQueue::SequentialActorSubmitQueue(ActorID actor_id)
    : actor_id(actor_id) {}

bool SequentialActorSubmitQueue::Emplace(uint64_t sequence_no,
                                         const TaskSpecification &spec) {
  return requests
      .emplace(sequence_no, std::make_pair(spec, /*dependency_resolved*/ false))
      .second;
}

bool SequentialActorSubmitQueue::Contains(uint64_t sequence_no) const {
  return requests.find(sequence_no) != requests.end();
}

const std::pair<TaskSpecification, bool> &SequentialActorSubmitQueue::Get(
    uint64_t sequence_no) const {
  auto it = requests.find(sequence_no);
  RAY_CHECK(it != requests.end());
  return it->second;
}

void SequentialActorSubmitQueue::MarkDependencyFailed(uint64_t sequence_no) {
  requests.erase(sequence_no);
}

void SequentialActorSubmitQueue::MarkTaskCanceled(uint64_t sequence_no) {
  requests.erase(sequence_no);
  // No need to clean out_of_order_completed_tasks because
  // it means a task has been already submitted and finished.
}

void SequentialActorSubmitQueue::MarkDependencyResolved(uint64_t sequence_no) {
  auto it = requests.find(sequence_no);
  RAY_CHECK(it != requests.end());
  it->second.second = true;
}

std::vector<TaskID> SequentialActorSubmitQueue::ClearAllTasks() {
  std::vector<TaskID> task_ids;
  for (auto &[pos, spec] : requests) {
    task_ids.push_back(spec.first.TaskId());
  }
  requests.clear();
  return task_ids;
}

absl::optional<std::pair<TaskSpecification, bool>>
SequentialActorSubmitQueue::PopNextTaskToSend() {
  auto head = requests.begin();
  if (head != requests.end() && (/*seqno*/ head->first <= next_send_position) &&
      (/*dependencies_resolved*/ head->second.second)) {
    // If the task has been sent before, skip the other tasks in the send
    // queue.
    bool skip_queue = head->first < next_send_position;
    auto task_spec = std::move(head->second.first);
    head = requests.erase(head);
    next_send_position++;
    return std::make_pair(std::move(task_spec), skip_queue);
  }
  return absl::nullopt;
}

std::map<uint64_t, TaskSpecification>
SequentialActorSubmitQueue::PopAllOutOfOrderCompletedTasks() {
  auto result = std::move(out_of_order_completed_tasks);
  out_of_order_completed_tasks.clear();
  return result;
}

void SequentialActorSubmitQueue::OnClientConnected() {
  // This assumes that all replies from the previous incarnation
  // of the actor have been received. This assumption should be OK
  // because we fail all inflight tasks in `DisconnectRpcClient`.
  RAY_LOG(DEBUG) << "Resetting caller starts at for actor " << actor_id << " from "
                 << caller_starts_at << " to " << next_task_reply_position;
  caller_starts_at = next_task_reply_position;
}

uint64_t SequentialActorSubmitQueue::GetSequenceNumber(
    const TaskSpecification &task_spec) const {
  RAY_CHECK(task_spec.ActorCounter() >= caller_starts_at)
      << "actor counter " << task_spec.ActorCounter() << " " << caller_starts_at;
  return task_spec.ActorCounter() - caller_starts_at;
}

void SequentialActorSubmitQueue::MarkTaskCompleted(uint64_t sequence_no,
                                                   const TaskSpecification &task_spec) {
  // Try to increment queue.next_task_reply_position consecutively until we
  // cannot. In the case of tasks not received in order, the following block
  // ensure queue.next_task_reply_position are incremented to the max possible
  // value.
  out_of_order_completed_tasks.insert({sequence_no, task_spec});
  auto min_completed_task = out_of_order_completed_tasks.begin();
  while (min_completed_task != out_of_order_completed_tasks.end()) {
    if (min_completed_task->first == next_task_reply_position) {
      next_task_reply_position++;
      // increment the iterator and erase the old value
      out_of_order_completed_tasks.erase(min_completed_task++);
    } else {
      break;
    }
  }

  RAY_LOG(DEBUG) << "Got PushTaskReply for actor " << actor_id << " with actor_counter "
                 << sequence_no << " new queue.next_task_reply_position is "
                 << next_task_reply_position << " and size of out_of_order_tasks set is "
                 << out_of_order_completed_tasks.size();
}

}  // namespace core
}  // namespace ray