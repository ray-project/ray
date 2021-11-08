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

#pragma once

#include <map>
#include <utility>

#include "absl/types/optional.h"
#include "ray/common/id.h"
#include "ray/core_worker/transport/actor_submit_queue.h"

namespace ray {
namespace core {

/**
 * SequentialActorSumitQueue ensures actor are send in the sequence_no.
 * TODO(scv119): failures/reconnection.
 */
class SequentialActorSubmitQueue : public IActorSubmitQueue {
 public:
  SequentialActorSubmitQueue(ActorID actor_id) : actor_id(actor_id) {}

  bool Emplace(uint64_t sequence_no, TaskSpecification spec) override {
    return requests
        .emplace(sequence_no, std::make_pair(spec, /*dependency_resolved*/ false))
        .second;
  }

  bool Contains(uint64_t sequence_no) const override {
    return requests.find(sequence_no) != requests.end();
  }

  const std::pair<TaskSpecification, bool> &Get(uint64_t sequence_no) const override {
    auto it = requests.find(sequence_no);
    RAY_CHECK(it != requests.end());
    return it->second;
  }

  void MarkDependencyFailed(uint64_t sequence_no) override {
    requests.erase(sequence_no);
  }

  void MarkDependencyResolved(uint64_t sequence_no) override {
    auto it = requests.find(sequence_no);
    RAY_CHECK(it != requests.end());
    it->second.second = true;
  }

  std::vector<TaskID> ClearAllTasks() override {
    std::vector<TaskID> task_ids;
    for (auto &[pos, spec] : requests) {
      task_ids.push_back(spec.first.TaskId());
    }
    requests.clear();
    return task_ids;
  }

  absl::optional<std::pair<TaskSpecification, bool>> PopNextTaskToSend() override {
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

  std::map<uint64_t, TaskSpecification> PopAllOutOfOrderCompletedTasks() override {
    auto result = std::move(out_of_order_completed_tasks);
    out_of_order_completed_tasks.clear();
    return result;
  }

  void OnClientConnected() override {
    // This assumes that all replies from the previous incarnation
    // of the actor have been received. This assumption should be OK
    // because we fail all inflight tasks in `DisconnectRpcClient`.
    RAY_LOG(DEBUG) << "Resetting caller starts at for actor " << actor_id << " from "
                   << caller_starts_at << " to " << next_task_reply_position;
    caller_starts_at = next_task_reply_position;
  }

  uint64_t GetSequenceNumber(const TaskSpecification &task_spec) const override {
    RAY_CHECK(task_spec.ActorCounter() >= caller_starts_at)
        << "actor counter " << task_spec.ActorCounter() << " " << caller_starts_at;
    return task_spec.ActorCounter() - caller_starts_at;
  }

  void MarkTaskCompleted(uint64_t sequence_no, TaskSpecification task_spec) override {
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
                   << next_task_reply_position
                   << " and size of out_of_order_tasks set is "
                   << out_of_order_completed_tasks.size();
  }

 private:
  /// The ID of the actor.
  ActorID actor_id;

  /// The actor's pending requests, ordered by the task number (see below
  /// diagram) in the request. The bool indicates whether the dependencies
  /// for that task have been resolved yet. A task will be sent after its
  /// dependencies have been resolved and its task number matches
  /// next_send_position.
  std::map<uint64_t, std::pair<TaskSpecification, bool>> requests;

  /// Diagram of the sequence numbers assigned to actor tasks during actor
  /// crash and restart:
  ///
  /// The actor starts, and 10 tasks are submitted. We have sent 6 tasks
  /// (0-5) so far, and have received a successful reply for 4 tasks (0-3).
  /// 0 1 2 3 4 5 6 7 8 9
  ///             ^ next_send_position
  ///         ^ next_task_reply_position
  /// ^ caller_starts_at
  ///
  /// Suppose the actor crashes and recovers. Then, caller_starts_at is reset
  /// to the current next_task_reply_position. caller_starts_at is then subtracted
  /// from each task's counter, so the recovered actor will receive the
  /// sequence numbers 0, 1, 2 (and so on) for tasks 4, 5, 6, respectively.
  /// Therefore, the recovered actor will restart execution from task 4.
  /// 0 1 2 3 4 5 6 7 8 9
  ///             ^ next_send_position
  ///         ^ next_task_reply_position
  ///         ^ caller_starts_at
  ///
  /// New actor tasks will continue to be sent even while tasks are being
  /// resubmitted, but the receiving worker will only execute them after the
  /// resent tasks. For example, this diagram shows what happens if task 6 is
  /// sent for the first time, tasks 4 and 5 have been resent, and we have
  /// received a successful reply for task 4.
  /// 0 1 2 3 4 5 6 7 8 9
  ///               ^ next_send_position
  ///           ^ next_task_reply_position
  ///         ^ caller_starts_at
  ///
  /// The send position of the next task to send to this actor. This sequence
  /// number increases monotonically.
  uint64_t next_send_position = 0;
  /// The offset at which the the actor should start its counter for this
  /// caller. This is used for actors that can be restarted, so that the new
  /// instance of the actor knows from which task to start executing.
  uint64_t caller_starts_at = 0;
  /// Out of the tasks sent by this worker to the actor, the number of tasks
  /// that we will never send to the actor again. This is used to reset
  /// caller_starts_at if the actor dies and is restarted. We only include
  /// tasks that will not be sent again, to support automatic task retry on
  /// actor failure. This value only tracks consecutive tasks that are completed.
  /// Tasks completed out of order will be cached in out_of_completed_tasks first.
  uint64_t next_task_reply_position = 0;

  /// The temporary container for tasks completed out of order. It can happen in
  /// async or threaded actor mode. This map is used to store the seqno and task
  /// spec for (1) increment next_task_reply_position later when the in order tasks are
  /// returned (2) resend the tasks to restarted actor so retried tasks can maintain
  /// ordering.
  // NOTE(simon): consider absl::btree_set for performance, but it requires updating
  // abseil.
  std::map<uint64_t, TaskSpecification> out_of_order_completed_tasks;
};
}  // namespace core
}  // namespace ray