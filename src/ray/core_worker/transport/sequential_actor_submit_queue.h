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
 * SequentialActorSumitQueue extends IActorSubmitQueue and ensures tasks are send
 * in the sequential order defined by the sequence no.
 */
class SequentialActorSubmitQueue : public IActorSubmitQueue {
 public:
  explicit SequentialActorSubmitQueue(ActorID actor_id);
  /// Add a task into the queue. Returns false if a task with the same sequence_no has
  /// already been inserted.
  bool Emplace(uint64_t sequence_no, const TaskSpecification &task_spec) override;
  /// If a task exists.
  bool Contains(uint64_t sequence_no) const override;
  /// Get a task; the bool indicates if the task's dependency was resolved.
  const std::pair<TaskSpecification, bool> &Get(uint64_t sequence_no) const override;
  /// Mark a task's dependency resolution failed thus remove from the queue.
  void MarkDependencyFailed(uint64_t sequence_no) override;
  /// Make a task's dependency is resolved thus ready to send.
  void MarkDependencyResolved(uint64_t sequence_no) override;
  // Mark a task has been canceled.
  // If a task hasn't been sent yet, this API will guarantee a task won't be
  // popped via PopNextTaskToSend.
  void MarkTaskCanceled(uint64_t sequence_no) override;
  /// Clear the queue and returns all tasks ids that haven't been sent yet.
  std::vector<TaskID> ClearAllTasks() override;
  /// Find next task to send.
  /// \return
  ///   - nullopt if no task ready to send
  ///   - a pair of task and bool represents the task to be send and if the receiver
  ///     should SKIP THE SCHEDULING QUEUE while executing it.
  absl::optional<std::pair<TaskSpecification, bool>> PopNextTaskToSend() override;
  /// On client connect/reconnect, find all the tasks which are known to be
  /// executed out of order.
  std::map<uint64_t, TaskSpecification> PopAllOutOfOrderCompletedTasks() override;
  /// Called on client connected/reconnected. This reset the offset for
  /// sequence number.
  void OnClientConnected() override;
  /// Get the task's sequence number according to the internal offset.
  uint64_t GetSequenceNumber(const TaskSpecification &task_spec) const override;
  /// Mark a task has been executed on the receiver side.
  void MarkSeqnoCompleted(uint64_t sequence_no,
                          const TaskSpecification &task_spec) override;

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
  ///
  /// If a task raised a retryable user exception, it's marked as "completed" via
  /// `MarkSeqnoCompleted` and `next_task_reply_position` may be updated. Afterwards Ray
  /// retries by creating another task pushed to the back of the queue, making it executes
  /// later than all tasks pending in the queue.
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