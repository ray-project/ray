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
#include <vector>

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
  std::optional<std::pair<TaskSpecification, bool>> PopNextTaskToSend() override;
  bool Empty() override;

 private:
  /// The ID of the actor.
  ActorID actor_id;

  /// The actor's pending requests, ordered by the sequence number in the request.
  /// The bool indicates whether the dependencies for that task have been resolved yet.
  /// A task will be sent after its dependencies have been resolved and its sequence
  /// number matches next_send_position.
  std::map<uint64_t, std::pair<TaskSpecification, bool>> requests;

  /// All tasks with sequence numbers less than next_send_position have already been
  /// sent to the actor.
  uint64_t next_send_position = 0;
};
}  // namespace core
}  // namespace ray
