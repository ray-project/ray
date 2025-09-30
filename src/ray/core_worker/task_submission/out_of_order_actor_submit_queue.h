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

#include <utility>
#include <vector>

#include "absl/container/btree_map.h"
#include "absl/types/optional.h"
#include "ray/common/id.h"
#include "ray/core_worker/task_submission/actor_submit_queue.h"

namespace ray {
namespace core {

/**
 * OutofOrderActorSubmitQueue sends request as soon as the dependencies are resolved.
 *
 * Under the hood, it keeps requests in two queues: pending queue and sending queue.
 * Emplaced request is inserted into pending queue; once the request's dependency
 * is resolved it's moved from pending queue into sending queue.
 */
class OutofOrderActorSubmitQueue : public IActorSubmitQueue {
 public:
  OutofOrderActorSubmitQueue();
  /// Add a task into the queue.
  void Emplace(uint64_t position, const TaskSpecification &spec) override;
  /// If a task exists.
  bool Contains(uint64_t position) const override;
  /// If the task's dependencies were resolved.
  bool DependenciesResolved(uint64_t position) const override;
  /// Mark a task's dependency resolution failed thus remove from the queue.
  void MarkDependencyFailed(uint64_t position) override;
  /// Make a task's dependency is resolved thus ready to send.
  void MarkDependencyResolved(uint64_t position) override;
  // Mark a task has been canceled.
  // If a task hasn't been sent yet, this API will guarantee a task won't be
  // popped via PopNextTaskToSend.
  void MarkTaskCanceled(uint64_t position) override;
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
  absl::btree_map<uint64_t, std::pair<TaskSpecification, bool>> pending_queue_;
  absl::btree_map<uint64_t, std::pair<TaskSpecification, bool>> sending_queue_;
};
}  // namespace core
}  // namespace ray
