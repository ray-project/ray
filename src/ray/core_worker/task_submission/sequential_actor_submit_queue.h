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

#include <string>
#include <utility>
#include <vector>

#include "absl/container/btree_map.h"
#include "absl/container/flat_hash_map.h"
#include "absl/types/optional.h"
#include "ray/common/id.h"
#include "ray/core_worker/task_submission/actor_submit_queue.h"

namespace ray {
namespace core {

/**
 * SequentialActorSumitQueue extends IActorSubmitQueue and ensures tasks are send
 * in the sequential order defined by the sequence no.
 */
class SequentialActorSubmitQueue : public IActorSubmitQueue {
 public:
  /// Add a task into the queue.
  void Emplace(const std::string &concurrency_group,
               uint64_t sequence_no,
               const TaskSpecification &task_spec) override;
  /// If a task exists.
  bool Contains(const std::string &concurrency_group,
                uint64_t sequence_no) const override;
  /// If the task's dependencies were resolved.
  bool DependenciesResolved(const std::string &concurrency_group,
                            uint64_t sequence_no) const override;
  /// Mark a task's dependency resolution failed thus remove from the queue.
  void MarkDependencyFailed(const std::string &concurrency_group,
                            uint64_t sequence_no) override;
  /// Make a task's dependency is resolved thus ready to send.
  void MarkDependencyResolved(const std::string &concurrency_group,
                              uint64_t sequence_no) override;
  // Mark a task has been canceled.
  // If a task hasn't been sent yet, this API will guarantee a task won't be
  // popped via PopNextTaskToSend.
  void MarkTaskCanceled(const std::string &concurrency_group,
                        uint64_t sequence_no) override;
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
  /// Per-concurrency-group pending requests, ordered by the sequence number.
  /// The bool indicates whether the dependencies for that task have been resolved yet.
  /// A task will be sent after its dependencies are resolved.
  absl::flat_hash_map<std::string,
                      absl::btree_map<uint64_t, std::pair<TaskSpecification, bool>>>
      requests_per_group_;

  /// Per-concurrency-group task retries. The bool indicates whether the dependencies
  /// have been resolved yet. A task will be sent after its dependencies are resolved.
  /// This is a separate unordered map because the order in which retries are executed
  /// is purposefully not guaranteed.
  absl::flat_hash_map<std::string,
                      absl::flat_hash_map<uint64_t, std::pair<TaskSpecification, bool>>>
      retry_requests_per_group_;
};

}  // namespace core
}  // namespace ray
