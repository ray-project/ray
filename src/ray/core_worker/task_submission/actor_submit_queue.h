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

#include "absl/types/optional.h"
#include "ray/common/id.h"
#include "ray/common/task/task_spec.h"

namespace ray {
namespace core {

/**
 * IActorSubmitQueue is responsible for queuing per actor requests on the client.
 * To use ActorSubmitQueue, user should Emplace a task with a monotincally increasing
 * (and unique) sequence_no.
 * The task is not ready until it's being marked as
 * MarkDependencyResolved; and being removed from the queue if it's beking marked as
 * MarkDependencyFailed.
 * The caller should call PopNextTaskToSend to find next task to send.
 * This queue is also aware of client connection/reconnection, so it needs to
 * call OnClientConnected whenever client is connected, and use GetSequenceNumber
 * to know the actual sequence_no to send over the network.
 *
 * This class is not thread safe.
 */
class IActorSubmitQueue {
 public:
  virtual ~IActorSubmitQueue() = default;
  /// Add a task into the queue.
  virtual void Emplace(uint64_t sequence_no, const TaskSpecification &task_spec) = 0;
  /// If a task exists.
  virtual bool Contains(uint64_t sequence_no) const = 0;
  /// If the task's dependencies were resolved.
  virtual bool DependenciesResolved(uint64_t sequence_no) const = 0;
  /// Mark a task's dependency resolution failed thus remove from the queue.
  virtual void MarkDependencyFailed(uint64_t sequence_no) = 0;
  /// Mark a task's dependency is resolved thus ready to send.
  virtual void MarkDependencyResolved(uint64_t sequence_no) = 0;
  // Mark a task has been canceled.
  // If a task hasn't been sent yet, this API will guarantee a task won't be
  // popped via PopNextTaskToSend.
  virtual void MarkTaskCanceled(uint64_t sequence_no) = 0;
  /// Clear the queue and returns all tasks ids that haven't been sent yet.
  virtual std::vector<TaskID> ClearAllTasks() = 0;
  /// Find next task to send.
  /// \return
  ///   - nullopt if no task ready to send
  ///   - a pair of task and bool represents the task to be send and if the receiver
  ///     should SKIP THE SCHEDULING QUEUE while executing it.
  virtual std::optional<std::pair<TaskSpecification, bool>> PopNextTaskToSend() = 0;
  virtual bool Empty() = 0;
};
}  // namespace core
}  // namespace ray
