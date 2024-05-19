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
 * The caller should call PopNextTaskToSend to find next task to send; and once the
 * task is known to send, MarkSeqnoCompleted is expected to be called to remove
 * the task from sending queue.
 * This queue is also aware of client connection/reconnection, so it needs to
 * call OnClientConnected whenever client is connected, and use GetSequenceNumber
 * to know the actual sequence_no to send over the network.
 * For some of the implementation (such as SequentialActorSubmitQueue), it guarantees
 * the sequential order between client and server and works with retry/actor restart,
 * so PopAllOutOfOrderCompletedTasks is called during actor restart.
 *
 * This class is not thread safe.
 * TODO(scv119): the protocol could be improved.
 */
class IActorSubmitQueue {
 public:
  virtual ~IActorSubmitQueue() = default;
  /// Add a task into the queue. Returns false if a task with the same sequence_no has
  /// already been inserted.
  virtual bool Emplace(uint64_t sequence_no, const TaskSpecification &task_spec) = 0;
  /// If a task exists.
  virtual bool Contains(uint64_t sequence_no) const = 0;
  /// Get a task; the bool indicates if the task's dependency was resolved.
  virtual const std::pair<TaskSpecification, bool> &Get(uint64_t sequence_no) const = 0;
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
  virtual absl::optional<std::pair<TaskSpecification, bool>> PopNextTaskToSend() = 0;
  /// On client connect/reconnect, find all the tasks that's known to be
  /// executed out of order. This is specific to SequentialActorSubmitQueue.
  virtual std::map<uint64_t, TaskSpecification> PopAllOutOfOrderCompletedTasks() = 0;
  /// On client connected.
  virtual void OnClientConnected() = 0;
  /// Get the sequence number of the task to send according to the protocol.
  virtual uint64_t GetSequenceNumber(const TaskSpecification &task_spec) const = 0;
  /// Mark a task has been executed on the receiver side.
  virtual void MarkSeqnoCompleted(uint64_t sequence_no,
                                  const TaskSpecification &task_spec) = 0;
};
}  // namespace core
}  // namespace ray
