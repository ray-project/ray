// Copyright 2025 The Ray Authors.
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

#include <deque>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "ray/common/id.h"
#include "ray/common/task/task_spec.h"
#include "ray/common/task/task_util.h"
#include "ray/core_worker/common.h"

namespace ray {
namespace core {

/// Represents a pool task in the actor pool queue.
struct PoolTask {
  /// The actor pool that owns this pool task.
  ActorPoolID pool_id;

  /// Unique ID for this pool task.
  TaskID pool_task_id;

  /// The function to execute.
  RayFunction function;

  /// Task arguments.
  std::vector<std::unique_ptr<TaskArg>> args;

  /// ObjectIDs of by-reference arguments, precomputed for locality-aware scheduling.
  std::vector<ObjectID> arg_ids;

  /// Task options (num_returns, resources, etc).
  TaskOptions options;

  /// Pre-created ObjectRefs tied to pool_task_id. Returned to caller immediately
  /// at submission time, before any actor is selected.
  std::vector<rpc::ObjectReference> return_refs;

  /// Number of times this pool task has been attempted.
  int32_t attempt_number = 0;

  /// Timestamp when this pool task was enqueued (in milliseconds).
  int64_t enqueued_at_ms = 0;

  /// Whether the task has been pushed to the actor via gRPC (OnTaskSubmitted fired).
  /// False means the task is in the ActorTaskSubmitter async pipeline but not yet
  /// counted in num_tasks_in_flight. GetOccupiedTaskSlots includes these as pending.
  bool pushed_to_actor = false;

  PoolTask() = default;

  /// Move constructor.
  PoolTask(PoolTask &&other) noexcept
      : pool_id(std::move(other.pool_id)),
        pool_task_id(std::move(other.pool_task_id)),
        function(std::move(other.function)),
        args(std::move(other.args)),
        arg_ids(std::move(other.arg_ids)),
        options(std::move(other.options)),
        return_refs(std::move(other.return_refs)),
        attempt_number(other.attempt_number),
        enqueued_at_ms(other.enqueued_at_ms),
        pushed_to_actor(other.pushed_to_actor) {}

  /// Move assignment.
  PoolTask &operator=(PoolTask &&other) noexcept {
    if (this != &other) {
      pool_id = std::move(other.pool_id);
      pool_task_id = std::move(other.pool_task_id);
      function = std::move(other.function);
      args = std::move(other.args);
      arg_ids = std::move(other.arg_ids);
      options = std::move(other.options);
      return_refs = std::move(other.return_refs);
      attempt_number = other.attempt_number;
      enqueued_at_ms = other.enqueued_at_ms;
      pushed_to_actor = other.pushed_to_actor;
    }
    return *this;
  }

  // Delete copy constructor and assignment
  PoolTask(const PoolTask &) = delete;
  PoolTask &operator=(const PoolTask &) = delete;
};

/// Abstract interface for actor pool work queues.
/// Different implementations can provide different ordering semantics.
class PoolTaskQueue {
 public:
  virtual ~PoolTaskQueue() = default;

  /// Enqueue a pool task.
  ///
  /// \param item The pool task to enqueue.
  virtual void Push(PoolTask item) = 0;

  /// Dequeue a pool task.
  ///
  /// \return The pool task, or nullopt if no work is available.
  virtual std::optional<PoolTask> Pop() = 0;

  /// Check if any work is available.
  ///
  /// \return True if there is work to process.
  virtual bool HasWork() const = 0;

  /// Get the total number of items in the queue.
  ///
  /// \return The queue depth.
  virtual size_t Size() const = 0;

  /// Clear all pool tasks from the queue.
  virtual void Clear() = 0;
};

/// Unordered work queue implementation.
/// Work items are processed in FIFO order with no ordering guarantees.
/// This is the simplest and highest-throughput implementation.
class FifoPoolTaskQueue : public PoolTaskQueue {
 public:
  FifoPoolTaskQueue() = default;
  ~FifoPoolTaskQueue() override = default;

  void Push(PoolTask item) override;

  std::optional<PoolTask> Pop() override;

  bool HasWork() const override;

  size_t Size() const override;

  void Clear() override;

 private:
  std::deque<PoolTask> queue_;
};

}  // namespace core
}  // namespace ray
