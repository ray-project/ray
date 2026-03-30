// Copyright 2026 The Ray Authors.
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

/// Represents a work item in the actor pool queue.
struct PoolWorkItem {
  /// Unique ID for this work item.
  TaskID work_item_id;

  /// The function to execute.
  RayFunction function;

  /// Task arguments.
  std::vector<std::unique_ptr<TaskArg>> args;

  /// ObjectIDs of by-reference arguments, precomputed for locality-aware scheduling.
  std::vector<ObjectID> arg_ids;

  /// Task options (num_returns, resources, etc).
  TaskOptions options;

  /// Number of times this work item has been attempted.
  int32_t attempt_number = 0;

  /// Timestamp when this work item was enqueued (in milliseconds).
  int64_t enqueued_at_ms = 0;

  PoolWorkItem() = default;
  PoolWorkItem(PoolWorkItem &&) noexcept = default;
  PoolWorkItem &operator=(PoolWorkItem &&) noexcept = default;
  PoolWorkItem(const PoolWorkItem &) = delete;
  PoolWorkItem &operator=(const PoolWorkItem &) = delete;
};

/// Abstract interface for actor pool work queues.
/// Different implementations can provide different ordering semantics.
class PoolWorkQueue {
 public:
  virtual ~PoolWorkQueue() = default;

  /// Enqueue a work item.
  ///
  /// \param item The work item to enqueue.
  virtual void Push(PoolWorkItem item) = 0;

  /// Dequeue a work item.
  ///
  /// \return The work item, or nullopt if no work is available.
  virtual std::optional<PoolWorkItem> Pop() = 0;

  /// Check if any work is available.
  ///
  /// \return True if there is work to process.
  virtual bool HasWork() const = 0;

  /// Get the total number of items in the queue.
  ///
  /// \return The queue depth.
  virtual size_t Size() const = 0;

  /// Clear all work items from the queue.
  virtual void Clear() = 0;
};

/// FIFO work queue implementation.
/// Work items are processed in first-in, first-out order.
/// This is the simplest and highest-throughput implementation.
class FifoPoolWorkQueue : public PoolWorkQueue {
 public:
  FifoPoolWorkQueue() = default;
  ~FifoPoolWorkQueue() override = default;

  void Push(PoolWorkItem item) override;

  std::optional<PoolWorkItem> Pop() override;

  bool HasWork() const override;

  size_t Size() const override;

  void Clear() override;

 private:
  std::deque<PoolWorkItem> queue_;
};

}  // namespace core
}  // namespace ray
