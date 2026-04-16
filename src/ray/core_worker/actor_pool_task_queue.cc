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

#include "ray/core_worker/actor_pool_task_queue.h"

#include <utility>

namespace ray {
namespace core {

void FifoPoolTaskQueue::Push(PoolTask item) { queue_.push_back(std::move(item)); }

void FifoPoolTaskQueue::PushFront(PoolTask item) { queue_.push_front(std::move(item)); }

std::optional<PoolTask> FifoPoolTaskQueue::Pop() {
  if (queue_.empty()) {
    return std::nullopt;
  }

  PoolTask item = std::move(queue_.front());
  queue_.pop_front();
  return std::move(item);
}

bool FifoPoolTaskQueue::HasWork() const { return !queue_.empty(); }

size_t FifoPoolTaskQueue::Size() const { return queue_.size(); }

void FifoPoolTaskQueue::Clear() { queue_.clear(); }

}  // namespace core
}  // namespace ray
