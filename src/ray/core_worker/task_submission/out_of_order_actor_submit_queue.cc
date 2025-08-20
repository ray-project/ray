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

#include "ray/core_worker/task_submission/out_of_order_actor_submit_queue.h"

#include <utility>
#include <vector>

namespace ray {
namespace core {

OutofOrderActorSubmitQueue::OutofOrderActorSubmitQueue() {}

void OutofOrderActorSubmitQueue::Emplace(uint64_t position,
                                         const TaskSpecification &spec) {
  RAY_CHECK(!sending_queue_.contains(position));
  RAY_CHECK(pending_queue_
                .emplace(position, std::make_pair(spec, /*dependency_resolved*/ false))
                .second);
}

bool OutofOrderActorSubmitQueue::Contains(uint64_t position) const {
  return pending_queue_.contains(position) || sending_queue_.contains(position);
}

bool OutofOrderActorSubmitQueue::DependenciesResolved(uint64_t position) const {
  auto it = pending_queue_.find(position);
  if (it != pending_queue_.end()) {
    return it->second.second;
  }
  auto rit = sending_queue_.find(position);
  RAY_CHECK(rit != sending_queue_.end());
  return rit->second.second;
}

void OutofOrderActorSubmitQueue::MarkDependencyFailed(uint64_t position) {
  pending_queue_.erase(position);
}

void OutofOrderActorSubmitQueue::MarkTaskCanceled(uint64_t position) {
  pending_queue_.erase(position);
  sending_queue_.erase(position);
}

bool OutofOrderActorSubmitQueue::Empty() {
  return pending_queue_.empty() && sending_queue_.empty();
}

void OutofOrderActorSubmitQueue::MarkDependencyResolved(uint64_t position) {
  // move the task from pending_requests queue to sending_requests queue.
  auto it = pending_queue_.find(position);
  RAY_CHECK(it != pending_queue_.end());
  auto spec = std::move(it->second.first);
  pending_queue_.erase(it);

  sending_queue_.emplace(position,
                         std::make_pair(std::move(spec), /*dependency_resolved*/ true));
}

std::vector<TaskID> OutofOrderActorSubmitQueue::ClearAllTasks() {
  std::vector<TaskID> task_ids;
  for (auto &[pos, spec] : pending_queue_) {
    task_ids.push_back(spec.first.TaskId());
  }
  pending_queue_.clear();
  for (auto &[pos, spec] : sending_queue_) {
    task_ids.push_back(spec.first.TaskId());
  }
  sending_queue_.clear();
  return task_ids;
}

std::optional<std::pair<TaskSpecification, bool>>
OutofOrderActorSubmitQueue::PopNextTaskToSend() {
  auto it = sending_queue_.begin();
  if (it == sending_queue_.end()) {
    return absl::nullopt;
  }
  auto task_spec = std::move(it->second.first);
  sending_queue_.erase(it);
  return std::make_pair(std::move(task_spec), /*skip_queue*/ true);
}

}  // namespace core
}  // namespace ray
