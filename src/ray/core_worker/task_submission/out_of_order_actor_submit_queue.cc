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

#include <string>
#include <utility>
#include <vector>

namespace ray {
namespace core {

void OutofOrderActorSubmitQueue::Emplace(const std::string &concurrency_group,
                                         uint64_t position,
                                         const TaskSpecification &spec) {
  auto &sending = sending_queue_per_group_[concurrency_group];
  RAY_CHECK(!sending.contains(position));
  RAY_CHECK(pending_queue_per_group_[concurrency_group]
                .emplace(position, std::make_pair(spec, /*dependency_resolved*/ false))
                .second);
}

bool OutofOrderActorSubmitQueue::Contains(const std::string &concurrency_group,
                                          uint64_t position) const {
  auto pending_it = pending_queue_per_group_.find(concurrency_group);
  if (pending_it != pending_queue_per_group_.end() &&
      pending_it->second.contains(position)) {
    return true;
  }
  auto sending_it = sending_queue_per_group_.find(concurrency_group);
  if (sending_it != sending_queue_per_group_.end() &&
      sending_it->second.contains(position)) {
    return true;
  }
  return false;
}

bool OutofOrderActorSubmitQueue::DependenciesResolved(
    const std::string &concurrency_group, uint64_t position) const {
  auto pending_it = pending_queue_per_group_.find(concurrency_group);
  if (pending_it != pending_queue_per_group_.end()) {
    auto it = pending_it->second.find(position);
    if (it != pending_it->second.end()) {
      return it->second.second;
    }
  }
  auto sending_it = sending_queue_per_group_.find(concurrency_group);
  RAY_CHECK(sending_it != sending_queue_per_group_.end());
  auto rit = sending_it->second.find(position);
  RAY_CHECK(rit != sending_it->second.end());
  return rit->second.second;
}

void OutofOrderActorSubmitQueue::MarkDependencyFailed(
    const std::string &concurrency_group, uint64_t position) {
  auto it = pending_queue_per_group_.find(concurrency_group);
  if (it != pending_queue_per_group_.end()) {
    it->second.erase(position);
    if (it->second.empty()) {
      pending_queue_per_group_.erase(it);
    }
  }
}

void OutofOrderActorSubmitQueue::MarkTaskCanceled(const std::string &concurrency_group,
                                                  uint64_t position) {
  auto pending_it = pending_queue_per_group_.find(concurrency_group);
  if (pending_it != pending_queue_per_group_.end()) {
    if (pending_it->second.erase(position) > 0) {
      if (pending_it->second.empty()) {
        pending_queue_per_group_.erase(pending_it);
      }
      return;
    }
  }
  auto sending_it = sending_queue_per_group_.find(concurrency_group);
  if (sending_it != sending_queue_per_group_.end()) {
    sending_it->second.erase(position);
    if (sending_it->second.empty()) {
      sending_queue_per_group_.erase(sending_it);
    }
  }
}

bool OutofOrderActorSubmitQueue::Empty() {
  for (const auto &[_, queue] : pending_queue_per_group_) {
    if (!queue.empty()) {
      return false;
    }
  }
  for (const auto &[_, queue] : sending_queue_per_group_) {
    if (!queue.empty()) {
      return false;
    }
  }
  return true;
}

void OutofOrderActorSubmitQueue::MarkDependencyResolved(
    const std::string &concurrency_group, uint64_t position) {
  auto pending_it = pending_queue_per_group_.find(concurrency_group);
  RAY_CHECK(pending_it != pending_queue_per_group_.end());
  auto it = pending_it->second.find(position);
  RAY_CHECK(it != pending_it->second.end());
  auto spec = std::move(it->second.first);
  pending_it->second.erase(it);
  if (pending_it->second.empty()) {
    pending_queue_per_group_.erase(pending_it);
  }

  sending_queue_per_group_[concurrency_group].emplace(
      position, std::make_pair(std::move(spec), /*dependency_resolved*/ true));
}

std::vector<TaskID> OutofOrderActorSubmitQueue::ClearAllTasks() {
  std::vector<TaskID> task_ids;
  for (auto &[_, queue] : pending_queue_per_group_) {
    for (auto &[__, spec] : queue) {
      task_ids.push_back(spec.first.TaskId());
    }
  }
  pending_queue_per_group_.clear();
  for (auto &[_, queue] : sending_queue_per_group_) {
    for (auto &[__, spec] : queue) {
      task_ids.push_back(spec.first.TaskId());
    }
  }
  sending_queue_per_group_.clear();
  return task_ids;
}

std::optional<std::pair<TaskSpecification, bool>>
OutofOrderActorSubmitQueue::PopNextTaskToSend() {
  for (auto it = sending_queue_per_group_.begin(); it != sending_queue_per_group_.end();
       it++) {
    auto &sending = it->second;
    auto task_it = sending.begin();
    if (task_it != sending.end()) {
      auto task_spec = std::move(task_it->second.first);
      sending.erase(task_it);
      if (sending.empty()) {
        sending_queue_per_group_.erase(it);
      }
      return std::make_pair(std::move(task_spec), /*skip_queue*/ true);
    }
  }
  return std::nullopt;
}

}  // namespace core
}  // namespace ray
