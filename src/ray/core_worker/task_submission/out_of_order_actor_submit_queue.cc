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

OutofOrderActorSubmitQueue::OutofOrderActorSubmitQueue(bool order_initial_submissions) : order_initial_submissions_(order_initial_submissions) {}

void OutofOrderActorSubmitQueue::Emplace(uint64_t position,
                                         const TaskSpecification &spec) {
  RAY_CHECK(
      spec.IsRetry()
          ? retries_waiting_for_dependencies_
                .emplace(sequence_no, spec)
                .second
          : waiting_for_dependencies_
                .emplace(sequence_no, spec)
                .second);
}

bool OutofOrderActorSubmitQueue::Contains(uint64_t position) const {
  return waiting_for_dependencies_.contains(position) || ready_to_send_.contains(position) || retries_waiting_for_dependencies_.contains(position) || retries_ready_to_send_.contains(position);
}

bool OutofOrderActorSubmitQueue::DependenciesResolved(uint64_t position) const {
  return ready_to_send_.contains(position) || retries_ready_to_send_.contains(position);
}

void OutofOrderActorSubmitQueue::MarkDependencyFailed(uint64_t position) {
  waiting_for_dependencies_.erase(position);
  retries_waiting_for_dependencies_.erase(position);
}

void OutofOrderActorSubmitQueue::MarkTaskCanceled(uint64_t position) {
  waiting_for_dependencies_.erase(position);
  ready_to_send_.erase(position);
  retries_waiting_for_dependencies_.erase(position);
  retries_ready_to_send_.erase(position);
}

bool OutofOrderActorSubmitQueue::Empty() {
  return waiting_for_dependencies_.empty() && ready_to_send_.empty() && retries_waiting_for_dependencies_.empty() && retries_ready_to_send_.empty();
}

void OutofOrderActorSubmitQueue::MarkDependencyResolved(uint64_t position) {
  // XXX.
  auto it = waiting_for_dependencies_.find(position);
  RAY_CHECK(it != waiting_for_dependencies_.end());
  auto spec = std::move(it->second.first);
  waiting_for_dependencies_.erase(it);

  ready_to_send_.emplace(position,
                         std::make_pair(std::move(spec), /*dependency_resolved*/ true));
}

std::vector<TaskID> OutofOrderActorSubmitQueue::ClearAllTasks() {
  std::vector<TaskID> task_ids;
  for (auto &[pos, spec] : waiting_for_dependencies_) {
    task_ids.push_back(spec.first.TaskId());
  }
  waiting_for_dependencies_.clear();
  for (auto &[pos, spec] : ready_to_send_) {
    task_ids.push_back(spec.first.TaskId());
  }
  ready_to_send_.clear();
  return task_ids;
}

std::optional<std::pair<TaskSpecification, bool>>
OutofOrderActorSubmitQueue::PopNextTaskToSend() {
  auto it = ready_to_send_.begin();
  if (it == ready_to_send_.end()) {
    return absl::nullopt;
  }

  auto task_spec = std::move(it->second.first);
  if (order_initial_submissions_) {
    // If the next one to send is a retry, then it's OK. Don't check seqno.
    RAY_CHECK(false); // TODO.
  } else {
    ready_to_send_.erase(it);
    return std::make_pair(std::move(task_spec), /*skip_queue*/ true);
  }

  return absl::nullopt;
}

}  // namespace core
}  // namespace ray
