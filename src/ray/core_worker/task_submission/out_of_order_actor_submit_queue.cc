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

#include "ray/core_worker/task_submission/out_of_order_actor_submit_queue.h"

#include <utility>
#include <vector>

namespace ray {
namespace core {

OutofOrderActorSubmitQueue::OutofOrderActorSubmitQueue(bool order_initial_submissions) : order_initial_submissions_(order_initial_submissions) {}

void OutofOrderActorSubmitQueue::Emplace(uint64_t seq_no,
                                         const TaskSpecification &spec) {
  RAY_CHECK(!ready_to_send_.contains(seq_no));
  RAY_CHECK(!retries_ready_to_send_.contains(seq_no));
  RAY_CHECK(waiting_for_dependencies_.emplace(seq_no, spec).second);
}

bool OutofOrderActorSubmitQueue::Contains(uint64_t seq_no) const {
  return waiting_for_dependencies_.contains(seq_no) || ready_to_send_.contains(seq_no) || retries_ready_to_send_.contains(seq_no);
}

bool OutofOrderActorSubmitQueue::DependenciesResolved(uint64_t seq_no) const {
  return ready_to_send_.contains(seq_no) || retries_ready_to_send_.contains(seq_no);
}

void OutofOrderActorSubmitQueue::MarkDependencyFailed(uint64_t seq_no) {
  waiting_for_dependencies_.erase(seq_no);
}

void OutofOrderActorSubmitQueue::MarkTaskCanceled(uint64_t seq_no) {
  waiting_for_dependencies_.erase(seq_no);
  ready_to_send_.erase(seq_no);
  retries_ready_to_send_.erase(seq_no);
}

bool OutofOrderActorSubmitQueue::Empty() const {
  return waiting_for_dependencies_.empty() && ready_to_send_.empty() && retries_ready_to_send_.empty();
}

void OutofOrderActorSubmitQueue::MarkDependencyResolved(uint64_t seq_no) {
  // Pop the seq_no entry from waiting_from_dependencies_ and push it onto the
  // appropriate queue.
  auto it = waiting_for_dependencies_.find(seq_no);
  RAY_CHECK(it != waiting_for_dependencies_.end());

  auto spec = std::move(it->second);
  waiting_for_dependencies_.erase(it);

  RAY_CHECK(
      spec.IsRetry()
          ? retries_ready_to_send_
                .emplace(seq_no, spec)
                .second
          : ready_to_send_
                .emplace(seq_no, spec)
                .second);
}

std::vector<TaskID> OutofOrderActorSubmitQueue::ClearAllTasks() {
  std::vector<TaskID> task_ids;

  for (auto &[pos, spec] : waiting_for_dependencies_) {
    task_ids.push_back(spec.TaskId());
  }
  waiting_for_dependencies_.clear();

  for (auto &[pos, spec] : ready_to_send_) {
    task_ids.push_back(spec.TaskId());
  }
  ready_to_send_.clear();

  for (auto &[pos, spec] : retries_ready_to_send_) {
    task_ids.push_back(spec.TaskId());
  }
  retries_ready_to_send_.clear();

  return task_ids;
}

std::optional<std::pair<TaskSpecification, bool>>
OutofOrderActorSubmitQueue::PopNextTaskToSend() {
  // First try to send any retries that have dependencies resolved.
  // Retries do not respect sequence number ordering.
  auto retry_it = retries_ready_to_send_.begin();
  if (retry_it != retries_ready_to_send_.end()) {
    auto task_spec = std::move(retry_it->second);
    retries_ready_to_send_.erase(retry_it);
    return std::make_pair(std::move(task_spec), /*skip_queue=*/true);
  }

  auto it = ready_to_send_.begin();
  if (it == ready_to_send_.end()) {
    // Nothing is ready to send.
    return absl::nullopt;
  }

  if (order_initial_submissions_) {
    if (it->first != next_seq_no_) {
      // Initial submissions must be ordered but the next seq_no is not ready to send.
      return absl::nullopt;
    }

    next_seq_no_++;
  }

  auto task_spec = std::move(it->second);
  ready_to_send_.erase(it);
  return std::make_pair(std::move(task_spec), /*skip_queue=*/false);
}

}  // namespace core
}  // namespace ray
