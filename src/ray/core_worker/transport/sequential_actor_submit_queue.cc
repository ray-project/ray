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

#include "ray/core_worker/transport/sequential_actor_submit_queue.h"

#include <utility>
#include <vector>

namespace ray {
namespace core {
SequentialActorSubmitQueue::SequentialActorSubmitQueue(ActorID actor_id)
    : actor_id(actor_id) {}

void SequentialActorSubmitQueue::Emplace(uint64_t sequence_no,
                                         const TaskSpecification &spec) {
  RAY_CHECK(
      spec.IsRetry()
          ? retries_args_pending.emplace(sequence_no, spec).second
          : requests
                .emplace(sequence_no, std::make_pair(spec, /*dependency_resolved*/ false))
                .second);
}

bool SequentialActorSubmitQueue::Contains(uint64_t sequence_no) const {
  return requests.contains(sequence_no) || retries_args_pending.contains(sequence_no) ||
         retries_args_resolved.contains(sequence_no);
}

bool SequentialActorSubmitQueue::Empty() {
  return requests.empty() && retries_args_pending.empty() &&
         retries_args_resolved.empty();
}

bool SequentialActorSubmitQueue::DependenciesResolved(uint64_t sequence_no) const {
  auto requests_it = requests.find(sequence_no);
  if (requests_it != requests.end()) {
    return requests_it->second.second;
  }
  if (retries_args_pending.contains(sequence_no)) {
    return false;
  }
  RAY_CHECK(retries_args_resolved.contains(sequence_no));
  return true;
}

void SequentialActorSubmitQueue::MarkDependencyFailed(uint64_t sequence_no) {
  void(requests.erase(sequence_no) > 0 || retries_args_pending.erase(sequence_no) > 0);
}

void SequentialActorSubmitQueue::MarkTaskCanceled(uint64_t sequence_no) {
  void(requests.erase(sequence_no) > 0 || retries_args_pending.erase(sequence_no) > 0 ||
       retries_args_resolved.erase(sequence_no) > 0);
}

void SequentialActorSubmitQueue::MarkDependencyResolved(uint64_t sequence_no) {
  auto request_it = requests.find(sequence_no);
  if (request_it != requests.end()) {
    request_it->second.second = true;
    return;
  }
  auto retry_pending_it = retries_args_pending.find(sequence_no);
  if (retry_pending_it != retries_args_pending.end()) {
    retries_args_resolved.emplace(sequence_no, std::move(retry_pending_it->second));
    retries_args_pending.erase(retry_pending_it);
    return;
  }
}

std::vector<TaskID> SequentialActorSubmitQueue::ClearAllTasks() {
  std::vector<TaskID> task_ids;
  task_ids.reserve(requests.size() + retries_args_pending.size() +
                   retries_args_resolved.size());
  for (auto &[_, spec] : requests) {
    task_ids.push_back(spec.first.TaskId());
  }
  for (auto &[_, spec] : retries_args_pending) {
    task_ids.push_back(spec.TaskId());
  }
  for (auto &[_, spec] : retries_args_resolved) {
    task_ids.push_back(spec.TaskId());
  }
  requests.clear();
  retries_args_pending.clear();
  retries_args_resolved.clear();
  return task_ids;
}

std::optional<std::pair<TaskSpecification, bool>>
SequentialActorSubmitQueue::PopNextTaskToSend() {
  if (!retries_args_resolved.empty()) {
    auto task_spec = std::move(retries_args_resolved.begin()->second);
    retries_args_resolved.erase(retries_args_resolved.begin());
    return std::make_pair(std::move(task_spec), /*skip_queue*/ true);
  }
  if (!requests.empty() && (/*dependencies_resolved*/ requests.begin()->second.second)) {
    auto task_spec = std::move(requests.begin()->second.first);
    requests.erase(requests.begin());
    return std::make_pair(std::move(task_spec), /*skip_queue*/ false);
  }
  return std::nullopt;
}

}  // namespace core
}  // namespace ray
