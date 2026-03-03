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

#include "ray/core_worker/task_submission/sequential_actor_submit_queue.h"

#include <utility>
#include <vector>

namespace ray {
namespace core {
SequentialActorSubmitQueue::SequentialActorSubmitQueue() {}

void SequentialActorSubmitQueue::Emplace(uint64_t sequence_no,
                                         const TaskSpecification &spec) {
  RAY_CHECK(
      spec.IsRetry()
          ? retry_requests
                .emplace(sequence_no, std::make_pair(spec, /*dependency_resolved*/ false))
                .second
          : requests
                .emplace(sequence_no, std::make_pair(spec, /*dependency_resolved*/ false))
                .second);
}

bool SequentialActorSubmitQueue::Contains(uint64_t sequence_no) const {
  return requests.contains(sequence_no) || retry_requests.contains(sequence_no);
}

bool SequentialActorSubmitQueue::Empty() {
  return requests.empty() && retry_requests.empty();
}

bool SequentialActorSubmitQueue::DependenciesResolved(uint64_t sequence_no) const {
  auto requests_it = requests.find(sequence_no);
  if (requests_it != requests.end()) {
    return requests_it->second.second;
  }
  auto retry_iter = retry_requests.find(sequence_no);
  RAY_CHECK(retry_iter != retry_requests.end());
  return retry_iter->second.second;
}

void SequentialActorSubmitQueue::MarkDependencyFailed(uint64_t sequence_no) {
  void(requests.erase(sequence_no) > 0 || retry_requests.erase(sequence_no) > 0);
}

void SequentialActorSubmitQueue::MarkTaskCanceled(uint64_t sequence_no) {
  void(requests.erase(sequence_no) > 0 || retry_requests.erase(sequence_no) > 0);
}

void SequentialActorSubmitQueue::MarkDependencyResolved(uint64_t sequence_no) {
  auto request_it = requests.find(sequence_no);
  if (request_it != requests.end()) {
    request_it->second.second = true;
    return;
  }
  auto retry_pending_it = retry_requests.find(sequence_no);
  if (retry_pending_it != retry_requests.end()) {
    retry_pending_it->second.second = true;
    return;
  }
}

std::vector<TaskID> SequentialActorSubmitQueue::ClearAllTasks() {
  std::vector<TaskID> task_ids;
  task_ids.reserve(requests.size() + retry_requests.size());
  for (auto &[_, spec] : requests) {
    task_ids.push_back(spec.first.TaskId());
  }
  for (auto &[_, spec] : retry_requests) {
    task_ids.push_back(spec.first.TaskId());
  }
  requests.clear();
  retry_requests.clear();
  return task_ids;
}

std::optional<std::pair<TaskSpecification, bool>>
SequentialActorSubmitQueue::PopNextTaskToSend() {
  auto retry_iter = retry_requests.begin();
  while (retry_iter != retry_requests.end()) {
    if (/*dependencies not resolved*/ !retry_iter->second.second) {
      retry_iter++;
      continue;
    }
    auto task_spec = std::move(retry_iter->second.first);
    retry_requests.erase(retry_iter);
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
