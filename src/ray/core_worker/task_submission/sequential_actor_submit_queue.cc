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

#include <string>
#include <utility>
#include <vector>

namespace ray {
namespace core {

void SequentialActorSubmitQueue::Emplace(const std::string &concurrency_group,
                                         uint64_t sequence_no,
                                         const TaskSpecification &spec) {
  if (spec.IsRetry()) {
    auto &requests_for_group = retry_requests_per_group_[concurrency_group];
    RAY_CHECK(
        requests_for_group
            .emplace(sequence_no, std::make_pair(spec, /*dependency_resolved*/ false))
            .second);
  } else {
    auto &requests_for_group = requests_per_group_[concurrency_group];
    RAY_CHECK(
        requests_for_group
            .emplace(sequence_no, std::make_pair(spec, /*dependency_resolved*/ false))
            .second);
  }
}

bool SequentialActorSubmitQueue::Contains(const std::string &concurrency_group,
                                          uint64_t sequence_no) const {
  auto req_it = requests_per_group_.find(concurrency_group);
  if (req_it != requests_per_group_.end() && req_it->second.contains(sequence_no)) {
    return true;
  }
  auto retry_it = retry_requests_per_group_.find(concurrency_group);
  if (retry_it != retry_requests_per_group_.end() &&
      retry_it->second.contains(sequence_no)) {
    return true;
  }
  return false;
}

bool SequentialActorSubmitQueue::Empty() {
  return requests_per_group_.empty() && retry_requests_per_group_.empty();
}

bool SequentialActorSubmitQueue::DependenciesResolved(
    const std::string &concurrency_group, uint64_t sequence_no) const {
  auto req_it = requests_per_group_.find(concurrency_group);
  if (req_it != requests_per_group_.end()) {
    auto it = req_it->second.find(sequence_no);
    if (it != req_it->second.end()) {
      return it->second.second;
    }
  }
  auto retry_it = retry_requests_per_group_.find(concurrency_group);
  RAY_CHECK(retry_it != retry_requests_per_group_.end());
  auto it = retry_it->second.find(sequence_no);
  RAY_CHECK(it != retry_it->second.end());
  return it->second.second;
}

void SequentialActorSubmitQueue::MarkDependencyFailed(
    const std::string &concurrency_group, uint64_t sequence_no) {
  auto req_it = requests_per_group_.find(concurrency_group);
  if (req_it != requests_per_group_.end()) {
    if (req_it->second.erase(sequence_no) > 0) {
      if (req_it->second.empty()) {
        requests_per_group_.erase(req_it);
      }
      return;
    }
  }
  auto retry_it = retry_requests_per_group_.find(concurrency_group);
  if (retry_it != retry_requests_per_group_.end()) {
    retry_it->second.erase(sequence_no);
    if (retry_it->second.empty()) {
      retry_requests_per_group_.erase(retry_it);
    }
  }
}

void SequentialActorSubmitQueue::MarkTaskCanceled(const std::string &concurrency_group,
                                                  uint64_t sequence_no) {
  auto req_it = requests_per_group_.find(concurrency_group);
  if (req_it != requests_per_group_.end()) {
    if (req_it->second.erase(sequence_no) > 0) {
      if (req_it->second.empty()) {
        requests_per_group_.erase(req_it);
      }
      return;
    }
  }
  auto retry_it = retry_requests_per_group_.find(concurrency_group);
  if (retry_it != retry_requests_per_group_.end()) {
    retry_it->second.erase(sequence_no);
    if (retry_it->second.empty()) {
      retry_requests_per_group_.erase(retry_it);
    }
  }
}

void SequentialActorSubmitQueue::MarkDependencyResolved(
    const std::string &concurrency_group, uint64_t sequence_no) {
  auto req_it = requests_per_group_.find(concurrency_group);
  if (req_it != requests_per_group_.end()) {
    auto it = req_it->second.find(sequence_no);
    if (it != req_it->second.end()) {
      it->second.second = true;
      return;
    }
  }
  auto retry_it = retry_requests_per_group_.find(concurrency_group);
  if (retry_it != retry_requests_per_group_.end()) {
    auto it = retry_it->second.find(sequence_no);
    if (it != retry_it->second.end()) {
      it->second.second = true;
      return;
    }
  }
}

std::vector<TaskID> SequentialActorSubmitQueue::ClearAllTasks() {
  std::vector<TaskID> task_ids;
  for (const auto &[_, requests] : requests_per_group_) {
    for (const auto &[seq_no, spec] : requests) {
      task_ids.push_back(spec.first.TaskId());
    }
  }
  for (const auto &[_, retries] : retry_requests_per_group_) {
    for (const auto &[seq_no, spec] : retries) {
      task_ids.push_back(spec.first.TaskId());
    }
  }
  requests_per_group_.clear();
  retry_requests_per_group_.clear();
  return task_ids;
}

std::optional<std::pair<TaskSpecification, bool>>
SequentialActorSubmitQueue::PopNextTaskToSend() {
  // Pop any resolved retry from any group (retries have priority)
  for (auto it = retry_requests_per_group_.begin();
       it != retry_requests_per_group_.end();) {
    auto &retries = it->second;
    auto retry_iter = retries.begin();
    while (retry_iter != retries.end()) {
      if (!retry_iter->second.second) {
        retry_iter++;
        continue;
      }
      auto task_spec = std::move(retry_iter->second.first);
      retries.erase(retry_iter);
      if (retries.empty()) {
        retry_requests_per_group_.erase(it);
      }
      return std::make_pair(std::move(task_spec), /*skip_queue*/ true);
    }
    ++it;
  }
  // For each group, check if the head task has dependencies resolved.
  for (auto it = requests_per_group_.begin(); it != requests_per_group_.end();) {
    auto &requests = it->second;
    if (requests.begin()->second.second) {
      auto task_spec = std::move(requests.begin()->second.first);
      requests.erase(requests.begin());
      if (requests.empty()) {
        requests_per_group_.erase(it);
      }
      return std::make_pair(std::move(task_spec), /*skip_queue*/ false);
    }
    ++it;
  }
  return std::nullopt;
}

}  // namespace core
}  // namespace ray
