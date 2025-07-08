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
          ? retry_pending_queue.emplace(sequence_no, spec).second
          : requests
                .emplace(sequence_no, std::make_pair(spec, /*dependency_resolved*/ false))
                .second);
}

bool SequentialActorSubmitQueue::Contains(uint64_t sequence_no) const {
  return requests.contains(sequence_no) || retry_pending_queue.contains(sequence_no) ||
         retry_sending_queue.contains(sequence_no);
}

bool SequentialActorSubmitQueue::Empty() {
  return requests.empty() && retry_pending_queue.empty() && retry_sending_queue.empty();
}

bool SequentialActorSubmitQueue::DependencyResolved(uint64_t sequence_no) const {
  auto requests_it = requests.find(sequence_no);
  if (requests_it != requests.end()) {
    return requests_it->second.second;
  }
  if (retry_pending_queue.contains(sequence_no)) {
    return false;
  }
  RAY_CHECK(retry_sending_queue.contains(sequence_no));
  return true;
}

void SequentialActorSubmitQueue::MarkDependencyFailed(uint64_t sequence_no) {
  void(requests.erase(sequence_no) > 0 || retry_pending_queue.erase(sequence_no) > 0);
}

void SequentialActorSubmitQueue::MarkTaskCanceled(uint64_t sequence_no) {
  void(requests.erase(sequence_no) > 0 || retry_pending_queue.erase(sequence_no) > 0 ||
       retry_sending_queue.erase(sequence_no) > 0);
}

void SequentialActorSubmitQueue::MarkDependencyResolved(uint64_t sequence_no) {
  auto requests_it = requests.find(sequence_no);
  if (requests_it != requests.end()) {
    requests_it->second.second = true;
    return;
  }
  auto retry_pending_it = retry_pending_queue.find(sequence_no);
  if (retry_pending_it != retry_pending_queue.end()) {
    retry_sending_queue.emplace(sequence_no, std::move(retry_pending_it->second));
    retry_pending_queue.erase(retry_pending_it);
    return;
  }
}

std::vector<TaskID> SequentialActorSubmitQueue::ClearAllTasks() {
  std::vector<TaskID> task_ids;
  task_ids.reserve(requests.size() + retry_pending_queue.size() +
                   retry_sending_queue.size());
  for (auto &[_, spec] : requests) {
    task_ids.push_back(spec.first.TaskId());
  }
  for (auto &[_, spec] : retry_pending_queue) {
    task_ids.push_back(spec.TaskId());
  }
  for (auto &[_, spec] : retry_sending_queue) {
    task_ids.push_back(spec.TaskId());
  }
  requests.clear();
  retry_pending_queue.clear();
  retry_sending_queue.clear();
  return task_ids;
}

std::optional<std::pair<TaskSpecification, bool>>
SequentialActorSubmitQueue::PopNextTaskToSend() {
  if (!retry_sending_queue.empty()) {
    auto task_spec = std::move(retry_sending_queue.begin()->second);
    retry_sending_queue.erase(retry_sending_queue.begin());
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
