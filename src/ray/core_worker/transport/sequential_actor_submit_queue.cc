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

#include <map>
#include <utility>
#include <vector>

namespace ray {
namespace core {
SequentialActorSubmitQueue::SequentialActorSubmitQueue(ActorID actor_id)
    : actor_id(actor_id) {}

bool SequentialActorSubmitQueue::Emplace(uint64_t sequence_no,
                                         const TaskSpecification &spec) {
  return requests
      .emplace(sequence_no, std::make_pair(spec, /*dependency_resolved*/ false))
      .second;
}

bool SequentialActorSubmitQueue::Contains(uint64_t sequence_no) const {
  return requests.find(sequence_no) != requests.end();
}

bool SequentialActorSubmitQueue::Empty() { return requests.empty(); }

const std::pair<TaskSpecification, bool> &SequentialActorSubmitQueue::Get(
    uint64_t sequence_no) const {
  auto it = requests.find(sequence_no);
  RAY_CHECK(it != requests.end());
  return it->second;
}

void SequentialActorSubmitQueue::MarkDependencyFailed(uint64_t sequence_no) {
  requests.erase(sequence_no);
}

void SequentialActorSubmitQueue::MarkTaskCanceled(uint64_t sequence_no) {
  requests.erase(sequence_no);
}

void SequentialActorSubmitQueue::MarkDependencyResolved(uint64_t sequence_no) {
  auto it = requests.find(sequence_no);
  RAY_CHECK(it != requests.end());
  it->second.second = true;
}

std::vector<TaskID> SequentialActorSubmitQueue::ClearAllTasks() {
  std::vector<TaskID> task_ids;
  for (auto &[pos, spec] : requests) {
    task_ids.push_back(spec.first.TaskId());
  }
  requests.clear();
  return task_ids;
}

std::optional<std::pair<TaskSpecification, bool>>
SequentialActorSubmitQueue::PopNextTaskToSend() {
  auto head = requests.begin();
  if (head != requests.end() && (/*seqno*/ head->first <= next_send_position) &&
      (/*dependencies_resolved*/ head->second.second)) {
    // If the task has been sent before, skip the other tasks in the send
    // queue.
    bool skip_queue = head->first < next_send_position;
    auto task_spec = std::move(head->second.first);
    head = requests.erase(head);
    next_send_position++;
    return std::make_pair(std::move(task_spec), skip_queue);
  }
  return absl::nullopt;
}

}  // namespace core
}  // namespace ray
