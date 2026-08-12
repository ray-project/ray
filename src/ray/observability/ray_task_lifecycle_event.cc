// Copyright 2026 The Ray Authors.
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

#include "ray/observability/ray_task_lifecycle_event.h"

#include <optional>
#include <string>
#include <utility>

#include "absl/strings/str_cat.h"
#include "ray/observability/task_event_populators.h"

namespace ray {
namespace observability {

RayTaskLifecycleEvent::RayTaskLifecycleEvent(
    const TaskID &task_id,
    const JobID &job_id,
    int32_t task_attempt,
    rpc::TaskStatus task_status,
    const std::optional<const TaskStateUpdate> &state_update,
    const std::string &session_name,
    int64_t timestamp)
    : RayEvent<rpc::events::TaskLifecycleEvent>(
          rpc::events::RayEvent::CORE_WORKER,
          rpc::events::RayEvent::TASK_LIFECYCLE_EVENT,
          rpc::events::RayEvent::INFO,
          "",
          session_name,
          timestamp),
      task_id_(task_id),
      job_id_(job_id),
      task_attempt_(task_attempt) {
  status_changes_.push_back({task_status, timestamp, state_update});
}

std::string RayTaskLifecycleEvent::GetEntityId() const {
  return absl::StrCat(task_id_.Binary(), task_attempt_);
}

TaskAttemptId RayTaskLifecycleEvent::GetTaskAttempt() const {
  return {task_id_.Binary(), task_attempt_};
}

void RayTaskLifecycleEvent::MergeData(RayEvent<rpc::events::TaskLifecycleEvent> &&other) {
  auto &&other_event = static_cast<RayTaskLifecycleEvent &&>(other);
  // The recorder merges later events into the earlier accumulator, so appending keeps the
  // status changes in chronological order.
  for (auto &status_change : other_event.status_changes_) {
    status_changes_.push_back(std::move(status_change));
  }
}

ray::rpc::events::RayEvent RayTaskLifecycleEvent::SerializeData() && {
  ray::rpc::events::RayEvent event;
  rpc::events::TaskLifecycleEvent *lifecycle_event_data =
      event.mutable_task_lifecycle_event();
  for (const StatusChange &status_change : status_changes_) {
    AppendTaskLifecycleUpdate(task_id_,
                              job_id_,
                              task_attempt_,
                              status_change.task_status,
                              status_change.timestamp,
                              status_change.state_update,
                              *lifecycle_event_data);
  }
  return event;
}

}  // namespace observability
}  // namespace ray
