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

#include "ray/observability/ray_task_definition_event.h"

#include <memory>
#include <string>
#include <utility>

#include "absl/strings/str_format.h"
#include "ray/observability/task_event_populators.h"
#include "ray/util/logging.h"

namespace ray {
namespace observability {

RayTaskDefinitionEvent::RayTaskDefinitionEvent(
    std::shared_ptr<const TaskSpecification> task_spec,
    const TaskID &task_id,
    const JobID &job_id,
    int32_t task_attempt,
    std::shared_ptr<const std::string> session_name,
    int64_t timestamp)
    : RayEvent<rpc::events::TaskDefinitionEvent>(
          rpc::events::RayEvent::CORE_WORKER,
          rpc::events::RayEvent::TASK_DEFINITION_EVENT,
          rpc::events::RayEvent::INFO,
          "",
          std::move(session_name),
          timestamp),
      task_spec_(std::move(task_spec)),
      task_id_(task_id),
      job_id_(job_id),
      task_attempt_(task_attempt) {}

std::string RayTaskDefinitionEvent::GetEntityId() const {
  return task_id_.Binary() + std::to_string(task_attempt_);
}

TaskAttemptId RayTaskDefinitionEvent::GetTaskAttempt() const {
  return {task_id_, task_attempt_};
}

void RayTaskDefinitionEvent::MergeData(
    RayEvent<rpc::events::TaskDefinitionEvent> &&other) {
  RAY_CHECK(false) << absl::StrFormat(
      "MergeData called on task definition event for task %s attempt %d; only one "
      "definition event is expected per task attempt.",
      task_id_.Hex(),
      task_attempt_);
}

ray::rpc::events::RayEvent RayTaskDefinitionEvent::SerializeData() && {
  ray::rpc::events::RayEvent event;
  PopulateTaskDefinitionEvent(*task_spec_,
                              task_id_,
                              job_id_,
                              task_attempt_,
                              *event.mutable_task_definition_event());
  return event;
}

}  // namespace observability
}  // namespace ray
