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

#include <string>
#include <utility>

#include "absl/strings/escaping.h"
#include "absl/strings/str_format.h"
#include "ray/util/logging.h"

namespace ray {
namespace observability {

RayTaskDefinitionEvent::RayTaskDefinitionEvent(rpc::events::TaskDefinitionEvent data,
                                               const std::string &session_name)
    : RayEvent<rpc::events::TaskDefinitionEvent>(
          rpc::events::RayEvent::CORE_WORKER,
          rpc::events::RayEvent::TASK_DEFINITION_EVENT,
          rpc::events::RayEvent::INFO,
          "",
          session_name) {
  data_ = std::move(data);
}

std::string RayTaskDefinitionEvent::GetEntityId() const {
  return data_.task_id() + std::to_string(data_.task_attempt());
}

TaskAttemptId RayTaskDefinitionEvent::GetTaskAttempt() const {
  return {data_.task_id(), data_.task_attempt()};
}

void RayTaskDefinitionEvent::MergeData(
    RayEvent<rpc::events::TaskDefinitionEvent> &&other) {
  RAY_CHECK(false) << absl::StrFormat(
      "MergeData called on task definition event for task %s attempt %d; only one "
      "definition event is expected per task attempt.",
      absl::BytesToHexString(data_.task_id()),
      data_.task_attempt());
}

ray::rpc::events::RayEvent RayTaskDefinitionEvent::SerializeData() && {
  ray::rpc::events::RayEvent event;
  event.mutable_task_definition_event()->Swap(&data_);
  return event;
}

}  // namespace observability
}  // namespace ray
