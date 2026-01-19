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

#include "ray/observability/task_event_util.h"

namespace ray {
namespace observability {

RayTaskDefinitionEvent::RayTaskDefinitionEvent(const TaskSpecification &task_spec,
                                               const TaskID &task_id,
                                               int32_t attempt_number,
                                               const JobID &job_id,
                                               const std::string &session_name)
    : RayEvent<rpc::events::TaskDefinitionEvent>(
          rpc::events::RayEvent::CORE_WORKER,
          rpc::events::RayEvent::TASK_DEFINITION_EVENT,
          rpc::events::RayEvent::INFO,
          "",
          session_name) {
  SetCommonDefinitionFields(&data_, task_spec, task_id, attempt_number, job_id);

  // Normal task specific fields
  data_.mutable_task_func()->CopyFrom(task_spec.FunctionDescriptor()->GetMessage());
  data_.set_task_type(task_spec.GetMessage().type());
  data_.set_task_name(task_spec.GetName());
}

std::string RayTaskDefinitionEvent::GetEntityId() const {
  return data_.task_id() + "_" + std::to_string(data_.task_attempt());
}

void RayTaskDefinitionEvent::MergeData(
    RayEvent<rpc::events::TaskDefinitionEvent> &&other) {
  // Task definition events should not be merged - they are immutable
  // If we receive another definition event for the same task, we just keep the first one
}

ray::rpc::events::RayEvent RayTaskDefinitionEvent::SerializeData() && {
  ray::rpc::events::RayEvent event;
  event.mutable_task_definition_event()->Swap(&data_);
  return event;
}

}  // namespace observability
}  // namespace ray
