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

#include "ray/observability/ray_task_definition_event.h"

#include "ray/common/scheduling/label_selector.h"

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
  // Task identifier
  data_.set_task_id(task_id.Binary());
  data_.set_task_attempt(attempt_number);

  // Common fields
  data_.set_language(task_spec.GetLanguage());
  const auto &required_resources = task_spec.GetRequiredResources().GetResourceMap();
  data_.mutable_required_resources()->insert(required_resources.begin(),
                                             required_resources.end());
  data_.set_serialized_runtime_env(task_spec.RuntimeEnvInfo().serialized_runtime_env());
  data_.set_job_id(job_id.Binary());
  // NOTE: we set the parent task id of a task to the submitter task id, where the
  // submitter task id is:
  // - For concurrent actors: the actor creation task's task id.
  // - Otherwise: the CoreWorker main thread's task id.
  data_.set_parent_task_id(task_spec.SubmitterTaskId().Binary());
  data_.set_placement_group_id(task_spec.PlacementGroupBundleId().first.Binary());
  const auto &labels = task_spec.GetMessage().labels();
  data_.mutable_ref_ids()->insert(labels.begin(), labels.end());
  const auto &call_site = task_spec.GetMessage().call_site();
  if (!call_site.empty()) {
    data_.set_call_site(call_site);
  }
  const auto &label_selector = task_spec.GetMessage().label_selector();
  if (label_selector.label_constraints_size() > 0) {
    *data_.mutable_label_selector() = ray::LabelSelector(label_selector).ToStringMap();
  }

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
