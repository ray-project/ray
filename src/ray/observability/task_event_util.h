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

#pragma once

#include "ray/common/scheduling/label_selector.h"
#include "ray/common/task/task_spec.h"

namespace ray {
namespace observability {

// Sets common fields shared by TaskDefinitionEvent and ActorTaskDefinitionEvent.
template <typename T>
void SetCommonDefinitionFields(T *data,
                               const TaskSpecification &task_spec,
                               const TaskID &task_id,
                               int32_t attempt_number,
                               const JobID &job_id) {
  data->set_task_id(task_id.Binary());
  data->set_task_attempt(attempt_number);
  data->set_language(task_spec.GetLanguage());
  const auto &required_resources = task_spec.GetRequiredResources().GetResourceMap();
  data->mutable_required_resources()->insert(required_resources.begin(),
                                             required_resources.end());
  data->set_serialized_runtime_env(task_spec.RuntimeEnvInfo().serialized_runtime_env());
  data->set_job_id(job_id.Binary());
  // NOTE: we set the parent task id of a task to the submitter task id, where the
  // submitter task id is:
  // - For concurrent actors: the actor creation task's task id.
  // - Otherwise: the CoreWorker main thread's task id.
  data->set_parent_task_id(task_spec.SubmitterTaskId().Binary());
  data->set_placement_group_id(task_spec.PlacementGroupBundleId().first.Binary());
  const auto &labels = task_spec.GetMessage().labels();
  data->mutable_ref_ids()->insert(labels.begin(), labels.end());
  const auto &call_site = task_spec.GetMessage().call_site();
  if (!call_site.empty()) {
    data->set_call_site(call_site);
  }
  const auto &label_selector = task_spec.GetMessage().label_selector();
  if (label_selector.label_constraints_size() > 0) {
    *data->mutable_label_selector() = ray::LabelSelector(label_selector).ToStringMap();
  }
}

}  // namespace observability
}  // namespace ray
