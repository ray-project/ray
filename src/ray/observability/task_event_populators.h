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

#include <cstdint>
#include <iterator>
#include <optional>
#include <type_traits>

#include "ray/common/id.h"
#include "ray/common/scheduling/label_selector.h"
#include "ray/common/task/task_spec.h"
#include "ray/observability/task_state_update.h"
#include "src/ray/protobuf/public/events_actor_task_definition_event.pb.h"
#include "src/ray/protobuf/public/events_task_definition_event.pb.h"
#include "src/ray/protobuf/public/events_task_lifecycle_event.pb.h"

namespace ray {
namespace observability {

/**
 * @brief (claude) Populate the static metadata of a task attempt into a
 * TaskDefinitionEvent or an ActorTaskDefinitionEvent.
 */
template <typename T>
void PopulateTaskDefinitionEvent(const TaskSpecification &task_spec,
                                 const TaskID &task_id,
                                 const JobID &job_id,
                                 int32_t task_attempt,
                                 T &definition_event_data) {
  // Task identifier
  definition_event_data.set_task_id(task_id.Binary());
  definition_event_data.set_task_attempt(task_attempt);

  // Common fields
  definition_event_data.set_language(task_spec.GetLanguage());
  const auto &required_resources = task_spec.GetRequiredResources().GetResourceMap();
  definition_event_data.mutable_required_resources()->insert(
      std::make_move_iterator(required_resources.begin()),
      std::make_move_iterator(required_resources.end()));
  definition_event_data.set_serialized_runtime_env(
      task_spec.RuntimeEnvInfo().serialized_runtime_env());
  definition_event_data.set_job_id(job_id.Binary());
  // NOTE: we set the parent task id of a task to the submitter task id, where the
  // submitter  task id is:
  // - For concurrent actors: the actor creation task's task id.
  // - Otherwise: the CoreWorker main thread's task id.
  definition_event_data.set_parent_task_id(task_spec.SubmitterTaskId().Binary());
  definition_event_data.set_placement_group_id(
      task_spec.PlacementGroupBundleId().first.Binary());
  const auto &labels = task_spec.GetMessage().labels();
  definition_event_data.mutable_ref_ids()->insert(labels.begin(), labels.end());
  const auto &call_site = task_spec.GetMessage().call_site();
  if (!call_site.empty()) {
    definition_event_data.set_call_site(call_site);
  }
  const auto &label_selector = task_spec.GetMessage().label_selector();
  if (label_selector.label_constraints_size() > 0) {
    *definition_event_data.mutable_label_selector() =
        ray::LabelSelector(label_selector).ToStringMap();
  }

  const auto &fallback_strategy = task_spec.GetMessage().fallback_strategy();
  if (fallback_strategy.options_size() > 0) {
    definition_event_data.mutable_fallback_strategy()->CopyFrom(fallback_strategy);
  }

  // Specific fields
  if constexpr (std::is_same_v<T, rpc::events::ActorTaskDefinitionEvent>) {
    definition_event_data.mutable_actor_func()->CopyFrom(
        task_spec.FunctionDescriptor()->GetMessage());
    definition_event_data.set_actor_id(task_spec.ActorId().Binary());
    definition_event_data.set_actor_task_name(task_spec.GetName());
  } else {
    definition_event_data.mutable_task_func()->CopyFrom(
        task_spec.FunctionDescriptor()->GetMessage());
    definition_event_data.set_task_type(task_spec.GetMessage().type());
    definition_event_data.set_task_name(task_spec.GetName());
  }
  if (task_spec.IsDetachedActor()) {
    definition_event_data.set_is_detached_actor(true);
  }
}

/**
 * @brief (claude) Append one status change of a task attempt to a TaskLifecycleEvent,
 * along with the task property updates the change carries. Repeated calls accumulate the
 * state transitions of a single attempt into one event.
 */
void AppendTaskLifecycleUpdate(const TaskID &task_id,
                               const JobID &job_id,
                               int32_t task_attempt,
                               rpc::TaskStatus task_status,
                               int64_t timestamp,
                               const std::optional<TaskStateUpdate> &state_update,
                               rpc::events::TaskLifecycleEvent &lifecycle_event_data);

}  // namespace observability
}  // namespace ray
