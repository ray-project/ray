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

#include "ray/gcs/gcs_server/gcs_ray_event_converter.h"

#include "ray/util/logging.h"

namespace ray {
namespace gcs {

void GcsRayEventConverter::ConvertToTaskEventDataRequest(
    rpc::events::AddEventsRequest &&request, rpc::AddTaskEventDataRequest &data) {
  auto *task_event_data = data.mutable_data();

  // Move dropped task attempts from the metadata
  *task_event_data->mutable_dropped_task_attempts() =
      std::move(request.events_data().task_events_metadata().dropped_task_attempts());

  // Move RayEvents to TaskEvents
  for (auto &event : *request.mutable_events_data()->mutable_events()) {
    rpc::TaskEvents task_event;
    switch (event.event_type()) {
    case rpc::events::RayEvent::TASK_DEFINITION_EVENT: {
      ConvertToTaskEvents(std::move(*event.mutable_task_definition_event()), task_event);
      break;
    }
    default:
      // TODO(can-anyscale): Handle other event types
      break;
    }
    *task_event_data->add_events_by_task() = std::move(task_event);
  }
  if (task_event_data->events_by_task_size() > 0) {
    task_event_data->set_job_id(task_event_data->events_by_task(0).job_id());
  }
}

void GcsRayEventConverter::ConvertToTaskEvents(rpc::events::TaskDefinitionEvent &&event,
                                               rpc::TaskEvents &task_event) {
  task_event.set_task_id(event.task_id());
  task_event.set_attempt_number(event.task_attempt());
  task_event.set_job_id(event.job_id());

  rpc::TaskInfoEntry *task_info = task_event.mutable_task_info();
  task_info->set_type(event.task_type());
  task_info->set_name(event.task_name());
  task_info->set_language(event.language());
  task_info->set_task_id(event.task_id());
  task_info->set_job_id(event.job_id());
  *task_info->mutable_runtime_env_info() = std::move(event.runtime_env_info());
  task_info->set_parent_task_id(event.parent_task_id());
  if (!event.placement_group_id().empty()) {
    task_info->set_placement_group_id(event.placement_group_id());
  }

  auto function_descriptor = event.task_func();
  if (event.language() == rpc::Language::CPP && function_descriptor.has_cpp_function_descriptor()) {
    task_info->set_func_or_class_name(
        function_descriptor.cpp_function_descriptor().function_name());
  } else if (event.language() == rpc::Language::PYTHON && function_descriptor.has_python_function_descriptor()) {
    task_info->set_func_or_class_name(
        function_descriptor.python_function_descriptor().function_name());
  } else if (event.language() == rpc::Language::JAVA && function_descriptor.has_java_function_descriptor()) {
    task_info->set_func_or_class_name(
        function_descriptor.java_function_descriptor().function_name());
  }
  *task_info->mutable_required_resources() = std::move(event.required_resources());
}

}  // namespace gcs
}  // namespace ray
