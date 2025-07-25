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

#include "ray/gcs/gcs_server/gcs_one_event_converter.h"

#include "ray/util/logging.h"

namespace ray {
namespace gcs {

void GcsOneEventConverter::ConvertToTaskEventData(
    const rpc::events::AddEventRequest &request, rpc::AddTaskEventDataRequest &data) {
  // Convert RayEventsData to TaskEventData
  auto *task_event_data = data.mutable_data();

  // Copy dropped task attempts from the metadata
  for (const auto &dropped_attempt :
       request.events_data().task_events_metadata().dropped_task_attempts()) {
    *task_event_data->add_dropped_task_attempts() = dropped_attempt;
  }

  // Convert RayEvents to TaskEvents
  for (const auto &event : request.events_data().events()) {
    rpc::TaskEvents task_event;
    switch (event.event_type()) {
    case rpc::events::RayEvent::TASK_DEFINITION_EVENT: {
      ConvertTaskDefinitionEventToTaskEvent(event.task_definition_event(), task_event);
      break;
    }
    default:
      // TODO(can-anyscale): Handle other event types
      break;
    }
    task_event_data->add_events_by_task()->Swap(&task_event);
  }
  if (task_event_data->events_by_task_size() > 0) {
    task_event_data->set_job_id(task_event_data->events_by_task(0).job_id());
  }
}

void GcsOneEventConverter::ConvertTaskDefinitionEventToTaskEvent(
    const rpc::events::TaskDefinitionEvent &event, rpc::TaskEvents &task_event) {
  task_event.set_task_id(event.task_id());
  task_event.set_attempt_number(event.task_attempt());
  task_event.set_job_id(event.job_id());

  rpc::TaskInfoEntry *task_info = task_event.mutable_task_info();
  task_info->set_name(event.task_name());
  task_info->mutable_runtime_env_info()->CopyFrom(event.runtime_env_info());
  auto function_descriptor = event.task_func();
  if (function_descriptor.has_cpp_function_descriptor()) {
    task_info->set_language(rpc::Language::CPP);
    task_info->set_func_or_class_name(
        function_descriptor.cpp_function_descriptor().function_name());
  } else if (function_descriptor.has_python_function_descriptor()) {
    task_info->set_language(rpc::Language::PYTHON);
    task_info->set_func_or_class_name(
        function_descriptor.python_function_descriptor().function_name());
  } else if (function_descriptor.has_java_function_descriptor()) {
    task_info->set_language(rpc::Language::JAVA);
    task_info->set_func_or_class_name(
        function_descriptor.java_function_descriptor().function_name());
  }

  // Copy required resources map
  for (const auto &resource : event.required_resources()) {
    try {
      double value = std::stod(resource.second);
      (*task_info->mutable_required_resources())[resource.first] = value;
    } catch (const std::exception &e) {
      // Skip invalid resource values but log a warning for debugging.
      RAY_LOG(WARNING)
              .WithField("resource_value", resource.second)
              .WithField("resource_name", resource.first)
              .WithField("error", e.what())
          << "Could not convert resource value.";
      continue;
    }
  }
}

}  // namespace gcs
}  // namespace ray
