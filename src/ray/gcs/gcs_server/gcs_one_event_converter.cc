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
    case rpc::events::RayEvent::TASK_EXECUTION_EVENT: {
      ConvertTaskExecutionEventToTaskEvent(event.task_execution_event(), task_event);
      break;
    }
    case rpc::events::RayEvent::ACTOR_TASK_DEFINITION_EVENT: {
      ConvertActorTaskDefinitionEventToTaskEvent(event.actor_task_definition_event(),
                                                 task_event);
      break;
    }
    case rpc::events::RayEvent::ACTOR_TASK_EXECUTION_EVENT: {
      ConvertActorTaskExecutionEventToTaskEvent(event.actor_task_execution_event(),
                                                task_event);
      break;
    }
    case rpc::events::RayEvent::TASK_PROFILE_EVENT: {
      // Use const_cast to avoid copying - safe because we're transferring ownership
      auto &mutable_profile_event =
          const_cast<rpc::events::TaskProfileEvents &>(event.task_profile_events());
      ConvertTaskProfileEventsToTaskEvent(mutable_profile_event, task_event);
      break;
    }
    default:
      RAY_CHECK(false) << "Unexpected one event type: " << event.event_type();
      break;
    }
    task_event_data->add_events_by_task()->Swap(&task_event);
  }
  if (task_event_data->events_by_task_size() > 0) {
    task_event_data->set_job_id(task_event_data->events_by_task(0).job_id());
  }
}

void GcsOneEventConverter::ConvertActorTaskExecutionEventToTaskEvent(
    const rpc::events::ActorTaskExecutionEvent &event, rpc::TaskEvents &task_event) {
  task_event.set_task_id(event.task_id());
  task_event.set_attempt_number(event.task_attempt());
  task_event.set_job_id(event.job_id());

  rpc::TaskStateUpdate *task_state_update = task_event.mutable_state_updates();
  task_state_update->set_node_id(event.node_id());
  task_state_update->set_worker_id(event.worker_id());
  task_state_update->mutable_error_info()->CopyFrom(event.ray_error_info());
  task_state_update->set_worker_pid(event.worker_pid());

  // Copy task state
  for (const auto &[state, timestamp] : event.task_state()) {
    int64_t ns = timestamp.seconds() * 1000000000LL + timestamp.nanos();
    (*task_state_update->mutable_state_ts_ns())[state] = ns;
  }
}

void GcsOneEventConverter::ConvertTaskExecutionEventToTaskEvent(
    const rpc::events::TaskExecutionEvent &event, rpc::TaskEvents &task_event) {
  task_event.set_task_id(event.task_id());
  task_event.set_attempt_number(event.task_attempt());
  task_event.set_job_id(event.job_id());

  rpc::TaskStateUpdate *task_state_update = task_event.mutable_state_updates();
  task_state_update->set_node_id(event.node_id());
  task_state_update->set_worker_id(event.worker_id());
  task_state_update->mutable_error_info()->CopyFrom(event.ray_error_info());
  task_state_update->set_worker_pid(event.worker_pid());

  // Copy task state
  for (const auto &[state, timestamp] : event.task_state()) {
    int64_t ns = timestamp.seconds() * 1000000000LL + timestamp.nanos();
    (*task_state_update->mutable_state_ts_ns())[state] = ns;
  }
}

void GcsOneEventConverter::ConvertActorTaskDefinitionEventToTaskEvent(
    const rpc::events::ActorTaskDefinitionEvent &event, rpc::TaskEvents &task_event) {
  task_event.set_task_id(event.task_id());
  task_event.set_attempt_number(event.task_attempt());
  task_event.set_job_id(event.job_id());

  rpc::TaskInfoEntry *task_info = task_event.mutable_task_info();
  GenerateTaskInfoEntry(event.runtime_env_info(),
                        event.actor_func(),
                        event.required_resources(),
                        task_info);
}

void GcsOneEventConverter::ConvertTaskDefinitionEventToTaskEvent(
    const rpc::events::TaskDefinitionEvent &event, rpc::TaskEvents &task_event) {
  task_event.set_task_id(event.task_id());
  task_event.set_attempt_number(event.task_attempt());
  task_event.set_job_id(event.job_id());

  rpc::TaskInfoEntry *task_info = task_event.mutable_task_info();
  task_info->set_name(event.task_name());
  GenerateTaskInfoEntry(
      event.runtime_env_info(), event.task_func(), event.required_resources(), task_info);
}

void GcsOneEventConverter::GenerateTaskInfoEntry(
    const rpc::RuntimeEnvInfo &runtime_env_info,
    const rpc::FunctionDescriptor &function_descriptor,
    const ::google::protobuf::Map<std::string, std::string> &required_resources,
    rpc::TaskInfoEntry *task_info) {
  task_info->mutable_runtime_env_info()->CopyFrom(runtime_env_info);
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
  for (const auto &resource : required_resources) {
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

void GcsOneEventConverter::ConvertTaskProfileEventsToTaskEvent(
    const rpc::events::TaskProfileEvents &event, rpc::TaskEvents &task_event) {
  task_event.set_task_id(event.task_id());
  task_event.set_attempt_number(event.attempt_number());
  task_event.set_job_id(event.job_id());

  task_event.mutable_profile_events()->CopyFrom(event.profile_events());
}

}  // namespace gcs
}  // namespace ray
