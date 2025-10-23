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

#include "ray/gcs/gcs_ray_event_converter.h"

#include <google/protobuf/map.h>

#include "absl/container/flat_hash_map.h"
#include "ray/common/grpc_util.h"
#include "ray/common/id.h"

namespace ray {
namespace gcs {

namespace {

/// Populate the TaskInfoEntry with the given runtime env info, function descriptor,
/// and required resources. This function is commonly used to convert the task
/// and actor task definition events to TaskEvents.
///
/// \param serialized_runtime_env The serialized runtime environment string.
/// \param function_descriptor The function descriptor.
/// \param required_resources The required resources.
/// \param language The language of the task.
/// \param task_info The output TaskInfoEntry to populate.
void PopulateTaskRuntimeAndFunctionInfo(
    std::string &&serialized_runtime_env,
    rpc::FunctionDescriptor &&function_descriptor,
    ::google::protobuf::Map<std::string, double> &&required_resources,
    rpc::Language language,
    rpc::TaskInfoEntry *task_info) {
  task_info->set_language(language);
  task_info->mutable_runtime_env_info()->set_serialized_runtime_env(
      std::move(serialized_runtime_env));
  switch (language) {
  case rpc::Language::CPP:
    if (function_descriptor.has_cpp_function_descriptor()) {
      task_info->set_func_or_class_name(
          std::move(*function_descriptor.mutable_cpp_function_descriptor()
                         ->mutable_function_name()));
    }
    break;
  case rpc::Language::PYTHON:
    if (function_descriptor.has_python_function_descriptor()) {
      task_info->set_func_or_class_name(
          std::move(*function_descriptor.mutable_python_function_descriptor()
                         ->mutable_function_name()));
    }
    break;
  case rpc::Language::JAVA:
    if (function_descriptor.has_java_function_descriptor()) {
      task_info->set_func_or_class_name(
          std::move(*function_descriptor.mutable_java_function_descriptor()
                         ->mutable_function_name()));
    }
    break;
  default:
    RAY_CHECK(false) << "Unsupported language: " << language;
  }
  task_info->mutable_required_resources()->swap(required_resources);
}

/// Convert a TaskDefinitionEvent to a TaskEvents.
///
/// \param event The TaskDefinitionEvent to convert.
/// \return The output TaskEvents to populate.
void ConvertToTaskEvents(rpc::events::TaskDefinitionEvent &&event,
                         rpc::AddTaskEventDataRequest &request) {
  auto *data = request.mutable_data();
  auto *task_event = data->add_events_by_task();
  task_event->set_task_id(event.task_id());
  task_event->set_attempt_number(event.task_attempt());
  task_event->set_job_id(event.job_id());

  rpc::TaskInfoEntry *task_info = task_event->mutable_task_info();
  task_info->set_type(event.task_type());
  task_info->set_name(event.task_name());
  task_info->set_task_id(event.task_id());
  task_info->set_job_id(event.job_id());
  task_info->set_parent_task_id(event.parent_task_id());
  if (!event.placement_group_id().empty()) {
    task_info->set_placement_group_id(event.placement_group_id());
  }

  PopulateTaskRuntimeAndFunctionInfo(std::move(*event.mutable_serialized_runtime_env()),
                                     std::move(*event.mutable_task_func()),
                                     std::move(*event.mutable_required_resources()),
                                     event.language(),
                                     task_info);
}

/// Convert a TaskExecutionEvent to a TaskEvents.
///
/// \param event The TaskExecutionEvent to convert.
/// \return The output TaskEvents to populate.
void ConvertToTaskEvents(const rpc::events::TaskExecutionEvent &event,
                         rpc::AddTaskEventDataRequest &request) {
  auto *data = request.mutable_data();
  auto *task_event = data->add_events_by_task();
  task_event->set_task_id(event.task_id());
  task_event->set_attempt_number(event.task_attempt());
  task_event->set_job_id(event.job_id());
  data->set_job_id(event.job_id());

  rpc::TaskStateUpdate task_state_update;
  task_state_update.set_node_id(event.node_id());
  task_state_update.set_worker_id(event.worker_id());
  task_state_update.set_worker_pid(event.worker_pid());
  task_state_update.mutable_error_info()->CopyFrom(event.ray_error_info());
  for (const auto &[state, timestamp] : event.task_state()) {
    int64_t ns = ProtoTimestampToAbslTimeNanos(timestamp);
    task_state_update.mutable_state_ts_ns()->insert({state, ns});
  }
  task_event->mutable_state_updates()->CopyFrom(task_state_update);
}

/// Convert an ActorTaskDefinitionEvent to a TaskEvents.
///
/// \param event The ActorTaskDefinitionEvent to convert.
/// \return The output TaskEvents to populate.
void ConvertToTaskEvents(rpc::events::ActorTaskDefinitionEvent &&event,
                         rpc::AddTaskEventDataRequest &request) {
  auto *data = request.mutable_data();
  auto *task_event = data->add_events_by_task();
  task_event->set_task_id(event.task_id());
  task_event->set_attempt_number(event.task_attempt());
  task_event->set_job_id(event.job_id());

  rpc::TaskInfoEntry *task_info = task_event->mutable_task_info();
  task_info->set_type(rpc::TaskType::ACTOR_TASK);
  task_info->set_name(event.actor_task_name());
  task_info->set_task_id(event.task_id());
  task_info->set_job_id(event.job_id());
  task_info->set_parent_task_id(event.parent_task_id());
  if (!event.placement_group_id().empty()) {
    task_info->set_placement_group_id(event.placement_group_id());
  }
  if (!event.actor_id().empty()) {
    task_info->set_actor_id(event.actor_id());
  }
  PopulateTaskRuntimeAndFunctionInfo(std::move(*event.mutable_serialized_runtime_env()),
                                     std::move(*event.mutable_actor_func()),
                                     std::move(*event.mutable_required_resources()),
                                     event.language(),
                                     task_info);
}

/// Convert ProfileEvents to a TaskEvents.
///
/// \param event TaskProfileEvents object to convert.
/// \return The output TaskEvents to populate.
void ConvertToTaskEvents(rpc::events::TaskProfileEvents &&event,
                         rpc::AddTaskEventDataRequest &request) {
  auto *data = request.mutable_data();
  auto *task_event = data->add_events_by_task();
  task_event->set_task_id(event.task_id());
  task_event->set_attempt_number(event.attempt_number());
  task_event->set_job_id(event.job_id());
  task_event->mutable_profile_events()->Swap(event.mutable_profile_events());
}

}  // namespace

void ConvertToTaskEventDataRequests(rpc::events::AddEventsRequest &events_request,
                                    rpc::AddTaskEventDataRequest &request) {
  // convert RayEvents to TaskEvents and add to single request
  for (auto &event : *events_request.mutable_events_data()->mutable_events()) {
    switch (event.event_type()) {
    case rpc::events::RayEvent::TASK_DEFINITION_EVENT: {
      ConvertToTaskEvents(std::move(*event.mutable_task_definition_event()), request);
      break;
    }
    case rpc::events::RayEvent::TASK_EXECUTION_EVENT: {
      ConvertToTaskEvents(*event.mutable_task_execution_event(), request);
      break;
    }
    case rpc::events::RayEvent::TASK_PROFILE_EVENT: {
      ConvertToTaskEvents(std::move(*event.mutable_task_profile_events()), request);
      break;
    }
    case rpc::events::RayEvent::ACTOR_TASK_DEFINITION_EVENT: {
      ConvertToTaskEvents(std::move(*event.mutable_actor_task_definition_event()),
                          request);
      break;
    }
    default:
      RAY_CHECK(false) << "Unsupported event type: " << event.event_type();
    }
  }

  // Handle task event metadata
  auto *metadata = events_request.mutable_events_data()->mutable_task_events_metadata();
  if (metadata->dropped_task_attempts_size() > 0) {
    auto *data = request.mutable_data();
    for (const auto &dropped_attempt : metadata->dropped_task_attempts()) {
      auto *dropped_attempt_proto = data->add_dropped_task_attempts();
      dropped_attempt_proto->set_task_id(dropped_attempt.task_id());
      dropped_attempt_proto->set_attempt_number(dropped_attempt.attempt_number());
    }
  }
}

}  // namespace gcs
}  // namespace ray
