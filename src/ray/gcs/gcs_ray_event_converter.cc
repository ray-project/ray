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
#include "ray/util/logging.h"

namespace ray {
namespace gcs {

std::vector<rpc::AddTaskEventDataRequest>
GcsRayEventConverter::ConvertToTaskEventDataRequests(
    rpc::events::AddEventsRequest &&request) {
  std::vector<rpc::AddTaskEventDataRequest> requests_per_job_id;
  absl::flat_hash_map<std::string, size_t> job_id_to_index;
  // convert RayEvents to TaskEvents and group by job id.
  for (auto &event : *request.mutable_events_data()->mutable_events()) {
    std::optional<rpc::TaskEvents> task_event = std::nullopt;
    switch (event.event_type()) {
    case rpc::events::RayEvent::TASK_DEFINITION_EVENT: {
      task_event = ConvertToTaskEvents(std::move(*event.mutable_task_definition_event()));
      break;
    }
    case rpc::events::RayEvent::TASK_EXECUTION_EVENT: {
      task_event = ConvertToTaskEvents(std::move(*event.mutable_task_execution_event()));
      break;
    }
    case rpc::events::RayEvent::TASK_PROFILE_EVENT: {
      task_event = ConvertToTaskEvents(std::move(*event.mutable_task_profile_events()));
      break;
    }
    case rpc::events::RayEvent::ACTOR_TASK_DEFINITION_EVENT: {
      task_event =
          ConvertToTaskEvents(std::move(*event.mutable_actor_task_definition_event()));
      break;
    }
    default:
      // TODO(can-anyscale): Handle other event types
      break;
    }

    // Groups all taskEvents belonging to same jobId into one AddTaskEventDataRequest
    if (task_event) {
      AddTaskEventToRequest(std::move(*task_event), requests_per_job_id, job_id_to_index);
    }
  }

  //  Groups all taskEventMetadata belonging to same jobId into one
  //  AddTaskEventDataRequest
  auto *metadata = request.mutable_events_data()->mutable_task_events_metadata();
  if (metadata->dropped_task_attempts_size() > 0) {
    AddDroppedTaskAttemptsToRequest(
        std::move(*metadata), requests_per_job_id, job_id_to_index);
  }
  return requests_per_job_id;
}

void GcsRayEventConverter::AddTaskEventToRequest(
    rpc::TaskEvents &&task_event,
    std::vector<rpc::AddTaskEventDataRequest> &requests_per_job_id,
    absl::flat_hash_map<std::string, size_t> &job_id_to_index) {
  const std::string job_id_key = task_event.job_id();
  auto it = job_id_to_index.find(job_id_key);
  if (it == job_id_to_index.end()) {
    // Create new AddTaskEventDataRequest entry and add index to map
    size_t idx = requests_per_job_id.size();
    requests_per_job_id.emplace_back();
    auto *data = requests_per_job_id.back().mutable_data();
    data->set_job_id(job_id_key);
    *data->add_events_by_task() = std::move(task_event);
    job_id_to_index.emplace(job_id_key, idx);
  } else {
    // add taskEvent to existing AddTaskEventDataRequest with same job id
    auto *data = requests_per_job_id[it->second].mutable_data();
    *data->add_events_by_task() = std::move(task_event);
  }
}

void GcsRayEventConverter::AddDroppedTaskAttemptsToRequest(
    rpc::events::TaskEventsMetadata &&metadata,
    std::vector<rpc::AddTaskEventDataRequest> &requests_per_job_id,
    absl::flat_hash_map<std::string, size_t> &job_id_to_index) {
  // Process each dropped task attempt individually and route to the correct job ID
  for (auto &dropped_attempt : *metadata.mutable_dropped_task_attempts()) {
    const auto task_id = TaskID::FromBinary(dropped_attempt.task_id());
    const auto job_id_key = task_id.JobId().Binary();

    auto it = job_id_to_index.find(job_id_key);
    if (it == job_id_to_index.end()) {
      // Create new request if job_id not found
      size_t idx = requests_per_job_id.size();
      requests_per_job_id.emplace_back();
      auto *data = requests_per_job_id.back().mutable_data();
      data->set_job_id(job_id_key);
      *data->add_dropped_task_attempts() = std::move(dropped_attempt);
      job_id_to_index.emplace(job_id_key, idx);
    } else {
      // Add to existing request with same job_id
      auto *data = requests_per_job_id[it->second].mutable_data();
      *data->add_dropped_task_attempts() = std::move(dropped_attempt);
    }
  }
}

rpc::TaskEvents GcsRayEventConverter::ConvertToTaskEvents(
    rpc::events::TaskDefinitionEvent &&event) {
  rpc::TaskEvents task_event;
  task_event.set_task_id(event.task_id());
  task_event.set_attempt_number(event.task_attempt());
  task_event.set_job_id(event.job_id());

  rpc::TaskInfoEntry *task_info = task_event.mutable_task_info();
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
  return task_event;
}

rpc::TaskEvents GcsRayEventConverter::ConvertToTaskEvents(
    rpc::events::TaskExecutionEvent &&event) {
  rpc::TaskEvents task_event;
  task_event.set_task_id(event.task_id());
  task_event.set_attempt_number(event.task_attempt());
  task_event.set_job_id(event.job_id());

  rpc::TaskStateUpdate *task_state_update = task_event.mutable_state_updates();
  task_state_update->set_node_id(event.node_id());
  task_state_update->set_worker_id(event.worker_id());
  task_state_update->set_worker_pid(event.worker_pid());
  task_state_update->mutable_error_info()->Swap(event.mutable_ray_error_info());

  for (const auto &[state, timestamp] : event.task_state()) {
    int64_t ns = ProtoTimestampToAbslTimeNanos(timestamp);
    (*task_state_update->mutable_state_ts_ns())[state] = ns;
  }
  return task_event;
}

rpc::TaskEvents GcsRayEventConverter::ConvertToTaskEvents(
    rpc::events::ActorTaskDefinitionEvent &&event) {
  rpc::TaskEvents task_event;
  task_event.set_task_id(event.task_id());
  task_event.set_attempt_number(event.task_attempt());
  task_event.set_job_id(event.job_id());

  rpc::TaskInfoEntry *task_info = task_event.mutable_task_info();
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
  return task_event;
}

rpc::TaskEvents GcsRayEventConverter::ConvertToTaskEvents(
    rpc::events::TaskProfileEvents &&event) {
  rpc::TaskEvents task_event;
  task_event.set_task_id(event.task_id());
  task_event.set_attempt_number(event.attempt_number());
  task_event.set_job_id(event.job_id());

  task_event.mutable_profile_events()->Swap(event.mutable_profile_events());
  return task_event;
}

void GcsRayEventConverter::PopulateTaskRuntimeAndFunctionInfo(
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
          function_descriptor.cpp_function_descriptor().function_name());
    }
    break;
  case rpc::Language::PYTHON:
    if (function_descriptor.has_python_function_descriptor()) {
      task_info->set_func_or_class_name(
          function_descriptor.python_function_descriptor().function_name());
    }
    break;
  case rpc::Language::JAVA:
    if (function_descriptor.has_java_function_descriptor()) {
      task_info->set_func_or_class_name(
          function_descriptor.java_function_descriptor().function_name());
    }
    break;
  default:
    // Other languages are not handled.
    break;
  }
  task_info->mutable_required_resources()->swap(required_resources);
}

}  // namespace gcs
}  // namespace ray
