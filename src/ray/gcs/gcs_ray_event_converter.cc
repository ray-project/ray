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

/// Deep copy ActorDeathCause to ensure no arena pointers leak through.
void DeepCopyActorDeathCause(const rpc::ActorDeathCause &src, rpc::ActorDeathCause *dst) {
  if (src.has_creation_task_failure_context()) {
    const auto &src_exc = src.creation_task_failure_context();
    auto *dst_exc = dst->mutable_creation_task_failure_context();
    dst_exc->set_language(src_exc.language());
    dst_exc->set_serialized_exception(src_exc.serialized_exception());
    dst_exc->set_formatted_exception_string(src_exc.formatted_exception_string());
  } else if (src.has_runtime_env_failed_context()) {
    const auto &src_ctx = src.runtime_env_failed_context();
    auto *dst_ctx = dst->mutable_runtime_env_failed_context();
    dst_ctx->set_error_message(src_ctx.error_message());
  } else if (src.has_actor_died_error_context()) {
    const auto &src_ctx = src.actor_died_error_context();
    auto *dst_ctx = dst->mutable_actor_died_error_context();
    dst_ctx->set_error_message(src_ctx.error_message());
    dst_ctx->set_owner_id(src_ctx.owner_id());
    dst_ctx->set_owner_ip_address(src_ctx.owner_ip_address());
    dst_ctx->set_node_ip_address(src_ctx.node_ip_address());
    dst_ctx->set_pid(src_ctx.pid());
    dst_ctx->set_name(src_ctx.name());
    dst_ctx->set_ray_namespace(src_ctx.ray_namespace());
    dst_ctx->set_class_name(src_ctx.class_name());
    dst_ctx->set_actor_id(src_ctx.actor_id());
    dst_ctx->set_never_started(src_ctx.never_started());
    dst_ctx->set_reason(src_ctx.reason());
    if (src_ctx.has_node_death_info()) {
      auto *dst_node_death = dst_ctx->mutable_node_death_info();
      dst_node_death->set_reason(src_ctx.node_death_info().reason());
      dst_node_death->set_reason_message(src_ctx.node_death_info().reason_message());
    }
  } else if (src.has_actor_unschedulable_context()) {
    const auto &src_ctx = src.actor_unschedulable_context();
    auto *dst_ctx = dst->mutable_actor_unschedulable_context();
    dst_ctx->set_error_message(src_ctx.error_message());
  } else if (src.has_oom_context()) {
    const auto &src_ctx = src.oom_context();
    auto *dst_ctx = dst->mutable_oom_context();
    dst_ctx->set_error_message(src_ctx.error_message());
    dst_ctx->set_fail_immediately(src_ctx.fail_immediately());
  }
}

/// Deep copy TaskLogInfo to ensure no arena pointers leak through.
void DeepCopyTaskLogInfo(const rpc::TaskLogInfo &src, rpc::TaskLogInfo *dst) {
  if (src.has_stdout_file()) {
    dst->set_stdout_file(src.stdout_file());
  }
  if (src.has_stderr_file()) {
    dst->set_stderr_file(src.stderr_file());
  }
  if (src.has_stdout_start()) {
    dst->set_stdout_start(src.stdout_start());
  }
  if (src.has_stdout_end()) {
    dst->set_stdout_end(src.stdout_end());
  }
  if (src.has_stderr_start()) {
    dst->set_stderr_start(src.stderr_start());
  }
  if (src.has_stderr_end()) {
    dst->set_stderr_end(src.stderr_end());
  }
}

/// Deep copy RayErrorInfo to ensure no arena pointers leak through.
/// This manually copies all fields to force heap allocation.
void DeepCopyRayErrorInfo(const rpc::RayErrorInfo &src, rpc::RayErrorInfo *dst) {
  // Copy scalar fields
  dst->set_error_type(src.error_type());
  dst->set_error_message(src.error_message());

  // Deep copy the oneof error context fields
  if (src.has_actor_died_error()) {
    DeepCopyActorDeathCause(src.actor_died_error(), dst->mutable_actor_died_error());
  } else if (src.has_runtime_env_setup_failed_error()) {
    const auto &src_ctx = src.runtime_env_setup_failed_error();
    auto *dst_ctx = dst->mutable_runtime_env_setup_failed_error();
    dst_ctx->set_error_message(src_ctx.error_message());
  } else if (src.has_actor_unavailable_error()) {
    const auto &src_ctx = src.actor_unavailable_error();
    auto *dst_ctx = dst->mutable_actor_unavailable_error();
    if (src_ctx.has_actor_id()) {
      dst_ctx->set_actor_id(src_ctx.actor_id());
    }
  }
}

/// Deep copy ProfileEvents to ensure no arena pointers leak through.
void DeepCopyProfileEvents(const rpc::ProfileEvents &src, rpc::ProfileEvents *dst) {
  dst->set_component_type(src.component_type());
  dst->set_component_id(src.component_id());
  dst->set_node_ip_address(src.node_ip_address());

  // Deep copy each ProfileEventEntry
  for (const auto &src_entry : src.events()) {
    auto *dst_entry = dst->add_events();
    dst_entry->set_start_time(src_entry.start_time());
    dst_entry->set_end_time(src_entry.end_time());
    if (src_entry.has_extra_data()) {
      dst_entry->set_extra_data(src_entry.extra_data());
    }
    dst_entry->set_event_name(src_entry.event_name());
  }
}

/// Deep copy TaskEvents to ensure no arena pointers leak through.
/// This ensures the entire TaskEvents object is heap-allocated.
void DeepCopyTaskEvents(const rpc::TaskEvents &src, rpc::TaskEvents *dst) {
  // Copy scalar fields
  dst->set_task_id(src.task_id());
  dst->set_job_id(src.job_id());
  dst->set_attempt_number(src.attempt_number());

  // Deep copy task_info if present
  if (src.has_task_info()) {
    const auto &src_info = src.task_info();
    auto *dst_info = dst->mutable_task_info();

    dst_info->set_type(src_info.type());
    dst_info->set_name(src_info.name());
    dst_info->set_language(src_info.language());
    dst_info->set_func_or_class_name(src_info.func_or_class_name());
    dst_info->set_scheduling_state(src_info.scheduling_state());
    dst_info->set_job_id(src_info.job_id());
    dst_info->set_task_id(src_info.task_id());
    dst_info->set_parent_task_id(src_info.parent_task_id());

    // Copy optional fields
    if (src_info.has_node_id()) {
      dst_info->set_node_id(src_info.node_id());
    }
    if (src_info.has_actor_id()) {
      dst_info->set_actor_id(src_info.actor_id());
    }
    if (src_info.has_placement_group_id()) {
      dst_info->set_placement_group_id(src_info.placement_group_id());
    }
    if (src_info.has_call_site()) {
      dst_info->set_call_site(src_info.call_site());
    }

    // Deep copy required_resources map
    for (const auto &[key, value] : src_info.required_resources()) {
      (*dst_info->mutable_required_resources())[key] = value;
    }

    // Deep copy label_selector map
    for (const auto &[key, value] : src_info.label_selector()) {
      (*dst_info->mutable_label_selector())[key] = value;
    }

    // Deep copy runtime_env_info
    if (src_info.has_runtime_env_info()) {
      dst_info->mutable_runtime_env_info()->set_serialized_runtime_env(
          src_info.runtime_env_info().serialized_runtime_env());
    }
  }

  // Deep copy state_updates if present
  if (src.has_state_updates()) {
    const auto &src_state = src.state_updates();
    auto *dst_state = dst->mutable_state_updates();

    // Copy optional bytes fields
    if (src_state.has_node_id()) {
      dst_state->set_node_id(src_state.node_id());
    }
    if (src_state.has_worker_id()) {
      dst_state->set_worker_id(src_state.worker_id());
    }
    if (src_state.has_worker_pid()) {
      dst_state->set_worker_pid(src_state.worker_pid());
    }

    // Copy optional string field
    if (src_state.has_actor_repr_name()) {
      dst_state->set_actor_repr_name(src_state.actor_repr_name());
    }

    // Copy optional bool field
    if (src_state.has_is_debugger_paused()) {
      dst_state->set_is_debugger_paused(src_state.is_debugger_paused());
    }

    // Deep copy state_ts_ns map
    for (const auto &[state, ts] : src_state.state_ts_ns()) {
      (*dst_state->mutable_state_ts_ns())[state] = ts;
    }

    // Deep copy task_log_info if present
    if (src_state.has_task_log_info()) {
      DeepCopyTaskLogInfo(src_state.task_log_info(), dst_state->mutable_task_log_info());
    }

    // Deep copy error_info if present
    if (src_state.has_error_info()) {
      DeepCopyRayErrorInfo(src_state.error_info(), dst_state->mutable_error_info());
    }
  }

  // Deep copy profile_events if present
  if (src.has_profile_events()) {
    DeepCopyProfileEvents(src.profile_events(), dst->mutable_profile_events());
  }
}

/// Add dropped task attempts to the appropriate job-grouped request.
///
/// \param metadata The task events metadata containing dropped task attempts.
/// \param requests_per_job_id The list of requests grouped by job id.
/// \param job_id_to_index The map from job id to index in requests_per_job_id.
void AddDroppedTaskAttemptsToRequest(
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
    const std::string &serialized_runtime_env,
    const rpc::FunctionDescriptor &function_descriptor,
    const ::google::protobuf::Map<std::string, double> &required_resources,
    rpc::Language language,
    rpc::TaskInfoEntry *task_info) {
  task_info->set_language(language);
  // Use set instead of move to copy from const reference
  task_info->mutable_runtime_env_info()->set_serialized_runtime_env(
      serialized_runtime_env);
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
    RAY_CHECK(false) << "Unsupported language: " << language;
  }
  // Deep copy the required resources map to avoid arena pointers
  // Manually iterate and insert each key-value pair
  auto *dst_resources = task_info->mutable_required_resources();
  for (const auto &[key, value] : required_resources) {
    (*dst_resources)[key] = value;
  }
}

/// Convert a TaskDefinitionEvent to a TaskEvents.
///
/// \param event The TaskDefinitionEvent to convert.
/// \return The output TaskEvents to populate.
rpc::TaskEvents ConvertToTaskEvents(rpc::events::TaskDefinitionEvent &&event) {
  rpc::TaskEvents task_event;

  // Force deep copy by copying arena strings to std::string first
  std::string task_id(event.task_id());
  std::string job_id(event.job_id());
  std::string parent_task_id(event.parent_task_id());

  task_event.set_task_id(task_id);
  task_event.set_attempt_number(event.task_attempt());
  task_event.set_job_id(job_id);

  rpc::TaskInfoEntry *task_info = task_event.mutable_task_info();
  task_info->set_type(event.task_type());

  // Force deep copy of strings
  std::string task_name(event.task_name());
  task_info->set_name(task_name);
  task_info->set_task_id(task_id);
  task_info->set_job_id(job_id);
  task_info->set_parent_task_id(parent_task_id);

  if (!event.placement_group_id().empty()) {
    std::string placement_group_id(event.placement_group_id());
    task_info->set_placement_group_id(placement_group_id);
  }

  // Force deep copy of strings in PopulateTaskRuntimeAndFunctionInfo
  std::string serialized_runtime_env(event.serialized_runtime_env());
  PopulateTaskRuntimeAndFunctionInfo(serialized_runtime_env,
                                     event.task_func(),
                                     event.required_resources(),
                                     event.language(),
                                     task_info);
  return task_event;
}

/// Convert a TaskExecutionEvent to a TaskEvents.
///
/// \param event The TaskExecutionEvent to convert.
/// \return The output TaskEvents to populate.
rpc::TaskEvents ConvertToTaskEvents(rpc::events::TaskExecutionEvent &&event) {
  rpc::TaskEvents task_event;

  // Simple approach: Set scalar fields directly
  task_event.set_task_id(std::string(event.task_id()));
  task_event.set_attempt_number(event.task_attempt());
  task_event.set_job_id(std::string(event.job_id()));

  rpc::TaskStateUpdate *task_state_update = task_event.mutable_state_updates();
  task_state_update->set_node_id(std::string(event.node_id()));
  task_state_update->set_worker_id(std::string(event.worker_id()));
  task_state_update->set_worker_pid(event.worker_pid());

  // Use CopyFrom() for complex nested message - protobuf SHOULD handle arena correctly
  if (event.has_ray_error_info()) {
    task_state_update->mutable_error_info()->CopyFrom(event.ray_error_info());
  }

  // For the map, copy to intermediate storage first
  std::vector<std::pair<int32_t, int64_t>> state_timestamps;
  state_timestamps.reserve(event.task_state().size());
  for (const auto &[state, timestamp] : event.task_state()) {
    state_timestamps.emplace_back(state, ProtoTimestampToAbslTimeNanos(timestamp));
  }

  auto *state_ts_map = task_state_update->mutable_state_ts_ns();
  for (const auto &[state, ns] : state_timestamps) {
    (*state_ts_map)[state] = ns;
  }

  return task_event;
}

/// Convert an ActorTaskDefinitionEvent to a TaskEvents.
///
/// \param event The ActorTaskDefinitionEvent to convert.
/// \return The output TaskEvents to populate.
rpc::TaskEvents ConvertToTaskEvents(rpc::events::ActorTaskDefinitionEvent &&event) {
  rpc::TaskEvents task_event;

  // Force deep copy by copying arena strings to std::string first
  std::string task_id(event.task_id());
  std::string job_id(event.job_id());
  std::string parent_task_id(event.parent_task_id());

  task_event.set_task_id(task_id);
  task_event.set_attempt_number(event.task_attempt());
  task_event.set_job_id(job_id);

  rpc::TaskInfoEntry *task_info = task_event.mutable_task_info();
  task_info->set_type(rpc::TaskType::ACTOR_TASK);

  // Force deep copy of strings
  std::string actor_task_name(event.actor_task_name());
  task_info->set_name(actor_task_name);
  task_info->set_task_id(task_id);
  task_info->set_job_id(job_id);
  task_info->set_parent_task_id(parent_task_id);

  if (!event.placement_group_id().empty()) {
    std::string placement_group_id(event.placement_group_id());
    task_info->set_placement_group_id(placement_group_id);
  }
  if (!event.actor_id().empty()) {
    std::string actor_id(event.actor_id());
    task_info->set_actor_id(actor_id);
  }

  // Force deep copy of strings in PopulateTaskRuntimeAndFunctionInfo
  std::string serialized_runtime_env(event.serialized_runtime_env());
  PopulateTaskRuntimeAndFunctionInfo(serialized_runtime_env,
                                     event.actor_func(),
                                     event.required_resources(),
                                     event.language(),
                                     task_info);
  return task_event;
}

/// Convert ProfileEvents to a TaskEvents.
///
/// \param event TaskProfileEvents object to convert.
/// \return The output TaskEvents to populate.
rpc::TaskEvents ConvertToTaskEvents(rpc::events::TaskProfileEvents &&event) {
  rpc::TaskEvents task_event;

  // Force deep copy by copying arena strings to std::string first
  std::string task_id(event.task_id());
  std::string job_id(event.job_id());

  task_event.set_task_id(task_id);
  task_event.set_attempt_number(event.attempt_number());
  task_event.set_job_id(job_id);

  // Deep copy ProfileEvents to avoid arena pointers
  DeepCopyProfileEvents(event.profile_events(), task_event.mutable_profile_events());

  return task_event;
}

}  // namespace

std::vector<rpc::AddTaskEventDataRequest> ConvertToTaskEventDataRequests(
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
      RAY_CHECK(false) << "Unsupported event type: " << event.event_type();
    }

    // Groups all taskEvents belonging to same jobId into one AddTaskEventDataRequest
    if (task_event) {
      const std::string job_id_key = task_event->job_id();
      auto it = job_id_to_index.find(job_id_key);
      if (it == job_id_to_index.end()) {
        // Create new AddTaskEventDataRequest entry and add index to map
        size_t idx = requests_per_job_id.size();
        requests_per_job_id.emplace_back();
        auto *data = requests_per_job_id.back().mutable_data();
        data->set_job_id(job_id_key);
        // Deep copy to ensure no arena pointers leak into the outgoing request
        DeepCopyTaskEvents(*task_event, data->add_events_by_task());
        job_id_to_index.emplace(job_id_key, idx);
      } else {
        // add taskEvent to existing AddTaskEventDataRequest with same job id
        auto *data = requests_per_job_id[it->second].mutable_data();
        // Deep copy to ensure no arena pointers leak into the outgoing request
        DeepCopyTaskEvents(*task_event, data->add_events_by_task());
      }
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

}  // namespace gcs
}  // namespace ray
