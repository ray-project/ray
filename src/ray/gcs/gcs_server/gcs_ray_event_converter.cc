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

#include <unordered_map>

#include "ray/util/logging.h"

namespace ray {
namespace gcs {

void GcsRayEventConverter::ConvertToTaskEventDataRequests(
    rpc::events::AddEventsRequest &&request,
    std::vector<rpc::AddTaskEventDataRequest> &grouped_requests) {
  std::unordered_map<std::string, size_t> job_id_to_index;

  // convert RayEvents to TaskEvents and group by job id.
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

    const std::string job_id_key = task_event.job_id();
    auto it = job_id_to_index.find(job_id_key);
    if (it == job_id_to_index.end()) {
      size_t idx = grouped_requests.size();
      grouped_requests.emplace_back();
      auto *data = grouped_requests.back().mutable_data();
      data->set_job_id(job_id_key);
      data->add_events_by_task()->Swap(&task_event);
      job_id_to_index.emplace(job_id_key, idx);
    } else {
      auto *data = grouped_requests[it->second].mutable_data();
      data->add_events_by_task()->Swap(&task_event);
    }
  }

  // Move dropped task attempts from the metadata into the first request only to avoid
  // double counting. These are aggregated by job id (derived from task id) in the
  // receiver so they can all be reported in the same request.
  auto *metadata = request.mutable_events_data()->mutable_task_events_metadata();
  if (metadata->dropped_task_attempts_size() > 0) {
    if (grouped_requests.empty()) {
      grouped_requests.emplace_back();
    }
    auto *first_task_event_data = grouped_requests.front().mutable_data();
    first_task_event_data->mutable_dropped_task_attempts()->Swap(
        metadata->mutable_dropped_task_attempts());
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
  task_info->mutable_runtime_env_info()->Swap(event.mutable_runtime_env_info());
  task_info->set_parent_task_id(event.parent_task_id());
  if (!event.placement_group_id().empty()) {
    task_info->set_placement_group_id(event.placement_group_id());
  }

  auto function_descriptor = event.task_func();
  if (event.language() == rpc::Language::CPP &&
      function_descriptor.has_cpp_function_descriptor()) {
    task_info->set_func_or_class_name(
        function_descriptor.cpp_function_descriptor().function_name());
  } else if (event.language() == rpc::Language::PYTHON &&
             function_descriptor.has_python_function_descriptor()) {
    task_info->set_func_or_class_name(
        function_descriptor.python_function_descriptor().function_name());
  } else if (event.language() == rpc::Language::JAVA &&
             function_descriptor.has_java_function_descriptor()) {
    task_info->set_func_or_class_name(
        function_descriptor.java_function_descriptor().function_name());
  }
  task_info->mutable_required_resources()->swap(*event.mutable_required_resources());
}

}  // namespace gcs
}  // namespace ray
