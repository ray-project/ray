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

#include "ray/observability/ray_driver_job_definition_event.h"

namespace ray {
namespace observability {

RayDriverJobDefinitionEvent::RayDriverJobDefinitionEvent(const rpc::JobTableData &data,
                                                         const std::string &session_name)
    : RayEvent<rpc::events::DriverJobDefinitionEvent>(
          rpc::events::RayEvent::GCS,
          rpc::events::RayEvent::DRIVER_JOB_DEFINITION_EVENT,
          rpc::events::RayEvent::INFO,
          "",
          session_name) {
  data_.set_job_id(data.job_id());
  data_.set_driver_pid(data.driver_pid());
  data_.set_driver_node_id(data.driver_address().node_id());
  data_.set_entrypoint(data.entrypoint());
  data_.mutable_config()->mutable_metadata()->insert(data.config().metadata().begin(),
                                                     data.config().metadata().end());

  data_.mutable_config()->set_serialized_runtime_env(
      data.config().runtime_env_info().serialized_runtime_env());
  auto submission_id_iter = data.config().metadata().find("job_submission_id");
  if (submission_id_iter != data.config().metadata().end() &&
      !submission_id_iter->second.empty()) {
    data_.set_is_submission_job(true);
    data_.set_submission_id(submission_id_iter->second);
  } else {
    data_.set_is_submission_job(false);
  }

  // Populate fields from job_info if available
  if (data.has_job_info()) {
    const auto &job_info = data.job_info();

    if (job_info.has_driver_agent_http_address()) {
      data_.set_driver_agent_http_address(job_info.driver_agent_http_address());
    }

    if (job_info.has_entrypoint_num_cpus()) {
      data_.set_entrypoint_num_cpus(job_info.entrypoint_num_cpus());
    }

    if (job_info.has_entrypoint_num_gpus()) {
      data_.set_entrypoint_num_gpus(job_info.entrypoint_num_gpus());
    }

    if (job_info.has_entrypoint_memory()) {
      data_.set_entrypoint_memory(job_info.entrypoint_memory());
    }

    if (!job_info.entrypoint_resources().empty()) {
      data_.mutable_entrypoint_resources()->insert(
          job_info.entrypoint_resources().begin(), job_info.entrypoint_resources().end());
    }
  }
}

std::string RayDriverJobDefinitionEvent::GetEntityId() const { return data_.job_id(); }

void RayDriverJobDefinitionEvent::MergeData(
    RayEvent<rpc::events::DriverJobDefinitionEvent> &&other) {
  RAY_LOG(WARNING) << "Merge should not be called for driver job definition event.";
  return;
}

ray::rpc::events::RayEvent RayDriverJobDefinitionEvent::SerializeData() && {
  ray::rpc::events::RayEvent event;
  event.mutable_driver_job_definition_event()->Swap(&data_);
  return event;
}

}  // namespace observability
}  // namespace ray
