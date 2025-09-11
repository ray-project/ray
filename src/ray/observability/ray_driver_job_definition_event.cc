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

  auto runtime_env_info = data_.mutable_config()->mutable_runtime_env_info();
  runtime_env_info->set_serialized_runtime_env(
      data.config().runtime_env_info().serialized_runtime_env());
  auto runtime_env_uris = runtime_env_info->mutable_uris();
  runtime_env_uris->set_working_dir_uri(
      data.config().runtime_env_info().uris().working_dir_uri());
  runtime_env_uris->mutable_py_modules_uris()->CopyFrom(
      data.config().runtime_env_info().uris().py_modules_uris());
  auto runtime_env_config = runtime_env_info->mutable_runtime_env_config();
  runtime_env_config->set_setup_timeout_seconds(
      data.config().runtime_env_info().runtime_env_config().setup_timeout_seconds());
  runtime_env_config->set_eager_install(
      data.config().runtime_env_info().runtime_env_config().eager_install());
  runtime_env_config->mutable_log_files()->CopyFrom(
      data.config().runtime_env_info().runtime_env_config().log_files());
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
