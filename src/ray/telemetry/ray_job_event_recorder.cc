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

#include "ray/telemetry/ray_job_event_recorder.h"

namespace ray {
namespace telemetry {

RayJobEventRecorder::RayJobEventRecorder(
    std::unique_ptr<rpc::EventAggregatorClient> event_aggregator_client)
    : RayEventRecorderBase<rpc::JobTableData>(std::move(event_aggregator_client)) {}

void RayJobEventRecorder::ConvertToRayEvent(const rpc::JobTableData &data,
                                            rpc::events::RayEvent &ray_event) {
  rpc::events::DriverJobEvent driver_job_event;
  driver_job_event.set_job_id(data.job_id());
  driver_job_event.set_is_dead(data.is_dead());
  driver_job_event.set_driver_pid(data.driver_pid());
  driver_job_event.set_start_time(data.start_time());
  driver_job_event.set_end_time(data.end_time());
  driver_job_event.set_entrypoint(data.entrypoint());
  driver_job_event.set_driver_ip_address(data.driver_ip_address());
  driver_job_event.mutable_config()->mutable_metadata()->insert(
      data.config().metadata().begin(), data.config().metadata().end());

  auto export_runtime_env_info =
      driver_job_event.mutable_config()->mutable_runtime_env_info();
  export_runtime_env_info->set_serialized_runtime_env(
      data.config().runtime_env_info().serialized_runtime_env());
  auto export_runtime_env_uris = export_runtime_env_info->mutable_uris();
  export_runtime_env_uris->set_working_dir_uri(
      data.config().runtime_env_info().uris().working_dir_uri());
  export_runtime_env_uris->mutable_py_modules_uris()->CopyFrom(
      data.config().runtime_env_info().uris().py_modules_uris());
  auto export_runtime_env_config = export_runtime_env_info->mutable_runtime_env_config();
  export_runtime_env_config->set_setup_timeout_seconds(
      data.config().runtime_env_info().runtime_env_config().setup_timeout_seconds());
  export_runtime_env_config->set_eager_install(
      data.config().runtime_env_info().runtime_env_config().eager_install());
  export_runtime_env_config->mutable_log_files()->CopyFrom(
      data.config().runtime_env_info().runtime_env_config().log_files());

  *ray_event.mutable_driver_job_event() = std::move(driver_job_event);
  ray_event.set_source_type(rpc::events::RayEvent::GCS);
  ray_event.set_event_type(rpc::events::RayEvent::DRIVER_JOB_EVENT);
  ray_event.set_severity(rpc::events::RayEvent::INFO);
  SetCurrentTimestamp(ray_event.mutable_timestamp());
  ray_event.set_message("driver job event");
}

}  // namespace telemetry
}  // namespace ray
