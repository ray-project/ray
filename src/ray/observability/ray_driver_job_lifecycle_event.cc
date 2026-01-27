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

#include "ray/observability/ray_driver_job_lifecycle_event.h"

namespace ray {
namespace observability {

rpc::events::DriverJobLifecycleEvent::SubmissionJobStatus StringToSubmissionJobStatus(
    const std::string &status) {
  if (status == "PENDING") {
    return rpc::events::DriverJobLifecycleEvent::PENDING;
  }
  if (status == "RUNNING") {
    return rpc::events::DriverJobLifecycleEvent::RUNNING;
  }
  if (status == "STOPPED") {
    return rpc::events::DriverJobLifecycleEvent::STOPPED;
  }
  if (status == "SUCCEEDED") {
    return rpc::events::DriverJobLifecycleEvent::SUCCEEDED;
  }
  if (status == "FAILED") {
    return rpc::events::DriverJobLifecycleEvent::FAILED;
  }
  return rpc::events::DriverJobLifecycleEvent::SUBMISSION_JOB_STATUS_UNSPECIFIED;
}

RayDriverJobLifecycleEvent::RayDriverJobLifecycleEvent(
    const rpc::JobTableData &data,
    rpc::events::DriverJobLifecycleEvent::State state,
    const std::string &session_name)
    : RayEvent<rpc::events::DriverJobLifecycleEvent>(
          rpc::events::RayEvent::GCS,
          rpc::events::RayEvent::DRIVER_JOB_LIFECYCLE_EVENT,
          rpc::events::RayEvent::INFO,
          "",
          session_name) {
  ray::rpc::events::DriverJobLifecycleEvent::StateTransition state_transition;
  state_transition.set_state(state);
  state_transition.mutable_timestamp()->CopyFrom(AbslTimeNanosToProtoTimestamp(
      absl::ToInt64Nanoseconds(absl::Now() - absl::UnixEpoch())));

  // Populate additional state transition fields from job_info
  if (data.has_job_info()) {
    const auto &job_info = data.job_info();

    if (job_info.has_message()) {
      state_transition.set_message(job_info.message());
    }

    if (job_info.has_error_type()) {
      state_transition.set_error_type(job_info.error_type());
    }

    if (job_info.has_driver_exit_code()) {
      state_transition.set_exit_code(job_info.driver_exit_code());
    }

    // Set submission job status from job_info.status() (for submission jobs)
    state_transition.set_submission_job_status(
        StringToSubmissionJobStatus(job_info.status()));
  }

  data_.mutable_state_transitions()->Add(std::move(state_transition));
  data_.set_job_id(data.job_id());
}

std::string RayDriverJobLifecycleEvent::GetEntityId() const { return data_.job_id(); }

void RayDriverJobLifecycleEvent::MergeData(
    RayEvent<rpc::events::DriverJobLifecycleEvent> &&other) {
  auto &&other_event = static_cast<RayDriverJobLifecycleEvent &&>(other);
  for (auto &state_transition : *other_event.data_.mutable_state_transitions()) {
    data_.mutable_state_transitions()->Add(std::move(state_transition));
  }
}

ray::rpc::events::RayEvent RayDriverJobLifecycleEvent::SerializeData() && {
  ray::rpc::events::RayEvent event;
  event.mutable_driver_job_lifecycle_event()->Swap(&data_);
  return event;
}

}  // namespace observability
}  // namespace ray
