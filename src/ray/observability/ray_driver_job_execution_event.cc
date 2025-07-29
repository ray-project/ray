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

#include "ray/observability/ray_driver_job_execution_event.h"

namespace ray {
namespace observability {

RayDriverJobExecutionEvent::RayDriverJobExecutionEvent(
    rpc::JobTableData &data, rpc::events::DriverJobExecutionEvent::State state) {
  event_type_ = rpc::events::RayEvent::DRIVER_JOB_EXECUTION_EVENT;
  ray::rpc::events::DriverJobExecutionEvent::StateTimestamp state_timestamp;
  state_timestamp.set_state(state);
  state_timestamp.mutable_timestamp()->CopyFrom(AbslTimeNanosToProtoTimestamp(
      absl::ToInt64Nanoseconds(absl::Now() - absl::UnixEpoch())));

  data_.mutable_states()->Add(std::move(state_timestamp));
  data_.set_job_id(data.job_id());
}

std::string RayDriverJobExecutionEvent::GetResourceId() const { return data_.job_id(); }

void RayDriverJobExecutionEvent::Merge(
    RayEvent<rpc::events::DriverJobExecutionEvent> &other) {
  auto &other_event =
      static_cast<RayEvent<rpc::events::DriverJobExecutionEvent> &>(other);
  for (auto &state : *other_event.GetData().mutable_states()) {
    data_.mutable_states()->Add(std::move(state));
  }
}

void RayDriverJobExecutionEvent::Serialize(rpc::events::RayEvent *event) const {
  event->set_message("driver job execution event");
  event->set_source_type(rpc::events::RayEvent::GCS);
  event->set_event_type(event_type_);
  event->set_severity(rpc::events::RayEvent::INFO);
  event->mutable_timestamp()->CopyFrom(AbslTimeNanosToProtoTimestamp(
      absl::ToInt64Nanoseconds(absl::Now() - absl::UnixEpoch())));
  *event->mutable_driver_job_execution_event() = std::move(data_);
}

}  // namespace observability
}  // namespace ray
