// Copyright 2026 The Ray Authors.
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

#include "ray/observability/ray_worker_lifecycle_event.h"

namespace ray {
namespace observability {

RayWorkerLifecycleEvent::RayWorkerLifecycleEvent(const rpc::WorkerTableData &data,
                                                 const std::string &session_name)
    : RayEvent<rpc::events::WorkerLifecycleEvent>(
          rpc::events::RayEvent::GCS,
          rpc::events::RayEvent::WORKER_LIFECYCLE_EVENT,
          rpc::events::RayEvent::INFO,
          "",
          session_name) {
  rpc::events::WorkerLifecycleEvent::StateTransition state_transition;
  state_transition.mutable_timestamp()->CopyFrom(AbslTimeNanosToProtoTimestamp(
      absl::ToInt64Nanoseconds(absl::Now() - absl::UnixEpoch())));
  if (data.is_alive()) {
    state_transition.set_state(rpc::events::WorkerLifecycleEvent::ALIVE);
    state_transition.set_worker_launch_time_ms(data.worker_launch_time_ms());
    state_transition.set_worker_launched_time_ms(data.worker_launched_time_ms());
    state_transition.set_start_time_ms(data.start_time_ms());
  } else {
    state_transition.set_state(rpc::events::WorkerLifecycleEvent::DEAD);
    state_transition.set_end_time_ms(data.end_time_ms());
    auto *death_info = state_transition.mutable_death_info();
    death_info->set_exit_type(data.exit_type());
    death_info->set_exit_detail(data.exit_detail());
    // Only carried through for OOM kills; omitted otherwise
    if (data.has_memory_used_bytes_at_death()) {
      death_info->set_memory_used_bytes(data.memory_used_bytes_at_death());
    }
  }

  data_.set_worker_id(data.worker_address().worker_id());
  data_.set_job_id(data.job_id());
  data_.mutable_state_transitions()->Add(std::move(state_transition));
}

std::string RayWorkerLifecycleEvent::GetEntityId() const { return data_.worker_id(); }

void RayWorkerLifecycleEvent::MergeData(
    RayEvent<rpc::events::WorkerLifecycleEvent> &&other) {
  auto &&other_event = static_cast<RayWorkerLifecycleEvent &&>(other);
  if (data_.job_id().empty() && !other_event.data_.job_id().empty()) {
    data_.set_job_id(other_event.data_.job_id());
  }
  for (auto &state_transition : *other_event.data_.mutable_state_transitions()) {
    data_.mutable_state_transitions()->Add(std::move(state_transition));
  }
}

ray::rpc::events::RayEvent RayWorkerLifecycleEvent::SerializeData() && {
  ray::rpc::events::RayEvent event;
  event.mutable_worker_lifecycle_event()->Swap(&data_);
  return event;
}

}  // namespace observability
}  // namespace ray
