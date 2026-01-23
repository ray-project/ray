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

#include "ray/observability/ray_placement_group_lifecycle_event.h"

namespace ray {
namespace observability {

RayPlacementGroupLifecycleEvent::RayPlacementGroupLifecycleEvent(
    const rpc::PlacementGroupTableData &data,
    rpc::events::PlacementGroupLifecycleEvent::State state,
    const std::string &session_name)
    : RayEvent<rpc::events::PlacementGroupLifecycleEvent>(
          rpc::events::RayEvent::GCS,
          rpc::events::RayEvent::PLACEMENT_GROUP_LIFECYCLE_EVENT,
          rpc::events::RayEvent::INFO,
          "",
          session_name) {
  data_.set_placement_group_id(data.placement_group_id());

  rpc::events::PlacementGroupLifecycleEvent::StateTransition state_transition;
  state_transition.set_state(state);
  state_transition.mutable_timestamp()->CopyFrom(AbslTimeNanosToProtoTimestamp(
      absl::ToInt64Nanoseconds(absl::Now() - absl::UnixEpoch())));

  if (data.has_stats()) {
    const auto &stats = data.stats();
    state_transition.set_scheduling_state(
        ConvertSchedulingState(stats.scheduling_state()));
    state_transition.set_scheduling_attempt(stats.scheduling_attempt());
    state_transition.set_highest_retry_delay_ms(stats.highest_retry_delay_ms());
    state_transition.set_scheduling_started_time_ns(stats.scheduling_started_time_ns());

    // Set latency fields only when PG is successfully created
    if (state == rpc::events::PlacementGroupLifecycleEvent::CREATED) {
      state_transition.set_scheduling_latency_us(stats.scheduling_latency_us());
      state_transition.set_end_to_end_creation_latency_us(
          stats.end_to_end_creation_latency_us());
    }
  }

  data_.mutable_state_transitions()->Add(std::move(state_transition));
}

std::string RayPlacementGroupLifecycleEvent::GetEntityId() const {
  return data_.placement_group_id();
}

void RayPlacementGroupLifecycleEvent::MergeData(
    RayEvent<rpc::events::PlacementGroupLifecycleEvent> &&other) {
  auto &&other_event = static_cast<RayPlacementGroupLifecycleEvent &&>(other);
  for (auto &state : *other_event.data_.mutable_state_transitions()) {
    data_.mutable_state_transitions()->Add(std::move(state));
  }
}

ray::rpc::events::RayEvent RayPlacementGroupLifecycleEvent::SerializeData() && {
  ray::rpc::events::RayEvent event;
  event.mutable_placement_group_lifecycle_event()->Swap(&data_);
  return event;
}

rpc::events::PlacementGroupLifecycleEvent::State
RayPlacementGroupLifecycleEvent::ConvertState(
    rpc::PlacementGroupTableData::PlacementGroupState state) {
  switch (state) {
  case rpc::PlacementGroupTableData::PENDING:
    return rpc::events::PlacementGroupLifecycleEvent::PENDING;
  case rpc::PlacementGroupTableData::PREPARED:
    return rpc::events::PlacementGroupLifecycleEvent::PREPARED;
  case rpc::PlacementGroupTableData::CREATED:
    return rpc::events::PlacementGroupLifecycleEvent::CREATED;
  case rpc::PlacementGroupTableData::REMOVED:
    return rpc::events::PlacementGroupLifecycleEvent::REMOVED;
  case rpc::PlacementGroupTableData::RESCHEDULING:
    return rpc::events::PlacementGroupLifecycleEvent::RESCHEDULING;
  default:
    RAY_LOG(WARNING) << "Unknown placement group state: " << state;
    return rpc::events::PlacementGroupLifecycleEvent::PENDING;
  }
}

rpc::events::PlacementGroupLifecycleEvent::SchedulingState
RayPlacementGroupLifecycleEvent::ConvertSchedulingState(
    rpc::PlacementGroupStats::SchedulingState state) {
  switch (state) {
  case rpc::PlacementGroupStats::QUEUED:
    return rpc::events::PlacementGroupLifecycleEvent::QUEUED;
  case rpc::PlacementGroupStats::REMOVED:
    return rpc::events::PlacementGroupLifecycleEvent::SCHEDULING_REMOVED;
  case rpc::PlacementGroupStats::SCHEDULING_STARTED:
    return rpc::events::PlacementGroupLifecycleEvent::SCHEDULING_STARTED;
  case rpc::PlacementGroupStats::NO_RESOURCES:
    return rpc::events::PlacementGroupLifecycleEvent::NO_RESOURCES;
  case rpc::PlacementGroupStats::INFEASIBLE:
    return rpc::events::PlacementGroupLifecycleEvent::INFEASIBLE;
  case rpc::PlacementGroupStats::FAILED_TO_COMMIT_RESOURCES:
    return rpc::events::PlacementGroupLifecycleEvent::FAILED_TO_COMMIT_RESOURCES;
  case rpc::PlacementGroupStats::FINISHED:
    return rpc::events::PlacementGroupLifecycleEvent::FINISHED;
  default:
    RAY_LOG(WARNING) << "Unknown scheduling state: " << state;
    return rpc::events::PlacementGroupLifecycleEvent::QUEUED;
  }
}

}  // namespace observability
}  // namespace ray
