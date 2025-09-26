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

#include "ray/observability/ray_node_lifecycle_event.h"

namespace ray {
namespace observability {

RayNodeLifecycleEvent::RayNodeLifecycleEvent(const rpc::GcsNodeInfo &data,
                                             const std::string &session_name)
    : RayEvent<rpc::events::NodeLifecycleEvent>(
          rpc::events::RayEvent::GCS,
          rpc::events::RayEvent::NODE_LIFECYCLE_EVENT,
          rpc::events::RayEvent::INFO,
          "",
          session_name) {
  ray::rpc::events::NodeLifecycleEvent::StateTransition state_transition;
  state_transition.mutable_timestamp()->CopyFrom(AbslTimeNanosToProtoTimestamp(
      absl::ToInt64Nanoseconds(absl::Now() - absl::UnixEpoch())));
  if (data.state() == rpc::GcsNodeInfo::ALIVE) {
    state_transition.set_state(rpc::events::NodeLifecycleEvent::ALIVE);
    state_transition.mutable_resources()->insert(data.resources_total().begin(),
                                                 data.resources_total().end());
  } else {
    state_transition.set_state(rpc::events::NodeLifecycleEvent::DEAD);
    auto death_info = state_transition.mutable_death_info();
    death_info->set_reason_message(data.death_info().reason_message());
    auto death_info_reason = data.death_info().reason();
    switch (death_info_reason) {
    case rpc::NodeDeathInfo::EXPECTED_TERMINATION:
      death_info->set_reason(
          rpc::events::NodeLifecycleEvent::DeathInfo::EXPECTED_TERMINATION);
      break;
    case rpc::NodeDeathInfo::UNEXPECTED_TERMINATION:
      death_info->set_reason(
          rpc::events::NodeLifecycleEvent::DeathInfo::UNEXPECTED_TERMINATION);
      break;
    case rpc::NodeDeathInfo::AUTOSCALER_DRAIN_PREEMPTED:
      death_info->set_reason(
          rpc::events::NodeLifecycleEvent::DeathInfo::AUTOSCALER_DRAIN_PREEMPTED);
      break;
    case rpc::NodeDeathInfo::AUTOSCALER_DRAIN_IDLE:
      death_info->set_reason(
          rpc::events::NodeLifecycleEvent::DeathInfo::AUTOSCALER_DRAIN_IDLE);
      break;
    default:
      death_info->set_reason(rpc::events::NodeLifecycleEvent::DeathInfo::UNSPECIFIED);
      break;
    }
  }

  data_.mutable_state_transitions()->Add(std::move(state_transition));
  data_.set_node_id(data.node_id());
}

std::string RayNodeLifecycleEvent::GetEntityId() const { return data_.node_id(); }

void RayNodeLifecycleEvent::MergeData(RayEvent<rpc::events::NodeLifecycleEvent> &&other) {
  auto &&other_event = static_cast<RayNodeLifecycleEvent &&>(other);
  for (auto &state_transition : *other_event.data_.mutable_state_transitions()) {
    data_.mutable_state_transitions()->Add(std::move(state_transition));
  }
}

ray::rpc::events::RayEvent RayNodeLifecycleEvent::SerializeData() && {
  ray::rpc::events::RayEvent event;
  event.mutable_node_lifecycle_event()->Swap(&data_);
  return event;
}

}  // namespace observability
}  // namespace ray
