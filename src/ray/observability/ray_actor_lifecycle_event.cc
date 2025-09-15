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

#include "ray/observability/ray_actor_lifecycle_event.h"

namespace ray {
namespace observability {

RayActorLifecycleEvent::RayActorLifecycleEvent(const rpc::ActorTableData &data,
                                               rpc::ActorLifecycleEvent::State state,
                                               const std::string &worker_id,
                                               const std::string &session_name)
    : RayEvent<rpc::ActorLifecycleEvent>(rpc::events::RayEvent::GCS,
                                         rpc::events::RayEvent::ACTOR_LIFECYCLE_EVENT,
                                         rpc::events::RayEvent::INFO,
                                         "",
                                         session_name) {
  ray::rpc::ActorLifecycleEvent::StateTransition state_transition;
  state_transition.set_state(state);
  state_transition.mutable_timestamp()->CopyFrom(AbslTimeNanosToProtoTimestamp(
      absl::ToInt64Nanoseconds(absl::Now() - absl::UnixEpoch())));

  // Set state specific fields
  if (state == rpc::ActorLifecycleEvent::ALIVE) {
    if (data.has_node_id()) {
      state_transition.set_node_id(data.node_id());
    }
    state_transition.set_worker_id(worker_id);
  }

  if (state == rpc::ActorLifecycleEvent::DEAD) {
    if (data.has_death_cause()) {
      *state_transition.mutable_death_cause() = data.death_cause();
    }
  }

  data_.set_actor_id(data.actor_id());
  data_.mutable_states()->Add(std::move(state_transition));
}

std::string RayActorLifecycleEvent::GetEntityId() const { return data_.actor_id(); }

void RayActorLifecycleEvent::MergeData(RayEvent<rpc::ActorLifecycleEvent> &&other) {
  auto &&other_event = static_cast<RayActorLifecycleEvent &&>(other);
  for (auto &state : *other_event.data_.mutable_states()) {
    data_.mutable_states()->Add(std::move(state));
  }
}

ray::rpc::events::RayEvent RayActorLifecycleEvent::SerializeData() const {
  ray::rpc::events::RayEvent event;
  event.mutable_actor_lifecycle_event()->Swap(&data_);
  return event;
}

}  // namespace observability
}  // namespace ray
