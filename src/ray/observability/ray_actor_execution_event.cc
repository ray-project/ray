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

#include "ray/observability/ray_actor_execution_event.h"

namespace ray {
namespace observability {

RayActorExecutionEvent::RayActorExecutionEvent(const rpc::ActorTableData &data,
                                               rpc::ActorExecutionEvent::State state,
                                               const std::string &session_name)
    : RayEvent<rpc::ActorExecutionEvent>(session_name) {
  event_type_ = rpc::events::RayEvent::ACTOR_EXECUTION_EVENT;
  ray::rpc::ActorExecutionEvent::StateTransition state_transition;
  state_transition.set_state(state);
  state_transition.mutable_timestamp()->CopyFrom(AbslTimeNanosToProtoTimestamp(
      absl::ToInt64Nanoseconds(absl::Now() - absl::UnixEpoch())));

  // Set state specific fields
  if (state == rpc::ActorExecutionEvent::ALIVE) {
    if (data.has_node_id()) {
      state_transition.set_node_id(data.node_id());
    }
    state_transition.set_pid(data.pid());
    state_transition.set_repr_name(data.repr_name());
  }

  if (state == rpc::ActorExecutionEvent::DEAD) {
    if (data.has_death_cause()) {
      *state_transition.mutable_death_cause() = data.death_cause();
    }
  }

  data_.set_actor_id(data.actor_id());
  data_.mutable_states()->Add(std::move(state_transition));
}

std::string RayActorExecutionEvent::GetResourceId() const { return data_.actor_id(); }

void RayActorExecutionEvent::Merge(RayEvent<rpc::ActorExecutionEvent> &&other) {
  auto &&other_event = static_cast<RayActorExecutionEvent &&>(other);
  for (auto &state : *other_event.data_.mutable_states()) {
    data_.mutable_states()->Add(std::move(state));
  }
}

ray::rpc::events::RayEvent RayActorExecutionEvent::SerializeData() const {
  ray::rpc::events::RayEvent event;
  event.set_source_type(rpc::events::RayEvent::GCS);
  event.set_severity(rpc::events::RayEvent::INFO);
  *event.mutable_actor_execution_event() = data_;
  return event;
}

}  // namespace observability
}  // namespace ray
