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

#include "ray/observability/ray_actor_task_definition_event.h"

#include <string>
#include <utility>

namespace ray {
namespace observability {

RayActorTaskDefinitionEvent::RayActorTaskDefinitionEvent(
    rpc::events::ActorTaskDefinitionEvent data, const std::string &session_name)
    : RayEvent<rpc::events::ActorTaskDefinitionEvent>(
          rpc::events::RayEvent::CORE_WORKER,
          rpc::events::RayEvent::ACTOR_TASK_DEFINITION_EVENT,
          rpc::events::RayEvent::INFO,
          "",
          session_name) {
  data_ = std::move(data);
}

std::string RayActorTaskDefinitionEvent::GetEntityId() const {
  return data_.task_id() + std::to_string(data_.task_attempt());
}

TaskAttemptId RayActorTaskDefinitionEvent::GetTaskAttempt() const {
  return {data_.task_id(), data_.task_attempt()};
}

void RayActorTaskDefinitionEvent::MergeData(
    RayEvent<rpc::events::ActorTaskDefinitionEvent> &&other) {
  // Definition events are static; merging does not change the event.
}

ray::rpc::events::RayEvent RayActorTaskDefinitionEvent::SerializeData() && {
  ray::rpc::events::RayEvent event;
  event.mutable_actor_task_definition_event()->Swap(&data_);
  return event;
}

}  // namespace observability
}  // namespace ray
