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

#include "ray/observability/ray_actor_definition_event.h"

namespace ray {
namespace observability {

RayActorDefinitionEvent::RayActorDefinitionEvent(
    const rpc::ActorTableData &data,
    const std::map<std::string, std::string> &labels,
    const std::string &session_name)
    : RayEvent<rpc::ActorDefinitionEvent>(session_name) {
  event_type_ = rpc::events::RayEvent::ACTOR_DEFINITION_EVENT;

  data_.set_actor_id(data.actor_id());
  data_.set_job_id(data.job_id());
  data_.set_is_detached(data.is_detached());
  data_.set_name(data.name());
  data_.set_ray_namespace(data.ray_namespace());
  data_.set_serialized_runtime_env(data.serialized_runtime_env());
  data_.set_class_name(data.class_name());
  data_.mutable_required_resources()->insert(data.required_resources().begin(),
                                             data.required_resources().end());
  if (data.has_placement_group_id()) {
    data_.set_placement_group_id(data.placement_group_id());
  }
  data_.mutable_labels()->insert(labels.begin(), labels.end());
  data_.mutable_label_selector()->insert(data.label_selector().begin(),
                                         data.label_selector().end());
}

std::string RayActorDefinitionEvent::GetResourceId() const { return data_.actor_id(); }

void RayActorDefinitionEvent::Merge(RayEvent<rpc::ActorDefinitionEvent> &&other) {
  // Definition events are static. Merging does not change the event.
  return;
}

ray::rpc::events::RayEvent RayActorDefinitionEvent::SerializeData() const {
  ray::rpc::events::RayEvent event;
  event.set_source_type(rpc::events::RayEvent::GCS);
  event.set_severity(rpc::events::RayEvent::INFO);
  *event.mutable_actor_definition_event() = data_;
  return event;
}

}  // namespace observability
}  // namespace ray
