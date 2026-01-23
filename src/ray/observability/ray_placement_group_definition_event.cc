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

#include "ray/observability/ray_placement_group_definition_event.h"

namespace ray {
namespace observability {

RayPlacementGroupDefinitionEvent::RayPlacementGroupDefinitionEvent(
    const rpc::PlacementGroupTableData &data, const std::string &session_name)
    : RayEvent<rpc::events::PlacementGroupDefinitionEvent>(
          rpc::events::RayEvent::GCS,
          rpc::events::RayEvent::PLACEMENT_GROUP_DEFINITION_EVENT,
          rpc::events::RayEvent::INFO,
          "",
          session_name) {
  data_.set_placement_group_id(data.placement_group_id());
  data_.set_name(data.name());
  data_.set_ray_namespace(data.ray_namespace());
  data_.set_strategy(data.strategy());
  data_.set_is_detached(data.is_detached());
  data_.set_creator_job_id(data.creator_job_id());
  data_.set_creator_actor_id(data.creator_actor_id());

  for (const auto &bundle : data.bundles()) {
    auto *bundle_spec = data_.add_bundles();
    bundle_spec->set_bundle_index(bundle.bundle_id().bundle_index());
    bundle_spec->mutable_unit_resources()->insert(bundle.unit_resources().begin(),
                                                  bundle.unit_resources().end());
    bundle_spec->mutable_label_selector()->insert(bundle.label_selector().begin(),
                                                  bundle.label_selector().end());
  }

  if (!data.soft_target_node_id().empty()) {
    data_.set_soft_target_node_id(data.soft_target_node_id());
  }

  if (data.has_stats()) {
    data_.set_creation_request_received_ns(data.stats().creation_request_received_ns());
  }
}

std::string RayPlacementGroupDefinitionEvent::GetEntityId() const {
  return data_.placement_group_id();
}

void RayPlacementGroupDefinitionEvent::MergeData(
    RayEvent<rpc::events::PlacementGroupDefinitionEvent> &&other) {
  // Definition events are static. Merging does not change the event.
  return;
}

ray::rpc::events::RayEvent RayPlacementGroupDefinitionEvent::SerializeData() && {
  ray::rpc::events::RayEvent event;
  event.mutable_placement_group_definition_event()->Swap(&data_);
  return event;
}

}  // namespace observability
}  // namespace ray
