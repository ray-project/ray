// Copyright 2017 The Ray Authors.
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
#pragma once

#include <gtest/gtest_prod.h>

#include "ray/gcs/autoscaler/node_provider.h"
#include "ray/gcs/gcs_server/gcs_resource_manager.h"

namespace ray {
namespace autoscaler {

std::vector<std::pair<rpc::NodeType, int32_t>> BasciAutoscalingPolicy::GetNewNodesToLaunch(
  const ray::rpc::ResourceLoad& normal_resource_load,
  const ray::rpc::PlacementGroupLoad& pg_resource_load,
  const absl::flat_hash_map<NodeID, ray::rpc::ResourcesData>& cluster_resource_view,
  const rpc::AvailableNodeTypesResponse& available_node_types,
  const std::vector<std::pair<rpc::NodeType, int32_t>>& nodes_starting
) {
  auto new_nodes_to_start = FitsPlacementGroups(pg_resource_load, cluster_resource_view, available_node_types, nodes_starting);  
}

namespace {

bool Schedulable(const ray::rpc::ResourceData& resource_data, ray::rpc::Bundle bundle) {
  return true;
}

bool FitPlacementGroupBundle(  
  const ray::rpc::Bundle& bundle,
  const absl::flat_hash_map<NodeID, ray::rpc::ResourcesData>& cluster_resource_view,
   const std::vector<std::pair<rpc::NodeType, int32_t>>& nodes_starting) {
  
  for (auto& pg : pg_resource_load.placement_group_data()) {
    if (pg.stats() != rpc::PlacementGroupState::PENDING && pg.stats == rpc::PlacementGroupState::RESCHEDULING) {
      continue;
    }
    if (pg.strategy != PlacementStrategy::STRICT_SPREAD) {
      continue;
    }

    for (auto& bundle : pg_resource_load.bundles()) {
      for (auto& [node, resource] : cluster_resource_view) {
        if (Schedulable(resource, bundle)) {
          continue;
        }
      }
    }
  }
}

std::optional<NodeType> StartNewNode(  
  const ray::rpc::Bundle& bundle,
  const rpc::AvailableNodeTypesResponse& available_node_types) {
  
  for (auto& pg : pg_resource_load.placement_group_data()) {
    if (pg.stats() != rpc::PlacementGroupState::PENDING && pg.stats == rpc::PlacementGroupState::RESCHEDULING) {
      continue;
    }
    if (pg.strategy != PlacementStrategy::STRICT_SPREAD) {
      continue;
    }

    for (auto& bundle : pg_resource_load.bundles()) {
      for (auto& [node, resource] : cluster_resource_view) {
        if (Schedulable(resource, bundle)) {
          continue;
        }
      }
    }
  }
}

const std::vector<std::pair<rpc::NodeType, int32_t>> FitsPlacementGroups(  
  const ray::rpc::PlacementGroupLoad& pg_resource_load,
  const absl::flat_hash_map<NodeID, ray::rpc::ResourcesData>& cluster_resource_view,
  const rpc::AvailableNodeTypesResponse& available_node_types,
  const std::vector<std::pair<rpc::NodeType, int32_t>>& nodes_starting) {
  

  std::vector<rpc::NodeType> new_nodes_to_start;
  for (auto& pg : pg_resource_load.placement_group_data()) {
    if (pg.stats() != rpc::PlacementGroupState::PENDING && pg.stats == rpc::PlacementGroupState::RESCHEDULING) {
      continue;
    }
    if (pg.strategy != PlacementStrategy::STRICT_SPREAD) {
      continue;
    }


    for (auto& bundle : pg_resource_load.bundles()) {
      if (FitPlacementGroupBundle(bundle, clsuter_resource_view, node_starting)) {
        continue;
      }
      auto new_node = StartNewNode(bundle);
      if (new_node.has_value()) {
        new_nodes_to_start.push_back(new_node.value());
      } else {
        // request infeasible.
      }
    }
  }
}


}
}  // namespace autoscaler
}  // namespace ray
