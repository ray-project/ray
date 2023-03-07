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

// autoscaling policy takes resource loads from the cluster and
// decide the next set of nodes to lauch.
class IAutoscalingPolicy {
 public:
  virtual ~IAutoscalingPolicy() = default;

  // decide the new nodes to startup, based on the current state.
  virtual std::vector<std::pair<rpc::NodeType, int32_t>> GetNewNodesToLaunch(
      const ray::rpc::ResourceLoad &normal_resource_load,
      const ray::rpc::PlacementGroupLoad &pg_resource_load,
      const absl::flat_hash_map<NodeID, ray::rpc::ResourcesData> &cluster_resource_view,
      const rpc::AvailableNodeTypesResponse &available_node_types,
      const std::vector<std::pair<rpc::NodeType, int32_t>> &nodes_starting) = 0;
};
}  // namespace autoscaler
}  // namespace ray
