// Copyright 2021 The Ray Authors.
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

#include <vector>

#include "ray/raylet/scheduling/policy/scheduling_policy.h"

namespace ray {
namespace raylet_scheduling_policy {

/// Policy that "randomly" picks a node that could fulfil the request.
/// TODO(scv119): if there are a lot of nodes died or can't fulfill the resource
/// requirement, the distribution might not be even.
class RandomSchedulingPolicy : public ISchedulingPolicy {
 public:
  RandomSchedulingPolicy(scheduling::NodeID local_node_id,
                         const absl::flat_hash_map<scheduling::NodeID, Node> &nodes,
                         std::function<bool(scheduling::NodeID)> is_node_available)
      : local_node_id_(local_node_id),
        nodes_(nodes),
        gen_(std::chrono::high_resolution_clock::now().time_since_epoch().count()),
        is_node_available_(is_node_available) {}

  scheduling::NodeID Schedule(const ResourceRequest &resource_request,
                              SchedulingOptions options) override;

  /// Identifier of local node.
  const scheduling::NodeID local_node_id_;
  /// List of nodes in the clusters and their resources organized as a map.
  /// The key of the map is the node ID.
  const absl::flat_hash_map<scheduling::NodeID, Node> &nodes_;
  /// Internally maintained random number generator.
  std::mt19937_64 gen_;
  /// Function Checks if node is alive.
  std::function<bool(scheduling::NodeID)> is_node_available_;
};
}  // namespace raylet_scheduling_policy
}  // namespace ray
