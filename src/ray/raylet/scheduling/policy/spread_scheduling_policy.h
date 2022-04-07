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

#include "ray/raylet/scheduling/policy/hybrid_scheduling_policy.h"
#include "ray/raylet/scheduling/policy/scheduling_policy.h"

namespace ray {
namespace raylet_scheduling_policy {

/// Round robin among available nodes.
/// If there are no available nodes, fallback to hybrid policy.
class SpreadSchedulingPolicy : public ISchedulingPolicy {
 public:
  SpreadSchedulingPolicy(scheduling::NodeID local_node_id,
                         const absl::flat_hash_map<scheduling::NodeID, Node> &nodes,
                         std::function<bool(scheduling::NodeID)> is_node_available)
      : local_node_id_(local_node_id),
        nodes_(nodes),
        is_node_available_(is_node_available),
        hybrid_policy_(local_node_id_, nodes_, is_node_available_) {}

  scheduling::NodeID Schedule(const ResourceRequest &resource_request,
                              SchedulingOptions options) override;

  /// Identifier of local node.
  const scheduling::NodeID local_node_id_;
  /// List of nodes in the clusters and their resources organized as a map.
  /// The key of the map is the node ID.
  const absl::flat_hash_map<scheduling::NodeID, Node> &nodes_;
  // The node to start round robin if it's spread scheduling.
  // The index may be inaccurate when nodes are added or removed dynamically,
  // but it should still be better than always scanning from 0 for spread scheduling.
  size_t spread_scheduling_next_index_ = 0;
  /// Function Checks if node is alive.
  std::function<bool(scheduling::NodeID)> is_node_available_;
  /// Instance of hybrid policy;
  HybridSchedulingPolicy hybrid_policy_;
};
}  // namespace raylet_scheduling_policy
}  // namespace ray
