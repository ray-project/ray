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

#include "ray/common/bundle_location_index.h"
#include "ray/raylet/scheduling/policy/scheduling_policy.h"

namespace ray {
namespace raylet_scheduling_policy {

// Select the node based on placement group id that the user specified.
class AffinityWithBundleSchedulingPolicy : public ISchedulingPolicy {
 public:
  AffinityWithBundleSchedulingPolicy(
      scheduling::NodeID local_node_id,
      const absl::flat_hash_map<scheduling::NodeID, Node> &nodes,
      std::function<bool(scheduling::NodeID)> is_node_alive,
      const BundleLocationIndex &pg_location_index)
      : local_node_id_(local_node_id),
        nodes_(nodes),
        is_node_alive_(is_node_alive),
        bundle_location_index_(pg_location_index) {}

  scheduling::NodeID Schedule(const ResourceRequest &resource_request,
                              SchedulingOptions options) override;

  const scheduling::NodeID local_node_id_;
  const absl::flat_hash_map<scheduling::NodeID, Node> &nodes_;
  std::function<bool(scheduling::NodeID)> is_node_alive_;
  const BundleLocationIndex &bundle_location_index_;

 private:
  bool IsNodeFeasibleAndAvailable(const scheduling::NodeID &node_id,
                                  const ResourceRequest &resource_request,
                                  bool avoid_gpu_nodes);
};
}  // namespace raylet_scheduling_policy
}  // namespace ray
