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

#include "ray/raylet/scheduling/policy/node_affinity_scheduling_policy.h"

namespace ray {
namespace raylet_scheduling_policy {

scheduling::NodeID NodeAffinitySchedulingPolicy::Schedule(
    const ResourceRequest &resource_request, SchedulingOptions options) {
  RAY_CHECK(options.scheduling_type == SchedulingType::NODE_AFFINITY);

  scheduling::NodeID target_node_id = scheduling::NodeID(options.node_affinity_node_id);
  if (nodes_.contains(target_node_id) && is_node_alive_(target_node_id) &&
      nodes_.at(target_node_id).GetLocalView().IsFeasible(resource_request)) {
    return target_node_id;
  }

  if (!options.node_affinity_soft) {
    return scheduling::NodeID::Nil();
  }

  options.scheduling_type = SchedulingType::HYBRID;
  return hybrid_policy_.Schedule(resource_request, options);
}

}  // namespace raylet_scheduling_policy
}  // namespace ray
