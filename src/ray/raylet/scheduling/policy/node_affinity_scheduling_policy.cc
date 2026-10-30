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

NodeSchedulingResult NodeAffinitySchedulingPolicy::Schedule(
    const ResourceRequest &resource_request, SchedulingOptions options) {
  RAY_CHECK(options.scheduling_type_ == SchedulingType::NODE_AFFINITY);

  scheduling::NodeID target_node_id = scheduling::NodeID(options.node_affinity_node_id_);
  const auto it = nodes_.find(target_node_id);
  const bool target_node_is_feasible =
      it != nodes_.end() && is_node_alive_(target_node_id) &&
      it->second.GetLocalView().IsFeasible(resource_request);
  if (target_node_is_feasible) {
    if (!options.node_affinity_spill_on_unavailable_ &&
        !options.node_affinity_fail_on_unavailable_) {
      return NodeSchedulingResult::Scheduled(target_node_id);
    } else if (it->second.GetLocalView().IsAvailable(resource_request)) {
      return NodeSchedulingResult::Scheduled(target_node_id);
    }
  }

  if (!options.node_affinity_soft_) {
    return target_node_is_feasible ? NodeSchedulingResult::NoNodeAvailable()
                                   : NodeSchedulingResult::Infeasible();
  }

  options.scheduling_type_ = SchedulingType::HYBRID;
  return hybrid_policy_.Schedule(resource_request, options);
}

}  // namespace raylet_scheduling_policy
}  // namespace ray
