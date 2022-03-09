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

#include "ray/raylet/scheduling/policy/spread_scheduling_policy.h"

#include <functional>

#include "ray/util/container_util.h"
#include "ray/util/util.h"

namespace ray {

namespace raylet_scheduling_policy {

scheduling::NodeID SpreadSchedulingPolicy::Schedule(
    const ResourceRequest &resource_request, SchedulingOptions options) {
  RAY_CHECK(options.spread_threshold == 0 &&
            options.scheduling_type == SchedulingType::SPREAD)
      << "SpreadPolicy policy requires spread_threshold = 0 and type = SPREAD";
  std::vector<scheduling::NodeID> round;
  round.reserve(nodes_.size());
  for (const auto &pair : nodes_) {
    round.emplace_back(pair.first);
  }
  std::sort(round.begin(), round.end());

  size_t round_index = spread_scheduling_next_index_;
  for (size_t i = 0; i < round.size(); ++i, ++round_index) {
    const auto &node_id = round[round_index % round.size()];
    const auto &node = map_find_or_die(nodes_, node_id);
    if (node_id == local_node_id_ && options.avoid_local_node) {
      continue;
    }
    if (!is_node_available_(node_id) ||
        !node.GetLocalView().IsFeasible(resource_request) ||
        !node.GetLocalView().IsAvailable(resource_request, true)) {
      continue;
    }

    spread_scheduling_next_index_ = ((round_index + 1) % round.size());
    return node_id;
  }
  options.scheduling_type = SchedulingType::HYBRID;
  return hybrid_policy_.Schedule(resource_request, options);
}

}  // namespace raylet_scheduling_policy
}  // namespace ray
