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

#include "ray/raylet/scheduling/policy/node_scheduling_policy.h"

namespace ray {
namespace raylet_scheduling_policy {

scheduling::NodeID NodeSchedulingPolicy::Schedule(const ResourceRequest &resource_request,
                                                  SchedulingOptions options) {
  RAY_CHECK(options.scheduling_type == SchedulingType::NODE);

  for (const auto &[node_id, node] : nodes_) {
    if (node_id.Binary() != options.node_id) {
      continue;
    }
    if (!is_node_available_(node_id)) {
      break;
    }
    if (!node.GetLocalView().IsFeasible(resource_request)) {
      break;
    }
    return node_id;
  }

  if (!options.soft) {
    return scheduling::NodeID::Nil();
  }

  options.scheduling_type = SchedulingType::HYBRID;
  return hybrid_policy_.Schedule(resource_request, options);
}

}  // namespace raylet_scheduling_policy
}  // namespace ray
