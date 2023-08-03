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

#include "ray/raylet/scheduling/policy/random_scheduling_policy.h"

#include <functional>

#include "ray/util/container_util.h"
#include "ray/util/util.h"

namespace ray {

namespace raylet_scheduling_policy {

scheduling::NodeID RandomSchedulingPolicy::Schedule(
    const ResourceRequest &resource_request, SchedulingOptions options) {
  RAY_CHECK(options.scheduling_type == SchedulingType::RANDOM)
      << "HybridPolicy policy requires type = RANDOM";
  scheduling::NodeID best_node = scheduling::NodeID::Nil();
  if (nodes_.empty()) {
    return best_node;
  }

  RAY_CHECK(options.spread_threshold == 0 && !options.avoid_local_node &&
            options.require_node_available && !options.avoid_gpu_nodes)
      << "Random policy requires spread_threshold = 0, "
      << "avoid_local_node = false, "
      << "require_node_available = true, "
      << "avoid_gpu_nodes = false.";

  std::uniform_int_distribution<int> distribution(0, nodes_.size() - 1);
  int idx = distribution(gen_);
  auto iter = std::next(nodes_.begin(), idx);
  for (size_t i = 0; i < nodes_.size(); ++i) {
    // TODO(scv119): if there are a lot of nodes died or can't fulfill the resource
    // requirement, the distribution might not be even.
    const auto &node_id = iter->first;
    const auto &node = iter->second;
    if (is_node_available_(node_id) && node.GetLocalView().IsFeasible(resource_request) &&
        node.GetLocalView().IsAvailable(resource_request,
                                        /*ignore_pull_manager_at_capacity*/ true)) {
      best_node = iter->first;
      break;
    }
    ++iter;
    if (iter == nodes_.end()) {
      iter = nodes_.begin();
    }
  }
  RAY_LOG(DEBUG) << "RandomPolicy, best_node = " << best_node.ToInt()
                 << ", # nodes = " << nodes_.size()
                 << ", resource_request = " << resource_request.DebugString();
  return best_node;
}

}  // namespace raylet_scheduling_policy
}  // namespace ray
