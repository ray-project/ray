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

#include "ray/raylet/scheduling/policy/hybrid_scheduling_policy.h"

#include <functional>

#include "ray/util/container_util.h"
#include "ray/util/util.h"

namespace ray {

namespace raylet_scheduling_policy {

scheduling::NodeID HybridSchedulingPolicy::HybridPolicyWithFilter(
    const ResourceRequest &resource_request,
    float spread_threshold,
    bool force_spillback,
    bool require_node_available,
    const std::string &preferred_node,
    NodeFilter node_filter) {
  // Step 1: Generate the traversal order. We guarantee that the first node is local (or
  // the preferred node, if provided), to encourage local/preferred scheduling. The rest
  // of the traversal order should be globally consistent, to encourage using "warm"
  // workers.
  std::vector<scheduling::NodeID> round;
  round.reserve(nodes_.size());
  auto preferred_node_id =
      preferred_node.empty() ? local_node_id_ : scheduling::NodeID(preferred_node);
  auto preferred_it = nodes_.find(preferred_node_id);
  RAY_CHECK(preferred_it != nodes_.end());
  auto predicate = [this, node_filter](scheduling::NodeID node_id,
                                       const NodeResources &node_resources) {
    if (!is_node_available_(node_id)) {
      return false;
    }
    if (node_filter == NodeFilter::kAny) {
      return true;
    }
    const bool has_gpu = node_resources.total.Has(ResourceID::GPU());
    if (node_filter == NodeFilter::kGPU) {
      return has_gpu;
    }
    RAY_CHECK(node_filter == NodeFilter::kNonGpu);
    return !has_gpu;
  };

  const auto &preferred_node_view = preferred_it->second.GetLocalView();
  // If we should include local/preferred node at all, make sure it is at the front of the
  // list so that
  // 1. It's first in traversal order.
  // 2. It's easy to avoid sorting it.
  if (predicate(preferred_node_id, preferred_node_view) && !force_spillback) {
    round.push_back(preferred_node_id);
  }

  const auto start_index = round.size();
  for (const auto &pair : nodes_) {
    if (pair.first != preferred_node_id &&
        predicate(pair.first, pair.second.GetLocalView())) {
      round.push_back(pair.first);
    }
  }
  // Sort all the nodes, making sure that if we added the local node in front, it stays in
  // place.
  std::sort(round.begin() + start_index, round.end());

  scheduling::NodeID best_node_id = scheduling::NodeID::Nil();
  float best_utilization_score = INFINITY;
  bool best_is_available = false;

  // Step 2: Perform the round robin.
  auto round_it = round.begin();
  for (; round_it != round.end(); round_it++) {
    const auto &node_id = *round_it;
    const auto &it = nodes_.find(node_id);
    RAY_CHECK(it != nodes_.end());
    const auto &node = it->second;
    if (!node.GetLocalView().IsFeasible(resource_request)) {
      continue;
    }

    bool ignore_pull_manager_at_capacity = false;
    if (node_id == local_node_id_) {
      // It's okay if the local node's pull manager is at
      // capacity because we will eventually spill the task
      // back from the waiting queue if its args cannot be
      // pulled.
      ignore_pull_manager_at_capacity = true;
    }
    bool is_available = node.GetLocalView().IsAvailable(resource_request,
                                                        ignore_pull_manager_at_capacity);
    float critical_resource_utilization =
        node.GetLocalView().CalculateCriticalResourceUtilization();
    RAY_LOG(DEBUG) << "Node " << node_id.ToInt() << " is "
                   << (is_available ? "available" : "not available") << " for request "
                   << resource_request.DebugString()
                   << " with critical resource utilization "
                   << critical_resource_utilization << " based on local view "
                   << node.GetLocalView().DebugString();
    if (critical_resource_utilization < spread_threshold) {
      critical_resource_utilization = 0;
    }

    bool update_best_node = false;

    if (is_available) {
      // Always prioritize available nodes over nodes where the task must be queued first.
      if (!best_is_available) {
        update_best_node = true;
      } else if (critical_resource_utilization < best_utilization_score) {
        // Break ties between available nodes by their critical resource utilization.
        update_best_node = true;
      }
    } else if (!best_is_available &&
               critical_resource_utilization < best_utilization_score &&
               !require_node_available) {
      // Pick the best feasible node by critical resource utilization.
      update_best_node = true;
    }

    if (update_best_node) {
      best_node_id = node_id;
      best_utilization_score = critical_resource_utilization;
      best_is_available = is_available;
    }
  }

  return best_node_id;
}

scheduling::NodeID HybridSchedulingPolicy::Schedule(
    const ResourceRequest &resource_request, SchedulingOptions options) {
  RAY_CHECK(options.scheduling_type == SchedulingType::HYBRID)
      << "HybridPolicy policy requires type = HYBRID";
  if (!options.avoid_gpu_nodes || resource_request.Has(ResourceID::GPU())) {
    return HybridPolicyWithFilter(resource_request,
                                  options.spread_threshold,
                                  options.avoid_local_node,
                                  options.require_node_available,
                                  options.preferred_node_id);
  }

  // Try schedule on non-GPU nodes.
  auto best_node_id = HybridPolicyWithFilter(resource_request,
                                             options.spread_threshold,
                                             options.avoid_local_node,
                                             /*require_node_available*/ true,
                                             options.preferred_node_id,
                                             NodeFilter::kNonGpu);
  if (!best_node_id.IsNil()) {
    return best_node_id;
  }

  // If we cannot find any available node from non-gpu nodes, fallback to the original
  // scheduling
  return HybridPolicyWithFilter(resource_request,
                                options.spread_threshold,
                                options.avoid_local_node,
                                options.require_node_available,
                                options.preferred_node_id);
}

}  // namespace raylet_scheduling_policy
}  // namespace ray
