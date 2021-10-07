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

#include "ray/raylet/scheduling/scheduling_policy.h"

#include <functional>

namespace ray {

namespace raylet_scheduling_policy {
namespace {

bool IsGPURequest(const ResourceRequest &resource_request) {
  if (resource_request.predefined_resources.size() <= GPU) {
    return false;
  }
  return resource_request.predefined_resources[GPU] > 0;
}

bool DoesNodeHaveGPUs(const NodeResources &resources) {
  if (resources.predefined_resources.size() <= GPU) {
    return false;
  }
  return resources.predefined_resources[GPU].total > 0;
}
}  // namespace

int64_t HybridPolicyWithFilter(const ResourceRequest &resource_request,
                               const int64_t local_node_id,
                               const absl::flat_hash_map<int64_t, Node> &nodes,
                               float spread_threshold, bool force_spillback,
                               bool require_available, NodeFilter node_filter) {
  // Step 1: Generate the traversal order. We guarantee that the first node is local, to
  // encourage local scheduling. The rest of the traversal order should be globally
  // consistent, to encourage using "warm" workers.
  std::vector<int64_t> round;
  round.reserve(nodes.size());
  const auto local_it = nodes.find(local_node_id);
  RAY_CHECK(local_it != nodes.end());

  auto predicate = [node_filter](const auto &node) {
    if (node_filter == NodeFilter::kAny) {
      return true;
    }
    const bool has_gpu = DoesNodeHaveGPUs(node);
    if (node_filter == NodeFilter::kGPU) {
      return has_gpu;
    }
    RAY_CHECK(node_filter == NodeFilter::kCPUOnly);
    return !has_gpu;
  };

  const auto &local_node = local_it->second.GetLocalView();
  // If we should include local node at all, make sure it is at the front of the list
  // so that
  // 1. It's first in traversal order.
  // 2. It's easy to avoid sorting it.
  if (predicate(local_node) && !force_spillback) {
    round.push_back(local_node_id);
  }

  const auto start_index = round.size();
  for (const auto &pair : nodes) {
    if (pair.first != local_node_id && predicate(pair.second.GetLocalView())) {
      round.push_back(pair.first);
    }
  }
  // Sort all the nodes, making sure that if we added the local node in front, it stays in
  // place.
  std::sort(round.begin() + start_index, round.end());

  int64_t best_node_id = -1;
  float best_utilization_score = INFINITY;
  bool best_is_available = false;

  // Step 2: Perform the round robin.
  auto round_it = round.begin();
  for (; round_it != round.end(); round_it++) {
    const auto &node_id = *round_it;
    const auto &it = nodes.find(node_id);
    RAY_CHECK(it != nodes.end());
    const auto &node = it->second;
    if (!node.GetLocalView().IsFeasible(resource_request)) {
      continue;
    }

    bool ignore_pull_manager_at_capacity = false;
    if (node_id == local_node_id) {
      // It's okay if the local node's pull manager is at
      // capacity because we will eventually spill the task
      // back from the waiting queue if its args cannot be
      // pulled.
      ignore_pull_manager_at_capacity = true;
    }
    bool is_available = node.GetLocalView().IsAvailable(resource_request,
                                                        ignore_pull_manager_at_capacity);
    RAY_LOG(DEBUG) << "Node " << node_id << " is "
                   << (is_available ? "available" : "not available");
    float critical_resource_utilization =
        node.GetLocalView().CalculateCriticalResourceUtilization();
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
               !require_available) {
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

int64_t HybridPolicy(const ResourceRequest &resource_request, const int64_t local_node_id,
                     const absl::flat_hash_map<int64_t, Node> &nodes,
                     float spread_threshold, bool force_spillback, bool require_available,
                     bool scheduler_avoid_gpu_nodes) {
  if (!scheduler_avoid_gpu_nodes || IsGPURequest(resource_request)) {
    return HybridPolicyWithFilter(resource_request, local_node_id, nodes,
                                  spread_threshold, force_spillback, require_available);
  }

  // Try schedule on CPU-only nodes.
  const auto node_id =
      HybridPolicyWithFilter(resource_request, local_node_id, nodes, spread_threshold,
                             force_spillback, require_available, NodeFilter::kCPUOnly);
  if (node_id != -1) {
    return node_id;
  }
  // Could not schedule on CPU-only nodes, schedule on GPU nodes as a last resort.
  return HybridPolicyWithFilter(resource_request, local_node_id, nodes, spread_threshold,
                                force_spillback, require_available, NodeFilter::kGPU);
}

}  // namespace raylet_scheduling_policy
}  // namespace ray
