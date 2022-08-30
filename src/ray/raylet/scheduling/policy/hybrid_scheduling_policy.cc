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

bool HybridSchedulingPolicy::IsNodeFeasible(
    const scheduling::NodeID &node_id,
    const NodeFilter &node_filter,
    const NodeResources &node_resources,
    const ResourceRequest &resource_request) const {
  if (!is_node_alive_(node_id)) {
    return false;
  }

  if (node_filter != NodeFilter::kAny) {
    const bool has_gpu = node_resources.total.Has(ResourceID::GPU());
    if (node_filter == NodeFilter::kGPU && !has_gpu) {
      return false;
    } else if (node_filter == NodeFilter::kNonGpu && has_gpu) {
      return false;
    }
  }

  return node_resources.IsFeasible(resource_request);
}

float ComputeNodeScore(const NodeResources &node_resources, float spread_threshold) {
  float critical_resource_utilization =
      node_resources.CalculateCriticalResourceUtilization();
  if (critical_resource_utilization < spread_threshold) {
    critical_resource_utilization = 0;
  }
  return critical_resource_utilization;
}

scheduling::NodeID HybridSchedulingPolicy::GetBestNode(
    std::vector<std::pair<scheduling::NodeID, float>> &node_scores,
    bool prioritize_local_node,
    int k,
    float spread_threshold) const {
  // Pick the top k nodes with the lowest score.
  // First, sort nodes so that we always break ties between nodes in the same
  // order.
  std::sort(
      node_scores.begin(),
      node_scores.end(),
      [](const std::pair<scheduling::NodeID, float> &a,
         const std::pair<scheduling::NodeID, float> &b) { return a.first < b.first; });
  std::stable_sort(
      node_scores.begin(),
      node_scores.end(),
      [](const std::pair<scheduling::NodeID, float> &a,
         const std::pair<scheduling::NodeID, float> &b) { return a.second < b.second; });

  if (prioritize_local_node) {
    const auto local_it = nodes_.find(local_node_id_);
    RAY_CHECK(local_it != nodes_.end());
    float local_node_score =
        ComputeNodeScore(local_it->second.GetLocalView(), spread_threshold);
    if (local_node_score <= node_scores.front().second) {
      return local_node_id_;
    }
  }

  std::uniform_int_distribution<int> distribution(
      0, std::min(k, static_cast<int>(node_scores.size())) - 1);
  int node_index = distribution(gen_);
  return node_scores[node_index].first;
}

scheduling::NodeID HybridSchedulingPolicy::HybridPolicyWithFilter(
    const ResourceRequest &resource_request,
    float spread_threshold,
    bool force_spillback,
    bool require_node_available,
    NodeFilter node_filter) {
  // Nodes that are feasible and currently have available resources.
  std::vector<std::pair<scheduling::NodeID, float>> available_nodes;
  // Nodes that are feasible but currently do not have available resources.
  std::vector<std::pair<scheduling::NodeID, float>> feasible_nodes;
  // Check whether the local node is available and feasible. We'll use this to
  // help prioritize the local node when force_spillback=false.
  bool local_node_is_available = false;
  bool local_node_is_feasible = false;
  for (const auto &pair : nodes_) {
    const auto &node_id = pair.first;
    const auto &node_resources = pair.second.GetLocalView();
    if (force_spillback && node_id == local_node_id_) {
      continue;
    }
    if (IsNodeFeasible(node_id, node_filter, node_resources, resource_request)) {
      bool ignore_pull_manager_at_capacity = false;
      if (node_id == local_node_id_) {
        // It's okay if the local node's pull manager is at
        // capacity because we will eventually spill the task
        // back from the waiting queue if its args cannot be
        // pulled.
        ignore_pull_manager_at_capacity = true;
        local_node_is_feasible = true;
      }
      bool is_available =
          node_resources.IsAvailable(resource_request, ignore_pull_manager_at_capacity);
      if (node_id == local_node_id_ and is_available) {
        local_node_is_available = true;
      }
      float node_score = ComputeNodeScore(node_resources, spread_threshold);
      RAY_LOG(DEBUG) << "Node " << node_id.ToInt() << " is "
                     << (is_available ? "available" : "not available") << " for request "
                     << resource_request.DebugString()
                     << " with critical resource utilization " << node_score
                     << " based on local view " << node_resources.DebugString();
      if (is_available) {
        available_nodes.push_back({node_id, node_score});
      } else {
        feasible_nodes.push_back({node_id, node_score});
      }
    }
  }

  if (!available_nodes.empty()) {
    // First prioritize available nodes.
    return GetBestNode(available_nodes,
                       !force_spillback && local_node_is_available,
                       1,
                       spread_threshold);
  } else if (!feasible_nodes.empty() && !require_node_available) {
    // If there are no available nodes, and the caller is okay with an
    // unavailable node, check the feasible nodes next.
    return GetBestNode(
        feasible_nodes, !force_spillback && local_node_is_feasible, 1, spread_threshold);
  } else {
    return scheduling::NodeID::Nil();
  }
}

scheduling::NodeID HybridSchedulingPolicy::Schedule(
    const ResourceRequest &resource_request, SchedulingOptions options) {
  RAY_CHECK(options.scheduling_type == SchedulingType::HYBRID)
      << "HybridPolicy policy requires type = HYBRID";
  if (!options.avoid_gpu_nodes || resource_request.Has(ResourceID::GPU())) {
    return HybridPolicyWithFilter(resource_request,
                                  options.spread_threshold,
                                  options.avoid_local_node,
                                  options.require_node_available);
  }

  // Try schedule on non-GPU nodes.
  auto best_node_id = HybridPolicyWithFilter(resource_request,
                                             options.spread_threshold,
                                             options.avoid_local_node,
                                             /*require_node_available*/ true,
                                             NodeFilter::kNonGpu);
  if (!best_node_id.IsNil()) {
    return best_node_id;
  }

  // If we cannot find any available node from non-gpu nodes, fallback to the original
  // scheduling
  return HybridPolicyWithFilter(resource_request,
                                options.spread_threshold,
                                options.avoid_local_node,
                                options.require_node_available);
}

}  // namespace raylet_scheduling_policy
}  // namespace ray
