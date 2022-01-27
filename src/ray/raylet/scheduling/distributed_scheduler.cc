// Copyright 2017 The Ray Authors.
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

#include "ray/raylet/scheduling/cluster_resource_scheduler.h"

#include <boost/algorithm/string.hpp>

#include "ray/common/grpc_util.h"
#include "ray/common/ray_config.h"

namespace ray {

DistributedScheduler::DistributedScheduler(
    gcs::GcsClient &gcs_client)
    : gen_(std::chrono::high_resolution_clock::now().time_since_epoch().count()),
      gcs_client_(&gcs_client) {
  scheduling_policy_ = std::make_unique<raylet_scheduling_policy::SchedulingPolicy>(
      local_node_id_, nodes_);
}

bool DistributedScheduler::NodeAlive(int64_t node_id) const {
  if (node_id == local_node_id_) {
    return true;
  }
  if (node_id == -1) {
    return false;
  }
  auto node_id_binary = string_to_int_map_.Get(node_id);
  return gcs_client_->Nodes().Get(NodeID::FromBinary(node_id_binary)) != nullptr;
}

bool DistributedScheduler::IsSchedulable(const ResourceRequest &resource_request,
                                             int64_t node_id,
                                             const NodeResources &resources) const {
  if (resource_request.requires_object_store_memory && resources.object_pulls_queued &&
      node_id != local_node_id_) {
    // It's okay if the local node's pull manager is at capacity because we
    // will eventually spill the task back from the waiting queue if its args
    // cannot be pulled.
    return false;
  }

  // First, check predefined resources.
  for (size_t i = 0; i < PredefinedResources_MAX; i++) {
    if (resource_request.predefined_resources[i] >
        resources.predefined_resources[i].available) {
      // A hard constraint has been violated, so we cannot schedule
      // this resource request.
      return false;
    }
  }

  // Now check custom resources.
  for (const auto &task_req_custom_resource : resource_request.custom_resources) {
    auto it = resources.custom_resources.find(task_req_custom_resource.first);

    if (it == resources.custom_resources.end()) {
      // Requested resource doesn't exist at this node.
      // This is a hard constraint so cannot schedule this resource request.
      return false;
    } else {
      if (task_req_custom_resource.second > it->second.available) {
        // Resource constraint is violated.
        return false;
      }
    }
  }

  return true;
}

int64_t DistributedScheduler::GetBestSchedulableNode(
    const ResourceRequest &resource_request,
    const rpc::SchedulingStrategy &scheduling_strategy, bool actor_creation,
    bool force_spillback, int64_t *total_violations, bool *is_infeasible) {
  // The zero cpu actor is a special case that must be handled the same way by all
  // scheduling policies.
  if (actor_creation && resource_request.IsEmpty()) {
    int64_t best_node = -1;
    // This is an actor which requires no resources.
    // Pick a random node to to avoid scheduling all actors on the local node.
    if (nodes_.size() > 0) {
      std::uniform_int_distribution<int> distribution(0, nodes_.size() - 1);
      int idx = distribution(gen_);
      auto iter = std::next(nodes_.begin(), idx);
      for (size_t i = 0; i < nodes_.size(); ++i) {
        // TODO(iycheng): Here is there are a lot of nodes died, the
        // distribution might not be even.
        if (NodeAlive(iter->first)) {
          best_node = iter->first;
          break;
        }
        ++iter;
        if (iter == nodes_.end()) {
          iter = nodes_.begin();
        }
      }
    }
    RAY_LOG(DEBUG) << "GetBestSchedulableNode, best_node = " << best_node
                   << ", # nodes = " << nodes_.size()
                   << ", resource_request = " << resource_request.DebugString();
    return best_node;
  }

  // TODO (Alex): Setting require_available == force_spillback is a hack in order to
  // remain bug compatible with the legacy scheduling algorithms.
  int64_t best_node_id = scheduling_policy_->HybridPolicy(
      resource_request,
      scheduling_strategy.scheduling_strategy_case() ==
              rpc::SchedulingStrategy::SchedulingStrategyCase::kSpreadSchedulingStrategy
          ? 0.0
          : RayConfig::instance().scheduler_spread_threshold(),
      force_spillback, force_spillback,
      [this](auto node_id) { return this->NodeAlive(node_id); });
  *is_infeasible = best_node_id == -1 ? true : false;
  if (!*is_infeasible) {
    // TODO (Alex): Support soft constraints if needed later.
    *total_violations = 0;
  }

  RAY_LOG(DEBUG) << "Scheduling decision. "
                 << "forcing spillback: " << force_spillback
                 << ". Best node: " << best_node_id << " "
                 << (string_to_int_map_.Get(best_node_id) == "-1"
                         ? NodeID::Nil()
                         : NodeID::FromBinary(string_to_int_map_.Get(best_node_id)))
                 << ", is infeasible: " << *is_infeasible;
  return best_node_id;
}

std::string DistributedScheduler::GetBestSchedulableNode(
    const absl::flat_hash_map<std::string, double> &task_resources,
    const rpc::SchedulingStrategy &scheduling_strategy, bool requires_object_store_memory,
    bool actor_creation, bool force_spillback, int64_t *total_violations,
    bool *is_infeasible) {
  ResourceRequest resource_request = ResourceMapToResourceRequest(
      string_to_int_map_, task_resources, requires_object_store_memory);
  int64_t node_id =
      GetBestSchedulableNode(resource_request, scheduling_strategy, actor_creation,
                             force_spillback, total_violations, is_infeasible);

  if (node_id == -1) {
    // This is not a schedulable node, so return empty string.
    return "";
  }
  // Return the string name of the node.
  return string_to_int_map_.Get(node_id);
}

bool DistributedScheduler::AllocateRemoteTaskResources(
    const std::string &node_string,
    const absl::flat_hash_map<std::string, double> &task_resources) {
  ResourceRequest resource_request = ResourceMapToResourceRequest(
      string_to_int_map_, task_resources, /*requires_object_store_memory=*/false);
  auto node_id = string_to_int_map_.Insert(node_string);
  RAY_CHECK(node_id != local_node_id_);
  return cluster_resource_manager_->SubtractRemoteNodeAvailableResources(node_id, resource_request);
}

bool DistributedScheduler::IsSchedulableOnNode(
    const std::string &node_name, const absl::flat_hash_map<std::string, double> &shape) {
  int64_t node_id = string_to_int_map_.Get(node_name);
  auto resource_request = ResourceMapToResourceRequest(
      string_to_int_map_, shape, /*requires_object_store_memory=*/false);
  return IsSchedulable(resource_request, node_id, GetNodeResources(node_name));
}

}  // namespace ray
