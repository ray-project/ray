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

ClusterResourceScheduler::ClusterResourceScheduler(
    int64_t local_node_id, const NodeResources &local_node_resources,
    gcs::GcsClient &gcs_client)
    : local_node_id_(local_node_id),
      gen_(std::chrono::high_resolution_clock::now().time_since_epoch().count()),
      gcs_client_(&gcs_client) {
  scheduling_policy_ = std::make_unique<raylet_scheduling_policy::SchedulingPolicy>(
      local_node_id_, nodes_);
  local_resource_manager_ = std::make_unique<LocalResourceManager>(
      local_node_id, string_to_int_map_, local_node_resources,
      /*get_used_object_store_memory*/ nullptr, /*get_pull_manager_at_capacity*/ nullptr,
      [&](const NodeResources &local_resource_update) {
        this->AddOrUpdateNode(local_node_id_, local_resource_update);
      });
  AddOrUpdateNode(local_node_id_, local_node_resources);
}

ClusterResourceScheduler::ClusterResourceScheduler(
    const std::string &local_node_id,
    const absl::flat_hash_map<std::string, double> &local_node_resources,
    gcs::GcsClient &gcs_client, std::function<int64_t(void)> get_used_object_store_memory,
    std::function<bool(void)> get_pull_manager_at_capacity)
    : gcs_client_(&gcs_client) {
  local_node_id_ = string_to_int_map_.Insert(local_node_id);
  scheduling_policy_ = std::make_unique<raylet_scheduling_policy::SchedulingPolicy>(
      local_node_id_, nodes_);
  NodeResources node_resources = ResourceMapToNodeResources(
      string_to_int_map_, local_node_resources, local_node_resources);

  local_resource_manager_ = std::make_unique<LocalResourceManager>(
      local_node_id_, string_to_int_map_, node_resources, get_used_object_store_memory,
      get_pull_manager_at_capacity, [&](const NodeResources &local_resource_update) {
        this->AddOrUpdateNode(local_node_id_, local_resource_update);
      });
  AddOrUpdateNode(local_node_id_, node_resources);
}

bool ClusterResourceScheduler::NodeAlive(int64_t node_id) const {
  if (node_id == local_node_id_) {
    return true;
  }
  if (node_id == -1) {
    return false;
  }
  auto node_id_binary = string_to_int_map_.Get(node_id);
  return gcs_client_->Nodes().Get(NodeID::FromBinary(node_id_binary)) != nullptr;
}

void ClusterResourceScheduler::AddOrUpdateNode(
    const std::string &node_id,
    const absl::flat_hash_map<std::string, double> &resources_total,
    const absl::flat_hash_map<std::string, double> &resources_available) {
  NodeResources node_resources = ResourceMapToNodeResources(
      string_to_int_map_, resources_total, resources_available);
  AddOrUpdateNode(string_to_int_map_.Insert(node_id), node_resources);
}

void ClusterResourceScheduler::AddOrUpdateNode(int64_t node_id,
                                               const NodeResources &node_resources) {
  RAY_LOG(DEBUG) << "Update node info, node_id: " << node_id << ", node_resources: "
                 << node_resources.DebugString(string_to_int_map_);
  auto it = nodes_.find(node_id);
  if (it == nodes_.end()) {
    // This node is new, so add it to the map.
    nodes_.emplace(node_id, node_resources);
  } else {
    // This node exists, so update its resources.
    it->second = Node(node_resources);
  }
}

bool ClusterResourceScheduler::UpdateNode(const std::string &node_id_string,
                                          const rpc::ResourcesData &resource_data) {
  auto node_id = string_to_int_map_.Insert(node_id_string);
  if (!nodes_.contains(node_id)) {
    return false;
  }

  auto resources_total = MapFromProtobuf(resource_data.resources_total());
  auto resources_available = MapFromProtobuf(resource_data.resources_available());
  NodeResources node_resources = ResourceMapToNodeResources(
      string_to_int_map_, resources_total, resources_available);
  NodeResources local_view;
  RAY_CHECK(GetNodeResources(node_id, &local_view));

  if (resource_data.resources_total_size() > 0) {
    for (size_t i = 0; i < node_resources.predefined_resources.size(); ++i) {
      local_view.predefined_resources[i].total =
          node_resources.predefined_resources[i].total;
    }
    for (auto &entry : node_resources.custom_resources) {
      local_view.custom_resources[entry.first].total = entry.second.total;
    }
  }

  if (resource_data.resources_available_changed()) {
    for (size_t i = 0; i < node_resources.predefined_resources.size(); ++i) {
      local_view.predefined_resources[i].available =
          node_resources.predefined_resources[i].available;
    }
    for (auto &entry : node_resources.custom_resources) {
      local_view.custom_resources[entry.first].available = entry.second.available;
    }

    local_view.object_pulls_queued = resource_data.object_pulls_queued();
  }

  AddOrUpdateNode(node_id, local_view);
  return true;
}

bool ClusterResourceScheduler::RemoveNode(int64_t node_id) {
  auto it = nodes_.find(node_id);
  if (it == nodes_.end()) {
    // Node not found.
    return false;
  } else {
    nodes_.erase(it);
    return true;
  }
}

bool ClusterResourceScheduler::RemoveNode(const std::string &node_id_string) {
  auto node_id = string_to_int_map_.Get(node_id_string);
  if (node_id == -1) {
    return false;
  }

  return RemoveNode(node_id);
}

bool ClusterResourceScheduler::IsSchedulable(const ResourceRequest &resource_request,
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

int64_t ClusterResourceScheduler::GetBestSchedulableNode(
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

std::string ClusterResourceScheduler::GetBestSchedulableNode(
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

bool ClusterResourceScheduler::SubtractRemoteNodeAvailableResources(
    int64_t node_id, const ResourceRequest &resource_request) {
  RAY_CHECK(node_id != local_node_id_);

  auto it = nodes_.find(node_id);
  if (it == nodes_.end()) {
    return false;
  }
  NodeResources *resources = it->second.GetMutableLocalView();

  // Just double check this node can still schedule the resource request.
  if (!IsSchedulable(resource_request, node_id, *resources)) {
    return false;
  }

  FixedPoint zero(0.);

  for (size_t i = 0; i < PredefinedResources_MAX; i++) {
    resources->predefined_resources[i].available =
        std::max(FixedPoint(0), resources->predefined_resources[i].available -
                                    resource_request.predefined_resources[i]);
  }

  for (const auto &task_req_custom_resource : resource_request.custom_resources) {
    auto it = resources->custom_resources.find(task_req_custom_resource.first);
    if (it != resources->custom_resources.end()) {
      it->second.available =
          std::max(FixedPoint(0), it->second.available - task_req_custom_resource.second);
    }
  }

  // TODO(swang): We should also subtract object store memory if the task has
  // arguments. Right now we do not modify object_pulls_queued in case of
  // performance regressions in spillback.

  return true;
}

bool ClusterResourceScheduler::GetNodeResources(int64_t node_id,
                                                NodeResources *ret_resources) const {
  auto it = nodes_.find(node_id);
  if (it != nodes_.end()) {
    *ret_resources = it->second.GetLocalView();
    return true;
  } else {
    return false;
  }
}

const NodeResources &ClusterResourceScheduler::GetLocalNodeResources() const {
  const auto &node_it = nodes_.find(local_node_id_);
  RAY_CHECK(node_it != nodes_.end());
  return node_it->second.GetLocalView();
}

int64_t ClusterResourceScheduler::NumNodes() const { return nodes_.size(); }

const StringIdMap &ClusterResourceScheduler::GetStringIdMap() const {
  return string_to_int_map_;
}

void ClusterResourceScheduler::UpdateResourceCapacity(const std::string &node_id_string,
                                                      const std::string &resource_name,
                                                      double resource_total) {
  int64_t node_id = string_to_int_map_.Get(node_id_string);

  auto it = nodes_.find(node_id);
  if (it == nodes_.end()) {
    NodeResources node_resources;
    node_resources.predefined_resources.resize(PredefinedResources_MAX);
    node_id = string_to_int_map_.Insert(node_id_string);
    it = nodes_.emplace(node_id, node_resources).first;
  }

  int idx = -1;
  if (resource_name == ray::kCPU_ResourceLabel) {
    idx = (int)CPU;
  } else if (resource_name == ray::kGPU_ResourceLabel) {
    idx = (int)GPU;
  } else if (resource_name == ray::kObjectStoreMemory_ResourceLabel) {
    idx = (int)OBJECT_STORE_MEM;
  } else if (resource_name == ray::kMemory_ResourceLabel) {
    idx = (int)MEM;
  };

  auto local_view = it->second.GetMutableLocalView();
  FixedPoint resource_total_fp(resource_total);
  if (idx != -1) {
    auto diff_capacity = resource_total_fp - local_view->predefined_resources[idx].total;
    local_view->predefined_resources[idx].total += diff_capacity;
    local_view->predefined_resources[idx].available += diff_capacity;
    if (local_view->predefined_resources[idx].available < 0) {
      local_view->predefined_resources[idx].available = 0;
    }
    if (local_view->predefined_resources[idx].total < 0) {
      local_view->predefined_resources[idx].total = 0;
    }
  } else {
    string_to_int_map_.Insert(resource_name);
    int64_t resource_id = string_to_int_map_.Get(resource_name);
    auto itr = local_view->custom_resources.find(resource_id);
    if (itr != local_view->custom_resources.end()) {
      auto diff_capacity = resource_total_fp - itr->second.total;
      itr->second.total += diff_capacity;
      itr->second.available += diff_capacity;
      if (itr->second.available < 0) {
        itr->second.available = 0;
      }
      if (itr->second.total < 0) {
        itr->second.total = 0;
      }
    } else {
      ResourceCapacity resource_capacity;
      resource_capacity.total = resource_capacity.available = resource_total_fp;
      local_view->custom_resources.emplace(resource_id, resource_capacity);
    }
  }
}

void ClusterResourceScheduler::DeleteResource(const std::string &node_id_string,
                                              const std::string &resource_name) {
  int64_t node_id = string_to_int_map_.Get(node_id_string);

  auto it = nodes_.find(node_id);
  if (it == nodes_.end()) {
    return;
  }

  int idx = -1;
  if (resource_name == ray::kCPU_ResourceLabel) {
    idx = (int)CPU;
  } else if (resource_name == ray::kGPU_ResourceLabel) {
    idx = (int)GPU;
  } else if (resource_name == ray::kObjectStoreMemory_ResourceLabel) {
    idx = (int)OBJECT_STORE_MEM;
  } else if (resource_name == ray::kMemory_ResourceLabel) {
    idx = (int)MEM;
  };
  auto local_view = it->second.GetMutableLocalView();
  if (idx != -1) {
    local_view->predefined_resources[idx].available = 0;
    local_view->predefined_resources[idx].total = 0;

  } else {
    int64_t resource_id = string_to_int_map_.Get(resource_name);
    auto itr = local_view->custom_resources.find(resource_id);
    if (itr != local_view->custom_resources.end()) {
      local_view->custom_resources.erase(itr);
    }
  }
}

std::string ClusterResourceScheduler::DebugString(void) const {
  std::stringstream buffer;
  buffer << "\nLocal id: " << local_node_id_;
  buffer << " Local resources: " << local_resource_manager_->DebugString();
  for (auto &node : nodes_) {
    buffer << "node id: " << node.first;
    buffer << node.second.GetLocalView().DebugString(string_to_int_map_);
  }
  return buffer.str();
}

std::string ClusterResourceScheduler::GetLocalResourceViewString() const {
  const auto &node_it = nodes_.find(local_node_id_);
  RAY_CHECK(node_it != nodes_.end());
  return node_it->second.GetLocalView().DictString(string_to_int_map_);
}

std::string ClusterResourceScheduler::GetResourceNameFromIndex(int64_t res_idx) {
  if (res_idx == CPU) {
    return ray::kCPU_ResourceLabel;
  } else if (res_idx == GPU) {
    return ray::kGPU_ResourceLabel;
  } else if (res_idx == OBJECT_STORE_MEM) {
    return ray::kObjectStoreMemory_ResourceLabel;
  } else if (res_idx == MEM) {
    return ray::kMemory_ResourceLabel;
  } else {
    return string_to_int_map_.Get((uint64_t)res_idx);
  }
}

bool ClusterResourceScheduler::AllocateRemoteTaskResources(
    const std::string &node_string,
    const absl::flat_hash_map<std::string, double> &task_resources) {
  ResourceRequest resource_request = ResourceMapToResourceRequest(
      string_to_int_map_, task_resources, /*requires_object_store_memory=*/false);
  auto node_id = string_to_int_map_.Insert(node_string);
  RAY_CHECK(node_id != local_node_id_);
  return SubtractRemoteNodeAvailableResources(node_id, resource_request);
}

bool ClusterResourceScheduler::IsLocallySchedulable(
    const absl::flat_hash_map<std::string, double> &shape) {
  auto resource_request = ResourceMapToResourceRequest(
      string_to_int_map_, shape, /*requires_object_store_memory=*/false);
  return IsSchedulable(resource_request, local_node_id_, GetLocalNodeResources());
}

}  // namespace ray
