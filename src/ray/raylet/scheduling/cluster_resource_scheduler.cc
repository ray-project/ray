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
#include "ray/raylet/scheduling/scheduling_policy.h"

namespace ray {

ClusterResourceScheduler::ClusterResourceScheduler()
    : hybrid_spillback_(RayConfig::instance().scheduler_hybrid_scheduling()),
      spread_threshold_(RayConfig::instance().scheduler_spread_threshold())

          {};

ClusterResourceScheduler::ClusterResourceScheduler(
    int64_t local_node_id, const NodeResources &local_node_resources)
    : hybrid_spillback_(RayConfig::instance().scheduler_hybrid_scheduling()),
      spread_threshold_(RayConfig::instance().scheduler_spread_threshold()),
      local_node_id_(local_node_id),
      gen_(std::chrono::high_resolution_clock::now().time_since_epoch().count()) {
  InitResourceUnitInstanceInfo();
  AddOrUpdateNode(local_node_id_, local_node_resources);
  InitLocalResources(local_node_resources);
}

ClusterResourceScheduler::ClusterResourceScheduler(
    const std::string &local_node_id,
    const std::unordered_map<std::string, double> &local_node_resources,
    std::function<int64_t(void)> get_used_object_store_memory,
    std::function<bool(void)> get_pull_manager_at_capacity)
    : hybrid_spillback_(RayConfig::instance().scheduler_hybrid_scheduling()),
      spread_threshold_(RayConfig::instance().scheduler_spread_threshold()),
      get_pull_manager_at_capacity_(get_pull_manager_at_capacity) {
  local_node_id_ = string_to_int_map_.Insert(local_node_id);
  NodeResources node_resources = ResourceMapToNodeResources(
      string_to_int_map_, local_node_resources, local_node_resources);

  InitResourceUnitInstanceInfo();
  AddOrUpdateNode(local_node_id_, node_resources);
  InitLocalResources(node_resources);
  get_used_object_store_memory_ = get_used_object_store_memory;
}

void ClusterResourceScheduler::InitResourceUnitInstanceInfo() {
  std::string predefined_unit_instance_resources =
      RayConfig::instance().predefined_unit_instance_resources();
  if (!predefined_unit_instance_resources.empty()) {
    std::vector<std::string> results;
    boost::split(results, predefined_unit_instance_resources, boost::is_any_of(","));
    for (std::string &result : results) {
      PredefinedResources resource = ResourceStringToEnum(result);
      RAY_CHECK(resource < PredefinedResources_MAX)
          << "Failed to parse predefined resource";
      predefined_unit_instance_resources_.emplace(resource);
    }
  }
  std::string custom_unit_instance_resources =
      RayConfig::instance().custom_unit_instance_resources();
  if (!custom_unit_instance_resources.empty()) {
    std::vector<std::string> results;
    boost::split(results, custom_unit_instance_resources, boost::is_any_of(","));
    for (std::string &result : results) {
      int64_t resource_id = string_to_int_map_.Insert(result);
      custom_unit_instance_resources_.emplace(resource_id);
    }
  }
}

void ClusterResourceScheduler::AddOrUpdateNode(
    const std::string &node_id,
    const std::unordered_map<std::string, double> &resources_total,
    const std::unordered_map<std::string, double> &resources_available) {
  NodeResources node_resources = ResourceMapToNodeResources(
      string_to_int_map_, resources_total, resources_available);
  AddOrUpdateNode(string_to_int_map_.Insert(node_id), node_resources);
}

void ClusterResourceScheduler::AddOrUpdateNode(int64_t node_id,
                                               const NodeResources &node_resources) {
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

bool ClusterResourceScheduler::IsFeasible(const ResourceRequest &resource_request,
                                          const NodeResources &resources) const {
  // First, check predefined resources.
  for (size_t i = 0; i < PredefinedResources_MAX; i++) {
    if (resource_request.predefined_resources[i] >
        resources.predefined_resources[i].total) {
      return false;
    }
  }

  // Now check custom resources.
  for (const auto &task_req_custom_resource : resource_request.custom_resources) {
    auto it = resources.custom_resources.find(task_req_custom_resource.first);

    if (it == resources.custom_resources.end()) {
      return false;
    }
    if (task_req_custom_resource.second > it->second.total) {
      return false;
    }
  }

  return true;
}

int64_t ClusterResourceScheduler::IsSchedulable(const ResourceRequest &resource_request,
                                                int64_t node_id,
                                                const NodeResources &resources) const {
  int violations = 0;

  if (resource_request.requires_object_store_memory && resources.object_pulls_queued &&
      node_id != local_node_id_) {
    // It's okay if the local node's pull manager is at capacity because we
    // will eventually spill the task back from the waiting queue if its args
    // cannot be pulled.
    return -1;
  }

  // First, check predefined resources.
  for (size_t i = 0; i < PredefinedResources_MAX; i++) {
    if (resource_request.predefined_resources[i] >
        resources.predefined_resources[i].available) {
      // A hard constraint has been violated, so we cannot schedule
      // this resource request.
      return -1;
    }
  }

  // Now check custom resources.
  for (const auto &task_req_custom_resource : resource_request.custom_resources) {
    auto it = resources.custom_resources.find(task_req_custom_resource.first);

    if (it == resources.custom_resources.end()) {
      // Requested resource doesn't exist at this node.
      // This is a hard constraint so cannot schedule this resource request.
      return -1;
    } else {
      if (task_req_custom_resource.second > it->second.available) {
        // Resource constraint is violated.
        return -1;
      }
    }
  }

  return violations;
}

int64_t ClusterResourceScheduler::GetBestSchedulableNodeSimpleBinPack(
    const ResourceRequest &resource_request, bool actor_creation, bool force_spillback,
    int64_t *total_violations, bool *is_infeasible) {
  // NOTE: We need to set `is_infeasible` to false in advance to avoid `is_infeasible` not
  // being set.
  *is_infeasible = false;

  // Minimum number of soft violations across all nodes that can schedule the request.
  // We will pick the node with the smallest number of soft violations.
  int64_t min_violations = INT_MAX;
  // Node associated to min_violations.
  std::vector<int64_t> best_nodes;
  *total_violations = 0;

  // Check whether local node is schedulable. We return immediately
  // the local node only if there are zero violations.
  const auto local_node_it = nodes_.find(local_node_id_);
  if (!force_spillback) {
    if (local_node_it != nodes_.end()) {
      if (IsSchedulable(resource_request, local_node_it->first,
                        local_node_it->second.GetLocalView()) == 0) {
        return local_node_id_;
      }
    }
  }

  bool local_node_feasible =
      IsFeasible(resource_request, local_node_it->second.GetLocalView());

  for (const auto &node : nodes_) {
    // Return -1 if node not schedulable. otherwise return the number
    // of soft constraint violations.
    int64_t violations =
        IsSchedulable(resource_request, node.first, node.second.GetLocalView());
    if (violations == -1) {
      if (!local_node_feasible && best_nodes.empty() &&
          IsFeasible(resource_request, node.second.GetLocalView())) {
        // If the local node is not feasible, and a better node has not yet
        // been found, and this node does not currently have the resources
        // available but is feasible, then schedule to this node.
        // NOTE(swang): This is needed to make sure that tasks that are not
        // feasible on this node are spilled back to a node that does have the
        // appropriate total resources in a timely manner.
        best_nodes.emplace_back(node.first);
      }
      continue;
    }

    if (force_spillback && node.first == local_node_id_) {
      // Forcing spill back to remote node, so skip the local node.
      continue;
    }

    // Update the node with the smallest number of soft constraints violated.
    if (min_violations > violations) {
      min_violations = violations;
      // Clear should be performed by O(1) by the compiler because the
      // vector entry type is int64_t.
      best_nodes.clear();
    }
    if (min_violations == violations) {
      best_nodes.emplace_back(node.first);
    }
  }
  *total_violations = min_violations;

  // Randomly select one of the best nodes to spillback.
  int64_t best_node = -1;
  if (!best_nodes.empty()) {
    best_node = best_nodes[0];
  }

  // If there's no best node, and the task is not feasible locally,
  // it means the task is infeasible.
  *is_infeasible = best_node == -1 && !local_node_feasible;
  return best_node;
}

int64_t ClusterResourceScheduler::GetBestSchedulableNode(
    const ResourceRequest &resource_request, bool actor_creation, bool force_spillback,
    int64_t *total_violations, bool *is_infeasible) {
  // The zero cpu actor is a special case that must be handled the same way by all
  // scheduling policies.
  if (actor_creation && resource_request.IsEmpty()) {
    int64_t best_node = -1;
    // This is an actor which requires no resources.
    // Pick a random node to to avoid scheduling all actors on the local node.
    if (nodes_.size() > 0) {
      std::uniform_int_distribution<int> distribution(0, nodes_.size() - 1);
      int idx = distribution(gen_);
      best_node = std::next(nodes_.begin(), idx)->first;
    }
    RAY_LOG(DEBUG) << "GetBestSchedulableNode, best_node = " << best_node
                   << ", # nodes = " << nodes_.size()
                   << ", resource_request = " << resource_request.DebugString();
    return best_node;
  }

  if (!hybrid_spillback_) {
    return GetBestSchedulableNodeSimpleBinPack(resource_request, actor_creation,
                                               force_spillback, total_violations,
                                               is_infeasible);
  }

  // TODO (Alex): Setting require_available == force_spillback is a hack in order to
  // remain bug compatible with the legacy scheduling algorithms.
  int64_t best_node_id = raylet_scheduling_policy::HybridPolicy(
      resource_request, local_node_id_, nodes_, spread_threshold_, force_spillback,
      force_spillback);
  *is_infeasible = best_node_id == -1 ? true : false;
  if (!*is_infeasible) {
    // TODO (Alex): Support soft constraints if needed later.
    *total_violations = 0;
  }

  RAY_LOG(DEBUG) << "Scheduling decision. "
                 << "forcing spillback: " << force_spillback
                 << ". Best node: " << best_node_id
                 << ", is infeasible: " << *is_infeasible;
  return best_node_id;
}

std::string ClusterResourceScheduler::GetBestSchedulableNode(
    const std::unordered_map<std::string, double> &task_resources,
    bool requires_object_store_memory, bool actor_creation, bool force_spillback,
    int64_t *total_violations, bool *is_infeasible) {
  ResourceRequest resource_request = ResourceMapToResourceRequest(
      string_to_int_map_, task_resources, requires_object_store_memory);
  int64_t node_id = GetBestSchedulableNode(
      resource_request, actor_creation, force_spillback, total_violations, is_infeasible);

  std::string id_string;
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
  if (IsSchedulable(resource_request, node_id, *resources) == -1) {
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

void ClusterResourceScheduler::AddLocalResourceInstances(
    const std::string &resource_name, const std::vector<FixedPoint> &instances) {
  ResourceInstanceCapacities *node_instances;
  local_resources_.predefined_resources.resize(PredefinedResources_MAX);
  if (kCPU_ResourceLabel == resource_name) {
    node_instances = &local_resources_.predefined_resources[CPU];
  } else if (kGPU_ResourceLabel == resource_name) {
    node_instances = &local_resources_.predefined_resources[GPU];
  } else if (kObjectStoreMemory_ResourceLabel == resource_name) {
    node_instances = &local_resources_.predefined_resources[OBJECT_STORE_MEM];
  } else if (kMemory_ResourceLabel == resource_name) {
    node_instances = &local_resources_.predefined_resources[MEM];
  } else {
    string_to_int_map_.Insert(resource_name);
    int64_t resource_id = string_to_int_map_.Get(resource_name);
    node_instances = &local_resources_.custom_resources[resource_id];
  }

  if (node_instances->total.size() < instances.size()) {
    node_instances->total.resize(instances.size());
    node_instances->available.resize(instances.size());
  }

  for (size_t i = 0; i < instances.size(); i++) {
    node_instances->available[i] += instances[i];
    node_instances->total[i] =
        std::max(node_instances->total[i], node_instances->available[i]);
  }
  UpdateLocalAvailableResourcesFromResourceInstances();
}

bool ClusterResourceScheduler::IsAvailableResourceEmpty(
    const std::string &resource_name) {
  auto it = nodes_.find(local_node_id_);
  if (it == nodes_.end()) {
    RAY_LOG(WARNING) << "Can't find local node:[" << local_node_id_
                     << "] when check local available resource.";
    return true;
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
    return local_view->predefined_resources[idx].available <= 0;
  }
  string_to_int_map_.Insert(resource_name);
  int64_t resource_id = string_to_int_map_.Get(resource_name);
  auto itr = local_view->custom_resources.find(resource_id);
  if (itr != local_view->custom_resources.end()) {
    return itr->second.available <= 0;
  } else {
    return true;
  }
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

void ClusterResourceScheduler::DeleteLocalResource(const std::string &resource_name) {
  DeleteResource(string_to_int_map_.Get(local_node_id_), resource_name);
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

    if (node_id == local_node_id_) {
      local_resources_.predefined_resources[idx].total.clear();
      local_resources_.predefined_resources[idx].available.clear();
    }
  } else {
    int64_t resource_id = string_to_int_map_.Get(resource_name);
    auto itr = local_view->custom_resources.find(resource_id);
    if (itr != local_view->custom_resources.end()) {
      local_view->custom_resources.erase(itr);
    }

    auto c_itr = local_resources_.custom_resources.find(resource_id);
    if (node_id == local_node_id_ && c_itr != local_resources_.custom_resources.end()) {
      local_resources_.custom_resources[resource_id].total.clear();
      local_resources_.custom_resources[resource_id].available.clear();
      local_resources_.custom_resources.erase(c_itr);
    }
  }
}

std::string ClusterResourceScheduler::SerializedTaskResourceInstances(
    std::shared_ptr<TaskResourceInstances> task_allocation) const {
  bool has_added_resource = false;
  std::stringstream buffer;
  buffer << "{";
  for (size_t i = 0; i < PredefinedResources_MAX; i++) {
    std::vector<FixedPoint> resource = task_allocation->predefined_resources[i];
    if (resource.empty()) {
      continue;
    }
    if (has_added_resource) {
      buffer << ",";
    }
    std::string resource_name = ResourceEnumToString(static_cast<PredefinedResources>(i));
    buffer << "\"" << resource_name << "\":";
    bool is_unit_instance = predefined_unit_instance_resources_.find(i) !=
                            predefined_unit_instance_resources_.end();
    if (!is_unit_instance) {
      buffer << resource[0];
    } else {
      buffer << "[";
      for (size_t i = 0; i < resource.size(); i++) {
        buffer << resource[i];
        if (i < resource.size() - 1) {
          buffer << ", ";
        }
      }
      buffer << "]";
    }
    has_added_resource = true;
  }
  // TODO (chenk008): add custom_resources
  buffer << "}";
  return buffer.str();
}

std::string ClusterResourceScheduler::DebugString(void) const {
  std::stringstream buffer;
  buffer << "\nLocal id: " << local_node_id_;
  buffer << " Local resources: " << local_resources_.DebugString(string_to_int_map_);
  for (auto &node : nodes_) {
    buffer << "node id: " << node.first;
    buffer << node.second.GetLocalView().DebugString(string_to_int_map_);
  }
  return buffer.str();
}

void ClusterResourceScheduler::InitResourceInstances(
    FixedPoint total, bool unit_instances, ResourceInstanceCapacities *instance_list) {
  if (unit_instances) {
    size_t num_instances = static_cast<size_t>(total.Double());
    instance_list->total.resize(num_instances);
    instance_list->available.resize(num_instances);
    for (size_t i = 0; i < num_instances; i++) {
      instance_list->total[i] = instance_list->available[i] = 1.0;
    };
  } else {
    instance_list->total.resize(1);
    instance_list->available.resize(1);
    instance_list->total[0] = instance_list->available[0] = total;
  }
}

std::string ClusterResourceScheduler::GetLocalResourceViewString() const {
  const auto &node_it = nodes_.find(local_node_id_);
  RAY_CHECK(node_it != nodes_.end());
  return node_it->second.GetLocalView().DictString(string_to_int_map_);
}

void ClusterResourceScheduler::InitLocalResources(const NodeResources &node_resources) {
  local_resources_.predefined_resources.resize(PredefinedResources_MAX);

  for (size_t i = 0; i < PredefinedResources_MAX; i++) {
    if (node_resources.predefined_resources[i].total > 0) {
      // when we enable cpushare, the CPU will not be treat as unit_instance.
      bool is_unit_instance = predefined_unit_instance_resources_.find(i) !=
                              predefined_unit_instance_resources_.end();
      InitResourceInstances(node_resources.predefined_resources[i].total,
                            is_unit_instance, &local_resources_.predefined_resources[i]);
    }
  }

  if (node_resources.custom_resources.size() == 0) {
    return;
  }

  for (auto it = node_resources.custom_resources.begin();
       it != node_resources.custom_resources.end(); ++it) {
    if (it->second.total > 0) {
      bool is_unit_instance = custom_unit_instance_resources_.find(it->first) !=
                              custom_unit_instance_resources_.end();
      ResourceInstanceCapacities instance_list;
      InitResourceInstances(it->second.total, is_unit_instance, &instance_list);
      local_resources_.custom_resources.emplace(it->first, instance_list);
    }
  }
}

std::vector<FixedPoint> ClusterResourceScheduler::AddAvailableResourceInstances(
    std::vector<FixedPoint> available, ResourceInstanceCapacities *resource_instances) {
  std::vector<FixedPoint> overflow(available.size(), 0.);
  for (size_t i = 0; i < available.size(); i++) {
    resource_instances->available[i] = resource_instances->available[i] + available[i];
    if (resource_instances->available[i] > resource_instances->total[i]) {
      overflow[i] = (resource_instances->available[i] - resource_instances->total[i]);
      resource_instances->available[i] = resource_instances->total[i];
    }
  }

  return overflow;
}

std::vector<FixedPoint> ClusterResourceScheduler::SubtractAvailableResourceInstances(
    std::vector<FixedPoint> available, ResourceInstanceCapacities *resource_instances,
    bool allow_going_negative) {
  RAY_CHECK(available.size() == resource_instances->available.size());

  std::vector<FixedPoint> underflow(available.size(), 0.);
  for (size_t i = 0; i < available.size(); i++) {
    if (resource_instances->available[i] < 0) {
      if (allow_going_negative) {
        resource_instances->available[i] =
            resource_instances->available[i] - available[i];
      } else {
        underflow[i] = available[i];  // No change in the value in this case.
      }
    } else {
      resource_instances->available[i] = resource_instances->available[i] - available[i];
      if (resource_instances->available[i] < 0 && !allow_going_negative) {
        underflow[i] = -resource_instances->available[i];
        resource_instances->available[i] = 0;
      }
    }
  }
  return underflow;
}

bool ClusterResourceScheduler::AllocateResourceInstances(
    FixedPoint demand, std::vector<FixedPoint> &available,
    std::vector<FixedPoint> *allocation) {
  allocation->resize(available.size());
  FixedPoint remaining_demand = demand;

  if (available.size() == 1) {
    // This resource has just an instance.
    if (available[0] >= remaining_demand) {
      available[0] -= remaining_demand;
      (*allocation)[0] = remaining_demand;
      return true;
    } else {
      // Not enough capacity.
      return false;
    }
  }

  // If resources has multiple instances, each instance has total capacity of 1.
  //
  // If this resource constraint is hard, as long as remaining_demand is greater than 1.,
  // allocate full unit-capacity instances until the remaining_demand becomes fractional.
  // Then try to find the best fit for the fractional remaining_resources. Best fist means
  // allocating the resource instance with the smallest available capacity greater than
  // remaining_demand
  //
  // If resource constraint is soft, allocate as many full unit-capacity resources and
  // then distribute remaining_demand across remaining instances. Note that in case we can
  // overallocate this resource.
  if (remaining_demand >= 1.) {
    for (size_t i = 0; i < available.size(); i++) {
      if (available[i] == 1.) {
        // Allocate a full unit-capacity instance.
        (*allocation)[i] = 1.;
        available[i] = 0;
        remaining_demand -= 1.;
      }
      if (remaining_demand < 1.) {
        break;
      }
    }
  }

  if (remaining_demand >= 1.) {
    // Cannot satisfy a demand greater than one if no unit capacity resource is available.
    return false;
  }

  // Remaining demand is fractional. Find the best fit, if exists.
  if (remaining_demand > 0.) {
    int64_t idx_best_fit = -1;
    FixedPoint available_best_fit = 1.;
    for (size_t i = 0; i < available.size(); i++) {
      if (available[i] >= remaining_demand) {
        if (idx_best_fit == -1 ||
            (available[i] - remaining_demand < available_best_fit)) {
          available_best_fit = available[i] - remaining_demand;
          idx_best_fit = static_cast<int64_t>(i);
        }
      }
    }
    if (idx_best_fit == -1) {
      return false;
    } else {
      (*allocation)[idx_best_fit] = remaining_demand;
      available[idx_best_fit] -= remaining_demand;
    }
  }
  return true;
}

bool ClusterResourceScheduler::AllocateTaskResourceInstances(
    const ResourceRequest &resource_request,
    std::shared_ptr<TaskResourceInstances> task_allocation) {
  RAY_CHECK(task_allocation != nullptr);
  if (nodes_.find(local_node_id_) == nodes_.end()) {
    return false;
  }
  task_allocation->predefined_resources.resize(PredefinedResources_MAX);
  for (size_t i = 0; i < PredefinedResources_MAX; i++) {
    if (resource_request.predefined_resources[i] > 0) {
      if (!AllocateResourceInstances(resource_request.predefined_resources[i],
                                     local_resources_.predefined_resources[i].available,
                                     &task_allocation->predefined_resources[i])) {
        // Allocation failed. Restore node's local resources by freeing the resources
        // of the failed allocation.
        FreeTaskResourceInstances(task_allocation);
        return false;
      }
    }
  }

  for (const auto &task_req_custom_resource : resource_request.custom_resources) {
    auto it = local_resources_.custom_resources.find(task_req_custom_resource.first);
    if (it != local_resources_.custom_resources.end()) {
      if (task_req_custom_resource.second > 0) {
        std::vector<FixedPoint> allocation;
        bool success = AllocateResourceInstances(task_req_custom_resource.second,
                                                 it->second.available, &allocation);
        // Even if allocation failed we need to remember partial allocations to correctly
        // free resources.
        task_allocation->custom_resources.emplace(it->first, allocation);
        if (!success) {
          // Allocation failed. Restore node's local resources by freeing the resources
          // of the failed allocation.
          FreeTaskResourceInstances(task_allocation);
          return false;
        }
      }
    } else {
      return false;
    }
  }
  return true;
}

void ClusterResourceScheduler::UpdateLocalAvailableResourcesFromResourceInstances() {
  auto it_local_node = nodes_.find(local_node_id_);
  RAY_CHECK(it_local_node != nodes_.end());

  auto local_view = it_local_node->second.GetMutableLocalView();
  for (size_t i = 0; i < PredefinedResources_MAX; i++) {
    local_view->predefined_resources[i].available = 0;
    for (size_t j = 0; j < local_resources_.predefined_resources[i].available.size();
         j++) {
      local_view->predefined_resources[i].available +=
          local_resources_.predefined_resources[i].available[j];
    }
  }

  for (auto &custom_resource : local_resources_.custom_resources) {
    int64_t resource_name = custom_resource.first;
    auto &instances = custom_resource.second;

    FixedPoint available = std::accumulate(instances.available.begin(),
                                           instances.available.end(), FixedPoint());
    FixedPoint total =
        std::accumulate(instances.total.begin(), instances.total.end(), FixedPoint());

    local_view->custom_resources[resource_name].available = available;
    local_view->custom_resources[resource_name].total = total;
  }
}

void ClusterResourceScheduler::FreeTaskResourceInstances(
    std::shared_ptr<TaskResourceInstances> task_allocation) {
  RAY_CHECK(task_allocation != nullptr);
  for (size_t i = 0; i < PredefinedResources_MAX; i++) {
    AddAvailableResourceInstances(task_allocation->predefined_resources[i],
                                  &local_resources_.predefined_resources[i]);
  }

  for (const auto &task_allocation_custom_resource : task_allocation->custom_resources) {
    auto it =
        local_resources_.custom_resources.find(task_allocation_custom_resource.first);
    if (it != local_resources_.custom_resources.end()) {
      AddAvailableResourceInstances(task_allocation_custom_resource.second, &it->second);
    }
  }
}

std::vector<double> ClusterResourceScheduler::AddCPUResourceInstances(
    std::vector<double> &cpu_instances) {
  std::vector<FixedPoint> cpu_instances_fp =
      VectorDoubleToVectorFixedPoint(cpu_instances);

  if (cpu_instances.size() == 0) {
    return cpu_instances;  // No overflow.
  }
  RAY_CHECK(nodes_.find(local_node_id_) != nodes_.end());

  auto overflow = AddAvailableResourceInstances(
      cpu_instances_fp, &local_resources_.predefined_resources[CPU]);
  UpdateLocalAvailableResourcesFromResourceInstances();

  return VectorFixedPointToVectorDouble(overflow);
}

std::vector<double> ClusterResourceScheduler::SubtractCPUResourceInstances(
    std::vector<double> &cpu_instances, bool allow_going_negative) {
  std::vector<FixedPoint> cpu_instances_fp =
      VectorDoubleToVectorFixedPoint(cpu_instances);

  if (cpu_instances.size() == 0) {
    return cpu_instances;  // No underflow.
  }
  RAY_CHECK(nodes_.find(local_node_id_) != nodes_.end());

  auto underflow = SubtractAvailableResourceInstances(
      cpu_instances_fp, &local_resources_.predefined_resources[CPU],
      allow_going_negative);
  UpdateLocalAvailableResourcesFromResourceInstances();

  return VectorFixedPointToVectorDouble(underflow);
}

std::vector<double> ClusterResourceScheduler::AddGPUResourceInstances(
    std::vector<double> &gpu_instances) {
  std::vector<FixedPoint> gpu_instances_fp =
      VectorDoubleToVectorFixedPoint(gpu_instances);

  if (gpu_instances.size() == 0) {
    return gpu_instances;  // No overflow.
  }
  RAY_CHECK(nodes_.find(local_node_id_) != nodes_.end());

  auto overflow = AddAvailableResourceInstances(
      gpu_instances_fp, &local_resources_.predefined_resources[GPU]);
  UpdateLocalAvailableResourcesFromResourceInstances();

  return VectorFixedPointToVectorDouble(overflow);
}

std::vector<double> ClusterResourceScheduler::SubtractGPUResourceInstances(
    std::vector<double> &gpu_instances) {
  std::vector<FixedPoint> gpu_instances_fp =
      VectorDoubleToVectorFixedPoint(gpu_instances);

  if (gpu_instances.size() == 0) {
    return gpu_instances;  // No underflow.
  }
  RAY_CHECK(nodes_.find(local_node_id_) != nodes_.end());

  auto underflow = SubtractAvailableResourceInstances(
      gpu_instances_fp, &local_resources_.predefined_resources[GPU]);
  UpdateLocalAvailableResourcesFromResourceInstances();

  return VectorFixedPointToVectorDouble(underflow);
}

bool ClusterResourceScheduler::AllocateLocalTaskResources(
    const ResourceRequest &resource_request,
    std::shared_ptr<TaskResourceInstances> task_allocation) {
  if (AllocateTaskResourceInstances(resource_request, task_allocation)) {
    UpdateLocalAvailableResourcesFromResourceInstances();
    return true;
  }
  return false;
}

bool ClusterResourceScheduler::AllocateLocalTaskResources(
    const std::unordered_map<std::string, double> &task_resources,
    std::shared_ptr<TaskResourceInstances> task_allocation) {
  RAY_CHECK(task_allocation != nullptr);
  // We don't track object store memory demands so no need to allocate them.
  ResourceRequest resource_request = ResourceMapToResourceRequest(
      string_to_int_map_, task_resources, /*requires_object_store_memory=*/false);
  return AllocateLocalTaskResources(resource_request, task_allocation);
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
    const std::unordered_map<std::string, double> &task_resources) {
  ResourceRequest resource_request = ResourceMapToResourceRequest(
      string_to_int_map_, task_resources, /*requires_object_store_memory=*/false);
  auto node_id = string_to_int_map_.Insert(node_string);
  RAY_CHECK(node_id != local_node_id_);
  return SubtractRemoteNodeAvailableResources(node_id, resource_request);
}

void ClusterResourceScheduler::ReleaseWorkerResources(
    std::shared_ptr<TaskResourceInstances> task_allocation) {
  if (task_allocation == nullptr || task_allocation->IsEmpty()) {
    return;
  }
  FreeTaskResourceInstances(task_allocation);
  UpdateLocalAvailableResourcesFromResourceInstances();
}

void ClusterResourceScheduler::UpdateLastResourceUsage(
    std::shared_ptr<SchedulingResources> gcs_resources) {
  NodeResources node_resources = ResourceMapToNodeResources(
      string_to_int_map_, gcs_resources->GetTotalResources().GetResourceMap(),
      gcs_resources->GetAvailableResources().GetResourceMap());
  last_report_resources_.reset(new NodeResources(node_resources));
}

void ClusterResourceScheduler::FillResourceUsage(rpc::ResourcesData &resources_data) {
  NodeResources resources;

  RAY_CHECK(GetNodeResources(local_node_id_, &resources))
      << "Error: Populating heartbeat failed. Please file a bug report: "
         "https://github.com/ray-project/ray/issues/new.";

  // Initialize if last report resources is empty.
  if (!last_report_resources_) {
    NodeResources node_resources =
        ResourceMapToNodeResources(string_to_int_map_, {{}}, {{}});
    last_report_resources_.reset(new NodeResources(node_resources));
  }

  // Reset all local views for remote nodes. This is needed in case tasks that
  // we spilled back to a remote node were not actually scheduled on the
  // node. Then, the remote node's resource availability may not change and
  // so it may not send us another update.
  for (auto &node : nodes_) {
    if (node.first != local_node_id_) {
      node.second.ResetLocalView();
    }
  }

  // Automatically report object store usage.
  // XXX: this MUTATES the resources field, which is needed since we are storing
  // it in last_report_resources_.
  if (get_used_object_store_memory_ != nullptr) {
    auto &capacity = resources.predefined_resources[OBJECT_STORE_MEM];
    double used = get_used_object_store_memory_();
    capacity.available = FixedPoint(capacity.total.Double() - used);
  }

  for (int i = 0; i < PredefinedResources_MAX; i++) {
    const auto &label = ResourceEnumToString((PredefinedResources)i);
    const auto &capacity = resources.predefined_resources[i];
    const auto &last_capacity = last_report_resources_->predefined_resources[i];
    // Note: available may be negative, but only report positive to GCS.
    if (capacity.available != last_capacity.available && capacity.available > 0) {
      resources_data.set_resources_available_changed(true);
      (*resources_data.mutable_resources_available())[label] =
          capacity.available.Double();
    }
    if (capacity.total != last_capacity.total) {
      (*resources_data.mutable_resources_total())[label] = capacity.total.Double();
    }
  }
  for (const auto &it : resources.custom_resources) {
    uint64_t custom_id = it.first;
    const auto &capacity = it.second;
    const auto &last_capacity = last_report_resources_->custom_resources[custom_id];
    const auto &label = string_to_int_map_.Get(custom_id);
    // Note: available may be negative, but only report positive to GCS.
    if (capacity.available != last_capacity.available && capacity.available > 0) {
      resources_data.set_resources_available_changed(true);
      (*resources_data.mutable_resources_available())[label] =
          capacity.available.Double();
    }
    if (capacity.total != last_capacity.total) {
      (*resources_data.mutable_resources_total())[label] = capacity.total.Double();
    }
  }

  if (get_pull_manager_at_capacity_ != nullptr) {
    resources.object_pulls_queued = get_pull_manager_at_capacity_();
    if (last_report_resources_->object_pulls_queued != resources.object_pulls_queued) {
      resources_data.set_object_pulls_queued(resources.object_pulls_queued);
      resources_data.set_resources_available_changed(true);
    }
  }

  if (resources != *last_report_resources_.get()) {
    last_report_resources_.reset(new NodeResources(resources));
  }

  if (!RayConfig::instance().enable_light_weight_resource_report()) {
    resources_data.set_resources_available_changed(true);
  }
}

double ClusterResourceScheduler::GetLocalAvailableCpus() const {
  NodeResources local_resources;
  RAY_CHECK(GetNodeResources(local_node_id_, &local_resources));
  auto &capacity = local_resources.predefined_resources[CPU];
  return capacity.available.Double();
}

ray::gcs::NodeResourceInfoAccessor::ResourceMap
ClusterResourceScheduler::GetResourceTotals() const {
  ray::gcs::NodeResourceInfoAccessor::ResourceMap map;
  auto it = nodes_.find(local_node_id_);
  RAY_CHECK(it != nodes_.end());
  const auto &local_resources = it->second.GetLocalView();
  for (size_t i = 0; i < local_resources.predefined_resources.size(); i++) {
    std::string resource_name = ResourceEnumToString(static_cast<PredefinedResources>(i));
    double resource_total = local_resources.predefined_resources[i].total.Double();
    if (resource_total > 0) {
      auto data = std::make_shared<rpc::ResourceTableData>();
      data->set_resource_capacity(resource_total);
      map.emplace(resource_name, std::move(data));
    }
  }

  for (auto entry : local_resources.custom_resources) {
    std::string resource_name = string_to_int_map_.Get(entry.first);
    double resource_total = entry.second.total.Double();
    if (resource_total > 0) {
      auto data = std::make_shared<rpc::ResourceTableData>();
      data->set_resource_capacity(resource_total);
      map.emplace(resource_name, std::move(data));
    }
  }
  return map;
}

}  // namespace ray
