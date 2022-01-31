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

#include "ray/raylet/scheduling/cluster_resource_manager.h"

#include <boost/algorithm/string.hpp>

#include "ray/common/grpc_util.h"
#include "ray/common/ray_config.h"
#include "ray/util/container_util.h"

namespace ray {

ClusterResourceManager::ClusterResourceManager(StringIdMap &string_to_int_map)
    : nodes_{}, string_to_int_map_{string_to_int_map} {}

void ClusterResourceManager::AddOrUpdateNode(
    const std::string &node_id,
    const absl::flat_hash_map<std::string, double> &resources_total,
    const absl::flat_hash_map<std::string, double> &resources_available) {
  NodeResources node_resources = ResourceMapToNodeResources(
      string_to_int_map_, resources_total, resources_available);
  AddOrUpdateNode(string_to_int_map_.Insert(node_id), node_resources);
}

void ClusterResourceManager::AddOrUpdateNode(int64_t node_id,
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

bool ClusterResourceManager::UpdateNode(const std::string &node_id_string,
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

bool ClusterResourceManager::RemoveNode(int64_t node_id) {
  auto it = nodes_.find(node_id);
  if (it == nodes_.end()) {
    // Node not found.
    return false;
  } else {
    nodes_.erase(it);
    return true;
  }
}

bool ClusterResourceManager::RemoveNode(const std::string &node_id_string) {
  auto node_id = string_to_int_map_.Get(node_id_string);
  if (node_id == -1) {
    return false;
  }

  return RemoveNode(node_id);
}

bool ClusterResourceManager::GetNodeResources(int64_t node_id,
                                              NodeResources *ret_resources) const {
  auto it = nodes_.find(node_id);
  if (it != nodes_.end()) {
    *ret_resources = it->second.GetLocalView();
    return true;
  } else {
    return false;
  }
}

const NodeResources &ClusterResourceManager::GetNodeResources(
    const std::string &node_name) const {
  int64_t node_id = string_to_int_map_.Get(node_name);
  const auto &node = map_find_or_die(nodes_, node_id);
  return node.GetLocalView();
}

int64_t ClusterResourceManager::NumNodes() const { return nodes_.size(); }

void ClusterResourceManager::UpdateResourceCapacity(const std::string &node_id_string,
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

  int idx = GetPredefinedResourceIndex(resource_name);

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

void ClusterResourceManager::DeleteResource(const std::string &node_id_string,
                                            const std::string &resource_name) {
  int64_t node_id = string_to_int_map_.Get(node_id_string);

  auto it = nodes_.find(node_id);
  if (it == nodes_.end()) {
    return;
  }

  int idx = GetPredefinedResourceIndex(resource_name);
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

std::string ClusterResourceManager::GetNodeResourceViewString(
    const std::string &node_name) const {
  int64_t node_id = string_to_int_map_.Get(node_name);
  const auto &node = map_find_or_die(nodes_, node_id);
  return node.GetLocalView().DictString(string_to_int_map_);
}

std::string ClusterResourceManager::GetResourceNameFromIndex(int64_t res_idx) {
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

const absl::flat_hash_map<int64_t, Node> &ClusterResourceManager::GetResourceView()
    const {
  return nodes_;
}

bool ClusterResourceManager::SubtractNodeAvailableResources(
    int64_t node_id, const ResourceRequest &resource_request) {
  auto it = nodes_.find(node_id);
  if (it == nodes_.end()) {
    return false;
  }

  NodeResources *resources = it->second.GetMutableLocalView();

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
}  // namespace ray
