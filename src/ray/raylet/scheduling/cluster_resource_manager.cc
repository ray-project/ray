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

ClusterResourceManager::ClusterResourceManager() : nodes_{} {}

void ClusterResourceManager::AddOrUpdateNode(
    scheduling::NodeID node_id,
    const absl::flat_hash_map<std::string, double> &resources_total,
    const absl::flat_hash_map<std::string, double> &resources_available) {
  NodeResources node_resources =
      ResourceMapToNodeResources(resources_total, resources_available);
  AddOrUpdateNode(node_id, node_resources);
}

void ClusterResourceManager::AddOrUpdateNode(scheduling::NodeID node_id,
                                             const NodeResources &node_resources) {
  RAY_LOG(DEBUG) << "Update node info, node_id: " << node_id.ToInt()
                 << ", node_resources: " << node_resources.DebugString();
  auto it = nodes_.find(node_id);
  if (it == nodes_.end()) {
    // This node is new, so add it to the map.
    nodes_.emplace(node_id, node_resources);
  } else {
    // This node exists, so update its resources.
    it->second = Node(node_resources);
  }
}

bool ClusterResourceManager::UpdateNode(scheduling::NodeID node_id,
                                        const rpc::ResourcesData &resource_data) {
  if (!nodes_.contains(node_id)) {
    return false;
  }

  auto resources_total = MapFromProtobuf(resource_data.resources_total());
  auto resources_available = MapFromProtobuf(resource_data.resources_available());
  NodeResources node_resources =
      ResourceMapToNodeResources(resources_total, resources_available);
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

bool ClusterResourceManager::RemoveNode(scheduling::NodeID node_id) {
  auto it = nodes_.find(node_id);
  if (it == nodes_.end()) {
    // Node not found.
    return false;
  } else {
    nodes_.erase(it);
    return true;
  }
}

bool ClusterResourceManager::GetNodeResources(scheduling::NodeID node_id,
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
    scheduling::NodeID node_id) const {
  const auto &node = map_find_or_die(nodes_, node_id);
  return node.GetLocalView();
}

int64_t ClusterResourceManager::NumNodes() const { return nodes_.size(); }

void ClusterResourceManager::UpdateResourceCapacity(scheduling::NodeID node_id,
                                                    scheduling::ResourceID resource_id,
                                                    double resource_total) {
  auto it = nodes_.find(node_id);
  if (it == nodes_.end()) {
    NodeResources node_resources;
    node_resources.predefined_resources.resize(PredefinedResources_MAX);
    it = nodes_.emplace(node_id, node_resources).first;
  }

  int idx = GetPredefinedResourceIndex(resource_id);

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
    auto itr = local_view->custom_resources.find(resource_id.ToInt());
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
      local_view->custom_resources.emplace(resource_id.ToInt(), resource_capacity);
    }
  }
}

void ClusterResourceManager::DeleteResource(scheduling::NodeID node_id,
                                            scheduling::ResourceID resource_id) {
  auto it = nodes_.find(node_id);
  if (it == nodes_.end()) {
    return;
  }

  int idx = GetPredefinedResourceIndex(resource_id);
  auto local_view = it->second.GetMutableLocalView();
  if (idx != -1) {
    local_view->predefined_resources[idx].available = 0;
    local_view->predefined_resources[idx].total = 0;

  } else {
    auto itr = local_view->custom_resources.find(resource_id.ToInt());
    if (itr != local_view->custom_resources.end()) {
      local_view->custom_resources.erase(itr);
    }
  }
}

std::string ClusterResourceManager::GetNodeResourceViewString(
    scheduling::NodeID node_id) const {
  const auto &node = map_find_or_die(nodes_, node_id);
  return node.GetLocalView().DictString();
}

std::string ClusterResourceManager::GetResourceNameFromIndex(int64_t res_idx) {
  return scheduling::ResourceID(res_idx).Binary();
}

const absl::flat_hash_map<scheduling::NodeID, Node>
    &ClusterResourceManager::GetResourceView() const {
  return nodes_;
}

bool ClusterResourceManager::SubtractNodeAvailableResources(
    scheduling::NodeID node_id, const ResourceRequest &resource_request) {
  auto it = nodes_.find(node_id);
  if (it == nodes_.end()) {
    return false;
  }

  NodeResources *resources = it->second.GetMutableLocalView();

  FixedPoint zero(0.);

  for (size_t i = 0; i < PredefinedResources_MAX; i++) {
    resources->predefined_resources[i].available =
        std::max(FixedPoint(0),
                 resources->predefined_resources[i].available -
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

bool ClusterResourceManager::HasSufficientResource(
    scheduling::NodeID node_id,
    const ResourceRequest &resource_request,
    bool ignore_object_store_memory_requirement) const {
  auto it = nodes_.find(node_id);
  if (it == nodes_.end()) {
    return false;
  }

  const NodeResources &resources = it->second.GetLocalView();

  if (!ignore_object_store_memory_requirement && resources.object_pulls_queued &&
      resource_request.requires_object_store_memory) {
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

bool ClusterResourceManager::AddNodeAvailableResources(
    scheduling::NodeID node_id, const ResourceRequest &resource_request) {
  auto it = nodes_.find(node_id);
  if (it == nodes_.end()) {
    return false;
  }

  auto node_resources = it->second.GetMutableLocalView();
  RAY_CHECK(resource_request.predefined_resources.size() <=
            node_resources->predefined_resources.size());

  for (size_t i = 0; i < resource_request.predefined_resources.size(); ++i) {
    node_resources->predefined_resources[i].available +=
        resource_request.predefined_resources[i];
    node_resources->predefined_resources[i].available =
        std::min(node_resources->predefined_resources[i].available,
                 node_resources->predefined_resources[i].total);
  }
  for (auto &entry : resource_request.custom_resources) {
    auto it = node_resources->custom_resources.find(entry.first);
    if (it != node_resources->custom_resources.end()) {
      it->second.available += entry.second;
      it->second.available = std::min(it->second.available, it->second.total);
    }
  }

  return true;
}

bool ClusterResourceManager::UpdateNodeAvailableResourcesIfExist(
    scheduling::NodeID node_id, const rpc::ResourcesData &resource_data) {
  auto iter = nodes_.find(node_id);
  if (iter == nodes_.end()) {
    return false;
  }

  if (!resource_data.resources_available_changed()) {
    return true;
  }

  auto resources =
      ResourceMapToResourceRequest(MapFromProtobuf(resource_data.resources_available()),
                                   /*requires_object_store_memory=*/false);
  auto node_resources = iter->second.GetMutableLocalView();
  for (size_t i = 0; i < node_resources->predefined_resources.size(); ++i) {
    node_resources->predefined_resources[i].available = resources.predefined_resources[i];
  }
  for (auto &entry : node_resources->custom_resources) {
    auto it = resources.custom_resources.find(entry.first);
    if (it != resources.custom_resources.end()) {
      entry.second.available = it->second;
    } else {
      entry.second.available = 0.;
    }
  }

  return true;
}

bool ClusterResourceManager::UpdateNodeNormalTaskResources(
    scheduling::NodeID node_id, const rpc::ResourcesData &resource_data) {
  auto iter = nodes_.find(node_id);
  if (iter != nodes_.end()) {
    auto node_resources = iter->second.GetMutableLocalView();
    if (resource_data.resources_normal_task_changed() &&
        resource_data.resources_normal_task_timestamp() >
            node_resources->latest_resources_normal_task_timestamp) {
      auto normal_task_resources = ResourceMapToResourceRequest(
          MapFromProtobuf(resource_data.resources_normal_task()),
          /*requires_object_store_memory=*/false);
      auto &local_normal_task_resources = node_resources->normal_task_resources;
      if (normal_task_resources != local_normal_task_resources) {
        local_normal_task_resources.predefined_resources.resize(PredefinedResources_MAX);
        for (size_t i = 0; i < PredefinedResources_MAX; ++i) {
          local_normal_task_resources.predefined_resources[i] =
              normal_task_resources.predefined_resources[i];
        }
        local_normal_task_resources.custom_resources =
            std::move(normal_task_resources.custom_resources);
        node_resources->latest_resources_normal_task_timestamp =
            resource_data.resources_normal_task_timestamp();
        return true;
      }
    }
  }

  return false;
}

bool ClusterResourceManager::ContainsNode(scheduling::NodeID node_id) const {
  return nodes_.contains(node_id);
}

void ClusterResourceManager::DebugString(std::stringstream &buffer) const {
  for (auto &node : GetResourceView()) {
    buffer << "node id: " << node.first.ToInt();
    buffer << node.second.GetLocalView().DebugString();
  }
}

}  // namespace ray
