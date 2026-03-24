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

#include <algorithm>
#include <cmath>
#include <string>
#include <utility>
#include <vector>

#include "ray/common/grpc_util.h"
#include "ray/common/ray_config.h"
#include "ray/raylet/metrics.h"

namespace ray {

ClusterResourceManager::ClusterResourceManager(instrumented_io_context &io_service)
    : timer_(PeriodicalRunner::Create(io_service)),
      local_resource_view_node_count_gauge_(
          raylet::GetLocalResourceViewNodeCountGaugeMetric()) {
  timer_->RunFnPeriodically(
      [this]() {
        auto syncer_delay = absl::Milliseconds(
            RayConfig::instance().ray_syncer_message_refresh_interval_ms());
        for (auto &[node_id, resource] : received_node_resources_) {
          auto modified_ts = GetNodeResourceModifiedTs(node_id);
          if (modified_ts && *modified_ts + syncer_delay < absl::Now()) {
            AddOrUpdateNode(node_id, resource);
          }
        }
      },
      RayConfig::instance().ray_syncer_message_refresh_interval_ms(),
      "ClusterResourceManager.ResetRemoteNodeView");
}

std::optional<absl::Time> ClusterResourceManager::GetNodeResourceModifiedTs(
    scheduling::NodeID node_id) const {
  auto iter = nodes_.find(node_id);
  if (iter == nodes_.end()) {
    return std::nullopt;
  }
  return iter->second.GetViewModifiedTs();
}

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
  auto it = nodes_.find(node_id);
  if (it == nodes_.end()) {
    // This node is new, so add it to the map.
    nodes_.emplace(node_id, node_resources);
  } else {
    // This node exists, so update its resources.
    it->second = Node(node_resources);
  }
}

bool ClusterResourceManager::UpdateNode(
    scheduling::NodeID node_id,
    const syncer::ResourceViewSyncMessage &resource_view_sync_message) {
  if (!nodes_.contains(node_id)) {
    return false;
  }

  const auto resources_total =
      MapFromProtobuf(resource_view_sync_message.resources_total());
  auto node_labels = MapFromProtobuf(resource_view_sync_message.labels());

  NodeResources local_view;
  RAY_CHECK(GetNodeResources(node_id, &local_view));

  local_view.total = NodeResourceSet(resources_total);

  const auto &instances_map = resource_view_sync_message.resources_available_instances();
  NodeResourceInstanceSet new_available;
  for (const auto &[resource_name, resource_instances] : instances_map) {
    std::vector<FixedPoint> instances;
    for (const auto &value : resource_instances.values()) {
      instances.push_back(FixedPoint(value));
    }
    new_available.Set(ResourceID(resource_name), std::move(instances));
  }
  local_view.available = std::move(new_available);

  local_view.labels = std::move(node_labels);
  local_view.object_pulls_queued = resource_view_sync_message.object_pulls_queued();

  // Update the idle duration for the node in terms of resources usage.
  local_view.idle_resource_duration_ms = resource_view_sync_message.idle_duration_ms();

  // Last update time to the local node resources view.
  local_view.last_resource_update_time = absl::Now();

  if (!local_view.is_draining) {
    local_view.is_draining = resource_view_sync_message.is_draining();
    local_view.draining_deadline_timestamp_ms =
        resource_view_sync_message.draining_deadline_timestamp_ms();
  }

  AddOrUpdateNode(node_id, local_view);
  received_node_resources_[node_id] = std::move(local_view);
  return true;
}

bool ClusterResourceManager::RemoveNode(scheduling::NodeID node_id) {
  received_node_resources_.erase(node_id);
  return nodes_.erase(node_id) != 0;
}

bool ClusterResourceManager::SetNodeDraining(const scheduling::NodeID &node_id,
                                             bool is_draining,
                                             int64_t draining_deadline_timestamp_ms) {
  auto it = nodes_.find(node_id);
  if (it == nodes_.end()) {
    return false;
  }

  auto *local_view = it->second.GetMutableLocalView();
  local_view->is_draining = is_draining;
  local_view->draining_deadline_timestamp_ms = draining_deadline_timestamp_ms;
  auto rnr_it = received_node_resources_.find(node_id);
  if (rnr_it != received_node_resources_.end()) {
    rnr_it->second.is_draining = is_draining;
    rnr_it->second.draining_deadline_timestamp_ms = draining_deadline_timestamp_ms;
  }

  return true;
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
    it = nodes_.emplace(node_id, NodeResources{}).first;
  }

  auto *local_view = it->second.GetMutableLocalView();
  FixedPoint new_total = std::max(FixedPoint(resource_total), FixedPoint(0));
  local_view->total.Set(resource_id, new_total);

  // Only init available for new resources. Existing resources' available can't be
  // correctly adjusted from a scalar total (don't know which instances to update).
  if (!local_view->available.Has(resource_id)) {
    local_view->available.Set(
        resource_id, NodeResourceInstanceSet::MakeInstances(resource_id, new_total));
  }
}

void ClusterResourceManager::AddResourceInstances(
    scheduling::NodeID node_id,
    scheduling::ResourceID resource_id,
    const std::vector<FixedPoint> &instances) {
  auto it = nodes_.find(node_id);
  RAY_CHECK(it != nodes_.end()) << "Node " << node_id.ToInt() << " not found.";

  auto *local_view = it->second.GetMutableLocalView();
  auto total_add = FixedPoint::Sum(instances);
  local_view->total.Set(resource_id, local_view->total.Get(resource_id) + total_add);
  local_view->available.Add(resource_id, instances);
}

bool ClusterResourceManager::DeleteResources(
    scheduling::NodeID node_id, const std::vector<scheduling::ResourceID> &resource_ids) {
  auto it = nodes_.find(node_id);
  if (it == nodes_.end()) {
    return false;
  }

  auto local_view = it->second.GetMutableLocalView();
  for (const auto &resource_id : resource_ids) {
    local_view->total.Set(resource_id, 0);
    local_view->available.Remove(resource_id);
  }
  return true;
}

std::string ClusterResourceManager::GetNodeResourceViewString(
    scheduling::NodeID node_id) const {
  const auto &node = map_find_or_die(nodes_, node_id);
  return node.GetLocalView().DictString();
}

const absl::flat_hash_map<scheduling::NodeID, Node>
    &ClusterResourceManager::GetResourceView() const {
  return nodes_;
}

std::optional<ResourceAllocation> ClusterResourceManager::SubtractNodeAvailableResources(
    scheduling::NodeID node_id, const ResourceRequest &resource_request) {
  auto it = nodes_.find(node_id);
  if (it == nodes_.end()) {
    return std::nullopt;
  }

  NodeResources *resources = it->second.GetMutableLocalView();

  // Use single-resource TryAllocate (not the multi-resource variant) because the
  // multi-resource version has PG cross-bundle logic that only applies to local
  // allocation, not speculative remote deduction.
  ResourceAllocation allocation;
  for (const auto &[resource_id, demand] :
       resource_request.GetResourceSet().Resources()) {
    auto alloc = resources->available.TryAllocate(resource_id, demand);
    if (!alloc.has_value()) {
      // Rollback already applied allocations.
      for (const auto &[rid, instances] : allocation) {
        resources->available.Free(rid, instances);
      }
      return std::nullopt;
    }
    allocation[resource_id] = std::move(*alloc);
  }

  // TODO(swang): We should also subtract object store memory if the task has
  // arguments. Right now we do not modify object_pulls_queued in case of
  // performance regressions in spillback.

  return allocation;
}

bool ClusterResourceManager::HasFeasibleResources(
    scheduling::NodeID node_id, const ResourceRequest &resource_request) const {
  auto it = nodes_.find(node_id);
  if (it == nodes_.end()) {
    return false;
  }

  return it->second.GetLocalView().IsFeasible(resource_request);
}

bool ClusterResourceManager::HasAvailableResources(
    scheduling::NodeID node_id,
    const ResourceRequest &resource_request,
    bool ignore_object_store_memory_requirement) const {
  auto it = nodes_.find(node_id);
  if (it == nodes_.end()) {
    return false;
  }

  return it->second.GetLocalView().IsAvailable(resource_request,
                                               ignore_object_store_memory_requirement);
}

bool ClusterResourceManager::AddNodeAvailableResources(
    scheduling::NodeID node_id, const ResourceAllocation &allocation) {
  auto it = nodes_.find(node_id);
  if (it == nodes_.end()) {
    return false;
  }

  auto node_resources = it->second.GetMutableLocalView();
  for (const auto &[resource_id, instances] : allocation) {
    node_resources->available.Free(resource_id, instances);
    // Cap at total to match master behavior. Only cap the freed resource,
    // not the entire node (avoids O(all_resources) on every rollback).
    node_resources->available.CapResourceAtTotal(resource_id, node_resources->total);
  }
  return true;
}

std::string ClusterResourceManager::DebugString(
    std::optional<size_t> max_num_nodes_to_include) const {
  std::stringstream buffer;
  size_t num_nodes_included = 0;
  for (auto &node : GetResourceView()) {
    if (max_num_nodes_to_include.has_value() &&
        num_nodes_included >= max_num_nodes_to_include.value()) {
      break;
    }
    buffer << "node id: " << node.first.ToInt();
    buffer << node.second.GetLocalView().DebugString();
    ++num_nodes_included;
  }
  buffer << " " << bundle_location_index_.DebugString();
  return buffer.str();
}

BundleLocationIndex &ClusterResourceManager::GetBundleLocationIndex() {
  return bundle_location_index_;
}

void ClusterResourceManager::SetNodeLabels(
    const scheduling::NodeID &node_id,
    absl::flat_hash_map<std::string, std::string> labels) {
  auto it = nodes_.find(node_id);
  if (it == nodes_.end()) {
    NodeResources node_resources;
    it = nodes_.emplace(node_id, node_resources).first;
  }
  it->second.GetMutableLocalView()->labels = std::move(labels);
}

void ClusterResourceManager::RecordMetrics() const {
  local_resource_view_node_count_gauge_.Record(static_cast<double>(nodes_.size()));
}

}  // namespace ray
