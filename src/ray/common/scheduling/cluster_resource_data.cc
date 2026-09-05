// Copyright 2020-2021 The Ray Authors.
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

#include "ray/common/scheduling/cluster_resource_data.h"

#include <algorithm>
#include <set>
#include <string>
#include <utility>

namespace ray {

/// Convert a map of resources to a ResourceRequest data structure.
ResourceRequest ResourceMapToResourceRequest(
    const absl::flat_hash_map<std::string, double> &resource_map,
    bool requires_object_store_memory) {
  ResourceRequest res({}, requires_object_store_memory);
  for (const auto &entry : resource_map) {
    res.Set(ResourceID(entry.first), FixedPoint(entry.second));
  }
  return res;
}

/// Convert a map of resources to a ResourceRequest data structure.
ResourceRequest ResourceMapToResourceRequest(
    const absl::flat_hash_map<ResourceID, double> &resource_map,
    bool requires_object_store_memory) {
  ResourceRequest res({}, requires_object_store_memory);
  for (auto entry : resource_map) {
    res.Set(entry.first, FixedPoint(entry.second));
  }
  return res;
}

/// Convert a map of resources to a NodeResources data structure.
///
/// \param resource_map_total: Total capacities of resources we want to convert.
/// \param resource_map_available: Available capacities of resources we want to convert.
/// \param node_labels: Labels for the node.
///
/// \return Conversion result to a NodeResources data structure.
NodeResources ResourceMapToNodeResources(
    const absl::flat_hash_map<std::string, double> &resource_map_total,
    const absl::flat_hash_map<std::string, double> &resource_map_available,
    const absl::flat_hash_map<std::string, std::string> &node_labels) {
  NodeResources node_resources;
  node_resources.total = NodeResourceSet(resource_map_total);
  node_resources.SetAvailable(NodeResourceSet(resource_map_available));
  node_resources.labels = node_labels;
  return node_resources;
}

float NodeResources::CalculateCriticalResourceUtilization() const {
  float highest = 0;
  for (const auto &i : {CPU, MEM, OBJECT_STORE_MEM}) {
    const auto &cur_total = this->total.Get(ResourceID(i));
    if (cur_total == 0) {
      continue;
    }
    auto cur_available = this->available.Sum(ResourceID(i)).Double();
    float utilization = 1 - (cur_available / cur_total.Double());
    if (utilization > highest) {
      highest = utilization;
    }
  }
  return highest;
}

bool NodeResources::IsAvailable(const ResourceRequest &resource_request,
                                bool ignore_pull_manager_at_capacity) const {
  if (!ignore_pull_manager_at_capacity && resource_request.RequiresObjectStoreMemory() &&
      object_pulls_queued) {
    RAY_LOG(DEBUG) << "At pull manager capacity";
    return false;
  }

  const auto &label_selector = resource_request.GetLabelSelector();
  if (!HasRequiredLabels(label_selector)) {
    return false;
  }

  return this->available.CanAllocate(resource_request.GetResourceSet());
}

bool NodeResources::IsFeasible(const ResourceRequest &resource_request) const {
  const auto &label_selector = resource_request.GetLabelSelector();
  if (!HasRequiredLabels(label_selector)) {
    return false;
  }
  return this->total >= resource_request.GetResourceSet();
}

bool NodeResources::HasRequiredLabels(const LabelSelector &label_selector) const {
  // Check if node labels satisfy all label constraints
  const auto &constraints = label_selector.GetConstraints();
  for (const auto &constraint : constraints) {
    if (!NodeLabelMatchesConstraint(constraint)) {
      return false;
    }
  }

  return true;
}

bool NodeResources::NodeLabelMatchesConstraint(const LabelConstraint &constraint) const {
  const auto &key = constraint.GetLabelKey();
  const auto &match_operator = constraint.GetOperator();
  const auto &values = constraint.GetLabelValues();

  const auto &node_labels = this->labels;
  if (match_operator == LabelSelectorOperator::LABEL_IN) {
    // Check for equals or in() labels
    if (node_labels.contains(key) && values.contains(node_labels.at(key))) {
      return true;
    }
  } else if (match_operator == LabelSelectorOperator::LABEL_NOT_IN) {
    // Check for not equals (!) or not in (!in()) labels
    if (!(node_labels.contains(key) && values.contains(node_labels.at(key)))) {
      return true;
    }
  } else {
    RAY_CHECK(false)
        << "Node label constraint operator type must be one of equals, not equals (!), "
           "in, or not in (!in)";
  }
  return false;
}

bool NodeResources::operator==(const NodeResources &other) const {
  return this->available == other.available && this->total == other.total &&
         this->labels == other.labels;
}

bool NodeResources::operator!=(const NodeResources &other) const {
  return !(*this == other);
}

std::string NodeResources::DebugString() const {
  std::stringstream buffer;
  buffer << "{\"total\":" << total.DebugString();
  buffer << ", \"available\": " << available.DebugString();
  buffer << ", \"labels\":{";
  bool first = true;
  for (const auto &[key, value] : labels) {
    if (!first) {
      buffer << ",";
    }
    first = false;
    buffer << "\"" << key << "\":\"" << value << "\"";
  }
  buffer << "}, \"is_draining\": " << is_draining;
  buffer << ", \"draining_deadline_timestamp_ms\": " << draining_deadline_timestamp_ms
         << "}";
  return buffer.str();
}

std::string NodeResources::DictString() const { return DebugString(); }

FixedPoint NodeResources::GetAvailableSum(scheduling::ResourceID resource_id) const {
  return available.Sum(resource_id);
}

std::set<scheduling::ResourceID> NodeResources::GetAvailableResourceIds() const {
  std::set<scheduling::ResourceID> ids;
  for (const auto &[id, _] : available.Resources()) {
    ids.insert(id);
  }
  return ids;
}

void NodeResources::SubtractAvailableAndRemoveNegative(const ResourceSet &resource_set) {
  for (const auto &[resource_id, demand] : resource_set.Resources()) {
    if (available.Has(resource_id)) {
      if (available.Sum(resource_id) <= demand) {
        available.Remove(resource_id);
      } else {
        available.Subtract(resource_id, {demand}, /*allow_going_negative=*/false);
      }
    }
  }
}

void NodeResources::SetAvailableResource(scheduling::ResourceID resource_id,
                                         FixedPoint value) {
  available.Set(resource_id, {value});
}

void NodeResources::SetAvailableInstances(scheduling::ResourceID resource_id,
                                          std::vector<FixedPoint> instances) {
  available.Set(resource_id, std::move(instances));
}

void NodeResources::AddAvailableInstances(scheduling::ResourceID resource_id,
                                          const std::vector<FixedPoint> &instances) {
  available.Add(resource_id, instances);
}

void NodeResources::RemoveAvailableResource(scheduling::ResourceID resource_id) {
  available.Remove(resource_id);
}

bool NodeResources::HasAvailableResource(scheduling::ResourceID resource_id) const {
  return available.Has(resource_id);
}

std::optional<std::vector<FixedPoint>> NodeResources::TryAllocateAvailable(
    scheduling::ResourceID resource_id, FixedPoint demand) {
  return available.TryAllocate(resource_id, demand);
}

void NodeResources::FreeAvailableInstances(scheduling::ResourceID resource_id,
                                           const std::vector<FixedPoint> &instances) {
  available.Free(resource_id, instances);
}

void NodeResources::SetAvailable(NodeResourceSet resource_set) {
  available = NodeResourceInstanceSet(resource_set);
}

void NodeResources::SetAvailable(NodeResourceInstanceSet instances) {
  available = std::move(instances);
}

absl::flat_hash_map<std::string, double> NodeResources::GetAvailableResourceMap() const {
  return available.ToNodeResourceSet().GetResourceMap();
}

NodeResourceSet NodeResources::GetAvailable() const {
  return available.ToNodeResourceSet();
}

const NodeResourceInstanceSet &NodeResources::GetAvailableInstances() const {
  return available;
}

bool NodeResourceInstances::operator==(const NodeResourceInstances &other) const {
  return this->total == other.total && this->available == other.available;
}

std::string NodeResourceInstances::DebugString() const {
  std::stringstream buffer;
  buffer << "{\"total\":" << total.DebugString();
  buffer << ", \"available\": " << available.DebugString();
  buffer << ", \"labels\":{";
  bool first = true;
  for (const auto &[key, value] : labels) {
    if (!first) {
      buffer << ",";
    }
    first = false;
    buffer << "\"" << key << "\":\"" << value << "\"";
  }
  buffer << "}}";
  return buffer.str();
};

const NodeResourceInstanceSet &NodeResourceInstances::GetAvailableResourceInstances()
    const {
  return this->available;
};

const NodeResourceInstanceSet &NodeResourceInstances::GetTotalResourceInstances() const {
  return this->total;
};

}  // namespace ray
