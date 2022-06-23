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

#include "ray/raylet/scheduling/cluster_resource_data.h"

#include "ray/common/bundle_spec.h"
#include "ray/common/task/scheduling_resources.h"

namespace ray {
using namespace ::ray::scheduling;

bool IsPredefinedResource(scheduling::ResourceID resource) {
  return resource.ToInt() >= 0 && resource.ToInt() < PredefinedResourcesEnum_MAX;
}

/// Convert a map of resources to a ResourceRequest data structure.
ResourceRequest ResourceMapToResourceRequest(
    const absl::flat_hash_map<std::string, double> &resource_map,
    bool requires_object_store_memory) {
  ResourceRequest res({}, requires_object_store_memory);
  for (auto entry : resource_map) {
    res.Set(ResourceID(entry.first), FixedPoint(entry.second));
  }
  return res;
}

/// Convert a map of resources to a ResourceRequest data structure.
///
/// \param string_to_int_map: Map between names and ids maintained by the
/// \param resource_map_total: Total capacities of resources we want to convert.
/// \param resource_map_available: Available capacities of resources we want to convert.
///
/// \request Conversion result to a ResourceRequest data structure.
NodeResources ResourceMapToNodeResources(
    const absl::flat_hash_map<std::string, double> &resource_map_total,
    const absl::flat_hash_map<std::string, double> &resource_map_available) {
  NodeResources node_resources;
  node_resources.total = ResourceMapToResourceRequest(resource_map_total, false);
  node_resources.available = ResourceMapToResourceRequest(resource_map_available, false);
  return node_resources;
}

float NodeResources::CalculateCriticalResourceUtilization() const {
  float highest = 0;
  for (const auto &i : {CPU, MEM, OBJECT_STORE_MEM}) {
    const auto &total = this->total.Get(ResourceID(i));
    if (total == 0) {
      continue;
    }
    auto available = this->available.Get(ResourceID(i)).Double();
    // Gcs scheduler handles the `normal_task_resources` specifically. So when calculating
    // the available resources, we have to take one more step to take that into account.
    // For raylet scheduling, the `normal_task_resources` is always empty.
    if (this->normal_task_resources.Has(ResourceID(i))) {
      available -= this->normal_task_resources.Get(ResourceID(i)).Double();
      if (available < 0) {
        available = 0;
      }
    }
    float utilization = 1 - (available / total.Double());
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

  if (!this->normal_task_resources.IsEmpty()) {
    auto available_resources = this->available;
    available_resources -= this->normal_task_resources;
    return resource_request <= available_resources;
  }
  return resource_request <= this->available;
}

bool NodeResources::IsFeasible(const ResourceRequest &resource_request) const {
  return resource_request <= this->total;
}

bool NodeResources::operator==(const NodeResources &other) const {
  return this->available == other.available && this->total == other.total;
}

bool NodeResources::operator!=(const NodeResources &other) const {
  return !(*this == other);
}

std::string NodeResources::DebugString() const {
  std::stringstream buffer;
  buffer << "{";
  bool first = true;
  for (auto &resource_id : total.ResourceIds()) {
    if (!first) {
      buffer << ", ";
    }
    first = false;
    buffer << resource_id.Binary() << ": " << available.Get(resource_id) << "/"
           << total.Get(resource_id);
  }
  buffer << "}";
  return buffer.str();
}

std::string NodeResources::DictString() const { return DebugString(); }

bool NodeResourceInstances::operator==(const NodeResourceInstances &other) {
  return this->total == other.total && this->available == other.available;
}

std::string NodeResourceInstances::DebugString() const {
  std::stringstream buffer;
  buffer << "{";
  bool first = true;
  for (auto &resource_id : total.ResourceIds()) {
    if (!first) {
      buffer << ", ";
    }
    first = false;
    buffer << resource_id.Binary() << ": "
           << FixedPointVectorToString(available.Get(resource_id)) << "/"
           << FixedPointVectorToString(total.Get(resource_id));
  }
  buffer << "}";
  return buffer.str();
};

TaskResourceInstances NodeResourceInstances::GetAvailableResourceInstances() {
  return this->available;
};

bool NodeResourceInstances::Contains(scheduling::ResourceID id) const {
  return total.Has(id);
}

}  // namespace ray
