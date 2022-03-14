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

std::string UnorderedMapToString(const absl::flat_hash_map<std::string, double> &map) {
  std::stringstream buffer;

  buffer << "[";
  for (auto it = map.begin(); it != map.end(); ++it) {
    buffer << "(" << it->first << ":" << it->second << ")";
  }
  buffer << "]";
  return buffer.str();
}

/// Convert a map of resources to a ResourceRequest data structure.
ResourceRequest ResourceMapToResourceRequest(
    const absl::flat_hash_map<std::string, double> &resource_map,
    bool requires_object_store_memory) {
  return ResourceMapToResourceRequest(resource_map, requires_object_store_memory);
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
    const auto &available = this->available.Get(ResourceID(i));

    float utilization = 1 - (available.Double() / total.Double());
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

  return resource_request <= this->available;
}

bool NodeResources::IsFeasible(const ResourceRequest &resource_request) const {
  return resource_request <= this->total;
}

bool NodeResources::operator==(const NodeResources &other) {
  return this->available == other.available && this->total == other.total;
}

bool NodeResources::operator!=(const NodeResources &other) { return !(*this == other); }

std::string NodeResources::DebugString() const {
  std::stringstream buffer;
  buffer << "{available: " << this->available.DebugString() << ",total: " << this->total.DebugString() + "}";
  return buffer.str();
}

std::string NodeResources::DictString() const {
  return DebugString();
  // std::stringstream buffer;
  // bool first = true;
  // buffer << "{";
  // for (size_t i = 0; i < this->predefined_resources.size(); i++) {
  //   if (this->predefined_resources[i].total <= 0) {
  //     continue;
  //   }
  //   if (first) {
  //     first = false;
  //   } else {
  //     buffer << ", ";
  //   }
  //   std::string name = "";
  //   switch (i) {
  //   case CPU:
  //     name = "CPU";
  //     break;
  //   case MEM:
  //     name = "memory";
  //     break;
  //   case GPU:
  //     name = "GPU";
  //     break;
  //   case OBJECT_STORE_MEM:
  //     name = "object_store_memory";
  //     break;
  //   default:
  //     RAY_CHECK(false) << "This should never happen.";
  //     break;
  //   }
  //   buffer << format_resource(name, this->predefined_resources[i].available.Double())
  //          << "/";
  //   buffer << format_resource(name, this->predefined_resources[i].total.Double());
  //   buffer << " " << name;
  // }
  // for (auto it = this->custom_resources.begin(); it != this->custom_resources.end();
  //      ++it) {
  //   auto name = ResourceID(it->first).Binary();
  //   buffer << ", " << format_resource(name, it->second.available.Double()) << "/"
  //          << format_resource(name, it->second.total.Double());
  //   buffer << " " << name;
  // }
  // buffer << "}" << std::endl;
  // return buffer.str();
}

bool NodeResourceInstances::operator==(const NodeResourceInstances &other) {
  return this->total == other.total && this->available == other.available;
}

std::string NodeResourceInstances::DebugString() const {
  std::stringstream buffer;
  // buffer << "{\n";
  // for (size_t i = 0; i < this->predefined_resources.size(); i++) {
  //   buffer << "\t";
  //   switch (i) {
  //   case CPU:
  //     buffer << "CPU: ";
  //     break;
  //   case MEM:
  //     buffer << "MEM: ";
  //     break;
  //   case GPU:
  //     buffer << "GPU: ";
  //     break;
  //   case OBJECT_STORE_MEM:
  //     buffer << "OBJECT_STORE_MEM: ";
  //     break;
  //   default:
  //     RAY_CHECK(false) << "This should never happen.";
  //     break;
  //   }
  //   buffer << "(" << VectorToString(predefined_resources[i].total) << ":"
  //          << VectorToString(this->predefined_resources[i].available) << ")\n";
  // }
  // for (auto it = this->custom_resources.begin(); it != this->custom_resources.end();
  //      ++it) {
  //   buffer << "\t" << ResourceID(it->first).Binary() << ":("
  //          << VectorToString(it->second.total) << ":"
  //          << VectorToString(it->second.available) << ")\n";
  // }
  // buffer << "}" << std::endl;
  return buffer.str();
};

TaskResourceInstances NodeResourceInstances::GetAvailableResourceInstances() {
  return this->available;
};

bool NodeResourceInstances::Contains(scheduling::ResourceID id) const {
  return total.Has(id);
}

}  // namespace ray
