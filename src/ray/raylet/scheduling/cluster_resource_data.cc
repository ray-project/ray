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

std::string VectorToString(const std::vector<FixedPoint> &vector) {
  std::stringstream buffer;

  buffer << "[";
  for (size_t i = 0; i < vector.size(); i++) {
    buffer << vector[i];
    if (i < vector.size() - 1) {
      buffer << ", ";
    }
  }
  buffer << "]";
  return buffer.str();
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

/// Convert a vector of doubles to a vector of resource units.
std::vector<FixedPoint> VectorDoubleToVectorFixedPoint(
    const std::vector<double> &vector) {
  std::vector<FixedPoint> vector_fp(vector.size());
  for (size_t i = 0; i < vector.size(); i++) {
    vector_fp[i] = vector[i];
  }
  return vector_fp;
}

/// Convert a vector of resource units to a vector of doubles.
std::vector<double> VectorFixedPointToVectorDouble(
    const std::vector<FixedPoint> &vector_fp) {
  std::vector<double> vector(vector_fp.size());
  for (size_t i = 0; i < vector_fp.size(); i++) {
    vector[i] = FixedPoint(vector_fp[i]).Double();
  }
  return vector;
}

/// Convert a map of resources to a ResourceRequest data structure.
ResourceRequest ResourceMapToResourceRequest(
    const absl::flat_hash_map<std::string, double> &resource_map,
    bool requires_object_store_memory) {
  ResourceRequest resource_request;
  resource_request.requires_object_store_memory = requires_object_store_memory;
  for (auto const &resource : resource_map) {
      resource_request.Set(ResourceID(resource.first), resource.second);
  }
  return resource_request;
}

ResourceRequest TaskResourceInstances::ToResourceRequest() const {
  ResourceRequest resource_request;

  for (size_t i = 0; i < PredefinedResourcesEnum_MAX; i++) {
    resource_request.Set(ResourceID(i), FixedPoint::Sum(this->predefined_resources[i]));
  }

  for (auto it = this->custom_resources.begin(); it != this->custom_resources.end();
       ++it) {
    resource_request.Set(ResourceID(it->first), FixedPoint::Sum(it->second));
  }
  return resource_request;
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
  if (!ignore_pull_manager_at_capacity && resource_request.requires_object_store_memory &&
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

void TaskResourceInstances::ClearCPUInstances() {
  if (predefined_resources.size() >= CPU) {
    predefined_resources[CPU].clear();
    predefined_resources[CPU].clear();
  }
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

bool ResourceRequest::IsEmpty() const {
  return this->predefined_resources.IsEmpty() && this->custom_resources.IsEmpty();
}

std::string ResourceRequest::DebugString() const {
  return this->predefined_resources.DebugString() + ", " + this->custom_resources.DebugString();
}

bool TaskResourceInstances::IsEmpty() const {
  // Check whether all resource instances of a task are zero.
  for (const auto &predefined_resource : predefined_resources) {
    for (const auto &predefined_resource_instance : predefined_resource) {
      if (predefined_resource_instance != 0) {
        return false;
      }
    }
  }

  for (const auto &custom_resource : custom_resources) {
    for (const auto &custom_resource_instances : custom_resource.second) {
      if (custom_resource_instances != 0) {
        return false;
      }
    }
  }
  return true;
}

std::string TaskResourceInstances::DebugString() const {
  std::stringstream buffer;
  buffer << std::endl << "  Allocation: {";
  for (size_t i = 0; i < this->predefined_resources.size(); i++) {
    buffer << VectorToString(this->predefined_resources[i]);
  }
  buffer << "}";

  buffer << "  [";
  for (auto it = this->custom_resources.begin(); it != this->custom_resources.end();
       ++it) {
    buffer << ResourceID(it->first).Binary() << ":" << VectorToString(it->second) << ", ";
  }

  buffer << "]" << std::endl;
  return buffer.str();
}

bool EqualVectors(const std::vector<FixedPoint> &v1, const std::vector<FixedPoint> &v2) {
  return (v1.size() == v2.size() && std::equal(v1.begin(), v1.end(), v2.begin()));
}

bool TaskResourceInstances::operator==(const TaskResourceInstances &other) {
  for (size_t i = 0; i < PredefinedResourcesEnum_MAX; i++) {
    if (!EqualVectors(this->predefined_resources[i], other.predefined_resources[i])) {
      return false;
    }
  }

  if (this->custom_resources.size() != other.custom_resources.size()) {
    return false;
  }

  for (auto it1 = this->custom_resources.begin(); it1 != this->custom_resources.end();
       ++it1) {
    auto it2 = other.custom_resources.find(it1->first);
    if (it2 == other.custom_resources.end()) {
      return false;
    }
    if (!EqualVectors(it1->second, it2->second)) {
      return false;
    }
  }
  return true;
}

}  // namespace ray
