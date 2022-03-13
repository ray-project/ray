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

const std::string resource_labels[] = {ray::kCPU_ResourceLabel,
                                       ray::kMemory_ResourceLabel,
                                       ray::kGPU_ResourceLabel,
                                       ray::kObjectStoreMemory_ResourceLabel};

const std::string ResourceEnumToString(PredefinedResources resource) {
  // TODO (Alex): We should replace this with a protobuf enum.
  RAY_CHECK(resource < PredefinedResources_MAX)
      << "Something went wrong. Please file a bug report with this stack "
         "trace: https://github.com/ray-project/ray/issues/new.";
  std::string label = resource_labels[resource];
  return label;
}

const PredefinedResources ResourceStringToEnum(const std::string &resource) {
  for (std::size_t i = 0; i < resource_labels->size(); i++) {
    if (resource_labels[i] == resource) {
      return static_cast<PredefinedResources>(i);
    }
  }
  // The resource is invalid.
  return PredefinedResources_MAX;
}

bool IsPredefinedResource(scheduling::ResourceID resource) {
  return resource.ToInt() >= 0 && resource.ToInt() < PredefinedResources_MAX;
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
  resource_request.predefined_resources.resize(PredefinedResources_MAX);

  for (auto const &resource : resource_map) {
    if (resource.first == ray::kCPU_ResourceLabel) {
      resource_request.predefined_resources[CPU] = resource.second;
    } else if (resource.first == ray::kGPU_ResourceLabel) {
      resource_request.predefined_resources[GPU] = resource.second;
    } else if (resource.first == ray::kObjectStoreMemory_ResourceLabel) {
      resource_request.predefined_resources[OBJECT_STORE_MEM] = resource.second;
    } else if (resource.first == ray::kMemory_ResourceLabel) {
      resource_request.predefined_resources[MEM] = resource.second;
    } else {
      resource_request.custom_resources[ResourceID(resource.first).ToInt()] =
          resource.second;
    }
  }

  return resource_request;
}

const std::vector<FixedPoint> &TaskResourceInstances::Get(
    const std::string &resource_name) const {
  if (ray::kCPU_ResourceLabel == resource_name) {
    return predefined_resources[CPU];
  } else if (ray::kGPU_ResourceLabel == resource_name) {
    return predefined_resources[GPU];
  } else if (ray::kObjectStoreMemory_ResourceLabel == resource_name) {
    return predefined_resources[OBJECT_STORE_MEM];
  } else if (ray::kMemory_ResourceLabel == resource_name) {
    return predefined_resources[MEM];
  } else {
    auto it = custom_resources.find(ResourceID(resource_name).ToInt());
    RAY_CHECK(it != custom_resources.end());
    return it->second;
  }
}

ResourceRequest TaskResourceInstances::ToResourceRequest() const {
  ResourceRequest resource_request;
  resource_request.predefined_resources.resize(PredefinedResources_MAX);

  for (size_t i = 0; i < PredefinedResources_MAX; i++) {
    resource_request.predefined_resources[i] = 0;
    for (auto predefined_resource_instance : this->predefined_resources[i]) {
      resource_request.predefined_resources[i] += predefined_resource_instance;
    }
  }

  for (auto it = this->custom_resources.begin(); it != this->custom_resources.end();
       ++it) {
    resource_request.custom_resources[it->first] = 0;
    for (size_t j = 0; j < it->second.size(); j++) {
      resource_request.custom_resources[it->first] += it->second[j];
    }
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
  node_resources.predefined_resources.resize(PredefinedResources_MAX);
  for (size_t i = 0; i < PredefinedResources_MAX; i++) {
    node_resources.predefined_resources[i].total =
        node_resources.predefined_resources[i].available = 0;
  }

  for (auto const &resource : resource_map_total) {
    ResourceCapacity resource_capacity;
    resource_capacity.total = resource.second;
    auto it = resource_map_available.find(resource.first);
    if (it == resource_map_available.end()) {
      resource_capacity.available = 0;
    } else {
      resource_capacity.available = it->second;
    }
    if (resource.first == ray::kCPU_ResourceLabel) {
      node_resources.predefined_resources[CPU] = resource_capacity;
    } else if (resource.first == ray::kGPU_ResourceLabel) {
      node_resources.predefined_resources[GPU] = resource_capacity;
    } else if (resource.first == ray::kObjectStoreMemory_ResourceLabel) {
      node_resources.predefined_resources[OBJECT_STORE_MEM] = resource_capacity;
    } else if (resource.first == ray::kMemory_ResourceLabel) {
      node_resources.predefined_resources[MEM] = resource_capacity;
    } else {
      // This is a custom resource.
      node_resources.custom_resources.emplace(ResourceID(resource.first).ToInt(),
                                              resource_capacity);
    }
  }
  return node_resources;
}

bool ResourceRequest::IsGPURequest() const {
  if (predefined_resources.size() <= GPU) {
    return false;
  }
  return predefined_resources[GPU] > 0;
}

float NodeResources::CalculateCriticalResourceUtilization() const {
  float highest = 0;
  for (const auto &i : {CPU, MEM, OBJECT_STORE_MEM}) {
    if (i >= this->predefined_resources.size()) {
      continue;
    }
    const auto &capacity = this->predefined_resources[i];
    if (capacity.total == 0) {
      continue;
    }

    float utilization = 1 - (capacity.available.Double() / capacity.total.Double());
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

  if (resource_request.IsEmpty()) {
    return true;
  }

  // First, check predefined resources.
  for (size_t i = 0; i < PredefinedResources_MAX; i++) {
    if (i >= this->predefined_resources.size()) {
      if (resource_request.predefined_resources[i] != 0) {
        return false;
      }
      continue;
    }

    const auto &resource = this->predefined_resources[i].available;
    const auto &demand = resource_request.predefined_resources[i];

    if (resource < demand) {
      RAY_LOG(DEBUG) << "At resource capacity";
      return false;
    }
  }

  // Now check custom resources.
  for (const auto &resource_req_custom_resource : resource_request.custom_resources) {
    auto it = this->custom_resources.find(resource_req_custom_resource.first);
    if (it == this->custom_resources.end()) {
      return false;
    } else if (resource_req_custom_resource.second > it->second.available) {
      return false;
    }
  }

  return true;
}

bool NodeResources::IsFeasible(const ResourceRequest &resource_request) const {
  if (resource_request.IsEmpty()) {
    return true;
  }
  // First, check predefined resources.
  for (size_t i = 0; i < PredefinedResources_MAX; i++) {
    if (i >= this->predefined_resources.size()) {
      if (resource_request.predefined_resources[i] != 0) {
        return false;
      }
      continue;
    }
    const auto &resource = this->predefined_resources[i].total;
    const auto &demand = resource_request.predefined_resources[i];

    if (resource < demand) {
      return false;
    }
  }

  // Now check custom resources.
  for (const auto &resource_req_custom_resource : resource_request.custom_resources) {
    auto it = this->custom_resources.find(resource_req_custom_resource.first);
    if (it == this->custom_resources.end()) {
      return false;
    } else if (resource_req_custom_resource.second > it->second.total) {
      return false;
    }
  }
  return true;
}

bool NodeResources::operator==(const NodeResources &other) const {
  for (size_t i = 0; i < PredefinedResources_MAX; i++) {
    if (this->predefined_resources[i].total != other.predefined_resources[i].total) {
      return false;
    }
    if (this->predefined_resources[i].available !=
        other.predefined_resources[i].available) {
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
    if (it1->second.total != it2->second.total) {
      return false;
    }
    if (it1->second.available != it2->second.available) {
      return false;
    }
  }
  return true;
}

bool NodeResources::operator!=(const NodeResources &other) const {
  return !(*this == other);
}

std::string NodeResources::DebugString() const {
  std::stringstream buffer;
  buffer << " {\n";
  for (size_t i = 0; i < this->predefined_resources.size(); i++) {
    buffer << "\t";
    switch (i) {
    case CPU:
      buffer << "CPU: ";
      break;
    case MEM:
      buffer << "MEM: ";
      break;
    case GPU:
      buffer << "GPU: ";
      break;
    case OBJECT_STORE_MEM:
      buffer << "OBJECT_STORE_MEM: ";
      break;
    default:
      RAY_CHECK(false) << "This should never happen.";
      break;
    }
    buffer << "(" << this->predefined_resources[i].total << ":"
           << this->predefined_resources[i].available << ")\n";
  }
  for (auto it = this->custom_resources.begin(); it != this->custom_resources.end();
       ++it) {
    buffer << "\t" << ResourceID(it->first).Binary() << ":(" << it->second.total << ":"
           << it->second.available << ")\n";
  }
  buffer << "}" << std::endl;
  return buffer.str();
}

std::string NodeResources::DictString() const {
  std::stringstream buffer;
  bool first = true;
  buffer << "{";
  for (size_t i = 0; i < this->predefined_resources.size(); i++) {
    if (this->predefined_resources[i].total <= 0) {
      continue;
    }
    if (first) {
      first = false;
    } else {
      buffer << ", ";
    }
    std::string name = "";
    switch (i) {
    case CPU:
      name = "CPU";
      break;
    case MEM:
      name = "memory";
      break;
    case GPU:
      name = "GPU";
      break;
    case OBJECT_STORE_MEM:
      name = "object_store_memory";
      break;
    default:
      RAY_CHECK(false) << "This should never happen.";
      break;
    }
    buffer << format_resource(name, this->predefined_resources[i].available.Double())
           << "/";
    buffer << format_resource(name, this->predefined_resources[i].total.Double());
    buffer << " " << name;
  }
  for (auto it = this->custom_resources.begin(); it != this->custom_resources.end();
       ++it) {
    auto name = ResourceID(it->first).Binary();
    buffer << ", " << format_resource(name, it->second.available.Double()) << "/"
           << format_resource(name, it->second.total.Double());
    buffer << " " << name;
  }
  buffer << "}" << std::endl;
  return buffer.str();
}

bool NodeResources::HasGPU() const {
  if (predefined_resources.size() <= GPU) {
    return false;
  }
  return predefined_resources[GPU].total > 0;
}

bool NodeResourceInstances::operator==(const NodeResourceInstances &other) {
  for (size_t i = 0; i < PredefinedResources_MAX; i++) {
    if (!EqualVectors(this->predefined_resources[i].total,
                      other.predefined_resources[i].total)) {
      return false;
    }
    if (!EqualVectors(this->predefined_resources[i].available,
                      other.predefined_resources[i].available)) {
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
    if (!EqualVectors(it1->second.total, it2->second.total)) {
      return false;
    }
    if (!EqualVectors(it1->second.available, it2->second.available)) {
      return false;
    }
  }
  return true;
}

void TaskResourceInstances::ClearCPUInstances() {
  if (predefined_resources.size() >= CPU) {
    predefined_resources[CPU].clear();
    predefined_resources[CPU].clear();
  }
}

std::string NodeResourceInstances::DebugString() const {
  std::stringstream buffer;
  buffer << "{\n";
  for (size_t i = 0; i < this->predefined_resources.size(); i++) {
    buffer << "\t";
    switch (i) {
    case CPU:
      buffer << "CPU: ";
      break;
    case MEM:
      buffer << "MEM: ";
      break;
    case GPU:
      buffer << "GPU: ";
      break;
    case OBJECT_STORE_MEM:
      buffer << "OBJECT_STORE_MEM: ";
      break;
    default:
      RAY_CHECK(false) << "This should never happen.";
      break;
    }
    buffer << "(" << VectorToString(predefined_resources[i].total) << ":"
           << VectorToString(this->predefined_resources[i].available) << ")\n";
  }
  for (auto it = this->custom_resources.begin(); it != this->custom_resources.end();
       ++it) {
    buffer << "\t" << ResourceID(it->first).Binary() << ":("
           << VectorToString(it->second.total) << ":"
           << VectorToString(it->second.available) << ")\n";
  }
  buffer << "}" << std::endl;
  return buffer.str();
};

TaskResourceInstances NodeResourceInstances::GetAvailableResourceInstances() {
  TaskResourceInstances task_resources;
  task_resources.predefined_resources.resize(PredefinedResources_MAX);

  for (size_t i = 0; i < this->predefined_resources.size(); i++) {
    task_resources.predefined_resources[i] = this->predefined_resources[i].available;
  }

  for (const auto &it : this->custom_resources) {
    task_resources.custom_resources.emplace(it.first, it.second.available);
  }
  return task_resources;
};

bool NodeResourceInstances::Contains(scheduling::ResourceID id) const {
  return IsPredefinedResource(id) || custom_resources.contains(id.ToInt());
}

ResourceInstanceCapacities &NodeResourceInstances::GetMutable(scheduling::ResourceID id) {
  RAY_CHECK(Contains(id));
  if (IsPredefinedResource(id)) {
    return predefined_resources.at(id.ToInt());
  }
  return custom_resources.at(id.ToInt());
}

bool ResourceRequest::IsEmpty() const {
  for (size_t i = 0; i < this->predefined_resources.size(); i++) {
    if (this->predefined_resources[i] != 0) {
      return false;
    }
  }
  for (auto &it : custom_resources) {
    if (it.second != 0) {
      return false;
    }
  }
  return true;
}

bool ResourceRequest::operator==(const ResourceRequest &other) const {
  if (predefined_resources.size() != other.predefined_resources.size() ||
      custom_resources.size() != other.custom_resources.size() ||
      requires_object_store_memory != other.requires_object_store_memory) {
    return false;
  }
  for (size_t i = 0; i < predefined_resources.size(); i++) {
    if (predefined_resources[i] != other.predefined_resources[i]) {
      return false;
    }
  }
  for (const auto &entry : custom_resources) {
    auto iter = other.custom_resources.find(entry.first);
    if (iter == other.custom_resources.end() || entry.second != iter->second) {
      return false;
    }
  }
  return true;
}

bool ResourceRequest::operator!=(const ResourceRequest &other) const {
  return !(*this == other);
}

std::string ResourceRequest::DebugString() const {
  std::stringstream buffer;
  buffer << " {";
  for (size_t i = 0; i < this->predefined_resources.size(); i++) {
    buffer << "(" << this->predefined_resources[i] << ") ";
  }
  buffer << "}";

  buffer << "  [";
  for (auto &it : this->custom_resources) {
    buffer << it.first << ":"
           << "(" << it.second << ") ";
  }
  buffer << "]" << std::endl;
  return buffer.str();
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
  for (size_t i = 0; i < PredefinedResources_MAX; i++) {
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
