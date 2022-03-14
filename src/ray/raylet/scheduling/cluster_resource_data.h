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

#pragma once

#include <iostream>
#include <sstream>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "ray/common/id.h"
#include "ray/raylet/scheduling/fixed_point.h"
#include "ray/raylet/scheduling/scheduling_ids.h"
#include "ray/util/logging.h"

namespace ray {

using scheduling::ResourceID;

bool IsPredefinedResource(scheduling::ResourceID resource_id);

/// Helper function to compare two vectors with FixedPoint values.
bool EqualVectors(const std::vector<FixedPoint> &v1, const std::vector<FixedPoint> &v2);

/// Convert a vector of doubles to a vector of resource units.
std::vector<FixedPoint> VectorDoubleToVectorFixedPoint(const std::vector<double> &vector);

/// Convert a vector of resource units to a vector of doubles.
std::vector<double> VectorFixedPointToVectorDouble(
    const std::vector<FixedPoint> &vector_fp);

// Data structure specifying the capacity of each resource requested by a task.
class ResourceRequest {
 public:
  ResourceRequest(): ResourceRequest({}, false) {}

  ResourceRequest(absl::flat_hash_map<std::string, FixedPoint> resource_map)
      : ResourceRequest(resource_map, false){};

  ResourceRequest(absl::flat_hash_map<std::string, FixedPoint> resource_map,
                  bool requires_object_store_memory)
      : requires_object_store_memory_(requires_object_store_memory) {
    for (int i = 0; i < PredefinedResourcesEnum_MAX; i++) {
      predefined_resources_.push_back(0);
    }
    for (auto entry : resource_map) {
      Set(ResourceID(entry.first), entry.second);
    }
  }

  bool RequiresObjectStoreMemory() const {
    return requires_object_store_memory_;
  }

  FixedPoint Get(ResourceID resource_id) const {
    auto ptr = GetPointer(resource_id);
    // FixedPoint *ptr;
    RAY_CHECK(ptr) << "Resource not found: " << resource_id;
    return *ptr;
  }

  FixedPoint GetOrZero(ResourceID resource_id) const {
    auto ptr = GetPointer(resource_id);
    // FixedPoint *ptr;
    if (ptr == nullptr) {
      return FixedPoint(0);
    } else {
      return *ptr;
    }
  }

  ResourceRequest &Set(ResourceID resource_id, const FixedPoint &value) {
    auto ptr = GetPointer(resource_id);
    if (ptr == nullptr) {
      custom_resources_[resource_id.ToInt()] = value;
    } else {
      *ptr = value;
    }
    return *this;
  }

  const bool Has(ResourceID resource_id) const { return GetOrZero(resource_id) > 0; }

  void Clear() {
    for (size_t i = 0; i < predefined_resources_.size(); i++) {
      predefined_resources_[i] = 0;
    }
    custom_resources_.clear();
  }

  void Normalize() {
    for (size_t i = 0; i < predefined_resources_.size(); i++) {
      if (predefined_resources_[i] < 0) {
        predefined_resources_[i] = 0;
      }
    }
    for (auto &entry : custom_resources_) {
      if (entry.second < 0) {
        entry.second = 0;
      }
    }
  }

  absl::flat_hash_set<ResourceID> ResourceIds() const {
    absl::flat_hash_set<ResourceID> res;
    for (size_t i = 0; i < predefined_resources_.size(); i++) {
      if (predefined_resources_[i] > 0) {
        res.insert(ResourceID(i));
      }
    }
    for (auto &entry : custom_resources_) {
      if (entry.second > 0) {
        res.insert(ResourceID(entry.first));
      }
    }
    return res;
  }

  absl::flat_hash_map<ResourceID, FixedPoint> ToMap() const {
    absl::flat_hash_map<ResourceID, FixedPoint> res;
    for (auto resource_id : ResourceIds()) {
      res.emplace(resource_id, Get(resource_id));
    }
    return res;
  }

  ResourceRequest &operator=(const ResourceRequest &other) {
    this->predefined_resources_ = other.predefined_resources_;
    this->custom_resources_ = other.custom_resources_;
    this->requires_object_store_memory_ = other.requires_object_store_memory_;
    return *this;
  }

  ResourceRequest operator+(const ResourceRequest &other) {
    ResourceRequest res = *this;
    res += other;
    return res;
  }

  ResourceRequest operator-(const ResourceRequest &other) {
    ResourceRequest res = *this;
    res -= other;
    return res;
  }

  ResourceRequest &operator+=(const ResourceRequest &other) {
    for (auto resource_id : ResourceIds()) {
      *GetPointer(resource_id) += other.GetOrZero(resource_id);
    }
    for (auto &resource_id : other.ResourceIds()) {
      if (!Has(resource_id)) {
        Set(resource_id, other.Get(resource_id));
      }
    }
    return *this;
  }

  ResourceRequest &operator-=(const ResourceRequest &other) {
    for (auto resource_id : ResourceIds()) {
      *GetPointer(resource_id) -= other.GetOrZero(resource_id);
    }
    for (auto &resource_id : other.ResourceIds()) {
      if (!Has(resource_id)) {
        Set(resource_id, -other.Get(resource_id));
      }
    }
    return *this;
  }

  bool operator==(const ResourceRequest &other) const {
    return EqualVectors(predefined_resources_, other.predefined_resources_) &&
           this->custom_resources_ == other.custom_resources_;
  }

  bool operator<=(const ResourceRequest &other) const {
    if (Size() > other.Size()) {
      return false;
    }
    for (auto resource_id : ResourceIds()) {
      if (Get(resource_id) > other.GetOrZero(resource_id)) {
        return false;
      }
    }
    return true;
  }

  bool operator>=(const ResourceRequest &other) const { return other <= *this; }

  size_t Size() const { return ResourceIds().size(); }

  /// Check whether the request contains no resources.
  bool IsEmpty() const { return Size() == 0; }

  /// Returns human-readable string for this task request.
  std::string DebugString() const {
    std::stringstream buffer;
    buffer << "{";
    bool first = true;
    for (auto resource_id : ResourceIds()) {
      if (!first) {
        buffer << ", ";
      }
      buffer << resource_id.Binary() << ": " << Get(resource_id);
    }
    buffer << "}";
    return buffer.str();
  }

 private:
  FixedPoint *GetPointer(ResourceID id) {
    if (IsPredefinedResource(id)) {
      return &predefined_resources_[id.ToInt()];
    } else {
      auto it = custom_resources_.find(id.ToInt());
      if (it == custom_resources_.end()) {
        return nullptr;
      } else {
        return &it->second;
      }
    }
  }

  const FixedPoint *GetPointer(ResourceID id) const {
    return const_cast<ResourceRequest *>(this)->GetPointer(id);
  }

  /// List of predefined resources required by the task.
  std::vector<FixedPoint> predefined_resources_;
  /// List of custom resources required by the task.
  absl::flat_hash_map<int64_t, FixedPoint> custom_resources_;
  /// Whether this task requires object store memory.
  /// TODO(swang): This should be a quantity instead of a flag.
  bool requires_object_store_memory_ = false;
};


// Data structure specifying the capacity of each instance of each resource
// allocated to a task.
class TaskResourceInstances {
 public:
  TaskResourceInstances() {
    for (size_t i = 0; i < PredefinedResourcesEnum_MAX; i++) {
      this->predefined_resources.push_back({});
    }
  }
  TaskResourceInstances(const ResourceRequest &request) {
    for (size_t i = 0; i < PredefinedResourcesEnum_MAX; i++) {
      this->predefined_resources.push_back({});
    }
    for (auto resource_id : request.ResourceIds()) {
      std::vector<FixedPoint> instances;
      auto value = request.Get(resource_id);
      if (resource_id.IsUnitInstanceResource()) {
        size_t num_instances = static_cast<size_t>(value.Double());
        for (size_t i = 0; i < num_instances; i++) {
          instances.push_back(1.0);
        };
      } else {
        instances.push_back(value);
      }
      Set(resource_id, instances);
    }
  }
  /// The list of instances of each predifined resource allocated to a task.
  std::vector<std::vector<FixedPoint>> predefined_resources;
  /// The list of instances of each custom resource allocated to a task.
  absl::flat_hash_map<int64_t, std::vector<FixedPoint>> custom_resources;
  bool operator==(const TaskResourceInstances &other);

  bool Has(ResourceID resource_id) const {
    if (IsPredefinedResource(resource_id)) {
      return predefined_resources[resource_id.ToInt()].size() > 0;
    } else {
      auto it = custom_resources.find(resource_id.ToInt());
      return it != custom_resources.end() && it->second.size() > 0;
    }
  }

  TaskResourceInstances &Set(const ResourceID resource_id, const std::vector<FixedPoint> &instances) {
     GetMutable(resource_id) = instances;
     return *this;
  }

  std::vector<FixedPoint> &GetMutable(const ResourceID resource_id) {
     if (IsPredefinedResource(resource_id)) {
       return predefined_resources[resource_id.ToInt()];
     } else {
       return custom_resources[resource_id.ToInt()];
     }
  }

  void Add(const ResourceID resource_id, const std::vector<FixedPoint> &delta) {
    auto &instances = GetMutable(resource_id);
    if (instances.size() <= delta.size()) {
      instances.resize(delta.size());
    }
    for (size_t i = 0; i < instances.size(); ++i) {
      instances[i] += delta[i];
    }
  }

  void Clear(ResourceID resource_id) {
    auto &instances = GetMutable(resource_id);
    for (size_t i = 0; i < instances.size(); ++i) {
      instances[i] = 0;
    }
  }

  absl::flat_hash_set<ResourceID> ResourceIds() const {
    absl::flat_hash_set<ResourceID> res;
    for (size_t i = 0; i < predefined_resources.size(); i++) {
      res.insert(ResourceID(i));
    }
    for (auto &entry : custom_resources) {
      res.insert(ResourceID(entry.first));
    }
    return res;
  }

  FixedPoint Sum(const ResourceID resource_id) const {
    return FixedPoint::Sum(Get(resource_id));
  }

  FixedPoint SumCPU() const {
    return FixedPoint::Sum(predefined_resources[CPU]);
  }

  /// Get instances based on the string.
  const std::vector<FixedPoint> &Get(const ResourceID resource_id) const {
     if (IsPredefinedResource(resource_id)) {
       return predefined_resources[resource_id.ToInt()];
     } else {
       return custom_resources.at(resource_id.ToInt());
     }
  }

  /// For each resource of this request aggregate its instances.
  ResourceRequest ToResourceRequest() const;
  /// Get CPU instances only.
  std::vector<FixedPoint> GetCPUInstances() const {
    return this->predefined_resources[CPU];
  };
  std::vector<double> GetCPUInstancesDouble() const {
    return VectorFixedPointToVectorDouble(this->predefined_resources[CPU]);
  };
  /// Get GPU instances only.
  std::vector<FixedPoint> GetGPUInstances() const {
    return this->predefined_resources[GPU];
  };
  std::vector<double> GetGPUInstancesDouble() const {
    return VectorFixedPointToVectorDouble(this->predefined_resources[GPU]);
  };
  /// Get mem instances only.
  std::vector<FixedPoint> GetMemInstances() const {
    return this->predefined_resources[MEM];
  };
  std::vector<double> GetMemInstancesDouble() const {
    return VectorFixedPointToVectorDouble(this->predefined_resources[MEM]);
  };

  std::vector<FixedPoint> GetObjectStoreMemory() const {
    return this->predefined_resources[OBJECT_STORE_MEM];
  }
  /// Clear only the CPU instances field.
  void ClearCPUInstances();
  /// Check whether there are no resource instances.
  bool IsEmpty() const;
  /// Returns human-readable string for these resources.
  [[nodiscard]] std::string DebugString() const;
  std::string SerializeAsJson() const {
    bool has_added_resource = false;
    std::stringstream buffer;
    buffer << "{";
    for (size_t i = 0; i < PredefinedResourcesEnum_MAX; i++) {
      std::vector<FixedPoint> resource = predefined_resources[i];
      if (resource.empty()) {
        continue;
      }
      if (has_added_resource) {
        buffer << ",";
      }
      std::string resource_name = ResourceID(i).Binary();
      buffer << "\"" << resource_name << "\":";
      if (!ResourceID(i).IsUnitInstanceResource()) {
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
};

/// Total and available capacities of each resource of a node.
class NodeResources {
 public:
  NodeResources() {}
  NodeResources(const NodeResources &other)
      : total(other.total),
        available(other.available),
        object_pulls_queued(other.object_pulls_queued) {}
  ResourceRequest total;
  ResourceRequest available;
  bool object_pulls_queued = false;

  /// Amongst CPU, memory, and object store memory, calculate the utilization percentage
  /// of each resource and return the highest.
  float CalculateCriticalResourceUtilization() const;
  /// Returns true if the node has the available resources to run the task.
  /// Note: This doesn't account for the binpacking of unit resources.
  bool IsAvailable(const ResourceRequest &resource_request,
                   bool ignore_at_capacity = false) const;
  /// Returns true if the node's total resources are enough to run the task.
  /// Note: This doesn't account for the binpacking of unit resources.
  bool IsFeasible(const ResourceRequest &resource_request) const;
  /// Returns if this equals another node resources.
  bool operator==(const NodeResources &other);
  bool operator!=(const NodeResources &other);
  /// Returns human-readable string for these resources.
  std::string DebugString() const;
  /// Returns compact dict-like string.
  std::string DictString() const;
};

/// Total and available capacities of each resource instance.
/// This is used to describe the resources of the local node.
class NodeResourceInstances {
 public:
  TaskResourceInstances available;
  TaskResourceInstances total;
  /// Extract available resource instances.
  TaskResourceInstances GetAvailableResourceInstances();
  /// Returns if this equals another node resources.
  bool operator==(const NodeResourceInstances &other);
  /// Returns human-readable string for these resources.
  [[nodiscard]] std::string DebugString() const;
  /// Returns true if it contains this resource.
  bool Contains(scheduling::ResourceID id) const;
};

struct Node {
  Node(const NodeResources &resources) : local_view_(resources) {}

  NodeResources *GetMutableLocalView() { return &local_view_; }

  const NodeResources &GetLocalView() const { return local_view_; }

 private:
  /// Our local view of the remote node's resources. This may be dirty
  /// because it includes any resource requests that we allocated to this
  /// node through spillback since our last heartbeat tick. This view will
  /// get overwritten by the last reported view on each heartbeat tick, to
  /// make sure that our local view does not skew too much from the actual
  /// resources when light heartbeats are enabled.
  NodeResources local_view_;
};

/// \request Conversion result to a ResourceRequest data structure.
NodeResources ResourceMapToNodeResources(
    const absl::flat_hash_map<std::string, double> &resource_map_total,
    const absl::flat_hash_map<std::string, double> &resource_map_available);

/// Convert a map of resources to a ResourceRequest data structure.
ResourceRequest ResourceMapToResourceRequest(
    const absl::flat_hash_map<std::string, double> &resource_map,
    bool requires_object_store_memory);

}  // namespace ray
