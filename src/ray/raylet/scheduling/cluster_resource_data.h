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

// Data structure specifying the capacity of each resource requested by a task.
class ResourceRequest {
 public:
  ResourceRequest() : ResourceRequest({}, false) {}

  ResourceRequest(absl::flat_hash_map<ResourceID, FixedPoint> resource_map)
      : ResourceRequest(resource_map, false){};

  ResourceRequest(absl::flat_hash_map<ResourceID, FixedPoint> resource_map,
                  bool requires_object_store_memory)
      : requires_object_store_memory_(requires_object_store_memory) {
    for (int i = 0; i < PredefinedResourcesEnum_MAX; i++) {
      predefined_resources_.push_back(0);
    }
    for (auto entry : resource_map) {
      Set(entry.first, entry.second);
    }
  }

  bool RequiresObjectStoreMemory() const { return requires_object_store_memory_; }

  FixedPoint Get(ResourceID resource_id) const {
    auto ptr = GetPointer(resource_id);
    RAY_CHECK(ptr) << "Resource not found: " << resource_id;
    return *ptr;
  }

  FixedPoint GetOrZero(ResourceID resource_id) const {
    auto ptr = GetPointer(resource_id);
    if (ptr == nullptr) {
      return FixedPoint(0);
    } else {
      return *ptr;
    }
  }

  ResourceRequest &Set(ResourceID resource_id, FixedPoint value) {
    if (value == 0) {
      if (IsPredefinedResource(resource_id)) {
        predefined_resources_[resource_id.ToInt()] = 0;
      } else {
        custom_resources_.erase(resource_id.ToInt());
      }
    } else {
      auto ptr = GetPointer(resource_id);
      if (ptr == nullptr) {
        custom_resources_[resource_id.ToInt()] = value;
      } else {
        *ptr = value;
      }
    }
    return *this;
  }

  bool Has(ResourceID resource_id) const { return GetOrZero(resource_id) > 0; }

  void Clear() {
    for (size_t i = 0; i < predefined_resources_.size(); i++) {
      predefined_resources_[i] = 0;
    }
    custom_resources_.clear();
  }

  void Cap(const ResourceRequest &other) {
    for (auto resource_id : ResourceIds()) {
      auto this_value = Get(resource_id);
      auto other_value = other.GetOrZero(resource_id);
      if (this_value > other_value) {
        Set(resource_id, other_value);
      }
    }
  }

  void RemoveNegative() {
    for (size_t i = 0; i < predefined_resources_.size(); i++) {
      if (predefined_resources_[i] < 0) {
        predefined_resources_[i] = 0;
      }
    }
    for (auto it = custom_resources_.begin(); it != custom_resources_.end();) {
      if (it->second < 0) {
        custom_resources_.erase(it++);
      } else {
        it++;
      }
    }
  }

  size_t Size() const { return ResourceIds().size(); }

  /// Check whether the request contains no resources.
  bool IsEmpty() const { return Size() == 0; }

  absl::flat_hash_set<ResourceID> ResourceIds() const {
    absl::flat_hash_set<ResourceID> res;
    for (size_t i = 0; i < predefined_resources_.size(); i++) {
      if (predefined_resources_[i] != 0) {
        res.insert(ResourceID(i));
      }
    }
    for (auto &entry : custom_resources_) {
      if (entry.second != 0) {
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
    auto resource_ids = ResourceIds();
    for (auto resource_id : resource_ids) {
      Set(resource_id, Get(resource_id) + other.GetOrZero(resource_id));
    }
    for (auto &resource_id : other.ResourceIds()) {
      if (resource_ids.find(resource_id) == resource_ids.end()) {
        Set(resource_id, other.Get(resource_id));
      }
    }
    return *this;
  }

  ResourceRequest &operator-=(const ResourceRequest &other) {
    auto resource_ids = ResourceIds();
    for (auto resource_id : resource_ids) {
      Set(resource_id, Get(resource_id) - other.GetOrZero(resource_id));
    }
    for (auto &resource_id : other.ResourceIds()) {
      if (resource_ids.find(resource_id) == resource_ids.end()) {
        Set(resource_id, -other.Get(resource_id));
      }
    }
    return *this;
  }

  bool operator==(const ResourceRequest &other) const {
    return FixedPointEqualVectors(predefined_resources_, other.predefined_resources_) &&
           this->custom_resources_ == other.custom_resources_;
  }

  bool operator!=(const ResourceRequest &other) const { return !(*this == other); }

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

  /// Returns human-readable string for this task request.
  std::string DebugString() const {
    std::stringstream buffer;
    buffer << "{";
    bool first = true;
    for (auto resource_id : ResourceIds()) {
      if (!first) {
        buffer << ", ";
      }
      first = false;
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
      this->predefined_resources_.push_back({});
    }
  }
  TaskResourceInstances(const ResourceRequest &request) {
    for (size_t i = 0; i < PredefinedResourcesEnum_MAX; i++) {
      this->predefined_resources_.push_back({});
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

  const std::vector<FixedPoint> &Get(const ResourceID resource_id) const {
    auto ptr = GetPointer(resource_id);
    RAY_CHECK(ptr != nullptr) << "Resource ID not found " << resource_id;
    return *ptr;
  }

  std::vector<double> GetDouble(const ResourceID resource_id) const {
    return VectorFixedPointToVectorDouble(Get(resource_id));
  }

  std::vector<FixedPoint> &GetMutable(const ResourceID resource_id) {
    auto ptr = GetPointer(resource_id);
    RAY_CHECK(ptr != nullptr) << "Resource ID not found " << resource_id;
    return *ptr;
  }

  bool Has(ResourceID resource_id) const {
    auto ptr = GetPointer(resource_id);
    return ptr != nullptr && ptr->size() > 0;
  }

  TaskResourceInstances &Set(const ResourceID resource_id, const std::vector<FixedPoint> &instances) {
    auto ptr = GetPointer(resource_id);
    if (ptr != nullptr) {
      *ptr = instances;
    } else {
      custom_resources_.emplace(resource_id.ToInt(), instances);
    }
    return *this;
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
    if (IsPredefinedResource(resource_id)) {
      auto &instances = GetMutable(resource_id);
      instances.clear();
    } else {
      custom_resources_.erase(resource_id.ToInt());
    }
  }

  absl::flat_hash_set<ResourceID> ResourceIds() const {
    absl::flat_hash_set<ResourceID> res;
    for (size_t i = 0; i < predefined_resources_.size(); i++) {
      if (predefined_resources_[i].size() > 0) {
        res.insert(ResourceID(i));
      }
    }
    for (auto &entry : custom_resources_) {
      if (entry.second.size() > 0) {
        res.insert(ResourceID(entry.first));
      }
    }
    return res;
  }

  FixedPoint Sum(const ResourceID resource_id) const {
    return FixedPoint::Sum(Get(resource_id));
  }

  size_t Size() const { return ResourceIds().size(); }

  /// Check whether there are no resource instances.
  bool IsEmpty() const { return Size() == 0; }

  bool operator==(const TaskResourceInstances &other) const {
    for (size_t i = 0; i < PredefinedResourcesEnum_MAX; i++) {
      if (!FixedPointEqualVectors(this->predefined_resources_[i],
                                  other.predefined_resources_[i])) {
        return false;
      }
    }
    return this->custom_resources_ == other.custom_resources_;
  }

  /// For each resource of this request aggregate its instances.
  ResourceRequest ToResourceRequest() const {
    ResourceRequest resource_request;
    for (auto resource_id : ResourceIds()) {
      resource_request.Set(resource_id, Sum(resource_id));
    }
    return resource_request;
  }

  /// Returns human-readable string for these resources.
  [[nodiscard]] std::string DebugString() const {
    std::stringstream buffer;
    buffer << "{";
    bool first = true;
    for (auto resource_id : ResourceIds()) {
      if (!first) {
        buffer << ", ";
      }
      first = false;
      buffer << resource_id.Binary() << ": "
             << FixedPointVectorToString(Get(resource_id));
    }
    buffer << "}";
    return buffer.str();
  }

  std::string SerializeAsJson() const {
    bool has_added_resource = false;
    std::stringstream buffer;
    buffer << "{";
    for (size_t i = 0; i < PredefinedResourcesEnum_MAX; i++) {
      std::vector<FixedPoint> resource = predefined_resources_[i];
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
    // TODO (chenk008): add custom_resources_
    buffer << "}";
    return buffer.str();
  }

 private:
  std::vector<FixedPoint> *GetPointer(ResourceID id) {
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

  const std::vector<FixedPoint> *GetPointer(ResourceID id) const {
    return const_cast<TaskResourceInstances *>(this)->GetPointer(id);
  }

  /// The list of instances of each predifined resource allocated to a task.
  std::vector<std::vector<FixedPoint>> predefined_resources_;
  /// The list of instances of each custom resource allocated to a task.
  absl::flat_hash_map<int64_t, std::vector<FixedPoint>> custom_resources_;
};

/// Total and available capacities of each resource of a node.
class NodeResources {
 public:
  NodeResources() {}
  NodeResources(const ResourceRequest &request) : total(request), available(request) {}
  NodeResources(const NodeResources &other)
      : total(other.total),
        available(other.available),
        normal_task_resources(other.normal_task_resources),
        latest_resources_normal_task_timestamp(
            other.latest_resources_normal_task_timestamp),
        object_pulls_queued(other.object_pulls_queued) {}
  ResourceRequest total;
  ResourceRequest available;
  /// Resources owned by normal tasks.
  ResourceRequest normal_task_resources;
  /// Normal task resources could be uploaded by 1) Raylets' periodical reporters; 2)
  /// Rejected RequestWorkerLeaseReply. So we need the timestamps to decide whether an
  /// upload is latest.
  int64_t latest_resources_normal_task_timestamp = 0;
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
  bool operator==(const NodeResources &other) const;
  bool operator!=(const NodeResources &other) const;
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
