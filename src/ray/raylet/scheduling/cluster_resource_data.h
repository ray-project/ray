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

const std::string ResourceEnumToString(PredefinedResourcesEnum resource);

const PredefinedResourcesEnum ResourceStringToEnum(const std::string &resource);

bool IsPredefinedResource(int64_t id);

bool IsPredefinedResource(scheduling::ResourceID resource_id);

/// Helper function to compare two vectors with FixedPoint values.
bool EqualVectors(const std::vector<FixedPoint> &v1, const std::vector<FixedPoint> &v2);

/// Convert a vector of doubles to a vector of resource units.
std::vector<FixedPoint> VectorDoubleToVectorFixedPoint(const std::vector<double> &vector);

/// Convert a vector of resource units to a vector of doubles.
std::vector<double> VectorFixedPointToVectorDouble(
    const std::vector<FixedPoint> &vector_fp);

/// Capacities of each instance of a resource.
struct ResourceInstanceCapacities {
  std::vector<FixedPoint> total;
  std::vector<FixedPoint> available;
};

class PredefinedResources {
 public:
  PredefinedResources() {
    for (size_t i = 0; i < PredefinedResourcesEnum_MAX; i++) {
      values_.push_back(0);
    }
  }

  const FixedPoint &Get(int64_t resource_id) const {
    return this->values_[resource_id];
  }

  PredefinedResources &Set(int64_t resource_id, const FixedPoint & value) {
    this->values_[resource_id] = value;
    return *this;
  }

  const bool Has(int64_t resource_id) const {
    return this->values_[resource_id] != 0;
  }

  const FixedPoint &GetCPU() const { return this->values_[CPU]; }

  const FixedPoint &GetMemory() const { return this->values_[MEM]; }

  const FixedPoint &GetGPU() const { return this->values_[GPU]; }

  const FixedPoint &GetObjectStoreMemory() const { return this->values_[OBJECT_STORE_MEM]; }

  PredefinedResources &SetCPU(const FixedPoint &value) { this->values_[CPU] = value; return *this; }

  PredefinedResources &SetMemory(const FixedPoint &value) { this->values_[MEM] = value; return *this; }

  PredefinedResources &SetGPU(const FixedPoint &value) { this->values_[GPU] = value; return *this; }

  PredefinedResources &SetObjectStoreMemory(const FixedPoint &value) { this->values_[OBJECT_STORE_MEM] = value; return *this; }

  bool HasCPU() const { return this->GetCPU() > 0; }

  bool HasMemory() const { return this->GetMemory() > 0; }

  bool HasGPU() const { return this->GetGPU() > 0; }

  bool HasObjectStoreMemory() const { return this->GetObjectStoreMemory() > 0; }

  size_t Size() const {
    size_t size = 0;
    for (auto value : values_) {
      if (value != 0) {
        size += 1;
      }
    }
    return size;
  }

  bool IsEmpty() const {
    for (auto value : values_) {
      if (value != 0) {
        return false;
      }
    }
    return true;
  }

  void Clear() {
    for (size_t i = 0; i < values_.size(); i++) {
      this->values_[i] = 0;
    }
  }

  PredefinedResources operator+(const PredefinedResources &other) {
    PredefinedResources res;
    for (size_t i = 0; i < this->values_.size(); i++) {
      res.values_[i] = this->values_[i] + other.values_[i];
    }
    return res;
  }

  PredefinedResources operator-(const PredefinedResources &other) {
    PredefinedResources res;
    for (size_t i = 0; i < this->values_.size(); i++) {
      res.values_[i] = this->values_[i] - other.values_[i];
    }
    return res;
  }

  PredefinedResources &operator=(const PredefinedResources &other) {
    for (size_t i = 0; i < this->values_.size(); i++) {
      this->values_[i] = other.values_[i];
    }
    return *this;
  }

  PredefinedResources &operator+=(const PredefinedResources &other) {
    for (size_t i = 0; i < this->values_.size(); i++) {
      this->values_[i] += other.values_[i];
    }
    return *this;
  }

  PredefinedResources &operator-=(const PredefinedResources &other) {
    for (size_t i = 0; i < this->values_.size(); i++) {
      this->values_[i] -= other.values_[i];
    }
    return *this;
  }

  bool operator==(const PredefinedResources &other) const {
    return std::equal(std::begin(this->values_), std::end(this->values_), std::begin(other.values_));
  }

  bool operator<=(const PredefinedResources &other) const {
    for (size_t i = 0; i < this->values_.size(); i++) {
      if (this->values_[i] > other.values_[i]) {
        return false;
      }
      return true;
    }
  }

  bool operator>=(const PredefinedResources &other) const {
    return other <= *this;
  }

  void Normalize() {
    for (size_t i = 0; i < this->values_.size(); i++) {
      if (this->values_[i] < 0) {
        this->values_[i] = 0;
      }
    }
  }

 std::string DebugString() const {
  std::stringstream buffer;
  buffer << "{";
  for (size_t i = 0; i < this->values_.size(); i++) {
    buffer << "(" << this->values_[i] << ") ";
  }
  buffer << "}";
  return buffer.str();
  }

  static size_t NumAllPredefinedResources() {
    return PredefinedResourcesEnum_MAX;
  }

 private:
  std::vector<FixedPoint> values_;
};

class CustomResources {
 public:
  const FixedPoint &Get(int64_t resource_id) const {
    auto it = this->values_.find(resource_id);
    RAY_CHECK(it != this->values_.end());
    return it->second;
  }

  const FixedPoint GetOrZero(int64_t resource_id) const {
    auto it = this->values_.find(resource_id);
    if (it == this->values_.end()) {
      return 0;
    }
    return it->second;
  }

  CustomResources &Set(int64_t resource_id, const FixedPoint & value) {
    this->values_[resource_id] = value;
    return *this;
  }

  const bool Has(int64_t resource_id) const {
    return this->values_.find(resource_id) != this->values_.end();
  }

  size_t Size() const { return values_.size(); }

  bool IsEmpty() const { return values_.size() == 0; }

  void Clear() { this->values_.clear(); }

  CustomResources operator+(const CustomResources &other) {
    CustomResources res;
    for (auto entry: values_) {
      res.values_[entry.first] = entry.second + other.GetOrZero(entry.first);
    }
    for (auto entry: other.values_) {
      if (!Has(entry.first)) {
        res.values_[entry.first] = entry.second;
      }
    }
    return res;
  }

  CustomResources operator-(const CustomResources &other) {
    CustomResources res;
    for (auto entry: values_) {
      res.values_[entry.first] = entry.second - other.GetOrZero(entry.first);
    }
    for (auto entry: other.values_) {
      if (!Has(entry.first)) {
        res.values_[entry.first] = -entry.second;
      }
    }
    return res;
  }

  CustomResources &operator=(const CustomResources &other) {
    this->values_ = other.values_;
    return *this;
  }

  CustomResources &operator+=(const CustomResources &other) {
    for (auto entry: values_) {
      entry.second += other.GetOrZero(entry.first);
    }
    for (auto entry: other.values_) {
      if (!Has(entry.first)) {
        values_[entry.first] = entry.second;
      }
    }
    return *this;
  }

  CustomResources &operator-=(const CustomResources &other) {
    for (auto entry: values_) {
      entry.second -= other.GetOrZero(entry.first);
    }
    for (auto entry: other.values_) {
      if (!Has(entry.first)) {
        values_[entry.first] = -entry.second;
      }
    }
    return *this;
  }

  bool operator==(const CustomResources &other) const {
    return this->values_ == other.values_;
  }

  bool operator<=(const CustomResources &other) const {
    for (auto entry: values_) {
      auto it = other.values_.find(entry.first);
      if (it == other.values_.end()) {
        return false;
      }
      if (entry.second > it->second) {
        return false;
      }
    }
    return true;
  }

  bool operator>=(const CustomResources &other) const {
    return other <= *this;
  }

  std::string DebugString() const {
    std::stringstream buffer;
    buffer << "[";
    for (auto &it : this->values_) {
      buffer << it.first << ":"
             << "(" << it.second << ") ";
    }
    buffer << "]" << std::endl;
    return buffer.str();
  }

 private:
  absl::flat_hash_map<int64_t, FixedPoint> values_;
};

// Data structure specifying the capacity of each resource requested by a task.
class ResourceRequest {
 public:
  /// List of predefined resources required by the task.
  PredefinedResources predefined_resources;
  /// List of custom resources required by the task.
  CustomResources custom_resources;
  /// Whether this task requires object store memory.
  /// TODO(swang): This should be a quantity instead of a flag.
  bool requires_object_store_memory = false;
  /// Check whether the request contains no resources.
  bool IsEmpty() const;
  /// Returns human-readable string for this task request.
  std::string DebugString() const;

  const FixedPoint &Get(int64_t resource_id) const {
    if (IsPredefinedResource(resource_id)) {
      return this->predefined_resources.Get(resource_id);
    } else {
      return this->custom_resources.Get(resource_id);
    }
  }

  const FixedPoint GetOrZero(int64_t resource_id) const {
    if (IsPredefinedResource(resource_id)) {
      return this->predefined_resources.Get(resource_id);
    } else {
      return this->custom_resources.GetOrZero(resource_id);
    }
  }

  ResourceRequest &Set(int64_t resource_id, const FixedPoint & value) {
    if (IsPredefinedResource(resource_id)) {
      this->predefined_resources.Set(resource_id, value);
    } else {
      this->custom_resources.Set(resource_id, value);
    }
    return *this;
  }

  const bool Has(int64_t resource_id) const {
    if (IsPredefinedResource(resource_id)) {
      return this->predefined_resources.Has(resource_id);
    } else {
      return this->custom_resources.Has(resource_id);
    }
  }

  const FixedPoint &GetCPU() const { return this->predefined_resources.GetCPU(); }

  const FixedPoint &GetMemory() const { return this->predefined_resources.GetMemory(); }

  const FixedPoint &GetGPU() const { return this->predefined_resources.GetGPU(); }

  const FixedPoint &GetObjectStoreMemory() const { return this->predefined_resources.GetObjectStoreMemory(); }

  ResourceRequest &SetCPU(const FixedPoint &value) { this->predefined_resources.SetCPU(value); return *this; }

  ResourceRequest &SetMemory(const FixedPoint &value) { this->predefined_resources.SetMemory(value); return *this; }

  ResourceRequest &SetGPU(const FixedPoint &value) { this->predefined_resources.SetGPU(value); return *this; }

  ResourceRequest &SetObjectStoreMemory(const FixedPoint &value) { this->predefined_resources.SetObjectStoreMemory(value); return *this; }

  bool HasCPU() const { return this->predefined_resources.HasCPU(); }

  bool HasMemory() const { return this->predefined_resources.HasMemory(); }

  bool HasGPU() const { return this->predefined_resources.HasGPU(); }

  bool HasObjectStoreMemory() const { return this->predefined_resources.HasObjectStoreMemory(); }

  void Clear() { this->predefined_resources.Clear(); this->custom_resources.Clear(); }

  ResourceRequest operator+(const ResourceRequest &other) {
    ResourceRequest res;
    res.predefined_resources = this->predefined_resources + other.predefined_resources;
    res.custom_resources = this->custom_resources + other.custom_resources;
    return res;
  }

  ResourceRequest operator-(const ResourceRequest &other) {
    ResourceRequest res;
    res.predefined_resources = this->predefined_resources - other.predefined_resources;
    res.custom_resources = this->custom_resources - other.custom_resources;
    return res;
 }

  ResourceRequest &operator=(const ResourceRequest &other) {
    this->predefined_resources = other.predefined_resources;
    this->custom_resources = other.custom_resources;
    return *this;
  }

  ResourceRequest &operator+=(const ResourceRequest &other) {
    this->predefined_resources += other.predefined_resources;
    this->custom_resources += other.custom_resources;
    return *this;
  }

  ResourceRequest &operator-=(const ResourceRequest &other) {
    this->predefined_resources -= other.predefined_resources;
    this->custom_resources -= other.custom_resources;
    return *this;
  }

  bool operator==(const ResourceRequest &other) const {
    return this->predefined_resources == other.predefined_resources && this->custom_resources == other.custom_resources;
  }

  bool operator<=(const ResourceRequest &other) const {
    return predefined_resources <= other.predefined_resources && custom_resources <= other.custom_resources;
  }

  bool operator>=(const ResourceRequest &other) const {
    return other <= *this;
  }
};


// Data structure specifying the capacity of each instance of each resource
// allocated to a task.
class TaskResourceInstances {
 public:
  /// The list of instances of each predifined resource allocated to a task.
  std::vector<std::vector<FixedPoint>> predefined_resources;
  /// The list of instances of each custom resource allocated to a task.
  absl::flat_hash_map<int64_t, std::vector<FixedPoint>> custom_resources;
  bool operator==(const TaskResourceInstances &other);
  /// Get instances based on the string.
  const std::vector<FixedPoint> &Get(const std::string &resource_name) const;
  /// For each resource of this request aggregate its instances.
  ResourceRequest ToResourceRequest() const;
  /// Get CPU instances only.
  std::vector<FixedPoint> GetCPUInstances() const {
    if (!this->predefined_resources.empty()) {
      return this->predefined_resources[CPU];
    } else {
      return {};
    }
  };
  std::vector<double> GetCPUInstancesDouble() const {
    if (!this->predefined_resources.empty()) {
      return VectorFixedPointToVectorDouble(this->predefined_resources[CPU]);
    } else {
      return {};
    }
  };
  /// Get GPU instances only.
  std::vector<FixedPoint> GetGPUInstances() const {
    if (!this->predefined_resources.empty()) {
      return this->predefined_resources[GPU];
    } else {
      return {};
    }
  };
  std::vector<double> GetGPUInstancesDouble() const {
    if (!this->predefined_resources.empty()) {
      return VectorFixedPointToVectorDouble(this->predefined_resources[GPU]);
    } else {
      return {};
    }
  };
  /// Get mem instances only.
  std::vector<FixedPoint> GetMemInstances() const {
    if (!this->predefined_resources.empty()) {
      return this->predefined_resources[MEM];
    } else {
      return {};
    }
  };
  std::vector<double> GetMemInstancesDouble() const {
    if (!this->predefined_resources.empty()) {
      return VectorFixedPointToVectorDouble(this->predefined_resources[MEM]);
    } else {
      return {};
    }
  };
  /// Clear only the CPU instances field.
  void ClearCPUInstances();
  /// Check whether there are no resource instances.
  bool IsEmpty() const;
  /// Returns human-readable string for these resources.
  [[nodiscard]] std::string DebugString() const;
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
  /// Available and total capacities for each instance of a predefined resource.
  std::vector<ResourceInstanceCapacities> predefined_resources;
  /// Map containing custom resources. The key of each entry represents the
  /// custom resource ID.
  absl::flat_hash_map<int64_t, ResourceInstanceCapacities> custom_resources;
  /// Extract available resource instances.
  TaskResourceInstances GetAvailableResourceInstances();
  /// Returns if this equals another node resources.
  bool operator==(const NodeResourceInstances &other);
  /// Returns human-readable string for these resources.
  [[nodiscard]] std::string DebugString() const;
  /// Returns true if it contains this resource.
  bool Contains(scheduling::ResourceID id) const;
  /// Returns the resource instance of a given resource id.
  ResourceInstanceCapacities &GetMutable(scheduling::ResourceID id);
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
