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

#include <boost/range/adaptor/map.hpp>
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

/// Represents a set of resources.
/// NOTE: negative values are valid in this set, while 0 is not. This means if any
/// resource value is changed to 0, the resource will be removed.
/// TODO(hchen): This class should be independent with tasks. We should move out the
/// "requires_object_store_memory_" field, and rename this class to ResourceSet.
class ResourceRequest {
 public:
  using ResourceIdIterator =
      boost::select_first_range<absl::flat_hash_map<ResourceID, FixedPoint>>;

  /// Construct an empty ResourceRequest.
  ResourceRequest() : ResourceRequest({}, false) {}

  /// Construct a ResourceRequest with a given resource map.
  ResourceRequest(absl::flat_hash_map<ResourceID, FixedPoint> resource_map)
      : ResourceRequest(resource_map, false){};

  ResourceRequest(absl::flat_hash_map<ResourceID, FixedPoint> resource_map,
                  bool requires_object_store_memory)
      : requires_object_store_memory_(requires_object_store_memory) {
    for (auto entry : resource_map) {
      if (entry.second != 0) {
        resources_[entry.first] = entry.second;
      }
    }
  }

  ResourceRequest &operator=(const ResourceRequest &other) = default;

  bool RequiresObjectStoreMemory() const { return requires_object_store_memory_; }

  /// Get the value of a particular resource.
  /// If the resource doesn't exist, return 0.
  FixedPoint Get(ResourceID resource_id) const {
    auto it = resources_.find(resource_id);
    if (it == resources_.end()) {
      return FixedPoint(0);
    } else {
      return it->second;
    }
  }

  /// Set a resource to the given value.
  /// NOTE: if the new value is 0, the resource will be removed.
  ResourceRequest &Set(ResourceID resource_id, FixedPoint value) {
    if (value == 0) {
      resources_.erase(resource_id);
    } else {
      resources_[resource_id] = value;
    }
    return *this;
  }

  /// Check whether a particular resource exist.
  bool Has(ResourceID resource_id) const { return resources_.contains(resource_id); }

  /// Clear the whole set.
  void Clear() { resources_.clear(); }

  /// Remove the negative values in this set.
  void RemoveNegative() {
    for (auto it = resources_.begin(); it != resources_.end();) {
      if (it->second < 0) {
        resources_.erase(it++);
      } else {
        it++;
      }
    }
  }

  /// Return the number of resources in this set.
  size_t Size() const { return resources_.size(); }

  /// Return true if this set is empty.
  bool IsEmpty() const { return resources_.empty(); }

  /// Return a boost::range object that can be used as an iterator of the resource IDs.
  ResourceIdIterator ResourceIds() const { return boost::adaptors::keys(resources_); }

  /// Return a map from the resource ids to the values.
  absl::flat_hash_map<ResourceID, FixedPoint> ToMap() const {
    absl::flat_hash_map<ResourceID, FixedPoint> res;
    for (auto entry : resources_) {
      res.emplace(entry.first, entry.second);
    }
    return res;
  }

  /// Return a map from resource names (string) to values (double).
  absl::flat_hash_map<std::string, double> ToResourceMap() const {
    absl::flat_hash_map<std::string, double> resource_map;
    for (auto entry : resources_) {
      resource_map.emplace(entry.first.Binary(), entry.second.Double());
    }
    return resource_map;
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
    for (auto &entry : other.resources_) {
      auto it = resources_.find(entry.first);
      if (it != resources_.end()) {
        it->second += entry.second;
        if (it->second == 0) {
          resources_.erase(it);
        }
      } else {
        resources_.emplace(entry.first, entry.second);
      }
    }
    return *this;
  }

  ResourceRequest &operator-=(const ResourceRequest &other) {
    for (auto &entry : other.resources_) {
      auto it = resources_.find(entry.first);
      if (it != resources_.end()) {
        it->second -= entry.second;
        if (it->second == 0) {
          resources_.erase(it);
        }
      } else {
        resources_.emplace(entry.first, -entry.second);
      }
    }
    return *this;
  }

  bool operator==(const ResourceRequest &other) const {
    return this->resources_ == other.resources_;
  }

  bool operator!=(const ResourceRequest &other) const { return !(*this == other); }

  /// Check whether this set is a subset of another one.
  /// If A <= B, it means for each resource, its value in A is less than or equqal to that
  /// in B.
  bool operator<=(const ResourceRequest &other) const {
    // Check all resources that exist in this.
    for (auto &entry : resources_) {
      auto &this_value = entry.second;
      auto other_value = FixedPoint(0);
      auto it = other.resources_.find(entry.first);
      if (it != other.resources_.end()) {
        other_value = it->second;
      }
      if (this_value > other_value) {
        return false;
      }
    }
    // Check all resources that exist in other, but not in this.
    for (auto &entry : other.resources_) {
      if (!resources_.contains(entry.first)) {
        if (entry.second < 0) {
          return false;
        }
      }
    }
    return true;
  }

  /// Check whether this set is a super set of another one.
  /// If A >= B, it means for each resource, its value in A is larger than or equqal to
  /// that in B.
  bool operator>=(const ResourceRequest &other) const { return other <= *this; }

  /// Return a human-readable string for this set.
  std::string DebugString() const {
    std::stringstream buffer;
    buffer << "{";
    bool first = true;
    for (auto &resource_id : ResourceIds()) {
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
  /// Map from the resource IDs to the resource values.
  absl::flat_hash_map<ResourceID, FixedPoint> resources_;
  /// Whether this task requires object store memory.
  /// TODO(swang): This should be a quantity instead of a flag.
  bool requires_object_store_memory_ = false;
};

/// Represents a resource set that contains the per-instance resource values.
/// NOTE, unlike ResourceRequest, zero values won't be automatically removed in this
/// class. Because otherwise we will lose the number of instances the set originally had
/// for the particular resource.
/// TODO(hchen): due to the same reason of ResourceRequest, we should rename it to
/// ResourceInstanceSet.
class TaskResourceInstances {
 public:
  using ResourceIdIterator =
      boost::select_first_range<absl::flat_hash_map<ResourceID, std::vector<FixedPoint>>>;

  /// Construct an empty TaskResourceInstances.
  TaskResourceInstances() {}

  /// Construct a TaskResourceInstances with the values from a ResourceRequest.
  TaskResourceInstances(const ResourceRequest &request) {
    for (auto &resource_id : request.ResourceIds()) {
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

  /// Get the per-instance values of a particular resource.
  /// NOTE: the resource MUST already exist in this TaskResourceInstances, otherwise a
  /// check fail will occur.
  const std::vector<FixedPoint> &Get(const ResourceID resource_id) const {
    auto it = resources_.find(resource_id);
    RAY_CHECK(it != resources_.end()) << "Resource ID not found " << resource_id;
    return it->second;
  }

  /// Get the per-instance double values of a particular resource.
  /// NOTE: the resource MUST already exist in this TaskResourceInstances, otherwise a
  /// check fail will occur.
  std::vector<double> GetDouble(const ResourceID resource_id) const {
    return FixedPointVectorToDouble(Get(resource_id));
  }

  /// Get the sum of per-instance values of a particular resource.
  /// If the resource doesn't exist, return 0.
  FixedPoint Sum(const ResourceID resource_id) const {
    if (Has(resource_id)) {
      return FixedPoint::Sum(Get(resource_id));
    } else {
      return FixedPoint(0);
    }
  }

  /// Get the mutable per-instance values of a particular resource.
  /// NOTE: the resource MUST already exist in this TaskResourceInstances, otherwise a
  /// check fail will occur.
  /// TODO(hchen): We should hide this method, and encapsulate all mutation operations.
  std::vector<FixedPoint> &GetMutable(const ResourceID resource_id) {
    auto it = resources_.find(resource_id);
    RAY_CHECK(it != resources_.end()) << "Resource ID not found " << resource_id;
    return it->second;
  }

  /// Check whether a particular resource exists.
  bool Has(ResourceID resource_id) const { return resources_.contains(resource_id); }

  /// Set the per-instance values for a particular resource.
  TaskResourceInstances &Set(const ResourceID resource_id,
                             const std::vector<FixedPoint> &instances) {
    if (instances.size() == 0) {
      Remove(resource_id);
    } else {
      resources_[resource_id] = instances;
    }
    return *this;
  }

  /// Add values for each instance of the given resource.
  /// Note, if the number of instances in this set is less than the given instance
  /// vector, more instances will be appended to match the number.
  void Add(const ResourceID resource_id, const std::vector<FixedPoint> &instances) {
    if (!Has(resource_id)) {
      Set(resource_id, instances);
    } else {
      auto &resource_instances = GetMutable(resource_id);
      if (resource_instances.size() <= instances.size()) {
        resource_instances.resize(instances.size());
      }
      for (size_t i = 0; i < instances.size(); ++i) {
        resource_instances[i] += instances[i];
      }
    }
  }

  /// Remove a particular resource.
  void Remove(ResourceID resource_id) { resources_.erase(resource_id); }

  /// Return a boost::range object that can be used as an iterator of the resource IDs.
  ResourceIdIterator ResourceIds() const { return boost::adaptors::keys(resources_); }

  /// Return the number of resources in this set.
  size_t Size() const { return resources_.size(); }

  /// Check whether this set is empty.
  bool IsEmpty() const { return resources_.empty(); }

  bool operator==(const TaskResourceInstances &other) const {
    return this->resources_ == other.resources_;
  }

  /// Return a ResourceRequest with the aggregated per-instance values.
  ResourceRequest ToResourceRequest() const {
    ResourceRequest resource_request;
    for (auto &resource_id : ResourceIds()) {
      resource_request.Set(resource_id, Sum(resource_id));
    }
    return resource_request;
  }

  /// Returns human-readable string for these resources.
  [[nodiscard]] std::string DebugString() const {
    std::stringstream buffer;
    buffer << "{";
    bool first = true;
    for (auto &resource_id : ResourceIds()) {
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
      auto resource_id = ResourceID(i);
      if (!Has(resource_id)) {
        continue;
      }
      auto &resource = Get(resource_id);
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
  /// Map from the resource IDs to the resource values.
  absl::flat_hash_map<ResourceID, std::vector<FixedPoint>> resources_;
};

/// Total and available capacities of each resource of a node.
class NodeResources {
 public:
  NodeResources() {}
  NodeResources(const ResourceRequest &request) : total(request), available(request) {}
  NodeResources(const NodeResources &other)
      : total(other.total),
        available(other.available),
        load(other.load),
        normal_task_resources(other.normal_task_resources),
        latest_resources_normal_task_timestamp(
            other.latest_resources_normal_task_timestamp),
        object_pulls_queued(other.object_pulls_queued) {}
  ResourceRequest total;
  ResourceRequest available;
  /// Only used by light resource report.
  ResourceRequest load;
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
