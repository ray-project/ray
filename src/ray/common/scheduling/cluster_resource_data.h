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
#include "ray/common/scheduling/fixed_point.h"
#include "ray/common/scheduling/resource_instance_set.h"
#include "ray/common/scheduling/resource_set.h"
#include "ray/common/scheduling/scheduling_ids.h"
#include "ray/util/logging.h"

namespace ray {

using scheduling::ResourceID;

/// Represents a request of resources.
class ResourceRequest {
 public:
  /// Construct an empty ResourceRequest.
  ResourceRequest() : ResourceRequest({}, false) {}

  /// Construct a ResourceRequest with a given resource map.
  ResourceRequest(absl::flat_hash_map<ResourceID, FixedPoint> resource_map)
      : ResourceRequest(resource_map, false){};

  ResourceRequest(absl::flat_hash_map<ResourceID, FixedPoint> resource_map,
                  bool requires_object_store_memory)
      : resources_(resource_map),
        requires_object_store_memory_(requires_object_store_memory) {}

  bool RequiresObjectStoreMemory() const { return requires_object_store_memory_; }

  const ResourceSet &GetResourceSet() const { return resources_; }

  FixedPoint Get(ResourceID resource_id) const { return resources_.Get(resource_id); }

  void Set(ResourceID resource_id, FixedPoint value) {
    resources_.Set(resource_id, value);
  }

  bool Has(ResourceID resource_id) const { return resources_.Has(resource_id); }

  ResourceSet::ResourceIdIterator ResourceIds() const { return resources_.ResourceIds(); }

  absl::flat_hash_map<std::string, double> ToResourceMap() const {
    return resources_.GetResourceMap();
  }

  bool IsEmpty() const { return resources_.IsEmpty(); }

  size_t Size() const { return resources_.Size(); }

  void Clear() { resources_.Clear(); }

  bool operator==(const ResourceRequest &other) const {
    return this->resources_ == other.resources_;
  }

  bool operator<=(const ResourceRequest &other) const {
    return this->resources_ <= other.resources_;
  }

  bool operator>=(const ResourceRequest &other) const {
    return this->resources_ >= other.resources_;
  }

  bool operator!=(const ResourceRequest &other) const {
    return this->resources_ != other.resources_;
  }

  ResourceRequest operator+(const ResourceRequest &other) const {
    ResourceRequest res = *this;
    res += other;
    return res;
  }

  ResourceRequest operator-(const ResourceRequest &other) const {
    ResourceRequest res = *this;
    res -= other;
    return res;
  }

  ResourceRequest &operator+=(const ResourceRequest &other) {
    resources_ += other.resources_;
    return *this;
  }

  ResourceRequest &operator-=(const ResourceRequest &other) {
    resources_ -= other.resources_;
    return *this;
  }

  /// Return a human-readable string for this set.
  std::string DebugString() const { return resources_.DebugString(); }

 private:
  ResourceSet resources_;
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

  /// Construct a TaskResourceInstances with the values from a ResourceSet.
  TaskResourceInstances(const ResourceSet &resources) {
    for (auto &resource_id : resources.ResourceIds()) {
      std::vector<FixedPoint> instances;
      auto value = resources.Get(resource_id);
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

  explicit TaskResourceInstances(
      absl::flat_hash_map<ResourceID, std::vector<FixedPoint>> resources)
      : resources_(std::move(resources)) {}

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

  /// Return a ResourceSet with the aggregated per-instance values.
  ResourceSet ToResourceSet() const {
    ResourceSet resource_set;
    for (auto &resource_id : ResourceIds()) {
      resource_set.Set(resource_id, Sum(resource_id));
    }
    return resource_set;
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
  NodeResources(const NodeResourceSet &resources)
      : total(resources), available(resources) {}
  NodeResourceSet total;
  NodeResourceSet available;
  /// Only used by light resource report.
  ResourceSet load;
  /// Resources owned by normal tasks.
  ResourceSet normal_task_resources;

  // The key-value labels of this node.
  absl::flat_hash_map<std::string, std::string> labels;

  // The idle duration of the node from resources reported by raylet.
  int64_t idle_resource_duration_ms = 0;

  // Whether the node is being drained or not.
  bool is_draining = false;

  // The timestamp of the last resource update if there was a resource report.
  absl::optional<absl::Time> last_resource_update_time = absl::nullopt;

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
  NodeResourceInstanceSet available;
  NodeResourceInstanceSet total;
  // The key-value labels of this node.
  absl::flat_hash_map<std::string, std::string> labels;

  /// Extract available resource instances.
  const NodeResourceInstanceSet &GetAvailableResourceInstances() const;
  const NodeResourceInstanceSet &GetTotalResourceInstances() const;
  /// Returns if this equals another node resources.
  bool operator==(const NodeResourceInstances &other);
  /// Returns human-readable string for these resources.
  [[nodiscard]] std::string DebugString() const;
};

struct Node {
  Node(const NodeResources &resources) : local_view_(resources) {}

  NodeResources *GetMutableLocalView() {
    local_view_modified_ts_ = absl::Now();
    return &local_view_;
  }

  std::optional<absl::Time> GetViewModifiedTs() const { return local_view_modified_ts_; }

  const NodeResources &GetLocalView() const { return local_view_; }

 private:
  /// Our local view of the remote node's resources. This may be dirty
  /// because it includes any resource requests that we allocated to this
  /// node through spillback since our last heartbeat tick. This view will
  /// get overwritten by the last reported view on each heartbeat tick, to
  /// make sure that our local view does not skew too much from the actual
  /// resources when light heartbeats are enabled.
  NodeResources local_view_;
  /// The timestamp this node got updated.
  std::optional<absl::Time> local_view_modified_ts_;
};

/// \request Conversion result to a ResourceRequest data structure.
NodeResources ResourceMapToNodeResources(
    const absl::flat_hash_map<std::string, double> &resource_map_total,
    const absl::flat_hash_map<std::string, double> &resource_map_available,
    const absl::flat_hash_map<std::string, std::string> &node_labels = {});

/// Convert a map of resources to a ResourceRequest data structure.
ResourceRequest ResourceMapToResourceRequest(
    const absl::flat_hash_map<std::string, double> &resource_map,
    bool requires_object_store_memory);

/// Convert a map of resources to a ResourceRequest data structure.
ResourceRequest ResourceMapToResourceRequest(
    const absl::flat_hash_map<ResourceID, double> &resource_map,
    bool requires_object_store_memory);

}  // namespace ray
