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

/// List of predefined resources.
enum PredefinedResources { CPU, MEM, GPU, OBJECT_STORE_MEM, PredefinedResources_MAX };

const std::string ResourceEnumToString(PredefinedResources resource);

const PredefinedResources ResourceStringToEnum(const std::string &resource);

/// Helper function to compare two vectors with FixedPoint values.
bool EqualVectors(const std::vector<FixedPoint> &v1, const std::vector<FixedPoint> &v2);

/// Convert a vector of doubles to a vector of resource units.
std::vector<FixedPoint> VectorDoubleToVectorFixedPoint(const std::vector<double> &vector);

/// Convert a vector of resource units to a vector of doubles.
std::vector<double> VectorFixedPointToVectorDouble(
    const std::vector<FixedPoint> &vector_fp);

struct ResourceCapacity {
  FixedPoint total;
  FixedPoint available;
  ResourceCapacity() {}
  ResourceCapacity(FixedPoint &&_available, FixedPoint &&_total)
      : total(_total), available(_available) {}
};

/// Capacities of each instance of a resource.
struct ResourceInstanceCapacities {
  std::vector<FixedPoint> total;
  std::vector<FixedPoint> available;
};

// Data structure specifying the capacity of each resource requested by a task.
class ResourceRequest {
 public:
  /// List of predefined resources required by the task.
  std::vector<FixedPoint> predefined_resources;
  /// List of custom resources required by the task.
  absl::flat_hash_map<int64_t, FixedPoint> custom_resources;
  /// Whether this task requires object store memory.
  /// TODO(swang): This should be a quantity instead of a flag.
  bool requires_object_store_memory = false;
  /// Check whether the request contains no resources.
  bool IsEmpty() const;
  /// Returns human-readable string for this task request.
  std::string DebugString() const;
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
  const std::vector<FixedPoint> &Get(const std::string &resource_name,
                                     const StringIdMap &string_id_map) const;
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
  [[nodiscard]] std::string DebugString(const StringIdMap &string_id_map) const;
};

/// Total and available capacities of each resource of a node.
class NodeResources {
 public:
  NodeResources() {}
  NodeResources(const NodeResources &other)
      : predefined_resources(other.predefined_resources),
        custom_resources(other.custom_resources),
        object_pulls_queued(other.object_pulls_queued) {}
  /// Available and total capacities for predefined resources.
  std::vector<ResourceCapacity> predefined_resources;
  /// Map containing custom resources. The key of each entry represents the
  /// custom resource ID.
  absl::flat_hash_map<int64_t, ResourceCapacity> custom_resources;
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
  std::string DebugString(StringIdMap string_to_int_map) const;
  /// Returns compact dict-like string.
  std::string DictString(StringIdMap string_to_int_map) const;
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
  [[nodiscard]] std::string DebugString(StringIdMap string_to_int_map) const;
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
    StringIdMap &string_to_int_map,
    const absl::flat_hash_map<std::string, double> &resource_map_total,
    const absl::flat_hash_map<std::string, double> &resource_map_available);

/// Convert a map of resources to a ResourceRequest data structure.
ResourceRequest ResourceMapToResourceRequest(
    StringIdMap &string_to_int_map,
    const absl::flat_hash_map<std::string, double> &resource_map,
    bool requires_object_store_memory);

}  // namespace ray
