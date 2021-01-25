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
#include "ray/common/task/scheduling_resources.h"
#include "ray/raylet/scheduling/fixed_point.h"
#include "ray/raylet/scheduling/scheduling_ids.h"
#include "ray/util/logging.h"

/// List of predefined resources.
enum PredefinedResources { CPU, MEM, GPU, TPU, PredefinedResources_MAX };

const std::string ResourceEnumToString(PredefinedResources resource);

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
};

/// Capacities of each instance of a resource.
struct ResourceInstanceCapacities {
  std::vector<FixedPoint> total;
  std::vector<FixedPoint> available;
};

struct ResourceRequest {
  /// Amount of resource being requested.
  FixedPoint demand;
  /// Specify whether the request is soft or hard.
  /// If hard, the entire request is denied if the demand exceeds the resource
  /// availability. Otherwise, the request can be still be granted.
  /// Prefernces are given to the nodes with the lowest number of violations.
  bool soft;
};

/// Resource request, including resource ID. This is used for custom resources.
struct ResourceRequestWithId : ResourceRequest {
  /// Resource ID.
  int64_t id;
};

// Data structure specifying the capacity of each resource requested by a task.
class TaskRequest {
 public:
  /// List of predefined resources required by the task.
  std::vector<ResourceRequest> predefined_resources;
  /// List of custom resources required by the task.
  std::vector<ResourceRequestWithId> custom_resources;
  /// List of placement hints. A placement hint is a node on which
  /// we desire to run this task. This is a soft constraint in that
  /// the task will run on a different node in the cluster, if none of the
  /// nodes in this list can schedule this task.
  absl::flat_hash_set<int64_t> placement_hints;
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
  /// For each resource of this request aggregate its instances.
  TaskRequest ToTaskRequest() const;
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
  /// Check whether there are no resource instances.
  bool IsEmpty() const;
  /// Returns human-readable string for these resources.
  std::string DebugString() const;
};

/// Total and available capacities of each resource of a node.
class NodeResources {
 public:
  NodeResources() {}
  NodeResources(const NodeResources &other)
      : predefined_resources(other.predefined_resources),
        custom_resources(other.custom_resources) {}
  /// Available and total capacities for predefined resources.
  std::vector<ResourceCapacity> predefined_resources;
  /// Map containing custom resources. The key of each entry represents the
  /// custom resource ID.
  absl::flat_hash_map<int64_t, ResourceCapacity> custom_resources;
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
  std::string DebugString(StringIdMap string_to_int_map) const;
};

/// \request Conversion result to a TaskRequest data structure.
NodeResources ResourceMapToNodeResources(
    StringIdMap &string_to_int_map,
    const std::unordered_map<std::string, double> &resource_map_total,
    const std::unordered_map<std::string, double> &resource_map_available);

/// Convert a map of resources to a TaskRequest data structure.
TaskRequest ResourceMapToTaskRequest(
    StringIdMap &string_to_int_map,
    const std::unordered_map<std::string, double> &resource_map);
