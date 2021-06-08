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
#include "ray/raylet/scheduling/fixed_point.h"
#include "ray/raylet/scheduling/scheduling_ids.h"
#include "ray/util/logging.h"

/// List of predefined resources.
enum PredefinedResources { CPU, MEM, GPU, OBJECT_STORE_MEM, PredefinedResources_MAX };

const std::string ResourceEnumToString(PredefinedResources resource);

/// Helper function to compare two vectors with FixedPoint values.
bool EqualVectors(const std::vector<FixedPoint> &v1, const std::vector<FixedPoint> &v2);

/// Convert a vector of doubles to a vector of resource units.
std::vector<FixedPoint> VectorDoubleToVectorFixedPoint(const std::vector<double> &vector);

/// Convert a vector of resource units to a vector of doubles.
std::vector<double> VectorFixedPointToVectorDouble(
    const std::vector<FixedPoint> &vector_fp);

/// \class ResourceSet
/// \brief Encapsulates and operates on a set of resources, including CPUs,
/// GPUs, and custom labels.
class ResourceSet {
 public:
  static std::shared_ptr<ResourceSet> Nil() {
    static auto nil = std::make_shared<ResourceSet>();
    return nil;
  }

  /// \brief empty ResourceSet constructor.
  ResourceSet();

  /// \brief Constructs ResourceSet from the specified resource map.
  ResourceSet(const std::unordered_map<std::string, FixedPoint> &resource_map);

  /// \brief Constructs ResourceSet from the specified resource map.
  ResourceSet(const std::unordered_map<std::string, double> &resource_map);

  /// \brief Constructs ResourceSet from two equal-length vectors with label and capacity
  /// specification.
  ResourceSet(const std::vector<std::string> &resource_labels,
              const std::vector<double> resource_capacity);

  /// \brief Empty ResourceSet destructor.
  ~ResourceSet() = default;

  /// \brief Test equality with the other specified ResourceSet object.
  ///
  /// \param rhs: Right-hand side object for equality comparison.
  /// \return True if objects are equal, False otherwise.
  bool operator==(const ResourceSet &rhs) const;

  /// \brief Test equality with the other specified ResourceSet object.
  ///
  /// \param other: Right-hand side object for equality comparison.
  /// \return True if objects are equal, False otherwise.
  bool IsEqual(const ResourceSet &other) const;

  /// \brief Get predefined resource quantity
  FixedPoint GetPredefinedResource(PredefinedResources resource) const;

  /// \brief Get predefined resource quantity
  FixedPoint GetPredefinedResource(int resource) const;

  /// \brief Get custom resource quantity
  FixedPoint GetCustomResource(int64_t resource_id) const;

  /// \brief Test whether this ResourceSet is a subset of the other ResourceSet.
  ///
  /// \param other: The resource set we check being a subset of.
  /// \return True if the current resource set is the subset of other. False
  /// otherwise.
  bool IsSubset(const ResourceSet &other) const;

  /// \brief Test if this ResourceSet is a superset of the other ResourceSet.
  ///
  /// \param other: The resource set we check being a superset of.
  /// \return True if the current resource set is the superset of other.
  /// False otherwise.
  bool IsSuperset(const ResourceSet &other) const;

  /// \brief Add or update a new resource to the resource set.
  ///
  /// \param resource_name: name/label of the resource to add.
  /// \param capacity: numeric capacity value for the resource to add.
  /// \return True, if the resource was successfully added. False otherwise.
  void AddOrUpdateResource(const std::string &resource_name, const FixedPoint &capacity);

  /// \brief [DEPARATED] Delete a resource from the resource set. This method is departed,
  /// kept just for back compatibility and will be removed in future. Please do not use
  /// it.
  ///
  /// \param resource_name: name/label of the resource to delete.
  /// \return True if the resource was found while deleting, false if the resource did not
  /// exist in the set.
  bool DeleteResource(const std::string &resource_name);

  /// \brief Getter method for custom_resources_
  const std::unordered_map<int64_t, FixedPoint> &GetCustomResourceAmountMap() const;

  /// \brief Add a set of resources to the current set of resources subject to upper
  /// limits on capacity from the total_resource set.
  ///
  /// \param other: The other resource set to add.
  /// \param total_resources: Total resource set which sets upper limits on capacity for
  /// each label.
  void AddResourcesCapacityConstrained(const ResourceSet &other,
                                       const ResourceSet &total_resources);

  /// \brief Aggregate resources from the other set into this set, adding any missing
  /// resource labels to this set.
  ///
  /// \param other: The other resource set to add.
  /// \return Void.
  void AddResources(const ResourceSet &other);

  /// \brief Subtract a set of resources from the current set of resources and
  /// check that the post-subtraction result nonnegative. Assumes other
  /// is a subset of the ResourceSet. Deletes any resource if the capacity after
  /// subtraction is zero.
  ///
  /// \param other: The resource set to subtract from the current resource set.
  /// \return Void.
  void SubtractResources(const ResourceSet &other);

  /// \brief Same as SubtractResources but throws an error if the resource value
  /// goes below zero.
  ///
  /// \param other: The resource set to subtract from the current resource set.
  /// \return Void.
  void SubtractResourcesStrict(const ResourceSet &other);

  /// \brief [DEPARATED] Return the capacity value associated with the specified
  /// resource.This method is departed, kept just for back compatibility and will be
  /// removed in future. Please do not use it.
  ///
  /// \param resource_name: Resource name for which capacity is requested.
  /// \return The capacity value associated with the specified resource, zero if resource
  /// does not exist.
  FixedPoint GetResource(const std::string &resource_name) const;

  /// \brief Set predefined resource quantity by index.
  void SetPredefinedResources(PredefinedResources resource, FixedPoint quantity);

  /// Return the number of CPUs.
  ///
  /// \return Number of CPUs.
  const ResourceSet GetNumCpus() const;

  /// Return true if the resource set is empty. False otherwise.
  ///
  /// \return True if the resource capacity is zero. False otherwise.
  bool IsEmpty() const;

  /// \brief [DEPARATED] Return a map of the resource and size in doubles. This method is
  /// departed, kept just for back compatibility and will be removed in future. Please do
  /// not use it.
  ///
  /// \return map of resource in string to size in double.
  const std::unordered_map<std::string, double> GetResourceMap() const;

  /// \brief [DEPARATED] Return a map of the resource and size in FixedPoint. This method
  /// is departed, kept just for back compatibility and will be removed in future. Please
  /// do not use it.
  ///
  /// \return map of resource in string to size in FixedPoint.
  const std::unordered_map<std::string, FixedPoint> GetResourceAmountMap() const;

  const std::string ToString() const;

 private:
  void FillByResourceMap(const std::unordered_map<std::string, FixedPoint> &resource_map);
  /// List of predefined resources
  std::vector<FixedPoint> predefined_resources_;
  /// Key: resource ID, Value: quantity
  std::unordered_map<int64_t, FixedPoint> custom_resources_;
  /// Map between resource string name and integer ID
  StringIdMap string_id_map_;
};

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
class TaskRequest {
 public:
  /// List of predefined resources required by the task.
  std::vector<FixedPoint> predefined_resources;
  /// List of custom resources required by the task.
  std::unordered_map<int64_t, FixedPoint> custom_resources;
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
  /// Clear only the CPU instances field.
  void ClearCPUInstances();
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
  /// Amongst CPU, memory, and object store memory, calculate the utilization percentage
  /// of each resource and return the highest.
  float CalculateCriticalResourceUtilization() const;
  /// Returns true if the node has the available resources to run the task.
  /// Note: This doesn't account for the binpacking of unit resources.
  bool IsAvailable(const TaskRequest &task_req) const;
  /// Returns true if the node's total resources are enough to run the task.
  /// Note: This doesn't account for the binpacking of unit resources.
  bool IsFeasible(const TaskRequest &task_req) const;
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

struct Node {
  Node(const NodeResources &resources)
      : last_reported_(resources), local_view_(resources) {}

  void ResetLocalView() { local_view_ = last_reported_; }

  NodeResources *GetMutableLocalView() { return &local_view_; }

  const NodeResources &GetLocalView() const { return local_view_; }

 private:
  /// The resource information according to the last heartbeat reported by
  /// this node.
  /// NOTE(swang): For the local node, this field should be ignored because
  /// we do not receive heartbeats from ourselves and the local view is
  /// therefore always the most up-to-date.
  NodeResources last_reported_;
  /// Our local view of the remote node's resources. This may be dirty
  /// because it includes any resource requests that we allocated to this
  /// node through spillback since our last heartbeat tick. This view will
  /// get overwritten by the last reported view on each heartbeat tick, to
  /// make sure that our local view does not skew too much from the actual
  /// resources when light heartbeats are enabled.
  NodeResources local_view_;
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

namespace std {
template <>
struct hash<ResourceSet> {
  size_t operator()(ResourceSet const &k) const {
    size_t seed = k.GetResourceMap().size();
    for (auto &elem : k.GetResourceMap()) {
      seed ^= std::hash<std::string>()(elem.first);
      seed ^= std::hash<double>()(elem.second);
    }
    return seed;
  }
};
}  // namespace std
