// Copyright 2023 The Ray Authors.
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

#include "absl/container/flat_hash_map.h"
#include "ray/common/scheduling/fixed_point.h"
#include "ray/common/scheduling/resource_set.h"
#include "ray/common/scheduling/scheduling_ids.h"

namespace ray {

/// Represents a node resource set that contains the per-instance resource values.
class NodeResourceInstanceSet {
 public:
  NodeResourceInstanceSet(){};

  /// Construct a NodeResourceInstanceSet from a node total resources.
  NodeResourceInstanceSet(const NodeResourceSet &total);

  /// Check whether a particular node resource exist.
  bool Has(ResourceID resource_id) const;

  /// Get the per-instance values of a node resource.
  /// If the resource doesn't exist, an empty vector is returned.
  const std::vector<FixedPoint> &Get(ResourceID resource_id) const;

  /// Set a node resource to the given per-instance values.
  NodeResourceInstanceSet &Set(ResourceID resource_id, std::vector<FixedPoint> instances);

  /// Remove the specified resource.
  void Remove(ResourceID resource_id);

  /// Get the sum of per-instance values of a particular resource.
  /// If the resource doesn't exist, return 0.
  FixedPoint Sum(ResourceID resource_id) const;

  /// Check whether two node resource sets are equal meaning
  /// they have the same resources and instances.
  bool operator==(const NodeResourceInstanceSet &other) const;

  std::string DebugString() const;

  /// Try to allocate resources specified by `resource_demands`.
  /// This operation is all or nothing meaning that if any single resource
  /// cannot be allocated, the entire allocation fails and std::nullopt is returned.
  std::optional<absl::flat_hash_map<ResourceID, std::vector<FixedPoint>>> TryAllocate(
      const ResourceSet &resource_demands);

  /// Free allocated resources and add them back to this set.
  void Free(ResourceID resource_id, const std::vector<FixedPoint> &allocation);

  /// Add values for each instance of the given resource.
  /// Note, if the number of instances in this set is less than the given instance
  /// vector, more instances will be appended to match the number.
  void Add(ResourceID resource_id, const std::vector<FixedPoint> &instances);

  /// Decrease the capacities of the instances of a given resource.
  ///
  /// \param resource_id The id of the resource to be subtracted.
  /// \param instances A list of capacities for resource's instances to be subtracted.
  /// \param allow_going_negative Allow the values to go negative (disable underflow).
  ///
  /// \return Underflow of resource capacities after subtracting instance
  /// capacities in "instances", i.e.,.
  /// max(instances - Get(resource_id), 0)
  std::vector<FixedPoint> Subtract(ResourceID resource_id,
                                   const std::vector<FixedPoint> &instances,
                                   bool allow_going_negative);

  /// Convert to node resource set with summed per-instance values.
  NodeResourceSet ToNodeResourceSet() const;

  /// Only for testing.
  const absl::flat_hash_map<ResourceID, std::vector<FixedPoint>> &Resources() const {
    return resources_;
  }

 private:
  /// Allocate enough capacity across the instances of a resource to satisfy "demand".
  ///
  /// Allocate full unit-capacity instances until
  /// demand becomes fractional, and then satisfy the fractional demand using the
  /// instance with the smallest available capacity that can satisfy the fractional
  /// demand. For example, assume a resource conisting of 4 instances, with available
  /// capacities: (1., 1., .7, 0.5) and deman of 1.2. Then we allocate one full
  /// instance and then allocate 0.2 of the 0.5 instance (as this is the instance
  /// with the smalest available capacity that can satisfy the remaining demand of 0.2).
  /// As a result remaining available capacities will be (0., 1., .7, .3).
  /// Thus, we will allocate a bunch of full instances and
  /// at most a fractional instance.
  ///
  /// \param resource_id: The id of the resource to be allocated.
  /// \param demand: The resource amount to be allocated.
  ///
  /// \return the allocated instances, if allocation successful. Else, return nullopt.
  std::optional<std::vector<FixedPoint>> TryAllocate(ResourceID resource_id,
                                                     FixedPoint demand);

  /// Map from the resource IDs to the resource instance values.
  absl::flat_hash_map<ResourceID, std::vector<FixedPoint>> resources_;
};

}  // namespace ray
