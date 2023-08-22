// Copyright 2019-2021 The Ray Authors.
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
#include <string>
#include <unordered_map>

#include "absl/container/flat_hash_map.h"
#include "ray/common/scheduling/fixed_point.h"
#include "ray/common/scheduling/scheduling_ids.h"

namespace ray {

using scheduling::ResourceID;

/// Represents a set of resources and their values.
/// If any resource value is changed to 0, the resource will be removed.
class ResourceSet {
 public:
  using ResourceIdIterator =
      boost::select_first_range<absl::flat_hash_map<ResourceID, FixedPoint>>;

  static std::shared_ptr<ResourceSet> Nil() {
    static auto nil = std::make_shared<ResourceSet>();
    return nil;
  }

  /// \brief Empty ResourceSet constructor.
  ResourceSet(){};

  /// \brief Constructs ResourceSet from the specified resource map.
  explicit ResourceSet(const absl::flat_hash_map<std::string, FixedPoint> &resource_map);

  /// \brief Constructs ResourceSet from the specified resource map.
  explicit ResourceSet(const absl::flat_hash_map<std::string, double> &resource_map);

  explicit ResourceSet(const absl::flat_hash_map<ResourceID, FixedPoint> &resource_map);

  explicit ResourceSet(const absl::flat_hash_map<ResourceID, double> &resource_map);

  /// \brief Test equality with the other specified ResourceSet object.
  ///
  /// \param other: Right-hand side object for equality comparison.
  /// \return True if objects are equal, False otherwise.
  bool operator==(const ResourceSet &other) const;

  /// Add other's resource quantity to this one and return a new ResourceSet.
  ResourceSet operator+(const ResourceSet &other) const;

  /// Subtract other's resource quantity from this one and return a new ResourceSet.
  ResourceSet operator-(const ResourceSet &other) const;

  /// Add other's resource quantity to this one.
  ResourceSet &operator+=(const ResourceSet &other);

  /// Subtract other's resource quantity from this one.
  ResourceSet &operator-=(const ResourceSet &other);

  /// Test inequality with the other specified ResourceSet object.
  bool operator!=(const ResourceSet &other) const { return !(*this == other); }

  /// Check whether this set is a subset of another one.
  /// If A <= B, it means for each resource, its value in A is less than or equqal to that
  /// in B.
  bool operator<=(const ResourceSet &other) const;

  /// Check whether this set is a super set of another one.
  /// If A >= B, it means for each resource, its value in A is larger than or equqal to
  /// that in B.
  bool operator>=(const ResourceSet &other) const { return other <= *this; }

  /// Return the quantity value associated with the specified resource.
  /// If the resource doesn't exist, return 0.
  ///
  /// \param resource_id: Resource id for which quantity value is requested.
  /// \return The quantity value associated with the specified resource, zero if resource
  /// does not exist.
  FixedPoint Get(ResourceID resource_id) const;

  /// Set a resource to the given value.
  /// NOTE: if the new value is 0, the resource will be removed.
  ResourceSet &Set(ResourceID resource_id, FixedPoint value);

  /// Check whether a particular resource exist.
  bool Has(ResourceID resource_id) const { return resources_.contains(resource_id); }

  /// Return the number of resources in this set.
  size_t Size() const { return resources_.size(); }

  /// Clear the whole set.
  void Clear() { resources_.clear(); }

  /// Return true if the resource set is empty. False otherwise.
  bool IsEmpty() const;

  /// Return a boost::range object that can be used as an iterator of the resource IDs.
  ResourceIdIterator ResourceIds() const { return boost::adaptors::keys(resources_); }

  /// Returns the underlying resource map.
  const absl::flat_hash_map<ResourceID, FixedPoint> &Resources() const {
    return resources_;
  }

  // TODO(atumanov): implement const_iterator class for the ResourceSet container.
  // TODO(williamma12): Make sure that everywhere we use doubles we don't
  // convert it back to FixedPoint.
  /// \brief Return a map of the resource and size in doubles. Note, size is in
  /// regular units and does not need to be multiplied by kResourceConversionFactor.
  ///
  /// \return map of resource in string to size in double.
  absl::flat_hash_map<std::string, double> GetResourceMap() const;

  /// Return the resources in unordered map. This is used for some languate frontend that
  /// requires unordered map instead of flat hash map.
  std::unordered_map<std::string, double> GetResourceUnorderedMap() const;

  const std::string DebugString() const;

 private:
  /// Map from the resource IDs to the resource values.
  absl::flat_hash_map<ResourceID, FixedPoint> resources_;
};

/// Represents a set of node resources and their values.
/// Node resources contain both explicit resources (default value is 0)
/// and implicit resources (default value is 1).
/// Negative values are valid in this set.
class NodeResourceSet {
 public:
  using ResourceIdIterator =
      boost::select_first_range<absl::flat_hash_map<ResourceID, FixedPoint>>;

  NodeResourceSet(){};

  /// Constructs NodeResourceSet from the specified resource map.
  explicit NodeResourceSet(const absl::flat_hash_map<std::string, double> &resource_map);
  explicit NodeResourceSet(const absl::flat_hash_map<ResourceID, double> &resource_map);
  explicit NodeResourceSet(
      const absl::flat_hash_map<ResourceID, FixedPoint> &resource_map);

  /// Set a node resource to the given value.
  NodeResourceSet &Set(ResourceID resource_id, FixedPoint value);

  /// Get the value of a node resource.
  FixedPoint Get(ResourceID resource_id) const;

  /// Check whether a particular node resource exist (value != 0).
  bool Has(ResourceID resource_id) const;

  /// Subtract other's resources from this node resource set.
  NodeResourceSet &operator-=(const ResourceSet &other);

  /// Check whether this set is a super set of other.
  /// If A >= B, it means for each resource, its value in A is larger than or equqal to
  /// that in B.
  bool operator>=(const ResourceSet &other) const;

  /// Check whether two node resource sets are equal meaning
  /// they have the same resources and values.
  bool operator==(const NodeResourceSet &other) const;

  /// Check whether two node resource sets are not equal.
  bool operator!=(const NodeResourceSet &other) const { return !(*this == other); }

  /// Remove the negative values in this set.
  void RemoveNegative();

  /// Return a map of the node resource and value in doubles.
  absl::flat_hash_map<std::string, double> GetResourceMap() const;

  /// Return all the ids of explicit resources that this set has.
  std::set<ResourceID> ExplicitResourceIds() const;

  std::string DebugString() const;

 private:
  /// Return the default value for a resource depending on whether
  /// the resource is the explicit or implicit resource.
  FixedPoint ResourceDefaultValue(ResourceID resource_id) const;

  /// Map from the resource IDs to the resource values.
  /// If the resource value is the default value for the resource
  /// it will be removed from the map.
  absl::flat_hash_map<ResourceID, FixedPoint> resources_;
};

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

namespace std {
template <>
struct hash<ray::ResourceSet> {
  size_t operator()(ray::ResourceSet const &k) const {
    size_t seed = k.GetResourceMap().size();
    for (auto &elem : k.GetResourceMap()) {
      seed ^= std::hash<std::string>()(elem.first);
      seed ^= std::hash<double>()(elem.second);
    }
    return seed;
  }
};
}  // namespace std
