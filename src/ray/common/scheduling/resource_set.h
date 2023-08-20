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
/// NOTE: negative values are valid in this set, while 0 is not. This means if any
/// resource value is changed to 0, the resource will be removed.
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

  /// Clear the whole set.
  void Clear() { resources_.clear(); }

  /// Return true if the resource set is empty. False otherwise.
  bool IsEmpty() const;

  /// Return a boost::range object that can be used as an iterator of the resource IDs.
  ResourceIdIterator ResourceIds() const { return boost::adaptors::keys(resources_); }

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

  explicit NodeResourceSet(const absl::flat_hash_map<std::string, double> &resource_map);

  NodeResourceSet &Set(ResourceID resource_id, FixedPoint value);

  FixedPoint Get(ResourceID resource_id) const;

  bool Has(ResourceID resource_id) const;

  NodeResourceSet &operator-=(const ResourceSet &other);

  bool operator>=(const ResourceSet &other) const;

  bool operator==(const NodeResourceSet &other) const;

  /// Remove the negative values in this set.
  void RemoveNegative();

  absl::flat_hash_map<std::string, double> GetResourceMap() const;

  std::set<ResourceID> ExplicitResourceIds() const;

  std::string DebugString() const;

 private:
  FixedPoint ResourceDefaultValue(ResourceID resource_id) const;

  /// Map from the resource IDs to the resource values.
  /// If the resource value is the default value for the resource
  /// it will be removed from the map.
  absl::flat_hash_map<ResourceID, FixedPoint> resources_;
};

class NodeResourceInstanceSet {
 public:
  NodeResourceInstanceSet(){};

  /// Construct a TaskResourceInstances from a node total resources.
  NodeResourceInstanceSet(const NodeResourceSet &total);

  NodeResourceInstanceSet &Set(const ResourceID resource_id,
                               const std::vector<FixedPoint> &instances);

  void Remove(ResourceID resource_id);

  bool operator==(const NodeResourceInstanceSet &other) const;

  std::string DebugString() const;

 private:
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
