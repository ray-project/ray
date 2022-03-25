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

#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "ray/common/id.h"
#include "ray/raylet/format/node_manager_generated.h"
#include "ray/raylet/scheduling/cluster_resource_data.h"

namespace ray {

/// Conversion factor that is the amount in internal units is equivalent to
/// one actual resource. Multiply to convert from actual to internal and
/// divide to convert from internal to actual.
constexpr double kResourceConversionFactor = 10000;

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
  explicit ResourceSet(const absl::flat_hash_map<std::string, FixedPoint> &resource_map);

  /// \brief Constructs ResourceSet from the specified resource map.
  explicit ResourceSet(const absl::flat_hash_map<std::string, double> &resource_map);

  /// \brief Constructs ResourceSet from two equal-length vectors with label and capacity
  /// specification.
  ResourceSet(const std::vector<std::string> &resource_labels,
              const std::vector<double> resource_capacity);

  /// \brief Empty ResourceSet destructor.
  ~ResourceSet();

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

  /// \brief Delete a resource from the resource set.
  ///
  /// \param resource_name: name/label of the resource to delete.
  /// \return True if the resource was found while deleting, false if the resource did not
  /// exist in the set.
  bool DeleteResource(const std::string &resource_name);

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

  /// Return the capacity value associated with the specified resource.
  ///
  /// \param resource_name: Resource name for which capacity is requested.
  /// \return The capacity value associated with the specified resource, zero if resource
  /// does not exist.
  FixedPoint GetResource(const std::string &resource_name) const;

  /// Return the number of CPUs.
  ///
  /// \return Number of CPUs.
  const ResourceSet GetNumCpus() const;

  /// Return the number of CPUs.
  ///
  /// \return Number of CPUs.
  double GetNumCpusAsDouble() const;

  /// Return true if the resource set is empty. False otherwise.
  ///
  /// \return True if the resource capacity is zero. False otherwise.
  bool IsEmpty() const;

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

  /// \brief Return a map of the resource and size in FixedPoint. Note,
  /// size is in kResourceConversionFactor of a unit.
  ///
  /// \return map of resource in string to size in FixedPoint.
  const absl::flat_hash_map<std::string, FixedPoint> &GetResourceAmountMap() const;

  const std::string ToString() const;

 private:
  /// Resource capacity map.
  absl::flat_hash_map<std::string, FixedPoint> resource_capacity_;
};

/// \class SchedulingResources
/// SchedulingResources class encapsulates the state of all local resources and
/// manages accounting of those resources. Resources include configured resource
/// bundle capacity, and GPU allocation map.
class SchedulingResources {
 public:
  /// SchedulingResources constructor: sets configured and available resources
  /// to an empty set.
  SchedulingResources();

  /// SchedulingResources constructor: sets available and configured capacity
  /// to the resource set specified.
  ///
  /// \param total: The amount of total configured capacity.
  SchedulingResources(const ResourceSet &total);

  /// \brief SchedulingResources destructor.
  ~SchedulingResources();

  /// \brief Request the set and capacity of resources currently available.
  ///
  /// \return Immutable set of resources with currently available capacity.
  const ResourceSet &GetAvailableResources() const;

  /// \brief Overwrite available resource capacity with the specified resource set.
  ///
  /// \param newset: The set of resources that replaces available resource capacity.
  /// \return Void.
  void SetAvailableResources(ResourceSet &&newset);

  /// \brief Request the total resources capacity.
  ///
  /// \return Immutable set of resources with currently total capacity.
  const ResourceSet &GetTotalResources() const;

  /// \brief Overwrite total resource capacity with the specified resource set.
  ///
  /// \param newset: The set of resources that replaces total resource capacity.
  /// \return Void.
  void SetTotalResources(ResourceSet &&newset);

  /// \brief Request the resource load information.
  ///
  /// \return Immutable set of resources describing the load information.
  const ResourceSet &GetLoadResources() const;

  /// \brief Overwrite information about resource load with new resource load set.
  ///
  /// \param newset: The set of resources that replaces resource load information.
  /// \return Void.
  void SetLoadResources(ResourceSet &&newset);

  /// \brief Release the amount of resources specified.
  ///
  /// \param resources: the amount of resources to be released.
  /// \return Void.
  void Release(const ResourceSet &resources);

  /// \brief Acquire the amount of resources specified.
  ///
  /// \param resources: the amount of resources to be acquired.
  /// \return Void.
  void Acquire(const ResourceSet &resources);

  /// \brief Add a new resource to available resource.
  ///
  /// \param resources: the amount of resources to be added.
  /// \return Void.
  void AddResource(const ResourceSet &resources);

  /// Returns debug string for class.
  ///
  /// \return string.
  std::string DebugString() const;

  /// \brief Update total, available and load resources with the specified capacity.
  /// Create if not exists.
  ///
  /// \param resource_name: Name of the resource to be modified
  /// \param capacity: New capacity of the resource.
  /// \return Void.
  void UpdateResourceCapacity(const std::string &resource_name, int64_t capacity);

  /// \brief Delete resource from total, available and load resources.
  ///
  /// \param resource_name: Name of the resource to be deleted.
  /// \return Void.
  void DeleteResource(const std::string &resource_name);

  /// \brief Get the resources used by normal tasks.
  ///
  /// \return Resources used by normal tasks.
  const ResourceSet &GetNormalTaskResources() const;

  /// \brief Set the amount of resources used by normal tasks.
  ///
  /// \param newset: The new resource set to update.
  /// \return Void.
  void SetNormalTaskResources(const ResourceSet &newset);

 private:
  /// Static resource configuration (e.g., static_resources).
  ResourceSet resources_total_;
  /// Dynamic resource capacity (e.g., dynamic_resources).
  ResourceSet resources_available_;
  /// Resource load.
  ResourceSet resources_load_;
  /// Resources used by normal tasks.
  ResourceSet resources_normal_tasks_;
};

std::string format_resource(std::string resource_name, double quantity);

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
