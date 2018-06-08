#ifndef RAY_RAYLET_SCHEDULING_RESOURCES_H
#define RAY_RAYLET_SCHEDULING_RESOURCES_H

#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace ray {

namespace raylet {

const std::string kCPU_ResourceLabel = "CPU";

/// Resource availability status reports whether the resource requirement is
/// (1) infeasible, (2) feasible but currently unavailable, or (3) available.
enum class ResourceAvailabilityStatus : int {
  kInfeasible,            ///< Cannot ever satisfy resource requirements.
  kResourcesUnavailable,  ///< Feasible, but not currently available.
  kFeasible               ///< Feasible and currently available.
};

/// \class ResourceSet
/// \brief Encapsulates and operates on a set of resources, including CPUs,
/// GPUs, and custom labels.
class ResourceSet {
 public:
  /// \brief empty ResourceSet constructor.
  ResourceSet();

  /// \brief Constructs ResourceSet from the specified resource map.
  ResourceSet(const std::unordered_map<std::string, double> &resource_map);

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
  ///          otherwise.
  bool IsSubset(const ResourceSet &other) const;

  /// \brief Test if this ResourceSet is a superset of the other ResourceSet.
  ///
  /// \param other: The resource set we check being a superset of.
  /// \return True if the current resource set is the superset of other.
  ///         False otherwise.
  bool IsSuperset(const ResourceSet &other) const;

  /// \brief Add a new resource to the resource set.
  ///
  /// \param resource_name: name/label of the resource to add.
  /// \param capacity: numeric capacity value for the resource to add.
  /// \return True, if the resource was successfully added. False otherwise.
  bool AddResource(const std::string &resource_name, double capacity);

  /// \brief Remove the specified resource from the resource set.
  ///
  /// \param resource_name: name/label of the resource to remove.
  /// \return True, if the resource was successfully removed. False otherwise.
  bool RemoveResource(const std::string &resource_name);

  /// \brief Add a set of resources to the current set of resources.
  ///
  /// \param other: The other resource set to add.
  /// \return True if the resource set was added successfully. False otherwise.
  bool AddResources(const ResourceSet &other);

  /// \brief Subtract a set of resources from the current set of resources.
  ///
  /// \param other: The resource set to subtract from the current resource set.
  /// \return True if the resource set was subtracted successfully.
  ///         False otherwise.
  bool SubtractResources(const ResourceSet &other);

  /// Return the capacity value associated with the specified resource.
  ///
  /// \param resource_name: Resource name for which capacity is requested.
  /// \param[out] value: Resource capacity value.
  /// \return True if the resource capacity value was successfully retrieved.
  ///         False otherwise.
  bool GetResource(const std::string &resource_name, double *value) const;

  /// Return the number of CPUs.
  ///
  /// \return Number of CPUs.
  double GetNumCpus() const;

  /// Return true if the resource set is empty. False otherwise.
  ///
  /// \return True if the resource capacity is zero. False otherwise.
  bool IsEmpty() const;

  // TODO(atumanov): implement const_iterator class for the ResourceSet container.
  const std::unordered_map<std::string, double> &GetResourceMap() const;

  const std::string ToString() const;

 private:
  /// Resource capacity map.
  std::unordered_map<std::string, double> resource_capacity_;
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

  /// \brief Check if the specified resource request can be satisfied.
  ///
  /// \param set: The set of resources representing the resource request.
  /// \return Availability status that specifies if the requested resource set
  ///         is feasible, infeasible, or feasible but unavailable.
  ResourceAvailabilityStatus CheckResourcesSatisfied(ResourceSet &set) const;

  /// \brief Request the set and capacity of resources currently available.
  ///
  /// \return Immutable set of resources with currently available capacity.
  const ResourceSet &GetAvailableResources() const;

  /// \brief Overwrite available resource capacity with the specified resource set.
  ///
  /// \param newset: The set of resources that replaces available resource capacity.
  /// \return None.
  void SetAvailableResources(ResourceSet &&newset);

  const ResourceSet &GetTotalResources() const;

  /// \brief Release the amount of resources specified.
  ///
  /// \param resources: the amount of resources to be released.
  /// \return True if resources were successfully released. False otherwise.
  bool Release(const ResourceSet &resources);

  /// \brief Acquire the amount of resources specified.
  ///
  /// \param resources: the amount of resources to be acquired.
  /// \return True if resources were acquired without oversubscription. If this
  /// returns false, then the resources were still acquired, but we are now at
  /// negative resources.
  bool Acquire(const ResourceSet &resources);

 private:
  /// Static resource configuration (e.g., static_resources).
  ResourceSet resources_total_;
  /// Dynamic resource capacity (e.g., dynamic_resources).
  ResourceSet resources_available_;
  /// gpu_map - replace with ResourceMap (for generality).
};

}  // namespace raylet

}  // namespace ray

#endif  // RAY_RAYLET_SCHEDULING_RESOURCES_H
