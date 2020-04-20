#ifndef RAY_COMMON_TASK_SCHEDULING_RESOURCES_H
#define RAY_COMMON_TASK_SCHEDULING_RESOURCES_H

#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "ray/raylet/format/node_manager_generated.h"

namespace ray {

/// Conversion factor that is the amount in internal units is equivalent to
/// one actual resource. Multiply to convert from actual to interal and
/// divide to convert from internal to actual.
constexpr double kResourceConversionFactor = 10000;

const std::string kCPU_ResourceLabel = "CPU";
const std::string kGPU_ResourceLabel = "GPU";
const std::string kTPU_ResourceLabel = "TPU";
const std::string kMemory_ResourceLabel = "memory";

/// \class FractionalResourceQuantity
/// \brief Converts the resource quantities to an internal representation to
/// avoid machine precision errors.
class FractionalResourceQuantity {
 public:
  /// \brief Construct a FractionalResourceQuantity representing zero
  /// resources. This constructor is used by std::unordered_map when we try
  /// to add a new FractionalResourceQuantity in ResourceSets.
  FractionalResourceQuantity();

  /// \brief Construct a FractionalResourceQuantity representing
  /// resource_quantity.
  FractionalResourceQuantity(double resource_quantity);

  /// \brief Addition of FractionalResourceQuantity.
  const FractionalResourceQuantity operator+(const FractionalResourceQuantity &rhs) const;

  /// \brief Subtraction of FractionalResourceQuantity.
  const FractionalResourceQuantity operator-(const FractionalResourceQuantity &rhs) const;

  /// \brief Addition and assignment of FractionalResourceQuantity.
  void operator+=(const FractionalResourceQuantity &rhs);

  /// \brief Subtraction and assignment of FractionalResourceQuantity.
  void operator-=(const FractionalResourceQuantity &rhs);

  bool operator==(const FractionalResourceQuantity &rhs) const;
  bool operator!=(const FractionalResourceQuantity &rhs) const;
  bool operator<(const FractionalResourceQuantity &rhs) const;
  bool operator>(const FractionalResourceQuantity &rhs) const;
  bool operator<=(const FractionalResourceQuantity &rhs) const;
  bool operator>=(const FractionalResourceQuantity &rhs) const;

  /// \brief Return actual resource amount as a double.
  double ToDouble() const;

 private:
  /// The resource quantity represented as 1/kResourceConversionFactor-th of a
  /// unit.
  int64_t resource_quantity_;
};

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
  ResourceSet(
      const std::unordered_map<std::string, FractionalResourceQuantity> &resource_map);

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
  void AddOrUpdateResource(const std::string &resource_name,
                           const FractionalResourceQuantity &capacity);

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
  /// each label. \return True if the resource set was added successfully. False
  /// otherwise.
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
  FractionalResourceQuantity GetResource(const std::string &resource_name) const;

  /// Return the number of CPUs.
  ///
  /// \return Number of CPUs.
  const ResourceSet GetNumCpus() const;

  /// Return true if the resource set is empty. False otherwise.
  ///
  /// \return True if the resource capacity is zero. False otherwise.
  bool IsEmpty() const;

  // TODO(atumanov): implement const_iterator class for the ResourceSet container.
  // TODO(williamma12): Make sure that everywhere we use doubles we don't
  // convert it back to FractionalResourceQuantity.
  /// \brief Return a map of the resource and size in doubles. Note, size is in
  /// regular units and does not need to be multiplied by kResourceConversionFactor.
  ///
  /// \return map of resource in string to size in double.
  const std::unordered_map<std::string, double> GetResourceMap() const;

  /// \brief Return a map of the resource and size in FractionalResourceQuantity. Note,
  /// size is in kResourceConversionFactor of a unit.
  ///
  /// \return map of resource in string to size in FractionalResourceQuantity.
  const std::unordered_map<std::string, FractionalResourceQuantity>
      &GetResourceAmountMap() const;

  const std::string ToString() const;

 private:
  /// Resource capacity map.
  std::unordered_map<std::string, FractionalResourceQuantity> resource_capacity_;
};

/// \class ResourceIds
/// \brief This class generalizes the concept of a resource "quantity" to
/// include specific resource IDs and fractions of those resources. A typical example
/// is GPUs, where the GPUs are numbered 0 through N-1, where N is the total number
/// of GPUs. This information is ultimately passed through to the worker processes
/// which need to know which GPUs to use.
class ResourceIds {
 public:
  /// \brief empty ResourceIds constructor.
  ResourceIds();

  /// \brief Constructs ResourceIds with a given amount of resource.
  ///
  /// \param resource_quantity: The total amount of resource. This must either be
  /// a whole number or a fraction less than 1.
  explicit ResourceIds(double resource_quantity);

  /// \brief Constructs ResourceIds with a given set of whole IDs.
  ///
  /// \param whole_ids: A vector of the resource IDs that are completely available.
  explicit ResourceIds(const std::vector<int64_t> &whole_ids);

  /// \brief Constructs ResourceIds with a given set of fractional IDs.
  ///
  /// \param fractional_ids: A vector of the resource IDs that are partially available.
  explicit ResourceIds(
      const std::vector<std::pair<int64_t, FractionalResourceQuantity>> &fractional_ids);

  /// \brief Constructs ResourceIds with a given set of whole IDs and fractional IDs.
  ///
  /// \param whole_ids: A vector of the resource IDs that are completely available.
  /// \param fractional_ids: A vector of the resource IDs that are partially available.
  ResourceIds(
      const std::vector<int64_t> &whole_ids,
      const std::vector<std::pair<int64_t, FractionalResourceQuantity>> &fractional_ids);

  /// \brief Check if we have at least the requested amount.
  ///
  /// If the argument is a whole number, then we return True precisely when
  /// we have enough whole IDs (ignoring fractional IDs). If the argument is a
  /// fraction, then there must either be a whole ID or a single fractional ID with
  /// a sufficiently large availability. E.g., if there are two IDs that have
  /// availability 0.5, then Contains(0.75) will return false.
  ///
  /// \param resource_quantity Either a whole number or a fraction less than 1.
  /// \return True if there we have enough of the resource.
  bool Contains(const FractionalResourceQuantity &resource_quantity) const;

  /// \brief Acquire the requested amount of the resource.
  ///
  /// \param resource_quantity The amount to acquire. Either a whole number or a
  /// fraction less than 1.
  /// \return A ResourceIds representing the specific acquired IDs.
  ResourceIds Acquire(const FractionalResourceQuantity &resource_quantity);

  /// \brief Return some resource IDs.
  ///
  /// \param resource_ids The specific resource IDs to return.
  /// \return Void.
  void Release(const ResourceIds &resource_ids);

  /// \brief Combine these IDs with some other IDs and return the result.
  ///
  /// \param resource_ids The IDs to add to these ones.
  /// \return The combination of the IDs.
  ResourceIds Plus(const ResourceIds &resource_ids) const;

  /// \brief Return just the whole IDs.
  ///
  /// \return The whole IDs.
  const std::vector<int64_t> &WholeIds() const;

  /// \brief Return just the fractional IDs.
  ///
  /// \return The fractional IDs.
  const std::vector<std::pair<int64_t, FractionalResourceQuantity>> &FractionalIds()
      const;

  /// \brief Check if ResourceIds has any resources.
  ///
  /// \return True if there are no whole or fractional resources. False otherwise.
  bool TotalQuantityIsZero() const;

  /// \brief Return the total quantity of resources, ignoring the specific IDs.
  ///
  /// \return The total quantity of the resource.
  FractionalResourceQuantity TotalQuantity() const;

  /// \brief Return a string representation of the object.
  ///
  /// \return A human-readable string representing the object.
  std::string ToString() const;

  /// \brief Increase resource capacity by the given amount. This may throw an error if
  /// decrement is more than currently available resources.
  ///
  /// \param new_capacity int of new capacity
  /// \return Void.
  void UpdateCapacity(int64_t new_capacity);

 private:
  /// Check that a double is in fact a whole number.
  ///
  /// \param resource_quantity A double.
  /// \return True if the double is an integer and false otherwise.
  bool IsWhole(double resource_quantity) const;

  /// \brief Increase resource capacity by the given amount.
  ///
  /// \param increment_quantity The quantity of resources to add.
  /// \return Void.
  void IncreaseCapacity(int64_t increment_quantity);

  /// \brief Decrease resource capacity by the given amount. Adds to the decrement backlog
  /// if more than available resources are decremented.
  ///
  /// \param decrement_quantity The quantity of resources to remove.
  /// \return Void.
  void DecreaseCapacity(int64_t decrement_quantity);

  /// A vector of distinct whole resource IDs.
  std::vector<int64_t> whole_ids_;
  /// A vector of pairs of resource ID and a fraction of that ID (the fraction
  /// is at least zero and strictly less than 1).
  std::vector<std::pair<int64_t, FractionalResourceQuantity>> fractional_ids_;
  /// Quantity to track the total capacity of the resource, since the whole_ids_ vector
  /// keeps changing
  FractionalResourceQuantity total_capacity_;
  /// Quantity to track any pending decrements in capacity that weren't executed because
  /// of insufficient available resources. This backlog in cleared in the release method.
  int64_t decrement_backlog_;
};

/// \class ResourceIdSet
/// \brief This class keeps track of the specific IDs that are available for a
/// collection of resources.
class ResourceIdSet {
 public:
  /// \brief empty ResourceIdSet constructor.
  ResourceIdSet();

  /// \brief Construct a ResourceIdSet from a ResourceSet.
  ///
  /// \param resource_set A mapping from resource name to quantity.
  ResourceIdSet(const ResourceSet &resource_set);

  /// \brief Construct a ResourceIdSet from a mapping from resource names to ResourceIds.
  ///
  /// \param resource_set A mapping from resource name to IDs.
  ResourceIdSet(const std::unordered_map<std::string, ResourceIds> &available_resources);

  /// \brief See if a requested collection of resources is contained.
  ///
  /// \param resource_set A mapping from resource name to quantity.
  /// \return True if each resource in resource_set is contained in the corresponding
  /// ResourceIds in this ResourceIdSet.
  bool Contains(const ResourceSet &resource_set) const;

  /// \brief Acquire a set of resources and return the specific acquired IDs.
  ///
  /// \param resource_set A mapping from resource name to quantity. This specifies
  /// the amount of each resource to acquire.
  /// \return A ResourceIdSet with the requested quantities, but with specific IDs.
  ResourceIdSet Acquire(const ResourceSet &resource_set);

  /// \brief Return a set of resource IDs.
  ///
  /// \param resource_id_set The resource IDs to return.
  /// \return Void.
  void Release(const ResourceIdSet &resource_id_set);

  /// \brief Return a set of resource IDs subject to their existence in the
  /// resources_total set.
  ///
  /// \param resource_id_set The resource IDs to return.
  /// \param resources_total Constraint set to restrict the release to. If a resource
  /// exists in resource_id_set but not in resources_total, it is not added to this
  /// ResourceIdSet. \return Void.
  void ReleaseConstrained(const ResourceIdSet &resource_id_set,
                          const ResourceSet &resources_total);

  /// \brief Clear out all of the resource IDs.
  ///
  /// \return Void.
  void Clear();

  /// \brief Combine another ResourceIdSet with this one.
  ///
  /// \param resource_id_set The other set of resource IDs to combine with this one.
  /// \return The combination of the two sets of resource IDs.
  ResourceIdSet Plus(const ResourceIdSet &resource_id_set) const;

  /// \brief Creates or updates a resource in the ResourceIdSet if it already exists.
  /// Raises an exception if the new capacity (when less than old capacity) cannot be set
  /// because of busy resources.
  ///
  /// \param resource_name the name of the resource to create/update
  /// \param capacity capacity of the resource being added
  void AddOrUpdateResource(const std::string &resource_name, int64_t capacity);

  /// \brief Deletes a resource in the ResourceIdSet. This does not raise an exception,
  /// just deletes the resource. Tasks with acquired resources keep running.
  ///
  /// \param resource_name the name of the resource to delete
  void DeleteResource(const std::string &resource_name);

  /// \brief Get the underlying mapping from resource name to resource IDs.
  ///
  /// \return The resource name to resource IDs mapping.
  const std::unordered_map<std::string, ResourceIds> &AvailableResources() const;

  /// Return the CPU resources.
  ///
  /// \return The CPU resources.
  ResourceIdSet GetCpuResources() const;

  /// \brief Get a mapping from each resource to the total quantity.
  ///
  /// \return A mapping from each resource to the total quantity.
  ResourceSet ToResourceSet() const;

  /// \brief Get a string representation of the object.
  ///
  /// \return A human-readable string version of the object.
  std::string ToString() const;

  /// \brief Serialize this object using flatbuffers.
  ///
  /// \param fbb A flatbuffer builder object.
  /// \return A flatbuffer serialized version of this object.
  std::vector<flatbuffers::Offset<ray::protocol::ResourceIdSetInfo>> ToFlatbuf(
      flatbuffers::FlatBufferBuilder &fbb) const;

  /// \brief Serialize this object as a string.
  ///
  /// \return A serialized string of this object.
  /// TODO(zhijunfu): this can be removed after raylet client is migrated to grpc.
  const std::string Serialize() const;

 private:
  /// A mapping from resource name to a set of resource IDs for that resource.
  std::unordered_map<std::string, ResourceIds> available_resources_;
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

  const ResourceSet &GetTotalResources() const;

  /// \brief Overwrite information about resource load with new resource load set.
  ///
  /// \param newset: The set of resources that replaces resource load information.
  /// \return Void.
  void SetLoadResources(ResourceSet &&newset);

  /// \brief Request the resource load information.
  ///
  /// \return Immutable set of resources describing the load information.
  const ResourceSet &GetLoadResources() const;

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

 private:
  /// Static resource configuration (e.g., static_resources).
  ResourceSet resources_total_;
  /// Dynamic resource capacity (e.g., dynamic_resources).
  ResourceSet resources_available_;
  /// Resource load.
  ResourceSet resources_load_;
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

#endif  // RAY_COMMON_TASK_SCHEDULING_RESOURCES_H
