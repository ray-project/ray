#ifndef LS_RESOURCES_H
#define LS_RESOURCES_H

#include <string>
#include <unordered_map>
#include <unordered_set>

using namespace std;
namespace ray {

typedef enum {
  kInfeasible,      // Cannot ever satisfy resource requirements.
  kResourcesUnavailable,  // Resource requirements eventually satisfied.
  // TODO(swang): Do we need this value?
  kWorkerUnavailable,   // Resource requirements satisfied, but no worker available.
  kFeasible       // Resource and worker requirements satisfied.
} ResourceAvailabilityStatus;

/// ResourceSet class encapsulates a set of resources (e.g., CPUs, GPUs, custom
/// labels.
class ResourceSet {
public:
  /// Resource set object constructor
  ResourceSet(const std::unordered_map<string, double> &resource_map):
    resource_capacity_(resource_map) {}

  /// Test the equality of two resource sets.
  bool operator==(const ResourceSet &rhs) const;
  /// Test whether this ResourceSet is a subset of the other ResourceSet
  bool isSubset(const ResourceSet &other) const;
  /// Test whether this ResourceSet is a superset of the other ResourceSet
  bool isSuperset(const ResourceSet &other) const;
  /// Test whether this ResourceSet is precisely equal to the other ResourceSet.
  bool IsEqual(const ResourceSet &other) const;
  bool AddResource(const std::string &resource_name, double capacity);
  bool RemoveResource(const std::string &resource_name);
  /// Add a set of resources.
  bool AddResources(const ResourceSet &other);
  /// Subtract a set of resources.
  bool SubtractResources(const ResourceSet &other);

  /// Return the capacity value of a specified resource.
  bool GetResource(const std::string &resource_name, double *value) const;

private:
  std::unordered_map<std::string, double> resource_capacity_;
};


/// LsResources class encapsulates state of all local resources and manages
/// accounting of those resources. Resources include configured resource
/// bundle capacity, and GPU allocation map.
class LsResources {
 public:
  // Raylet resource object constructors: set the total configured resource
  // capacity
  LsResources(const ResourceSet& total):
    resources_total_(total), resources_available_(total) {}

  LsResources(const ResourceSet &total, const WorkerPool &pool):
    resources_total_(total), resources_available_(total), pool_(pool) {}

  ResourceAvailabilityStatus CheckResourcesSatisfied(ResourceSet &resources) const;

  const ResourceSet &GetAvailableResources() const;

  /// Methods that mutate state.
  bool Release(const ResourceSet &resources);
  bool Acquire(const ResourceSet &resources);
 private:
   // static resource configuration (e.g., static_resources)
  ResourceSet resources_total_;
   // dynamic resource capacity (e.g., dynamic_resources)
  ResourceSet resources_available_;
   // gpu_map - replace with ResourceMap (for generality)
};

} // end namespace ray

#endif
