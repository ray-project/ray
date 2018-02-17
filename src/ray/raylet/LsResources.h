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
  kWorkerUnavailable,   // Resource requirements satisfied, but no worker available.
  kFeasible       // Resource and worker requirements satisfied.
} ResourceAvailabilityStatus;

/// ResourceSet class encapsulates a set of resources (e.g., CPUs, GPUs, custom
/// labels.
class ResourceSet {
public:
  ResourceSet();
  ~ResourceSet();
  /// Test the equality of two resource sets.
  bool operator=(const ResourceSet&a, const ResourceSet &b) const;
  /// Test whether this ResourceSet is a subset of the other ResourceSet
  bool isSubset(const ResourceSet &other) const;
  /// Test whether this ResourceSet is a superset of the other ResourceSet
  bool isSuperset(const ResourceSet &other) const;
  /// Test whether this ResourceSet is precisely equal to the other ResourceSet.
  bool IsEqual(const ResourceSet &other) const;
  void AddResource(const std::string &resource_name, double capacity);
  void RemoveResource(const std::string &resource_name);
  void SubtractResources(const ResourceSet &other);
  void AddResources(const ResourceSet &other);
  double GetResource(const std::string &resource_name);

private:
  std::unordered_map<std::string, double> resource_capacity_;
};


/// LsResources class encapsulates state of all local resources and manages
/// accounting of those resources. Resources include configured resource
/// bundle capacity, a worker pool, and GPU allocation map.
class LsResources {
 /** Responsible for resource accounting, including CPUs/GPUs, workers, actors. */
 public:
  ResourceAvailabilityStatus CheckResourcesSatisfied(ResourceSet &resources);
  ResourceSet CurrentResources();
  void Release(ResourceSet &resources);
  void Acquire(ResourceSet &resources);
  void AddWorker(Worker *worker);
  void RemoveWorker(Worker *worker);
 private:
   // static resource configuration (e.g., static_resources)
  ResourceSet resources_total;
   // dynamic resource capacity (e.g., dynamic_resources)
  ResourceSet resources_available;
   // set of workers, in a WorkerPool()
   // gpu_map
};


} // end namespace ray

#endif
