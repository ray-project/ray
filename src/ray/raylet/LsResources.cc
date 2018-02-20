#ifndef LS_RESOURCES_CC
#define LS_RESOURCES_CC

#include "LsResources.h"

namespace ray {

// ResourceSet class implementation
bool ResourceSet::operator=(const ResourceSet &other) const {
  throw std::runtime_error("Method not implemented");
}
/// Test whether this ResourceSet is a subset of the other ResourceSet
bool ResourceSet::isSubset(const ResourceSet &other) const {
  throw std::runtime_error("Method not implemented");
}
/// Test whether this ResourceSet is a superset of the other ResourceSet
bool ResourceSet::isSuperset(const ResourceSet &other) const {
  throw std::runtime_error("Method not implemented");
}
/// Test whether this ResourceSet is precisely equal to the other ResourceSet.
bool ResourceSet::IsEqual(const ResourceSet &other) const {
  throw std::runtime_error("Method not implemented");
}
bool ResourceSet::AddResource(const std::string &resource_name, double capacity) {
  throw std::runtime_error("Method not implemented");
}
bool ResourceSet::RemoveResource(const std::string &resource_name) {
  throw std::runtime_error("Method not implemented");
}
bool ResourceSet::SubtractResources(const ResourceSet &other) {
  throw std::runtime_error("Method not implemented");
}
bool ResourceSet::AddResources(const ResourceSet &other) {
  throw std::runtime_error("Method not implemented");
}
double ResourceSet::GetResource(const std::string &resource_name) {
  throw std::runtime_error("Method not implemented");
}

// LsResources class implementation
ResourceAvailabilityStatus LsResources::CheckResourcesSatisfied(ResourceSet &resources) const {
  if (!resources.isSubset(this->resources_total_)) {
    return kInfeasible;
  }
  // Resource demand specified is feasible. Check if it's available.
  if (!resources.isSubset(this->resources_available_)) {
    return kResourcesUnavailable;
  }
  // Resource demand is feasible, and can be met with available resources.
  // Check if we have enough workers.
  if (this->pool_.PoolSize() == 0) {
    return kWorkerUnavailable;
  }
  return kFeasible;
}

const ResourceSet &LsResources::GetAvailableResources() const{
  return this->resources_available_;
}
bool LsResources::Release(ResourceSet &resources) {
  throw std::runtime_error("Method not implemented");
}
bool LsResources::Acquire(ResourceSet &resources) {
  throw std::runtime_error("Method not implemented");
}
bool LsResources::AddWorker(Worker *worker) {
  throw std::runtime_error("Method not implemented");
}
bool LsResources::RemoveWorker(Worker *worker) {
  throw std::runtime_error("Method not implemented");
}

} // end namespace ray

#endif
