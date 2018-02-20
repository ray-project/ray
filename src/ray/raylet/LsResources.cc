#ifndef LS_RESOURCES_CC
#define LS_RESOURCES_CC

#include "LsResources.h"

namespace ray {

// ResourceSet class implementation
bool ResourceSet::operator==(const ResourceSet &rhs) const {
  return (this->isSubset(rhs) && rhs.isSubset(*this));
}

/// Test whether this ResourceSet is a subset of the other ResourceSet
bool ResourceSet::isSubset(const ResourceSet &other) const {
  // Check to make sure all keys of this are in other.
  for (const auto &resource_pair: resource_capacity_) {
    const auto &resource_name = resource_pair.first;
    const double lhs_quantity = resource_pair.second;
    double rhs_quantity = 0;
    if (!other.GetResource(resource_name, &rhs_quantity)) {
      // Resource not found in rhs, therefore lhs is not a subset of rhs.
      return false;
    }
    if (lhs_quantity > rhs_quantity) {
      // Resource found in rhs, but lhs capacity exceeds rhs capacity.
      return false;
    }
  }
  return true;
}

/// Test whether this ResourceSet is a superset of the other ResourceSet
bool ResourceSet::isSuperset(const ResourceSet &other) const {
  return other.isSubset(*this);
}
/// Test whether this ResourceSet is precisely equal to the other ResourceSet.
bool ResourceSet::IsEqual(const ResourceSet &rhs) const {
  return (this->isSubset(rhs) && rhs.isSubset(*this));
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

bool ResourceSet::GetResource(
    const std::string &resource_name, double *value) const {
  if (!value) {
    return false;
  }
  if (this->resource_capacity_.count(resource_name) == 0) {
    *value = nan("");
    return false;
  }
  *value = this->resource_capacity_.at(resource_name);
  return true;
}

/// LsResources class implementation

ResourceAvailabilityStatus LsResources::CheckResourcesSatisfied(
    ResourceSet &resources) const {
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

// Return specified resources back to LsResources.
bool LsResources::Release(const ResourceSet &resources) {
  return this->resources_available_.AddResources(resources);
}

// Take specified resources from LsResources.
bool LsResources::Acquire(const ResourceSet &resources) {
  return this->resources_available_.SubtractResources(resources);
}

bool LsResources::AddWorker(Worker *worker) {
  throw std::runtime_error("Method not implemented");
}
bool LsResources::RemoveWorker(Worker *worker) {
  throw std::runtime_error("Method not implemented");
}

WorkerPool& LsResources::GetWorkerPool() {
  return pool_;
}

} // end namespace ray

#endif
