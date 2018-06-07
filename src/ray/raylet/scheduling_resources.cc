#include "scheduling_resources.h"

#include <cmath>

#include "ray/util/logging.h"

namespace ray {

namespace raylet {

ResourceSet::ResourceSet() {}

ResourceSet::ResourceSet(const std::unordered_map<std::string, double> &resource_map)
    : resource_capacity_(resource_map) {}

ResourceSet::ResourceSet(const std::vector<std::string> &resource_labels,
                         const std::vector<double> resource_capacity) {
  RAY_CHECK(resource_labels.size() == resource_capacity.size());
  for (uint i = 0; i < resource_labels.size(); i++) {
    RAY_CHECK(this->AddResource(resource_labels[i], resource_capacity[i]));
  }
}

ResourceSet::~ResourceSet() {}

bool ResourceSet::operator==(const ResourceSet &rhs) const {
  return (this->IsSubset(rhs) && rhs.IsSubset(*this));
}

bool ResourceSet::IsEmpty() const {
  // Check whether the capacity of each resource type is zero. Exit early if not.
  if (resource_capacity_.empty()) return true;
  for (const auto &resource_pair : resource_capacity_) {
    if (resource_pair.second > 0) {
      return false;
    }
  }
  return true;
}

bool ResourceSet::IsSubset(const ResourceSet &other) const {
  // Check to make sure all keys of this are in other.
  for (const auto &resource_pair : resource_capacity_) {
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
bool ResourceSet::IsSuperset(const ResourceSet &other) const {
  return other.IsSubset(*this);
}
/// Test whether this ResourceSet is precisely equal to the other ResourceSet.
bool ResourceSet::IsEqual(const ResourceSet &rhs) const {
  return (this->IsSubset(rhs) && rhs.IsSubset(*this));
}

bool ResourceSet::AddResource(const std::string &resource_name, double capacity) {
  this->resource_capacity_[resource_name] = capacity;
  return true;
}
bool ResourceSet::RemoveResource(const std::string &resource_name) {
  throw std::runtime_error("Method not implemented");
}
bool ResourceSet::SubtractResources(const ResourceSet &other) {
  // Subtract the resources and track whether a resource goes below zero.
  bool oversubscribed = false;
  for (const auto &resource_pair : other.GetResourceMap()) {
    const std::string &resource_label = resource_pair.first;
    const double &resource_capacity = resource_pair.second;
    RAY_CHECK(resource_capacity_.count(resource_label) == 1)
        << "Attempt to acquire unknown resource: " << resource_label;
    resource_capacity_[resource_label] -= resource_capacity;
    if (resource_capacity_[resource_label] < 0) {
      oversubscribed = true;
    }
  }
  return !oversubscribed;
}

bool ResourceSet::AddResources(const ResourceSet &other) {
  // Return failure if attempting to perform vector addition with unknown labels.
  // TODO(atumanov): make the implementation atomic. Currently, if false is returned
  // the resource capacity may be partially mutated. To reverse, call SubtractResources.
  for (const auto &resource_pair : other.GetResourceMap()) {
    const std::string &resource_label = resource_pair.first;
    const double &resource_capacity = resource_pair.second;
    if (resource_capacity_.count(resource_label) == 0) {
      return false;
    } else {
      resource_capacity_[resource_label] += resource_capacity;
    }
  }
  return true;
}

bool ResourceSet::GetResource(const std::string &resource_name, double *value) const {
  if (!value) {
    return false;
  }
  if (this->resource_capacity_.count(resource_name) == 0) {
    *value = std::nan("");
    return false;
  }
  *value = this->resource_capacity_.at(resource_name);
  return true;
}

double ResourceSet::GetNumCpus() const {
  double num_cpus;
  RAY_CHECK(GetResource(kCPU_ResourceLabel, &num_cpus));
  return num_cpus;
}

const std::string ResourceSet::ToString() const {
  std::string return_string = "";
  for (const auto &resource_pair : this->resource_capacity_) {
    return_string +=
        "{" + resource_pair.first + "," + std::to_string(resource_pair.second) + "}, ";
  }
  return return_string;
}

const std::unordered_map<std::string, double> &ResourceSet::GetResourceMap() const {
  return this->resource_capacity_;
};

/// SchedulingResources class implementation

SchedulingResources::SchedulingResources()
    : resources_total_(ResourceSet()), resources_available_(ResourceSet()) {}

SchedulingResources::SchedulingResources(const ResourceSet &total)
    : resources_total_(total), resources_available_(total) {}

SchedulingResources::~SchedulingResources() {}

ResourceAvailabilityStatus SchedulingResources::CheckResourcesSatisfied(
    ResourceSet &resources) const {
  if (!resources.IsSubset(this->resources_total_)) {
    return ResourceAvailabilityStatus::kInfeasible;
  }
  // Resource demand specified is feasible. Check if it's available.
  if (!resources.IsSubset(this->resources_available_)) {
    return ResourceAvailabilityStatus::kResourcesUnavailable;
  }
  return ResourceAvailabilityStatus::kFeasible;
}

const ResourceSet &SchedulingResources::GetAvailableResources() const {
  return this->resources_available_;
}

void SchedulingResources::SetAvailableResources(ResourceSet &&newset) {
  this->resources_available_ = newset;
}

const ResourceSet &SchedulingResources::GetTotalResources() const {
  return this->resources_total_;
}

// Return specified resources back to SchedulingResources.
bool SchedulingResources::Release(const ResourceSet &resources) {
  return this->resources_available_.AddResources(resources);
}

// Take specified resources from SchedulingResources.
bool SchedulingResources::Acquire(const ResourceSet &resources) {
  return this->resources_available_.SubtractResources(resources);
}

}  // namespace raylet

}  // namespace ray
