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

#include "ray/common/task/scheduling_resources.h"

#include <cmath>
#include <sstream>

#include "absl/container/flat_hash_map.h"
#include "ray/util/logging.h"

namespace ray {
ResourceSet::ResourceSet() {}

ResourceSet::ResourceSet(const absl::flat_hash_map<std::string, FixedPoint> &resource_map)
    : resource_capacity_(resource_map) {
  for (auto const &resource_pair : resource_map) {
    RAY_CHECK(resource_pair.second > 0);
  }
}

ResourceSet::ResourceSet(const absl::flat_hash_map<std::string, double> &resource_map) {
  for (auto const &resource_pair : resource_map) {
    RAY_CHECK(resource_pair.second > 0);
    resource_capacity_[resource_pair.first] = FixedPoint(resource_pair.second);
  }
}

ResourceSet::ResourceSet(const std::vector<std::string> &resource_labels,
                         const std::vector<double> resource_capacity) {
  RAY_CHECK(resource_labels.size() == resource_capacity.size());
  for (size_t i = 0; i < resource_labels.size(); i++) {
    RAY_CHECK(resource_capacity[i] > 0);
    resource_capacity_[resource_labels[i]] = FixedPoint(resource_capacity[i]);
  }
}

ResourceSet::~ResourceSet() {}

bool ResourceSet::operator==(const ResourceSet &rhs) const {
  return (this->IsSubset(rhs) && rhs.IsSubset(*this));
}

bool ResourceSet::IsEmpty() const {
  // Check whether the capacity of each resource type is zero. Exit early if not.
  return resource_capacity_.empty();
}

bool ResourceSet::IsSubset(const ResourceSet &other) const {
  // Check to make sure all keys of this are in other.
  for (const auto &resource_pair : resource_capacity_) {
    const auto &resource_name = resource_pair.first;
    const FixedPoint &lhs_quantity = resource_pair.second;
    const FixedPoint &rhs_quantity = other.GetResource(resource_name);
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

void ResourceSet::AddOrUpdateResource(const std::string &resource_name,
                                      const FixedPoint &capacity) {
  if (capacity > 0) {
    resource_capacity_[resource_name] = capacity;
  }
}

bool ResourceSet::DeleteResource(const std::string &resource_name) {
  if (resource_capacity_.count(resource_name) == 1) {
    resource_capacity_.erase(resource_name);
    return true;
  } else {
    return false;
  }
}

void ResourceSet::SubtractResources(const ResourceSet &other) {
  // Subtract the resources, make sure none goes below zero and delete any if new capacity
  // is zero.
  for (const auto &resource_pair : other.GetResourceAmountMap()) {
    const std::string &resource_label = resource_pair.first;
    const FixedPoint &resource_capacity = resource_pair.second;
    if (resource_capacity_.count(resource_label) == 1) {
      resource_capacity_[resource_label] -= resource_capacity;
    }
    if (resource_capacity_[resource_label] <= 0) {
      resource_capacity_.erase(resource_label);
    }
  }
}

void ResourceSet::SubtractResourcesStrict(const ResourceSet &other) {
  // Subtract the resources, make sure none goes below zero and delete any if new capacity
  // is zero.
  for (const auto &resource_pair : other.GetResourceAmountMap()) {
    const std::string &resource_label = resource_pair.first;
    const FixedPoint &resource_capacity = resource_pair.second;
    RAY_CHECK(resource_capacity_.count(resource_label) == 1)
        << "Attempt to acquire unknown resource: " << resource_label << " capacity "
        << resource_capacity.Double();
    resource_capacity_[resource_label] -= resource_capacity;

    // Ensure that quantity is positive. Note, we have to have the check before
    // erasing the object to make sure that it doesn't get added back.
    RAY_CHECK(resource_capacity_[resource_label] >= 0)
        << "Capacity of resource after subtraction is negative, "
        << resource_capacity_[resource_label].Double() << ".";

    if (resource_capacity_[resource_label] == 0) {
      resource_capacity_.erase(resource_label);
    }
  }
}

// Add a set of resources to the current set of resources subject to upper limits on
// capacity from the total_resource set
void ResourceSet::AddResourcesCapacityConstrained(const ResourceSet &other,
                                                  const ResourceSet &total_resources) {
  const absl::flat_hash_map<std::string, FixedPoint> &total_resource_map =
      total_resources.GetResourceAmountMap();
  for (const auto &resource_pair : other.GetResourceAmountMap()) {
    const std::string &to_add_resource_label = resource_pair.first;
    const FixedPoint &to_add_resource_capacity = resource_pair.second;
    if (total_resource_map.count(to_add_resource_label) != 0) {
      // If resource exists in total map, add to the local capacity map.
      // If the new capacity is less than the total capacity, set the new capacity to
      // the local capacity (capping to the total).
      const FixedPoint &total_capacity = total_resource_map.at(to_add_resource_label);
      resource_capacity_[to_add_resource_label] =
          std::min(resource_capacity_[to_add_resource_label] + to_add_resource_capacity,
                   total_capacity);
    } else {
      // Resource does not exist in the total map, it probably got deleted from the total.
      // Don't panic, do nothing and simply continue.
      RAY_LOG(DEBUG) << "[AddResourcesCapacityConstrained] Resource "
                     << to_add_resource_label
                     << " not found in the total resource map. It probably got deleted, "
                        "not adding back to resource_capacity_.";
    }
  }
}

// Perform an outer join.
void ResourceSet::AddResources(const ResourceSet &other) {
  for (const auto &resource_pair : other.GetResourceAmountMap()) {
    const std::string &resource_label = resource_pair.first;
    const FixedPoint &resource_capacity = resource_pair.second;
    resource_capacity_[resource_label] += resource_capacity;
  }
}

FixedPoint ResourceSet::GetResource(const std::string &resource_name) const {
  if (resource_capacity_.count(resource_name) == 0) {
    return 0;
  }
  const FixedPoint &capacity = resource_capacity_.at(resource_name);
  return capacity;
}

const ResourceSet ResourceSet::GetNumCpus() const {
  ResourceSet cpu_resource_set;
  const FixedPoint cpu_quantity = GetResource(kCPU_ResourceLabel);
  if (cpu_quantity > 0) {
    cpu_resource_set.resource_capacity_[kCPU_ResourceLabel] = cpu_quantity;
  }
  return cpu_resource_set;
}

double ResourceSet::GetNumCpusAsDouble() const {
  const FixedPoint cpu_quantity = GetResource(kCPU_ResourceLabel);
  return cpu_quantity.Double();
}

std::string format_resource(std::string resource_name, double quantity) {
  if (resource_name == "object_store_memory" ||
      resource_name.find(kMemory_ResourceLabel) == 0) {
    return std::to_string(quantity / (1024 * 1024 * 1024)) + " GiB";
  }
  return std::to_string(quantity);
}

const std::string ResourceSet::ToString() const {
  if (resource_capacity_.size() == 0) {
    return "{}";
  } else {
    std::string return_string = "";

    auto it = resource_capacity_.begin();

    // Convert the first element to a string.
    if (it != resource_capacity_.end()) {
      double resource_amount = (it->second).Double();
      return_string +=
          "{" + it->first + ": " + format_resource(it->first, resource_amount) + "}";
      it++;
    }

    // Add the remaining elements to the string (along with a comma).
    for (; it != resource_capacity_.end(); ++it) {
      double resource_amount = (it->second).Double();
      return_string +=
          ", {" + it->first + ": " + format_resource(it->first, resource_amount) + "}";
    }

    return return_string;
  }
}

std::unordered_map<std::string, double> ResourceSet::GetResourceUnorderedMap() const {
  std::unordered_map<std::string, double> result;
  for (const auto &[name, quantity] : resource_capacity_) {
    result[name] = quantity.Double();
  }
  return result;
};

absl::flat_hash_map<std::string, double> ResourceSet::GetResourceMap() const {
  absl::flat_hash_map<std::string, double> result;
  for (const auto &[name, quantity] : resource_capacity_) {
    result[name] = quantity.Double();
  }
  return result;
};

const absl::flat_hash_map<std::string, FixedPoint> &ResourceSet::GetResourceAmountMap()
    const {
  return resource_capacity_;
};

/// SchedulingResources class implementation

SchedulingResources::SchedulingResources()
    : resources_total_(ResourceSet()),
      resources_available_(ResourceSet()),
      resources_load_(ResourceSet()) {}

SchedulingResources::SchedulingResources(const ResourceSet &total)
    : resources_total_(total),
      resources_available_(total),
      resources_load_(ResourceSet()) {}

SchedulingResources::~SchedulingResources() {}

const ResourceSet &SchedulingResources::GetAvailableResources() const {
  return resources_available_;
}

void SchedulingResources::SetAvailableResources(ResourceSet &&newset) {
  resources_available_ = newset;
}

const ResourceSet &SchedulingResources::GetTotalResources() const {
  return resources_total_;
}

void SchedulingResources::SetTotalResources(ResourceSet &&newset) {
  resources_total_ = newset;
}

const ResourceSet &SchedulingResources::GetLoadResources() const {
  return resources_load_;
}

void SchedulingResources::SetLoadResources(ResourceSet &&newset) {
  resources_load_ = newset;
}

// Return specified resources back to SchedulingResources.
void SchedulingResources::Release(const ResourceSet &resources) {
  return resources_available_.AddResourcesCapacityConstrained(resources,
                                                              resources_total_);
}

// Take specified resources from SchedulingResources.
void SchedulingResources::Acquire(const ResourceSet &resources) {
  resources_available_.SubtractResourcesStrict(resources);
}

// The reason we need this method is sometimes we may want add some converted
// resource which is not exist in total resource to the available resource.
// (e.g., placement group)
void SchedulingResources::AddResource(const ResourceSet &resources) {
  resources_total_.AddResources(resources);
  resources_available_.AddResources(resources);
}

void SchedulingResources::UpdateResourceCapacity(const std::string &resource_name,
                                                 int64_t capacity) {
  const FixedPoint new_capacity = FixedPoint(capacity);
  const FixedPoint &current_capacity = resources_total_.GetResource(resource_name);
  if (current_capacity > 0) {
    // If the resource exists, add to total and available resources
    const FixedPoint capacity_difference = new_capacity - current_capacity;
    const FixedPoint &current_available_capacity =
        resources_available_.GetResource(resource_name);
    FixedPoint new_available_capacity = current_available_capacity + capacity_difference;
    if (new_available_capacity < 0) {
      new_available_capacity = 0;
    }
    resources_total_.AddOrUpdateResource(resource_name, new_capacity);
    resources_available_.AddOrUpdateResource(resource_name, new_available_capacity);
  } else {
    // Resource does not exist, just add it to total and available. Do not add to load.
    resources_total_.AddOrUpdateResource(resource_name, new_capacity);
    resources_available_.AddOrUpdateResource(resource_name, new_capacity);
  }
}

void SchedulingResources::DeleteResource(const std::string &resource_name) {
  resources_total_.DeleteResource(resource_name);
  resources_available_.DeleteResource(resource_name);
  resources_load_.DeleteResource(resource_name);
}

const ResourceSet &SchedulingResources::GetNormalTaskResources() const {
  return resources_normal_tasks_;
}

void SchedulingResources::SetNormalTaskResources(const ResourceSet &newset) {
  resources_normal_tasks_ = newset;
}

std::string SchedulingResources::DebugString() const {
  std::stringstream result;

  auto resources_available = resources_available_;
  resources_available.SubtractResources(resources_normal_tasks_);

  result << "\n- total: " << resources_total_.ToString();
  result << "\n- avail: " << resources_available.ToString();
  result << "\n- normal task usage: " << resources_normal_tasks_.ToString();
  return result.str();
};

}  // namespace ray
