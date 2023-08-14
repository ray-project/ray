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

#include "ray/common/scheduling/scheduling_resources.h"

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

}  // namespace ray
