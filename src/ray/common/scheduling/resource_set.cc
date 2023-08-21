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

#include "ray/common/scheduling/resource_set.h"

#include <cmath>
#include <sstream>

#include "absl/container/flat_hash_map.h"
#include "ray/util/logging.h"

namespace ray {

ResourceSet::ResourceSet(
    const absl::flat_hash_map<std::string, FixedPoint> &resource_map) {
  for (auto const &[name, quantity] : resource_map) {
    Set(ResourceID(name), quantity);
  }
}

ResourceSet::ResourceSet(
    const absl::flat_hash_map<ResourceID, FixedPoint> &resource_map) {
  for (auto const &[id, quantity] : resource_map) {
    Set(id, quantity);
  }
}

ResourceSet::ResourceSet(const absl::flat_hash_map<ResourceID, double> &resource_map) {
  for (auto const &[id, quantity] : resource_map) {
    Set(id, FixedPoint(quantity));
  }
}

ResourceSet::ResourceSet(const absl::flat_hash_map<std::string, double> &resource_map) {
  for (auto const &[name, quantity] : resource_map) {
    Set(ResourceID(name), FixedPoint(quantity));
  }
}

bool ResourceSet::operator==(const ResourceSet &other) const {
  return this->resources_ == other.resources_;
}

ResourceSet ResourceSet::operator+(const ResourceSet &other) const {
  ResourceSet res = *this;
  res += other;
  return res;
}

ResourceSet ResourceSet::operator-(const ResourceSet &other) const {
  ResourceSet res = *this;
  res -= other;
  return res;
}

ResourceSet &ResourceSet::operator+=(const ResourceSet &other) {
  for (auto &entry : other.resources_) {
    auto it = resources_.find(entry.first);
    if (it != resources_.end()) {
      it->second += entry.second;
      if (it->second == 0) {
        resources_.erase(it);
      }
    } else {
      resources_.emplace(entry.first, entry.second);
    }
  }
  return *this;
}

ResourceSet &ResourceSet::operator-=(const ResourceSet &other) {
  for (auto &entry : other.resources_) {
    auto it = resources_.find(entry.first);
    if (it != resources_.end()) {
      it->second -= entry.second;
      if (it->second == 0) {
        resources_.erase(it);
      }
    } else {
      resources_.emplace(entry.first, -entry.second);
    }
  }
  return *this;
}

bool ResourceSet::operator<=(const ResourceSet &other) const {
  // Check all resources that exist in this.
  for (auto &entry : resources_) {
    auto &this_value = entry.second;
    auto other_value = FixedPoint(0);
    auto it = other.resources_.find(entry.first);
    if (it != other.resources_.end()) {
      other_value = it->second;
    }
    if (this_value > other_value) {
      return false;
    }
  }
  // Check all resources that exist in other, but not in this.
  for (auto &entry : other.resources_) {
    if (!resources_.contains(entry.first)) {
      if (entry.second < 0) {
        return false;
      }
    }
  }
  return true;
}

bool ResourceSet::IsEmpty() const { return resources_.empty(); }

FixedPoint ResourceSet::Get(ResourceID resource_id) const {
  auto it = resources_.find(resource_id);
  if (it == resources_.end()) {
    return FixedPoint(0);
  } else {
    return it->second;
  }
}

ResourceSet &ResourceSet::Set(ResourceID resource_id, FixedPoint value) {
  if (value == 0) {
    resources_.erase(resource_id);
  } else {
    resources_[resource_id] = value;
  }
  return *this;
}

const std::string ResourceSet::DebugString() const {
  std::stringstream buffer;
  buffer << "{";
  bool first = true;
  for (const auto &[id, quantity] : resources_) {
    if (!first) {
      buffer << ", ";
    }
    first = false;
    buffer << id.Binary() << ": " << quantity;
  }
  buffer << "}";
  return buffer.str();
}

std::unordered_map<std::string, double> ResourceSet::GetResourceUnorderedMap() const {
  std::unordered_map<std::string, double> result;
  for (const auto &[id, quantity] : resources_) {
    result[id.Binary()] = quantity.Double();
  }
  return result;
};

absl::flat_hash_map<std::string, double> ResourceSet::GetResourceMap() const {
  absl::flat_hash_map<std::string, double> result;
  for (const auto &[id, quantity] : resources_) {
    result[id.Binary()] = quantity.Double();
  }
  return result;
};

NodeResourceSet::NodeResourceSet(
    const absl::flat_hash_map<std::string, double> &resource_map) {
  for (auto const &[name, quantity] : resource_map) {
    Set(ResourceID(name), FixedPoint(quantity));
  }
}

NodeResourceSet::NodeResourceSet(
    const absl::flat_hash_map<ResourceID, double> &resource_map) {
  for (auto const &[id, quantity] : resource_map) {
    Set(id, FixedPoint(quantity));
  }
}

NodeResourceSet &NodeResourceSet::Set(ResourceID resource_id, FixedPoint value) {
  if (value == ResourceDefaultValue(resource_id)) {
    resources_.erase(resource_id);
  } else {
    resources_[resource_id] = value;
  }
  return *this;
}

FixedPoint NodeResourceSet::Get(ResourceID resource_id) const {
  auto it = resources_.find(resource_id);
  if (it == resources_.end()) {
    return ResourceDefaultValue(resource_id);
  } else {
    return it->second;
  }
}

bool NodeResourceSet::Has(ResourceID resource_id) const { return Get(resource_id) != 0; }

NodeResourceSet &NodeResourceSet::operator-=(const ResourceSet &other) {
  for (auto &entry : other.Resources()) {
    Set(entry.first, Get(entry.first) - entry.second);
  }
  return *this;
}

bool NodeResourceSet::operator>=(const ResourceSet &other) const {
  for (auto &entry : other.Resources()) {
    if (Get(entry.first) < entry.second) {
      return false;
    }
  }
  return true;
}

bool NodeResourceSet::operator==(const NodeResourceSet &other) const {
  return this->resources_ == other.resources_;
}

FixedPoint NodeResourceSet::ResourceDefaultValue(ResourceID resource_id) const {
  if (resource_id.IsImplicitResource()) {
    return FixedPoint(1);
  } else {
    return FixedPoint(0);
  }
}

absl::flat_hash_map<std::string, double> NodeResourceSet::GetResourceMap() const {
  absl::flat_hash_map<std::string, double> result;
  for (const auto &[id, quantity] : resources_) {
    result[id.Binary()] = quantity.Double();
  }
  return result;
};

void NodeResourceSet::RemoveNegative() {
  for (auto it = resources_.begin(); it != resources_.end();) {
    if (it->second < 0) {
      resources_.erase(it++);
    } else {
      it++;
    }
  }
}

std::set<ResourceID> NodeResourceSet::ExplicitResourceIds() const {
  std::set<ResourceID> result;
  for (const auto &[id, quantity] : resources_) {
    if (!id.IsImplicitResource()) {
      result.emplace(id);
    }
  }
  return result;
}

std::string NodeResourceSet::DebugString() const {
  std::stringstream buffer;
  buffer << "{";
  bool first = true;
  for (const auto &[id, quantity] : resources_) {
    if (!first) {
      buffer << ", ";
    }
    first = false;
    buffer << id.Binary() << ": " << quantity;
  }
  buffer << "}";
  return buffer.str();
}

NodeResourceInstanceSet::NodeResourceInstanceSet(const NodeResourceSet &total) {
  for (auto &resource_id : total.ExplicitResourceIds()) {
    std::vector<FixedPoint> instances;
    auto value = total.Get(resource_id);
    if (resource_id.IsUnitInstanceResource()) {
      size_t num_instances = static_cast<size_t>(value.Double());
      for (size_t i = 0; i < num_instances; i++) {
        instances.push_back(1.0);
      };
    } else {
      instances.push_back(value);
    }
    Set(resource_id, instances);
  }
}

void NodeResourceInstanceSet::Remove(ResourceID resource_id) {
  resources_.erase(resource_id);
}

const std::vector<FixedPoint> &NodeResourceInstanceSet::Get(
    ResourceID resource_id) const {
  static std::vector<FixedPoint> empty = std::vector<FixedPoint>();
  static std::vector<FixedPoint> implicit_resource_instances =
      std::vector<FixedPoint>({FixedPoint(1)});

  auto it = resources_.find(resource_id);
  if (it != resources_.end()) {
    return it->second;
  } else if (resource_id.IsImplicitResource()) {
    return implicit_resource_instances;
  } else {
    return empty;
  }
}

NodeResourceInstanceSet &NodeResourceInstanceSet::Set(ResourceID resource_id,
                                                      std::vector<FixedPoint> instances) {
  if (instances.size() == 0) {
    resources_.erase(resource_id);
  } else if (resource_id.IsImplicitResource() && instances[0] == FixedPoint(1)) {
    resources_.erase(resource_id);
  } else {
    resources_[resource_id] = std::move(instances);
  }
  return *this;
}

FixedPoint NodeResourceInstanceSet::Sum(ResourceID resource_id) const {
  auto it = resources_.find(resource_id);
  if (it == resources_.end()) {
    if (resource_id.IsImplicitResource()) {
      return FixedPoint(1);
    } else {
      return FixedPoint(0);
    }
  } else {
    return FixedPoint::Sum(it->second);
  }
}

bool NodeResourceInstanceSet::operator==(const NodeResourceInstanceSet &other) const {
  return this->resources_ == other.resources_;
}

std::optional<absl::flat_hash_map<ResourceID, std::vector<FixedPoint>>>
NodeResourceInstanceSet::TryAllocate(const ResourceSet &resource_demands) {
  absl::flat_hash_map<ResourceID, std::vector<FixedPoint>> allocations;
  for (const auto &[resource_id, demand] : resource_demands.Resources()) {
    auto allocation = TryAllocate(resource_id, demand);
    if (allocation) {
      // Even if allocation failed we need to remember partial allocations to correctly
      // free resources.
      allocations[resource_id] = std::move(*allocation);
    } else {
      // Allocation failed. Restore partially allocated resources.
      for (const auto &[resource_id, allocation] : allocations) {
        Free(resource_id, allocation);
      }
      return std::nullopt;
    }
  }

  return std::make_optional<absl::flat_hash_map<ResourceID, std::vector<FixedPoint>>>(
      std::move(allocations));
}

std::optional<std::vector<FixedPoint>> NodeResourceInstanceSet::TryAllocate(
    ResourceID resource_id, FixedPoint demand) {
  std::vector<FixedPoint> available = Get(resource_id);
  if (available.empty()) {
    return std::nullopt;
  }

  std::vector<FixedPoint> allocation(available.size());
  FixedPoint remaining_demand = demand;

  if (available.size() == 1) {
    // This resource has just one instance.
    if (available[0] >= remaining_demand) {
      available[0] -= remaining_demand;
      allocation[0] = remaining_demand;
      Set(resource_id, std::move(available));
      return std::make_optional<std::vector<FixedPoint>>(std::move(allocation));
    } else {
      // Not enough capacity.
      return std::nullopt;
    }
  }

  // If resources has multiple instances, each instance has total capacity of 1.
  //
  // As long as remaining_demand is greater than 1.,
  // allocate full unit-capacity instances until the remaining_demand becomes fractional.
  // Then try to find the best fit for the fractional remaining_resources. Best fist means
  // allocating the resource instance with the smallest available capacity greater than
  // remaining_demand
  if (remaining_demand >= 1.) {
    for (size_t i = 0; i < available.size(); i++) {
      if (available[i] == 1.) {
        // Allocate a full unit-capacity instance.
        allocation[i] = 1.;
        available[i] = 0;
        remaining_demand -= 1.;
      }
      if (remaining_demand < 1.) {
        break;
      }
    }
  }

  if (remaining_demand >= 1.) {
    // Cannot satisfy a demand greater than one if no unit capacity resource is available.
    return std::nullopt;
  }

  // Remaining demand is fractional. Find the best fit, if exists.
  if (remaining_demand > 0.) {
    int64_t idx_best_fit = -1;
    FixedPoint available_best_fit = 1.;
    for (size_t i = 0; i < available.size(); i++) {
      if (available[i] >= remaining_demand) {
        if (idx_best_fit == -1 ||
            (available[i] - remaining_demand < available_best_fit)) {
          available_best_fit = available[i] - remaining_demand;
          idx_best_fit = static_cast<int64_t>(i);
        }
      }
    }
    if (idx_best_fit == -1) {
      return std::nullopt;
    } else {
      allocation[idx_best_fit] = remaining_demand;
      available[idx_best_fit] -= remaining_demand;
    }
  }

  Set(resource_id, std::move(available));
  return std::make_optional<std::vector<FixedPoint>>(std::move(allocation));
}

void NodeResourceInstanceSet::Free(ResourceID resource_id,
                                   const std::vector<FixedPoint> &allocation) {
  std::vector<FixedPoint> available = Get(resource_id);
  RAY_CHECK_EQ(allocation.size(), available.size());
  for (size_t i = 0; i < available.size(); i++) {
    available[i] += allocation[i];
  }

  Set(resource_id, std::move(available));
}

void NodeResourceInstanceSet::Add(ResourceID resource_id,
                                  const std::vector<FixedPoint> &instances) {
  RAY_CHECK(!resource_id.IsImplicitResource());

  if (!resources_.contains(resource_id)) {
    Set(resource_id, instances);
  } else {
    auto &resource_instances = resources_[resource_id];
    if (resource_instances.size() <= instances.size()) {
      resource_instances.resize(instances.size());
    }
    for (size_t i = 0; i < instances.size(); ++i) {
      resource_instances[i] += instances[i];
    }
  }
}

std::vector<FixedPoint> NodeResourceInstanceSet::Subtract(
    ResourceID resource_id,
    const std::vector<FixedPoint> &instances,
    bool allow_going_negative) {
  std::vector<FixedPoint> available = Get(resource_id);
  RAY_CHECK_EQ(available.size(), instances.size());

  std::vector<FixedPoint> underflow(available.size(), 0.);
  for (size_t i = 0; i < available.size(); i++) {
    if (available[i] < 0) {
      if (allow_going_negative) {
        available[i] = available[i] - instances[i];
      } else {
        underflow[i] = instances[i];  // No change in the value in this case.
      }
    } else {
      available[i] = available[i] - instances[i];
      if (available[i] < 0 && !allow_going_negative) {
        underflow[i] = -available[i];
        available[i] = 0;
      }
    }
  }

  Set(resource_id, std::move(available));
  return underflow;
}

std::string NodeResourceInstanceSet::DebugString() const {
  std::stringstream buffer;
  buffer << "{";
  bool first = true;
  for (const auto &[id, quantity] : resources_) {
    if (!first) {
      buffer << ", ";
    }
    first = false;
    buffer << id.Binary() << ": " << FixedPointVectorToString(quantity);
  }
  buffer << "}";
  return buffer.str();
}

NodeResourceSet NodeResourceInstanceSet::ToNodeResourceSet() const {
  NodeResourceSet node_resource_set;
  for (const auto &[resource_id, instances] : resources_) {
    node_resource_set.Set(resource_id, FixedPoint::Sum(instances));
  }
  return node_resource_set;
}

}  // namespace ray
