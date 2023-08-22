// Copyright 2023 The Ray Authors.
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

#include "ray/common/scheduling/resource_instance_set.h"

#include <cmath>
#include <sstream>

#include "ray/util/logging.h"

namespace ray {

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

bool NodeResourceInstanceSet::Has(ResourceID resource_id) const {
  return !(Get(resource_id).empty());
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
    // This is the default value so there is no need to store it.
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
