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
#include <utility>

#include "ray/common/bundle_spec.h"
#include "ray/util/container_util.h"
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

  // Remove from the pg_indexed_resources_ as well
  auto data = ParsePgFormattedResource(resource_id.Binary(),
                                       /*for_wildcard_resource=*/false,
                                       /*for_indexed_resource=*/true);
  if (data) {
    ResourceID original_resource_id(data->original_resource);

    auto pg_resource_map_it = pg_indexed_resources_.find(original_resource_id);
    if (pg_resource_map_it != pg_indexed_resources_.end()) {
      auto resource_set_it = pg_resource_map_it->second.find(data->group_id);

      if (resource_set_it != pg_resource_map_it->second.end()) {
        resource_set_it->second.erase(resource_id);
        if (resource_set_it->second.empty()) {
          pg_resource_map_it->second.erase(data->group_id);
        }
        if (pg_resource_map_it->second.empty()) {
          pg_indexed_resources_.erase(original_resource_id);
        }
      }
    }
  }
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
  RAY_CHECK(!instances.empty());
  if (resource_id.IsImplicitResource() && instances[0] == FixedPoint(1)) {
    // This is the default value so there is no need to store it.
    resources_.erase(resource_id);
  } else {
    resources_[resource_id] = std::move(instances);

    // Popluate the pg_indexed_resources_map_
    // TODO (myan): The parsing of the resource_id String can be costly and impact the
    // task creation throughput if the parting is required every time we allocate
    // resources for a task and updating the available resources. The current benchmark
    // shows no observable impact for now. But in the future, ideas of improvement are:
    // (1) to add the placement group id as well as the bundle index inside the
    // ResourceID class. And instead of parse the String, leveraging the fields in the
    // ResourceID class directly; (2) to update the pg resource id format to start with
    // a special prefix so that we can do "startwith" instead of regex match which is
    // less costly
    auto data = ParsePgFormattedResource(resource_id.Binary(),
                                         /*for_wildcard_resource=*/false,
                                         /*for_indexed_resource=*/true);
    if (data) {
      pg_indexed_resources_[ResourceID(data->original_resource)][data->group_id].emplace(
          resource_id);
    }
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

  // During resource allocation with a placement group, no matter whether the allocation
  // requirement specifies a bundle index, we generate the allocation on both
  // the wildcard resource and the indexed resource. The resource_demand shouldn't be
  // assigned across bundles. And If no bundle index is specified, we will iterate
  // through the bundles and find the first bundle that can fit the required resources.
  // In addition, for unit resources, we make sure that the allocation on the
  // wildcard resource and the indexed resource are consistent, meaning the same
  // instance ids should be allocated.
  //
  // For example, considering the GPU resource on a host. Assuming the host has 3 GPUs
  // and 1 placement group with 2 bundles. The bundle with index 1 contains 1 GPU and
  // the bundle with index 2 contains 2 GPU.
  //
  // The current node resource can be as follows:
  // resource id: total, available
  // GPU: [1, 1, 1], [0, 0, 0]
  // GPU_<pg_id>: [1, 1, 1], [1, 1, 1]
  // GPU_1_<pg_id>: [1, 0, 0], [1, 0, 0]
  // GPU_2_<pg_id>: [0, 1, 1], [0, 1, 1]
  //
  // Now, we want to allocate a task with 2 GPUs and in the placement group <pg_id>,
  // reflecting in the following resource demand:
  // GPU_<pg_id> : 2
  //
  // We will iterate though all the bundles in the placement group and bundle with
  // index=2 has the required capacity. So we will allocate the task to the 2 GPUs in
  // bundle 2 in placement group <pg_id> and the same allocation should be reflected in
  // the wildcard GPU resource. So the allocation will be:
  // GPU_<pg_id> : [0, 1, 1]
  // GPU_2_<pg_id> : [0, 1, 1]
  //
  // And as a result, after the allocation, current node resource will be:
  // resource id: total, available
  // GPU: [1, 1, 1], [0, 0, 0]
  // GPU_<pg_id>: [1, 1, 1], [1, 0, 0]
  // GPU_1_<pg_id>: [1, 0, 0], [1, 0, 0]
  // GPU_2_<pg_id>: [0, 1, 1], [0, 0, 0]

  absl::flat_hash_map</* original resource id (GPU, CPU, etc.) */ ResourceID,
                      std::vector<std::pair</* actual resource id */ ResourceID,
                                            PgFormattedResourceData>>>
      pg_resource_map;

  for (const auto &[resource_id, demand] : resource_demands.Resources()) {
    auto data = ParsePgFormattedResource(resource_id.Binary(),
                                         /*for_wildcard_resource*/ true,
                                         /*for_indexed_resource*/ true);

    if (data) {
      // Aggregate based on resource type
      ResourceID original_resource_id{data->original_resource};
      pg_resource_map[original_resource_id].push_back(
          std::make_pair(resource_id, data.value()));
    } else {
      // Directly allocate the resources if the resource is not with a placement group
      auto allocation = TryAllocate(resource_id, demand);
      if (allocation) {
        // Even if allocation failed we need to remember partial allocations to
        // correctly free resources.
        allocations[resource_id] = std::move(*allocation);
      } else {
        // Allocation failed. Restore partially allocated resources.
        for (const auto &[resource_id, allocation] : allocations) {
          Free(resource_id, allocation);
        }
        return std::nullopt;
      }
    }
  }

  // Handle the resource allocation for resources with placement group
  for (const auto &[original_resource_id, resource_id_vector] : pg_resource_map) {
    // Assuming exactly 1 placement group and at most 1 bundle index can be specified in
    // the resource requirement for a single resource type
    // Also assuming the wildcard resource id will always exist in the resource_demands
    // no matter how the resource requirement is specified in task
    std::optional<std::vector<FixedPoint>> wildcard_allocation;
    const ResourceID *wildcard_resource_id = nullptr;
    const std::string *pg_id = nullptr;

    // Allocate indexed resource
    if (resource_id_vector.size() == 1) {
      // The case where no bundle index is specified
      // Iterate through the bundles with the same original resource and pg_id to find
      // the first one with enough space
      bool found = false;
      wildcard_resource_id = &resource_id_vector[0].first;
      pg_id = &resource_id_vector[0].second.group_id;

      auto pg_index_resources_it = pg_indexed_resources_.find(original_resource_id);
      if (pg_index_resources_it != pg_indexed_resources_.end()) {
        auto index_resources_it = pg_index_resources_it->second.find(*pg_id);
        if (index_resources_it != pg_index_resources_it->second.end()) {
          for (ResourceID indexed_resource_id : index_resources_it->second) {
            if (Has(indexed_resource_id)) {
              wildcard_allocation = TryAllocate(
                  indexed_resource_id, resource_demands.Get(*wildcard_resource_id));
              if (wildcard_allocation) {
                // Found the allocation in a bundle
                allocations[indexed_resource_id] = *wildcard_allocation;
                found = true;
                break;
              }
            }
          }
        }
      }

      if (!found) {
        // No bundle can fit the required resources, allocation failed
        for (const auto &[resource_id, allocation] : allocations) {
          Free(resource_id, allocation);
        }
        return std::nullopt;
      }
    } else {
      // The case where the bundle index is specified
      // For each resource type, both the wildcard resource and the indexed resource
      // should be in the resource_demand
      for (const std::pair<ResourceID, PgFormattedResourceData> &pair :
           resource_id_vector) {
        if (pair.second.bundle_index != -1) {
          // This is the indexed resource
          wildcard_allocation = TryAllocate(pair.first, resource_demands.Get(pair.first));

          if (wildcard_allocation) {
            allocations[pair.first] = *wildcard_allocation;
          } else {
            // The corresponding bundle cannot hold the required resources.
            // Allocation failed
            for (const auto &[resource_id, allocation] : allocations) {
              Free(resource_id, allocation);
            }
            return std::nullopt;
          }
        } else {
          // This is the wildcard resource
          wildcard_resource_id = &pair.first;
        }
      }
    }

    // Allocate wildcard resource, should be consistent with the indexed resource
    RAY_CHECK(wildcard_resource_id != nullptr);
    RAY_CHECK(!(*wildcard_allocation).empty());
    AllocateWithReference(*wildcard_allocation, *wildcard_resource_id);
    allocations[*wildcard_resource_id] = std::move(*wildcard_allocation);
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
  // Then try to find the best fit for the fractional remaining_resources. Best fit means
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

void NodeResourceInstanceSet::AllocateWithReference(
    const std::vector<FixedPoint> &ref_allocation, ResourceID resource_id) {
  std::vector<FixedPoint> available = Get(resource_id);
  RAY_CHECK(!available.empty());
  RAY_CHECK_EQ(available.size(), ref_allocation.size());

  for (size_t i = 0; i < ref_allocation.size(); i++) {
    if (available[i] < ref_allocation[i]) {
      // Only CPU resource can go negative due to the behavior
      // that ray.get() will temporarily release the CPU resource.
      // See https://github.com/ray-project/ray/pull/50517.
      RAY_CHECK(IsCPUOrPlacementGroupCPUResource(resource_id))
          << "Resource " << resource_id.Binary()
          << " has less availability than requested. Available: "
          << debug_string(available) << ", requested: " << debug_string(ref_allocation);
    }
    available[i] -= ref_allocation[i];
  }

  Set(resource_id, std::move(available));
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
