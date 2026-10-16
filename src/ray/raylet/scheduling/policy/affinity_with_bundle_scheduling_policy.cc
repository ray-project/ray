// Copyright 2021 The Ray Authors.
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

#include "ray/raylet/scheduling/policy/affinity_with_bundle_scheduling_policy.h"

namespace ray {
namespace raylet_scheduling_policy {

NodeSchedulingResult AffinityWithBundleSchedulingPolicy::TryNode(
    const scheduling::NodeID &node_id,
    const ResourceRequest &resource_request,
    bool avoid_gpu_nodes) {
  const auto it = nodes_.find(node_id);
  if (it == nodes_.end() || !is_node_alive_(node_id) ||
      !it->second.GetLocalView().IsFeasible(resource_request)) {
    return NodeSchedulingResult::Infeasible();
  }
  const auto &node_resources = it->second.GetLocalView();
  if (avoid_gpu_nodes) {
    // Avoiding gpu nodes is only needed for requests with no bundle id specified, so we
    // only avoid the nodes with the PG's gpu wildcard resource.
    // Now combine the right prefix and suffix for the gpu wildcard resource name.
    std::string gpu_wildcard_resource_name =
        "GPU_group_" +
        GetGroupIDFromResource(resource_request.ResourceIds().begin()->Binary());
    if (node_resources.total.Has(scheduling::ResourceID(gpu_wildcard_resource_name))) {
      return NodeSchedulingResult::Infeasible();
    }
  }
  if (!node_resources.IsAvailable(resource_request)) {
    return NodeSchedulingResult::NoNodeAvailable();
  }
  return NodeSchedulingResult::Scheduled(node_id);
}

NodeSchedulingResult AffinityWithBundleSchedulingPolicy::Schedule(
    const ResourceRequest &resource_request, SchedulingOptions options) {
  RAY_CHECK(options.scheduling_type_ == SchedulingType::AFFINITY_WITH_BUNDLE);

  auto bundle_scheduling_context =
      dynamic_cast<const AffinityWithBundleSchedulingContext *>(
          options.scheduling_context_.get());
  const BundleID &bundle_id = bundle_scheduling_context->GetAffinityBundleID();
  bool saw_feasible_but_unavailable = false;
  if (bundle_id.second != -1) {
    const auto &node_id_opt = bundle_location_index_.GetBundleLocation(bundle_id);
    if (node_id_opt) {
      const auto result = TryNode(scheduling::NodeID(node_id_opt.value().Binary()),
                                  resource_request,
                                  /*avoid_gpu_nodes=*/false);
      if (result.IsScheduled()) {
        return result;
      }
      if (result.IsNoNodeAvailable()) {
        saw_feasible_but_unavailable = true;
      }
    }
  } else {
    const PlacementGroupID &pg_id = bundle_id.first;
    const auto &bundle_locations_opt = bundle_location_index_.GetBundleLocations(pg_id);
    if (bundle_locations_opt) {
      // Find a target with gpu nodes avoided (if required).
      if (options.avoid_gpu_nodes_) {
        for (const auto &iter : *(bundle_locations_opt.value())) {
          const auto result = TryNode(scheduling::NodeID(iter.second.first.Binary()),
                                      resource_request,
                                      /*avoid_gpu_nodes=*/true);
          if (result.IsScheduled()) {
            return result;
          }
          if (result.IsNoNodeAvailable()) {
            saw_feasible_but_unavailable = true;
          }
        }
      }
      // Find a target from all nodes.
      for (const auto &iter : *(bundle_locations_opt.value())) {
        const auto result = TryNode(scheduling::NodeID(iter.second.first.Binary()),
                                    resource_request,
                                    /*avoid_gpu_nodes=*/false);
        if (result.IsScheduled()) {
          return result;
        }
        if (result.IsNoNodeAvailable()) {
          saw_feasible_but_unavailable = true;
        }
      }
    }
  }
  if (saw_feasible_but_unavailable) {
    return NodeSchedulingResult::NoNodeAvailable();
  }
  return NodeSchedulingResult::Infeasible();
}

}  // namespace raylet_scheduling_policy
}  // namespace ray
