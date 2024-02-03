// Copyright 2024 The Ray Authors.
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

#pragma once

#include <vector>

#include "ray/common/scheduling/cluster_resource_data.h"
#include "ray/common/scheduling/scheduling_ids.h"

namespace ray {
namespace raylet_scheduling_policy {

// The new scheduling interface.
// Deprecates ISchedulingPolicy.
// Each SchedulingStrategy is translated to a series of filters, ended with one finalizer.
//
// Scheduling process: begin with all nodes known by this raylet.
// all_nodes -> filter1 -> fileter2 -> ... -> filterN -> finalizer -> the node.
//
// Selector: ([NodeID], resource_request) -> [NodeID] # subset of the input
// Finalizer: ([NodeID], resource_request) -> NodeID # one of the input
//
// This emphasizes composability, e.g. all policies needs to make sure the node is
// alive and feasible.
//
// We support each rpc::SchedulingStrategy by this:
// 1. DefaultSchedulingStrategy (HybridSchedulingPolicy)
//    - Make this Hybrid a Finalizer.
// 2. PlacementGroupSchedulingStrategy (AffinityWithBundleSchedulingPolicy)
//    - Needs ray::BundleID = {PlacementGroupID, bundle_index}
//    - Then: Select first non-GPU nodes, or GPU nodes if no non-GPU nodes.
// 3. SpreadSchedulingStrategy
//    - stateful round robin. How to compose?
//    - Q: why not just random?
// 4. NodeAffinitySchedulingStrategy
//     - soft, spill_on_unavailable, fail_on_unavailable
//     - then hybrid
//     - TODO: make a filter (OrNot), then to Hybrid.
// 5. NodeLabelSchedulingStrategy (NodeLabelSchedulingPolicy)
//     - hard . feasible . OrNot(available . OrNot(soft)) . OrNot(soft) $ Random

// The filter is lightweight to create on-demand for each scheduling request.
// It's const, i.e. Filter should not change any internal states.
class ISchedulingFilter {
 public:
  virtual ~ISchedulingFilter() = default;

  // Filters `candidate_nodes` based on `resource_request`.
  // Constraint: return value must be a subset of `candidate_nodes`.
  virtual std::vector<scheduling::NodeID> Filter(
      const absl::flat_hash_map<scheduling::NodeID, Node> &all_nodes_view,
      const std::vector<scheduling::NodeID> &candidate_nodes,
      const ResourceRequest &resource_request) const = 0;
};

// It's NON const.
class ISchedulingFinalizer {
 public:
  virtual ~ISchedulingFinalizer() = default;

  // Selects one node from `candidate_nodes` based on `resource_request`.
  // Constraint: return value must be one of `candidate_nodes`.
  virtual scheduling::NodeID Finalize(
      const absl::flat_hash_map<scheduling::NodeID, Node> &all_nodes_view,
      const std::vector<scheduling::NodeID> &candidate_nodes,
      const ResourceRequest &resource_request) = 0;
};

}  // namespace raylet_scheduling_policy
}  // namespace ray
