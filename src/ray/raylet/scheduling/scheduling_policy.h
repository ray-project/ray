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

#include <vector>

#include "ray/raylet/scheduling/cluster_resource_data.h"

namespace ray {
namespace raylet_scheduling_policy {

/// This scheduling policy was designed with the following assumptions in mind:
///   1. Scheduling a task on a new node incurs a cold start penalty (warming the worker
///   pool).
///   2. Past a certain utilization threshold, a big noisy neighbor problem occurs (caused
///   by object spilling).
///   3. Locality is helpful, but generally outweighed by (1) and (2).
///
/// In order to solve these problems, we use the following scheduling policy.
///   1. Generate a traversal.
///   2. Run a priority scheduler.
///
/// A node's priorities are determined by the following factors:
///   * Always skip infeasible nodes
///   * Always prefer available nodes over feasible nodes.
///   * Break ties in available/feasible by critical resource utilization.
///   * Critical resource utilization below a threshold should be truncated to 0.
///
/// The traversal order should:
///   * Prioritize the local node above all others.
///   * All other nodes should have a globally fixed priority across the cluster.
///
/// We call this a hybrid policy because below the threshold, the traversal and truncation
/// properties will lead to packing of nodes. Above the threshold, the policy will act
/// like a traditional weighted round robin.
///
/// \param resource_request: The resource request we're attempting to schedule.
/// \param local_node_id: The id of the local node, which is needed for traversal order.
/// \param nodes: The summary view of all the nodes that can be scheduled on.
/// \param spread_threshold: Below this threshold, critical resource utilization will be
/// truncated to 0.
///
/// \return -1 if the task is infeasible, otherwise the node id (key in `nodes`) to
/// schedule on.
int64_t HybridPolicy(const ResourceRequest &resource_request, const int64_t local_node_id,
                     const absl::flat_hash_map<int64_t, Node> &nodes,
                     float spread_threshold, bool force_spillback,
                     bool require_available);
}  // namespace raylet_scheduling_policy
}  // namespace ray
