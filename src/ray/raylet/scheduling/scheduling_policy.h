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

#pragma once

#include <vector>

#include "ray/common/ray_config.h"
#include "ray/gcs/gcs_client/gcs_client.h"
#include "ray/raylet/scheduling/cluster_resource_data.h"

namespace ray {
namespace raylet_scheduling_policy {

class SchedulingPolicy {
 public:
  SchedulingPolicy(int64_t local_node_id, const absl::flat_hash_map<int64_t, Node> &nodes)
      : local_node_id_(local_node_id), nodes_(nodes) {}

  /// This scheduling policy was designed with the following assumptions in mind:
  ///   1. Scheduling a task on a new node incurs a cold start penalty (warming the worker
  ///   pool).
  ///   2. Past a certain utilization threshold, a big noisy neighbor problem occurs
  ///   (caused by object spilling).
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
  /// We call this a hybrid policy because below the threshold, the traversal and
  /// truncation properties will lead to packing of nodes. Above the threshold, the policy
  /// will act like a traditional weighted round robin.
  ///
  /// \param resource_request: The resource request we're attempting to schedule.
  /// \param scheduler_avoid_gpu_nodes: if set, we would try scheduling
  /// CPU-only requests on CPU-only nodes, and will fallback to scheduling on GPU nodes if
  /// needed.
  ///
  /// \return -1 if the task is unfeasible, otherwise the node id (key in `nodes`) to
  /// schedule on.
  int64_t HybridPolicy(
      const ResourceRequest &resource_request, float spread_threshold,
      bool force_spillback, bool require_available,
      std::function<bool(int64_t)> is_node_available,
      bool scheduler_avoid_gpu_nodes = RayConfig::instance().scheduler_avoid_gpu_nodes());

 private:
  /// Identifier of local node.
  const int64_t local_node_id_;
  /// List of nodes in the clusters and their resources organized as a map.
  /// The key of the map is the node ID.
  const absl::flat_hash_map<int64_t, Node> &nodes_;

  enum class NodeFilter {
    /// Default scheduling.
    kAny,
    /// Schedule on GPU only nodes.
    kGPU,
    /// Schedule on nodes that don't have GPU. Since GPUs are more scarce resources, we
    /// need
    /// special handling for this.
    kNonGpu
  };

  /// \param resource_request: The resource request we're attempting to schedule.
  /// \param node_filter: defines the subset of nodes were are allowed to schedule on.
  /// can be one of kAny (can schedule on all nodes), kGPU (can only schedule on kGPU
  /// nodes), kNonGpu (can only schedule on non-GPU nodes.
  ///
  /// \return -1 if the task is unfeasible, otherwise the node id (key in `nodes`) to
  /// schedule on.
  int64_t HybridPolicyWithFilter(const ResourceRequest &resource_request,
                                 float spread_threshold, bool force_spillback,
                                 bool require_available,
                                 std::function<bool(int64_t)> is_node_available,
                                 NodeFilter node_filter = NodeFilter::kAny);
};
}  // namespace raylet_scheduling_policy
}  // namespace ray
