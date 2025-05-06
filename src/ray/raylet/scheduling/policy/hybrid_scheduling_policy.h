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

#include <gtest/gtest_prod.h>

#include <optional>
#include <vector>

#include "absl/random/bit_gen_ref.h"
#include "absl/random/random.h"
#include "ray/raylet/scheduling/policy/scheduling_policy.h"

namespace ray {
namespace raylet_scheduling_policy {

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
///   3. Randomly choose one node from top-k nodes according to the priority.
///      If preferred node is specified, and the preferred node has the highest
///      priority (lowest score), we always skip top k but directly pick
///      the preferred node.
///
/// A node's priorities are determined by the following factors:
///   * Always skip infeasible nodes
///   * Always prefer available nodes over feasible nodes.
///   * Break ties in available/feasible by critical resource utilization.
///   * Critical resource utilization below a threshold should be truncated to 0.
///
class HybridSchedulingPolicy : public ISchedulingPolicy {
 public:
  HybridSchedulingPolicy(scheduling::NodeID local_node_id,
                         const absl::flat_hash_map<scheduling::NodeID, Node> &nodes,
                         std::function<bool(scheduling::NodeID)> is_node_alive)
      : local_node_id_(local_node_id),
        nodes_(nodes),
        is_node_alive_(is_node_alive),
        bitgen_(),
        bitgenref_(bitgen_) {}

  scheduling::NodeID Schedule(const ResourceRequest &resource_request,
                              SchedulingOptions options) override;

 private:
  enum class NodeFilter {
    /// Default scheduling.
    kAny,  /// Schedule on GPU only nodes.
    kGPU,
    /// Schedule on nodes that don't have GPU. Since GPUs are more scarce resources, we
    /// need
    /// special handling for this.
    kNonGpu
  };

  /// Return true if the node is alive and its total resource
  /// satisfy the filter and resource requirement.
  bool IsNodeFeasible(const scheduling::NodeID &node_id,
                      const NodeFilter &node_filter,
                      const NodeResources &node_resources,
                      const ResourceRequest &resource_request) const;

  /// helper function compute a score between 0-1 indicates
  /// the preference of the node (the lower score,
  /// the more preferable.
  float ComputeNodeScore(const scheduling::NodeID &node_id, float spread_threshold) const;

  scheduling::NodeID GetBestNode(
      std::vector<std::pair<scheduling::NodeID, float>> &node_scores,
      size_t num_candidate_nodes,
      std::optional<scheduling::NodeID> preferred_node_id,
      float preferred_node_score) const;

  /// \param resource_request: The resource request we're attempting to schedule.
  /// \param spread_threshold: The fraction of resource utilization on a node after
  /// which the scheduler starts to prefer spreading tasks to other nodes.
  //// This balances between locality and
  /// even balancing of load. Low values (min 0.0) encourage more load spreading.
  /// \param force_spillback: don't schedule on local node if true.
  /// \param require_available: If true, only schedule on nodes who have resources
  /// available to fulfill the request. Otherwise, schedule on nodes whose resources
  /// capacity can fulfill the request, even if the resources are not currently
  /// available.
  /// \param node_filter: defines the subset of nodes were are allowed to schedule on.
  /// can be one of kAny (can schedule on all nodes), kGPU (can only schedule on kGPU
  /// nodes), kNonGpu (can only schedule on non-GPU nodes.
  /// \param preferred_node_id: defines the preferred node to schedule on.
  /// \param schedule_top_k_absolute: scheduler will randomly pick
  /// one node from the top k in the cluster to improve load balancing. The
  /// scheduler guarantees k is at least equal to schedule_top_k_absolute.
  /// \param schedule_top_k_fraction: The scheduler will randomly pick
  /// one node from the top k in the cluster to improve load balancing. The
  /// scheduler guarantees k is at least equal to this fraction * the number of
  /// nodes in the cluster.
  ///
  /// \return -1 if the task is unfeasible, otherwise the node id (key in `nodes`) to
  /// schedule on.
  scheduling::NodeID ScheduleImpl(const ResourceRequest &resource_request,
                                  float spread_threshold,
                                  bool force_spillback,
                                  bool require_available,
                                  NodeFilter node_filter,
                                  const std::string &preferred_node,
                                  int32_t schedule_top_k_absolute,
                                  float scheduler_top_k_fraction);

  /// Identifier of local node.
  const scheduling::NodeID local_node_id_;
  /// List of nodes in the clusters and their resources organized as a map.
  /// The key of the map is the node ID.
  const absl::flat_hash_map<scheduling::NodeID, Node> &nodes_;
  /// Function Checks if node is alive.
  std::function<bool(scheduling::NodeID)> is_node_alive_;
  /// Random number generator to choose a random node out of the top K.
  mutable absl::BitGen bitgen_;
  /// Using BitGenRef to simplify testing.
  mutable absl::BitGenRef bitgenref_;

  FRIEND_TEST(HybridSchedulingPolicyTest, GetBestNode);
  FRIEND_TEST(HybridSchedulingPolicyTest, GetBestNodePrioritizePreferredNode);
};
}  // namespace raylet_scheduling_policy
}  // namespace ray
