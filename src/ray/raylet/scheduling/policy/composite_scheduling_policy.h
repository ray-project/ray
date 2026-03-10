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

#include "ray/raylet/scheduling/cluster_resource_manager.h"
#include "ray/raylet/scheduling/policy/affinity_with_bundle_scheduling_policy.h"
#include "ray/raylet/scheduling/policy/bundle_scheduling_policy.h"
#include "ray/raylet/scheduling/policy/hybrid_scheduling_policy.h"
#include "ray/raylet/scheduling/policy/label_domain_bundle_scheduling_policy.h"
#include "ray/raylet/scheduling/policy/node_affinity_scheduling_policy.h"
#include "ray/raylet/scheduling/policy/node_label_scheduling_policy.h"
#include "ray/raylet/scheduling/policy/random_scheduling_policy.h"
#include "ray/raylet/scheduling/policy/spread_scheduling_policy.h"

namespace ray {
namespace raylet_scheduling_policy {

/// A composite scheduling policy that routes the request to the underlining
/// scheduling_policy according to the scheduling_type_.
class CompositeSchedulingPolicy : public ISchedulingPolicy {
 public:
  CompositeSchedulingPolicy(scheduling::NodeID local_node_id,
                            ClusterResourceManager &cluster_resource_manager,
                            std::function<bool(scheduling::NodeID)> is_node_available)
      : hybrid_policy_(
            local_node_id, cluster_resource_manager.GetResourceView(), is_node_available),
        random_policy_(
            local_node_id, cluster_resource_manager.GetResourceView(), is_node_available),
        spread_policy_(
            local_node_id, cluster_resource_manager.GetResourceView(), is_node_available),
        node_affinity_policy_(
            local_node_id, cluster_resource_manager.GetResourceView(), is_node_available),
        affinity_with_bundle_policy_(local_node_id,
                                     cluster_resource_manager.GetResourceView(),
                                     is_node_available,
                                     cluster_resource_manager.GetBundleLocationIndex()),
        node_label_scheduling_policy_(local_node_id,
                                      cluster_resource_manager.GetResourceView(),
                                      is_node_available) {}

  scheduling::NodeID Schedule(const ResourceRequest &resource_request,
                              SchedulingOptions options) override;

 private:
  HybridSchedulingPolicy hybrid_policy_;
  RandomSchedulingPolicy random_policy_;
  SpreadSchedulingPolicy spread_policy_;
  NodeAffinitySchedulingPolicy node_affinity_policy_;
  AffinityWithBundleSchedulingPolicy affinity_with_bundle_policy_;
  NodeLabelSchedulingPolicy node_label_scheduling_policy_;
};

/// A composite scheduling policy that routes the request to the underlining
/// bundle_scheduling_policy according to the scheduling_type_.
class CompositeBundleSchedulingPolicy : public IBundleSchedulingPolicy {
 public:
  explicit CompositeBundleSchedulingPolicy(
      ClusterResourceManager &cluster_resource_manager)
      : bundle_pack_policy_(cluster_resource_manager),
        bundle_spread_policy_(cluster_resource_manager),
        bundle_strict_spread_policy_(cluster_resource_manager),
        bundle_strict_pack_policy_(cluster_resource_manager),
        label_domain_strict_pack_policy_(cluster_resource_manager) {}

  SchedulingResult Schedule(
      const std::vector<const ResourceRequest *> &resource_request_list,
      SchedulingOptions options,
      absl::flat_hash_map<scheduling::NodeID, const Node *> candidate_nodes) override;

 private:
  /**
   * @brief Routes to the appropriate label-domain-level policy (tier 1) and
   * returns candidate domains.
   *
   * @param resource_request_list The resource requirements for each bundle.
   * @param options Scheduling options including the label key and optional
   *   target domain value in target_label_domain_.
   * @param candidate_nodes All available candidate nodes.
   * @return A LabelDomainFilterResult with feasible domains or an error status.
   */
  LabelDomainFilterResult ScheduleLabelDomainLevel(
      const std::vector<const ResourceRequest *> &resource_request_list,
      const SchedulingOptions &options,
      absl::flat_hash_map<scheduling::NodeID, const Node *> candidate_nodes);

  /**
   * @brief Routes to the appropriate node-level bundle policy (tier 2) based
   * on scheduling_type_ and places bundles onto individual nodes.
   *
   * @param resource_request_list The resource requirements for each bundle.
   * @param options Scheduling options including the scheduling type.
   * @param candidate_nodes The candidate nodes (possibly pre-filtered by a
   *   label-domain tier).
   * @return A SchedulingResult with selected nodes or an error status.
   */
  SchedulingResult ScheduleNodeLevel(
      const std::vector<const ResourceRequest *> &resource_request_list,
      SchedulingOptions options,
      absl::flat_hash_map<scheduling::NodeID, const Node *> candidate_nodes);

  BundlePackSchedulingPolicy bundle_pack_policy_;
  BundleSpreadSchedulingPolicy bundle_spread_policy_;
  BundleStrictSpreadSchedulingPolicy bundle_strict_spread_policy_;
  BundleStrictPackSchedulingPolicy bundle_strict_pack_policy_;
  LabelDomainStrictPackSchedulingPolicy label_domain_strict_pack_policy_;
};

}  // namespace raylet_scheduling_policy
}  // namespace ray
