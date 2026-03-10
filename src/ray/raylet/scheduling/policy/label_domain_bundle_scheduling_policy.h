// Copyright 2026 The Ray Authors.
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

#include <string>
#include <vector>

#include "ray/raylet/scheduling/cluster_resource_manager.h"
#include "ray/raylet/scheduling/policy/scheduling_policy.h"

namespace ray {
namespace raylet_scheduling_policy {

/**
 * @brief A single candidate label domain with its associated nodes.
 *
 * @param label_domain The value of the label key for this domain
 *   (e.g. "rack-1").
 * @param candidate_nodes The set of nodes that belong to this domain.
 */
struct LabelDomainCandidate {
  std::string label_domain;
  absl::flat_hash_map<scheduling::NodeID, const Node *> candidate_nodes;
};

/**
 * @brief Result of filtering candidate nodes by label domain.
 *
 * @param status The scheduling result status (SUCCESS, INFEASIBLE, etc.).
 * @param candidates The list of feasible label-domain candidates. Empty when
 *   the status is not SUCCESS.
 */
struct LabelDomainFilterResult {
  SchedulingResultStatus status;
  std::vector<LabelDomainCandidate> candidates;
};

/**
 * @brief Abstract base class for label-domain-level scheduling policies.
 *
 * @details Label-domain scheduling partitions the cluster's candidate nodes
 * into groups by an arbitrary node label key (e.g. "ray.io/gpu-domain") and
 * selects which groups can feasibly host a placement group's bundles.
 */
class LabelDomainSchedulingPolicyInterface {
 public:
  explicit LabelDomainSchedulingPolicyInterface(
      ClusterResourceManager &cluster_resource_manager)
      : cluster_resource_manager_(cluster_resource_manager) {}
  virtual ~LabelDomainSchedulingPolicyInterface() = default;

  /**
   * @brief Filters candidate nodes into label-domain groups and returns
   * the feasible groups.
   *
   * @param resource_request_list The resource/label requirements for each
   * bundle in the placement group.
   * @param options Scheduling options that contains which label domain key to group by.
   *   If the label domain value is specified, scheduling is constrained to that specific
   * domain (bundle rescheduling).
   * @param candidate_nodes All available candidate nodes to consider.
   * @return A LabelDomainFilterResult with the feasible candidate groups, or
   *   an INFEASIBLE / FAILED status if no group qualifies.
   */
  virtual LabelDomainFilterResult FilterCandidateNodes(
      const std::vector<const ResourceRequest *> &resource_request_list,
      const SchedulingOptions &options,
      absl::flat_hash_map<scheduling::NodeID, const Node *> candidate_nodes) = 0;

 protected:
  /**
   * @brief Checks whether a set of bundles can be placed on the given nodes.
   *
   * @details Checks that every bundle is individually feasible on
   * at least one candidate node, and the aggregate resource demand across
   * all bundles does not exceed the aggregate total resources of the candidate
   * nodes.
   *
   * @param resource_request_list The resource requirements for each bundle.
   * @param candidate_nodes The candidate nodes to check against.
   * @return True if the request is feasible, false otherwise.
   */
  bool IsRequestFeasible(
      const std::vector<const ResourceRequest *> &resource_request_list,
      const absl::flat_hash_map<scheduling::NodeID, const Node *> &candidate_nodes) const;

  ClusterResourceManager &cluster_resource_manager_;
};

/**
 * @brief Strict-pack label-domain scheduling policy.
 *
 * @details Ensures that each bundle is placed on a node in the same label domain value.
 */
class LabelDomainStrictPackSchedulingPolicy
    : public LabelDomainSchedulingPolicyInterface {
 public:
  using LabelDomainSchedulingPolicyInterface::LabelDomainSchedulingPolicyInterface;

  LabelDomainFilterResult FilterCandidateNodes(
      const std::vector<const ResourceRequest *> &resource_request_list,
      const SchedulingOptions &options,
      absl::flat_hash_map<scheduling::NodeID, const Node *> candidate_nodes) override;
};

}  // namespace raylet_scheduling_policy
}  // namespace ray
