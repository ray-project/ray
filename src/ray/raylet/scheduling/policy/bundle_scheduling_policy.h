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

#include "ray/common/bundle_spec.h"
#include "ray/raylet/scheduling/cluster_resource_manager.h"
#include "ray/raylet/scheduling/fixed_point.h"
#include "ray/raylet/scheduling/policy/scheduling_context.h"
#include "ray/raylet/scheduling/policy/scheduling_policy.h"
#include "ray/raylet/scheduling/policy/scorer.h"

namespace ray {
namespace raylet_scheduling_policy {

/// Base class for scheduling policies that implement different placement group
/// strategies.
class BundleSchedulingPolicy : public IBundleSchedulingPolicy {
 public:
  explicit BundleSchedulingPolicy(
      ClusterResourceManager &cluster_resource_manager,
      std::function<bool(scheduling::NodeID)> is_node_available)
      : cluster_resource_manager_(cluster_resource_manager),
        is_node_available_(is_node_available),
        node_scorer_(new LeastResourceScorer()) {}

 protected:
  /// Filter out candidate nodes which can be used for scheduling.
  ///
  /// \return The candidate nodes which can be used for scheduling.
  virtual absl::flat_hash_map<scheduling::NodeID, const Node *> SelectCandidateNodes(
      const SchedulingContext *context) const;

  /// Sort required resources according to the scarcity and capacity of resources.
  /// We will first schedule scarce resources (such as GPU) and large capacity resources
  /// to improve the scheduling success rate.
  ///
  /// \param required_resources The resources to be scheduled.
  /// \return The pair of sorted resources index and sorted resource request.
  std::pair<std::vector<int>, std::vector<const ResourceRequest *>> SortRequiredResources(
      const std::vector<const ResourceRequest *> &resource_request_list);

  /// Score all nodes according to the specified resources.
  ///
  /// \param required_resources The resources to be scheduled.
  /// \param candidate_nodes The nodes can be used for scheduling.
  /// \return Score of all nodes.
  std::pair<scheduling::NodeID, const Node *> GetBestNode(
      const ResourceRequest &required_resources,
      const absl::flat_hash_map<scheduling::NodeID, const Node *> &candidate_nodes,
      const SchedulingOptions &options,
      const absl::flat_hash_map<scheduling::NodeID, double>
          &available_cpus_before_bundle_scheduling) const;

  /// Return the map of node id -> available cpus before the current bundle scheduling.
  /// It is used to calculate how many CPUs have been allocated for the current bundles.
  const absl::flat_hash_map<scheduling::NodeID, double>
  GetAvailableCpusBeforeBundleScheduling() const;

 protected:
  /// The cluster resource manager.
  ClusterResourceManager &cluster_resource_manager_;
  /// Function Checks if node is alive.
  std::function<bool(scheduling::NodeID)> is_node_available_;
  /// Scorer to make a grade to the node.
  std::unique_ptr<NodeScorer> node_scorer_;
};

class BundlePackSchedulingPolicy : public BundleSchedulingPolicy {
 public:
  using BundleSchedulingPolicy::BundleSchedulingPolicy;
  SchedulingResult Schedule(
      const std::vector<const ResourceRequest *> &resource_request_list,
      SchedulingOptions options) override;
};

class BundleSpreadSchedulingPolicy : public BundleSchedulingPolicy {
 public:
  using BundleSchedulingPolicy::BundleSchedulingPolicy;
  SchedulingResult Schedule(
      const std::vector<const ResourceRequest *> &resource_request_list,
      SchedulingOptions options) override;
};

class BundleStrictPackSchedulingPolicy : public BundleSchedulingPolicy {
 public:
  using BundleSchedulingPolicy::BundleSchedulingPolicy;
  SchedulingResult Schedule(
      const std::vector<const ResourceRequest *> &resource_request_list,
      SchedulingOptions options) override;
};

class BundleStrictSpreadSchedulingPolicy : public BundleSchedulingPolicy {
 public:
  using BundleSchedulingPolicy::BundleSchedulingPolicy;
  SchedulingResult Schedule(
      const std::vector<const ResourceRequest *> &resource_request_list,
      SchedulingOptions options) override;

 protected:
  /// Filter out candidate nodes which can be used for scheduling.
  ///
  /// \return The candidate nodes which can be used for scheduling.
  absl::flat_hash_map<scheduling::NodeID, const Node *> SelectCandidateNodes(
      const SchedulingContext *context) const override;
};
}  // namespace raylet_scheduling_policy
}  // namespace ray
