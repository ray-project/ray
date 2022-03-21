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

#include "ray/raylet/scheduling/policy/scheduling_context.h"
#include "ray/raylet/scheduling/policy/scheduling_policy.h"
#include "ray/raylet/scheduling/policy/scorer.h"

namespace ray {
namespace raylet_scheduling_policy {

class BundleSchedulingPolicy : public ISchedulingPolicy {
 public:
  explicit BundleSchedulingPolicy(
      const absl::flat_hash_map<scheduling::NodeID, Node> &nodes,
      std::function<bool(scheduling::NodeID)> is_node_available,
      std::function<bool(scheduling::NodeID, const ResourceRequest &)>
          add_node_available_resources_fn,
      std::function<bool(scheduling::NodeID, const ResourceRequest &)>
          subtract_node_available_resources_fn)
      : node_scorer_(new LeastResourceScorer()),
        nodes_(nodes),
        is_node_available_(is_node_available),
        add_node_available_resources_fn_(add_node_available_resources_fn),
        subtract_node_available_resources_fn_(subtract_node_available_resources_fn) {}

 protected:
  /// Filter out candidate nodes which can be used for scheduling.
  ///
  /// \return The candidate nodes which can be used for scheduling.
  absl::flat_hash_map<scheduling::NodeID, const Node *> FilterCandidateNodes() const;

  /// Sort required resources according to the scarcity and capacity of resources.
  /// We will first schedule scarce resources (such as GPU) and large capacity resources
  /// to improve the scheduling success rate.
  ///
  /// \param required_resources The resources to be scheduled.
  /// \return The Sorted resources.
  std::vector<int> SortRequiredResources(
      const std::vector<const ResourceRequest *> &resource_request_list);

  /// Score all nodes according to the specified resources.
  ///
  /// \param required_resources The resources to be scheduled.
  /// \param candidate_nodes The nodes can be used for scheduling.
  /// \return Score of all nodes.
  std::pair<scheduling::NodeID, const Node *> GetBestNode(
      const ResourceRequest &required_resources,
      const absl::flat_hash_map<scheduling::NodeID, const Node *> &candidate_nodes) const;

 protected:
  /// Scorer to make a grade to the node.
  std::unique_ptr<NodeScorer> node_scorer_;
  /// List of nodes in the clusters and their resources organized as a map.
  /// The key of the map is the node ID.
  const absl::flat_hash_map<scheduling::NodeID, Node> &nodes_;
  /// Function Checks if node is alive.
  std::function<bool(scheduling::NodeID)> is_node_available_;
  std::function<bool(scheduling::NodeID, const ResourceRequest &)>
      add_node_available_resources_fn_;
  std::function<bool(scheduling::NodeID, const ResourceRequest &)>
      subtract_node_available_resources_fn_;
};

class BundlePackSchedulingPolicy : public BundleSchedulingPolicy {
 public:
  using BundleSchedulingPolicy::BundleSchedulingPolicy;

  SchedulingResult Schedule(
      const std::vector<const ResourceRequest *> &resource_request_list,
      SchedulingOptions options,
      SchedulingContext *context) override;
};

class BundleSpreadSchedulingPolicy : public BundleSchedulingPolicy {
 public:
  using BundleSchedulingPolicy::BundleSchedulingPolicy;

  SchedulingResult Schedule(
      const std::vector<const ResourceRequest *> &resource_request_list,
      SchedulingOptions options,
      SchedulingContext *context) override;
};

class BundleStrictPackSchedulingPolicy : public BundleSchedulingPolicy {
 public:
  using BundleSchedulingPolicy::BundleSchedulingPolicy;

  SchedulingResult Schedule(
      const std::vector<const ResourceRequest *> &resource_request_list,
      SchedulingOptions options,
      SchedulingContext *context) override;
};

class BundleStrictSpreadSchedulingPolicy : public BundleSchedulingPolicy {
 public:
  using BundleSchedulingPolicy::BundleSchedulingPolicy;

  SchedulingResult Schedule(
      const std::vector<const ResourceRequest *> &resource_request_list,
      SchedulingOptions options,
      SchedulingContext *context) override;
};
}  // namespace raylet_scheduling_policy
}  // namespace ray
