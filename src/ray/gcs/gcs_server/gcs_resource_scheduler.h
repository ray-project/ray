// Copyright 2017 The Ray Authors.
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

#include "absl/container/flat_hash_set.h"
#include "ray/common/task/scheduling_resources.h"
#include "ray/gcs/gcs_server/gcs_resource_manager.h"

namespace ray {
namespace gcs {

// Type of resource scheduling strategy.
enum SchedulingType {
  // Places resources across distinct nodes as even as possible.
  SPREAD = 0,
  // Places resources across distinct nodes.
  // It is not allowed to deploy more than one resource on a node.
  STRICT_SPREAD = 1,
  // Packs resources into as few nodes as possible.
  PACK = 2,
  // Packs resources within one node. It is not allowed to span multiple nodes.
  STRICT_PACK = 3,
  SchedulingType_MAX = 4,
};

typedef std::pair<NodeID, double> NodeScore;

/// NodeScorer is a scorer to make a grade to the node, which is used for scheduling
/// decision.
class NodeScorer {
 public:
  virtual ~NodeScorer() = default;

  /// \brief Score according to node resources.
  ///
  /// \param required_resources The required resources.
  /// \param node_resources The node resources which contains available and total
  /// resources.
  /// \return Score of the node.
  virtual double Score(const ResourceSet &required_resources,
                       const SchedulingResources &node_resources) = 0;
};

/// LeastResourceScorer is a score plugin that favors nodes with fewer allocation
/// requested resources based on requested resources.
class LeastResourceScorer : public NodeScorer {
 public:
  double Score(const ResourceSet &required_resources,
               const SchedulingResources &node_resources) override;

 private:
  /// \brief Calculate one of the resource scores.
  ///
  /// \param requested Quantity of one of the required resources.
  /// \param available Quantity of one of the available resources.
  /// \return Score of the node.
  double Calculate(const FractionalResourceQuantity &requested,
                   const FractionalResourceQuantity &available);
};

/// Gcs resource scheduler implementation.
/// Non-thread safe.
class GcsResourceScheduler {
 public:
  GcsResourceScheduler(GcsResourceManager &gcs_resource_manager)
      : gcs_resource_manager_(gcs_resource_manager),
        node_scorer_(new LeastResourceScorer()) {}

  virtual ~GcsResourceScheduler() = default;

  /// Schedule the specified resources to the cluster nodes.
  ///
  /// \param required_resources_list The resources to be scheduled.
  /// \param scheduling_type This scheduling strategy.
  /// \param node_filter_func This function is used to filter candidate nodes. If a node
  /// returns true, it can be used for scheduling. By default, all nodes in the cluster
  /// can be used for scheduling.
  /// \return Scheduling selected nodes, it corresponds to `required_resources_list` one
  /// by one. If the scheduling fails, an empty vector is returned.
  std::vector<NodeID> Schedule(
      const std::vector<ResourceSet> &required_resources_list,
      const SchedulingType &scheduling_type,
      const std::function<bool(const NodeID &)> &node_filter_func = nullptr);

 private:
  /// Filter out candidate nodes which can be used for scheduling.
  ///
  /// \param cluster_resources The cluster node resources.
  /// \param node_filter_func This function is used to filter candidate nodes. If a node
  /// returns true, it can be used for scheduling. By default, all nodes in the cluster
  /// can be used for scheduling.
  /// \return The candidate nodes which can be used for scheduling.
  absl::flat_hash_set<NodeID> FilterCandidateNodes(
      const absl::flat_hash_map<NodeID, SchedulingResources> &cluster_resources,
      const std::function<bool(const NodeID &)> &node_filter_func);

  /// Sort required resources according to the scarcity and capacity of resources.
  /// We will first schedule scarce resources (such as GPU) and large capacity resources
  /// to improve the scheduling success rate.
  ///
  /// \param required_resources The resources to be scheduled.
  /// \return The Sorted resources.
  const std::vector<ResourceSet> &SortRequiredResources(
      const std::vector<ResourceSet> &required_resources);

  /// Schedule resources according to `STRICT_SPREAD` strategy.
  ///
  /// \param required_resources_list The resources to be scheduled.
  /// \param candidate_nodes The nodes can be used for scheduling.
  /// \return Scheduling selected nodes, it corresponds to `required_resources_list` one
  /// by one. If the scheduling fails, an empty vector is returned.
  std::vector<NodeID> StrictSpreadSchedule(
      const std::vector<ResourceSet> &required_resources_list,
      const absl::flat_hash_set<NodeID> &candidate_nodes);

  /// Schedule resources according to `SPREAD` strategy.
  ///
  /// \param required_resources_list The resources to be scheduled.
  /// \param candidate_nodes The nodes can be used for scheduling.
  /// \return Scheduling selected nodes, it corresponds to `required_resources_list` one
  /// by one. If the scheduling fails, an empty vector is returned.
  std::vector<NodeID> SpreadSchedule(
      const std::vector<ResourceSet> &required_resources_list,
      const absl::flat_hash_set<NodeID> &candidate_nodes);

  /// Schedule resources according to `STRICT_PACK` strategy.
  ///
  /// \param required_resources_list The resources to be scheduled.
  /// \param candidate_nodes The nodes can be used for scheduling.
  /// \return Scheduling selected nodes, it corresponds to `required_resources_list` one
  /// by one. If the scheduling fails, an empty vector is returned.
  std::vector<NodeID> StrictPackSchedule(
      const std::vector<ResourceSet> &required_resources_list,
      const absl::flat_hash_set<NodeID> &candidate_nodes);

  /// Schedule resources according to `PACK` strategy.
  ///
  /// \param required_resources_list The resources to be scheduled.
  /// \param candidate_nodes The nodes can be used for scheduling.
  /// \return Scheduling selected nodes, it corresponds to `required_resources_list` one
  /// by one. If the scheduling fails, an empty vector is returned.
  std::vector<NodeID> PackSchedule(
      const std::vector<ResourceSet> &required_resources_list,
      const absl::flat_hash_set<NodeID> &candidate_nodes);

  /// Score all nodes according to the specified resources.
  ///
  /// \param required_resources The resources to be scheduled.
  /// \param candidate_nodes The nodes can be used for scheduling.
  /// \return Score of all nodes.
  std::list<NodeScore> ScoreNodes(const ResourceSet &required_resources,
                                  const absl::flat_hash_set<NodeID> &candidate_nodes);

  /// Return the resources temporarily deducted from gcs resource manager.
  ///
  /// \param required_resources_list The resources to be scheduled.
  /// \param nodes Scheduling selected nodes, it corresponds to `required_resources_list`
  /// one by one.
  void ReleaseTemporarilyDeductedResources(
      const std::vector<ResourceSet> &required_resources_list,
      const std::vector<NodeID> &nodes);

  /// Reference of GcsResourceManager.
  GcsResourceManager &gcs_resource_manager_;

  /// Scorer to make a grade to the node.
  std::unique_ptr<NodeScorer> node_scorer_;
};

}  // namespace gcs
}  // namespace ray
