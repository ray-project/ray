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
#include <optional>

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "ray/common/id.h"
#include "ray/gcs/gcs_server/gcs_resource_manager.h"
#include "ray/raylet/scheduling/cluster_resource_data.h"
#include "ray/raylet/scheduling/cluster_resource_manager.h"
#include "ray/raylet/scheduling/policy/scheduling_policy.h"

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

using SchedulingResultStatus = raylet_scheduling_policy::SchedulingResultStatus;
using SchedulingResult = raylet_scheduling_policy::SchedulingResult;
using NodeScore = std::pair<scheduling::NodeID, double>;

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
  virtual double Score(const ResourceRequest &required_resources,
                       const NodeResources &node_resources) = 0;
};

/// LeastResourceScorer is a score plugin that favors nodes with fewer allocation
/// requested resources based on requested resources.
class LeastResourceScorer : public NodeScorer {
 public:
  double Score(const ResourceRequest &required_resources,
               const NodeResources &node_resources) override;

 private:
  /// \brief Calculate one of the resource scores.
  ///
  /// \param requested Quantity of one of the required resources.
  /// \param available Quantity of one of the available resources.
  /// \return Score of the node.
  double Calculate(const FixedPoint &requested, const FixedPoint &available);
};

/// Gcs resource scheduler implementation.
/// Non-thread safe.
class GcsResourceScheduler {
 public:
  GcsResourceScheduler()
      : cluster_resource_manager_(std::make_unique<ClusterResourceManager>()),
        node_scorer_(new LeastResourceScorer()) {}

  virtual ~GcsResourceScheduler() = default;

  /// Schedule the specified resources to the cluster nodes.
  ///
  /// \param required_resources_list The resources to be scheduled.
  /// \param scheduling_type This scheduling strategy.
  /// \param node_filter_func This function is used to filter candidate nodes. If a node
  /// returns true, it can be used for scheduling. By default, all nodes in the cluster
  /// can be used for scheduling.
  /// \return `SchedulingResult`, including the selected nodes if schedule successful,
  /// otherwise, it will return an empty vector and a flag to indicate whether this
  /// request can be retry or not.
  SchedulingResult Schedule(
      const std::vector<ResourceRequest> &required_resources_list,
      const SchedulingType &scheduling_type,
      const std::function<bool(const scheduling::NodeID &)> &node_filter_func = nullptr);

  ClusterResourceManager &GetClusterResourceManager() {
    return *cluster_resource_manager_;
  }

 private:
  /// Filter out candidate nodes which can be used for scheduling.
  ///
  /// \param cluster_resources The cluster node resources.
  /// \param node_filter_func This function is used to filter candidate nodes. If a node
  /// returns true, it can be used for scheduling. By default, all nodes in the cluster
  /// can be used for scheduling.
  /// \return The candidate nodes which can be used for scheduling.
  absl::flat_hash_set<scheduling::NodeID> FilterCandidateNodes(
      const std::function<bool(const scheduling::NodeID &)> &node_filter_func);

  /// Sort required resources according to the scarcity and capacity of resources.
  /// We will first schedule scarce resources (such as GPU) and large capacity resources
  /// to improve the scheduling success rate.
  ///
  /// \param required_resources The resources to be scheduled.
  /// \return The Sorted resources.
  std::vector<int> SortRequiredResources(
      const std::vector<ResourceRequest> &required_resources);

  /// Schedule resources according to `STRICT_SPREAD` strategy.
  ///
  /// \param required_resources_list The resources to be scheduled.
  /// \param candidate_nodes The nodes can be used for scheduling.
  /// \return `SchedulingResult`, including the selected nodes if schedule successful,
  /// otherwise, it will return an empty vector and a flag to indicate whether this
  /// request can be retry or not.
  SchedulingResult StrictSpreadSchedule(
      const std::vector<ResourceRequest> &required_resources_list,
      const absl::flat_hash_set<scheduling::NodeID> &candidate_nodes);

  /// Schedule resources according to `SPREAD` strategy.
  ///
  /// \param required_resources_list The resources to be scheduled.
  /// \param candidate_nodes The nodes can be used for scheduling.
  /// \return `SchedulingResult`, including the selected nodes if schedule successful,
  /// otherwise, it will return an empty vector and a flag to indicate whether this
  /// request can be retry or not.
  SchedulingResult SpreadSchedule(
      const std::vector<ResourceRequest> &required_resources_list,
      const absl::flat_hash_set<scheduling::NodeID> &candidate_nodes);

  /// Schedule resources according to `STRICT_PACK` strategy.
  ///
  /// \param required_resources_list The resources to be scheduled.
  /// \param candidate_nodes The nodes can be used for scheduling.
  /// \return `SchedulingResult`, including the selected nodes if schedule successful,
  /// otherwise, it will return an empty vector and a flag to indicate whether this
  /// request can be retry or not.
  SchedulingResult StrictPackSchedule(
      const std::vector<ResourceRequest> &required_resources_list,
      const absl::flat_hash_set<scheduling::NodeID> &candidate_nodes);

  /// Schedule resources according to `PACK` strategy.
  ///
  /// \param required_resources_list The resources to be scheduled.
  /// \param candidate_nodes The nodes can be used for scheduling.
  /// \return `SchedulingResult`, including the selected nodes if schedule successful,
  /// otherwise, it will return an empty vector and a flag to indicate whether this
  /// request can be retry or not.
  SchedulingResult PackSchedule(
      const std::vector<ResourceRequest> &required_resources_list,
      const absl::flat_hash_set<scheduling::NodeID> &candidate_nodes);

  /// Score all nodes according to the specified resources.
  ///
  /// \param required_resources The resources to be scheduled.
  /// \param candidate_nodes The nodes can be used for scheduling.
  /// \return Score of all nodes.
  scheduling::NodeID GetBestNode(
      const ResourceRequest &required_resources,
      const absl::flat_hash_set<scheduling::NodeID> &candidate_nodes);

  /// Get node resources.
  const NodeResources &GetNodeResources(const scheduling::NodeID &node_id) const;

  /// Return the resources temporarily deducted from gcs resource manager.
  ///
  /// \param required_resources_list The resources to be scheduled.
  /// \param nodes Scheduling selected nodes, it corresponds to `required_resources_list`
  /// one by one.
  void ReleaseTemporarilyDeductedResources(
      const std::vector<ResourceRequest> &required_resources_list,
      const std::vector<scheduling::NodeID> &nodes);

  /// Subtract the resources required by a given resource request (resource_request) from
  /// a given remote node.
  ///
  /// \param node_id Remote node whose resources we allocate.
  /// \param resource_request Task for which we allocate resources.
  /// \return True if remote node has enough resources to satisfy the resource request.
  /// False otherwise.
  bool AllocateRemoteTaskResources(const scheduling::NodeID &node_id,
                                   const ResourceRequest &resource_request);

  bool ReleaseRemoteTaskResources(const scheduling::NodeID &node_id,
                                  const ResourceRequest &resource_request);

  const absl::flat_hash_map<scheduling::NodeID, Node> &GetResourceView() const;

  /// Reference of GcsResourceManager.
  std::unique_ptr<ClusterResourceManager> cluster_resource_manager_;

  /// Scorer to make a grade to the node.
  std::unique_ptr<NodeScorer> node_scorer_;
};

}  // namespace gcs
}  // namespace ray
