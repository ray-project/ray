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

enum SchedulingType {
  SPREAD = 0,
  STRICT_SPREAD = 1,
  PACK = 2,
  STRICT_PACK = 3,
  SchedulingType_MAX = 4,
};

typedef std::pair<NodeID, double> NodeScore;

class SchedulingPolicy {
 public:
  SchedulingPolicy(const SchedulingType &type) : type_(type) {}

  const SchedulingType type_;
};

/// NodeScorer is a scorer to make a grade to the node, which is used for scheduling
/// decision.
class NodeScorer {
 public:
  virtual ~NodeScorer() = default;

  /// \brief Make a grade based on the node resources.
  ///
  /// \param required_resources The required resources.
  /// \param node_resources The node resources which contains available and total
  /// resources.
  virtual double MakeGrade(const ResourceSet &required_resources,
                           const SchedulingResources &node_resources) = 0;
};

/// LeastResourceScorer is a score plugin that favors nodes with fewer allocation
/// requested resources based on requested resources.
class LeastResourceScorer : public NodeScorer {
 public:
  /// \brief Make a grade based on the node resources.
  ///
  /// \param required_resources The required resources.
  /// \param node_resources The node resources which contains available and total
  /// resources.
  /// \return Score of the node.
  double MakeGrade(const ResourceSet &required_resources,
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

// class ResourceScheduleContext {
// public:
//  virtual ~ResourceScheduleContext() {}
//
//  virtual bool IsNodeSelected(const NodeID &node_id) {
//    return std::find(selected_nodes.begin(), selected_nodes.end(), node_id) !=
//        selected_nodes.end();
//  }
//
//  virtual bool IsNodeSchedulable(const NodeID &node_id) { return true; }
//
//  std::vector<NodeID> selected_nodes;
//};

/// Gcs resource scheduler implementation.
/// Non-thread safe.
class GcsResourceScheduler {
 public:
  GcsResourceScheduler(GcsResourceManager &gcs_resource_manager)
      : gcs_resource_manager_(gcs_resource_manager),
        node_scorer_(new LeastResourceScorer()) {}

  virtual ~GcsResourceScheduler() = default;

  std::vector<NodeID> Schedule(
      const std::vector<ResourceSet> &required_resources, const SchedulingPolicy &policy,
      const std::function<bool(const NodeID &)> &node_filter_func);

 private:
  absl::flat_hash_set<NodeID> FilterCandidateNodes(
      const absl::flat_hash_map<NodeID, SchedulingResources> &cluster_resources,
      const std::function<bool(const NodeID &)> &node_filter_func);

  std::vector<ResourceSet> SortRequiredResources(
      const std::vector<ResourceSet> &required_resources);

  void StrictSpreadSchedule(const std::vector<ResourceSet> &required_resources_list,
                            const absl::flat_hash_set<NodeID> &candidate_nodes,
                            std::vector<NodeID> *result);

  void SpreadSchedule(const std::vector<ResourceSet> &required_resources_list,
                      const absl::flat_hash_set<NodeID> &candidate_nodes,
                      std::vector<NodeID> *result);

  void StrictPackSchedule(const std::vector<ResourceSet> &required_resources_list,
                          const absl::flat_hash_set<NodeID> &candidate_nodes,
                          std::vector<NodeID> *result);

  void PackSchedule(const std::vector<ResourceSet> &required_resources_list,
                    const absl::flat_hash_set<NodeID> &candidate_nodes,
                    std::vector<NodeID> *result);

  std::list<NodeScore> ScoreNodes(const ResourceSet &required_resources,
                                  const absl::flat_hash_set<NodeID> &candidate_nodes);

  /// Reference of GcsResourceManager.
  GcsResourceManager &gcs_resource_manager_;

  /// Scorer to make a grade to the node.
  std::unique_ptr<NodeScorer> node_scorer_;
};

}  // namespace gcs
}  // namespace ray
