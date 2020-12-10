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

#include "ray/gcs/gcs_server/gcs_resource_scheduler.h"
#include "absl/container/flat_hash_set.h"

namespace ray {
namespace gcs {

//////////////////////////////////// Begin of NodeScorer ////////////////////////////////
double LeastResourceScorer::MakeGrade(const ResourceSet &required_resources,
                                      const SchedulingResources &node_resources) {
  const auto &available_resources = node_resources.GetAvailableResources();
  const auto &available_resource_amount_map = available_resources.GetResourceAmountMap();

  double node_score = 0.0;
  for (const auto &entry : required_resources.GetResourceAmountMap()) {
    auto available_resource_amount_iter = available_resource_amount_map.find(entry.first);
    RAY_CHECK(available_resource_amount_iter != available_resource_amount_map.end());
    node_score += Calculate(entry.second, available_resource_amount_iter->second);
  }
  if (!required_resources.GetResourceAmountMap().empty()) {
    node_score /= required_resources.GetResourceAmountMap().size();
  }

  return node_score;
}

double LeastResourceScorer::Calculate(const FractionalResourceQuantity &requested,
                                      const FractionalResourceQuantity &available) {
  if (available == 0 || requested > available) {
    return 0;
  }
  return (available - requested).ToDouble() / available.ToDouble();
}
//////////////////////////////////// End of NodeScorer ////////////////////////////////

/////////////////////////////// Begin of GcsResourceScheduler ///////////////////////////
std::vector<NodeID> GcsResourceScheduler::Schedule(
    std::vector<ResourceSet> required_resources, SchedulingPolicy policy,
    const std::function<bool(const NodeID &)> &node_filter_func) {
  const auto &cluster_resources = gcs_resource_manager_.GetClusterResources();

  // Filter candidate nodes.
  absl::flat_hash_set<NodeID> candidate_nodes = Filter(cluster_resources, node_filter_func);
  if (candidate_nodes.size() < required_resources.size()) {
    return {};
  }

  // TODO(ffbin): Filter required resources.

  // Scorer.
//  std::vector<NodeScore> node_score_list;
//  for (const auto &iter : cluster_resources) {
//    double node_score = node_scorer_->MakeGrade(required_resources, iter->second);
//    node_score_list.emplace_back(NodeScore(iter->first, node_score));
//  }

  // TODO(ffbin): Rank.
  std::vector<NodeID> result;
  return result;
}

absl::flat_hash_set<NodeID> GcsResourceScheduler::Filter(
    const absl::flat_hash_map<NodeID, ResourceSet> &cluster_resources,
    std::function<bool(const NodeID &)> node_filter_func) {
  absl::flat_hash_set<NodeID> result;
  for (const auto &iter : cluster_resources) {
    const auto &node_id = iter.first;
    if (node_filter_func(node_id)) {
      result.emplace(node_id);
    }
  }
  return result;
}
//////////////////////////////////// End of GcsResourceScheduler ////////////////////////////////

}  // namespace gcs
}  // namespace ray
