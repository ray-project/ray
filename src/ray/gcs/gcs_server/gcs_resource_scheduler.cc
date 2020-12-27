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
    const std::vector<ResourceSet> &required_resources, const SchedulingPolicy &policy,
    const std::function<bool(const NodeID &)> &node_filter_func) {
  const auto &cluster_resources = gcs_resource_manager_.GetClusterResources();

  // Filter candidate nodes.
  absl::flat_hash_set<NodeID> candidate_nodes =
      FilterCandidateNodes(cluster_resources, node_filter_func);
  if (candidate_nodes.empty()) {
    RAY_LOG(INFO) << "The candidate nodes is empty, return directly.";
    return {};
  }

  // First schedule scarce resources (such as GPU) and large capacity resources to improve
  // the scheduling success rate.
  const auto &to_schedule_resources = SortRequiredResources(required_resources);

  // Score and rank nodes.
  std::vector<NodeID> result;
  switch (policy.type_) {
  case SPREAD:
    SpreadSchedule(to_schedule_resources, candidate_nodes, &result);
    break;
  case STRICT_SPREAD:
    StrictSpreadSchedule(to_schedule_resources, candidate_nodes, &result);
    break;
  case PACK:
    PackSchedule(to_schedule_resources, candidate_nodes, &result);
    break;
  case STRICT_PACK:
    StrictPackSchedule(to_schedule_resources, candidate_nodes, &result);
    break;
  default:
    RAY_LOG(FATAL) << "Unsupported policy type: " << policy.type_;
    break;
  }
  return result;
}

absl::flat_hash_set<NodeID> GcsResourceScheduler::FilterCandidateNodes(
    const absl::flat_hash_map<NodeID, SchedulingResources> &cluster_resources,
    const std::function<bool(const NodeID &)> &node_filter_func) {
  absl::flat_hash_set<NodeID> result;
  for (const auto &iter : cluster_resources) {
    const auto &node_id = iter.first;
    if (node_filter_func(node_id)) {
      result.emplace(node_id);
    }
  }
  return result;
}

std::vector<ResourceSet> GcsResourceScheduler::SortRequiredResources(
    const std::vector<ResourceSet> &required_resources) {
  // TODO(ffbin): We will implement it in next pr.
  return required_resources;
}

void GcsResourceScheduler::StrictSpreadSchedule(
    const std::vector<ResourceSet> &required_resources_list,
    const absl::flat_hash_set<NodeID> &candidate_nodes, std::vector<NodeID> *result) {}

void GcsResourceScheduler::SpreadSchedule(
    const std::vector<ResourceSet> &required_resources_list,
    const absl::flat_hash_set<NodeID> &candidate_nodes, std::vector<NodeID> *result) {}

void GcsResourceScheduler::StrictPackSchedule(
    const std::vector<ResourceSet> &required_resources_list,
    const absl::flat_hash_set<NodeID> &candidate_nodes, std::vector<NodeID> *result) {
  // Aggregate required resources.
  ResourceSet required_resources;
  for (const auto &iter : required_resources_list) {
    required_resources.AddResources(iter);
  }

  // Score and sort nodes.
  const auto &node_scores = ScoreNodes(required_resources, candidate_nodes);

  // Select the node with the highest score.
  // `StrictPackSchedule` does not need to consider the scheduling context, because it
  // only schedules to a node and triggers rescheduling when node dead.
  if (!node_scores.empty()) {
    const auto &highest_node_score = node_scores.front();
    if (highest_node_score.second > 0) {
      result->push_back(highest_node_score.first);
    }
  }
}

void GcsResourceScheduler::PackSchedule(
    const std::vector<ResourceSet> &required_resources_list,
    const absl::flat_hash_set<NodeID> &candidate_nodes, std::vector<NodeID> *result) {}

std::list<NodeScore> GcsResourceScheduler::ScoreNodes(
    const ResourceSet &required_resources,
    const absl::flat_hash_set<NodeID> &candidate_nodes) {
  std::list<NodeScore> node_scores;
  const auto &cluster_resources = gcs_resource_manager_.GetClusterResources();
  for (const auto &node_id : candidate_nodes) {
    const auto &iter = cluster_resources.find(node_id);
    RAY_CHECK(iter != cluster_resources.end());
    double node_grade = node_scorer_->MakeGrade(required_resources, iter->second);
    node_scores.emplace_back(node_id, node_grade);
  }
  node_scores.sort([](const NodeScore &left, const NodeScore &right) {
    return right.second < left.second;
  });
  return node_scores;
}

/////////////////////////////// End of GcsResourceScheduler ///////////////////////////

}  // namespace gcs
}  // namespace ray
