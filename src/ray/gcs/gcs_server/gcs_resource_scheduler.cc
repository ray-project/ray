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

namespace ray {
namespace gcs {

double LeastResourceScorer::Score(const ResourceSet &required_resources,
                                  const SchedulingResources &node_resources) {
  // In GCS-based actor scheduling, the `resources_available_` (of class
  // `SchedulingResources`) is only acquired or released by actor scheduling, instead of
  // being updated by resource reports from raylets. So the 'actual' available resources
  // (if there exist normal tasks) are equal to `resources_available_` -
  // `resources_normal_tasks_`.
  ResourceSet new_available_resource_set;
  const ResourceSet *available_resource_set = &node_resources.GetAvailableResources();
  if (!node_resources.GetNormalTaskResources().IsEmpty()) {
    new_available_resource_set = node_resources.GetAvailableResources();
    new_available_resource_set.SubtractResources(node_resources.GetNormalTaskResources());
    available_resource_set = &new_available_resource_set;
  }
  const auto &available_resource_amount_map =
      available_resource_set->GetResourceAmountMap();

  double node_score = 0.0;
  for (const auto &entry : required_resources.GetResourceAmountMap()) {
    auto available_resource_amount_iter = available_resource_amount_map.find(entry.first);
    if (available_resource_amount_iter == available_resource_amount_map.end()) {
      return -1;
    }

    auto calculated_score =
        Calculate(entry.second, available_resource_amount_iter->second);
    if (calculated_score < 0) {
      return -1;
    }
    node_score += calculated_score;
  }

  // TODO(ffbin): We always want to choose the node with the least matching resources. We
  // will solve it in next pr.
  return node_score;
}

double LeastResourceScorer::Calculate(const FixedPoint &requested,
                                      const FixedPoint &available) {
  RAY_CHECK(available >= 0) << "Available resource " << available.Double()
                            << " should be nonnegative.";
  if (requested > available) {
    return -1;
  }
  return (available - requested).Double() / available.Double();
}

/////////////////////////////////////////////////////////////////////////////////////////

SchedulingResult GcsResourceScheduler::Schedule(
    const std::vector<ResourceSet> &required_resources_list,
    const SchedulingType &scheduling_type,
    const std::function<bool(const NodeID &)> &node_filter_func) {
  const auto &cluster_resources = gcs_resource_manager_.GetClusterResources();

  // Filter candidate nodes.
  absl::flat_hash_set<NodeID> candidate_nodes =
      FilterCandidateNodes(cluster_resources, node_filter_func);
  if (candidate_nodes.empty()) {
    RAY_LOG(DEBUG) << "The candidate nodes is empty, return directly.";
    return std::make_pair(SchedulingResultStatus::INFEASIBLE, std::vector<NodeID>());
  }

  // First schedule scarce resources (such as GPU) and large capacity resources to improve
  // the scheduling success rate.
  const auto &to_schedule_resources = SortRequiredResources(required_resources_list);

  // Score and rank nodes.
  SchedulingResult result;
  switch (scheduling_type) {
  case SPREAD:
    result = SpreadSchedule(to_schedule_resources, candidate_nodes);
    break;
  case STRICT_SPREAD:
    result = StrictSpreadSchedule(to_schedule_resources, candidate_nodes);
    break;
  case PACK:
    result = PackSchedule(to_schedule_resources, candidate_nodes);
    break;
  case STRICT_PACK:
    result = StrictPackSchedule(to_schedule_resources, candidate_nodes);
    break;
  default:
    RAY_LOG(FATAL) << "Unsupported scheduling type: " << scheduling_type;
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
    if (node_filter_func == nullptr || node_filter_func(node_id)) {
      result.emplace(node_id);
    }
  }
  return result;
}

const std::vector<ResourceSet> &GcsResourceScheduler::SortRequiredResources(
    const std::vector<ResourceSet> &required_resources) {
  // TODO(ffbin): A bundle may require special resources, such as GPU. We need to
  // schedule bundles with special resource requirements first, which will be implemented
  // in the next pr.
  return required_resources;
}

SchedulingResult GcsResourceScheduler::StrictSpreadSchedule(
    const std::vector<ResourceSet> &required_resources_list,
    const absl::flat_hash_set<NodeID> &candidate_nodes) {
  if (required_resources_list.size() > candidate_nodes.size()) {
    RAY_LOG(DEBUG) << "The number of required resources "
                   << required_resources_list.size()
                   << " is greater than the number of candidate nodes "
                   << candidate_nodes.size() << ", scheduling fails.";
    return std::make_pair(SchedulingResultStatus::INFEASIBLE, std::vector<NodeID>());
  }

  std::vector<NodeID> result_nodes;
  absl::flat_hash_set<NodeID> candidate_nodes_copy(candidate_nodes);
  for (const auto &iter : required_resources_list) {
    // Score and sort nodes.
    const auto &node_scores = ScoreNodes(iter, candidate_nodes_copy);

    // There are nodes to meet the scheduling requirements.
    if (!node_scores.empty() && node_scores.front().second >= 0) {
      const auto &highest_score_node_id = node_scores.front().first;
      result_nodes.push_back(highest_score_node_id);
      candidate_nodes_copy.erase(highest_score_node_id);
    } else {
      // There is no node to meet the scheduling requirements.
      break;
    }
  }

  if (result_nodes.size() != required_resources_list.size()) {
    // Can't meet the scheduling requirements temporarily.
    return std::make_pair(SchedulingResultStatus::FAILED, std::vector<NodeID>());
  }
  return std::make_pair(SchedulingResultStatus::SUCCESS, result_nodes);
}

SchedulingResult GcsResourceScheduler::SpreadSchedule(
    const std::vector<ResourceSet> &required_resources_list,
    const absl::flat_hash_set<NodeID> &candidate_nodes) {
  std::vector<NodeID> result_nodes;
  absl::flat_hash_set<NodeID> candidate_nodes_copy(candidate_nodes);
  absl::flat_hash_set<NodeID> selected_nodes;
  for (const auto &iter : required_resources_list) {
    // Score and sort nodes.
    const auto &node_scores = ScoreNodes(iter, candidate_nodes_copy);

    // There are nodes to meet the scheduling requirements.
    if (!node_scores.empty() && node_scores.front().second >= 0) {
      const auto &highest_score_node_id = node_scores.front().first;
      result_nodes.push_back(highest_score_node_id);
      RAY_CHECK(gcs_resource_manager_.AcquireResources(highest_score_node_id, iter));
      candidate_nodes_copy.erase(highest_score_node_id);
      selected_nodes.insert(highest_score_node_id);
    } else {
      // Scheduling from selected nodes.
      const auto &node_scores = ScoreNodes(iter, selected_nodes);
      if (!node_scores.empty() && node_scores.front().second >= 0) {
        const auto &highest_score_node_id = node_scores.front().first;
        result_nodes.push_back(highest_score_node_id);
        RAY_CHECK(gcs_resource_manager_.AcquireResources(highest_score_node_id, iter));
      } else {
        break;
      }
    }
  }

  // Releasing the resources temporarily deducted from `gcs_resource_manager_`.
  ReleaseTemporarilyDeductedResources(required_resources_list, result_nodes);

  if (result_nodes.size() != required_resources_list.size()) {
    // Can't meet the scheduling requirements temporarily.
    return std::make_pair(SchedulingResultStatus::FAILED, std::vector<NodeID>());
  }
  return std::make_pair(SchedulingResultStatus::SUCCESS, result_nodes);
}

SchedulingResult GcsResourceScheduler::StrictPackSchedule(
    const std::vector<ResourceSet> &required_resources_list,
    const absl::flat_hash_set<NodeID> &candidate_nodes) {
  // Aggregate required resources.
  ResourceSet required_resources;
  for (const auto &iter : required_resources_list) {
    required_resources.AddResources(iter);
  }

  const auto &cluster_resource = gcs_resource_manager_.GetClusterResources();

  const auto &right_node_it = std::find_if(
      cluster_resource.begin(), cluster_resource.end(),
      [required_resources](const auto &node_resource) {
        return required_resources.IsSubset(node_resource.second.GetTotalResources());
      });

  if (right_node_it == cluster_resource.end()) {
    RAY_LOG(DEBUG) << "The required resource is bigger than the maximum resource in the "
                      "whole cluster, schedule failed.";
    return std::make_pair(SchedulingResultStatus::INFEASIBLE, std::vector<NodeID>());
  }

  std::vector<NodeID> result_nodes;
  // Score and sort nodes.
  const auto &node_scores = ScoreNodes(required_resources, candidate_nodes);

  // Select the node with the highest score.
  // `StrictPackSchedule` does not need to consider the scheduling context, because it
  // only schedules to a node and triggers rescheduling when node dead.
  if (!node_scores.empty() && node_scores.front().second >= 0) {
    for (int index = 0; index < (int)required_resources_list.size(); ++index) {
      result_nodes.push_back(node_scores.front().first);
    }
  }
  if (result_nodes.empty()) {
    // Can't meet the scheduling requirements temporarily.
    return std::make_pair(SchedulingResultStatus::FAILED, std::vector<NodeID>());
  }

  return std::make_pair(SchedulingResultStatus::SUCCESS, result_nodes);
}

SchedulingResult GcsResourceScheduler::PackSchedule(
    const std::vector<ResourceSet> &required_resources_list,
    const absl::flat_hash_set<NodeID> &candidate_nodes) {
  std::vector<NodeID> result_nodes;
  result_nodes.resize(required_resources_list.size());
  absl::flat_hash_set<NodeID> candidate_nodes_copy(candidate_nodes);
  std::list<std::pair<int, ResourceSet>> required_resources_list_copy;
  int index = 0;
  for (const auto &iter : required_resources_list) {
    required_resources_list_copy.emplace_back(index++, iter);
  }

  while (!required_resources_list_copy.empty()) {
    // Score and sort nodes.
    const auto &required_resources_index = required_resources_list_copy.front().first;
    const auto &required_resources = required_resources_list_copy.front().second;
    const auto &node_scores = ScoreNodes(required_resources, candidate_nodes_copy);
    if (node_scores.empty() || node_scores.front().second < 0) {
      // There is no node to meet the scheduling requirements.
      break;
    }

    const auto &highest_score_node_id = node_scores.front().first;
    RAY_CHECK(gcs_resource_manager_.AcquireResources(highest_score_node_id,
                                                     required_resources));
    result_nodes[required_resources_index] = highest_score_node_id;
    required_resources_list_copy.pop_front();

    // We try to schedule more resources on one node.
    for (auto iter = required_resources_list_copy.begin();
         iter != required_resources_list_copy.end();) {
      if (gcs_resource_manager_.AcquireResources(highest_score_node_id, iter->second)) {
        result_nodes[iter->first] = highest_score_node_id;
        required_resources_list_copy.erase(iter++);
      } else {
        ++iter;
      }
    }
    candidate_nodes_copy.erase(highest_score_node_id);
  }

  // Releasing the resources temporarily deducted from `gcs_resource_manager_`.
  ReleaseTemporarilyDeductedResources(required_resources_list, result_nodes);

  if (!required_resources_list_copy.empty()) {
    // Can't meet the scheduling requirements temporarily.
    return std::make_pair(SchedulingResultStatus::FAILED, std::vector<NodeID>());
  }
  return std::make_pair(SchedulingResultStatus::SUCCESS, result_nodes);
}

std::list<NodeScore> GcsResourceScheduler::ScoreNodes(
    const ResourceSet &required_resources,
    const absl::flat_hash_set<NodeID> &candidate_nodes) {
  std::list<NodeScore> node_scores;
  const auto &cluster_resources = gcs_resource_manager_.GetClusterResources();

  // Score the nodes.
  for (const auto &node_id : candidate_nodes) {
    const auto &iter = cluster_resources.find(node_id);
    RAY_CHECK(iter != cluster_resources.end());
    double node_score = node_scorer_->Score(required_resources, iter->second);
    node_scores.emplace_back(node_id, node_score);
  }

  // Sort node scores, the large score is in the front.
  node_scores.sort([](const NodeScore &left, const NodeScore &right) {
    return right.second < left.second;
  });
  return node_scores;
}

void GcsResourceScheduler::ReleaseTemporarilyDeductedResources(
    const std::vector<ResourceSet> &required_resources_list,
    const std::vector<NodeID> &nodes) {
  for (int index = 0; index < (int)nodes.size(); index++) {
    // If `PackSchedule` fails, the id of some nodes may be nil.
    if (!nodes[index].IsNil()) {
      RAY_CHECK(gcs_resource_manager_.ReleaseResources(nodes[index],
                                                       required_resources_list[index]));
    }
  }
}

}  // namespace gcs
}  // namespace ray
