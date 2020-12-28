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
  // TODO(ffbin): A bundle may require special resources, such as GPU. We need to
  // schedule bundles with special resource requirements first, which will be implemented
  // in the next pr.
  return required_resources;
}

void GcsResourceScheduler::StrictSpreadSchedule(
    const std::vector<ResourceSet> &required_resources_list,
    const absl::flat_hash_set<NodeID> &candidate_nodes, std::vector<NodeID> *result) {
  if (required_resources_list.size() > candidate_nodes.size()) {
    RAY_LOG(INFO) << "The number of required resources is greater than the number of "
                     "candidate nodes, scheduling fails.";
    return;
  }

  absl::flat_hash_set<NodeID> candidate_nodes_copy(candidate_nodes);
  for (const auto &iter : required_resources_list) {
    // Score and sort nodes.
    const auto &node_scores = ScoreNodes(iter, candidate_nodes_copy);

    // There are nodes to meet the scheduling requirements.
    if (!node_scores.empty() && node_scores.front().second > 0) {
      const auto &highest_score_node_id = node_scores.front().first;
      result->push_back(highest_score_node_id);
      candidate_nodes_copy.erase(highest_score_node_id);
    } else {
      // There is no node to meet the scheduling requirements.
      break;
    }
  }

  if (result->size() != required_resources_list.size()) {
    // Unable to meet the resources required for scheduling, scheduling failed.
    result->clear();
  }
}

void GcsResourceScheduler::SpreadSchedule(
    const std::vector<ResourceSet> &required_resources_list,
    const absl::flat_hash_set<NodeID> &candidate_nodes, std::vector<NodeID> *result) {
  absl::flat_hash_set<NodeID> candidate_nodes_copy(candidate_nodes);
  absl::flat_hash_set<NodeID> selected_nodes;
  for (const auto &iter : required_resources_list) {
    // Score and sort nodes.
    const auto &node_scores = ScoreNodes(iter, candidate_nodes_copy);

    // There are nodes to meet the scheduling requirements.
    if (!node_scores.empty() && node_scores.front().second > 0) {
      const auto &highest_score_node_id = node_scores.front().first;
      result->push_back(highest_score_node_id);
      RAY_CHECK(gcs_resource_manager_.AcquireResources(highest_score_node_id, iter));
      candidate_nodes_copy.erase(highest_score_node_id);
      selected_nodes.insert(highest_score_node_id);
    } else {
      // Scheduling from selected nodes.
      const auto &node_scores = ScoreNodes(iter, candidate_nodes_copy);
      if (!node_scores.empty() && node_scores.front().second > 0) {
        const auto &highest_score_node_id = node_scores.front().first;
        result->push_back(highest_score_node_id);
        RAY_CHECK(gcs_resource_manager_.AcquireResources(highest_score_node_id, iter));
      } else {
        break;
      }
    }
  }

  if (result->size() != required_resources_list.size()) {
    // Unable to meet the resources required for scheduling, scheduling failed.
    result->clear();
  } else {
    // Scheduling succeeded, releasing the resources temporarily deducted from
    // `gcs_resource_manager_`.
    ReleaseTemporarilyDeductedResources(required_resources_list, *result);
  }
}

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
  if (!node_scores.empty() && node_scores.front().second > 0) {
    result->push_back(node_scores.front().first);
  }
}

void GcsResourceScheduler::PackSchedule(
    const std::vector<ResourceSet> &required_resources_list,
    const absl::flat_hash_set<NodeID> &candidate_nodes, std::vector<NodeID> *result) {
  result->resize(required_resources_list.size());
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
    if (node_scores.empty() || node_scores.front().second <= 0) {
      // There is no node to meet the scheduling requirements.
      break;
    }

    const auto &highest_score_node_id = node_scores.front().first;
    result->at(required_resources_index) = highest_score_node_id;
    required_resources_list_copy.pop_front();

    // We try to schedule more resources on one node.
    for (auto iter = required_resources_list_copy.begin();
         iter != required_resources_list_copy.end();) {
      if (gcs_resource_manager_.AcquireResources(highest_score_node_id, iter->second)) {
        result->at(iter->first) = highest_score_node_id;
        required_resources_list_copy.erase(iter++);
      } else {
        ++iter;
      }
    }
    candidate_nodes_copy.erase(highest_score_node_id);
  }

  if (result->size() != required_resources_list.size()) {
    // Unable to meet the resources required for scheduling, scheduling failed.
    result->clear();
  } else {
    // Scheduling succeeded, releasing the resources temporarily deducted from
    // `gcs_resource_manager_`.
    ReleaseTemporarilyDeductedResources(required_resources_list, *result);
  }
}

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

void GcsResourceScheduler::ReleaseTemporarilyDeductedResources(
    const std::vector<ResourceSet> &required_resources_list,
    const std::vector<NodeID> &nodes) {
  RAY_CHECK(required_resources_list.size() == nodes.size());
  for (int index = 0; index < nodes.size(); ++index) {
    gcs_resource_manager_.ReleaseResources(nodes[index], required_resources_list[index]);
  }
}

/////////////////////////////// End of GcsResourceScheduler ///////////////////////////

}  // namespace gcs
}  // namespace ray
