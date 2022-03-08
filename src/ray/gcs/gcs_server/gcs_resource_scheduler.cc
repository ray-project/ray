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

#include <numeric>

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

SchedulingResult SortSchedulingResult(const SchedulingResult &result,
                                      const std::vector<int> &sorted_index) {
  if (result.first == SchedulingResultStatus::SUCCESS) {
    std::vector<NodeID> sorted_nodes(result.second.size());
    for (int i = 0; i < (int)sorted_index.size(); i++) {
      sorted_nodes[sorted_index[i]] = result.second[i];
    }
    return std::make_pair(result.first, sorted_nodes);
  } else {
    return result;
  }
}

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

  // If scheduling type is strict pack, we do not need to sort resources of each placement
  // group since they must exist on the same node
  if (scheduling_type == SchedulingType::STRICT_PACK) {
    return StrictPackSchedule(required_resources_list, candidate_nodes);
  }

  // First schedule scarce resources (such as GPU) and large capacity resources to improve
  // the scheduling success rate.
  const auto &sorted_index = SortRequiredResources(required_resources_list);

  std::vector<ResourceSet> sorted_resources(required_resources_list);
  for (int i = 0; i < (int)sorted_index.size(); i++) {
    sorted_resources[i] = required_resources_list[sorted_index[i]];
  }

  // Score and rank nodes.
  switch (scheduling_type) {
  case PACK:
    return SortSchedulingResult(PackSchedule(sorted_resources, candidate_nodes),
                                sorted_index);
  case SPREAD:
    return SortSchedulingResult(SpreadSchedule(sorted_resources, candidate_nodes),
                                sorted_index);
  case STRICT_SPREAD:
    return SortSchedulingResult(StrictSpreadSchedule(sorted_resources, candidate_nodes),
                                sorted_index);
  default:
    RAY_LOG(FATAL) << "Unsupported scheduling type: " << scheduling_type;
  }
  UNREACHABLE;
}

absl::flat_hash_set<NodeID> GcsResourceScheduler::FilterCandidateNodes(
    const absl::flat_hash_map<NodeID, std::shared_ptr<SchedulingResources>>
        &cluster_resources,
    const std::function<bool(const NodeID &)> &node_filter_func) {
  absl::flat_hash_set<NodeID> result;
  result.reserve(cluster_resources.size());
  for (const auto &iter : cluster_resources) {
    const auto &node_id = iter.first;
    if (node_filter_func == nullptr || node_filter_func(node_id)) {
      result.emplace(node_id);
    }
  }
  return result;
}

std::tuple<bool, bool> cmp_resource(
    const std::string &resource,
    const absl::flat_hash_map<std::string, FixedPoint> &a_map,
    const absl::flat_hash_map<std::string, FixedPoint> &b_map) {
  auto a_resource = a_map.find(resource);
  auto b_resource = b_map.find(resource);

  bool a_less_than_b = false;
  bool finished = false;

  if (a_resource != a_map.end() && b_resource != b_map.end()) {
    if (a_resource->second != b_resource->second) {
      a_less_than_b = a_resource->second < b_resource->second;
      finished = true;
    }
  } else if (a_resource != a_map.end() && a_resource->second != 0) {
    a_less_than_b = false;
    finished = true;
  } else if (b_resource != b_map.end() && b_resource->second != 0) {
    a_less_than_b = true;
    finished = true;
  }
  return std::make_tuple(finished, a_less_than_b);
}

std::vector<int> GcsResourceScheduler::SortRequiredResources(
    const std::vector<ResourceSet> &required_resources) {
  std::vector<int> sorted_index(required_resources.size());
  std::iota(sorted_index.begin(), sorted_index.end(), 0);
  std::sort(sorted_index.begin(), sorted_index.end(), [&](int b, int a) {
    auto a_map = required_resources[a].GetResourceAmountMap();
    auto b_map = required_resources[b].GetResourceAmountMap();
    bool finished, cmp_res;

    // Make sure that resources are always sorted in the same order
    std::set<std::string> extra_resources_set;
    for (auto r : a_map) {
      if (r.first != "CPU" && r.first != "GPU") {
        extra_resources_set.insert(r.first);
      }
    }
    for (auto r : b_map) {
      if (r.first != "CPU" && r.first != "GPU") {
        extra_resources_set.insert(r.first);
      }
    }

    // TODO (jon-chuang): the exact resource priority defined here needs to be revisted.

    // Notes: This is a comparator for sorting in c++. We return true if a < b.
    //
    // Here we are comparing two maps to see if a_map < b_map, based on the given resource
    // e.g. "GPU". If we can resolve the predicate, we return finished = true.
    //
    // Else, we rely on the resource of the next priority level to try to
    // resolve the comparison, all the way until "CPU", after which we return false,
    // since the a_map's and b_map's resources are tied by priority.

    std::tie(finished, cmp_res) = cmp_resource("GPU", a_map, b_map);
    if (finished) {
      return cmp_res;
    }
    for (auto r : extra_resources_set) {
      std::tie(finished, cmp_res) = cmp_resource(r, a_map, b_map);
      if (finished) {
        return cmp_res;
      }
    }
    std::tie(finished, cmp_res) = cmp_resource("CPU", a_map, b_map);
    if (finished) {
      return cmp_res;
    } else {
      return false;
    }
  });
  return sorted_index;
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
    auto best_node = GetBestNode(iter, candidate_nodes_copy);

    // There are nodes to meet the scheduling requirements.
    if (best_node) {
      candidate_nodes_copy.erase(*best_node);
      result_nodes.emplace_back(std::move(*best_node));
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
    auto best_node = GetBestNode(iter, candidate_nodes_copy);

    // There are nodes to meet the scheduling requirements.
    if (best_node) {
      result_nodes.emplace_back(std::move(*best_node));
      RAY_CHECK(gcs_resource_manager_.AcquireResources(result_nodes.back(), iter));
      candidate_nodes_copy.erase(result_nodes.back());
      selected_nodes.insert(result_nodes.back());
    } else {
      // Scheduling from selected nodes.
      auto best_node = GetBestNode(iter, selected_nodes);
      if (best_node) {
        result_nodes.push_back(std::move(*best_node));
        RAY_CHECK(gcs_resource_manager_.AcquireResources(result_nodes.back(), iter));
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
        return required_resources.IsSubset(node_resource.second->GetTotalResources());
      });

  if (right_node_it == cluster_resource.end()) {
    RAY_LOG(DEBUG) << "The required resource is bigger than the maximum resource in the "
                      "whole cluster, schedule failed.";
    return std::make_pair(SchedulingResultStatus::INFEASIBLE, std::vector<NodeID>());
  }

  std::vector<NodeID> result_nodes;

  auto best_node = GetBestNode(required_resources, candidate_nodes);

  // Select the node with the highest score.
  // `StrictPackSchedule` does not need to consider the scheduling context, because it
  // only schedules to a node and triggers rescheduling when node dead.
  if (best_node) {
    for (int index = 0; index < (int)required_resources_list.size(); ++index) {
      result_nodes.emplace_back(std::move(*best_node));
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
    const auto &required_resources_index = required_resources_list_copy.front().first;
    const auto &required_resources = required_resources_list_copy.front().second;
    auto best_node = GetBestNode(required_resources, candidate_nodes_copy);
    if (!best_node) {
      // There is no node to meet the scheduling requirements.
      break;
    }

    RAY_CHECK(gcs_resource_manager_.AcquireResources(*best_node, required_resources));
    result_nodes[required_resources_index] = *best_node;
    required_resources_list_copy.pop_front();

    // We try to schedule more resources on one node.
    for (auto iter = required_resources_list_copy.begin();
         iter != required_resources_list_copy.end();) {
      if (gcs_resource_manager_.AcquireResources(*best_node, iter->second)) {
        result_nodes[iter->first] = *best_node;
        required_resources_list_copy.erase(iter++);
      } else {
        ++iter;
      }
    }
    candidate_nodes_copy.erase(*best_node);
  }

  // Releasing the resources temporarily deducted from `gcs_resource_manager_`.
  ReleaseTemporarilyDeductedResources(required_resources_list, result_nodes);

  if (!required_resources_list_copy.empty()) {
    // Can't meet the scheduling requirements temporarily.
    return std::make_pair(SchedulingResultStatus::FAILED, std::vector<NodeID>());
  }
  return std::make_pair(SchedulingResultStatus::SUCCESS, result_nodes);
}

std::optional<NodeID> GcsResourceScheduler::GetBestNode(
    const ResourceSet &required_resources,
    const absl::flat_hash_set<NodeID> &candidate_nodes) {
  double best_node_score = -1;
  const NodeID *best_node_id = nullptr;
  const auto &cluster_resources = gcs_resource_manager_.GetClusterResources();

  // Score the nodes.
  for (const auto &node_id : candidate_nodes) {
    const auto &iter = cluster_resources.find(node_id);
    RAY_CHECK(iter != cluster_resources.end());
    double node_score = node_scorer_->Score(required_resources, *iter->second);
    if (best_node_id == nullptr || best_node_score < node_score) {
      best_node_id = &node_id;
      best_node_score = node_score;
    }
  }
  if (best_node_id && best_node_score >= 0) {
    return *best_node_id;
  } else {
    return std::nullopt;
  }
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
