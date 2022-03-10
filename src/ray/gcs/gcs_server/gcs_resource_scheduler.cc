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

double LeastResourceScorer::Score(const ResourceRequest &required_resources,
                                  const NodeResources &node_resources) {
  // TODO(Shanly): Take normal task resources into account later for GCS-based actor
  // scheduling.

  double node_score = 0.;

  if (required_resources.predefined_resources.size() >
      node_resources.predefined_resources.size()) {
    return -1.;
  }

  for (size_t i = 0; i < required_resources.predefined_resources.size(); ++i) {
    const auto &request_resource = required_resources.predefined_resources[i];
    const auto &node_available_resource =
        node_resources.predefined_resources[i].available;
    auto score = Calculate(request_resource, node_available_resource);
    if (score < 0.) {
      return -1.;
    }

    node_score += score;
  }

  for (const auto &request_resource_entry : required_resources.custom_resources) {
    auto iter = node_resources.custom_resources.find(request_resource_entry.first);
    if (iter == node_resources.custom_resources.end()) {
      return -1.;
    }

    const auto &request_resource = request_resource_entry.second;
    const auto &node_available_resource = iter->second.available;
    auto score = Calculate(request_resource, node_available_resource);
    if (score < 0.) {
      return -1.;
    }

    node_score += score;
  }

  return node_score;
}

double LeastResourceScorer::Calculate(const FixedPoint &requested,
                                      const FixedPoint &available) {
  RAY_CHECK(available >= 0) << "Available resource " << available.Double()
                            << " should be nonnegative.";
  if (requested > available) {
    return -1;
  }

  if (available == 0) {
    return 0;
  }

  return (available - requested).Double() / available.Double();
}

/////////////////////////////////////////////////////////////////////////////////////////

SchedulingResult GcsResourceScheduler::Schedule(
    const std::vector<ResourceRequest> &required_resources_list,
    const SchedulingType &scheduling_type,
    const std::function<bool(const NodeID &)> &node_filter_func) {
  // Filter candidate nodes.
  auto candidate_nodes = FilterCandidateNodes(node_filter_func);
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
    const std::function<bool(const NodeID &)> &node_filter_func) {
  absl::flat_hash_set<NodeID> result;
  const auto &resource_view = GetResourceView();
  result.reserve(resource_view.size());
  for (const auto &iter : resource_view) {
    const auto &node_id = iter.first;
    if (node_filter_func == nullptr || node_filter_func(node_id)) {
      result.emplace(node_id);
    }
  }
  return result;
}

const std::vector<ResourceRequest> &GcsResourceScheduler::SortRequiredResources(
    const std::vector<ResourceRequest> &required_resources) {
  // TODO(ffbin): A bundle may require special resources, such as GPU. We need to
  // schedule bundles with special resource requirements first, which will be implemented
  // in the next pr.
  return required_resources;
}

SchedulingResult GcsResourceScheduler::StrictSpreadSchedule(
    const std::vector<ResourceRequest> &required_resources_list,
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
    const std::vector<ResourceRequest> &required_resources_list,
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
      RAY_CHECK(AllocateRemoteTaskResources(result_nodes.back(), iter));
      candidate_nodes_copy.erase(result_nodes.back());
      selected_nodes.insert(result_nodes.back());
    } else {
      // Scheduling from selected nodes.
      auto best_node = GetBestNode(iter, selected_nodes);
      if (best_node) {
        result_nodes.push_back(std::move(*best_node));
        RAY_CHECK(AllocateRemoteTaskResources(result_nodes.back(), iter));
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
    const std::vector<ResourceRequest> &required_resources_list,
    const absl::flat_hash_set<NodeID> &candidate_nodes) {
  // Aggregate required resources.
  ResourceRequest aggregated_resource_request;
  for (const auto &resource_request : required_resources_list) {
    if (aggregated_resource_request.predefined_resources.size() <
        resource_request.predefined_resources.size()) {
      aggregated_resource_request.predefined_resources.resize(
          resource_request.predefined_resources.size());
    }
    for (size_t i = 0; i < resource_request.predefined_resources.size(); ++i) {
      aggregated_resource_request.predefined_resources[i] +=
          resource_request.predefined_resources[i];
    }
    for (const auto &entry : resource_request.custom_resources) {
      aggregated_resource_request.custom_resources[entry.first] += entry.second;
    }
  }

  const auto &cluster_resource = GetResourceView();
  const auto &right_node_it = std::find_if(
      cluster_resource.begin(),
      cluster_resource.end(),
      [&aggregated_resource_request](const auto &entry) {
        return entry.second->GetLocalView().IsAvailable(aggregated_resource_request);
      });

  if (right_node_it == cluster_resource.end()) {
    RAY_LOG(DEBUG) << "The required resource is bigger than the maximum resource in the "
                      "whole cluster, schedule failed.";
    return std::make_pair(SchedulingResultStatus::INFEASIBLE, std::vector<NodeID>());
  }

  std::vector<NodeID> result_nodes;

  auto best_node = GetBestNode(aggregated_resource_request, candidate_nodes);

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
    const std::vector<ResourceRequest> &required_resources_list,
    const absl::flat_hash_set<NodeID> &candidate_nodes) {
  std::vector<NodeID> result_nodes;
  result_nodes.resize(required_resources_list.size());
  absl::flat_hash_set<NodeID> candidate_nodes_copy(candidate_nodes);
  std::list<std::pair<int, ResourceRequest>> required_resources_list_copy;
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

    RAY_CHECK(AllocateRemoteTaskResources(*best_node, required_resources));
    result_nodes[required_resources_index] = *best_node;
    required_resources_list_copy.pop_front();

    // We try to schedule more resources on one node.
    for (auto iter = required_resources_list_copy.begin();
         iter != required_resources_list_copy.end();) {
      if (AllocateRemoteTaskResources(*best_node, iter->second)) {
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
    const ResourceRequest &required_resources,
    const absl::flat_hash_set<NodeID> &candidate_nodes) {
  double best_node_score = -1;
  const NodeID *best_node_id = nullptr;

  // Score the nodes.
  for (const auto &node_id : candidate_nodes) {
    const auto &node_resources = GetNodeResources(node_id);
    double node_score = node_scorer_->Score(required_resources, node_resources);
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
  return std::nullopt;
}

void GcsResourceScheduler::ReleaseTemporarilyDeductedResources(
    const std::vector<ResourceRequest> &required_resources_list,
    const std::vector<NodeID> &nodes) {
  for (int index = 0; index < (int)nodes.size(); index++) {
    // If `PackSchedule` fails, the id of some nodes may be nil.
    if (!nodes[index].IsNil()) {
      RAY_CHECK(ReleaseRemoteTaskResources(nodes[index], required_resources_list[index]));
    }
  }
}

const NodeResources &GcsResourceScheduler::GetNodeResources(const NodeID &node_id) const {
  const auto &resource_view = GetResourceView();
  auto iter = resource_view.find(node_id);
  RAY_CHECK(iter != resource_view.end());
  return iter->second->GetLocalView();
}

bool GcsResourceScheduler::AllocateRemoteTaskResources(
    const NodeID &node_id, const ResourceRequest &resource_request) {
  return gcs_resource_manager_.AcquireResources(node_id, resource_request);
}

bool GcsResourceScheduler::ReleaseRemoteTaskResources(
    const NodeID &node_id, const ResourceRequest &resource_request) {
  return gcs_resource_manager_.ReleaseResources(node_id, resource_request);
}

const absl::flat_hash_map<NodeID, std::shared_ptr<Node>>
    &GcsResourceScheduler::GetResourceView() const {
  return gcs_resource_manager_.GetClusterResources();
}

}  // namespace gcs
}  // namespace ray
