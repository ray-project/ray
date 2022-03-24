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

#include "ray/raylet/scheduling/scheduling_ids.h"

namespace ray {
namespace gcs {

double LeastResourceScorer::Score(const ResourceRequest &required_resources,
                                  const NodeResources &node_resources) {
  // In GCS-based actor scheduling, the `predefined_resources` and `custom_resources` (of
  // class `NodeResources`) are only acquired or released by actor scheduling, instead of
  // being updated by resource reports from raylets. So we have to subtract normal task
  // resources (if exist) from the current available resources.
  const NodeResources *node_resources_ptr = &node_resources;
  NodeResources new_node_resources;
  if (!node_resources.normal_task_resources.IsEmpty()) {
    new_node_resources = node_resources;
    for (size_t i = 0;
         i < node_resources.normal_task_resources.predefined_resources.size();
         ++i) {
      new_node_resources.predefined_resources[i].available -=
          node_resources.normal_task_resources.predefined_resources[i];
      if (new_node_resources.predefined_resources[i].available < 0) {
        new_node_resources.predefined_resources[i].available = 0;
      }
    }
    for (const auto &request_resource_entry :
         node_resources.normal_task_resources.custom_resources) {
      auto iter = new_node_resources.custom_resources.find(request_resource_entry.first);
      if (iter != new_node_resources.custom_resources.end()) {
        iter->second.available -= request_resource_entry.second;
        if (iter->second.available < 0) {
          iter->second.available = 0;
        }
      }
    }
    node_resources_ptr = &new_node_resources;
  }

  double node_score = 0.;

  if (required_resources.predefined_resources.size() >
      node_resources_ptr->predefined_resources.size()) {
    return -1.;
  }

  for (size_t i = 0; i < required_resources.predefined_resources.size(); ++i) {
    const auto &request_resource = required_resources.predefined_resources[i];
    const auto &node_available_resource =
        node_resources_ptr->predefined_resources[i].available;
    auto score = Calculate(request_resource, node_available_resource);
    if (score < 0.) {
      return -1.;
    }

    node_score += score;
  }

  for (const auto &request_resource_entry : required_resources.custom_resources) {
    auto iter = node_resources_ptr->custom_resources.find(request_resource_entry.first);
    if (iter == node_resources_ptr->custom_resources.end()) {
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

SchedulingResult SortSchedulingResult(const SchedulingResult &result,
                                      const std::vector<int> &sorted_index) {
  if (result.status.IsSuccess()) {
    std::vector<scheduling::NodeID> sorted_nodes(result.selected_nodes.size());
    for (int i = 0; i < (int)sorted_index.size(); i++) {
      sorted_nodes[sorted_index[i]] = result.selected_nodes[i];
    }
    return SchedulingResult::Success(std::move(sorted_nodes));
  } else {
    return result;
  }
}

SchedulingResult GcsResourceScheduler::Schedule(
    const std::vector<ResourceRequest> &required_resources_list,
    const SchedulingType &scheduling_type,
    const std::function<bool(const scheduling::NodeID &)> &node_filter_func) {
  // Filter candidate nodes.
  auto candidate_nodes = FilterCandidateNodes(node_filter_func);
  if (candidate_nodes.empty()) {
    RAY_LOG(DEBUG) << "The candidate nodes is empty, return directly.";
    return SchedulingResult::Infeasible();
  }

  // If scheduling type is strict pack, we do not need to sort resources of each placement
  // group since they must exist on the same node
  if (scheduling_type == SchedulingType::STRICT_PACK) {
    return StrictPackSchedule(required_resources_list, candidate_nodes);
  }

  // First schedule scarce resources (such as GPU) and large capacity resources to improve
  // the scheduling success rate.
  const auto &sorted_index = SortRequiredResources(required_resources_list);

  std::vector<ResourceRequest> sorted_resources(required_resources_list);
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

absl::flat_hash_set<scheduling::NodeID> GcsResourceScheduler::FilterCandidateNodes(
    const std::function<bool(const scheduling::NodeID &)> &node_filter_func) {
  absl::flat_hash_set<scheduling::NodeID> result;
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

std::vector<int> GcsResourceScheduler::SortRequiredResources(
    const std::vector<ResourceRequest> &required_resources) {
  std::vector<int> sorted_index(required_resources.size());
  std::iota(sorted_index.begin(), sorted_index.end(), 0);

  // Here we sort in reverse order:
  // sort(_, _, a < b) would result in the vector [a < b < c]
  // sort(_, _, a > b) would result in the vector [c > b > a] which leads to our desired
  // outcome of having highest priority `ResourceRequest` being scheduled first.

  std::sort(sorted_index.begin(), sorted_index.end(), [&](int b_idx, int a_idx) {
    const auto &a = required_resources[a_idx];
    const auto &b = required_resources[b_idx];

    RAY_CHECK(a.predefined_resources.size() == (int)PredefinedResources_MAX);
    RAY_CHECK(b.predefined_resources.size() == (int)PredefinedResources_MAX);

    // Make sure that resources are always sorted in the same order
    std::set<uint64_t> extra_resources_set;
    for (auto r : a.custom_resources) {
      extra_resources_set.insert(r.first);
    }
    for (auto r : b.custom_resources) {
      extra_resources_set.insert(r.first);
    }

    // TODO (jon-chuang): the exact resource priority defined here needs to be revisted.

    // Notes: This is a comparator for sorting in c++. We return true if a < b based on a
    // resource at the given level of priority. If tied, we attempt to resolve based on
    // the resource at the next level of priority.
    //
    // The order of priority is: `ResourceRequest`s with GPU requirements first, then
    // extra resources, then object store memory, memory and finally CPU requirements. If
    // two `ResourceRequest`s require a resource under consideration, the one requiring
    // more of the resource is prioritized.

    if (a.predefined_resources[GPU] != b.predefined_resources[GPU]) {
      return a.predefined_resources[GPU] < b.predefined_resources[GPU];
    }
    for (auto r : extra_resources_set) {
      auto a_iter = a.custom_resources.find(r);
      const auto &a_resource = a_iter != a.custom_resources.end() ? a_iter->second : 0;
      auto b_iter = a.custom_resources.find(r);
      const auto &b_resource = b_iter != b.custom_resources.end() ? b_iter->second : 0;
      if (a_resource != b_resource) {
        return a_resource < b_resource;
      }
    }
    for (auto idx : std::vector({OBJECT_STORE_MEM, MEM, CPU})) {
      if (a.predefined_resources[idx] != b.predefined_resources[idx]) {
        return a.predefined_resources[idx] < b.predefined_resources[idx];
      }
    }
    return false;
  });
  return sorted_index;
}

SchedulingResult GcsResourceScheduler::StrictSpreadSchedule(
    const std::vector<ResourceRequest> &required_resources_list,
    const absl::flat_hash_set<scheduling::NodeID> &candidate_nodes) {
  if (required_resources_list.size() > candidate_nodes.size()) {
    RAY_LOG(DEBUG) << "The number of required resources "
                   << required_resources_list.size()
                   << " is greater than the number of candidate nodes "
                   << candidate_nodes.size() << ", scheduling fails.";
    return SchedulingResult::Infeasible();
  }

  std::vector<scheduling::NodeID> result_nodes;
  absl::flat_hash_set<scheduling::NodeID> candidate_nodes_copy(candidate_nodes);
  for (const auto &iter : required_resources_list) {
    // Score and sort nodes.
    auto best_node = GetBestNode(iter, candidate_nodes_copy);

    // There are nodes to meet the scheduling requirements.
    if (!best_node.IsNil()) {
      candidate_nodes_copy.erase(best_node);
      result_nodes.emplace_back(best_node);
    } else {
      // There is no node to meet the scheduling requirements.
      break;
    }
  }

  if (result_nodes.size() != required_resources_list.size()) {
    // Can't meet the scheduling requirements temporarily.
    return SchedulingResult::Failed();
  }
  return SchedulingResult::Success(std::move(result_nodes));
}

SchedulingResult GcsResourceScheduler::SpreadSchedule(
    const std::vector<ResourceRequest> &required_resources_list,
    const absl::flat_hash_set<scheduling::NodeID> &candidate_nodes) {
  std::vector<scheduling::NodeID> result_nodes;
  absl::flat_hash_set<scheduling::NodeID> candidate_nodes_copy(candidate_nodes);
  absl::flat_hash_set<scheduling::NodeID> selected_nodes;
  for (const auto &iter : required_resources_list) {
    // Score and sort nodes.
    auto best_node = GetBestNode(iter, candidate_nodes_copy);

    // There are nodes to meet the scheduling requirements.
    if (!best_node.IsNil()) {
      result_nodes.emplace_back(best_node);
      RAY_CHECK(AllocateRemoteTaskResources(result_nodes.back(), iter));
      candidate_nodes_copy.erase(result_nodes.back());
      selected_nodes.insert(result_nodes.back());
    } else {
      // Scheduling from selected nodes.
      auto best_node = GetBestNode(iter, selected_nodes);
      if (!best_node.IsNil()) {
        result_nodes.push_back(best_node);
        RAY_CHECK(AllocateRemoteTaskResources(result_nodes.back(), iter));
      } else {
        break;
      }
    }
  }

  // Releasing the resources temporarily deducted from `cluster_resource_manager_`.
  ReleaseTemporarilyDeductedResources(required_resources_list, result_nodes);

  if (result_nodes.size() != required_resources_list.size()) {
    // Can't meet the scheduling requirements temporarily.
    return SchedulingResult::Failed();
  }
  return SchedulingResult::Success(std::move(result_nodes));
}

SchedulingResult GcsResourceScheduler::StrictPackSchedule(
    const std::vector<ResourceRequest> &required_resources_list,
    const absl::flat_hash_set<scheduling::NodeID> &candidate_nodes) {
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

  const auto &resource_view = GetResourceView();
  const auto &right_node_it = std::find_if(
      resource_view.begin(),
      resource_view.end(),
      [&aggregated_resource_request](const auto &entry) {
        return entry.second.GetLocalView().IsAvailable(aggregated_resource_request);
      });

  if (right_node_it == resource_view.end()) {
    RAY_LOG(DEBUG) << "The required resource is bigger than the maximum resource in the "
                      "whole cluster, schedule failed.";
    return SchedulingResult::Infeasible();
  }

  std::vector<scheduling::NodeID> result_nodes;

  auto best_node = GetBestNode(aggregated_resource_request, candidate_nodes);

  // Select the node with the highest score.
  // `StrictPackSchedule` does not need to consider the scheduling context, because it
  // only schedules to a node and triggers rescheduling when node dead.
  if (!best_node.IsNil()) {
    for (int index = 0; index < (int)required_resources_list.size(); ++index) {
      result_nodes.emplace_back(best_node);
    }
  }
  if (result_nodes.empty()) {
    // Can't meet the scheduling requirements temporarily.
    return SchedulingResult::Failed();
  }

  return SchedulingResult::Success(std::move(result_nodes));
}

SchedulingResult GcsResourceScheduler::PackSchedule(
    const std::vector<ResourceRequest> &required_resources_list,
    const absl::flat_hash_set<scheduling::NodeID> &candidate_nodes) {
  std::vector<scheduling::NodeID> result_nodes;
  result_nodes.resize(required_resources_list.size());
  absl::flat_hash_set<scheduling::NodeID> candidate_nodes_copy(candidate_nodes);
  std::list<std::pair<int, ResourceRequest>> required_resources_list_copy;
  int index = 0;
  for (const auto &iter : required_resources_list) {
    required_resources_list_copy.emplace_back(index++, iter);
  }

  while (!required_resources_list_copy.empty()) {
    const auto &required_resources_index = required_resources_list_copy.front().first;
    const auto &required_resources = required_resources_list_copy.front().second;
    auto best_node = GetBestNode(required_resources, candidate_nodes_copy);
    if (best_node.IsNil()) {
      // There is no node to meet the scheduling requirements.
      break;
    }

    RAY_CHECK(AllocateRemoteTaskResources(best_node, required_resources));
    result_nodes[required_resources_index] = best_node;
    required_resources_list_copy.pop_front();

    // We try to schedule more resources on one node.
    for (auto iter = required_resources_list_copy.begin();
         iter != required_resources_list_copy.end();) {
      if (AllocateRemoteTaskResources(best_node, iter->second)) {
        result_nodes[iter->first] = best_node;
        required_resources_list_copy.erase(iter++);
      } else {
        ++iter;
      }
    }
    candidate_nodes_copy.erase(best_node);
  }

  // Releasing the resources temporarily deducted from `cluster_resource_manager_`.
  ReleaseTemporarilyDeductedResources(required_resources_list, result_nodes);

  if (!required_resources_list_copy.empty()) {
    // Can't meet the scheduling requirements temporarily.
    return SchedulingResult::Failed();
  }
  return SchedulingResult::Success(std::move(result_nodes));
}

scheduling::NodeID GcsResourceScheduler::GetBestNode(
    const ResourceRequest &required_resources,
    const absl::flat_hash_set<scheduling::NodeID> &candidate_nodes) {
  double best_node_score = -1;
  auto best_node_id = scheduling::NodeID::Nil();

  // Score the nodes.
  for (const auto &node_id : candidate_nodes) {
    const auto &node_resources = GetNodeResources(node_id);
    double node_score = node_scorer_->Score(required_resources, node_resources);
    if (best_node_id.IsNil() || best_node_score < node_score) {
      best_node_id = node_id;
      best_node_score = node_score;
    }
  }
  if (!best_node_id.IsNil() && best_node_score >= 0) {
    return best_node_id;
  }
  return scheduling::NodeID::Nil();
}

void GcsResourceScheduler::ReleaseTemporarilyDeductedResources(
    const std::vector<ResourceRequest> &required_resources_list,
    const std::vector<scheduling::NodeID> &nodes) {
  for (int index = 0; index < (int)nodes.size(); index++) {
    // If `PackSchedule` fails, the id of some nodes may be nil.
    if (!nodes[index].IsNil()) {
      RAY_CHECK(ReleaseRemoteTaskResources(nodes[index], required_resources_list[index]));
    }
  }
}

const NodeResources &GcsResourceScheduler::GetNodeResources(
    const scheduling::NodeID &node_id) const {
  return cluster_resource_manager_->GetNodeResources(
      scheduling::NodeID(node_id.Binary()));
}

bool GcsResourceScheduler::AllocateRemoteTaskResources(
    const scheduling::NodeID &node_id, const ResourceRequest &resource_request) {
  if (!cluster_resource_manager_->HasSufficientResource(
          node_id, resource_request, /*ignore_object_store_memory_requirement=*/true)) {
    return false;
  }
  return cluster_resource_manager_->SubtractNodeAvailableResources(node_id,
                                                                   resource_request);
}

bool GcsResourceScheduler::ReleaseRemoteTaskResources(
    const scheduling::NodeID &node_id, const ResourceRequest &resource_request) {
  return cluster_resource_manager_->AddNodeAvailableResources(node_id, resource_request);
}

const absl::flat_hash_map<scheduling::NodeID, Node>
    &GcsResourceScheduler::GetResourceView() const {
  return cluster_resource_manager_->GetResourceView();
}

}  // namespace gcs
}  // namespace ray
