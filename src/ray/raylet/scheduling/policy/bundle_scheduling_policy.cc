// Copyright 2021 The Ray Authors.
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

#include "ray/raylet/scheduling/policy/bundle_scheduling_policy.h"

namespace ray {
namespace raylet_scheduling_policy {

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

absl::flat_hash_map<scheduling::NodeID, const Node *>
BundleSchedulingPolicy::SelectCandidateNodes(const SchedulingContext *context) const {
  RAY_UNUSED(context);
  absl::flat_hash_map<scheduling::NodeID, const Node *> result;
  for (const auto &entry : cluster_resource_manager_.GetResourceView()) {
    if (is_node_available_ == nullptr || is_node_available_(entry.first)) {
      result.emplace(entry.first, &entry.second);
    }
  }
  return result;
}

bool BundleSchedulingPolicy::IsRequestFeasible(
    const std::vector<const ResourceRequest *> &resource_request_list,
    const absl::flat_hash_map<scheduling::NodeID, const Node *> &candidate_nodes) const {
  for (const auto &request : resource_request_list) {
    bool bundle_feasible = std::any_of(
        candidate_nodes.begin(), candidate_nodes.end(), [&](const auto &entry) {
          // Validates both resource and label constraints are feasible.
          return entry.second->GetLocalView().IsFeasible(*request);
        });
    if (!bundle_feasible) {
      return false;
    }
  }
  return true;
}

std::pair<std::vector<int>, std::vector<const ResourceRequest *>>
BundleSchedulingPolicy::SortRequiredResources(
    const std::vector<const ResourceRequest *> &resource_request_list) {
  std::vector<int> sorted_index(resource_request_list.size());
  std::iota(sorted_index.begin(), sorted_index.end(), 0);

  // Here we sort in reverse order:
  // sort(_, _, a < b) would result in the vector [a < b < c]
  // sort(_, _, a > b) would result in the vector [c > b > a] which leads to our desired
  // outcome of having highest priority `ResourceRequest` being scheduled first.

  std::sort(sorted_index.begin(), sorted_index.end(), [&](int b_idx, int a_idx) {
    const auto &a = *resource_request_list[a_idx];
    const auto &b = *resource_request_list[b_idx];

    // TODO (jon-chuang): the exact resource priority defined here needs to be revisted.

    // Notes: This is a comparator for sorting in c++. We return true if a < b based on a
    // resource at the given level of priority. If tied, we attempt to resolve based on
    // the resource at the next level of priority.
    //
    // The order of priority is: `ResourceRequest`s with GPU requirements first, then
    // extra resources, then object store memory, memory and finally CPU requirements. If
    // two `ResourceRequest`s require a resource under consideration, the one requiring
    // more of the resource is prioritized.

    auto gpu = scheduling::ResourceID::GPU();
    if (a.Get(gpu) != b.Get(gpu)) {
      return a.Get(gpu) < b.Get(gpu);
    }

    // Make sure that resources are always sorted in the same order
    std::set<scheduling::ResourceID> extra_resources_set;
    for (const auto &r : a.ResourceIds()) {
      if (!r.IsPredefinedResource()) {
        extra_resources_set.insert(r);
      }
    }
    for (const auto &r : b.ResourceIds()) {
      if (!r.IsPredefinedResource()) {
        extra_resources_set.insert(r);
      }
    }

    for (const auto &r : extra_resources_set) {
      auto a_resource = a.Get(r);
      auto b_resource = b.Get(r);
      if (a_resource != b_resource) {
        return a_resource < b_resource;
      }
    }
    for (auto id : std::vector({scheduling::ResourceID::ObjectStoreMemory(),
                                scheduling::ResourceID::Memory(),
                                scheduling::ResourceID::CPU()})) {
      if (a.Get(id) != b.Get(id)) {
        return a.Get(id) < b.Get(id);
      }
    }
    return false;
  });

  std::vector<const ResourceRequest *> sorted_resource_request_list(
      resource_request_list);
  for (size_t i = 0; i < sorted_index.size(); i++) {
    sorted_resource_request_list[i] = resource_request_list[sorted_index[i]];
  }

  return {std::move(sorted_index), std::move(sorted_resource_request_list)};
}

std::pair<scheduling::NodeID, const Node *> BundleSchedulingPolicy::GetBestNode(
    const ResourceRequest &required_resources,
    const absl::flat_hash_map<scheduling::NodeID, const Node *> &candidate_nodes,
    const SchedulingOptions &options) const {
  double best_node_score = -1;
  auto best_node_id = scheduling::NodeID::Nil();
  const Node *best_node = nullptr;

  // Score the nodes.
  for (const auto &[node_id, node] : candidate_nodes) {
    const auto &node_resources = node->GetLocalView();
    double node_score = node_scorer_->Score(required_resources, node_resources);
    if (best_node_id.IsNil() || best_node_score < node_score) {
      best_node_id = node_id;
      best_node_score = node_score;
      best_node = node;
    }
  }
  if (!best_node_id.IsNil() && best_node_score >= 0) {
    return {best_node_id, best_node};
  }
  return {scheduling::NodeID::Nil(), nullptr};
}

////////////////////  BundlePackSchedulingPolicy  ///////////////////////////////
SchedulingResult BundlePackSchedulingPolicy::Schedule(
    const std::vector<const ResourceRequest *> &resource_request_list,
    SchedulingOptions options) {
  RAY_CHECK(!resource_request_list.empty());

  auto candidate_nodes = SelectCandidateNodes(options.scheduling_context_.get());
  if (candidate_nodes.empty()) {
    RAY_LOG(DEBUG) << "The candidate nodes is empty, return directly.";
    return SchedulingResult::Infeasible();
  }

  // First schedule scarce resources (such as GPU) and large capacity resources to improve
  // the scheduling success rate.
  auto sorted_result = SortRequiredResources(resource_request_list);
  const auto &sorted_index = sorted_result.first;
  const auto &sorted_resource_request_list = sorted_result.second;

  if (!IsRequestFeasible(sorted_resource_request_list, candidate_nodes)) {
    RAY_LOG(DEBUG) << "Request requires labels or resources not present in the cluster.";
    return SchedulingResult::Infeasible();
  }

  std::vector<scheduling::NodeID> result_nodes;
  result_nodes.resize(sorted_resource_request_list.size());
  std::vector<std::optional<ResourceAllocation>> allocations;
  allocations.resize(sorted_resource_request_list.size());
  std::list<std::pair<int, const ResourceRequest *>> required_resources_list_copy;
  int index = 0;
  for (const auto &resource_request : sorted_resource_request_list) {
    required_resources_list_copy.emplace_back(index++, resource_request);
  }

  while (!required_resources_list_copy.empty()) {
    const auto &required_resources_index = required_resources_list_copy.front().first;
    const auto &required_resources = required_resources_list_copy.front().second;
    auto best_node = GetBestNode(*required_resources, candidate_nodes, options);
    if (best_node.first.IsNil()) {
      // There is no node to meet the scheduling requirements.
      break;
    }

    const auto &node_resources = best_node.second->GetLocalView();

    auto allocation = cluster_resource_manager_.SubtractNodeAvailableResources(
        best_node.first, *required_resources);
    RAY_CHECK(allocation.has_value());
    allocations[required_resources_index] = std::move(allocation);
    result_nodes[required_resources_index] = best_node.first;
    required_resources_list_copy.pop_front();

    // We try to schedule more resources on one node.
    for (auto iter = required_resources_list_copy.begin();
         iter != required_resources_list_copy.end();) {
      // If the node has sufficient resources, allocate it.
      if (node_resources.IsAvailable(*iter->second)) {
        auto iter_allocation = cluster_resource_manager_.SubtractNodeAvailableResources(
            best_node.first, *iter->second);
        RAY_CHECK(iter_allocation.has_value());
        allocations[iter->first] = std::move(iter_allocation);
        result_nodes[iter->first] = best_node.first;
        required_resources_list_copy.erase(iter++);
      } else {
        // Otherwise try other node.
        ++iter;
      }
    }
    candidate_nodes.erase(best_node.first);
  }

  // Releasing the resources temporarily deducted from `cluster_resource_manager_`.
  for (size_t res_node_idx = 0; res_node_idx < result_nodes.size(); res_node_idx++) {
    // If `PackSchedule` fails, the id of some nodes may be nil.
    if (!result_nodes[res_node_idx].IsNil() && allocations[res_node_idx].has_value()) {
      RAY_CHECK(cluster_resource_manager_.AddNodeAvailableResources(
          result_nodes[res_node_idx], allocations[res_node_idx].value()));
    }
  }

  if (!required_resources_list_copy.empty()) {
    // Can't meet the scheduling requirements temporarily.
    return SchedulingResult::Failed();
  }
  return SortSchedulingResult(SchedulingResult::Success(std::move(result_nodes)),
                              sorted_index);
}

//////////////////////  BundleSpreadSchedulingPolicy  ///////////////////////////
SchedulingResult BundleSpreadSchedulingPolicy::Schedule(
    const std::vector<const ResourceRequest *> &resource_request_list,
    SchedulingOptions options) {
  RAY_CHECK(!resource_request_list.empty());

  auto candidate_nodes = SelectCandidateNodes(options.scheduling_context_.get());
  if (candidate_nodes.empty()) {
    RAY_LOG(DEBUG) << "The candidate nodes is empty, return directly.";
    return SchedulingResult::Infeasible();
  }

  // First schedule scarce resources (such as GPU) and large capacity resources to improve
  // the scheduling success rate.
  auto sorted_result = SortRequiredResources(resource_request_list);
  const auto &sorted_index = sorted_result.first;
  const auto &sorted_resource_request_list = sorted_result.second;

  if (!IsRequestFeasible(sorted_resource_request_list, candidate_nodes)) {
    RAY_LOG(DEBUG) << "Request requires labels or resources not present in the cluster.";
    return SchedulingResult::Infeasible();
  }

  std::vector<scheduling::NodeID> result_nodes;
  std::vector<std::optional<ResourceAllocation>> allocations;
  absl::flat_hash_map<scheduling::NodeID, const Node *> selected_nodes;
  for (const auto &resource_request : sorted_resource_request_list) {
    // Score and sort nodes.
    auto best_node = GetBestNode(*resource_request, candidate_nodes, options);

    // There are nodes to meet the scheduling requirements.
    if (!best_node.first.IsNil()) {
      result_nodes.emplace_back(best_node.first);
      auto allocation = cluster_resource_manager_.SubtractNodeAvailableResources(
          best_node.first, *resource_request);
      RAY_CHECK(allocation.has_value());
      allocations.emplace_back(std::move(allocation));
      candidate_nodes.erase(result_nodes.back());
      selected_nodes.emplace(best_node);
    } else {
      // Scheduling from selected nodes.
      best_node = GetBestNode(*resource_request, selected_nodes, options);
      if (!best_node.first.IsNil()) {
        result_nodes.emplace_back(best_node.first);
        auto allocation = cluster_resource_manager_.SubtractNodeAvailableResources(
            best_node.first, *resource_request);
        RAY_CHECK(allocation.has_value());
        allocations.emplace_back(std::move(allocation));
      } else {
        break;
      }
    }
  }

  // Releasing the resources temporarily deducted from `cluster_resource_manager_`.
  for (size_t index = 0; index < result_nodes.size(); index++) {
    // If `SpreadSchedule` fails, the id of some nodes may be nil.
    if (!result_nodes[index].IsNil() && allocations[index].has_value()) {
      RAY_CHECK(cluster_resource_manager_.AddNodeAvailableResources(
          result_nodes[index], allocations[index].value()));
    }
  }

  if (result_nodes.size() != sorted_resource_request_list.size()) {
    // Can't meet the scheduling requirements temporarily.
    return SchedulingResult::Failed();
  }
  return SortSchedulingResult(SchedulingResult::Success(std::move(result_nodes)),
                              sorted_index);
}

/////////////////////  BundleStrictPackSchedulingPolicy  //////////////////////////
SchedulingResult BundleStrictPackSchedulingPolicy::Schedule(
    const std::vector<const ResourceRequest *> &resource_request_list,
    SchedulingOptions options) {
  RAY_CHECK(!resource_request_list.empty());

  auto candidate_nodes = SelectCandidateNodes(options.scheduling_context_.get());
  if (candidate_nodes.empty()) {
    RAY_LOG(DEBUG) << "The candidate nodes is empty, return directly.";
    return SchedulingResult::Infeasible();
  }

  // Aggregate required resources.
  ResourceRequest aggregated_resource_request;
  LabelSelector aggregated_label_selector;
  for (const auto &resource_request : resource_request_list) {
    for (auto &resource_id : resource_request->ResourceIds()) {
      auto value = aggregated_resource_request.Get(resource_id) +
                   resource_request->Get(resource_id);
      aggregated_resource_request.Set(resource_id, value);
    }
    // Aggregate label constraints from all requests. The selected node
    // must satisfy the union of all label constraints.
    const auto &label_selector = resource_request->GetLabelSelector();
    for (const auto &constraint : label_selector.GetConstraints()) {
      aggregated_label_selector.AddConstraint(constraint);
    }
  }
  aggregated_resource_request.SetLabelSelector(std::move(aggregated_label_selector));

  // Remove any node that does not satisfy the aggregated request.
  for (auto it = candidate_nodes.begin(); it != candidate_nodes.end();) {
    const auto &node_resources = it->second->GetLocalView();
    if (!node_resources.IsFeasible(aggregated_resource_request)) {
      candidate_nodes.erase(it++);
    } else {
      ++it;
    }
  }

  if (candidate_nodes.empty()) {
    RAY_LOG(DEBUG) << "The required resource is bigger than the maximum resource in the "
                      "whole cluster or no node satisfies the label constraints, "
                      "schedule failed.";
    return SchedulingResult::Infeasible();
  }

  // Try each candidate node by allocating bundles one by one (per-bundle
  // SubtractNodeAvailableResources). This avoids the aggregated-request problem
  // where per-instance CanAllocate rejects a summed demand that could be
  // satisfied by distributing across instances.
  auto try_allocate_all_bundles = [&](scheduling::NodeID node_id) -> bool {
    std::vector<std::optional<ResourceAllocation>> allocs;
    for (const auto *request : resource_request_list) {
      auto alloc =
          cluster_resource_manager_.SubtractNodeAvailableResources(node_id, *request);
      if (!alloc.has_value()) {
        // Rollback all previous allocations on this node.
        for (auto &prev : allocs) {
          if (prev.has_value()) {
            cluster_resource_manager_.AddNodeAvailableResources(node_id, *prev);
          }
        }
        return false;
      }
      allocs.push_back(std::move(alloc));
    }
    // Rollback -- we're only checking feasibility, not committing.
    for (auto &a : allocs) {
      if (a.has_value()) {
        cluster_resource_manager_.AddNodeAvailableResources(node_id, *a);
      }
    }
    return true;
  };

  // Prefer the soft target node if specified and viable.
  scheduling::NodeID best_node_id = scheduling::NodeID::Nil();
  if (!options.bundle_strict_pack_soft_target_node_id_.IsNil()) {
    if (candidate_nodes.contains(options.bundle_strict_pack_soft_target_node_id_) &&
        try_allocate_all_bundles(options.bundle_strict_pack_soft_target_node_id_)) {
      best_node_id = options.bundle_strict_pack_soft_target_node_id_;
    }
  }

  if (best_node_id.IsNil()) {
    // Score viable candidates per-bundle using the existing scorer.
    // try_allocate_all_bundles already verified all bundles fit, so each
    // individual Score(bundle) will pass IsAvailable.
    double best_score = -1;
    for (const auto &[node_id, node] : candidate_nodes) {
      if (!try_allocate_all_bundles(node_id)) {
        continue;
      }
      double score = 0;
      for (const auto *request : resource_request_list) {
        score += node_scorer_->Score(*request, node->GetLocalView());
      }
      if (best_node_id.IsNil() || score > best_score) {
        best_score = score;
        best_node_id = node_id;
      }
    }
  }

  std::vector<scheduling::NodeID> result_nodes;
  if (!best_node_id.IsNil()) {
    result_nodes.resize(resource_request_list.size(), best_node_id);
  }
  if (result_nodes.empty()) {
    return SchedulingResult::Failed();
  }

  return SchedulingResult::Success(std::move(result_nodes));
}

/////////////////////  BundleStrictSpreadSchedulingPolicy  //////////////////////////
SchedulingResult BundleStrictSpreadSchedulingPolicy::Schedule(
    const std::vector<const ResourceRequest *> &resource_request_list,
    SchedulingOptions options) {
  RAY_CHECK(!resource_request_list.empty());

  // Filter candidate nodes.
  auto candidate_nodes = SelectCandidateNodes(options.scheduling_context_.get());
  if (candidate_nodes.empty()) {
    RAY_LOG(DEBUG) << "The candidate nodes is empty, return directly.";
    return SchedulingResult::Infeasible();
  }

  if (resource_request_list.size() > candidate_nodes.size()) {
    RAY_LOG(DEBUG) << "The number of required resources " << resource_request_list.size()
                   << " is greater than the number of candidate nodes "
                   << candidate_nodes.size() << ", scheduling fails.";
    return SchedulingResult::Infeasible();
  }

  // First schedule scarce resources (such as GPU) and large capacity resources to improve
  // the scheduling success rate.
  auto sorted_result = SortRequiredResources(resource_request_list);
  const auto &sorted_index = sorted_result.first;
  const auto &sorted_resource_request_list = sorted_result.second;

  if (!IsRequestFeasible(sorted_resource_request_list, candidate_nodes)) {
    RAY_LOG(DEBUG) << "Request requires labels or resources not present in the cluster.";
    return SchedulingResult::Infeasible();
  }

  std::vector<scheduling::NodeID> result_nodes;
  for (const auto &resource_request : sorted_resource_request_list) {
    // Score and sort nodes.
    auto best_node = GetBestNode(*resource_request, candidate_nodes, options);

    // There are nodes to meet the scheduling requirements.
    if (!best_node.first.IsNil()) {
      candidate_nodes.erase(best_node.first);
      result_nodes.emplace_back(best_node.first);
    } else {
      // There is no node to meet the scheduling requirements.
      break;
    }
  }

  if (result_nodes.size() != sorted_resource_request_list.size()) {
    // Can't meet the scheduling requirements temporarily.
    return SchedulingResult::Failed();
  }
  return SortSchedulingResult(SchedulingResult::Success(std::move(result_nodes)),
                              sorted_index);
}

absl::flat_hash_map<scheduling::NodeID, const Node *>
BundleStrictSpreadSchedulingPolicy::SelectCandidateNodes(
    const SchedulingContext *context) const {
  auto bundle_scheduling_context = dynamic_cast<const BundleSchedulingContext *>(context);

  absl::flat_hash_set<scheduling::NodeID> nodes_in_use;
  if (bundle_scheduling_context &&
      bundle_scheduling_context->bundle_locations_.has_value()) {
    const auto &bundle_locations = bundle_scheduling_context->bundle_locations_.value();
    if (bundle_locations != nullptr) {
      for (auto &bundle : *bundle_locations) {
        nodes_in_use.insert(scheduling::NodeID(bundle.second.first.Binary()));
      }
    }
  }

  absl::flat_hash_map<scheduling::NodeID, const Node *> candidate_nodes;
  for (const auto &entry : cluster_resource_manager_.GetResourceView()) {
    if (is_node_available_ && !is_node_available_(entry.first)) {
      continue;
    }

    if (nodes_in_use.contains(entry.first)) {
      continue;
    }

    candidate_nodes.emplace(entry.first, &entry.second);
  }
  return candidate_nodes;
}

}  // namespace raylet_scheduling_policy
}  // namespace ray
