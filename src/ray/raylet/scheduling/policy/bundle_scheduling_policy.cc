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

bool BundleSchedulingPolicy::IsRequestFeasible(
    const std::vector<const ResourceRequest *> &resource_request_list,
    const absl::flat_hash_set<scheduling::NodeID> &candidate_nodes) const {
  for (const auto &request : resource_request_list) {
    bool bundle_feasible = std::any_of(
        candidate_nodes.begin(), candidate_nodes.end(), [&](const auto &node_id) {
          // Validates both resource and label constraints are feasible.
          return cluster_resource_manager_.HasFeasibleResources(node_id, *request);
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

scheduling::NodeID BundleSchedulingPolicy::GetBestNode(
    const ResourceRequest &required_resources,
    const absl::flat_hash_set<scheduling::NodeID> &candidate_nodes,
    const SchedulingOptions &options) const {
  double best_node_score = -1;
  auto best_node_id = scheduling::NodeID::Nil();

  // Score the nodes.
  for (const auto &node_id : candidate_nodes) {
    const auto &node_resources = cluster_resource_manager_.GetNodeResources(node_id);
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

////////////////////  BundlePackSchedulingPolicy  ///////////////////////////////
SchedulingResult BundlePackSchedulingPolicy::Schedule(
    const std::vector<const ResourceRequest *> &resource_request_list,
    SchedulingOptions options,
    absl::flat_hash_set<scheduling::NodeID> candidate_nodes) {
  RAY_CHECK(!resource_request_list.empty());
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
  std::list<std::pair<int, const ResourceRequest *>> required_resources_list_copy;
  int index = 0;
  for (const auto &resource_request : sorted_resource_request_list) {
    required_resources_list_copy.emplace_back(index++, resource_request);
  }

  while (!required_resources_list_copy.empty()) {
    const auto &required_resources_index = required_resources_list_copy.front().first;
    const auto &required_resources = required_resources_list_copy.front().second;
    auto best_node_id = GetBestNode(*required_resources, candidate_nodes, options);
    if (best_node_id.IsNil()) {
      // There is no node to meet the scheduling requirements.
      break;
    }

    RAY_CHECK(cluster_resource_manager_.SubtractNodeAvailableResources(
        best_node_id, *required_resources));
    result_nodes[required_resources_index] = best_node_id;
    required_resources_list_copy.pop_front();

    // We try to schedule more resources on one node.
    for (auto iter = required_resources_list_copy.begin();
         iter != required_resources_list_copy.end();) {
      // If the node has sufficient resources, allocate it.
      if (cluster_resource_manager_.HasAvailableResources(
              best_node_id, *iter->second, false)) {
        RAY_CHECK(cluster_resource_manager_.SubtractNodeAvailableResources(
            best_node_id, *iter->second));
        result_nodes[iter->first] = best_node_id;
        required_resources_list_copy.erase(iter++);
      } else {
        // Otherwise try other node.
        ++iter;
      }
    }
    candidate_nodes.erase(best_node_id);
  }

  // Releasing the resources temporarily deducted from `cluster_resource_manager_`.
  for (size_t res_node_idx = 0; res_node_idx < result_nodes.size(); res_node_idx++) {
    // If `PackSchedule` fails, the id of some nodes may be nil.
    if (!result_nodes[res_node_idx].IsNil()) {
      RAY_CHECK(cluster_resource_manager_.AddNodeAvailableResources(
          result_nodes[res_node_idx],
          (*sorted_resource_request_list[res_node_idx]).GetResourceSet()));
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
    SchedulingOptions options,
    absl::flat_hash_set<scheduling::NodeID> candidate_nodes) {
  RAY_CHECK(!resource_request_list.empty());
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
  absl::flat_hash_set<scheduling::NodeID> selected_nodes;
  for (const auto &resource_request : sorted_resource_request_list) {
    // Score and sort nodes.
    auto best_node_id = GetBestNode(*resource_request, candidate_nodes, options);

    // There are nodes to meet the scheduling requirements.
    if (!best_node_id.IsNil()) {
      result_nodes.emplace_back(best_node_id);
      RAY_CHECK(cluster_resource_manager_.SubtractNodeAvailableResources(
          best_node_id, *resource_request));
      candidate_nodes.erase(best_node_id);
      selected_nodes.insert(best_node_id);
    } else {
      // Scheduling from selected nodes.
      best_node_id = GetBestNode(*resource_request, selected_nodes, options);
      if (!best_node_id.IsNil()) {
        result_nodes.emplace_back(best_node_id);
        RAY_CHECK(cluster_resource_manager_.SubtractNodeAvailableResources(
            best_node_id, *resource_request));
      } else {
        break;
      }
    }
  }

  // Releasing the resources temporarily deducted from `cluster_resource_manager_`.
  for (size_t index = 0; index < result_nodes.size(); index++) {
    // If `PackSchedule` fails, the id of some nodes may be nil.
    if (!result_nodes[index].IsNil()) {
      RAY_CHECK(cluster_resource_manager_.AddNodeAvailableResources(
          result_nodes[index], (*sorted_resource_request_list[index]).GetResourceSet()));
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
    SchedulingOptions options,
    absl::flat_hash_set<scheduling::NodeID> candidate_nodes) {
  RAY_CHECK(!resource_request_list.empty());
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
    if (!cluster_resource_manager_.HasFeasibleResources(*it,
                                                        aggregated_resource_request)) {
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

  auto best_node_id = scheduling::NodeID::Nil();
  if (!options.bundle_strict_pack_soft_target_node_id_.IsNil()) {
    if (candidate_nodes.contains(options.bundle_strict_pack_soft_target_node_id_)) {
      best_node_id = GetBestNode(aggregated_resource_request,
                                 absl::flat_hash_set<scheduling::NodeID>{
                                     options.bundle_strict_pack_soft_target_node_id_},
                                 options);
    }
  }

  if (best_node_id.IsNil()) {
    best_node_id = GetBestNode(aggregated_resource_request, candidate_nodes, options);
  }

  // Select the node with the highest score.
  // `StrictPackSchedule` does not need to consider the scheduling context, because it
  // only schedules to a node and triggers rescheduling when node dead.
  std::vector<scheduling::NodeID> result_nodes;
  if (!best_node_id.IsNil()) {
    result_nodes.resize(resource_request_list.size(), best_node_id);
  }
  if (result_nodes.empty()) {
    // Can't meet the scheduling requirements temporarily.
    return SchedulingResult::Failed();
  }

  return SchedulingResult::Success(std::move(result_nodes));
}

/////////////////////  BundleStrictSpreadSchedulingPolicy  //////////////////////////
void BundleStrictSpreadSchedulingPolicy::ExcludeNodesAlreadyContainingBundles(
    absl::flat_hash_set<scheduling::NodeID> &candidate_nodes,
    const SchedulingContext *context) {
  const BundleSchedulingContext *bundle_scheduling_context =
      dynamic_cast<const BundleSchedulingContext *>(context);
  if (bundle_scheduling_context &&
      bundle_scheduling_context->bundle_locations_.has_value()) {
    const std::shared_ptr<BundleLocations> &bundle_locations =
        bundle_scheduling_context->bundle_locations_.value();
    if (bundle_locations != nullptr) {
      for (auto &bundle : *bundle_locations) {
        candidate_nodes.erase(scheduling::NodeID(bundle.second.first.Binary()));
      }
    }
  }
}

SchedulingResult BundleStrictSpreadSchedulingPolicy::Schedule(
    const std::vector<const ResourceRequest *> &resource_request_list,
    SchedulingOptions options,
    absl::flat_hash_set<scheduling::NodeID> candidate_nodes) {
  RAY_CHECK(!resource_request_list.empty());

  ExcludeNodesAlreadyContainingBundles(candidate_nodes,
                                       options.scheduling_context_.get());

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
    auto best_node_id = GetBestNode(*resource_request, candidate_nodes, options);

    // There are nodes to meet the scheduling requirements.
    if (!best_node_id.IsNil()) {
      candidate_nodes.erase(best_node_id);
      result_nodes.emplace_back(best_node_id);
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

SchedulingResult HierarchicalBundleSchedulingPolicy::Schedule(
    const std::vector<const ResourceRequest *> &resource_request_list,
    SchedulingOptions options,
    absl::flat_hash_set<scheduling::NodeID> candidate_nodes,
    NodeScheduleFn node_schedule_fn) {
  std::vector<std::vector<int>> group_indices = std::move(options.bundle_group_indices_);
  options.bundle_group_indices_.clear();

  std::vector<scheduling::NodeID> final_nodes(resource_request_list.size(),
                                              scheduling::NodeID::Nil());
  bool is_infeasible = false;

  absl::flat_hash_set<std::string> selected_topologies =
      options.previously_occupied_topologies_;

  for (const auto &indices : group_indices) {
    std::vector<const ResourceRequest *> sub_list;
    for (int idx : indices) {
      RAY_CHECK_GE(idx, 0);
      RAY_CHECK_LT(static_cast<size_t>(idx), resource_request_list.size());
      sub_list.push_back(resource_request_list[idx]);
    }

    absl::flat_hash_set<scheduling::NodeID> group_candidates = candidate_nodes;
    absl::flat_hash_set<scheduling::NodeID> preferred_candidates = candidate_nodes;
    bool use_preferred_candidates = false;

    const auto &target_domain = !options.target_topology_assignment_.first.empty()
                                    ? options.target_topology_assignment_
                                    : options.target_label_domain_;
    const std::string &label_key = target_domain.first;

    if (options.outer_strategy_ == rpc::PlacementStrategy::STRICT_SPREAD) {
      for (const auto &node : final_nodes) {
        if (!node.IsNil()) {
          group_candidates.erase(node);
        }
      }
      if (!label_key.empty()) {
        for (auto it = group_candidates.begin(); it != group_candidates.end();) {
          const auto &labels = cluster_resource_manager_.GetNodeLabels(*it);
          auto label_it = labels.find(label_key);
          if (label_it != labels.end() &&
              selected_topologies.contains(label_it->second)) {
            group_candidates.erase(it++);
          } else {
            ++it;
          }
        }
      }
    } else if (options.outer_strategy_ == rpc::PlacementStrategy::SPREAD) {
      use_preferred_candidates = true;
      for (const auto &node : final_nodes) {
        if (!node.IsNil()) {
          preferred_candidates.erase(node);
        }
      }
      if (!label_key.empty()) {
        for (auto it = preferred_candidates.begin(); it != preferred_candidates.end();) {
          const auto &labels = cluster_resource_manager_.GetNodeLabels(*it);
          auto label_it = labels.find(label_key);
          if (label_it != labels.end() &&
              selected_topologies.contains(label_it->second)) {
            preferred_candidates.erase(it++);
          } else {
            ++it;
          }
        }
      }
    } else if (options.outer_strategy_ == rpc::PlacementStrategy::PACK ||
               options.outer_strategy_ == rpc::PlacementStrategy::STRICT_PACK) {
      if (target_domain.second.has_value()) {
        const std::string &required_domain = *target_domain.second;
        for (auto it = group_candidates.begin(); it != group_candidates.end();) {
          const auto &labels = cluster_resource_manager_.GetNodeLabels(*it);
          auto label_it = labels.find(label_key);
          if (label_it == labels.end() || label_it->second != required_domain) {
            group_candidates.erase(it++);
          } else {
            ++it;
          }
        }
      }
    }

    SchedulingResult result = SchedulingResult::Failed();
    if (use_preferred_candidates && !preferred_candidates.empty()) {
      result =
          node_schedule_fn(sub_list,
                           options,
                           absl::flat_hash_set<scheduling::NodeID>(preferred_candidates));
    }
    if (!result.status.IsSuccess()) {
      if (!label_key.empty() && !target_domain.second.has_value()) {
        // Bucket candidates by their outer domain label
        absl::flat_hash_map<std::string, absl::flat_hash_set<scheduling::NodeID>>
            domain_buckets;
        for (const auto &node : group_candidates) {
          const auto &labels = cluster_resource_manager_.GetNodeLabels(node);
          auto it = labels.find(label_key);
          if (it != labels.end()) {
            domain_buckets[it->second].insert(node);
          }
        }
        // Attempt to schedule the entire group within a single outer domain bucket
        for (auto &[domain_val, bucket_candidates] : domain_buckets) {
          result = node_schedule_fn(sub_list, options, std::move(bucket_candidates));
          if (result.status.IsSuccess()) {
            break;
          }
        }
      } else {
        // Either no outer topology, or the domain is already pinned by a previous
        // group/rescheduling
        result = node_schedule_fn(sub_list, options, std::move(group_candidates));
      }
    }
    if (result.status.IsSuccess()) {
      for (size_t i = 0; i < indices.size(); i++) {
        final_nodes[indices[i]] = result.selected_nodes[i];
        RAY_CHECK(cluster_resource_manager_.SubtractNodeAvailableResources(
            final_nodes[indices[i]], *sub_list[i]));
      }
      const std::string &outer_label_key = label_key;
      std::optional<std::string> outer_domain;
      if (!outer_label_key.empty() && !result.selected_nodes.empty()) {
        const auto &labels =
            cluster_resource_manager_.GetNodeLabels(result.selected_nodes[0]);
        auto it = labels.find(outer_label_key);
        if (it != labels.end()) {
          outer_domain = it->second;
        }
      }

      if ((options.outer_strategy_ == rpc::PlacementStrategy::STRICT_SPREAD ||
           options.outer_strategy_ == rpc::PlacementStrategy::SPREAD) &&
          outer_domain.has_value()) {
        selected_topologies.insert(*outer_domain);
      } else if ((options.outer_strategy_ == rpc::PlacementStrategy::PACK ||
                  options.outer_strategy_ == rpc::PlacementStrategy::STRICT_PACK) &&
                 outer_domain.has_value()) {
        // For PACK and STRICT_PACK, all bundles should be in the same label domain.
        // Force subsequent groups into the domain selected by the first group.
        options.target_topology_assignment_.second = *outer_domain;
        options.target_label_domain_.second = *outer_domain;
      }
    } else {
      if (result.status.IsInfeasible()) {
        is_infeasible = true;
      }
      break;
    }
  }

  // Restore the temporarily subtracted resources.
  for (size_t i = 0; i < final_nodes.size(); i++) {
    if (!final_nodes[i].IsNil()) {
      RAY_CHECK(cluster_resource_manager_.AddNodeAvailableResources(
          final_nodes[i], resource_request_list[i]->GetResourceSet()));
    }
  }

  for (const auto &node : final_nodes) {
    if (node.IsNil()) {
      return is_infeasible ? SchedulingResult::Infeasible() : SchedulingResult::Failed();
    }
  }

  auto success_result = SchedulingResult::Success(std::move(final_nodes));
  const auto &target_domain = !options.target_topology_assignment_.first.empty()
                                  ? options.target_topology_assignment_
                                  : options.target_label_domain_;
  if ((options.outer_strategy_ == rpc::PlacementStrategy::PACK ||
       options.outer_strategy_ == rpc::PlacementStrategy::STRICT_PACK) &&
      target_domain.second.has_value()) {
    success_result.selected_topology_assignment =
        std::make_pair(target_domain.first, *target_domain.second);
  }
  return success_result;
}

}  // namespace raylet_scheduling_policy
}  // namespace ray
