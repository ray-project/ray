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

namespace {

/// Return true if scheduling this bundle (with resource_request) will exceed the
/// max cpu fraction for placement groups. This is per node.
///
/// \param node_resources The resource of the current node.
/// \param bundle_resource_request The requested resources for the current bundle.
/// \param max_cpu_fraction_per_node Highest CPU fraction the bundles can take up.
/// \param available_cpus_before_curernt_pg_request Available CPUs on this node before
///   scheduling the current pg request. It is used to calculate how many CPUs are
///   allocated by the current bundles so far. It will help us figuring out
///   the total CPU allocation from the current bundles for this node.
bool AllocationWillExceedMaxCpuFraction(
    const ray::NodeResources &node_resources,
    const ray::ResourceRequest &bundle_resource_request,
    double max_cpu_fraction_per_node,
    double available_cpus_before_curernt_pg_request) {
  if (max_cpu_fraction_per_node == 1.0) {
    // Allocation will never exceed the threshold if the fraction == 1.0.
    return false;
  }

  auto cpu_id = ray::ResourceID::CPU();
  auto total_cpus = node_resources.total.Get(cpu_id).Double();

  // Calculate max_reservable_cpus
  auto max_reservable_cpus =
      max_cpu_fraction_per_node * node_resources.total.Get(cpu_id).Double();

  // If the max reservable cpu < 1, we allow at least 1 CPU.
  if (max_reservable_cpus < 1) {
    max_reservable_cpus = 1;
  }

  // We guarantee at least 1 CPU is excluded from the placement group
  // when max_cpu_fraction_per_node is specified.
  if (max_reservable_cpus > total_cpus - 1) {
    max_reservable_cpus = total_cpus - 1;
  }

  /*
    To calculate if allocating a new bundle will exceed the pg max_fraction,
    we need a sum of

    - CPUs used by placement groups before.
    - CPUs that will be allocated by the current pg request.
  */

  // Get the sum of all cpu allocated by placement group on this node.
  FixedPoint cpus_used_by_pg_before(0);
  for (const auto &resource_id : node_resources.total.ResourceIds()) {
    if (ray::GetOriginalResourceNameFromWildcardResource(resource_id.Binary()) == "CPU") {
      cpus_used_by_pg_before += node_resources.total.Get(resource_id);
    }
  }

  // Get the CPUs allocated by current pg request so far.
  // Note that when we schedule the current pg, we allocate resources
  // temporarily meaning `node_resources.available` will contain
  // available CPUs after allocating CPUs for the current pg request.
  auto cpus_allocated_by_current_pg_request =
      (available_cpus_before_curernt_pg_request -
       node_resources.available.Get(cpu_id).Double());

  auto cpus_to_allocate_by_current_pg_request =
      (cpus_allocated_by_current_pg_request +
       bundle_resource_request.Get(cpu_id).Double());

  auto cpus_used_by_pg_after =
      cpus_used_by_pg_before.Double() + cpus_to_allocate_by_current_pg_request;
  return cpus_used_by_pg_after > max_reservable_cpus;
}

}  // namespace

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

/// Return the map of node id -> available cpus before the current bundle scheduling.
/// It is used to calculate how many CPUs have been allocated for the current bundles.
const absl::flat_hash_map<scheduling::NodeID, double>
BundleSchedulingPolicy::GetAvailableCpusBeforeBundleScheduling() const {
  absl::flat_hash_map<scheduling::NodeID, double> result;
  for (const auto &entry : cluster_resource_manager_.GetResourceView()) {
    result.emplace(
        entry.first,
        entry.second.GetLocalView().available.Get(ray::ResourceID::CPU()).Double());
  }
  return result;
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
      if (!IsPredefinedResource(r)) {
        extra_resources_set.insert(r);
      }
    }
    for (const auto &r : b.ResourceIds()) {
      if (!IsPredefinedResource(r)) {
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
    const SchedulingOptions &options,
    const absl::flat_hash_map<scheduling::NodeID, double>
        &available_cpus_before_bundle_scheduling) const {
  double best_node_score = -1;
  auto best_node_id = scheduling::NodeID::Nil();
  const Node *best_node = nullptr;

  // Score the nodes.
  for (const auto &[node_id, node] : candidate_nodes) {
    const auto &node_resources = node->GetLocalView();
    if (AllocationWillExceedMaxCpuFraction(
            node_resources,
            required_resources,
            options.max_cpu_fraction_per_node,
            available_cpus_before_bundle_scheduling.at(node_id))) {
      continue;
    }

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

  auto candidate_nodes = SelectCandidateNodes(options.scheduling_context.get());
  if (candidate_nodes.empty()) {
    RAY_LOG(DEBUG) << "The candidate nodes is empty, return directly.";
    return SchedulingResult::Infeasible();
  }

  const auto available_cpus_before_bundle_scheduling =
      GetAvailableCpusBeforeBundleScheduling();

  // First schedule scarce resources (such as GPU) and large capacity resources to improve
  // the scheduling success rate.
  auto sorted_result = SortRequiredResources(resource_request_list);
  const auto &sorted_index = sorted_result.first;
  const auto &sorted_resource_request_list = sorted_result.second;

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
    auto best_node = GetBestNode(*required_resources,
                                 candidate_nodes,
                                 options,
                                 available_cpus_before_bundle_scheduling);
    if (best_node.first.IsNil()) {
      // There is no node to meet the scheduling requirements.
      break;
    }

    const auto &node_resources = best_node.second->GetLocalView();

    RAY_CHECK(cluster_resource_manager_.SubtractNodeAvailableResources(
        best_node.first, *required_resources));
    result_nodes[required_resources_index] = best_node.first;
    required_resources_list_copy.pop_front();

    // We try to schedule more resources on one node.
    for (auto iter = required_resources_list_copy.begin();
         iter != required_resources_list_copy.end();) {
      if (node_resources.IsAvailable(*iter->second)  // If the node has enough resources.
          && !AllocationWillExceedMaxCpuFraction(    // and allocating resources won't
                                                     // exceed max cpu fraction.
                 node_resources,
                 *iter->second,
                 options.max_cpu_fraction_per_node,
                 available_cpus_before_bundle_scheduling.at(best_node.first))) {
        // Then allocate it.
        RAY_CHECK(cluster_resource_manager_.SubtractNodeAvailableResources(
            best_node.first, *iter->second));
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
  for (size_t index = 0; index < result_nodes.size(); index++) {
    // If `PackSchedule` fails, the id of some nodes may be nil.
    if (!result_nodes[index].IsNil()) {
      RAY_CHECK(cluster_resource_manager_.AddNodeAvailableResources(
          result_nodes[index], *sorted_resource_request_list[index]));
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

  auto candidate_nodes = SelectCandidateNodes(options.scheduling_context.get());
  if (candidate_nodes.empty()) {
    RAY_LOG(DEBUG) << "The candidate nodes is empty, return directly.";
    return SchedulingResult::Infeasible();
  }

  const auto available_cpus_before_bundle_scheduling =
      GetAvailableCpusBeforeBundleScheduling();

  // First schedule scarce resources (such as GPU) and large capacity resources to improve
  // the scheduling success rate.
  auto sorted_result = SortRequiredResources(resource_request_list);
  const auto &sorted_index = sorted_result.first;
  const auto &sorted_resource_request_list = sorted_result.second;

  std::vector<scheduling::NodeID> result_nodes;
  absl::flat_hash_map<scheduling::NodeID, const Node *> selected_nodes;
  for (const auto &resource_request : sorted_resource_request_list) {
    // Score and sort nodes.
    auto best_node = GetBestNode(*resource_request,
                                 candidate_nodes,
                                 options,
                                 available_cpus_before_bundle_scheduling);

    // There are nodes to meet the scheduling requirements.
    if (!best_node.first.IsNil()) {
      result_nodes.emplace_back(best_node.first);
      RAY_CHECK(cluster_resource_manager_.SubtractNodeAvailableResources(
          best_node.first, *resource_request));
      candidate_nodes.erase(result_nodes.back());
      selected_nodes.emplace(best_node);
    } else {
      // Scheduling from selected nodes.
      auto best_node = GetBestNode(*resource_request,
                                   selected_nodes,
                                   options,
                                   available_cpus_before_bundle_scheduling);
      if (!best_node.first.IsNil()) {
        result_nodes.emplace_back(best_node.first);
        RAY_CHECK(cluster_resource_manager_.SubtractNodeAvailableResources(
            best_node.first, *resource_request));
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
          result_nodes[index], *sorted_resource_request_list[index]));
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

  auto candidate_nodes = SelectCandidateNodes(options.scheduling_context.get());
  if (candidate_nodes.empty()) {
    RAY_LOG(DEBUG) << "The candidate nodes is empty, return directly.";
    return SchedulingResult::Infeasible();
  }

  const auto available_cpus_before_bundle_scheduling =
      GetAvailableCpusBeforeBundleScheduling();

  // Aggregate required resources.
  ResourceRequest aggregated_resource_request;
  for (const auto &resource_request : resource_request_list) {
    for (auto &resource_id : resource_request->ResourceIds()) {
      auto value = aggregated_resource_request.Get(resource_id) +
                   resource_request->Get(resource_id);
      aggregated_resource_request.Set(resource_id, value);
    }
  }

  const auto &right_node_it = std::find_if(
      candidate_nodes.begin(),
      candidate_nodes.end(),
      [&aggregated_resource_request, &options, &available_cpus_before_bundle_scheduling](
          const auto &entry) {
        const auto &node_resources = entry.second->GetLocalView();
        auto allocatable =
            (node_resources.IsFeasible(
                 aggregated_resource_request)         // If the resource is available
             && !AllocationWillExceedMaxCpuFraction(  // and allocating resources won't
                                                      // exceed max cpu fraction.
                    node_resources,
                    aggregated_resource_request,
                    options.max_cpu_fraction_per_node,
                    available_cpus_before_bundle_scheduling.at(entry.first)));
        return allocatable;
      });

  if (right_node_it == candidate_nodes.end()) {
    RAY_LOG(DEBUG) << "The required resource is bigger than the maximum resource in the "
                      "whole cluster, schedule failed.";
    return SchedulingResult::Infeasible();
  }

  auto best_node = GetBestNode(aggregated_resource_request,
                               candidate_nodes,
                               options,
                               available_cpus_before_bundle_scheduling);

  // Select the node with the highest score.
  // `StrictPackSchedule` does not need to consider the scheduling context, because it
  // only schedules to a node and triggers rescheduling when node dead.
  std::vector<scheduling::NodeID> result_nodes;
  if (!best_node.first.IsNil()) {
    result_nodes.resize(resource_request_list.size(), best_node.first);
  }
  if (result_nodes.empty()) {
    // Can't meet the scheduling requirements temporarily.
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
  auto candidate_nodes = SelectCandidateNodes(options.scheduling_context.get());
  if (candidate_nodes.empty()) {
    RAY_LOG(DEBUG) << "The candidate nodes is empty, return directly.";
    return SchedulingResult::Infeasible();
  }

  const auto available_cpus_before_bundle_scheduling =
      GetAvailableCpusBeforeBundleScheduling();

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

  std::vector<scheduling::NodeID> result_nodes;
  for (const auto &resource_request : sorted_resource_request_list) {
    // Score and sort nodes.
    auto best_node = GetBestNode(*resource_request,
                                 candidate_nodes,
                                 options,
                                 available_cpus_before_bundle_scheduling);

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
