// Copyright 2026 The Ray Authors.
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

#include "ray/raylet/scheduling/policy/label_domain_bundle_scheduling_policy.h"

namespace ray {
namespace raylet_scheduling_policy {

bool LabelDomainSchedulingPolicyInterface::IsRequestFeasible(
    const std::vector<const ResourceRequest *> &resource_request_list,
    const absl::flat_hash_map<scheduling::NodeID, const Node *> &candidate_nodes) const {
  // Check that each bundle is individually feasible on at least one node.
  for (const ResourceRequest *const &request : resource_request_list) {
    bool bundle_feasible =
        std::any_of(candidate_nodes.begin(),
                    candidate_nodes.end(),
                    [&](const std::pair<const scheduling::NodeID, const Node *> &entry) {
                      return entry.second->GetLocalView().IsFeasible(*request);
                    });
    if (!bundle_feasible) {
      return false;
    }
  }

  // Check that aggregate demand does not exceed aggregate capacity.
  ResourceSet aggregate_demand;
  for (const ResourceRequest *request : resource_request_list) {
    aggregate_demand += request->GetResourceSet();
  }
  for (auto resource_id : aggregate_demand.ResourceIds()) {
    FixedPoint total_capacity;
    for (const auto &[node_id, node] : candidate_nodes) {
      total_capacity += node->GetLocalView().total.Get(resource_id);
    }
    if (total_capacity < aggregate_demand.Get(resource_id)) {
      return false;
    }
  }

  return true;
}

SchedulingResult LabelDomainStrictPackSchedulingPolicy::Schedule(
    const std::vector<const ResourceRequest *> &resource_request_list,
    const SchedulingOptions &options,
    absl::flat_hash_map<scheduling::NodeID, const Node *> candidate_nodes,
    NodeScheduleFn node_schedule_fn) {
  RAY_CHECK(!resource_request_list.empty());

  const std::string &label_key = options.target_label_domain_.first;
  RAY_CHECK(!label_key.empty());

  // If a target label domain value is specified (partial failure rescheduling),
  // prune to only nodes in that domain and use the node-level scheduling callback to
  // schedule the bundles.
  if (!options.target_label_domain_.second.empty()) {
    const std::string &target = options.target_label_domain_.second;
    for (auto it = candidate_nodes.begin(); it != candidate_nodes.end();) {
      const absl::flat_hash_map<std::string, std::string> &labels =
          it->second->GetLocalView().labels;
      auto label_it = labels.find(label_key);
      if (label_it == labels.end() || label_it->second != target) {
        candidate_nodes.erase(it++);
      } else {
        ++it;
      }
    }
    if (!IsRequestFeasible(resource_request_list, candidate_nodes)) {
      RAY_LOG(DEBUG) << "Target label domain '" << target
                     << "' has insufficient resources; infeasible.";
      return SchedulingResult::Infeasible();
    }
    SchedulingResult result =
        node_schedule_fn(resource_request_list, options, std::move(candidate_nodes));
    if (result.status.IsSuccess()) {
      result.selected_label_domain = std::make_pair(label_key, std::string(target));
    }
    return result;
  }

  // Group candidate nodes by their label domain.
  absl::flat_hash_map<std::string, absl::flat_hash_map<scheduling::NodeID, const Node *>>
      domain_groups;
  for (const auto &[node_id, node] : candidate_nodes) {
    const absl::flat_hash_map<std::string, std::string> &labels =
        node->GetLocalView().labels;
    auto it = labels.find(label_key);
    if (it != labels.end() && !it->second.empty()) {
      domain_groups[it->second].emplace(node_id, node);
    }
  }

  if (domain_groups.empty()) {
    RAY_LOG(DEBUG) << "No candidate nodes have a " << label_key
                   << " label; label domain scheduling infeasible.";
    return SchedulingResult::Infeasible();
  }

  // Try each feasible domain: call node-level scheduling and return on first success.
  bool all_infeasible = true;
  for (auto &[domain_id, domain_nodes] : domain_groups) {
    if (!IsRequestFeasible(resource_request_list, domain_nodes)) {
      continue;
    }
    SchedulingResult result =
        node_schedule_fn(resource_request_list, options, std::move(domain_nodes));
    if (result.status.IsSuccess()) {
      result.selected_label_domain = std::make_pair(label_key, std::move(domain_id));
      return result;
    }
    if (!result.status.IsInfeasible()) {
      all_infeasible = false;
    }
  }

  if (all_infeasible) {
    RAY_LOG(DEBUG) << "No label domain has sufficiental total resources; infeasible.";
    return SchedulingResult::Infeasible();
  } else {
    RAY_LOG(DEBUG) << "No label domain has sufficient available resources; failed";
    return SchedulingResult::Failed();
  }
}

}  // namespace raylet_scheduling_policy
}  // namespace ray
