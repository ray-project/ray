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
  for (const auto &request : resource_request_list) {
    bool bundle_feasible = std::any_of(
        candidate_nodes.begin(), candidate_nodes.end(), [&](const auto &entry) {
          return entry.second->GetLocalView().IsFeasible(*request);
        });
    if (!bundle_feasible) {
      return false;
    }
  }

  // Check that aggregate demand does not exceed aggregate capacity.
  ResourceSet aggregate_demand;
  for (const auto *request : resource_request_list) {
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

LabelDomainFilterResult LabelDomainStrictPackSchedulingPolicy::FilterCandidateNodes(
    const std::vector<const ResourceRequest *> &resource_request_list,
    const SchedulingOptions &options,
    absl::flat_hash_map<scheduling::NodeID, const Node *> candidate_nodes) {
  RAY_CHECK(!resource_request_list.empty());

  const auto &label_key = options.target_label_domain_.first;
  RAY_CHECK(!label_key.empty());

  // If a target label domain value is specified (partial failure rescheduling),
  // prune to only nodes in that domain and return as the sole candidate.
  if (!options.target_label_domain_.second.empty()) {
    const auto &target = options.target_label_domain_.second;
    for (auto it = candidate_nodes.begin(); it != candidate_nodes.end();) {
      const auto &labels = it->second->GetLocalView().labels;
      auto label_it = labels.find(label_key);
      if (label_it == labels.end() || label_it->second != target) {
        candidate_nodes.erase(it++);
      } else {
        ++it;
      }
    }
    LabelDomainFilterResult result;
    if (!IsRequestFeasible(resource_request_list, candidate_nodes)) {
      RAY_LOG(DEBUG) << "Target label domain '" << target
                     << "' has insufficient resources; infeasible.";
      result.status.code = SchedulingResultStatus::SchedulingResultStatusCode::INFEASIBLE;
    } else {
      result.status.code = SchedulingResultStatus::SchedulingResultStatusCode::SUCCESS;
      result.candidates.push_back(
          LabelDomainCandidate{target, std::move(candidate_nodes)});
    }
    return result;
  }

  // Group candidate nodes by their label domain.
  absl::flat_hash_map<std::string, absl::flat_hash_map<scheduling::NodeID, const Node *>>
      domain_groups;
  for (const auto &[node_id, node] : candidate_nodes) {
    const auto &labels = node->GetLocalView().labels;
    auto it = labels.find(label_key);
    if (it != labels.end() && !it->second.empty()) {
      domain_groups[it->second].emplace(node_id, node);
    }
  }

  if (domain_groups.empty()) {
    RAY_LOG(DEBUG) << "No candidate nodes have a " << label_key
                   << " label; label domain scheduling infeasible.";
    LabelDomainFilterResult result;
    result.status.code = SchedulingResultStatus::SchedulingResultStatusCode::INFEASIBLE;
    return result;
  }

  // Collect all domains that pass both feasibility checks.
  LabelDomainFilterResult result;
  for (auto &[domain_id, domain_nodes] : domain_groups) {
    if (IsRequestFeasible(resource_request_list, domain_nodes)) {
      result.candidates.push_back(
          LabelDomainCandidate{domain_id, std::move(domain_nodes)});
    }
  }

  if (result.candidates.empty()) {
    RAY_LOG(DEBUG) << "No label domain has sufficient total resources; infeasible.";
    result.status.code = SchedulingResultStatus::SchedulingResultStatusCode::INFEASIBLE;
  } else {
    result.status.code = SchedulingResultStatus::SchedulingResultStatusCode::SUCCESS;
  }
  return result;
}

}  // namespace raylet_scheduling_policy
}  // namespace ray
