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

#include "ray/raylet/scheduling/policy/composite_scheduling_policy.h"

#include <functional>

namespace ray {

namespace raylet_scheduling_policy {

scheduling::NodeID CompositeSchedulingPolicy::Schedule(
    const ResourceRequest &resource_request, SchedulingOptions options) {
  switch (options.scheduling_type_) {
  case SchedulingType::SPREAD:
    return spread_policy_.Schedule(resource_request, options);
  case SchedulingType::RANDOM:
    return random_policy_.Schedule(resource_request, options);
  case SchedulingType::HYBRID:
    return hybrid_policy_.Schedule(resource_request, options);
  case SchedulingType::NODE_AFFINITY:
    return node_affinity_policy_.Schedule(resource_request, options);
  case SchedulingType::AFFINITY_WITH_BUNDLE:
    return affinity_with_bundle_policy_.Schedule(resource_request, options);
  case SchedulingType::NODE_LABEL:
    return node_label_scheduling_policy_.Schedule(resource_request, options);
  default:
    RAY_LOG(FATAL) << "Unsupported scheduling type: "
                   << static_cast<typename std::underlying_type<SchedulingType>::type>(
                          options.scheduling_type_);
  }
  UNREACHABLE;
}

SchedulingResult CompositeBundleSchedulingPolicy::Schedule(
    const std::vector<const ResourceRequest *> &resource_request_list,
    SchedulingOptions options,
    absl::flat_hash_map<scheduling::NodeID, const Node *> candidate_nodes) {
  if (options.gpu_domain_scheduling_strategy_ == GpuDomainSchedulingStrategy::NONE) {
    return ScheduleNodeLevel(resource_request_list, options, std::move(candidate_nodes));
  }

  // Tier 1: Get candidate GPU domains.
  auto filter_result =
      ScheduleGpuDomainLevel(resource_request_list, options, std::move(candidate_nodes));
  if (!filter_result.status.IsSuccess()) {
    SchedulingResult fail;
    fail.status = filter_result.status;
    return fail;
  }

  // Tier 2: Try the node-level policy on each candidate domain.
  for (auto &candidate : filter_result.candidates) {
    auto result = ScheduleNodeLevel(
        resource_request_list, options, std::move(candidate.candidate_nodes));
    if (result.status.IsSuccess()) {
      result.selected_gpu_domain = std::move(candidate.gpu_domain);
      return result;
    }
  }

  RAY_LOG(DEBUG) << "All candidate GPU domains exhausted; scheduling failed.";
  return SchedulingResult::Failed();
}

GpuDomainFilterResult CompositeBundleSchedulingPolicy::ScheduleGpuDomainLevel(
    const std::vector<const ResourceRequest *> &resource_request_list,
    const SchedulingOptions &options,
    absl::flat_hash_map<scheduling::NodeID, const Node *> candidate_nodes) {
  switch (options.gpu_domain_scheduling_strategy_) {
  case GpuDomainSchedulingStrategy::STRICT_PACK:
    return gpu_domain_strict_pack_policy_.FilterCandidateNodes(
        resource_request_list, options, std::move(candidate_nodes));
  default:
    RAY_LOG(FATAL) << "Unsupported GPU domain scheduling strategy: "
                   << static_cast<int>(options.gpu_domain_scheduling_strategy_);
  }
  UNREACHABLE;
}

SchedulingResult CompositeBundleSchedulingPolicy::ScheduleNodeLevel(
    const std::vector<const ResourceRequest *> &resource_request_list,
    SchedulingOptions options,
    absl::flat_hash_map<scheduling::NodeID, const Node *> candidate_nodes) {
  switch (options.scheduling_type_) {
  case SchedulingType::BUNDLE_PACK:
    return bundle_pack_policy_.Schedule(
        resource_request_list, options, std::move(candidate_nodes));
  case SchedulingType::BUNDLE_SPREAD:
    return bundle_spread_policy_.Schedule(
        resource_request_list, options, std::move(candidate_nodes));
  case SchedulingType::BUNDLE_STRICT_PACK:
    return bundle_strict_pack_policy_.Schedule(
        resource_request_list, options, std::move(candidate_nodes));
  case SchedulingType::BUNDLE_STRICT_SPREAD:
    return bundle_strict_spread_policy_.Schedule(
        resource_request_list, options, std::move(candidate_nodes));
  default:
    RAY_LOG(FATAL) << "Unsupported scheduling type: "
                   << static_cast<typename std::underlying_type<SchedulingType>::type>(
                          options.scheduling_type_);
  }
  UNREACHABLE;
}

}  // namespace raylet_scheduling_policy
}  // namespace ray
