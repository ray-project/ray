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

#pragma once

#include <string>
#include <vector>

#include "ray/raylet/scheduling/policy/bundle_scheduling_policy.h"

namespace ray {
namespace raylet_scheduling_policy {

struct GpuDomainCandidate {
  std::string gpu_domain;
  absl::flat_hash_map<scheduling::NodeID, const Node *> candidate_nodes;
};

struct GpuDomainFilterResult {
  SchedulingResultStatus status;
  std::vector<GpuDomainCandidate> candidates;
};

/// Domain-level scheduling tier for GPU-domain-aware placement groups.
///
/// Groups candidate nodes by their `ray.io/gpu-domain` label and returns
/// all feasible domains as candidates.  Each candidate passes both an
/// individual-bundle feasibility check and an aggregate-resource check.
/// The composite policy then tries the node-level strategy on each
/// candidate domain until one succeeds.
class GpuDomainStrictPackSchedulingPolicy : public BundleSchedulingPolicy {
 public:
  static constexpr const char *kGpuDomainLabelKey = "ray.io/gpu-domain";

  using BundleSchedulingPolicy::BundleSchedulingPolicy;

  /// Returns candidate GPU domains whose nodes can feasibly host the bundles.
  ///
  /// \param resource_request_list The resource/label requirements for each bundle.
  /// \param options Contains target_gpu_domain_ for partial-failure
  ///   rescheduling (empty for fresh domain selection).
  /// \param candidate_nodes All available candidate nodes.
  /// \return A GpuDomainFilterResult with candidate domains that pass both
  ///   IsRequestFeasible and aggregate resource checks, or
  ///   a failure/infeasible status if none qualify.
  GpuDomainFilterResult FilterCandidateNodes(
      const std::vector<const ResourceRequest *> &resource_request_list,
      const SchedulingOptions &options,
      absl::flat_hash_map<scheduling::NodeID, const Node *> candidate_nodes);

 private:
  /// Checks that the aggregate resource demand of all bundles does not
  /// exceed the aggregate available resources across the given nodes.
  bool HasSufficientAggregateResources(
      const std::vector<const ResourceRequest *> &resource_request_list,
      const absl::flat_hash_map<scheduling::NodeID, const Node *> &candidate_nodes) const;
};

}  // namespace raylet_scheduling_policy
}  // namespace ray
