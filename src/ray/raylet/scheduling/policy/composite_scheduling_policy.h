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

#pragma once

#include <vector>

#include "ray/raylet/scheduling/cluster_resource_manager.h"
#include "ray/raylet/scheduling/policy/bundle_scheduling_policy.h"
#include "ray/raylet/scheduling/policy/hybrid_scheduling_policy.h"
#include "ray/raylet/scheduling/policy/random_scheduling_policy.h"
#include "ray/raylet/scheduling/policy/spread_scheduling_policy.h"

namespace ray {
namespace raylet_scheduling_policy {

/// A composite scheduling policy that routes the request to the underlining
/// scheduling_policy according to the scheduling_type.
class CompositeSchedulingPolicy : public ISchedulingPolicy {
 public:
  CompositeSchedulingPolicy(scheduling::NodeID local_node_id,
                            ClusterResourceManager &cluster_resource_manager,
                            std::function<bool(scheduling::NodeID)> is_node_available)
      : hybrid_policy_(
            local_node_id, cluster_resource_manager.GetResourceView(), is_node_available),
        random_policy_(
            local_node_id, cluster_resource_manager.GetResourceView(), is_node_available),
        spread_policy_(local_node_id,
                       cluster_resource_manager.GetResourceView(),
                       is_node_available) {}

  scheduling::NodeID Schedule(const ResourceRequest &resource_request,
                              SchedulingOptions options) override;

 private:
  HybridSchedulingPolicy hybrid_policy_;
  RandomSchedulingPolicy random_policy_;
  SpreadSchedulingPolicy spread_policy_;
};

/// A composite scheduling policy that routes the request to the underlining
/// bundle_scheduling_policy according to the scheduling_type.
class CompositeBundleSchedulingPolicy : public IBundleSchedulingPolicy {
 public:
  explicit CompositeBundleSchedulingPolicy(
      ClusterResourceManager &cluster_resource_manager,
      std::function<bool(scheduling::NodeID)> is_node_available)
      : bundle_pack_policy_(cluster_resource_manager, is_node_available),
        bundle_spread_policy_(cluster_resource_manager, is_node_available),
        bundle_strict_spread_policy_(cluster_resource_manager, is_node_available),
        bundle_strict_pack_policy_(cluster_resource_manager, is_node_available) {}

  SchedulingResult Schedule(
      const std::vector<const ResourceRequest *> &resource_request_list,
      SchedulingOptions options) override;

 private:
  BundlePackSchedulingPolicy bundle_pack_policy_;
  BundleSpreadSchedulingPolicy bundle_spread_policy_;
  BundleStrictSpreadSchedulingPolicy bundle_strict_spread_policy_;
  BundleStrictPackSchedulingPolicy bundle_strict_pack_policy_;
};

}  // namespace raylet_scheduling_policy
}  // namespace ray
