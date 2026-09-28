// Copyright The Ray Authors.
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

#include <memory>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "ray/gcs/gcs_placement_group_scheduler.h"

namespace ray {
namespace gcs {

// Hand-written fake for GcsPlacementGroupSchedulerInterface. Records calls in
// public vectors and exposes settable return fields / hooks so tests can drive
// behavior.
class FakeGcsPlacementGroupSchedulerInterface
    : public GcsPlacementGroupSchedulerInterface {
 public:
  void ScheduleUnplacedBundles(const SchedulePgRequest &request) override {
    schedule_unplaced_bundles_calls.push_back(request);
  }

  absl::flat_hash_map<PlacementGroupID, std::vector<int64_t>> GetAndRemoveBundlesOnNode(
      const NodeID &node_id) override {
    get_and_remove_bundles_on_node_calls.push_back(node_id);
    return get_and_remove_bundles_on_node_return;
  }

  absl::flat_hash_map<PlacementGroupID, std::vector<int64_t>> GetBundlesOnNode(
      const NodeID &node_id) const override {
    return get_bundles_on_node_return;
  }

  void DestroyPlacementGroupBundleResourcesIfExists(
      const PlacementGroupID &placement_group_id) override {
    destroy_placement_group_bundle_resources_calls.push_back(placement_group_id);
  }

  void MarkScheduleCancelled(const PlacementGroupID &placement_group_id) override {
    mark_schedule_cancelled_calls.push_back(placement_group_id);
  }

  void ReleaseUnusedBundles(const absl::flat_hash_map<NodeID, std::vector<rpc::Bundle>>
                                &node_to_bundles) override {
    release_unused_bundles_calls.push_back(node_to_bundles);
  }

  void Initialize(
      const absl::flat_hash_map<PlacementGroupID,
                                std::vector<std::shared_ptr<BundleSpecification>>>
          &group_to_bundles,
      const std::vector<SchedulePgRequest> &prepared_pgs) override {
    initialize_group_to_bundles_calls.push_back(group_to_bundles);
    initialize_prepared_pgs_calls.push_back(prepared_pgs);
  }

  // Recorded calls.
  std::vector<SchedulePgRequest> schedule_unplaced_bundles_calls;
  std::vector<NodeID> get_and_remove_bundles_on_node_calls;
  std::vector<PlacementGroupID> destroy_placement_group_bundle_resources_calls;
  std::vector<PlacementGroupID> mark_schedule_cancelled_calls;
  std::vector<absl::flat_hash_map<NodeID, std::vector<rpc::Bundle>>>
      release_unused_bundles_calls;
  std::vector<absl::flat_hash_map<PlacementGroupID,
                                  std::vector<std::shared_ptr<BundleSpecification>>>>
      initialize_group_to_bundles_calls;
  std::vector<std::vector<SchedulePgRequest>> initialize_prepared_pgs_calls;

  // Settable return values.
  absl::flat_hash_map<PlacementGroupID, std::vector<int64_t>>
      get_and_remove_bundles_on_node_return;
  absl::flat_hash_map<PlacementGroupID, std::vector<int64_t>> get_bundles_on_node_return;
};

}  // namespace gcs
}  // namespace ray
