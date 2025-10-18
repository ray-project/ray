// Copyright 2025 The Ray Authors.
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

#include "ray/gcs/gcs_placement_group.h"

#include <memory>
#include <string>
#include <vector>

namespace ray {
namespace gcs {

void GcsPlacementGroup::UpdateState(
    rpc::PlacementGroupTableData::PlacementGroupState state) {
  if (state == rpc::PlacementGroupTableData::CREATED) {
    RAY_CHECK_EQ(placement_group_table_data_.state(),
                 rpc::PlacementGroupTableData::PREPARED);
    placement_group_table_data_.set_placement_group_final_bundle_placement_timestamp_ms(
        current_sys_time_ms());

    double duration_s =
        (placement_group_table_data_
             .placement_group_final_bundle_placement_timestamp_ms() -
         placement_group_table_data_.placement_group_creation_timestamp_ms()) /
        1000;
    scheduler_placement_time_s_histogram_.Record(duration_s,
                                                 {{"WorkloadType", "PlacementGroup"}});
  }
  placement_group_table_data_.set_state(state);
  RefreshMetrics();
}

rpc::PlacementGroupTableData::PlacementGroupState GcsPlacementGroup::GetState() const {
  return placement_group_table_data_.state();
}

PlacementGroupID GcsPlacementGroup::GetPlacementGroupID() const {
  return PlacementGroupID::FromBinary(placement_group_table_data_.placement_group_id());
}

std::string GcsPlacementGroup::GetName() const {
  return placement_group_table_data_.name();
}

std::string GcsPlacementGroup::GetRayNamespace() const {
  return placement_group_table_data_.ray_namespace();
}

std::vector<std::shared_ptr<const BundleSpecification>> &GcsPlacementGroup::GetBundles()
    const {
  // Fill the cache if it wasn't.
  if (cached_bundle_specs_.empty()) {
    const auto &bundles = placement_group_table_data_.bundles();
    for (const auto &bundle : bundles) {
      cached_bundle_specs_.push_back(std::make_shared<const BundleSpecification>(bundle));
    }
  }
  return cached_bundle_specs_;
}

std::vector<std::shared_ptr<const BundleSpecification>>
GcsPlacementGroup::GetUnplacedBundles() const {
  const auto &bundle_specs = GetBundles();

  std::vector<std::shared_ptr<const BundleSpecification>> unplaced_bundles;
  for (const auto &bundle : bundle_specs) {
    if (bundle->NodeId().IsNil()) {
      unplaced_bundles.push_back(bundle);
    }
  }
  return unplaced_bundles;
}

bool GcsPlacementGroup::HasUnplacedBundles() const {
  return !GetUnplacedBundles().empty();
}

rpc::PlacementStrategy GcsPlacementGroup::GetStrategy() const {
  return placement_group_table_data_.strategy();
}

const rpc::PlacementGroupTableData &GcsPlacementGroup::GetPlacementGroupTableData()
    const {
  return placement_group_table_data_;
}

std::string GcsPlacementGroup::DebugString() const {
  std::stringstream stream;
  stream << "placement group id = " << GetPlacementGroupID() << ", name = " << GetName()
         << ", strategy = " << GetStrategy();
  return stream.str();
}

rpc::Bundle *GcsPlacementGroup::GetMutableBundle(int bundle_index) {
  // Invalidate the cache.
  cached_bundle_specs_.clear();
  return placement_group_table_data_.mutable_bundles(bundle_index);
}

const ActorID GcsPlacementGroup::GetCreatorActorId() const {
  return ActorID::FromBinary(placement_group_table_data_.creator_actor_id());
}

const JobID GcsPlacementGroup::GetCreatorJobId() const {
  return JobID::FromBinary(placement_group_table_data_.creator_job_id());
}

void GcsPlacementGroup::MarkCreatorJobDead() {
  placement_group_table_data_.set_creator_job_dead(true);
}

void GcsPlacementGroup::MarkCreatorActorDead() {
  placement_group_table_data_.set_creator_actor_dead(true);
}

bool GcsPlacementGroup::IsPlacementGroupLifetimeDone() const {
  return !IsDetached() && placement_group_table_data_.creator_job_dead() &&
         placement_group_table_data_.creator_actor_dead();
}

bool GcsPlacementGroup::IsDetached() const {
  return placement_group_table_data_.is_detached();
}

NodeID GcsPlacementGroup::GetSoftTargetNodeID() const {
  return NodeID::FromBinary(placement_group_table_data_.soft_target_node_id());
}

const rpc::PlacementGroupStats &GcsPlacementGroup::GetStats() const {
  return placement_group_table_data_.stats();
}

rpc::PlacementGroupStats *GcsPlacementGroup::GetMutableStats() {
  return placement_group_table_data_.mutable_stats();
}

}  // namespace gcs
}  // namespace ray
