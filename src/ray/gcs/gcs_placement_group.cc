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

#include "ray/common/constants.h"
#include "ray/common/scheduling/label_selector.h"

namespace ray {
namespace gcs {

void GcsPlacementGroup::UpdateState(
    rpc::PlacementGroupTableData::PlacementGroupState state) {
  if (state == rpc::PlacementGroupTableData::CREATED) {
    RAY_CHECK_EQ(placement_group_table_data_.state(),
                 rpc::PlacementGroupTableData::PREPARED);
    placement_group_table_data_.set_placement_group_final_bundle_placement_timestamp_ms(
        clock_.NowUnixMillis());

    double duration_ms =
        placement_group_table_data_
            .placement_group_final_bundle_placement_timestamp_ms() -
        placement_group_table_data_.placement_group_creation_timestamp_ms();
    scheduler_placement_time_ms_histogram_.Record(duration_ms,
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

std::optional<std::string> GcsPlacementGroup::GetLabelDomainKey() const {
  const std::string &key = placement_group_table_data_.label_domain_key();
  if (key.empty()) {
    return std::nullopt;
  }
  return key;
}

void GcsPlacementGroup::ComputeLabelDomainKey() {
  const auto &proto_bundles = placement_group_table_data_.bundles();
  if (proto_bundles.empty()) {
    return;
  }
  BundleSpecification first_bundle(proto_bundles.Get(0));
  const LabelSelector &label_selector =
      first_bundle.GetRequiredResources().GetLabelSelector();
  for (const LabelConstraint &constraint : label_selector.GetConstraints()) {
    if (constraint.GetLabelKey() == kLabelKeyNodeAcceleratorType &&
        constraint.GetOperator() == LabelSelectorOperator::LABEL_IN &&
        (constraint.GetLabelValues().contains(kGB300) ||
         constraint.GetLabelValues().contains(kGB200))) {
      placement_group_table_data_.set_label_domain_key(kGpuDomainLabelKey);
      return;
    }
  }
}

std::optional<std::string> GcsPlacementGroup::GetLabelDomainAssignment(
    const std::string &label_key) const {
  const auto &assignments = placement_group_table_data_.label_domain_assignments();
  auto it = assignments.find(label_key);
  if (it != assignments.end()) {
    return it->second;
  }
  return std::nullopt;
}

void GcsPlacementGroup::SetLabelDomainAssignment(const std::string &label_key,
                                                 const std::string &label_value) {
  (*placement_group_table_data_.mutable_label_domain_assignments())[label_key] =
      label_value;
}

void GcsPlacementGroup::ClearLabelDomainAssignments() {
  placement_group_table_data_.mutable_label_domain_assignments()->clear();
}

}  // namespace gcs
}  // namespace ray
