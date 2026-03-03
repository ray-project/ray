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

#pragma once
#include <gtest/gtest_prod.h>

#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "ray/common/bundle_spec.h"
#include "ray/common/id.h"
#include "ray/common/metrics.h"
#include "ray/util/counter_map.h"
#include "ray/util/time.h"
#include "src/ray/protobuf/gcs_service.pb.h"

namespace ray {
namespace gcs {

/// GcsPlacementGroup just wraps `PlacementGroupTableData` and provides some convenient
/// interfaces to access the fields inside `PlacementGroupTableData`. This class is not
/// thread-safe.
class GcsPlacementGroup {
 public:
  /// Create a GcsPlacementGroup by placement_group_table_data.
  ///
  /// \param placement_group_table_data Data of the placement_group (see gcs.proto).
  explicit GcsPlacementGroup(
      rpc::PlacementGroupTableData placement_group_table_data,
      std::shared_ptr<CounterMap<rpc::PlacementGroupTableData::PlacementGroupState>>
          counter)
      : placement_group_table_data_(std::move(placement_group_table_data)),
        counter_(counter) {
    SetupStates();
  }

  /// Create a GcsPlacementGroup by CreatePlacementGroupRequest.
  ///
  /// \param request Contains the placement group creation task specification.
  explicit GcsPlacementGroup(
      const ray::rpc::CreatePlacementGroupRequest &request,
      std::string ray_namespace,
      std::shared_ptr<CounterMap<rpc::PlacementGroupTableData::PlacementGroupState>>
          counter)
      : counter_(counter) {
    const auto &placement_group_spec = request.placement_group_spec();
    placement_group_table_data_.set_placement_group_id(
        placement_group_spec.placement_group_id());
    placement_group_table_data_.set_name(placement_group_spec.name());
    placement_group_table_data_.set_state(rpc::PlacementGroupTableData::PENDING);
    placement_group_table_data_.mutable_bundles()->CopyFrom(
        placement_group_spec.bundles());
    placement_group_table_data_.set_strategy(placement_group_spec.strategy());
    placement_group_table_data_.set_creator_job_id(placement_group_spec.creator_job_id());
    placement_group_table_data_.set_creator_actor_id(
        placement_group_spec.creator_actor_id());
    placement_group_table_data_.set_creator_job_dead(
        placement_group_spec.creator_job_dead());
    placement_group_table_data_.set_creator_actor_dead(
        placement_group_spec.creator_actor_dead());
    placement_group_table_data_.set_is_detached(placement_group_spec.is_detached());
    placement_group_table_data_.set_soft_target_node_id(
        placement_group_spec.soft_target_node_id());
    placement_group_table_data_.set_ray_namespace(ray_namespace);
    placement_group_table_data_.set_placement_group_creation_timestamp_ms(
        current_sys_time_ms());
    SetupStates();
  }

  ~GcsPlacementGroup() {
    if (last_metric_state_ &&
        last_metric_state_.value() != rpc::PlacementGroupTableData::REMOVED) {
      RAY_LOG(DEBUG) << "Decrementing state at "
                     << rpc::PlacementGroupTableData::PlacementGroupState_Name(
                            last_metric_state_.value());
      // Retain groups in the REMOVED state so we have a history of past groups.
      counter_->Decrement(last_metric_state_.value());
    }
  }

  /// Get the immutable PlacementGroupTableData of this placement group.
  const rpc::PlacementGroupTableData &GetPlacementGroupTableData() const;

  /// Get the mutable bundle of this placement group.
  rpc::Bundle *GetMutableBundle(int bundle_index);

  /// Update the state of this placement_group.
  void UpdateState(rpc::PlacementGroupTableData::PlacementGroupState state);

  /// Get the state of this gcs placement_group.
  rpc::PlacementGroupTableData::PlacementGroupState GetState() const;

  /// Get the id of this placement_group.
  PlacementGroupID GetPlacementGroupID() const;

  /// Get the name of this placement_group.
  std::string GetName() const;

  /// Get the name of this placement_group.
  std::string GetRayNamespace() const;

  /// Get the bundles of this placement_group (including unplaced).
  std::vector<std::shared_ptr<const BundleSpecification>> &GetBundles() const;

  /// Get the unplaced bundles of this placement group.
  std::vector<std::shared_ptr<const BundleSpecification>> GetUnplacedBundles() const;

  /// Check if there are unplaced bundles.
  bool HasUnplacedBundles() const;

  /// Get the Strategy
  rpc::PlacementStrategy GetStrategy() const;

  /// Get debug string for the placement group.
  std::string DebugString() const;

  /// Below fields are used for automatic cleanup of placement groups.

  /// Get the actor id that created the placement group.
  const ActorID GetCreatorActorId() const;

  /// Get the job id that created the placement group.
  const JobID GetCreatorJobId() const;

  /// Mark that the creator job of this placement group is dead.
  void MarkCreatorJobDead();

  /// Mark that the creator actor of this placement group is dead.
  void MarkCreatorActorDead();

  /// Return True if the placement group lifetime is done. False otherwise.
  bool IsPlacementGroupLifetimeDone() const;

  /// Returns whether or not this is a detached placement group.
  bool IsDetached() const;

  /// Return the target node ID where bundles of this placement group should be placed.
  /// Only works for STRICT_PACK placement group.
  NodeID GetSoftTargetNodeID() const;

  const rpc::PlacementGroupStats &GetStats() const;

  rpc::PlacementGroupStats *GetMutableStats();

 private:
  // XXX.
  FRIEND_TEST(GcsPlacementGroupManagerTest, TestPlacementGroupBundleCache);

  /// Setup states other than placement_group_table_data_.
  void SetupStates() {
    auto stats = placement_group_table_data_.mutable_stats();
    // The default value for the field is 0
    if (stats->creation_request_received_ns() == 0) {
      auto now = absl::GetCurrentTimeNanos();
      stats->set_creation_request_received_ns(now);
    }
    // The default value for the field is 0
    // Only set the state to the QUEUED when the state wasn't persisted before.
    if (stats->scheduling_state() == 0) {
      stats->set_scheduling_state(rpc::PlacementGroupStats::QUEUED);
    }
    RefreshMetrics();
  }

  /// Record metric updates if there have been any state changes.
  void RefreshMetrics() {
    auto cur_state = GetState();
    if (last_metric_state_) {
      RAY_LOG(DEBUG) << "Swapping state from "
                     << rpc::PlacementGroupTableData::PlacementGroupState_Name(
                            last_metric_state_.value())
                     << " to "
                     << rpc::PlacementGroupTableData::PlacementGroupState_Name(cur_state);
      counter_->Swap(last_metric_state_.value(), cur_state);
    } else {
      RAY_LOG(DEBUG) << "Incrementing state at "
                     << rpc::PlacementGroupTableData::PlacementGroupState_Name(cur_state);
      counter_->Increment(cur_state);
    }
    last_metric_state_ = cur_state;
  }

  /// The placement_group meta data which contains the task specification as well as the
  /// state of the gcs placement_group and so on (see gcs.proto).
  rpc::PlacementGroupTableData placement_group_table_data_;
  /// Creating bundle specification requires heavy computation because it needs to compute
  /// formatted strings for all resources (heavy string operations). To optimize the CPU
  /// usage, we cache bundle specs.
  mutable std::vector<std::shared_ptr<const BundleSpecification>> cached_bundle_specs_;

  /// Reference to the counter to use for placement group state metrics tracking.
  std::shared_ptr<CounterMap<rpc::PlacementGroupTableData::PlacementGroupState>> counter_;

  /// The last recorded metric state.
  std::optional<rpc::PlacementGroupTableData::PlacementGroupState> last_metric_state_;

  ray::stats::Histogram scheduler_placement_time_ms_histogram_{
      ray::GetSchedulerPlacementTimeMsHistogramMetric()};
};

}  // namespace gcs
}  // namespace ray
