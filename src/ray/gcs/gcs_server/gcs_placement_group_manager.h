// Copyright 2017 The Ray Authors.
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
#include <ray/common/id.h>
#include <ray/common/task/task_execution_spec.h>
#include <ray/common/task/task_spec.h>
#include <ray/protobuf/gcs_service.pb.h>
#include <ray/rpc/worker/core_worker_client.h>
#include <utility>

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "gcs_placement_group_scheduler.h"
#include "gcs_table_storage.h"
#include "ray/gcs/pubsub/gcs_pub_sub.h"

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
  explicit GcsPlacementGroup(rpc::PlacementGroupTableData placement_group_table_data)
      : placement_group_table_data_(std::move(placement_group_table_data)) {}

  /// Create a GcsPlacementGroup by CreatePlacementGroupRequest.
  ///
  /// \param request Contains the placement group creation task specification.
  explicit GcsPlacementGroup(const ray::rpc::CreatePlacementGroupRequest &request) {
    const auto &placement_group_spec = request.placement_group_spec();
    placement_group_table_data_.set_placement_group_id(
        placement_group_spec.placement_group_id());

    placement_group_table_data_.set_state(rpc::PlacementGroupTableData::PENDING);
    placement_group_table_data_.mutable_bundles()->CopyFrom(
        placement_group_spec.bundles());
    placement_group_table_data_.set_strategy(placement_group_spec.strategy());
  }

  /// Get the immutable PlacementGroupTableData of this placement group.
  const rpc::PlacementGroupTableData &GetPlacementGroupTableData();

  /// Update the state of this placement_group.
  void UpdateState(rpc::PlacementGroupTableData::PlacementGroupState state);
  /// Get the state of this gcs placement_group.
  rpc::PlacementGroupTableData::PlacementGroupState GetState() const;

  /// Get the id of this placement_group.
  PlacementGroupID GetPlacementGroupID() const;
  /// Get the name of this placement_group.
  std::string GetName() const;

  /// Get the bundles of this placement_group
  std::vector<std::shared_ptr<BundleSpecification>> GetBundles() const;

  /// Get the Strategy
  rpc::PlacementStrategy GetStrategy() const;

 private:
  /// The placement_group meta data which contains the task specification as well as the
  /// state of the gcs placement_group and so on (see gcs.proto).
  rpc::PlacementGroupTableData placement_group_table_data_;
};
}  // namespace gcs
}  // namespace ray
