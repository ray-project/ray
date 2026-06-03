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

#include <memory>
#include <utility>
#include <vector>

#include "ray/common/id.h"
#include "ray/gcs/gcs_resource_manager_interface.h"
#include "src/ray/protobuf/gcs.pb.h"
#include "src/ray/protobuf/ray_syncer.pb.h"

namespace ray {
namespace gcs {

/// Test double for `GcsResourceManagerInterface`. Records every push so tests
/// can assert which updates were applied, without dragging in the real GCS
/// resource manager's `ClusterResourceManager` / `GcsNodeManager` /
/// `ray_syncer` graph.
class FakeGcsResourceManager : public GcsResourceManagerInterface {
 public:
  void UpdateFromResourceView(
      const NodeID &node_id,
      const rpc::syncer::ResourceViewSyncMessage &resource_view_sync_message) override {
    resource_view_updates_.emplace_back(node_id, resource_view_sync_message);
  }

  void UpdatePlacementGroupLoad(
      const std::shared_ptr<rpc::PlacementGroupLoad> placement_group_load) override {
    placement_group_load_ = placement_group_load;
  }

  /// Recorded (node_id, sync_message) tuples from UpdateFromResourceView, in
  /// call order.
  const std::vector<std::pair<NodeID, rpc::syncer::ResourceViewSyncMessage>>
      &resource_view_updates() const {
    return resource_view_updates_;
  }

  /// Latest PlacementGroupLoad pushed via UpdatePlacementGroupLoad.
  const std::shared_ptr<rpc::PlacementGroupLoad> &placement_group_load() const {
    return placement_group_load_;
  }

 private:
  std::vector<std::pair<NodeID, rpc::syncer::ResourceViewSyncMessage>>
      resource_view_updates_;
  std::shared_ptr<rpc::PlacementGroupLoad> placement_group_load_;
};

}  // namespace gcs
}  // namespace ray
