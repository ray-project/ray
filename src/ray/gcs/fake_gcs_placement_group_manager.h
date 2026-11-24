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
#include "ray/asio/instrumented_io_context.h"
#include "ray/gcs/gcs_placement_group_manager.h"
#include "ray/observability/metric_interface.h"
#include "ray/util/clock.h"

namespace ray {
namespace gcs {

// Owns the Clock and io_context that back GcsPlacementGroupManager. Inherited
// privately and first by the fake so that these are constructed before the
// GcsPlacementGroupManager base (base-from-member idiom); a plain member would be
// constructed after the base, which reads them.
struct FakeGcsPlacementGroupManagerDeps {
  Clock clock;
  instrumented_io_context context;
};

// Hand-written fake for GcsPlacementGroupManager. Uses the protected
// testing-only base constructor. Overrides the RPC handlers with no-op recording
// bodies and exposes settable return fields for GetBundlesOnNode /
// GetPlacementGroupLoad.
class FakeGcsPlacementGroupManager : private FakeGcsPlacementGroupManagerDeps,
                                     public GcsPlacementGroupManager {
 public:
  explicit FakeGcsPlacementGroupManager(
      GcsResourceManager &gcs_resource_manager,
      ray::observability::MetricInterface &placement_group_gauge,
      ray::observability::MetricInterface
          &placement_group_creation_latency_in_ms_histogram,
      ray::observability::MetricInterface
          &placement_group_scheduling_latency_in_ms_histogram,
      ray::observability::MetricInterface &placement_group_count_gauge)
      : GcsPlacementGroupManager(context,
                                 gcs_resource_manager,
                                 placement_group_gauge,
                                 placement_group_creation_latency_in_ms_histogram,
                                 placement_group_scheduling_latency_in_ms_histogram,
                                 placement_group_count_gauge,
                                 clock) {}

  void HandleCreatePlacementGroup(rpc::CreatePlacementGroupRequest request,
                                  rpc::CreatePlacementGroupReply *reply,
                                  rpc::SendReplyCallback send_reply_callback) override {}
  void HandleRemovePlacementGroup(rpc::RemovePlacementGroupRequest request,
                                  rpc::RemovePlacementGroupReply *reply,
                                  rpc::SendReplyCallback send_reply_callback) override {}
  void HandleGetPlacementGroup(rpc::GetPlacementGroupRequest request,
                               rpc::GetPlacementGroupReply *reply,
                               rpc::SendReplyCallback send_reply_callback) override {}
  void HandleGetNamedPlacementGroup(rpc::GetNamedPlacementGroupRequest request,
                                    rpc::GetNamedPlacementGroupReply *reply,
                                    rpc::SendReplyCallback send_reply_callback) override {
  }
  void HandleGetAllPlacementGroup(rpc::GetAllPlacementGroupRequest request,
                                  rpc::GetAllPlacementGroupReply *reply,
                                  rpc::SendReplyCallback send_reply_callback) override {}
  void HandleWaitPlacementGroupUntilReady(
      rpc::WaitPlacementGroupUntilReadyRequest request,
      rpc::WaitPlacementGroupUntilReadyReply *reply,
      rpc::SendReplyCallback send_reply_callback) override {}

  absl::flat_hash_map<PlacementGroupID, std::vector<int64_t>> GetBundlesOnNode(
      const NodeID &node_id) const override {
    return get_bundles_on_node_return;
  }

  std::shared_ptr<rpc::PlacementGroupLoad> GetPlacementGroupLoad() const override {
    return get_placement_group_load_return;
  }

  // Settable return values.
  absl::flat_hash_map<PlacementGroupID, std::vector<int64_t>> get_bundles_on_node_return;
  std::shared_ptr<rpc::PlacementGroupLoad> get_placement_group_load_return;
};

}  // namespace gcs
}  // namespace ray
