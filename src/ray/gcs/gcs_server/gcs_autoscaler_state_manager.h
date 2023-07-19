// Copyright 2023 The Ray Authors.
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

#include "ray/rpc/gcs_server/gcs_rpc_server.h"
#include "src/ray/protobuf/gcs.pb.h"

namespace ray {
class ClusterResourceManager;
namespace gcs {

class GcsResourceManager;
class GcsNodeManager;
class GcsPlacementGroupManager;

class GcsAutoscalerStateManager : public rpc::AutoscalerStateHandler {
 public:
  GcsAutoscalerStateManager(const std::string &session_name,
                            const ClusterResourceManager &cluster_resource_manager,
                            const GcsResourceManager &gcs_resource_manager,
                            const GcsNodeManager &gcs_node_manager,
                            const GcsPlacementGroupManager &gcs_placement_group_manager);

  void HandleGetClusterResourceState(
      rpc::autoscaler::GetClusterResourceStateRequest request,
      rpc::autoscaler::GetClusterResourceStateReply *reply,
      rpc::SendReplyCallback send_reply_callback) override;

  void HandleReportAutoscalingState(
      rpc::autoscaler::ReportAutoscalingStateRequest request,
      rpc::autoscaler::ReportAutoscalingStateReply *reply,
      rpc::SendReplyCallback send_reply_callback) override;

  void HandleRequestClusterResourceConstraint(
      rpc::autoscaler::RequestClusterResourceConstraintRequest request,
      rpc::autoscaler::RequestClusterResourceConstraintReply *reply,
      rpc::SendReplyCallback send_reply_callback) override;

  void HandleGetClusterStatus(rpc::autoscaler::GetClusterStatusRequest request,
                              rpc::autoscaler::GetClusterStatusReply *reply,
                              rpc::SendReplyCallback send_reply_callback) override;

  void RecordMetrics() const { throw std::runtime_error("Unimplemented"); }

  std::string DebugString() const { throw std::runtime_error("Unimplemented"); }

 private:
  /// \brief Internal method for populating the rpc::ClusterResourceState
  /// protobuf.
  /// \param state The state to be filled.
  void MakeClusterResourceStateInternal(rpc::autoscaler::ClusterResourceState *state);

  /// \brief Increment and get the next cluster resource state version.
  /// \return The incremented cluster resource state version.
  int64_t IncrementAndGetNextClusterResourceStateVersion() {
    return ++last_cluster_resource_state_version_;
  }

  /// \brief Get the current cluster resource state.
  /// \param reply The reply to be filled.
  ///
  /// See rpc::autoscaler::ClusterResourceState::node_states for more details.
  void GetNodeStates(rpc::autoscaler::ClusterResourceState *state);

  /// \brief Get the resource requests state.
  /// \param reply The reply to be filled.
  ///
  /// See rpc::autoscaler::ClusterResourceState::pending_resource_requests for
  /// more details.
  void GetPendingResourceRequests(rpc::autoscaler::ClusterResourceState *state);

  /// \brief Get the gang resource requests (e.g. from placement group) state.
  /// \param reply The reply to be filled.
  ///
  /// This method fills up the `pending_gang_resource_requests` field.
  /// The `pending_gang_resource_requests` field is a list of resource requests from
  /// placement groups, which should be fulfilled atomically (either all fulfilled or all
  /// failed). Each pending or rescheduling placement group should generate one
  /// GangResourceRequest.
  ///
  /// Scheduling STRICT_SPREAD PGs
  /// ===============================
  /// If a pending/rescheduling placement group is STRICT_SPREAD, then its resources
  /// requests should also have anti-affinity constraint attached to it.
  ///
  /// When a placement group is rescheduled due to node failures, some bundles might
  /// get unplaced. In this case, the request corresponding to the placement group will
  /// only include those unplaced bundles.
  ///
  /// See rpc::autoscaler::ClusterResourceState::pending_gang_resource_requests
  /// for more details.
  void GetPendingGangResourceRequests(rpc::autoscaler::ClusterResourceState *state);

  /// \brief Get the cluster resource constraints state.
  /// \param reply The reply to be filled.
  ///
  /// See rpc::autoscaler::ClusterResourceState::cluster_resource_constraints for
  /// more details. This is requested through autoscaler SDK for request_resources().
  void GetClusterResourceConstraints(rpc::autoscaler::ClusterResourceState *state);

  // Ray cluster session name.
  const std::string session_name_ = "";

  /// Cluster resources manager that provides cluster resources information.
  const ClusterResourceManager &cluster_resource_manager_;

  /// Gcs node manager that provides node status information.
  const GcsNodeManager &gcs_node_manager_;

  /// GCS resource manager that provides resource demand/load information.
  const GcsResourceManager &gcs_resource_manager_;

  /// GCS placement group manager reference.
  const GcsPlacementGroupManager &gcs_placement_group_manager_;

  // The default value of the last seen version for the request is 0, which indicates
  // no version has been reported. So the first reported version should be 1.
  // We currently provide two guarantees for this version:
  //    1. It will increase monotonically.
  //    2. If a state is updated, the version will be higher.
  // Ideally we would want to have a guarantee where consecutive versions will always
  // be different, but it's currently hard to do.
  // TODO(rickyx): https://github.com/ray-project/ray/issues/35873
  // We will need to make the version correct when GCS fails over.
  int64_t last_cluster_resource_state_version_ = 0;

  /// The last seen autoscaler state version. Use 0 as the default value to indicate
  /// no previous autoscaler state has been seen.
  int64_t last_seen_autoscaler_state_version_ = 0;

  /// The most recent cluster resource constraints requested.
  /// This is requested through autoscaler SDK from request_resources().
  absl::optional<rpc::ClusterResourceConstraint> cluster_resource_constraint_ =
      absl::nullopt;

  /// Cached autoscaling state.
  absl::optional<rpc::AutoscalingState> autoscaling_state_ = absl::nullopt;

  FRIEND_TEST(GcsAutoscalerStateManagerTest, TestReportAutoscalingState);
};

}  // namespace gcs
}  // namespace ray
