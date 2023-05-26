// Copyright 2022 The Ray Authors.
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
#include "src/ray/gcs/gcs_server/gcs_node_manager.h"
#include "src/ray/gcs/gcs_server/gcs_resource_manager.h"
#include "src/ray/protobuf/gcs.pb.h"
#include "src/ray/raylet/scheduling/cluster_resource_manager.h"

namespace ray {
namespace gcs {

class GcsAutoscalerStateManager : public rpc::AutoscalerStateHandler {
 public:
  GcsAutoscalerStateManager(ClusterResourceManager &cluster_resource_manager,
                            GcsResourceManager &gcs_resource_manager,
                            GcsNodeManager &gcs_node_manager);

  void HandleGetClusterResourceState(
      rpc::autoscaler::GetClusterResourceStateRequest request,
      rpc::autoscaler::GetClusterResourceStateReply *reply,
      rpc::SendReplyCallback send_reply_callback) override;

  void HandleReportAutoscalingState(
      rpc::autoscaler::ReportAutoscalingStateRequest request,
      rpc::autoscaler::ReportAutoscalingStateReply *reply,
      rpc::SendReplyCallback send_reply_callback) override;

  void RecordMetrics() const { throw std::runtime_error("Unimplemented"); }

  std::string DebugString() const { throw std::runtime_error("Unimplemented"); }

 private:
  int64_t GetNextClusterResourceStateVersion() {
    return ++cluster_resource_state_version_;
  }

  /// \brief Get the current cluster resource state.
  /// \param reply The reply to be filled.
  ///
  /// See rpc::autoscaler::GetClusterResourceStateReply::node_states for more details.
  void GetNodeStates(rpc::autoscaler::GetClusterResourceStateReply *reply);

  /// \brief Get the resource requests state.
  /// \param reply The reply to be filled.
  ///
  /// See rpc::autoscaler::GetClusterResourceStateReply::pending_resource_requests for
  /// more details.
  void GetPendingResourceRequests(rpc::autoscaler::GetClusterResourceStateReply *reply);

  /// \brief Get the gang resource requests (e.g. from placement group) state.
  /// \param reply The reply to be filled.
  ///
  /// See rpc::autoscaler::GetClusterResourceStateReply::pending_gang_resource_requests
  /// for more
  void GetPendingGangResourceRequests(
      rpc::autoscaler::GetClusterResourceStateReply *reply);

  /// \brief Get the cluster resource constraints state.
  /// \param reply The reply to be filled.
  ///
  /// See rpc::autoscaler::GetClusterResourceStateReply::cluster_resource_constraints for
  /// more details. This is requested through autoscaler SDK for request_resources().
  void GetClusterResourceConstraints(
      rpc::autoscaler::GetClusterResourceStateReply *reply);

  /// Cluster resources manager that provides cluster resources information.
  ClusterResourceManager &cluster_resource_manager_;

  /// Gcs node manager that provides node status information.
  GcsNodeManager &gcs_node_manager_;

  /// GCS resource manager that provides resource demand/load information.
  GcsResourceManager &gcs_resource_manager_;

  // The default value of the last seen version for the request is 0, which indicates
  // no version has been seen. When node is added, the version will be incremented.
  // Therefore, a cluster in init state with at least 1 node, will have the version > 0.
  int64_t cluster_resource_state_version_ = 0;

  /// The last seen autoscaler state version. Use 0 as the default value to indicate
  /// no previous autoscaler state has been reported.
  int64_t last_seen_autoscaler_state_version_ = 0;
};

}  // namespace gcs
}  // namespace ray
