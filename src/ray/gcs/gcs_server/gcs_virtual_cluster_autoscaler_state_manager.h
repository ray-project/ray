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

#include "ray/gcs/gcs_server/gcs_autoscaler_state_manager.h"
#include "ray/gcs/gcs_server/gcs_virtual_cluster_manager.h"

namespace ray {
namespace gcs {

class GcsVirtualClusterAutoscalerStateManager : public GcsAutoscalerStateManager {
 public:
  GcsVirtualClusterAutoscalerStateManager(
      std::string session_name,
      GcsNodeManager &gcs_node_manager,
      GcsActorManager &gcs_actor_manager,
      const GcsPlacementGroupManager &gcs_placement_group_manager,
      rpc::NodeManagerClientPool &raylet_client_pool,
      InternalKVInterface &kv,
      instrumented_io_context &io_context,
      std::shared_ptr<GcsVirtualClusterManager> gcs_virtual_cluster_manager);

  void HandleGetClusterResourceState(
      rpc::autoscaler::GetClusterResourceStateRequest request,
      rpc::autoscaler::GetClusterResourceStateReply *reply,
      rpc::SendReplyCallback send_reply_callback) override;

 private:
  /// \brief Internal method for populating the rpc::ClusterResourceState
  /// protobuf. It is only called when virtual clusters exist.
  /// \param state The state to be filled.
  void MakeVirtualClusterResourceStatesInternal(
      rpc::autoscaler::ClusterResourceState *state);

  /// \brief Get the resource requests state of a specified virtual cluster.
  /// \param state The virtul cluster state to be filled.
  void GetVirtualClusterPendingResourceRequests(
      rpc::autoscaler::VirtualClusterState *state);

  /// \brief Get the gang resource requests (e.g. from placement group) state for each
  /// virtual cluster. \param state The cluster resource state (including member field for
  /// virtual clusters) to be filled.
  void GetVirtualClusterPendingGangResourceRequests(
      rpc::autoscaler::ClusterResourceState *state);

  std::shared_ptr<GcsVirtualClusterManager> gcs_virtual_cluster_manager_;
};

}  // namespace gcs
}  // namespace ray
