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

#include "ray/gcs/gcs_server/gcs_init_data.h"
#include "ray/gcs/gcs_server/gcs_kv_manager.h"
#include "ray/rpc/gcs_server/gcs_rpc_server.h"
#include "ray/rpc/node_manager/node_manager_client_pool.h"
#include "ray/util/thread_checker.h"
#include "src/ray/protobuf/gcs.pb.h"

namespace ray {
namespace gcs {

class GcsActorManager;
class GcsNodeManager;
class GcsPlacementGroupManager;
class GcsResourceManager;

class GcsAutoscalerStateManager : public rpc::autoscaler::AutoscalerStateHandler {
 public:
  GcsAutoscalerStateManager(std::string session_name,
                            GcsNodeManager &gcs_node_manager,
                            GcsActorManager &gcs_actor_manager,
                            const GcsPlacementGroupManager &gcs_placement_group_manager,
                            rpc::NodeManagerClientPool &raylet_client_pool,
                            InternalKVInterface &kv,
                            instrumented_io_context &io_context);

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

  void HandleDrainNode(rpc::autoscaler::DrainNodeRequest request,
                       rpc::autoscaler::DrainNodeReply *reply,
                       rpc::SendReplyCallback send_reply_callback) override;

  void HandleReportClusterConfig(rpc::autoscaler::ReportClusterConfigRequest request,
                                 rpc::autoscaler::ReportClusterConfigReply *reply,
                                 rpc::SendReplyCallback send_reply_callback) override;

  void UpdateResourceLoadAndUsage(rpc::ResourcesData data);

  void RecordMetrics() const { throw std::runtime_error("Unimplemented"); }

  std::string DebugString() const;

  void Initialize(const GcsInitData &gcs_init_data);

  void OnNodeAdd(const rpc::GcsNodeInfo &node);

  void OnNodeDead(const NodeID &node) { node_resource_info_.erase(node); }

  const absl::flat_hash_map<ray::NodeID, std::pair<absl::Time, rpc::ResourcesData>>
      &GetNodeResourceInfo() const {
    return node_resource_info_;
  }

 private:
  /// \brief Get the aggregated resource load from all nodes.
  absl::flat_hash_map<google::protobuf::Map<std::string, double>, rpc::ResourceDemand>
  GetAggregatedResourceLoad() const;

  /// \brief Internal method for populating the rpc::ClusterResourceState
  /// protobuf.
  /// \param state The state to be filled.
  void MakeClusterResourceStateInternal(rpc::autoscaler::ClusterResourceState *state);

  /// \brief Get the placement group load from GcsPlacementGroupManager
  ///
  /// \return The placement group load, nullptr if there is no placement group load.
  std::shared_ptr<rpc::PlacementGroupLoad> GetPlacementGroupLoad() const;

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

  /// \brief Get the autoscaler infeasible request resource shapes for each node.
  /// \return a map of node id to the corresponding infeasible resource requests shapes.
  ///
  /// The function takes the infeasible requests from `autoscaling_state_` and maps the
  /// corresponding resource shapes to the ResourceLoad of each node in
  /// `node_resource_info_` to get the infeasible requests per node.
  /// The resource shapes that meets the following criteria are added to the map:
  /// (1) It is in the `infeasible_resource_requests` of the `autoscaling_state_`.
  /// (2) The `num_infeasible_requests_queued` in the `ResourceDemand` of the shape in
  ///    corresponding entry in `node_resource_info_` is greater than 0.
  absl::flat_hash_map<ray::NodeID,
                      std::vector<google::protobuf::Map<std::string, double>>>
  GetPerNodeInfeasibleResourceRequests() const;

  /// \brief Cancel the tasks with autoscaler infeasible requests.
  /// TODO: Implement the function
  void CancelInfeasibleRequests() const;

  // Ray cluster session name.
  const std::string session_name_;

  /// Gcs node manager that provides node status information.
  GcsNodeManager &gcs_node_manager_;

  /// Gcs actor manager that provides actor information.
  GcsActorManager &gcs_actor_manager_;

  /// GCS placement group manager reference.
  const GcsPlacementGroupManager &gcs_placement_group_manager_;

  /// Raylet client pool.
  rpc::NodeManagerClientPool &raylet_client_pool_;

  // Handler for internal KV
  InternalKVInterface &kv_;
  instrumented_io_context &io_context_;

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
  std::optional<rpc::autoscaler::ClusterResourceConstraint> cluster_resource_constraint_ =
      std::nullopt;

  /// Cached autoscaling state.
  std::optional<rpc::autoscaler::AutoscalingState> autoscaling_state_ = std::nullopt;

  /// Resource load and usage of all nodes.
  /// Note: This is similar to the data structure in `gcs_resource_manager`
  /// but we update load and usage together.
  ///
  /// The absl::Time in the pair is the last time the item was updated.
  absl::flat_hash_map<ray::NodeID, std::pair<absl::Time, rpc::ResourcesData>>
      node_resource_info_;

  ThreadChecker thread_checker_;

  FRIEND_TEST(GcsAutoscalerStateManagerTest, TestReportAutoscalingState);
  FRIEND_TEST(GcsAutoscalerStateManagerTest,
              TestGetPerNodeInfeasibleResourceRequests_NoInfeasibleRequests);
  FRIEND_TEST(GcsAutoscalerStateManagerTest,
              TestGetPerNodeInfeasibleResourceRequests_WithInfeasibleRequests);
};

}  // namespace gcs
}  // namespace ray
