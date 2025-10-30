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

#include <gtest/gtest_prod.h>

#include <memory>
#include <string>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/id.h"
#include "ray/gcs/gcs_init_data.h"
#include "ray/gcs/gcs_node_manager.h"
#include "ray/gcs/grpc_service_interfaces.h"
#include "ray/ray_syncer/ray_syncer.h"
#include "ray/raylet/scheduling/cluster_lease_manager.h"
#include "ray/raylet/scheduling/cluster_resource_manager.h"
#include "src/ray/protobuf/gcs.pb.h"
#include "src/ray/protobuf/ray_syncer.pb.h"

namespace ray {
namespace gcs {

/// Ideally, the logic related to resource calculation should be moved from
/// `gcs_resource_manager` to `cluster_resource_manager`, and all logic related to
/// resource modification should directly depend on `cluster_resource_manager`, while
/// `gcs_resource_manager` is still responsible for processing resource-related RPC
/// request. We will split several small PR to achieve this goal, so as to prevent one PR
/// from being too large to review.
///
/// 1). Remove `node_resource_usages_` related code as it could be calculated from
/// `cluster_resource_manager`
/// 2). Move all resource-write-related logic out from `gcs_resource_manager`
/// 3). Move `placement_group_load_` from `gcs_resource_manager` to
/// `placement_group_manager` and make `gcs_resource_manager` depend on
/// `placement_group_manager`

/// Gcs resource manager interface.
/// It is responsible for handing node resource related rpc requests and it is used for
/// actor and placement group scheduling. It obtains the available resources of nodes
/// through heartbeat reporting. Non-thread safe.
class GcsResourceManager : public rpc::NodeResourceInfoGcsServiceHandler,
                           public syncer::ReceiverInterface {
 public:
  /// Create a GcsResourceManager.
  explicit GcsResourceManager(
      instrumented_io_context &io_context,
      ClusterResourceManager &cluster_resource_manager,
      GcsNodeManager &gcs_node_manager,
      NodeID local_node_id,
      raylet::ClusterLeaseManager *cluster_lease_manager = nullptr);

  virtual ~GcsResourceManager() = default;

  /// Handle the resource update.
  void ConsumeSyncMessage(
      std::shared_ptr<const rpc::syncer::RaySyncMessage> message) override;

  /// Handle get available resources of all nodes.
  /// Autoscaler-specific RPC called from Python.
  void HandleGetAllAvailableResources(
      rpc::GetAllAvailableResourcesRequest request,
      rpc::GetAllAvailableResourcesReply *reply,
      rpc::SendReplyCallback send_reply_callback) override;

  /// Handle get total resources of all nodes.
  /// Autoscaler-specific RPC called from Python.
  void HandleGetAllTotalResources(rpc::GetAllTotalResourcesRequest request,
                                  rpc::GetAllTotalResourcesReply *reply,
                                  rpc::SendReplyCallback send_reply_callback) override;

  /// Handle get ids of draining nodes.
  /// Autoscaler-specific RPC called from Python.
  void HandleGetDrainingNodes(rpc::GetDrainingNodesRequest request,
                              rpc::GetDrainingNodesReply *reply,
                              rpc::SendReplyCallback send_reply_callback) override;

  /// Handle get all resource usage rpc request.
  /// Autoscaler-specific RPC called from Python.
  void HandleGetAllResourceUsage(rpc::GetAllResourceUsageRequest request,
                                 rpc::GetAllResourceUsageReply *reply,
                                 rpc::SendReplyCallback send_reply_callback) override;

  /// Handle a node registration.
  ///
  /// \param node The specified node to add.
  void OnNodeAdd(const rpc::GcsNodeInfo &node);

  /// Handle a node death.
  ///
  /// \param node_id The specified node id.
  void OnNodeDead(const NodeID &node_id);

  /// Initialize with the gcs tables data synchronously.
  /// This should be called when GCS server restarts after a failure.
  ///
  /// \param gcs_init_data.
  void Initialize(const GcsInitData &gcs_init_data);

  std::string ToString() const;

  std::string DebugString() const;

  /// Add resources changed listener.
  void AddResourcesChangedListener(std::function<void()> &&listener);

  // Update node normal task resources.
  void UpdateNodeNormalTaskResources(const NodeID &node_id,
                                     const rpc::ResourcesData &heartbeat);

  /// Update resource usage of given node.
  ///
  /// \param node_id Node id.
  /// \param resource_view_sync_message The resource usage of the node.
  void UpdateNodeResourceUsage(
      const NodeID &node_id,
      const syncer::ResourceViewSyncMessage &resource_view_sync_message);

  /// Process a new resource report from a node, independent of the rpc handler it came
  /// from.
  ///
  /// \param node_id Node id.
  /// \param resource_view_sync_message The resource usage of the node.
  void UpdateFromResourceView(
      const NodeID &node_id,
      const syncer::ResourceViewSyncMessage &resource_view_sync_message);

  /// Update the resource usage of a node from syncer COMMANDS
  ///
  /// This is currently used for setting cluster full of actors info from syncer.
  /// \param data The resource report.
  void UpdateClusterFullOfActorsDetected(const NodeID &node_id,
                                         bool cluster_full_of_actors_detected);

  /// Update the placement group load information so that it will be reported through
  /// heartbeat.
  ///
  /// \param placement_group_load placement group load protobuf.
  void UpdatePlacementGroupLoad(
      const std::shared_ptr<rpc::PlacementGroupLoad> placement_group_load);

  /// Update the resource loads.
  ///
  /// \param data The resource loads reported by raylet.
  void UpdateResourceLoads(const rpc::ResourcesData &data);

  /// Returns the mapping from node id to latest resource report.
  ///
  /// \returns The mapping from node id to latest resource report.
  const absl::flat_hash_map<NodeID, rpc::ResourcesData> &NodeResourceReportView() const;

  /// Get the placement group load info. This is used for autoscaler.
  const std::shared_ptr<rpc::PlacementGroupLoad> GetPlacementGroupLoad() const {
    if (placement_group_load_.has_value()) {
      return placement_group_load_.value();
    }
    return nullptr;
  }

 private:
  /// io context. This is to ensure thread safety. Ideally, all public
  /// funciton needs to post job to this io_context.
  instrumented_io_context &io_context_;

  /// Newest resource usage of all nodes.
  absl::flat_hash_map<NodeID, rpc::ResourcesData> node_resource_usages_;

  /// Placement group load information that is used for autoscaler.
  std::optional<std::shared_ptr<rpc::PlacementGroupLoad>> placement_group_load_;
  /// The resources changed listeners.
  std::vector<std::function<void()>> resources_changed_listeners_;

  /// Debug info.
  enum CountType {
    GET_ALL_AVAILABLE_RESOURCES_REQUEST = 1,
    REPORT_RESOURCE_USAGE_REQUEST = 2,
    GET_ALL_RESOURCE_USAGE_REQUEST = 3,
    GET_All_TOTAL_RESOURCES_REQUEST = 4,
    CountType_MAX = 5,
  };
  uint64_t counts_[CountType::CountType_MAX] = {0};

  ClusterResourceManager &cluster_resource_manager_;
  GcsNodeManager &gcs_node_manager_;
  NodeID local_node_id_;
  raylet::ClusterLeaseManager *cluster_lease_manager_;
  /// Num of alive nodes in the cluster.
  size_t num_alive_nodes_ = 0;
};

}  // namespace gcs
}  // namespace ray
