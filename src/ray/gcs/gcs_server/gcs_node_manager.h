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

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "ray/common/id.h"
#include "ray/gcs/accessor.h"
#include "ray/gcs/gcs_server/gcs_init_data.h"
#include "ray/gcs/gcs_server/gcs_resource_manager.h"
#include "ray/gcs/gcs_server/gcs_table_storage.h"
#include "ray/gcs/pubsub/gcs_pub_sub.h"
#include "ray/rpc/client_call.h"
#include "ray/rpc/gcs_server/gcs_rpc_server.h"
#include "src/ray/protobuf/gcs.pb.h"

namespace ray {
namespace gcs {

/// GcsNodeManager is responsible for managing and monitoring nodes as well as handing
/// node and resource related rpc requests.
/// This class is not thread-safe.
class GcsNodeManager : public rpc::NodeInfoHandler {
 public:
  /// Create a GcsNodeManager.
  ///
  /// \param main_io_service The main event loop.
  /// \param gcs_pub_sub GCS message publisher.
  /// \param gcs_table_storage GCS table external storage accessor.
  /// \param gcs_resource_manager GCS resource manager.
  explicit GcsNodeManager(boost::asio::io_service &main_io_service,
                          std::shared_ptr<gcs::GcsPubSub> gcs_pub_sub,
                          std::shared_ptr<gcs::GcsTableStorage> gcs_table_storage,
                          std::shared_ptr<gcs::GcsResourceManager> gcs_resource_manager);

  /// Handle register rpc request come from raylet.
  void HandleRegisterNode(const rpc::RegisterNodeRequest &request,
                          rpc::RegisterNodeReply *reply,
                          rpc::SendReplyCallback send_reply_callback) override;

  /// Handle unregister rpc request come from raylet.
  void HandleUnregisterNode(const rpc::UnregisterNodeRequest &request,
                            rpc::UnregisterNodeReply *reply,
                            rpc::SendReplyCallback send_reply_callback) override;

  /// Handle get all node info rpc request.
  void HandleGetAllNodeInfo(const rpc::GetAllNodeInfoRequest &request,
                            rpc::GetAllNodeInfoReply *reply,
                            rpc::SendReplyCallback send_reply_callback) override;

  /// Handle report resource usage rpc come from raylet.
  void HandleReportResourceUsage(const rpc::ReportResourceUsageRequest &request,
                                 rpc::ReportResourceUsageReply *reply,
                                 rpc::SendReplyCallback send_reply_callback) override;

  /// Handle get all resource usage rpc request.
  void HandleGetAllResourceUsage(const rpc::GetAllResourceUsageRequest &request,
                                 rpc::GetAllResourceUsageReply *reply,
                                 rpc::SendReplyCallback send_reply_callback) override;

  /// Handle get resource rpc request.
  void HandleGetResources(const rpc::GetResourcesRequest &request,
                          rpc::GetResourcesReply *reply,
                          rpc::SendReplyCallback send_reply_callback) override;

  /// Handle update resource rpc request.
  void HandleUpdateResources(const rpc::UpdateResourcesRequest &request,
                             rpc::UpdateResourcesReply *reply,
                             rpc::SendReplyCallback send_reply_callback) override;

  /// Handle delete resource rpc request.
  void HandleDeleteResources(const rpc::DeleteResourcesRequest &request,
                             rpc::DeleteResourcesReply *reply,
                             rpc::SendReplyCallback send_reply_callback) override;

  /// Handle set internal config.
  void HandleSetInternalConfig(const rpc::SetInternalConfigRequest &request,
                               rpc::SetInternalConfigReply *reply,
                               rpc::SendReplyCallback send_reply_callback) override;

  /// Handle get internal config.
  void HandleGetInternalConfig(const rpc::GetInternalConfigRequest &request,
                               rpc::GetInternalConfigReply *reply,
                               rpc::SendReplyCallback send_reply_callback) override;

  /// Handle get available resources of all nodes.
  void HandleGetAllAvailableResources(
      const rpc::GetAllAvailableResourcesRequest &request,
      rpc::GetAllAvailableResourcesReply *reply,
      rpc::SendReplyCallback send_reply_callback) override;

  /// Update resource usage of given node.
  ///
  /// \param node_id Node id.
  /// \param request Request containing resource usage.
  void UpdateNodeResourceUsage(const NodeID node_id,
                               const rpc::ReportResourceUsageRequest &request);

  void OnNodeFailure(const NodeID &node_id);

  /// Add an alive node.
  ///
  /// \param node The info of the node to be added.
  void AddNode(std::shared_ptr<rpc::GcsNodeInfo> node);

  /// Remove from alive nodes.
  ///
  /// \param node_id The ID of the node to be removed.
  /// \param is_intended False if this is triggered by `node_failure_detector_`, else
  /// True.
  std::shared_ptr<rpc::GcsNodeInfo> RemoveNode(const NodeID &node_id,
                                               bool is_intended = false);

  /// Get alive node by ID.
  ///
  /// \param node_id The id of the node.
  /// \return the node if it is alive. Optional empty value if it is not alive.
  absl::optional<std::shared_ptr<rpc::GcsNodeInfo>> GetAliveNode(
      const NodeID &node_id) const;

  /// Get all alive nodes.
  ///
  /// \return all alive nodes.
  const absl::flat_hash_map<NodeID, std::shared_ptr<rpc::GcsNodeInfo>> &GetAllAliveNodes()
      const {
    return alive_nodes_;
  }

  /// Add listener to monitor the remove action of nodes.
  ///
  /// \param listener The handler which process the remove of nodes.
  void AddNodeRemovedListener(
      std::function<void(std::shared_ptr<rpc::GcsNodeInfo>)> listener) {
    RAY_CHECK(listener);
    node_removed_listeners_.emplace_back(std::move(listener));
  }

  /// Add listener to monitor the add action of nodes.
  ///
  /// \param listener The handler which process the add of nodes.
  void AddNodeAddedListener(
      std::function<void(std::shared_ptr<rpc::GcsNodeInfo>)> listener) {
    RAY_CHECK(listener);
    node_added_listeners_.emplace_back(std::move(listener));
  }

  /// Initialize with the gcs tables data synchronously.
  /// This should be called when GCS server restarts after a failure.
  ///
  /// \param gcs_init_data.
  void Initialize(const GcsInitData &gcs_init_data);

  // Update node realtime resources.
  void UpdateNodeRealtimeResources(const NodeID &node_id,
                                   const rpc::ResourcesData &heartbeat);

  /// Update the placement group load information so that it will be reported through
  /// heartbeat.
  ///
  /// \param placement_group_load placement group load protobuf.
  void UpdatePlacementGroupLoad(
      const std::shared_ptr<rpc::PlacementGroupLoad> placement_group_load);

  std::string DebugString() const;

 private:
  /// Add the dead node to the cache. If the cache is full, the earliest dead node is
  /// evicted.
  ///
  /// \param node The node which is dead.
  void AddDeadNodeToCache(std::shared_ptr<rpc::GcsNodeInfo> node);

  /// Send any buffered resource usage as a single publish.
  void SendBatchedResourceUsage();

  /// A timer that ticks every raylet_report_resources_period_milliseconds.
  boost::asio::deadline_timer resource_timer_;
  // Only the changed part will be reported if this is true.
  const bool light_report_resource_usage_enabled_;
  /// Alive nodes.
  absl::flat_hash_map<NodeID, std::shared_ptr<rpc::GcsNodeInfo>> alive_nodes_;
  /// Dead nodes.
  absl::flat_hash_map<NodeID, std::shared_ptr<rpc::GcsNodeInfo>> dead_nodes_;
  /// The nodes are sorted according to the timestamp, and the oldest is at the head of
  /// the list.
  std::list<std::pair<NodeID, int64_t>> sorted_dead_node_list_;
  /// Cluster resources.
  absl::flat_hash_map<NodeID, rpc::ResourceMap> cluster_resources_;
  /// Newest resource usage of all nodes.
  absl::flat_hash_map<NodeID, rpc::ResourcesData> node_resource_usages_;
  /// A buffer containing resource usage received from node managers in the last tick.
  absl::flat_hash_map<NodeID, rpc::ResourcesData> resources_buffer_;
  /// Listeners which monitors the addition of nodes.
  std::vector<std::function<void(std::shared_ptr<rpc::GcsNodeInfo>)>>
      node_added_listeners_;
  /// Listeners which monitors the removal of nodes.
  std::vector<std::function<void(std::shared_ptr<rpc::GcsNodeInfo>)>>
      node_removed_listeners_;
  /// A publisher for publishing gcs messages.
  std::shared_ptr<gcs::GcsPubSub> gcs_pub_sub_;
  /// Storage for GCS tables.
  std::shared_ptr<gcs::GcsTableStorage> gcs_table_storage_;
  /// Gcs resource manager.
  std::shared_ptr<gcs::GcsResourceManager> gcs_resource_manager_;
  /// Placement group load information that is used for autoscaler.
  absl::optional<std::shared_ptr<rpc::PlacementGroupLoad>> placement_group_load_;

  // Debug info.
  enum CountType {
    REGISTER_NODE_REQUEST = 0,
    UNREGISTER_NODE_REQUEST = 1,
    GET_ALL_NODE_INFO_REQUEST = 2,
    REPORT_RESOURCE_USAGE_REQUEST = 3,
    GET_HEARTBEAT_REQUEST = 4,
    GET_ALL_RESOURCE_USAGE_REQUEST = 5,
    GET_RESOURCES_REQUEST = 6,
    UPDATE_RESOURCES_REQUEST = 7,
    DELETE_RESOURCES_REQUEST = 8,
    SET_INTERNAL_CONFIG_REQUEST = 9,
    GET_INTERNAL_CONFIG_REQUEST = 10,
    GET_ALL_AVAILABLE_RESOURCES_REQUEST = 11,
    CountType_MAX = 12,
  };
  uint64_t counts_[CountType::CountType_MAX] = {0};
};

}  // namespace gcs
}  // namespace ray
