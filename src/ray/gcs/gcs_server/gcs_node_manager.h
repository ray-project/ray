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
  /// \param node_failure_detector_io_service The event loop of node failure detector.
  /// \param error_info_accessor The error info accessor, which is used to report error.
  /// \param gcs_pub_sub GCS message publisher.
  /// \param gcs_table_storage GCS table external storage accessor.
  /// when detecting the death of nodes.
  explicit GcsNodeManager(boost::asio::io_service &main_io_service,
                          boost::asio::io_service &node_failure_detector_io_service,
                          gcs::ErrorInfoAccessor &error_info_accessor,
                          std::shared_ptr<gcs::GcsPubSub> gcs_pub_sub,
                          std::shared_ptr<gcs::GcsTableStorage> gcs_table_storage);

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

  /// Handle heartbeat rpc come from raylet.
  void HandleReportHeartbeat(const rpc::ReportHeartbeatRequest &request,
                             rpc::ReportHeartbeatReply *reply,
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

  /// Handle setting internal config.
  void HandleSetInternalConfig(const rpc::SetInternalConfigRequest &request,
                               rpc::SetInternalConfigReply *reply,
                               rpc::SendReplyCallback send_reply_callback) override;

  /// Handle getting internal config.
  void HandleGetInternalConfig(const rpc::GetInternalConfigRequest &request,
                               rpc::GetInternalConfigReply *reply,
                               rpc::SendReplyCallback send_reply_callback) override;

  /// Add an alive node.
  ///
  /// \param node The info of the node to be added.
  void AddNode(std::shared_ptr<rpc::GcsNodeInfo> node);

  /// Remove from alive nodes.
  ///
  /// \param node_id The ID of the node to be removed.
  /// \param is_intended False if this is triggered by `node_failure_detector_`, else
  /// True.
  std::shared_ptr<rpc::GcsNodeInfo> RemoveNode(const ClientID &node_id,
                                               bool is_intended = false);

  /// Get alive node by ID.
  ///
  /// \param node_id The id of the node.
  /// \return the node if it is alive else return nullptr.
  std::shared_ptr<rpc::GcsNodeInfo> GetNode(const ClientID &node_id) const;

  /// Get all alive nodes.
  ///
  /// \return all alive nodes.
  const absl::flat_hash_map<ClientID, std::shared_ptr<rpc::GcsNodeInfo>>
      &GetAllAliveNodes() const {
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

  /// Load initial data from gcs storage to memory cache asynchronously.
  /// This should be called when GCS server restarts after a failure.
  ///
  /// \param done Callback that will be called when load is complete.
  void LoadInitialData(const EmptyCallback &done);

  /// Start node failure detector.
  void StartNodeFailureDetector();

 protected:
  class NodeFailureDetector {
   public:
    /// Create a NodeFailureDetector.
    ///
    /// \param io_service The event loop to run the monitor on.
    /// \param gcs_table_storage GCS table external storage accessor.
    /// \param gcs_pub_sub GCS message publisher.
    /// \param on_node_death_callback Callback that will be called when node death is
    /// detected.
    explicit NodeFailureDetector(
        boost::asio::io_service &io_service,
        std::shared_ptr<gcs::GcsTableStorage> gcs_table_storage,
        std::shared_ptr<gcs::GcsPubSub> gcs_pub_sub,
        std::function<void(const ClientID &)> on_node_death_callback);

    // Note: To avoid heartbeats being delayed by main thread, all public methods below
    // should be posted to its own IO service.

    /// Start failure detector.
    void Start();

    /// Register node to this detector.
    /// Only if the node has registered, its heartbeat data will be accepted.
    ///
    /// \param node_id ID of the node to be registered.
    void AddNode(const ClientID &node_id);

    /// Handle a heartbeat from a Raylet.
    ///
    /// \param node_id The client ID of the Raylet that sent the heartbeat.
    /// \param heartbeat_data The heartbeat sent by the client.
    void HandleHeartbeat(const ClientID &node_id,
                         const rpc::HeartbeatTableData &heartbeat_data);

   protected:
    /// A periodic timer that fires on every heartbeat period. Raylets that have
    /// not sent a heartbeat within the last num_heartbeats_timeout ticks will be
    /// marked as dead in the client table.
    void Tick();

    /// Check that if any raylet is inactive due to no heartbeat for a period of time.
    /// If found any, mark it as dead.
    void DetectDeadNodes();

    /// Send any buffered heartbeats as a single publish.
    void SendBatchedHeartbeat();

    /// Schedule another tick after a short time.
    void ScheduleTick();

   protected:
    /// Storage for GCS tables.
    std::shared_ptr<gcs::GcsTableStorage> gcs_table_storage_;
    /// The callback of node death.
    std::function<void(const ClientID &)> on_node_death_callback_;
    /// The number of heartbeats that can be missed before a node is removed.
    int64_t num_heartbeats_timeout_;
    // Only the changed part will be included in heartbeat if this is true.
    const bool light_heartbeat_enabled_;
    /// A timer that ticks every heartbeat_timeout_ms_ milliseconds.
    boost::asio::deadline_timer detect_timer_;
    /// For each Raylet that we receive a heartbeat from, the number of ticks
    /// that may pass before the Raylet will be declared dead.
    absl::flat_hash_map<ClientID, int64_t> heartbeats_;
    /// A buffer containing heartbeats received from node managers in the last tick.
    absl::flat_hash_map<ClientID, rpc::HeartbeatTableData> heartbeat_buffer_;
    /// A publisher for publishing gcs messages.
    std::shared_ptr<gcs::GcsPubSub> gcs_pub_sub_;
    /// Is the detect started.
    bool is_started_ = false;
  };

 private:
  /// Error info accessor.
  gcs::ErrorInfoAccessor &error_info_accessor_;
  /// The main event loop for node failure detector.
  boost::asio::io_service &main_io_service_;
  /// Detector to detect the failure of node.
  std::unique_ptr<NodeFailureDetector> node_failure_detector_;
  /// The event loop for node failure detector.
  boost::asio::io_service &node_failure_detector_service_;
  /// Alive nodes.
  absl::flat_hash_map<ClientID, std::shared_ptr<rpc::GcsNodeInfo>> alive_nodes_;
  /// Dead nodes.
  absl::flat_hash_map<ClientID, std::shared_ptr<rpc::GcsNodeInfo>> dead_nodes_;
  /// Cluster resources.
  absl::flat_hash_map<ClientID, rpc::ResourceMap> cluster_resources_;
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
};

}  // namespace gcs
}  // namespace ray
