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

#include <deque>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/id.h"
#include "ray/gcs/gcs_init_data.h"
#include "ray/gcs/gcs_table_storage.h"
#include "ray/gcs/grpc_service_interfaces.h"
#include "ray/observability/ray_event_recorder_interface.h"
#include "ray/pubsub/gcs_publisher.h"
#include "ray/raylet_rpc_client/raylet_client_pool.h"
#include "ray/stats/metric_defs.h"
#include "ray/util/event.h"
#include "src/ray/protobuf/autoscaler.pb.h"
#include "src/ray/protobuf/gcs.pb.h"
#include "src/ray/protobuf/ray_syncer.pb.h"

namespace ray {
namespace gcs {

class GcsAutoscalerStateManagerTest;
class GcsStateTest;

/// GcsNodeManager is responsible for managing and monitoring nodes as well as handing
/// node and resource related rpc requests.
/// This class is not thread-safe.
class GcsNodeManager : public rpc::NodeInfoGcsServiceHandler {
 public:
  /// Create a GcsNodeManager.
  ///
  /// \param gcs_publisher GCS message publisher.
  /// \param gcs_table_storage GCS table external storage accessor.
  GcsNodeManager(pubsub::GcsPublisher *gcs_publisher,
                 GcsTableStorage *gcs_table_storage,
                 instrumented_io_context &io_context,
                 rpc::RayletClientPool *raylet_client_pool,
                 const ClusterID &cluster_id,
                 observability::RayEventRecorderInterface &ray_event_recorder,
                 const std::string &session_name);

  /// Handle register rpc request come from raylet.
  void HandleGetClusterId(rpc::GetClusterIdRequest request,
                          rpc::GetClusterIdReply *reply,
                          rpc::SendReplyCallback send_reply_callback) override;

  /// Handle register rpc request come from raylet.
  void HandleRegisterNode(rpc::RegisterNodeRequest request,
                          rpc::RegisterNodeReply *reply,
                          rpc::SendReplyCallback send_reply_callback) override;

  /// Handle unregister rpc request come from raylet.
  void HandleUnregisterNode(rpc::UnregisterNodeRequest request,
                            rpc::UnregisterNodeReply *reply,
                            rpc::SendReplyCallback send_reply_callback) override;

  /// TODO(#56627): This method is only called by autoscaler v1. It will be deleted
  /// once autoscaler v1 is fully deprecated. Autoscaler v2 calls
  /// GcsAutoscalerStateManager::HandleDrainNode.
  void HandleDrainNode(rpc::DrainNodeRequest request,
                       rpc::DrainNodeReply *reply,
                       rpc::SendReplyCallback send_reply_callback) override;

  /// Handle get all node info rpc request.
  void HandleGetAllNodeInfo(rpc::GetAllNodeInfoRequest request,
                            rpc::GetAllNodeInfoReply *reply,
                            rpc::SendReplyCallback send_reply_callback) override;

  /// Handle check alive request for GCS.
  void HandleCheckAlive(rpc::CheckAliveRequest request,
                        rpc::CheckAliveReply *reply,
                        rpc::SendReplyCallback send_reply_callback) override;

  /// Handle a node failure. This will mark the failed node as dead in gcs
  /// node table.
  ///
  /// \param node_id The ID of the failed node.
  /// \param node_table_updated_callback The status callback function after
  /// failed node info is updated to gcs node table.
  void OnNodeFailure(const NodeID &node_id,
                     const std::function<void()> &node_table_updated_callback);

  /// Set the node to be draining.
  ///
  /// \param node_id The ID of the draining node. This node must already
  /// be in the alive nodes.
  /// \param request The drain node request.
  void SetNodeDraining(const NodeID &node_id,
                       std::shared_ptr<rpc::autoscaler::DrainNodeRequest> request);

  /// Get alive node by ID.
  ///
  /// \param node_id The id of the node.
  /// \return the node if it is alive. Optional empty value if it is not alive.
  std::optional<std::shared_ptr<const rpc::GcsNodeInfo>> GetAliveNode(
      const NodeID &node_id) const;

  /// Check if node is dead by ID where dead means that it's still in the dead node list
  /// N.B. this method may return false when the nodes isn't included in the dead node
  /// cache.
  ///
  /// \param node_id The id of the node.
  /// \return If the node is known to be dead
  bool IsNodeDead(const ray::NodeID &node_id) const;

  /// Check if node is alive by ID.
  ///
  /// \param node_id The id of the node.
  /// \return If the node is known to be dead
  bool IsNodeAlive(const ray::NodeID &node_id) const;

  /// Get all alive nodes.
  ///
  /// \return all alive nodes. Returns a copy of the map for thread safety
  absl::flat_hash_map<NodeID, std::shared_ptr<const rpc::GcsNodeInfo>> GetAllAliveNodes()
      const {
    absl::ReaderMutexLock lock(&mutex_);
    return alive_nodes_;
  }

  /// Selects a random node from the list of alive nodes
  ///
  /// \returns a random node or nullptr if there are no alive nodes
  std::shared_ptr<const rpc::GcsNodeInfo> SelectRandomAliveNode() const;

  /// Get all dead nodes.
  ///
  /// \return all dead nodes. Returns a copy of the map for thread safety
  absl::flat_hash_map<NodeID, std::shared_ptr<const rpc::GcsNodeInfo>> GetAllDeadNodes()
      const {
    absl::ReaderMutexLock lock(&mutex_);
    return dead_nodes_;
  }

  /// Add listener to monitor the remove action of nodes.
  ///
  /// \param listener The handler which process the remove of nodes.
  /// \param io_context the context to post the listener function to
  void AddNodeRemovedListener(
      std::function<void(std::shared_ptr<const rpc::GcsNodeInfo>)> listener,
      instrumented_io_context &io_context) {
    absl::MutexLock lock(&mutex_);
    RAY_CHECK(listener);
    node_removed_listeners_.emplace_back(std::move(listener), io_context);
  }

  /// Add listener to monitor the add action of nodes.
  ///
  /// \param listener The handler which process the add of nodes.
  /// \param io_context the context to post the listener function toƒ
  void AddNodeAddedListener(
      std::function<void(std::shared_ptr<const rpc::GcsNodeInfo>)> listener,
      instrumented_io_context &io_context) {
    absl::MutexLock lock(&mutex_);
    RAY_CHECK(listener);
    node_added_listeners_.emplace_back(std::move(listener), io_context);
  }

  /// Initialize with the gcs tables data synchronously.
  /// This should be called when GCS server restarts after a failure.
  ///
  /// \param gcs_init_data.
  void Initialize(const GcsInitData &gcs_init_data);

  std::string DebugString() const;

  /// Drain the given node.
  /// Idempotent.
  /// This is technically not draining a node. It should be just called "kill node".
  virtual void DrainNode(const NodeID &node_id);

  /// Update node state from a resource view sync message if the node is alive.
  ///
  /// \param node_id The ID of the node to update.
  /// \param resource_view_sync_message The sync message containing the new state.
  void UpdateAliveNode(
      const NodeID &node_id,
      const rpc::syncer::ResourceViewSyncMessage &resource_view_sync_message);

  /// Add an alive node.
  ///
  /// \param node The info of the node to be added.
  void AddNode(std::shared_ptr<const rpc::GcsNodeInfo> node);

  /// Remove a node from alive nodes. The node's death information will also be set.
  ///
  /// \param node_id The ID of the node to be removed.
  /// \param node_death_info The node death info to set.
  /// \param node_state the state to set the node to after it's removed
  /// \param update_time the update time to be applied to the node info
  /// \return The removed node, with death info set. If the node is not found, return
  /// nullptr.
  std::shared_ptr<const rpc::GcsNodeInfo> RemoveNode(
      const NodeID &node_id,
      const rpc::NodeDeathInfo &node_death_info,
      const rpc::GcsNodeInfo::GcsNodeState node_state,
      const int64_t update_time);

 private:
  /// Add an alive node.
  ///
  /// \param node The info of the node to be added.
  void AddNodeToCache(std::shared_ptr<const rpc::GcsNodeInfo> node)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  /// Add the dead node to the cache. If the cache is full, the earliest dead node is
  /// evicted.
  ///
  /// \param node The node which is dead.
  void AddDeadNodeToCache(std::shared_ptr<const rpc::GcsNodeInfo> node)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  /// Remove a node from alive nodes cache. The node's death information will also be set.
  ///
  /// \param node_id The ID of the node to be removed.
  /// \param node_death_info The node death info to set.
  /// \param node_state the state to set the node to after it's removed
  /// \param update_time the update time to be applied to the node info
  /// \return The removed node, with death info set. If the node is not found, return
  /// nullptr.
  std::shared_ptr<const rpc::GcsNodeInfo> RemoveNodeFromCache(
      const NodeID &node_id,
      const rpc::NodeDeathInfo &node_death_info,
      const rpc::GcsNodeInfo::GcsNodeState node_state,
      const int64_t update_time) ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  /// Retrieves the node from the alive_nodes cache without acquiring a lock
  ///
  /// \param node_id The id of the node.
  /// \return the node if it is alive. Optional empty value if it is not alive.
  std::optional<std::shared_ptr<const rpc::GcsNodeInfo>> GetAliveNodeFromCache(
      const ray::NodeID &node_id) const ABSL_SHARED_LOCKS_REQUIRED(mutex_);

  /// Handle a node failure. This will mark the failed node as dead in gcs
  /// node table.
  ///
  /// \param node_id The ID of the failed node.
  /// \param node_table_updated_callback The status callback function after
  /// failed node info is updated to gcs node table.
  void InternalOnNodeFailure(const NodeID &node_id,
                             const std::function<void()> &node_table_updated_callback)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  /// Infer death cause of the node based on existing draining requests.
  ///
  /// \param node_id The ID of the node. The node must not be removed
  /// from alive nodes yet.
  /// \return The inferred death info of the node.
  rpc::NodeDeathInfo InferDeathInfo(const NodeID &node_id)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  void WriteNodeExportEvent(const rpc::GcsNodeInfo &node_info,
                            bool is_register_event) const;

  // Verify if export events should be written for EXPORT_NODE source types
  bool IsExportAPIEnabledNode() const {
    return IsExportAPIEnabledSourceType(
        "EXPORT_NODE",
        RayConfig::instance().enable_export_api_write(),
        RayConfig::instance().enable_export_api_write_config());
  }

  static rpc::ExportNodeData::GcsNodeState ConvertGCSNodeStateToExport(
      rpc::GcsNodeInfo::GcsNodeState node_state) {
    switch (node_state) {
    case rpc::GcsNodeInfo_GcsNodeState::GcsNodeInfo_GcsNodeState_ALIVE:
      return rpc::ExportNodeData_GcsNodeState::ExportNodeData_GcsNodeState_ALIVE;
    case rpc::GcsNodeInfo_GcsNodeState::GcsNodeInfo_GcsNodeState_DEAD:
      return rpc::ExportNodeData_GcsNodeState::ExportNodeData_GcsNodeState_DEAD;
    default:
      // Unknown rpc::GcsNodeInfo::GcsNodeState value
      RAY_LOG(FATAL) << "Invalid value for rpc::GcsNodeInfo::GcsNodeState "
                     << rpc::GcsNodeInfo::GcsNodeState_Name(node_state);
      return rpc::ExportNodeData_GcsNodeState::ExportNodeData_GcsNodeState_DEAD;
    }
  }

  static rpc::ExportNodeData::NodeDeathInfo::Reason ConvertNodeDeathReasonToExport(
      const rpc::NodeDeathInfo::Reason reason) {
    switch (reason) {
    case rpc::NodeDeathInfo_Reason::NodeDeathInfo_Reason_UNSPECIFIED:
      return rpc::ExportNodeData_NodeDeathInfo_Reason::
          ExportNodeData_NodeDeathInfo_Reason_UNSPECIFIED;
    case rpc::NodeDeathInfo_Reason::NodeDeathInfo_Reason_EXPECTED_TERMINATION:
      return rpc::ExportNodeData_NodeDeathInfo_Reason::
          ExportNodeData_NodeDeathInfo_Reason_EXPECTED_TERMINATION;
    case rpc::NodeDeathInfo_Reason::NodeDeathInfo_Reason_UNEXPECTED_TERMINATION:
      return rpc::ExportNodeData_NodeDeathInfo_Reason::
          ExportNodeData_NodeDeathInfo_Reason_UNEXPECTED_TERMINATION;
    case rpc::NodeDeathInfo_Reason::NodeDeathInfo_Reason_AUTOSCALER_DRAIN_PREEMPTED:
      return rpc::ExportNodeData_NodeDeathInfo_Reason::
          ExportNodeData_NodeDeathInfo_Reason_AUTOSCALER_DRAIN_PREEMPTED;
    case rpc::NodeDeathInfo_Reason::NodeDeathInfo_Reason_AUTOSCALER_DRAIN_IDLE:
      return rpc::ExportNodeData_NodeDeathInfo_Reason::
          ExportNodeData_NodeDeathInfo_Reason_AUTOSCALER_DRAIN_IDLE;
    default:
      // Unknown rpc::GcsNodeInfo::GcsNodeState value
      RAY_LOG(FATAL) << "Invalid value for rpc::NodeDeathInfo::Reason "
                     << rpc::NodeDeathInfo::Reason_Name(reason);
      return rpc::ExportNodeData_NodeDeathInfo_Reason::
          ExportNodeData_NodeDeathInfo_Reason_UNSPECIFIED;
    }
  }

  /// Alive nodes.
  absl::flat_hash_map<NodeID, std::shared_ptr<const rpc::GcsNodeInfo>> alive_nodes_
      ABSL_GUARDED_BY(mutex_);
  /// Draining nodes.
  /// This map is used to store the nodes which have received the drain request.
  /// Invariant: its keys should always be a subset of the keys of `alive_nodes_`,
  /// and entry in it should be removed whenever a node is removed from `alive_nodes_`.
  absl::flat_hash_map<NodeID, std::shared_ptr<const rpc::autoscaler::DrainNodeRequest>>
      draining_nodes_ ABSL_GUARDED_BY(mutex_);
  /// Dead nodes.
  absl::flat_hash_map<NodeID, std::shared_ptr<const rpc::GcsNodeInfo>> dead_nodes_
      ABSL_GUARDED_BY(mutex_);
  /// The nodes are sorted according to the timestamp, and the oldest is at the head of
  /// the deque.
  std::deque<std::pair<NodeID, int64_t>> sorted_dead_node_list_ ABSL_GUARDED_BY(mutex_);

  /// Listeners which monitors the addition of nodes.
  std::vector<Postable<void(std::shared_ptr<const rpc::GcsNodeInfo>)>>
      node_added_listeners_ ABSL_GUARDED_BY(mutex_);

  /// Listeners which monitors the removal of nodes.
  std::vector<Postable<void(std::shared_ptr<const rpc::GcsNodeInfo>)>>
      node_removed_listeners_ ABSL_GUARDED_BY(mutex_);

  /// A publisher for publishing gcs messages.
  pubsub::GcsPublisher *gcs_publisher_;
  /// Storage for GCS tables.
  GcsTableStorage *gcs_table_storage_;
  instrumented_io_context &io_context_;
  /// Raylet client pool.
  rpc::RayletClientPool *raylet_client_pool_;
  /// Cluster ID to be shared with clients when connecting.
  const ClusterID cluster_id_;
  /// Class lock for node manager
  mutable absl::Mutex mutex_;

  observability::RayEventRecorderInterface &ray_event_recorder_;
  std::string session_name_;

  // Debug info.
  enum CountType {
    REGISTER_NODE_REQUEST = 0,
    DRAIN_NODE_REQUEST = 1,
    GET_ALL_NODE_INFO_REQUEST = 2,
    CountType_MAX = 3,
  };
  std::atomic<uint64_t> counts_[CountType::CountType_MAX] = {0};

  /// If true, node events are exported for Export API
  bool export_event_write_enabled_ = false;

  /// Ray metrics
  ray::stats::Count ray_metric_node_failures_total_{
      /*name=*/"node_failure_total",
      /*description=*/"Number of node failures that have happened in the cluster.",
      /*unit=*/""};

  friend GcsAutoscalerStateManagerTest;
  friend GcsStateTest;
};

}  // namespace gcs
}  // namespace ray
