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

#include <boost/bimap.hpp>
#include <boost/bimap/unordered_multiset_of.hpp>
#include <boost/bimap/unordered_set_of.hpp>

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "ray/common/id.h"
#include "ray/gcs/gcs_server/gcs_init_data.h"
#include "ray/gcs/gcs_server/gcs_resource_manager.h"
#include "ray/gcs/gcs_server/gcs_table_storage.h"
#include "ray/gcs/pubsub/gcs_pub_sub.h"
#include "ray/rpc/client_call.h"
#include "ray/rpc/gcs_server/gcs_rpc_server.h"
#include "ray/rpc/node_manager/node_manager_client.h"
#include "ray/rpc/node_manager/node_manager_client_pool.h"
#include "src/ray/protobuf/gcs.pb.h"

namespace ray {
namespace gcs {

class GcsAutoscalerStateManagerTest;
class GcsStateTest;
/// GcsNodeManager is responsible for managing and monitoring nodes as well as handing
/// node and resource related rpc requests.
/// This class is not thread-safe.
class GcsNodeManager : public rpc::NodeInfoHandler {
 public:
  /// Create a GcsNodeManager.
  ///
  /// \param gcs_publisher GCS message publisher.
  /// \param gcs_table_storage GCS table external storage accessor.
  explicit GcsNodeManager(std::shared_ptr<GcsPublisher> gcs_publisher,
                          std::shared_ptr<gcs::GcsTableStorage> gcs_table_storage,
                          std::shared_ptr<rpc::NodeManagerClientPool> raylet_client_pool,
                          const ClusterID &cluster_id);

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

  /// Handle unregister rpc request come from raylet.
  void HandleDrainNode(rpc::DrainNodeRequest request,
                       rpc::DrainNodeReply *reply,
                       rpc::SendReplyCallback send_reply_callback) override;

  /// Handle get all node info rpc request.
  void HandleGetAllNodeInfo(rpc::GetAllNodeInfoRequest request,
                            rpc::GetAllNodeInfoReply *reply,
                            rpc::SendReplyCallback send_reply_callback) override;

  /// Handle get internal config.
  void HandleGetInternalConfig(rpc::GetInternalConfigRequest request,
                               rpc::GetInternalConfigReply *reply,
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
  /// faled node info is updated to gcs node table.
  void OnNodeFailure(const NodeID &node_id,
                     const StatusCallback &node_table_updated_callback);

  /// Add an alive node.
  ///
  /// \param node The info of the node to be added.
  void AddNode(std::shared_ptr<rpc::GcsNodeInfo> node);

  /// Set the node to be draining.
  ///
  /// \param node_id The ID of the draining node. This node must already
  /// be in the alive nodes.
  /// \param request The drain node request.
  void SetNodeDraining(const NodeID &node_id,
                       std::shared_ptr<rpc::autoscaler::DrainNodeRequest> request);

  /// Remove a node from alive nodes. The node's death information will also be set.
  ///
  /// \param node_id The ID of the node to be removed.
  /// \param node_death_info The node death info to set.
  /// \return The removed node, with death info set. If the node is not found, return
  /// nullptr.
  std::shared_ptr<rpc::GcsNodeInfo> RemoveNode(const NodeID &node_id,
                                               const rpc::NodeDeathInfo &node_death_info);

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

  /// Get all dead nodes.
  const absl::flat_hash_map<NodeID, std::shared_ptr<rpc::GcsNodeInfo>> &GetAllDeadNodes()
      const {
    return dead_nodes_;
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

  std::string DebugString() const;

  /// Drain the given node.
  /// Idempotent.
  /// This is technically not draining a node. It should be just called "kill node".
  virtual void DrainNode(const NodeID &node_id);

 private:
  /// Add the dead node to the cache. If the cache is full, the earliest dead node is
  /// evicted.
  ///
  /// \param node The node which is dead.
  void AddDeadNodeToCache(std::shared_ptr<rpc::GcsNodeInfo> node);

  /// Infer death cause of the node based on existing draining requests.
  ///
  /// \param node_id The ID of the node. The node must not be removed
  /// from alive nodes yet.
  /// \return The inferred death info of the node.
  rpc::NodeDeathInfo InferDeathInfo(const NodeID &node_id);

  /// Alive nodes.
  absl::flat_hash_map<NodeID, std::shared_ptr<rpc::GcsNodeInfo>> alive_nodes_;
  /// Draining nodes.
  /// This map is used to store the nodes which have received the drain request.
  /// Invariant: its keys should alway be a subset of the keys of `alive_nodes_`,
  /// and entry in it should be removed whenever a node is removed from `alive_nodes_`.
  absl::flat_hash_map<NodeID, std::shared_ptr<rpc::autoscaler::DrainNodeRequest>>
      draining_nodes_;
  /// Dead nodes.
  absl::flat_hash_map<NodeID, std::shared_ptr<rpc::GcsNodeInfo>> dead_nodes_;
  /// The nodes are sorted according to the timestamp, and the oldest is at the head of
  /// the list.
  std::list<std::pair<NodeID, int64_t>> sorted_dead_node_list_;
  /// Listeners which monitors the addition of nodes.
  std::vector<std::function<void(std::shared_ptr<rpc::GcsNodeInfo>)>>
      node_added_listeners_;
  /// Listeners which monitors the removal of nodes.
  std::vector<std::function<void(std::shared_ptr<rpc::GcsNodeInfo>)>>
      node_removed_listeners_;
  /// A publisher for publishing gcs messages.
  std::shared_ptr<GcsPublisher> gcs_publisher_;
  /// Storage for GCS tables.
  std::shared_ptr<gcs::GcsTableStorage> gcs_table_storage_;
  /// Raylet client pool.
  std::shared_ptr<rpc::NodeManagerClientPool> raylet_client_pool_;
  /// Cluster ID to be shared with clients when connecting.
  const ClusterID cluster_id_;

  // Debug info.
  enum CountType {
    REGISTER_NODE_REQUEST = 0,
    DRAIN_NODE_REQUEST = 1,
    GET_ALL_NODE_INFO_REQUEST = 2,
    GET_INTERNAL_CONFIG_REQUEST = 3,
    CountType_MAX = 4,
  };
  uint64_t counts_[CountType::CountType_MAX] = {0};

  /// A map of NodeId <-> ip:port of raylet
  using NodeIDAddrBiMap =
      boost::bimap<boost::bimaps::unordered_set_of<NodeID, std::hash<NodeID>>,
                   boost::bimaps::unordered_multiset_of<std::string>>;
  NodeIDAddrBiMap node_map_;

  friend GcsAutoscalerStateManagerTest;
  friend GcsStateTest;
};

}  // namespace gcs
}  // namespace ray
