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

#include <boost/bimap.hpp>
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
                          std::shared_ptr<rpc::NodeManagerClientPool> raylet_client_pool);

  /// Handle register rpc request come from raylet.
  void HandleRegisterNode(rpc::RegisterNodeRequest request,
                          rpc::RegisterNodeReply *reply,
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

  std::string DebugString() const;

  /// Drain the given node.
  /// Idempotent.
  void DrainNode(const NodeID &node_id);

 private:
  /// Add the dead node to the cache. If the cache is full, the earliest dead node is
  /// evicted.
  ///
  /// \param node The node which is dead.
  void AddDeadNodeToCache(std::shared_ptr<rpc::GcsNodeInfo> node);

  /// Alive nodes.
  absl::flat_hash_map<NodeID, std::shared_ptr<rpc::GcsNodeInfo>> alive_nodes_;
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
                   boost::bimaps::unordered_set_of<std::string>>;
  NodeIDAddrBiMap node_map_;
};

}  // namespace gcs
}  // namespace ray
