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

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "ray/common/gcs_callback_types.h"
#include "ray/common/id.h"
#include "ray/common/status_or.h"
#include "src/ray/protobuf/gcs.pb.h"
#include "src/ray/protobuf/gcs_service.pb.h"

namespace ray {
namespace gcs {

/// \class NodeInfoAccessorInterface
/// Interface for NodeInfo operations.
class NodeInfoAccessorInterface {
 public:
  virtual ~NodeInfoAccessorInterface() = default;

  /// Register local node to GCS asynchronously.
  ///
  /// \param node_info The information of node to register to GCS.
  /// \param callback Callback that will be called when registration is complete.
  /// \return Status
  virtual Status RegisterSelf(const rpc::GcsNodeInfo &local_node_info,
                              const StatusCallback &callback) = 0;

  /// Unregister local node to GCS asynchronously.
  ///
  /// \param node_death_info The death information regarding why to unregister from GCS.
  /// \param unregister_done_callback Callback that will be called when unregistration is
  /// done.
  virtual void UnregisterSelf(const rpc::NodeDeathInfo &node_death_info,
                              std::function<void()> unregister_done_callback) = 0;

  /// Get id of local node which was registered by 'RegisterSelf'.
  ///
  /// \return NodeID
  virtual const NodeID &GetSelfId() const = 0;

  /// Get information of local node which was registered by 'RegisterSelf'.
  ///
  /// \return GcsNodeInfo
  virtual const rpc::GcsNodeInfo &GetSelfInfo() const = 0;

  /// Register a node to GCS asynchronously.
  ///
  /// \param node_info The information of node to register to GCS.
  /// \param callback Callback that will be called when registration is complete.
  virtual void AsyncRegister(const rpc::GcsNodeInfo &node_info,
                             const StatusCallback &callback) = 0;

  /// Send a check alive request to GCS for the liveness of this node.
  ///
  /// \param callback The callback function once the request is finished.
  /// \param timeout_ms The timeout for this request.
  virtual void AsyncCheckSelfAlive(const std::function<void(Status, bool)> &callback,
                                   int64_t timeout_ms) = 0;

  /// Send a check alive request to GCS for the liveness of some nodes.
  ///
  /// \param callback The callback function once the request is finished.
  /// \param timeout_ms The timeout for this request.
  virtual void AsyncCheckAlive(const std::vector<NodeID> &node_ids,
                               int64_t timeout_ms,
                               const MultiItemCallback<bool> &callback) = 0;

  /// Get information of all nodes from GCS asynchronously.
  ///
  /// \param callback Callback that will be called after lookup finishes.
  /// \param timeout_ms The timeout for this request.
  /// \param node_ids If this is not empty, only return the node info of the specified
  /// nodes.
  virtual void AsyncGetAll(const MultiItemCallback<rpc::GcsNodeInfo> &callback,
                           int64_t timeout_ms,
                           const std::vector<NodeID> &node_ids = {}) = 0;

  /// Subscribe to node addition and removal events from GCS and cache those information.
  ///
  /// \param subscribe Callback that will be called if a node is
  /// added or a node is removed. The callback needs to be idempotent because it will also
  /// be called for existing nodes.
  /// \param done Callback that will be called when subscription is complete.
  virtual void AsyncSubscribeToNodeChange(
      std::function<void(NodeID, const rpc::GcsNodeInfo &)> subscribe,
      StatusCallback done) = 0;

  /// Get node information from local cache.
  /// Non-thread safe.
  /// Note, the local cache is only available if `AsyncSubscribeToNodeChange`
  /// is called before.
  ///
  /// \param node_id The ID of node to look up in local cache.
  /// \param filter_dead_nodes Whether or not if this method will filter dead nodes.
  /// \return The item returned by GCS. If the item to read doesn't exist or the node is
  virtual  /// dead, this optional object is empty.
      const rpc::GcsNodeInfo *
      Get(const NodeID &node_id, bool filter_dead_nodes = true) const = 0;

  /// Get information of all nodes from local cache.
  /// Non-thread safe.
  /// Note, the local cache is only available if `AsyncSubscribeToNodeChange`
  /// is called before.
  ///
  /// \return All nodes in cache.
  virtual const absl::flat_hash_map<NodeID, rpc::GcsNodeInfo> &GetAll() const = 0;

  /// Get information of all nodes from an RPC to GCS synchronously with optional filters.
  ///
  /// \return All nodes that match the given filters from the gcs without the cache.
  virtual StatusOr<std::vector<rpc::GcsNodeInfo>> GetAllNoCache(
      int64_t timeout_ms,
      std::optional<rpc::GcsNodeInfo::GcsNodeState> state_filter = std::nullopt,
      std::optional<rpc::GetAllNodeInfoRequest::NodeSelector> node_selector =
          std::nullopt) = 0;

  /// Send a check alive request to GCS for the liveness of some nodes.
  ///
  /// \param raylet_addresses The addresses of the nodes to check, each like "ip:port".
  /// \param timeout_ms The timeout for this request.
  /// \param nodes_alive The liveness of the nodes. Only valid if the status is OK.
  /// \return Status
  virtual Status CheckAlive(const std::vector<NodeID> &node_ids,
                            int64_t timeout_ms,
                            std::vector<bool> &nodes_alive) = 0;

  /// Drain (remove the information of the nodes from the cluster) the specified nodes
  /// from GCS synchronously.
  ///
  /// Check gcs_service.proto NodeInfoGcsService.DrainNode for the API spec.
  ///
  /// \param node_ids The IDs of nodes to be unregistered.
  /// \param timeout_ms The timeout for this request.
  /// \param drained_node_ids The IDs of nodes that are drained.
  /// \return Status
  virtual Status DrainNodes(const std::vector<NodeID> &node_ids,
                            int64_t timeout_ms,
                            std::vector<std::string> &drained_node_ids) = 0;

  /// Search the local cache to find out if the given node is dead.
  /// If the node is not confirmed to be dead (this returns false), it could be that:
  /// 1. We haven't even received a node alive publish for it yet.
  /// 2. The node is alive and we have that information in the cache.
  /// 3. The GCS has evicted the node from its dead node cache based on
  ///    maximum_gcs_dead_node_cached_count
  /// Non-thread safe.
  /// Note, the local cache is only available if `AsyncSubscribeToNodeChange` is called
  /// before.
  virtual bool IsNodeDead(const NodeID &node_id) const = 0;

  /// Reestablish subscription.
  /// This should be called when GCS server restarts from a failure.
  /// PubSub server restart will cause GCS server restart. In this case, we need to
  /// resubscribe from PubSub server, otherwise we only need to fetch data from GCS
  /// server.
  virtual void AsyncResubscribe() = 0;

  /// Add a node to accessor cache.
  virtual void HandleNotification(rpc::GcsNodeInfo &&node_info) = 0;

  virtual bool IsSubscribedToNodeChange() const = 0;
};

}  // namespace gcs
}  // namespace ray
