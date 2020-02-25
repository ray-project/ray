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

#ifndef RAY_GCS_NODE_MANAGER_H
#define RAY_GCS_NODE_MANAGER_H

#include <ray/common/id.h>
#include <ray/protobuf/gcs.pb.h>
#include <ray/rpc/client_call.h>

namespace ray {
namespace gcs {
class RedisGcsClient;
/// GcsNodeManager is responsible for managing and monitoring nodes.
class GcsNodeManager {
 public:
  /// Create a GcsNodeManager.
  ///
  /// \param io_service The event loop to run the monitor on.
  /// \param gcs_client The client of gcs to access/pub/sub data.
  explicit GcsNodeManager(boost::asio::io_service &io_service,
                          std::shared_ptr<gcs::RedisGcsClient> gcs_client);

  /// Add an alive node.
  ///
  /// \param node The info of the node to be added.
  void AddNode(std::shared_ptr<rpc::GcsNodeInfo> node);

  /// Remove from alive nodes.
  ///
  /// \param node_id The ID of the node to be removed.
  void RemoveNode(const ClientID &node_id);

  /// Get alive node by ID.
  ///
  /// \param node_id The id of the node.
  /// \return the node if it is alive else return nullptr.
  std::shared_ptr<rpc::GcsNodeInfo> GetNode(const ClientID &node_id) const;

  /// Get all alive nodes.
  ///
  /// \return all alive nodes.
  const std::unordered_map<ClientID, std::shared_ptr<rpc::GcsNodeInfo>>
      &GetAllAliveNodes() const;

  /// Add listener to monitor the remove action of nodes.
  ///
  /// \param listener The handler which process the remove of nodes.
  void AddNodeRemovedListener(
      std::function<void(std::shared_ptr<rpc::GcsNodeInfo>)> &&listener) {
    RAY_CHECK(listener);
    node_removed_listeners_.emplace_back(std::move(listener));
  }

  /// Add listener to monitor the add action of nodes.
  ///
  /// \param listener The handler which process the add of nodes.
  void AddNodeAddedListener(
      std::function<void(std::shared_ptr<rpc::GcsNodeInfo>)> &&listener) {
    RAY_CHECK(listener);
    node_added_listeners_.emplace_back(std::move(listener));
  }

  /// Handle a heartbeat from a Raylet.
  ///
  /// \param node_id The client ID of the Raylet that sent the heartbeat.
  /// \param heartbeat_data The heartbeat sent by the client.
  void HandleHeartbeat(const ClientID &node_id,
                       const rpc::HeartbeatTableData &heartbeat_data);

 protected:
  /// Listen for heartbeats from Raylets and mark Raylets
  /// that do not send a heartbeat within a given period as dead.
  void Start();

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

 private:
  /// Alive nodes.
  std::unordered_map<ClientID, std::shared_ptr<rpc::GcsNodeInfo>> alive_nodes_;
  /// A client to the GCS, through which heartbeats are received.
  std::shared_ptr<gcs::RedisGcsClient> gcs_client_;
  /// The number of heartbeats that can be missed before a node is removed.
  int64_t num_heartbeats_timeout_;
  /// A timer that ticks every heartbeat_timeout_ms_ milliseconds.
  boost::asio::deadline_timer heartbeat_timer_;
  /// For each Raylet that we receive a heartbeat from, the number of ticks
  /// that may pass before the Raylet will be declared dead.
  std::unordered_map<ClientID, int64_t> heartbeats_;
  /// The Raylets that have been marked as dead in gcs.
  std::unordered_set<ClientID> dead_nodes_;
  /// A buffer containing heartbeats received from node managers in the last tick.
  std::unordered_map<ClientID, rpc::HeartbeatTableData> heartbeat_buffer_;
  /// Listeners which monitors the addition of nodes.
  std::vector<std::function<void(std::shared_ptr<rpc::GcsNodeInfo>)>>
      node_added_listeners_;
  /// Listeners which monitors the removal of nodes.
  std::vector<std::function<void(std::shared_ptr<rpc::GcsNodeInfo>)>>
      node_removed_listeners_;
};

}  // namespace gcs
}  // namespace ray

#endif  // RAY_GCS_NODE_MANAGER_H
