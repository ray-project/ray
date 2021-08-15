// Copyright 2021 The Ray Authors.
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

#include "ray/common/asio/instrumented_io_context.h"
#include "ray/gcs/gcs_server/gcs_resource_manager.h"
#include "ray/rpc/node_manager/node_manager_client_pool.h"

namespace ray {
namespace gcs {

/// Broadcasts resource report batches to raylets from a separate thread.
class GrpcBasedResourceBroadcaster {
 public:
  GrpcBasedResourceBroadcaster(
      std::shared_ptr<rpc::NodeManagerClientPool> raylet_client_pool,
      std::function<void(rpc::ResourceUsageBroadcastData &)>
          get_resource_usage_batch_for_broadcast,
      /* Default values should only be changed for testing. */
      std::function<void(const rpc::Address &,
                         std::shared_ptr<rpc::NodeManagerClientPool> &, std::string &,
                         const rpc::ClientCallback<rpc::UpdateResourceUsageReply> &)>
          send_batch =
              [](const rpc::Address &address,
                 std::shared_ptr<rpc::NodeManagerClientPool> &raylet_client_pool,
                 std::string &serialized_resource_usage_batch,
                 const rpc::ClientCallback<rpc::UpdateResourceUsageReply> &callback) {
                auto raylet_client = raylet_client_pool->GetOrConnectByAddress(address);
                raylet_client->UpdateResourceUsage(serialized_resource_usage_batch,
                                                   callback);
              });
  ~GrpcBasedResourceBroadcaster();

  void Initialize(const GcsInitData &gcs_init_data);

  /// Start a thread to broadcast resource reports..
  void Start();

  /// Stop broadcasting resource reports.
  void Stop();

  /// Event handler when a new node joins the cluster.
  void HandleNodeAdded(const rpc::GcsNodeInfo &node_info) LOCKS_EXCLUDED(mutex_);

  /// Event handler when a node leaves the cluster.
  void HandleNodeRemoved(const rpc::GcsNodeInfo &node_info) LOCKS_EXCLUDED(mutex_);

  std::string DebugString();

 private:
  // The sequence number of the next broadcast to send.
  int64_t seq_no_;
  // An asio service which does the broadcasting work.
  instrumented_io_context broadcast_service_;
  // The associated thread it runs on.
  std::unique_ptr<std::thread> broadcast_thread_;
  // Timer tick to send the next broadcast round.
  PeriodicalRunner ticker_;

  // The shared, thread safe pool of raylet clients, which we use to minimize connections.
  std::shared_ptr<rpc::NodeManagerClientPool> raylet_client_pool_;
  /// See GcsResourcManager::GetResourceUsageBatchForBroadcast. This is passed as an
  /// argument for unit testing purposes only.
  std::function<void(rpc::ResourceUsageBroadcastData &)>
      get_resource_usage_batch_for_broadcast_;

  std::function<void(const rpc::Address &, std::shared_ptr<rpc::NodeManagerClientPool> &,
                     std::string &,
                     const rpc::ClientCallback<rpc::UpdateResourceUsageReply> &)>
      send_batch_;

  /// A lock to protect the data structures.
  absl::Mutex mutex_;
  /// The set of nodes and their addresses which are subscribed to resource usage changes.
  std::unordered_map<NodeID, rpc::Address> nodes_ GUARDED_BY(mutex_);
  /// Whether the node currently has an inflight request already. An inflight request is
  /// one that has been sent, with no reply, error, or timeout.
  std::unordered_map<NodeID, bool> inflight_updates_ GUARDED_BY(mutex_);

  /// The number of nodes skipped in the latest broadcast round. This is useful for
  /// diagnostic purposes.
  uint64_t num_skipped_nodes_;
  const uint64_t broadcast_period_ms_;

  void SendBroadcast();

  friend class GrpcBasedResourceBroadcasterTest;
};
}  // namespace gcs
}  // namespace ray
