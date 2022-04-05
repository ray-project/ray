// Copyright 2022 The Ray Authors.
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
#include <grpcpp/server.h>
#include <gtest/gtest_prod.h>

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "boost/functional/hash.hpp"
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/asio/periodical_runner.h"
#include "ray/common/id.h"
#include "src/ray/protobuf/ray_syncer.grpc.pb.h"

namespace ray {
namespace syncer {

using ray::rpc::syncer::DummyRequest;
using ray::rpc::syncer::DummyResponse;
using ray::rpc::syncer::RayComponentId;
using ray::rpc::syncer::RaySyncMessage;
using ray::rpc::syncer::RaySyncMessages;
using ray::rpc::syncer::StartSyncRequest;
using ray::rpc::syncer::StartSyncResponse;

static constexpr size_t kComponentArraySize =
    static_cast<size_t>(ray::rpc::syncer::RayComponentId_ARRAYSIZE);

/// The interface for a reporter. Reporter is defined to be a local module which would
/// like to let the other nodes know its state. For example, local cluster resource
/// manager.
struct ReporterInterface {
  /// Interface to get the snapshot of the component. It asks the module to take a
  /// snapshot of the current state. Each snapshot is versioned and it should return
  /// std::nullopt if it doesn't have qualified version.
  ///
  /// \param version_after Request snapshot with version after `version_after`. If the
  /// reporter doesn't have the qualified version, just return std::nullopt
  /// \param component_id The component id asked for.
  ///
  /// \return std::nullopt if the reporter doesn't have such component or the current
  /// snapshot of the component is not newer the asked one. Otherwise, return the
  /// actual message.
  virtual std::optional<RaySyncMessage> Snapshot(int64_t version_after,
                                                 RayComponentId component_id) const = 0;
  virtual ~ReporterInterface() {}
};

/// The interface for a receiver. Receiver is defined to be a module which would like
/// to get the state of other nodes. For example, cluster resource manager.
struct ReceiverInterface {
  /// Interface to update a module. The module should read the `sync_message` fields and
  /// deserialize it to update its internal state.
  ///
  /// \param message The message received from remote node.
  virtual void Update(std::shared_ptr<const RaySyncMessage> message) = 0;

  virtual ~ReceiverInterface() {}
};

// Forward declaration of internal structures
class NodeState;
class NodeSyncConnection;

/// RaySyncer is an embedding service for component synchronization.
/// All operations in this class needs to be finished GetIOContext()
/// for thread-safety.
/// RaySyncer is the control plane to make sure all connections eventually
/// have the latest view of the cluster components registered.
/// RaySyncer has two components:
///    1. NodeSyncConnection: keeps track of the sending and receiving information
///       and make sure not sending the information the remote node knows.
///    2. NodeState: keeps track of the local status, similar to NodeSyncConnection,
//        but it's for local node.
class RaySyncer {
 public:
  /// Constructor of RaySyncer
  ///
  /// \param io_context The io context for this component.
  /// \param node_id The id of current node.
  RaySyncer(instrumented_io_context &io_context, const std::string &node_id);
  ~RaySyncer();

  /// Connect to a node.
  /// TODO (iycheng): Introduce grpc channel pool and use node_id
  /// for the connection.
  ///
  /// \param connection The connection to the remote node.
  void Connect(std::unique_ptr<NodeSyncConnection> connection);

  /// Connect to a node.
  /// TODO (iycheng): Introduce grpc channel pool and use node_id
  /// for the connection.
  ///
  /// \param connection The connection to the remote node.
  void Connect(std::shared_ptr<grpc::Channel> channel);

  void Disconnect(const std::string &node_id);

  /// Register the components to the syncer module. Syncer will make sure eventually
  /// it'll have a global view of the cluster.
  ///
  /// Right now there are two types of components. One type of components will
  /// try to broadcast the messages to make sure eventually the cluster will reach
  /// an agreement (upward_only=false). The other type of components will only
  /// send the message to upward (upward_only=true). Right now, upward is defined
  /// to be the place which received the connection. In Ray, one type of this message
  /// is resource load which only GCS needs.
  /// TODO (iycheng): 1) Revisit this and come with a better solution; or 2) implement
  /// resource loads in another way to avoid this feature; or 3) broadcast resource
  /// loads so the scheduler can also use this.
  ///
  /// \param component_id The component to sync.
  /// \param reporter The local component to be broadcasted.
  /// \param receiver The snapshot of the component in the cluster.
  /// \param upward_only Only send the message to the upward of this node.
  /// component.
  /// \param pull_from_reporter_interval_ms The frequence to pull a message
  /// from reporter and push it to sending queue.
  bool Register(RayComponentId component_id,
                const ReporterInterface *reporter,
                ReceiverInterface *receiver,
                bool upward_only = false,
                int64_t pull_from_reporter_interval_ms = 100);

  /// Function to broadcast the messages to other nodes.
  /// A message will be sent to a node if that node doesn't have this message.
  /// The message can be generated by local reporter or received by the other node.
  ///
  /// \param message The message to be broadcasted.
  void BroadcastMessage(std::shared_ptr<const RaySyncMessage> message);

  /// Get the current node id.
  const std::string &GetLocalNodeID() const { return local_node_id_; }

  /// Get the io_context used by RaySyncer.
  instrumented_io_context &GetIOContext() { return io_context_; }

  /// Get the SyncConnection of a node.
  ///
  /// \param node_id The node id to lookup.
  ///
  /// \return nullptr if it doesn't exist, otherwise, the connection associated with the
  /// node.
  NodeSyncConnection *GetSyncConnection(const std::string &node_id) const {
    auto iter = sync_connections_.find(node_id);
    if (iter == sync_connections_.end()) {
      return nullptr;
    }
    return iter->second.get();
  }

 private:
  /// io_context for this thread
  instrumented_io_context &io_context_;

  /// The current node id.
  const std::string local_node_id_;

  /// Manage connections. Here the key is the NodeID in binary form.
  absl::flat_hash_map<std::string, std::unique_ptr<NodeSyncConnection>> sync_connections_;

  /// Upward connections. These are connections initialized not by the local node.
  absl::flat_hash_set<NodeSyncConnection *> upward_connections_;

  /// The local node state
  std::unique_ptr<NodeState> node_state_;

  /// Each component will define a flag to indicate whether the message should be sent
  /// to ClientSyncConnection only.
  std::array<bool, kComponentArraySize> upward_only_;

  /// Timer is used to do broadcasting.
  ray::PeriodicalRunner timer_;

  std::shared_ptr<bool> stopped_;

  /// Test purpose
  friend struct SyncerServerTest;
  FRIEND_TEST(SyncerTest, Broadcast);
  FRIEND_TEST(SyncerTest, Test1To1);
  FRIEND_TEST(SyncerTest, Test1ToN);
  FRIEND_TEST(SyncerTest, TestMToN);
};

class ClientSyncConnection;
class ServerSyncConnection;

/// RaySyncerService is a service to take care of resource synchronization
/// related operations.
/// Right now only raylet needs to setup this service. But in the future,
/// we can use this to construct more complicated resource reporting algorithm,
/// like tree-based one.
class RaySyncerService : public ray::rpc::syncer::RaySyncer::CallbackService {
 public:
  RaySyncerService(RaySyncer &syncer) : syncer_(syncer) {}

  ~RaySyncerService();

  grpc::ServerUnaryReactor *StartSync(grpc::CallbackServerContext *context,
                                      const StartSyncRequest *request,
                                      StartSyncResponse *response) override;

  grpc::ServerUnaryReactor *Update(grpc::CallbackServerContext *context,
                                   const RaySyncMessages *request,
                                   DummyResponse *) override;

  grpc::ServerUnaryReactor *LongPolling(grpc::CallbackServerContext *context,
                                        const DummyRequest *,
                                        RaySyncMessages *response) override;

 private:
  // This will be created after connection is established.
  // Ideally this should be owned by RaySyncer, but since we are doing
  // long-polling right now, we have to put it here so that when
  // long-polling request comes, we can set it up.
  std::string remote_node_id_;

  // The ray syncer this RPC wrappers of.
  RaySyncer &syncer_;
};

}  // namespace syncer
}  // namespace ray

#include "ray/common/ray_syncer/ray_syncer-inl.h"
