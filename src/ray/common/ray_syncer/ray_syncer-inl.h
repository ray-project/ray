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

namespace ray {
namespace syncer {

/// NodeState keeps track of the modules in the local nodes.
/// It contains the local components for receiving and reporting.
/// It also keeps the raw messages receivers got.
class NodeState {
 public:
  /// Constructor of NodeState.
  NodeState();

  /// Set the local component.
  ///
  /// \param cid The component id.
  /// \param reporter The reporter is defined to be the local module which wants to
  /// broadcast its internal status to the whole clsuter. When it's null, it means there
  /// is no reporter in this node for this component. This is the place there messages are
  /// generated.
  /// \param receiver The receiver is defined to be the module which eventually
  /// will have the view of of the cluster for this component. It's the place where
  /// received messages are consumed.
  ///
  /// \return true if set successfully.
  bool SetComponent(RayComponentId cid,
                    const ReporterInterface *reporter,
                    ReceiverInterface *receiver);

  /// Get the snapshot of a component for a newer version.
  ///
  /// \param cid The component id to take the snapshot.
  ///
  /// \return If a snapshot is taken, return the message, otherwise std::nullopt.
  std::optional<RaySyncMessage> GetSnapshot(RayComponentId cid);

  /// Consume a message. Receiver will consume this message if it doesn't have
  /// this message.
  ///
  /// \param message The message received.
  ///
  /// \return true if this node doesn't have message with newer version.
  bool ConsumeMessage(std::shared_ptr<const RaySyncMessage> message);

  /// Return the cluster view of this local node.
  const absl::flat_hash_map<
      std::string,
      std::array<std::shared_ptr<const RaySyncMessage>, kComponentArraySize>>
      &GetClusterView() const {
    return cluster_view_;
  }

 private:
  /// For local nodes
  std::array<const ReporterInterface *, kComponentArraySize> reporters_ = {nullptr};
  std::array<ReceiverInterface *, kComponentArraySize> receivers_ = {nullptr};

  /// This field records the version of the snapshot that has been taken.
  std::array<int64_t, kComponentArraySize> snapshots_taken_;
  /// Keep track of the latest messages received.
  /// Use shared pointer for easier liveness management since these messages might be
  /// sending via rpc.
  absl::flat_hash_map<
      std::string,
      std::array<std::shared_ptr<const RaySyncMessage>, kComponentArraySize>>
      cluster_view_;
};

class NodeSyncConnection {
 public:
  NodeSyncConnection(RaySyncer &instance,
                     instrumented_io_context &io_context,
                     std::string node_id);

  /// Push a message to the sending queue to be sent later. Some message
  /// might be dropped if the module think the target node has already got the
  /// information. Usually it'll happen when the message has the same node id
  /// as the target or the message is sent from this node.
  ///
  /// \param message The message to be sent.
  ///
  /// \return true if push to queue successfully.
  bool PushToSendingQueue(std::shared_ptr<const RaySyncMessage> message);

  /// Send the message queued.
  virtual void DoSend() = 0;

  virtual ~NodeSyncConnection() {}

  /// Return the node id of this connection.
  const std::string &GetNodeId() const { return node_id_; }

  /// Handle the udpates sent from this node.
  ///
  /// \param messages The message received.
  void ReceiveUpdate(RaySyncMessages messages);

 protected:
  FRIEND_TEST(RaySyncerTest, NodeSyncConnection);

  std::array<int64_t, kComponentArraySize> &GetNodeComponentVersions(
      const std::string &node_id);

  RaySyncer &instance_;
  instrumented_io_context &io_context_;
  std::string node_id_;

  struct _MessageHash {
    std::size_t operator()(
        const std::shared_ptr<const RaySyncMessage> &m) const noexcept {
      std::size_t seed = 0;
      boost::hash_combine(seed, m->node_id());
      boost::hash_combine(seed, m->component_id());
      return seed;
    }
  };

  struct _MessageEq {
    bool operator()(const std::shared_ptr<const RaySyncMessage> &lhs,
                    const std::shared_ptr<const RaySyncMessage> &rhs) const noexcept {
      if (lhs == rhs) {
        return true;
      }
      if (lhs == nullptr || rhs == nullptr) {
        return false;
      }
      // We don't check the version here since we want the old version to be deleted.
      return lhs->node_id() == rhs->node_id() &&
             lhs->component_id() == rhs->component_id();
    }
  };

  absl::flat_hash_set<std::shared_ptr<const RaySyncMessage>, _MessageHash, _MessageEq>
      sending_queue_;
  // Keep track of the versions of components in this node.
  absl::flat_hash_map<std::string, std::array<int64_t, kComponentArraySize>>
      node_versions_;
};

class ServerSyncConnection : public NodeSyncConnection {
 public:
  ServerSyncConnection(RaySyncer &instance,
                       instrumented_io_context &io_context,
                       const std::string &node_id);

  ~ServerSyncConnection() override;

  void HandleLongPollingRequest(grpc::ServerUnaryReactor *reactor,
                                RaySyncMessages *response);

 protected:
  /// Send the message from the pending queue to the target node.
  /// It'll send nothing unless there is a request from the remote node
  /// for the sending request.
  /// TODO (iycheng): Unify the sending algorithm when we migrate to gRPC streaming
  void DoSend() override;

  /// These two fields are RPC related. When the server got long-polling requests,
  /// these two fields will be set so that it can be used to send message.
  /// After the message being sent, these two fields will be set to be empty again.
  /// When the periodical timer wake up, it'll check whether these two fields are set
  /// and it'll only send data when these are set.
  RaySyncMessages *response_ = nullptr;
  grpc::ServerUnaryReactor *unary_reactor_ = nullptr;
};

class ClientSyncConnection : public NodeSyncConnection {
 public:
  ClientSyncConnection(RaySyncer &instance,
                       instrumented_io_context &io_context,
                       const std::string &node_id,
                       std::shared_ptr<grpc::Channel> channel);

 protected:
  /// Send the message from the pending queue to the target node.
  /// It'll use gRPC to send the message directly.
  void DoSend() override;

  /// Start to send long-polling request to remote nodes.
  void StartLongPolling();

  /// Stub for this connection.
  std::unique_ptr<ray::rpc::syncer::RaySyncer::Stub> stub_;

  /// Where the received message is stored.
  ray::rpc::syncer::RaySyncMessages in_message_;

  /// Dummy request for long-polling.
  DummyRequest dummy_;
};

}  // namespace syncer
}  // namespace ray
