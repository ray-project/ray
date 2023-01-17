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
  /// \param message_type The type of the message for this component.
  /// \param reporter The reporter is defined to be the local module which wants to
  /// broadcast its internal status to the whole clsuter. When it's null, it means there
  /// is no reporter in the local node for this component. This is the place there
  /// messages are
  /// generated.
  /// \param receiver The receiver is defined to be the module which eventually
  /// will have the view of of the cluster for this component. It's the place where
  /// received messages are consumed.
  ///
  /// \return true if set successfully.
  bool SetComponent(MessageType message_type,
                    const ReporterInterface *reporter,
                    ReceiverInterface *receiver);

  /// Get the snapshot of a component for a newer version.
  ///
  /// \param message_type The component to take the snapshot.
  ///
  /// \return If a snapshot is taken, return the message, otherwise std::nullopt.
  std::optional<RaySyncMessage> CreateSyncMessage(MessageType message_type);

  /// Consume a message. Receiver will consume this message if it doesn't have
  /// this message.
  ///
  /// \param message The message received.
  ///
  /// \return true if the local node doesn't have message with newer version.
  bool ConsumeSyncMessage(std::shared_ptr<const RaySyncMessage> message);

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

  /// This field records the version of the sync message that has been taken.
  std::array<int64_t, kComponentArraySize> sync_message_versions_taken_;
  /// Keep track of the latest messages received.
  /// Use shared pointer for easier liveness management since these messages might be
  /// sending via rpc.
  absl::flat_hash_map<
      std::string,
      std::array<std::shared_ptr<const RaySyncMessage>, kComponentArraySize>>
      cluster_view_;
};

class RaySyncerBidiReactorBase {
 public:
  /// Constructor of RaySyncerBidiReactorBase.
  ///
  /// \param io_context The io context for the callback.
  /// \param remote_node_id The node id connects to.
  /// \param message_processor The callback for the message received.
  /// \param cleanup_cb When the connection terminates, it'll be called to cleanup
  ///     the environment.
  RaySyncerBidiReactorBase(
      instrumented_io_context &io_context,
      const std::string &remote_node_id,
      std::function<void(std::shared_ptr<const RaySyncMessage>)> message_processor,
      std::function<void(const std::string &, bool)> cleanup_cb);

  /// Push a message to the sending queue to be sent later. Some message
  /// might be dropped if the module think the target node has already got the
  /// information. Usually it'll happen when the message has the source node id
  /// as the target or the message is sent from the remote node.
  ///
  /// \param message The message to be sent.
  ///
  /// \return true if push to queue successfully.
  bool PushToSendingQueue(std::shared_ptr<const RaySyncMessage> message);

  virtual ~RaySyncerBidiReactorBase() {}

  /// Return the remote node id of this connection.
  const std::string &GetRemoteNodeID() const { return remote_node_id_; }

  /// Handle the udpates sent from the remote node.
  ///
  /// \param messages The message received.
  void ReceiveUpdate(std::shared_ptr<const RaySyncMessage> message);

  virtual void Disconnect() = 0;

 protected:
  /// The io context
  instrumented_io_context &io_context_;

  /// Handler of a message update.
  const std::function<void(std::shared_ptr<const RaySyncMessage>)> message_processor_;
  const std::function<void(const std::string &, bool)> cleanup_cb_;
  void SendNext();

 private:
  void StartSend();

  virtual void Send(std::shared_ptr<const RaySyncMessage> message, bool flush) = 0;

  // For testing
  FRIEND_TEST(RaySyncerTest, RaySyncerBidiReactorBase);
  friend struct SyncerServerTest;

  std::array<int64_t, kComponentArraySize> &GetNodeComponentVersions(
      const std::string &node_id);

  /// The remote node id.
  const std::string remote_node_id_;

  /// Buffering all the updates. Sending will be done in an async way.
  absl::flat_hash_map<std::pair<std::string, MessageType>,
                      std::shared_ptr<const RaySyncMessage>>
      sending_buffer_;

  /// Keep track of the versions of components in the remote node.
  /// This field will be udpated when messages are received or sent.
  /// We'll filter the received or sent messages when the message is stale.
  absl::flat_hash_map<std::string, std::array<int64_t, kComponentArraySize>>
      node_versions_;

  bool sending_ = false;
};

template <typename T>
class BidiReactor : public T, public RaySyncerBidiReactorBase {
 public:
  BidiReactor(
      instrumented_io_context &io_context,
      const std::string &remote_node_id,
      std::function<void(std::shared_ptr<const RaySyncMessage>)> message_processor,
      std::function<void(const std::string &, bool)> cleanup_cb)
      : RaySyncerBidiReactorBase(io_context,
                                 remote_node_id,
                                 std::move(message_processor),
                                 std::move(cleanup_cb)) {}

  using T::StartRead;
  using T::StartWrite;

  void Send(std::shared_ptr<const RaySyncMessage> message, bool flush) override {
    sending_message_ = std::move(message);
    grpc::WriteOptions opts;
    if (flush) {
      opts.clear_buffer_hint();
    } else {
      opts.set_buffer_hint();
    }
    RAY_LOG(DEBUG) << "[BidiReactor] Sending message to "
                   << NodeID::FromBinary(GetRemoteNodeID()) << " about node "
                   << NodeID::FromBinary(sending_message_->node_id());
    StartWrite(sending_message_.get(), opts);
  }

  void OnWriteDone(bool ok) override {
    if (ok) {
      io_context_.dispatch([this]() { SendNext(); }, "");
    } else {
      RAY_LOG_EVERY_N(ERROR, 100)
          << "Failed to send the message to: " << NodeID::FromBinary(GetRemoteNodeID());
    }
  }

  void OnReadDone(bool ok) override {
    if (!ok) {
      return;
    }
    io_context_.dispatch(
        [this, msg = std::move(receiving_message_)]() mutable {
          RAY_CHECK(!msg->node_id().empty());
          ReceiveUpdate(std::move(msg));
          StartPull();
        },
        "");
  }

 protected:
  void StartPull() {
    receiving_message_ = std::make_shared<RaySyncMessage>();
    RAY_LOG(DEBUG) << "Start reading: " << NodeID::FromBinary(GetRemoteNodeID());
    StartRead(receiving_message_.get());
  }

  /// grpc requests for sending and receiving
  std::shared_ptr<const RaySyncMessage> sending_message_;
  std::shared_ptr<RaySyncMessage> receiving_message_;
};

/// Reactor for gRPC server side. It has customized logic for sending.
class RayServerBidiReactor : public BidiReactor<ServerBidiReactor> {
 public:
  RayServerBidiReactor(
      grpc::CallbackServerContext *server_context,
      instrumented_io_context &io_context,
      const std::string &local_node_id,
      std::function<void(std::shared_ptr<const RaySyncMessage>)> message_processor,
      std::function<void(const std::string &, bool)> cleanup_cb);

  ~RayServerBidiReactor() override = default;

  void Disconnect() override;

 protected:
  void OnCancel() override;
  void OnDone() override;

  /// grpc callback context
  grpc::CallbackServerContext *server_context_;
};

/// Reactor for gRPC client side. It has customized logic for sending.
class RayClientBidiReactor : public BidiReactor<ClientBidiReactor> {
 public:
  RayClientBidiReactor(
      const std::string &remote_node_id,
      const std::string &local_node_id,
      instrumented_io_context &io_context,
      std::function<void(std::shared_ptr<const RaySyncMessage>)> message_processor,
      std::function<void(const std::string &, bool)> cleanup_cb,
      std::unique_ptr<ray::rpc::syncer::RaySyncer::Stub> stub);

  ~RayClientBidiReactor() override = default;

  void Disconnect() override;

 protected:
  /// Callback from gRPC
  void OnDone(const grpc::Status &status) override;

  /// grpc callback context
  grpc::ClientContext client_context_;

  std::unique_ptr<ray::rpc::syncer::RaySyncer::Stub> stub_;
};

}  // namespace syncer
}  // namespace ray
