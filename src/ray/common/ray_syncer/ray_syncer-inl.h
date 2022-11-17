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

class NodeSyncConnection {
 public:
  NodeSyncConnection(
      instrumented_io_context &io_context,
      std::function<void(std::shared_ptr<const RaySyncMessage>)> message_processor,
      std::function<void(const std::string&, bool)> cleanup_cb);

  /// Push a message to the sending queue to be sent later. Some message
  /// might be dropped if the module think the target node has already got the
  /// information. Usually it'll happen when the message has the source node id
  /// as the target or the message is sent from the remote node.
  ///
  /// \param message The message to be sent.
  ///
  /// \return true if push to queue successfully.
  bool PushToSendingQueue(std::shared_ptr<const RaySyncMessage> message);

  /// Flush the streaming buffer
  void Flush();

  virtual ~NodeSyncConnection() {}

  /// Return the remote node id of this connection.
  const std::string &GetRemoteNodeID() const { return remote_node_id_; }

  /// Handle the udpates sent from the remote node.
  ///
  /// \param messages The message received.
  void ReceiveUpdate(std::shared_ptr<const RaySyncMessage> message);

 protected:
  void StartSend();
  
  virtual void Send(std::shared_ptr<const RaySyncMessage> message) = 0;

  // For testing
  FRIEND_TEST(RaySyncerTest, NodeSyncConnection);
  friend struct SyncerServerTest;

  std::array<int64_t, kComponentArraySize> &GetNodeComponentVersions(
      const std::string &node_id);

  /// The io context
  instrumented_io_context &io_context_;

  /// The remote node id.
  std::string remote_node_id_;

  /// Handler of a message update.
  std::function<void(std::shared_ptr<const RaySyncMessage>)> message_processor_;
  std::function<void(const std::string&, bool)> cleanup_cb_;

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

extern RaySyncMessage _empty_msg;

template<typename T>
class BidiReactor : public T, public NodeSyncConnection {
 public:
  BidiReactor(
      instrumented_io_context &io_context,
      std::function<void(std::shared_ptr<const RaySyncMessage>)> message_processor,
      std::function<void(const std::string&, bool)> cleanup_cb)
      : NodeSyncConnection(io_context, std::move(message_processor), std::move(cleanup_cb)) {}

  using T::StartWrite;
  using T::StartRead;
  
  void Send(std::shared_ptr<const RaySyncMessage> message) {
    sending_message_ = std::move(message);
    if(sending_message_ != nullptr) {
      grpc::WriteOptions opts;
      opts.set_corked();
      StartWrite(message.get(), opts);
    } else {
      grpc::WriteOptions opts;
      opts.clear_corked();
      StartWrite(&_empty_msg, opts);
    }
  }

  void StartPull() {
    receiving_message_ = std::make_shared<RaySyncMessage>();
    RAY_LOG(DEBUG) << "Start reading: " << NodeID::FromBinary(GetRemoteNodeID());
    StartRead(receiving_message_.get());
  }
  
  void OnWriteDone(bool ok) {
    io_context_.dispatch([this, ok]() {
      if(ok) {
        sending_message_.reset();
      } else {
        RAY_LOG(ERROR) << "Failed to send the message to: " << NodeID::FromBinary(remote_node_id_);
        if(sending_message_ == nullptr) {
          // This is a flush message
          sending_ = true;
        } else {
          // Resend the message
          PushToSendingQueue(std::move(sending_message_));
        }
      }
      StartSend();
    }, "");
  }

  void OnReadDone(bool ok) {
    RAY_LOG(DEBUG) << "Receive messages: " << ok << ", node_id=" << NodeID::FromBinary(receiving_message_->node_id());
    if(ok && !receiving_message_->node_id().empty()) {
      io_context_.dispatch([this, msg = std::move(receiving_message_)]() mutable {
        ReceiveUpdate(std::move(msg));
      }, "");
    }
    StartPull();
  }
  
 protected:
  /// grpc requests for sending and receiving
  std::shared_ptr<const RaySyncMessage> sending_message_;
  std::shared_ptr<RaySyncMessage> receiving_message_;
};

/// SyncConnection for gRPC server side. It has customized logic for sending.
class RayServerBidiReactor : public BidiReactor<ServerBidiReactor> {
 public:
  RayServerBidiReactor(
      grpc::CallbackServerContext* server_context,
      instrumented_io_context &io_context,
      const std::string &local_node_id,
      std::function<void(std::shared_ptr<const RaySyncMessage>)> message_processor,
      std::function<void(const std::string&, bool)> cleanup_cb);

  ~RayServerBidiReactor() override = default;
  
 protected:
  void OnDone()  override;
  
  /// grpc callback context
  grpc::CallbackServerContext* server_context_;

};

/// SyncConnection for gRPC client side. It has customized logic for sending.
class RayClientBidiReactor : public BidiReactor<ClientBidiReactor> {
 public:
  RayClientBidiReactor(
      const std::string& remote_node_id,
      const std::string& local_node_id,
      instrumented_io_context &io_context,
      std::function<void(std::shared_ptr<const RaySyncMessage>)> message_processor,
      std::function<void(const std::string&, bool)> cleanup_cb,
      std::unique_ptr<ray::rpc::syncer::RaySyncer::Stub> stub);

  ~RayClientBidiReactor() override = default;
 protected:

  void Send(std::shared_ptr<const RaySyncMessage> message) override;

  /// Callback from gRPC
  void OnDone(const grpc::Status& status) override;

  std::unique_ptr<ray::rpc::syncer::RaySyncer::Stub> stub_;

  /// grpc callback context
  std::unique_ptr<grpc::ClientContext> client_context_;

  /// grpc requests for sending and receiving
  std::shared_ptr<const RaySyncMessage> sending_message_;
  std::shared_ptr<RaySyncMessage> receiving_message_;

};

}  // namespace syncer
}  // namespace ray
