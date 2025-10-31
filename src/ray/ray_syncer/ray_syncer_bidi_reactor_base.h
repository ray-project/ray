// Copyright 2024 The Ray Authors.
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

#include <functional>
#include <memory>
#include <string>
#include <utility>

#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/id.h"
#include "ray/ray_syncer/common.h"
#include "ray/ray_syncer/ray_syncer_bidi_reactor.h"
#include "src/ray/protobuf/ray_syncer.grpc.pb.h"

namespace ray::syncer {

/// This class implements the communication between two nodes except the initialization
/// and cleanup.
/// It keeps track of the message received and sent between two nodes and uses that to
/// deduplicate the messages. It also supports the batching for performance purposes.
template <typename T>
class RaySyncerBidiReactorBase : public RaySyncerBidiReactor, public T {
 public:
  /// Constructor of RaySyncerBidiReactor.
  ///
  /// \param io_context The io context for the callback.
  /// \param remote_node_id The node id connects to.
  /// \param message_processor The callback for the message received.
  /// \param cleanup_cb When the connection terminates, it'll be called to cleanup
  ///     the environment.
  RaySyncerBidiReactorBase(
      instrumented_io_context &io_context,
      std::string remote_node_id,
      std::function<void(std::shared_ptr<const RaySyncMessage>)> message_processor)
      : RaySyncerBidiReactor(std::move(remote_node_id)),
        io_context_(io_context),
        message_processor_(std::move(message_processor)) {}

  bool PushToSendingQueue(std::shared_ptr<const RaySyncMessage> message) override {
    if (*IsDisconnected()) {
      return false;
    }

    // Try to filter out the messages the target node already has.
    // Usually it'll be the case when the message is generated from the
    // target node or it's sent from the target node.
    // No need to resend the message sent from a node back.
    if (message->node_id() == GetRemoteNodeID()) {
      // Skip the message when it's about the node of this connection.
      return false;
    }

    auto &node_versions = GetNodeComponentVersions(message->node_id());
    if (node_versions[message->message_type()] < message->version()) {
      node_versions[message->message_type()] = message->version();
      sending_buffer_[std::make_pair(message->node_id(), message->message_type())] =
          std::move(message);
      StartSend();
      return true;
    }
    return false;
  }

  virtual ~RaySyncerBidiReactorBase() = default;

  void StartPull() {
    receiving_message_ = std::make_shared<RaySyncMessage>();
    RAY_LOG(DEBUG) << "Start reading: " << NodeID::FromBinary(GetRemoteNodeID());
    StartRead(receiving_message_.get());
  }

 protected:
  /// The io context
  instrumented_io_context &io_context_;

 private:
  /// Handle the updates sent from the remote node.
  ///
  /// \param messages The message received.
  void ReceiveUpdate(std::shared_ptr<const RaySyncMessage> message) {
    auto &node_versions = GetNodeComponentVersions(message->node_id());
    RAY_LOG(DEBUG) << "Receive update: "
                   << " message_type=" << message->message_type()
                   << ", message_version=" << message->version()
                   << ", local_message_version="
                   << node_versions[message->message_type()];
    if (node_versions[message->message_type()] < message->version()) {
      node_versions[message->message_type()] = message->version();
      message_processor_(message);
    } else {
      RAY_LOG_EVERY_MS(WARNING, 1000)
          << "Drop message received from " << NodeID::FromBinary(message->node_id())
          << " because the message version " << message->version()
          << " is older than the local version " << node_versions[message->message_type()]
          << ". Message type: " << message->message_type();
    }
  }

  void SendNext() {
    sending_ = false;
    StartSend();
  }

  void StartSend() {
    if (sending_) {
      return;
    }

    if (sending_buffer_.size() != 0) {
      auto iter = sending_buffer_.begin();
      auto msg = std::move(iter->second);
      sending_buffer_.erase(iter);
      Send(std::move(msg), sending_buffer_.empty());
      sending_ = true;
    }
  }

  /// Sending a message to the remote node
  ///
  /// \param message The message to be sent
  /// \param flush Whether to flush the sending queue in gRPC.
  void Send(std::shared_ptr<const RaySyncMessage> message, bool flush) {
    sending_message_ = std::move(message);
    grpc::WriteOptions opts;
    if (flush) {
      opts.clear_buffer_hint();
    } else {
      opts.set_buffer_hint();
    }
    RAY_LOG(DEBUG) << "[BidiReactor] Sending message to "
                   << NodeID::FromBinary(GetRemoteNodeID()) << " about node "
                   << NodeID::FromBinary(sending_message_->node_id()) << " with flush "
                   << flush;
    StartWrite(sending_message_.get(), opts);
  }

  // Please refer to grpc callback api for the following four methods:
  //     https://github.com/grpc/proposal/blob/master/L67-cpp-callback-api.md
  using T::StartRead;
  using T::StartWrite;

  void OnWriteDone(bool ok) override {
    io_context_.dispatch(
        [this, disconnected = IsDisconnected(), ok]() {
          if (*disconnected) {
            return;
          }
          if (ok) {
            SendNext();
          } else {
            RAY_LOG_EVERY_MS(INFO, 1000) << "Failed to send a message to node: "
                                         << NodeID::FromBinary(GetRemoteNodeID());
            Disconnect();
          }
        },
        "");
  }

  void OnReadDone(bool ok) override {
    io_context_.dispatch(
        [this,
         ok,
         disconnected = IsDisconnected(),
         msg = std::move(receiving_message_)]() mutable {
          if (*disconnected) {
            return;
          }

          if (!ok) {
            RAY_LOG_EVERY_MS(INFO, 1000) << "Failed to read a message from node: "
                                         << NodeID::FromBinary(GetRemoteNodeID());
            Disconnect();
            return;
          }

          // Successful rpc completion callback.
          RAY_CHECK(!msg->node_id().empty());
          if (on_rpc_completion_) {
            on_rpc_completion_(NodeID::FromBinary(remote_node_id_));
          }
          ReceiveUpdate(std::move(msg));
          StartPull();
        },
        "");
  }

  /// grpc requests for sending and receiving
  std::shared_ptr<const RaySyncMessage> sending_message_;
  std::shared_ptr<RaySyncMessage> receiving_message_;

  // For testing
  FRIEND_TEST(RaySyncerTest, RaySyncerBidiReactorBase);
  friend struct SyncerServerTest;

  std::array<int64_t, kComponentArraySize> &GetNodeComponentVersions(
      const std::string &node_id) {
    auto iter = node_versions_.find(node_id);
    if (iter == node_versions_.end()) {
      iter = node_versions_.emplace(node_id, std::array<int64_t, kComponentArraySize>())
                 .first;
      iter->second.fill(-1);
    }
    return iter->second;
  }

  /// Handler of a message update.
  const std::function<void(std::shared_ptr<const RaySyncMessage>)> message_processor_;

 private:
  /// Buffering all the updates. Sending will be done in an async way.
  absl::flat_hash_map<std::pair<std::string, MessageType>,
                      std::shared_ptr<const RaySyncMessage>>
      sending_buffer_;

  /// Keep track of the versions of components in the remote node.
  /// This field will be updated when messages are received or sent.
  /// We'll filter the received or sent messages when the message is stale.
  absl::flat_hash_map<std::string, std::array<int64_t, kComponentArraySize>>
      node_versions_;

  bool sending_ = false;
};

}  // namespace ray::syncer
