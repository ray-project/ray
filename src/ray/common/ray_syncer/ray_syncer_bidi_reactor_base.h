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
#include "ray/common/ray_syncer/common.h"
#include "ray/common/ray_syncer/ray_syncer_bidi_reactor.h"
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
      std::function<void(std::shared_ptr<MergedRaySyncMessage>)> message_processor)
      : RaySyncerBidiReactor(std::move(remote_node_id)),
        io_context_(io_context),
        message_processor_(std::move(message_processor)) {}

  bool PushToSendingQueue(std::shared_ptr<MergedRaySyncMessage> message) override {
    if (*IsDisconnected()) {
      return false;
    }

    // Try to filter out the messages the target node already has.
    // Usually it'll be the case when the message is generated from the
    // target node or it's sent from the target node.

    // Create a copy of the message for this reactor to avoid affecting other reactors
    auto sending_message = std::make_shared<MergedRaySyncMessage>();
    sending_message->CopyFrom(*message);

    RAY_LOG(DEBUG) << "Push merged message to sending queue to "
                   << NodeID::FromBinary(GetRemoteNodeID()) << ", batched_messages_size="
                   << sending_message->batched_messages_size();

    // Filter out messages in-place using map structure
    auto *mutable_batched_messages = sending_message->mutable_batched_messages();

    for (auto it = mutable_batched_messages->begin();
         it != mutable_batched_messages->end();) {
      const auto &inner_message = it->second;
      // No need to resend the message sent from a node back.
      if (inner_message.node_id() == GetRemoteNodeID()) {
        // Skip the message when it's about the node of this connection.
        RAY_LOG(DEBUG) << "Remove inner message about self node "
                       << NodeID::FromBinary(GetRemoteNodeID());
        it = mutable_batched_messages->erase(it);
        continue;
      }

      auto &node_versions = GetNodeComponentVersions(inner_message.node_id());
      if (node_versions[inner_message.message_type()] < inner_message.version()) {
        RAY_LOG(DEBUG) << "Queue inner message to send to "
                       << NodeID::FromBinary(GetRemoteNodeID()) << " about node "
                       << NodeID::FromBinary(inner_message.node_id())
                       << ", message_type=" << inner_message.message_type()
                       << ", message_version=" << inner_message.version()
                       << ", local_message_version="
                       << node_versions[inner_message.message_type()];
        node_versions[inner_message.message_type()] = inner_message.version();
        ++it;
      } else {
        RAY_LOG(DEBUG) << "Remove outdated inner message to "
                       << NodeID::FromBinary(GetRemoteNodeID()) << " about node "
                       << NodeID::FromBinary(inner_message.node_id())
                       << " because the message version " << inner_message.version()
                       << " is older than the local version "
                       << node_versions[inner_message.message_type()];
        it = mutable_batched_messages->erase(it);
      }
    }

    if (sending_message->batched_messages_size() == 0) {
      RAY_LOG(DEBUG) << "Skip to send merged message to "
                     << NodeID::FromBinary(GetRemoteNodeID())
                     << " because all messages are filtered out";
      return false;
    }

    sending_buffer_.emplace_back(std::move(sending_message));
    StartSend();
    return true;
  }

  virtual ~RaySyncerBidiReactorBase() = default;

  void StartPull() {
    receiving_message_ = std::make_shared<MergedRaySyncMessage>();
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
  void ReceiveUpdate(std::shared_ptr<MergedRaySyncMessage> message) {
    RAY_CHECK(message->batched_messages_size() > 0);

    RAY_LOG(DEBUG) << "Receive merged message with batched_messages_size="
                   << message->batched_messages_size();

    // Direct update message as no other reactors use this message
    // Filter out outdated messages in-place using map structure
    auto *mutable_batched_messages = message->mutable_batched_messages();

    for (auto it = mutable_batched_messages->begin();
         it != mutable_batched_messages->end();) {
      const auto &inner_message = it->second;
      RAY_CHECK(!inner_message.node_id().empty());
      auto &node_versions = GetNodeComponentVersions(inner_message.node_id());

      if (node_versions[inner_message.message_type()] < inner_message.version()) {
        RAY_LOG(DEBUG) << "Receive inner message from: "
                       << NodeID::FromBinary(inner_message.node_id())
                       << ", message_type=" << inner_message.message_type()
                       << ", message_version=" << inner_message.version()
                       << ", local_version="
                       << node_versions[inner_message.message_type()];
        node_versions[inner_message.message_type()] = inner_message.version();
        ++it;
      } else {
        RAY_LOG_EVERY_MS(WARNING, 1000)
            << "Drop inner message received from "
            << NodeID::FromBinary(inner_message.node_id())
            << " because the message version " << inner_message.version()
            << " is older than the local version "
            << node_versions[inner_message.message_type()]
            << ", message_type=" << inner_message.message_type();
        it = mutable_batched_messages->erase(it);
      }
    }

    if (message->batched_messages_size() == 0) {
      RAY_LOG_EVERY_MS(WARNING, 1000)
          << "Drop the whole merged message received because all inner "
             "messages are filtered out";
      return;
    }
    message_processor_(message);
  }

  void SendNext() {
    sending_ = false;
    StartSend();
  }

  void StartSend() {
    if (sending_) {
      return;
    }

    if (!sending_buffer_.empty()) {
      RAY_LOG(DEBUG) << "Start sending to " << NodeID::FromBinary(GetRemoteNodeID())
                     << ", pending messages: " << sending_buffer_.size();
      auto msg = std::move(sending_buffer_.front());
      sending_buffer_.pop_front();
      Send(std::move(msg), sending_buffer_.empty());
      sending_ = true;
    }
  }

  /// Sending a message to the remote node
  ///
  /// \param message The message to be sent
  /// \param flush Whether to flush the sending queue in gRPC.
  void Send(std::shared_ptr<MergedRaySyncMessage> message, bool flush) {
    sending_message_ = std::move(message);
    grpc::WriteOptions opts;
    if (flush) {
      opts.clear_buffer_hint();
    } else {
      opts.set_buffer_hint();
    }
    RAY_LOG(DEBUG) << "[BidiReactor] Sending message to "
                   << NodeID::FromBinary(GetRemoteNodeID()) << " with flush " << flush;
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
            RAY_LOG_EVERY_MS(INFO, 1000) << "Failed to send the message to: "
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
            RAY_LOG_EVERY_MS(INFO, 1000) << "Failed to read the message from: "
                                         << NodeID::FromBinary(GetRemoteNodeID());
            Disconnect();
            return;
          }

          // Successful rpc completion callback.
          RAY_CHECK(!msg->batched_messages().empty());
          if (on_rpc_completion_) {
            on_rpc_completion_(NodeID::FromBinary(remote_node_id_));
          }
          ReceiveUpdate(std::move(msg));
          StartPull();
        },
        "");
  }

  /// grpc requests for sending and receiving
  std::shared_ptr<MergedRaySyncMessage> sending_message_;
  std::shared_ptr<MergedRaySyncMessage> receiving_message_;

  // For testing
  FRIEND_TEST(RaySyncerTest, RaySyncerBidiReactorBase);
  FRIEND_TEST(RaySyncerTest, RaySyncerBidiReactorBaseMultipleInnerMessages);
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
  const std::function<void(std::shared_ptr<MergedRaySyncMessage>)> message_processor_;

 private:
  /// Buffering all the updates. Sending will be done in an async way.
  std::deque<std::shared_ptr<MergedRaySyncMessage>> sending_buffer_;

  /// Keep track of the versions of components in the remote node.
  /// This field will be updated when messages are received or sent.
  /// We'll filter the received or sent messages when the message is stale.
  absl::flat_hash_map<std::string, std::array<int64_t, kComponentArraySize>>
      node_versions_;

  bool sending_ = false;
};

}  // namespace ray::syncer
