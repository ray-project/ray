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

#include <deque>
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
  /// \param batching_enabled Whether to enable batching.
  /// \param batch_size The maximum number of messages in a batch.
  /// \param batch_delay_ms The maximum delay time to wait before sending a batch.
  RaySyncerBidiReactorBase(
      instrumented_io_context &io_context,
      std::string remote_node_id,
      std::function<void(std::shared_ptr<const InnerRaySyncMessage>)> message_processor,
      bool batching_enabled = false,
      size_t batch_size = RayConfig::instance().syncer_batch_size(),
      int64_t batch_delay_ms = RayConfig::instance().syncer_batch_delay_ms())
      : RaySyncerBidiReactor(std::move(remote_node_id)),
        io_context_(io_context),
        message_processor_(std::move(message_processor)),
        batch_size_(batching_enabled ? batch_size : 1),
        batch_delay_ms_(batching_enabled ? std::chrono::milliseconds(batch_delay_ms)
                                         : std::chrono::milliseconds(0)),
        batch_timer_(io_context),
        batch_timer_active_(false) {}

  bool PushToSendingQueue(std::shared_ptr<const InnerRaySyncMessage> message) override {
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
      if (sending_buffer_.size() >= batch_size_ || batch_delay_ms_.count() == 0) {
        // Send immediately if batch size limit is reached or delay is 0
        StartSend();
      } else {
        // Start or restart the batch timer
        if (!batch_timer_active_) {
          batch_timer_active_ = true;
          batch_timer_.expires_after(batch_delay_ms_);
          batch_timer_.async_wait([this](const boost::system::error_code &ec) {
            if (!ec && !*IsDisconnected()) {
              batch_timer_active_ = false;
              StartSend();
            } else if (ec != boost::asio::error::operation_aborted) {
              RAY_LOG(ERROR) << "Batch timer error: " << ec.message();
            }
          });
        }
      }
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
  void ReceiveUpdate(std::shared_ptr<RaySyncMessage> message) {
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
        message_processor_(std::make_shared<InnerRaySyncMessage>(inner_message));
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
      // Cancel any pending batch timer since we're sending now
      if (batch_timer_active_) {
        batch_timer_.cancel();
        batch_timer_active_ = false;
      }

      RAY_LOG(DEBUG) << "Start sending to " << NodeID::FromBinary(GetRemoteNodeID())
                     << ", pending messages: " << sending_buffer_.size();

      // Create a new batched message
      auto merged_message = std::make_shared<RaySyncMessage>();

      // Add all individual messages to the batch
      for (const auto &[key, inner_message] : sending_buffer_) {
        RAY_LOG(DEBUG) << "Adding message version: " << inner_message->version()
                       << " from node: " << NodeID::FromBinary(inner_message->node_id())
                       << " to batched message";
        (*merged_message->mutable_batched_messages())
            [NodeID::FromBinary(inner_message->node_id()).Hex()] = *inner_message;
      }

      RAY_LOG(DEBUG) << "Created batched sync message containing "
                     << merged_message->batched_messages_size() << " messages";

      sending_buffer_.clear();
      Send(std::move(merged_message), true);
      sending_ = true;
    }
  }

  /// Sending a message to the remote node
  ///
  /// \param message The message to be sent
  /// \param flush Whether to flush the sending queue in gRPC.
  void Send(std::shared_ptr<RaySyncMessage> message, bool flush) {
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
            RAY_LOG_EVERY_MS(INFO, 1000) << "Failed to send a message to node: "
                                         << NodeID::FromBinary(GetRemoteNodeID());
            Disconnect();
          }
        },
        "");
  }

  void OnReadDone(bool ok) override {
    io_context_.dispatch(
        [this, ok, msg = std::move(receiving_message_)]() mutable {
          // NOTE: According to the grpc callback streaming api best practices 3.)
          // https://grpc.io/docs/languages/cpp/best_practices/#callback-streaming-api
          // The client must read all incoming data i.e. until OnReadDone(ok = false)
          // happens for OnDone to be called. Hence even if disconnected_ is true, we
          // still need to allow OnReadDone to repeatedly execute until StartReadData has
          // consumed all the data for OnDone to be called.
          if (!ok) {
            RAY_LOG_EVERY_MS(INFO, 1000) << "Failed to read a message from node: "
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
  std::shared_ptr<RaySyncMessage> sending_message_;
  std::shared_ptr<RaySyncMessage> receiving_message_;

  // For testing
  FRIEND_TEST(RaySyncerTest, RaySyncerBidiReactorBase);
  FRIEND_TEST(RaySyncerTest, RaySyncerBidiReactorBaseBatching);
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
  const std::function<void(std::shared_ptr<const InnerRaySyncMessage>)>
      message_processor_;

 private:
  /// Buffering all the updates. Sending will be done in an async way.
  absl::flat_hash_map<std::pair<std::string, MessageType>,
                      std::shared_ptr<const InnerRaySyncMessage>>
      sending_buffer_;

  /// Keep track of the versions of components in the remote node.
  /// This field will be updated when messages are received or sent.
  /// We'll filter the received or sent messages when the message is stale.
  absl::flat_hash_map<std::string, std::array<int64_t, kComponentArraySize>>
      node_versions_;

  bool sending_ = false;

  /// Batch configuration
  const size_t batch_size_;
  const std::chrono::milliseconds batch_delay_ms_;

  /// Batch timer for delayed sending
  boost::asio::steady_timer batch_timer_;
  bool batch_timer_active_ = false;
};

}  // namespace ray::syncer
