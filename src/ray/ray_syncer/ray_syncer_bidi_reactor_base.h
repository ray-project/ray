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
  /// \param max_batch_size The maximum number of messages in a batch.
  /// \param max_batch_delay_ms The maximum delay time to wait before sending a batch.
  RaySyncerBidiReactorBase(
      instrumented_io_context &io_context,
      std::string remote_node_id,
      std::function<void(std::shared_ptr<const RaySyncMessage>)> message_processor,
      size_t max_batch_size,
      uint64_t max_batch_delay_ms)
      : RaySyncerBidiReactor(std::move(remote_node_id)),
        io_context_(io_context),
        message_processor_(std::move(message_processor)),
        max_batch_size_(max_batch_size),
        max_batch_delay_ms_(std::chrono::milliseconds(max_batch_delay_ms)),
        batch_timer_(io_context),
        batch_timer_active_(false) {}

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
    if (node_versions[message->message_type()] >= message->version()) {
      RAY_LOG(INFO) << "Dropping sync message with stale version. latest version: "
                    << node_versions[message->message_type()]
                    << ", dropped message version: " << message->version();
      return false;
    }

    node_versions[message->message_type()] = message->version();
    sending_buffer_[std::make_pair(message->node_id(), message->message_type())] =
        std::move(message);
    // sending_buffer_ size can be greater than max_batch_size_ as previous message batch
    // might be sending in progress, i.e., making sending_ = true.
    if (sending_buffer_.size() >= max_batch_size_ || max_batch_delay_ms_.count() == 0) {
      // Send immediately if batch size limit is reached or delay is 0
      if (batch_timer_active_) {
        batch_timer_.cancel();
        batch_timer_active_ = false;
      }
      StartSend();
    } else {
      // Start or restart the batch timer
      if (!batch_timer_active_) {
        RAY_LOG(DEBUG) << "Batch timer expires after " << max_batch_delay_ms_.count()
                       << " ms";
        batch_timer_active_ = true;
        batch_timer_.expires_after(max_batch_delay_ms_);
        // Use weak_ptr to avoid use-after-free when the reactor is destroyed.
        auto weak_self = std::weak_ptr<RaySyncerBidiReactor>(self_ref_);
        batch_timer_.async_wait([weak_self, this](const boost::system::error_code &ec) {
          auto self = weak_self.lock();
          if (!self) {
            return;
          }
          batch_timer_active_ = false;
          if (!ec && !*IsDisconnected()) {
            StartSend();
          } else if (ec != boost::asio::error::operation_aborted) {
            RAY_LOG(ERROR) << "Batch timer error: " << ec.message();
          }
        });
      }
    }
    return true;
  }

  virtual ~RaySyncerBidiReactorBase() {
    if (batch_timer_active_) {
      batch_timer_.cancel();
    }
  }

  void StartPull() {
    receiving_message_batch_ = std::make_shared<RaySyncMessageBatch>();
    RAY_LOG(DEBUG) << "Start reading: " << NodeID::FromBinary(GetRemoteNodeID());
    StartRead(receiving_message_batch_.get());
  }

 protected:
  /// The io context
  instrumented_io_context &io_context_;

 private:
  /// Handle the updates sent from the remote node.
  ///
  /// \param message_batch The message batch received.
  void ReceiveUpdate(std::shared_ptr<RaySyncMessageBatch> message_batch) {
    RAY_CHECK(message_batch->messages_size() > 0);

    RAY_LOG(DEBUG) << "Receive message batch with messages_size="
                   << message_batch->messages_size();

    for (const auto &message : message_batch->messages()) {
      auto &node_versions = GetNodeComponentVersions(message.node_id());
      RAY_LOG(DEBUG) << "Receive update: "
                     << " message_type=" << message.message_type()
                     << ", message_version=" << message.version()
                     << ", local_message_version="
                     << node_versions[message.message_type()];
      if (node_versions[message.message_type()] < message.version()) {
        node_versions[message.message_type()] = message.version();
        message_processor_(std::make_shared<RaySyncMessage>(message));
      } else {
        RAY_LOG_EVERY_MS(WARNING, 1000)
            << "Drop message received from " << NodeID::FromBinary(message.node_id())
            << " because the message version " << message.version()
            << " is older than the local version "
            << node_versions[message.message_type()]
            << ". Message type: " << message.message_type();
      }
    }
  }

  void SendNext() {
    sending_ = false;
    StartSend();
  }

  void StartSend() {
    if (sending_ || sending_buffer_.empty()) {
      return;
    }

    RAY_LOG(DEBUG) << "Start sending to " << NodeID::FromBinary(GetRemoteNodeID())
                   << ", pending messages: " << sending_buffer_.size();

    // Create a new message batch
    auto message_batch = std::make_shared<RaySyncMessageBatch>();

    // Add all individual messages to the message batch
    for (const auto &[key, message] : sending_buffer_) {
      RAY_LOG(DEBUG) << "Adding message version: " << message->version()
                     << " from node: " << NodeID::FromBinary(message->node_id())
                     << " to message batch";
      *message_batch->add_messages() = *message;
    }

    RAY_LOG(DEBUG) << "Created message batch containing "
                   << message_batch->messages_size() << " messages";

    sending_buffer_.clear();
    Send(std::move(message_batch));
    sending_ = true;
  }

  /// Sending a message to the remote node
  ///
  /// \param message The message batch to be sent
  void Send(std::shared_ptr<RaySyncMessageBatch> message_batch) {
    sending_message_batch_ = std::move(message_batch);

    RAY_LOG(DEBUG) << "[BidiReactor] Sending message batch to "
                   << NodeID::FromBinary(GetRemoteNodeID()) << "with message count "
                   << sending_message_batch_->messages_size();
    StartWrite(sending_message_batch_.get());
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
        [this, ok, msg_batch = std::move(receiving_message_batch_)]() mutable {
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
          RAY_CHECK(!msg_batch->messages().empty());
          if (on_rpc_completion_) {
            on_rpc_completion_(NodeID::FromBinary(remote_node_id_));
          }
          ReceiveUpdate(std::move(msg_batch));
          StartPull();
        },
        "");
  }

  /// grpc requests for sending and receiving
  std::shared_ptr<const RaySyncMessageBatch> sending_message_batch_;
  std::shared_ptr<RaySyncMessageBatch> receiving_message_batch_;

  // For testing
  FRIEND_TEST(RaySyncerTest, RaySyncerBidiReactorBase);
  FRIEND_TEST(RaySyncerTest, RaySyncerBidiReactorBaseBatchSizeTriggerSend);
  FRIEND_TEST(RaySyncerTest, RaySyncerBidiReactorBaseBatchTimeoutTriggerSend);

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

  /// Batch configuration
  const size_t max_batch_size_;
  const std::chrono::milliseconds max_batch_delay_ms_;

  /// Batch timer for delayed sending
  boost::asio::steady_timer batch_timer_;
  bool batch_timer_active_ = false;
};

}  // namespace ray::syncer
