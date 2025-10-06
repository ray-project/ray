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

#include "ray/common/ray_syncer/ray_syncer.h"

#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "ray/common/asio/asio_util.h"
#include "ray/common/ray_config.h"
#include "ray/common/ray_syncer/node_state.h"
#include "ray/common/ray_syncer/ray_syncer_client.h"
#include "ray/common/ray_syncer/ray_syncer_server.h"

namespace ray::syncer {

RaySyncer::RaySyncer(instrumented_io_context &io_context,
                     const std::string &local_node_id,
                     RpcCompletionCallback on_rpc_completion,
                     bool batching_enabled)
    : io_context_(io_context),
      local_node_id_(local_node_id),
      node_state_(std::make_unique<NodeState>()),
      timer_(PeriodicalRunner::Create(io_context)),
      on_rpc_completion_(std::move(on_rpc_completion)),
      batching_enabled_(batching_enabled)  {
  stopped_ = std::make_shared<bool>(false);
  if (batching_enabled_) {
    batch_size_ = static_cast<size_t>(RayConfig::instance().syncer_batch_size());
    batch_timeout_ =
        std::chrono::milliseconds(RayConfig::instance().syncer_batch_timeout_ms());
  } else {
    batch_size_ = 1;
    batch_timeout_ = std::chrono::milliseconds(0);
  }
}

RaySyncer::~RaySyncer() {
  *stopped_ = true;

  // Cancel batch timer and flush any pending messages
  FlushResourceViewBatch();
  boost::asio::dispatch(io_context_.get_executor(), [reactors = sync_reactors_]() {
    for (auto &[_, reactor] : reactors) {
      reactor->Disconnect();
    }
  });
}

std::shared_ptr<RaySyncMessage> RaySyncer::GetSyncMessage(
    const std::string &node_id, MessageType message_type) const {
  auto task = std::packaged_task<std::shared_ptr<RaySyncMessage>()>(
      [this, &node_id, message_type]() -> std::shared_ptr<RaySyncMessage> {
        auto &view = node_state_->GetClusterView();
        if (auto iter = view.find(node_id); iter != view.end()) {
          absl::flat_hash_map<
              std::string,
              std::shared_ptr<const ray::rpc::syncer::InnerRaySyncMessage>>
              messages;
          messages.emplace(NodeID::FromBinary(node_id).Hex(), iter->second[message_type]);
          return MergeResourceViewMessages(messages);
        }
        return nullptr;
      });

  return boost::asio::dispatch(io_context_.get_executor(), std::move(task)).get();
}

std::vector<std::string> RaySyncer::GetAllConnectedNodeIDs() const {
  auto task = std::packaged_task<std::vector<std::string>()>([&]() {
    std::vector<std::string> nodes;
    nodes.reserve(sync_reactors_.size());
    for (auto [node_id, _] : sync_reactors_) {
      nodes.emplace_back(std::move(node_id));
    }
    return nodes;
  });
  return boost::asio::dispatch(io_context_.get_executor(), std::move(task)).get();
}

void RaySyncer::Connect(const std::string &node_id,
                        std::shared_ptr<grpc::Channel> channel) {
  boost::asio::dispatch(
      io_context_.get_executor(), std::packaged_task<void()>([=]() {
        auto stub = ray::rpc::syncer::RaySyncer::NewStub(channel);
        auto *reactor = new RayClientBidiReactor(
            /* remote_node_id */ node_id,
            /* local_node_id */ GetLocalNodeID(),
            /* io_context */ io_context_,
            /* message_processor */
            [this](auto msg) { BroadcastMessage(std::move(msg)); },
            /* cleanup_cb */
            [this, channel](RaySyncerBidiReactor *bidi_reactor, bool restart) {
              const std::string &remote_node_id = bidi_reactor->GetRemoteNodeID();
              auto iter = sync_reactors_.find(remote_node_id);
              if (iter != sync_reactors_.end()) {
                if (iter->second != bidi_reactor) {
                  // The client is already reconnected.
                  return;
                }
                sync_reactors_.erase(iter);
              }
              if (restart) {
                execute_after(
                    io_context_,
                    [this, remote_node_id, channel]() {
                      RAY_LOG(INFO).WithField(NodeID::FromBinary(remote_node_id))
                          << "Connection is broken. Reconnect to node.";
                      Connect(remote_node_id, channel);
                    },
                    /* delay_microseconds = */ std::chrono::milliseconds(2000));
              } else {
                node_state_->RemoveNode(remote_node_id);
              }
            },
            /* stub */ std::move(stub));
        Connect(reactor);
        reactor->StartCall();
      }))
      .get();
}

void RaySyncer::Connect(RaySyncerBidiReactor *reactor) {
  // Bind rpc completion callback.
  if (on_rpc_completion_) {
    reactor->SetRpcCompletionCallbackForOnce(on_rpc_completion_);
  }

  boost::asio::dispatch(
      io_context_.get_executor(), std::packaged_task<void()>([this, reactor]() {
        auto is_new = sync_reactors_.emplace(reactor->GetRemoteNodeID(), reactor).second;
        RAY_CHECK(is_new) << NodeID::FromBinary(reactor->GetRemoteNodeID())
                          << " has already registered.";
        // Send the view for new connections.
        absl::flat_hash_map<std::string, std::shared_ptr<const InnerRaySyncMessage>>
            resource_view_messages;
        absl::flat_hash_map<std::string, std::shared_ptr<const InnerRaySyncMessage>>
            commands_messages;
        for (const auto &[_, messages] : node_state_->GetClusterView()) {
          for (const auto &message : messages) {
            if (!message) {
              continue;
            }
            RAY_LOG(DEBUG) << "Push init view from: "
                           << NodeID::FromBinary(GetLocalNodeID()) << " to "
                           << NodeID::FromBinary(reactor->GetRemoteNodeID()) << " about "
                           << NodeID::FromBinary(message->node_id());
            if (message->message_type() == MessageType::RESOURCE_VIEW) {
              resource_view_messages[NodeID::FromBinary(message->node_id()).Hex()] =
                  message;
            } else if (message->message_type() == MessageType::COMMANDS) {
              commands_messages[NodeID::FromBinary(message->node_id()).Hex()] = message;
            }
          }
        }

        if (!resource_view_messages.empty()) {
          std::shared_ptr<RaySyncMessage> batched_resource_view_message =
              MergeResourceViewMessages(resource_view_messages);
          reactor->PushToSendingQueue(batched_resource_view_message);
        } else if (!commands_messages.empty()) {
          // TODO: are we suppose to deduplicate command messages? like global gc.
          std::shared_ptr<RaySyncMessage> batched_command_message =
              MergeResourceViewMessages(commands_messages);
          reactor->PushToSendingQueue(batched_command_message);
        }
      }))
      .get();
}

void RaySyncer::Disconnect(const std::string &node_id) {
  auto task = std::packaged_task<void()>([&]() {
    auto iter = sync_reactors_.find(node_id);
    if (iter == sync_reactors_.end()) {
      return;
    }

    auto reactor = iter->second;
    sync_reactors_.erase(iter);
    reactor->Disconnect();
  });
  boost::asio::dispatch(io_context_.get_executor(), std::move(task)).get();
}

void RaySyncer::Register(MessageType message_type,
                         const ReporterInterface *reporter,
                         ReceiverInterface *receiver,
                         int64_t pull_from_reporter_interval_ms) {
  io_context_.dispatch(
      [this, message_type, reporter, receiver, pull_from_reporter_interval_ms]() mutable {
        if (!node_state_->SetComponent(message_type, reporter, receiver)) {
          return;
        }

        // Set job to pull from reporter periodically
        if (reporter != nullptr && pull_from_reporter_interval_ms > 0) {
          timer_->RunFnPeriodically(
              [this, stopped = stopped_, message_type]() {
                if (*stopped) {
                  return;
                }
                OnDemandBroadcasting(message_type);
              },
              pull_from_reporter_interval_ms,
              "RaySyncer.OnDemandBroadcasting");
        }

        RAY_LOG(DEBUG) << "Registered components: "
                       << "message_type:" << message_type << ", reporter:" << reporter
                       << ", receiver:" << receiver << ", pull_from_reporter_interval_ms:"
                       << pull_from_reporter_interval_ms;
      },
      "RaySyncerRegister");
}

bool RaySyncer::OnDemandBroadcasting(MessageType message_type) {
  auto msg = node_state_->CreateSyncMessage(message_type);
  if (msg) {
    RAY_CHECK(msg->batched_messages_size() == 1);
    RAY_CHECK(msg->batched_messages().begin()->second.node_id() == GetLocalNodeID());
    BroadcastMessage(std::make_shared<RaySyncMessage>(std::move(*msg)));
    return true;
  }
  return false;
}

void RaySyncer::BroadcastMessage(std::shared_ptr<RaySyncMessage> message) {
  io_context_.dispatch(
      [this, message] {
        RAY_CHECK(message->batched_messages_size() > 0);
        if (!node_state_->ConsumeSyncMessage(message)) {
          return;
        }
        if (message->message_type() == MessageType::RESOURCE_VIEW) {
          BatchResourceViewMessage(message);
        } else {
          // Directly push to sending queue for other message types.
          // NOTE: the message is still batched with size 1 to keep consistency.
          for (auto &reactor : sync_reactors_) {
            reactor.second->PushToSendingQueue(message);
          }
        }
      },
      "RaySyncer.BroadcastMessage");
}

void RaySyncer::BatchResourceViewMessage(std::shared_ptr<RaySyncMessage> message) {
  // Add message to batch buffer
  RAY_CHECK(message->message_type() == MessageType::RESOURCE_VIEW);
  RAY_CHECK(message->batched_messages_size() > 0);

  for (const auto &[node_id_hex, inner_message] : message->batched_messages()) {
    RAY_CHECK(inner_message.message_type() == MessageType::RESOURCE_VIEW);

    RAY_LOG(DEBUG) << "Batching resource view message version: "
                   << inner_message.version()
                   << " from node: " << NodeID::FromBinary(inner_message.node_id());

    std::string node_id = inner_message.node_id();
    auto it = resource_view_batch_buffer_.find(node_id_hex);
    if (it == resource_view_batch_buffer_.end() ||
        it->second->version() < inner_message.version()) {
      RAY_LOG(DEBUG) << "Updated latest message for node: "
                     << NodeID::FromBinary(inner_message.node_id())
                     << " to version: " << inner_message.version() << " (was: "
                     << (it == resource_view_batch_buffer_.end() ? -1
                                                                 : it->second->version())
                     << ")";
      resource_view_batch_buffer_[node_id_hex] =
          std::make_shared<const InnerRaySyncMessage>(inner_message);

    } else {
      RAY_LOG(DEBUG) << "Skipping older message for node: " << NodeID::FromBinary(node_id)
                     << " version: " << inner_message.version()
                     << " (current latest: " << it->second->version() << ")";
    }
  }

  // Check if buffer size reached the batch size limit
  if (resource_view_batch_buffer_.size() >= batch_size_) {
    // Cancel any pending timer and flush immediately
    RAY_LOG(DEBUG) << "Batch size reached. Flushing resource view messages immediately.";
    if (resource_view_batch_timer_active_) {
      RAY_CHECK(resource_view_batch_timer_ != nullptr);
      resource_view_batch_timer_->cancel();
      resource_view_batch_timer_active_ = false;
    }
    FlushResourceViewBatchInternal();
  } else if (!resource_view_batch_timer_active_) {
    // Start or restart the batch timer for timeout-based flushing
    resource_view_batch_timer_active_ = true;
    resource_view_batch_timer_->expires_after(batch_timeout_);
    resource_view_batch_timer_->async_wait(
        [this, stopped = stopped_](const boost::system::error_code &ec) {
          resource_view_batch_timer_active_ = false;
          if (!ec && !*stopped) {
            RAY_LOG(INFO) << "Batch timeout reached. Flushing resource view messages.";
            FlushResourceViewBatchInternal();
          }
        });
  }
}

void RaySyncer::FlushResourceViewBatch() {
  if (resource_view_batch_timer_active_) {
    resource_view_batch_timer_->cancel();
    resource_view_batch_timer_active_ = false;
  }
  if (!resource_view_batch_buffer_.empty()) {
    FlushResourceViewBatchInternal();
  }
}

void RaySyncer::FlushResourceViewBatchInternal() {
  RAY_CHECK(resource_view_batch_timer_active_ == false);
  RAY_CHECK(resource_view_batch_buffer_.size() > 0);

  RAY_LOG(DEBUG) << "Flushing " << resource_view_batch_buffer_.size()
                 << " resource view messages internally.";
  std::shared_ptr<RaySyncMessage> sending_message =
      MergeResourceViewMessages(resource_view_batch_buffer_);
  RAY_CHECK(sending_message != nullptr);
  for (auto &reactor : sync_reactors_) {
    reactor.second->PushToSendingQueue(sending_message);
  }

  // Clear the buffer
  resource_view_batch_buffer_.clear();
}

std::shared_ptr<RaySyncMessage> RaySyncer::MergeResourceViewMessages(
    absl::flat_hash_map<std::string, std::shared_ptr<const InnerRaySyncMessage>>
        &inner_messages) const {
  // Helper function to validate if a string is ASCII hex
  auto is_ascii_hex = [](const std::string &s) {
    if (s.empty()) return false;
    for (unsigned char c : s) {
      if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')))
        return false;
    }
    return true;
  };

  RAY_CHECK(!inner_messages.empty());

  // Create a new batched message
  auto batched_message = std::make_shared<RaySyncMessage>();

  // Set basic fields from the first message
  batched_message->set_message_type(MessageType::RESOURCE_VIEW);

  // Add all individual messages to the batch
  for (const auto &[node_id_hex, inner_message] : inner_messages) {
    RAY_LOG(DEBUG) << "Adding message version: " << inner_message->version()
                   << " from node: " << NodeID::FromBinary(inner_message->node_id())
                   << " to batched message";
    RAY_CHECK(is_ascii_hex(node_id_hex)) << "bad key len=" << node_id_hex.size();
    (*batched_message->mutable_batched_messages())[node_id_hex] = *inner_message;
  }

  RAY_LOG(DEBUG) << "Created batched resource view message type: "
                 << batched_message->message_type() << " containing "
                 << batched_message->batched_messages_size() << " messages";

  return batched_message;
}

ServerBidiReactor *RaySyncerService::StartSync(grpc::CallbackServerContext *context) {
  auto reactor = new RayServerBidiReactor(
      context,
      syncer_.GetIOContext(),
      syncer_.GetLocalNodeID(),
      /*message_processor=*/[this](auto msg) mutable { syncer_.BroadcastMessage(msg); },
      /*cleanup_cb=*/
      [this](RaySyncerBidiReactor *bidi_reactor, bool reconnect) mutable {
        // No need to reconnect for server side.
        RAY_CHECK(!reconnect);
        const auto &node_id = bidi_reactor->GetRemoteNodeID();
        auto iter = syncer_.sync_reactors_.find(node_id);
        if (iter != syncer_.sync_reactors_.end()) {
          if (iter->second != bidi_reactor) {
            // There is a new connection to the node, no need to clean up.
            // This can happen when there is transient network error and the client
            // reconnects. The sequence of events are:
            // 1. Client reconnects, StartSync is called
            // 2. syncer_.Disconnect is called and the old reactor is removed from
            // sync_reactors_
            // 3. syncer_.Connect is called and the new reactor is added to sync_reactors_
            // 4. OnDone method of the old reactor is called which calls this cleanup_cb_
            return;
          }
          syncer_.sync_reactors_.erase(iter);
        }
        RAY_LOG(INFO).WithField(NodeID::FromBinary(node_id)) << "Connection is broken.";
        syncer_.node_state_->RemoveNode(node_id);
      });
  RAY_LOG(DEBUG).WithField(NodeID::FromBinary(reactor->GetRemoteNodeID()))
      << "Get connection";
  // Disconnect exiting connection if there is any.
  // This can happen when there is transient network error
  // and the client reconnects.
  syncer_.Disconnect(reactor->GetRemoteNodeID());
  syncer_.Connect(reactor);
  return reactor;
}

}  // namespace ray::syncer
