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

#include "ray/ray_syncer/ray_syncer.h"

#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "ray/common/asio/asio_util.h"
#include "ray/common/ray_config.h"
#include "ray/ray_syncer/node_state.h"
#include "ray/ray_syncer/ray_syncer_client.h"
#include "ray/ray_syncer/ray_syncer_server.h"

namespace ray::syncer {

RaySyncer::RaySyncer(instrumented_io_context &io_context,
                     const std::string &local_node_id,
                     RpcCompletionCallback on_rpc_completion)
    : io_context_(io_context),
      local_node_id_(local_node_id),
      node_state_(std::make_unique<NodeState>()),
      timer_(PeriodicalRunner::Create(io_context)),
      on_rpc_completion_(std::move(on_rpc_completion)) {
  stopped_ = std::make_shared<bool>(false);
}

RaySyncer::~RaySyncer() {
  *stopped_ = true;
  boost::asio::dispatch(io_context_.get_executor(), [reactors = sync_reactors_]() {
    for (auto &[_, reactor] : reactors) {
      reactor->Disconnect();
    }
  });
}

std::shared_ptr<const RaySyncMessage> RaySyncer::GetSyncMessage(
    const std::string &node_id, MessageType message_type) const {
  auto task = std::packaged_task<std::shared_ptr<const RaySyncMessage>()>(
      [this, &node_id, message_type]() -> std::shared_ptr<const RaySyncMessage> {
        auto &view = node_state_->GetClusterView();
        if (auto iter = view.find(node_id); iter != view.end()) {
          return iter->second[message_type];
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
        for (const auto &[_, messages] : node_state_->GetClusterView()) {
          for (const auto &message : messages) {
            if (!message) {
              continue;
            }
            RAY_LOG(DEBUG) << "Push init view from: "
                           << NodeID::FromBinary(GetLocalNodeID()) << " to "
                           << NodeID::FromBinary(reactor->GetRemoteNodeID()) << " about "
                           << NodeID::FromBinary(message->node_id());
            reactor->PushToSendingQueue(message);
          }
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
    RAY_CHECK(msg->node_id() == GetLocalNodeID());
    BroadcastMessage(std::make_shared<RaySyncMessage>(std::move(*msg)));
    return true;
  }
  return false;
}

void RaySyncer::BroadcastMessage(std::shared_ptr<const RaySyncMessage> message) {
  io_context_.dispatch(
      [this, message] {
        // The message is stale. Just skip this one.
        RAY_LOG(DEBUG) << "Receive message from: "
                       << NodeID::FromBinary(message->node_id()) << " to "
                       << NodeID::FromBinary(GetLocalNodeID());
        if (!node_state_->ConsumeSyncMessage(message)) {
          return;
        }
        for (auto &reactor : sync_reactors_) {
          reactor.second->PushToSendingQueue(message);
        }
      },
      "RaySyncer.BroadcastMessage");
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
