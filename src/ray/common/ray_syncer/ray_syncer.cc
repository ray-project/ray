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

#include "ray/common/asio/asio_util.h"
#include "ray/common/ray_config.h"

namespace ray {
namespace syncer {

NodeState::NodeState() { sync_message_versions_taken_.fill(-1); }

bool NodeState::SetComponent(MessageType message_type,
                             const ReporterInterface *reporter,
                             ReceiverInterface *receiver) {
  if (message_type < static_cast<MessageType>(kComponentArraySize) &&
      reporters_[message_type] == nullptr && receivers_[message_type] == nullptr) {
    reporters_[message_type] = reporter;
    receivers_[message_type] = receiver;
    return true;
  } else {
    RAY_LOG(FATAL) << "Fail to set components, message_type:" << message_type
                   << ", reporter:" << reporter << ", receiver:" << receiver;
    return false;
  }
}

std::optional<RaySyncMessage> NodeState::CreateSyncMessage(MessageType message_type) {
  if (reporters_[message_type] == nullptr) {
    return std::nullopt;
  }
  auto message = reporters_[message_type]->CreateSyncMessage(
      sync_message_versions_taken_[message_type], message_type);
  if (message != std::nullopt) {
    sync_message_versions_taken_[message_type] = message->version();
    RAY_LOG(DEBUG) << "Sync message taken: message_type:" << message_type
                   << ", version:" << message->version()
                   << ", node:" << NodeID::FromBinary(message->node_id());
  }
  return message;
}

bool NodeState::RemoveNode(const std::string &node_id) {
  return cluster_view_.erase(node_id) != 0;
}

bool NodeState::ConsumeSyncMessage(std::shared_ptr<const RaySyncMessage> message) {
  auto &current = cluster_view_[message->node_id()][message->message_type()];

  RAY_LOG(DEBUG) << "ConsumeSyncMessage: local_version="
                 << (current ? current->version() : -1)
                 << " message_version=" << message->version()
                 << ", message_from=" << NodeID::FromBinary(message->node_id());
  // Check whether newer version of this message has been received.
  if (current && current->version() >= message->version()) {
    return false;
  }

  current = message;
  auto receiver = receivers_[message->message_type()];
  if (receiver != nullptr) {
    RAY_LOG(DEBUG) << "Consume message from: " << NodeID::FromBinary(message->node_id());
    receiver->ConsumeSyncMessage(message);
  }
  return true;
}

namespace {

std::string GetNodeIDFromServerContext(grpc::CallbackServerContext *server_context) {
  const auto &metadata = server_context->client_metadata();
  auto iter = metadata.find("node_id");
  RAY_CHECK(iter != metadata.end());
  return NodeID::FromHex(std::string(iter->second.begin(), iter->second.end())).Binary();
}

}  // namespace

RayServerBidiReactor::RayServerBidiReactor(
    grpc::CallbackServerContext *server_context,
    instrumented_io_context &io_context,
    const std::string &local_node_id,
    std::function<void(std::shared_ptr<const RaySyncMessage>)> message_processor,
    std::function<void(const std::string &, bool)> cleanup_cb)
    : RaySyncerBidiReactorBase<ServerBidiReactor>(
          io_context,
          GetNodeIDFromServerContext(server_context),
          std::move(message_processor)),
      cleanup_cb_(std::move(cleanup_cb)),
      server_context_(server_context) {
  // Send the local node id to the remote
  server_context_->AddInitialMetadata("node_id", NodeID::FromBinary(local_node_id).Hex());
  StartSendInitialMetadata();

  // Start pulling from remote
  StartPull();
}

void RayServerBidiReactor::DoDisconnect() {
  io_context_.dispatch([this]() { Finish(grpc::Status::OK); }, "");
}

void RayServerBidiReactor::OnCancel() {
  io_context_.dispatch([this]() { Disconnect(); }, "");
}

void RayServerBidiReactor::OnDone() {
  io_context_.dispatch(
      [this, cleanup_cb = cleanup_cb_, remote_node_id = GetRemoteNodeID()]() {
        cleanup_cb(remote_node_id, false);
        delete this;
      },
      "");
}

RayClientBidiReactor::RayClientBidiReactor(
    const std::string &remote_node_id,
    const std::string &local_node_id,
    instrumented_io_context &io_context,
    std::function<void(std::shared_ptr<const RaySyncMessage>)> message_processor,
    std::function<void(const std::string &, bool)> cleanup_cb,
    std::unique_ptr<ray::rpc::syncer::RaySyncer::Stub> stub)
    : RaySyncerBidiReactorBase<ClientBidiReactor>(
          io_context, remote_node_id, std::move(message_processor)),
      cleanup_cb_(std::move(cleanup_cb)),
      stub_(std::move(stub)) {
  client_context_.AddMetadata("node_id", NodeID::FromBinary(local_node_id).Hex());
  stub_->async()->StartSync(&client_context_, this);
  // Prevent this call from being terminated.
  // Check https://github.com/grpc/proposal/blob/master/L67-cpp-callback-api.md
  // for details.
  AddHold();
  StartPull();
}

void RayClientBidiReactor::OnDone(const grpc::Status &status) {
  io_context_.dispatch(
      [this, status]() {
        cleanup_cb_(GetRemoteNodeID(), !status.ok());
        delete this;
      },
      "");
}

void RayClientBidiReactor::DoDisconnect() {
  io_context_.dispatch(
      [this]() {
        StartWritesDone();
        // Free the hold to allow OnDone being called.
        RemoveHold();
      },
      "");
}

RaySyncer::RaySyncer(instrumented_io_context &io_context,
                     const std::string &local_node_id)
    : io_context_(io_context),
      local_node_id_(local_node_id),
      node_state_(std::make_unique<NodeState>()),
      timer_(io_context) {
  stopped_ = std::make_shared<bool>(false);
}

RaySyncer::~RaySyncer() {
  *stopped_ = true;
  boost::asio::dispatch(io_context_.get_executor(), [reactors = sync_reactors_]() {
    for (auto [_, reactor] : reactors) {
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
    for (auto [node_id, _] : sync_reactors_) {
      nodes.push_back(node_id);
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
        auto reactor = new RayClientBidiReactor(
            /* remote_node_id */ node_id,
            /* local_node_id */ GetLocalNodeID(),
            /* io_context */ io_context_,
            /* message_processor */ [this](auto msg) { BroadcastRaySyncMessage(msg); },
            /* cleanup_cb */
            [this, channel](const std::string &node_id, bool restart) {
              sync_reactors_.erase(node_id);
              if (restart) {
                execute_after(
                    io_context_,
                    [this, node_id, channel]() {
                      RAY_LOG(INFO) << "Connection is broken. Reconnect to node: "
                                    << NodeID::FromBinary(node_id);
                      Connect(node_id, channel);
                    },
                    /* delay_microseconds = */ std::chrono::milliseconds(2000));
              } else {
                node_state_->RemoveNode(node_id);
              }
            },
            /* stub */ std::move(stub));
        Connect(reactor);
        reactor->StartCall();
      }))
      .get();
}

void RaySyncer::Connect(RaySyncerBidiReactor *reactor) {
  boost::asio::dispatch(
      io_context_.get_executor(), std::packaged_task<void()>([this, reactor]() {
        RAY_CHECK(sync_reactors_.find(reactor->GetRemoteNodeID()) ==
                  sync_reactors_.end());
        sync_reactors_[reactor->GetRemoteNodeID()] = reactor;
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
    if (iter != sync_reactors_.end()) {
      sync_reactors_.erase(iter);
    }
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
          timer_.RunFnPeriodically(
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

void RaySyncer::BroadcastRaySyncMessage(std::shared_ptr<const RaySyncMessage> message) {
  BroadcastMessage(std::move(message));
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
      [this](auto msg) mutable { syncer_.BroadcastMessage(msg); },
      [this](const std::string &node_id, bool reconnect) mutable {
        // No need to reconnect for server side.
        RAY_CHECK(!reconnect);
        syncer_.sync_reactors_.erase(node_id);
        syncer_.node_state_->RemoveNode(node_id);
      });
  RAY_LOG(DEBUG) << "Get connection from "
                 << NodeID::FromBinary(reactor->GetRemoteNodeID()) << " to "
                 << NodeID::FromBinary(syncer_.GetLocalNodeID());
  syncer_.Connect(reactor);
  return reactor;
}

RaySyncerService::~RaySyncerService() {}

}  // namespace syncer
}  // namespace ray
