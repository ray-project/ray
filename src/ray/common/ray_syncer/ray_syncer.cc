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

bool NodeState::ConsumeSyncMessage(std::shared_ptr<const RaySyncMessage> message) {
  auto &current = cluster_view_[message->node_id()][message->message_type()];

  RAY_LOG(DEBUG) << "ConsumeSyncMessage: local_version=" << (current ? current->version() : -1)
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

NodeSyncConnection::NodeSyncConnection(
    instrumented_io_context &io_context,
    std::function<void(std::shared_ptr<const RaySyncMessage>)> message_processor,
    std::function<void(const std::string&, bool)> cleanup_cb)
    : io_context_(io_context),
      message_processor_(std::move(message_processor)),
      cleanup_cb_(std::move(cleanup_cb)) {}

void NodeSyncConnection::ReceiveUpdate(std::shared_ptr<const RaySyncMessage> message) {
  auto &node_versions = GetNodeComponentVersions(message->node_id());
  RAY_LOG(DEBUG) << "Receive update: "
                 << " message_type=" << message->message_type()
                 << ", message_version=" << message->version()
                 << ", local_message_version=" << node_versions[message->message_type()];
  if (node_versions[message->message_type()] < message->version()) {
    node_versions[message->message_type()] = message->version();
    message_processor_(message);
  }
}


bool NodeSyncConnection::PushToSendingQueue(
    std::shared_ptr<const RaySyncMessage> message) {
  // Try to filter out the messages the target node already has.
  // Usually it'll be the case when the message is generated from the
  // target node or it's sent from the target node.
  if (message->node_id() == GetRemoteNodeID()) {
    // Skip the message when it's about the node of this connection.
    return false;
  }

  auto &node_versions = GetNodeComponentVersions(message->node_id());
  if (node_versions[message->message_type()] < message->version()) {
    node_versions[message->message_type()] = message->version();
    sending_buffer_[std::make_pair(message->node_id(), message->message_type())] =
        message;
    if(!sending_) {
      StartSend();
    }
    return true;
  }
  return false;
}

void NodeSyncConnection::StartSend() {
  if(sending_buffer_.size() != 0) {
    auto iter = sending_buffer_.begin();
    auto msg = std::move(iter->second);
    sending_buffer_.erase(iter);
    Send(std::move(msg));
    sending_ = true;
  } else if(sending_) {
    Send(nullptr);
    sending_ = false;
  }
}

std::array<int64_t, kComponentArraySize> &NodeSyncConnection::GetNodeComponentVersions(
    const std::string &node_id) {
  auto iter = node_versions_.find(node_id);
  if (iter == node_versions_.end()) {
    iter =
        node_versions_.emplace(node_id, std::array<int64_t, kComponentArraySize>()).first;
    iter->second.fill(-1);
  }
  return iter->second;
}

RayServerBidiReactor::RayServerBidiReactor(
    grpc::CallbackServerContext* server_context,
    instrumented_io_context &io_context,
    const std::string &local_node_id,
    std::function<void(std::shared_ptr<const RaySyncMessage>)> message_processor,
    std::function<void(const std::string&, bool)> cleanup_cb)
    : BidiReactor<ServerBidiReactor>(io_context, std::move(message_processor), std::move(cleanup_cb)),
      server_context_(server_context) {
  const auto &metadata = server_context_->client_metadata();
  auto iter = metadata.find("node_id");
  RAY_CHECK(iter != metadata.end());
  remote_node_id_ = NodeID::FromHex(std::string(iter->second.begin(), iter->second.end())).Binary();

  // Send the local node id to the remote
  server_context_->AddInitialMetadata("node_id", NodeID::FromBinary(local_node_id).Hex());
  StartSendInitialMetadata();

  // Start pulling from remote
  StartPull();
}

void RayServerBidiReactor::OnDone() {
  io_context_.dispatch([this]() {
    cleanup_cb_(GetRemoteNodeID(), false);
    delete this;
  },
    "");
}

RaySyncMessage _empty_msg;

RayClientBidiReactor::RayClientBidiReactor(
    const std::string& remote_node_id,
    const std::string& local_node_id,
    instrumented_io_context &io_context,
    std::function<void(std::shared_ptr<const RaySyncMessage>)> message_processor,
    std::function<void(const std::string&, bool)> cleanup_cb,    
    std::unique_ptr<ray::rpc::syncer::RaySyncer::Stub> stub)
    : BidiReactor<ClientBidiReactor>(io_context, std::move(message_processor), std::move(cleanup_cb)),
      stub_(std::move(stub)),
      client_context_(std::make_unique<grpc::ClientContext>()) {
  client_context_->AddMetadata("node_id", NodeID::FromBinary(local_node_id).Hex());
  stub_->async()->StartSync(client_context_.get(), this);
  remote_node_id_ = remote_node_id;
  StartCall();
  StartPull();
}

void RayClientBidiReactor::Send(std::shared_ptr<const RaySyncMessage> message) {
  if(message != nullptr) {
    grpc::WriteOptions opts;
    opts.set_corked();
    StartWrite(message.get(), opts);
    sending_message_ = std::move(message);
  }
}

void RayClientBidiReactor::OnDone(const grpc::Status& status) {
  io_context_.dispatch([this, status]() {
    cleanup_cb_(GetRemoteNodeID(), !status.ok());
    delete this;
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
}

void RaySyncer::Connect(const std::string& node_id, std::shared_ptr<grpc::Channel> channel) {
  auto stub = ray::rpc::syncer::RaySyncer::NewStub(channel);
  auto reactor = std::make_unique<RayClientBidiReactor>(
      node_id,
      GetLocalNodeID(),
      io_context_,
      [this](auto msg) {
        BroadcastRaySyncMessage(msg);
      },
      [this, channel](const std::string& node_id, bool restart) {
        Disconnect(node_id);
        if(restart) {
          RAY_LOG(INFO) << "Connection is broken. Reconnect to node: " << NodeID::FromHex(node_id);
          Connect(node_id, channel);
        }
      },
      std::move(stub));
  Connect(reactor.release());
}

void RaySyncer::Connect(NodeSyncConnection* connection) {
  io_context_.dispatch(
      [this, connection]() {
        sync_connections_[connection->GetRemoteNodeID()] = connection;
        // Send the view for new connections.
        for (const auto &[_, messages] : node_state_->GetClusterView()) {
          for (auto &message : messages) {
            if (!message) {
              continue;
            }
            connection->PushToSendingQueue(message);
          }
        }
      }, "RaySyncerConnect");
}

void RaySyncer::Disconnect(const std::string &node_id) {
  io_context_.dispatch(
      [this, node_id]() {
        auto iter = sync_connections_.find(node_id);
        if (iter != sync_connections_.end()) {
          sync_connections_.erase(iter);
        }
      },
      "RaySyncerDisconnect");
}

bool RaySyncer::Register(MessageType message_type,
                         const ReporterInterface *reporter,
                         ReceiverInterface *receiver,
                         int64_t pull_from_reporter_interval_ms) {
  std::promise<bool> promise;
  io_context_.dispatch([&] () mutable {
    if (!node_state_->SetComponent(message_type, reporter, receiver)) {
      promise.set_value(false);
      return;
    }

    // Set job to pull from reporter periodically
    if (reporter != nullptr && pull_from_reporter_interval_ms > 0) {
      timer_.RunFnPeriodically(
          [this, stopped = stopped_, message_type]() {
            if(*stopped) {
              return;
            }
            OnDemandBroadcasting(message_type);
          },
          pull_from_reporter_interval_ms);
    }

    RAY_LOG(DEBUG) << "Registered components: "
                   << "message_type:" << message_type << ", reporter:" << reporter
                   << ", receiver:" << receiver
                   << ", pull_from_reporter_interval_ms:" << pull_from_reporter_interval_ms;
    promise.set_value(true);
  },
    "RaySyncerRegister");
  return promise.get_future().get();
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
        RAY_LOG(DEBUG) << "Receive message from: " << NodeID::FromBinary(message->node_id())
                       << " to " << NodeID::FromBinary(GetLocalNodeID());
        if (!node_state_->ConsumeSyncMessage(message)) {
          return;
        }
        for (auto &connection : sync_connections_) {
          connection.second->PushToSendingQueue(message);
        }
      },
      "RaySyncer.BroadcastMessage");
}

ServerBidiReactor* RaySyncerService::StartSync(grpc::CallbackServerContext* context) {
  auto reactor = std::make_unique<RayServerBidiReactor>(
      context,
      syncer_.GetIOContext(),
      syncer_.GetLocalNodeID(),
      [this] (auto msg) mutable {
        syncer_.BroadcastMessage(msg);
      },
      [this] (const std::string& node_id, bool reconnect) mutable {
        // No need to reconnect for server side.
        RAY_CHECK(!reconnect);
        syncer_.Disconnect(node_id);
      });
  syncer_.Connect(reactor.get());
  return reactor.release();
}

RaySyncerService::~RaySyncerService() {}

}  // namespace syncer
}  // namespace ray
