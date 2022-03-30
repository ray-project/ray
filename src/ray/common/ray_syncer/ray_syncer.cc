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

NodeState::NodeState() { snapshots_versions_taken_.fill(-1); }

bool NodeState::SetComponent(RayComponentId cid,
                             const ReporterInterface *reporter,
                             ReceiverInterface *receiver) {
  if (cid < static_cast<RayComponentId>(kComponentArraySize) &&
      reporters_[cid] == nullptr && receivers_[cid] == nullptr) {
    reporters_[cid] = reporter;
    receivers_[cid] = receiver;
    return true;
  } else {
    RAY_LOG(FATAL) << "Fail to set components, component_id:" << cid
                   << ", reporter:" << reporter << ", receiver:" << receiver;
    return false;
  }
}

std::optional<RaySyncMessage> NodeState::GetSnapshot(RayComponentId cid) {
  if (reporters_[cid] == nullptr) {
    return std::nullopt;
  }
  auto message = reporters_[cid]->Snapshot(snapshots_versions_taken_[cid], cid);
  if (message != std::nullopt) {
    snapshots_versions_taken_[cid] = message->version();
    RAY_LOG(DEBUG) << "Snapshot taken: cid:" << cid << ", version:" << message->version()
                   << ", node:" << NodeID::FromBinary(message->node_id());
  }
  return message;
}

bool NodeState::ConsumeMessage(std::shared_ptr<const RaySyncMessage> message) {
  auto &current = cluster_view_[message->node_id()][message->component_id()];

  RAY_LOG(DEBUG) << "ConsumeMessage: " << (current ? current->version() : -1)
                 << " message_version: " << message->version()
                 << ", message_from: " << NodeID::FromBinary(message->node_id());
  // Check whether newer version of this message has been received.
  if (current && current->version() >= message->version()) {
    return false;
  }

  current = message;
  auto receiver = receivers_[message->component_id()];
  if (receiver != nullptr) {
    receiver->Update(message);
  }
  return true;
}

NodeSyncConnection::NodeSyncConnection(
    instrumented_io_context &io_context,
    std::string remote_node_id,
    std::function<void(std::shared_ptr<RaySyncMessage>)> message_processor)
    : io_context_(io_context),
      remote_node_id_(std::move(remote_node_id)),
      message_processor_(std::move(message_processor)) {}

void NodeSyncConnection::ReceiveUpdate(RaySyncMessages messages) {
  for (auto &message : *messages.mutable_sync_messages()) {
    auto &node_versions = GetNodeComponentVersions(message.node_id());
    RAY_LOG(DEBUG) << "Receive update: "
                   << " component_id=" << message.component_id()
                   << ", message_version=" << message.version()
                   << ", local_message_version=" << node_versions[message.component_id()];
    if (node_versions[message.component_id()] < message.version()) {
      node_versions[message.component_id()] = message.version();
      message_processor_(std::make_shared<RaySyncMessage>(std::move(message)));
    }
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
  if (node_versions[message->component_id()] < message->version()) {
    node_versions[message->component_id()] = message->version();
    sending_buffer_[std::make_pair(message->node_id(), message->component_id())] =
        message;
    return true;
  }
  return false;
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

ClientSyncConnection::ClientSyncConnection(
    instrumented_io_context &io_context,
    const std::string &node_id,
    std::function<void(std::shared_ptr<RaySyncMessage>)> message_processor,
    std::shared_ptr<grpc::Channel> channel)
    : NodeSyncConnection(io_context, node_id, std::move(message_processor)),
      stub_(ray::rpc::syncer::RaySyncer::NewStub(channel)) {
  StartLongPolling();
}

void ClientSyncConnection::StartLongPolling() {
  // This will be a long-polling request. The node will only reply if
  //    1. there is a new version of message
  //    2. and it has passed X ms since last update.
  auto client_context = std::make_shared<grpc::ClientContext>();
  stub_->async()->LongPolling(
      client_context.get(),
      &dummy_,
      &in_message_,
      [this, client_context](grpc::Status status) {
        if (status.ok()) {
          RAY_CHECK(in_message_.GetArena() == nullptr);
          io_context_.post(
              [this, messages = std::move(in_message_)]() mutable {
                ReceiveUpdate(std::move(messages));
              },
              "LongPollingCallback");
          in_message_.Clear();
          // Start the next polling.
          StartLongPolling();
        }
      });
}

void ClientSyncConnection::DoSend() {
  if (sending_buffer_.empty()) {
    return;
  }

  auto client_context = std::make_shared<grpc::ClientContext>();
  auto arena = std::make_shared<google::protobuf::Arena>();
  auto request = google::protobuf::Arena::CreateMessage<RaySyncMessages>(arena.get());
  auto response = google::protobuf::Arena::CreateMessage<DummyResponse>(arena.get());

  std::vector<std::shared_ptr<const RaySyncMessage>> holder;

  size_t message_bytes = 0;
  auto iter = sending_buffer_.begin();
  while (message_bytes < RayConfig::instance().max_sync_message_batch_bytes() &&
         iter != sending_buffer_.end()) {
    message_bytes += iter->second->sync_message().size();
    // TODO (iycheng): Use arena allocator for optimization
    request->mutable_sync_messages()->UnsafeArenaAddAllocated(
        const_cast<RaySyncMessage *>(iter->second.get()));
    holder.push_back(iter->second);
    sending_buffer_.erase(iter++);
  }
  if (request->sync_messages_size() != 0) {
    stub_->async()->Update(
        client_context.get(),
        request,
        response,
        [arena, client_context, holder = std::move(holder)](grpc::Status status) {
          if (!status.ok()) {
            RAY_LOG(ERROR) << "Sending request failed because of "
                           << status.error_message();
          }
        });
  }
}

ServerSyncConnection::ServerSyncConnection(
    instrumented_io_context &io_context,
    const std::string &remote_node_id,
    std::function<void(std::shared_ptr<RaySyncMessage>)> message_processor)
    : NodeSyncConnection(io_context, remote_node_id, std::move(message_processor)) {}

ServerSyncConnection::~ServerSyncConnection() {
  // If there is a pending request, we need to cancel it. Otherwise, rpc will
  // hang there forever.
  if (unary_reactor_ != nullptr) {
    unary_reactor_->Finish(grpc::Status::CANCELLED);
  }
}

void ServerSyncConnection::HandleLongPollingRequest(grpc::ServerUnaryReactor *reactor,
                                                    RaySyncMessages *response) {
  RAY_CHECK(response_ == nullptr);
  RAY_CHECK(unary_reactor_ == nullptr);

  unary_reactor_ = reactor;
  response_ = response;
}

void ServerSyncConnection::DoSend() {
  // There is no receive request
  if (unary_reactor_ == nullptr || sending_buffer_.empty()) {
    return;
  }
  RAY_CHECK(unary_reactor_ != nullptr && response_ != nullptr);

  size_t message_bytes = 0;
  auto iter = sending_buffer_.begin();
  while (message_bytes < RayConfig::instance().max_sync_message_batch_bytes() &&
         iter != sending_buffer_.end()) {
    message_bytes += iter->second->sync_message().size();
    // TODO (iycheng): Use arena allocator for optimization
    response_->add_sync_messages()->CopyFrom(*iter->second);
    sending_buffer_.erase(iter++);
  }

  if (response_->sync_messages_size() != 0) {
    unary_reactor_->Finish(grpc::Status::OK);
    unary_reactor_ = nullptr;
    response_ = nullptr;
  }
}

RaySyncer::RaySyncer(instrumented_io_context &io_context,
                     const std::string &local_node_id)
    : io_context_(io_context),
      local_node_id_(local_node_id),
      node_state_(std::make_unique<NodeState>()),
      timer_(io_context) {
  stopped_ = std::make_shared<bool>(false);
  upward_only_.fill(false);
  timer_.RunFnPeriodically(
      [this]() {
        for (auto &[_, sync_connection] : sync_connections_) {
          sync_connection->DoSend();
        }
      },
      RayConfig::instance().raylet_report_resources_period_milliseconds());
}

RaySyncer::~RaySyncer() { *stopped_ = true; }

void RaySyncer::Connect(std::shared_ptr<grpc::Channel> channel) {
  auto stub = ray::rpc::syncer::RaySyncer::NewStub(channel);
  auto request = std::make_shared<StartSyncRequest>();
  request->set_node_id(local_node_id_);
  auto response = std::make_shared<StartSyncResponse>();

  auto client_context = std::make_shared<grpc::ClientContext>();
  stub->async()->StartSync(
      client_context.get(),
      request.get(),
      response.get(),
      [this, channel, request, response, client_context, stopped = this->stopped_](
          grpc::Status status) {
        if (*stopped) {
          return;
        }
        if (status.ok()) {
          io_context_.post(
              [this, channel, response]() {
                auto connection = std::make_unique<ClientSyncConnection>(
                    io_context_,
                    response->node_id(),
                    [this](auto msg) { BroadcastMessage(msg); },
                    channel);
                Connect(std::move(connection));
              },
              "StartSyncCallback");
        }
      });
}

void RaySyncer::Connect(std::unique_ptr<NodeSyncConnection> connection) {
  // Somehow connection=std::move(connection) won't be compiled here.
  // Potentially it might have a leak here if the function is not executed.
  io_context_.dispatch(
      [this, connection = connection.release()]() mutable {
        RAY_CHECK(connection != nullptr);
        RAY_CHECK(sync_connections_[connection->GetRemoteNodeID()] == nullptr);
        auto &conn = *connection;
        bool is_upward_conn = false;
        if (dynamic_cast<ClientSyncConnection *>(connection) == nullptr) {
          upward_connections_.insert(connection);
          is_upward_conn = true;
        }
        sync_connections_[connection->GetRemoteNodeID()].reset(connection);
        for (const auto &[_, messages] : node_state_->GetClusterView()) {
          for (auto &message : messages) {
            if (!message) {
              continue;
            }
            if (upward_only_[message->component_id()] && !is_upward_conn) {
              continue;
            }
            RAY_CHECK(conn.PushToSendingQueue(message));
          }
        }
      },
      "RaySyncer::Connect");
}

void RaySyncer::Disconnect(const std::string &node_id) {
  io_context_.post([this, node_id]() { sync_connections_.erase(node_id); },
                   "RaySyncerDisconnect");
}

bool RaySyncer::Register(RayComponentId component_id,
                         const ReporterInterface *reporter,
                         ReceiverInterface *receiver,
                         bool upward_only,
                         int64_t pull_from_reporter_interval_ms) {
  if (!node_state_->SetComponent(component_id, reporter, receiver)) {
    return false;
  }

  upward_only_[component_id] = upward_only;

  // Set job to pull from reporter periodically
  if (reporter != nullptr) {
    RAY_CHECK(pull_from_reporter_interval_ms > 0);
    timer_.RunFnPeriodically(
        [this, component_id]() {
          auto snapshot = node_state_->GetSnapshot(component_id);
          if (snapshot) {
            RAY_CHECK(snapshot->node_id() == GetLocalNodeID());
            BroadcastMessage(std::make_shared<RaySyncMessage>(std::move(*snapshot)));
          }
        },
        pull_from_reporter_interval_ms);
  }

  RAY_LOG(DEBUG) << "Registered components: "
                 << "component_id:" << component_id << ", reporter:" << reporter
                 << ", receiver:" << receiver
                 << ", pull_from_reporter_interval_ms:" << pull_from_reporter_interval_ms
                 << ", upward_only:" << upward_only_[component_id];
  return true;
}

void RaySyncer::BroadcastMessage(std::shared_ptr<const RaySyncMessage> message) {
  // The message is stale. Just skip this one.
  if (!node_state_->ConsumeMessage(message)) {
    return;
  }

  if (upward_only_[message->component_id()]) {
    for (auto &connection : upward_connections_) {
      connection->PushToSendingQueue(message);
    }
  } else {
    for (auto &connection : sync_connections_) {
      connection.second->PushToSendingQueue(message);
    }
  }
}

grpc::ServerUnaryReactor *RaySyncerService::StartSync(
    grpc::CallbackServerContext *context,
    const StartSyncRequest *request,
    StartSyncResponse *response) {
  auto *reactor = context->DefaultReactor();
  // Make sure server only have one client
  RAY_CHECK(remote_node_id_.empty());
  remote_node_id_ = request->node_id();
  RAY_LOG(DEBUG) << "Get connect from: " << NodeID::FromBinary(remote_node_id_);
  syncer_.GetIOContext().post(
      [this, response, reactor, context]() {
        if (context->IsCancelled()) {
          reactor->Finish(grpc::Status::CANCELLED);
          return;
        }

        syncer_.Connect(std::make_unique<ServerSyncConnection>(
            syncer_.GetIOContext(), remote_node_id_, [this](auto msg) {
              syncer_.BroadcastMessage(msg);
            }));
        response->set_node_id(syncer_.GetLocalNodeID());
        reactor->Finish(grpc::Status::OK);
      },
      "RaySyncer::StartSync");
  return reactor;
}

grpc::ServerUnaryReactor *RaySyncerService::Update(grpc::CallbackServerContext *context,
                                                   const RaySyncMessages *request,
                                                   DummyResponse *) {
  auto *reactor = context->DefaultReactor();
  // Make sure request is allocated from heap so that it can be moved safely.
  RAY_CHECK(request->GetArena() == nullptr);
  syncer_.GetIOContext().post(
      [this, request = std::move(*const_cast<RaySyncMessages *>(request))]() mutable {
        auto *sync_connection = dynamic_cast<ServerSyncConnection *>(
            syncer_.GetSyncConnection(remote_node_id_));
        if (sync_connection != nullptr) {
          sync_connection->ReceiveUpdate(std::move(request));
        } else {
          RAY_LOG(FATAL) << "Fail to get the sync context";
        }
      },
      "SyncerUpdate");
  reactor->Finish(grpc::Status::OK);
  return reactor;
}

grpc::ServerUnaryReactor *RaySyncerService::LongPolling(
    grpc::CallbackServerContext *context,
    const DummyRequest *,
    RaySyncMessages *response) {
  auto *reactor = context->DefaultReactor();
  syncer_.GetIOContext().post(
      [this, reactor, response]() mutable {
        auto *sync_connection = dynamic_cast<ServerSyncConnection *>(
            syncer_.GetSyncConnection(remote_node_id_));
        if (sync_connection != nullptr) {
          sync_connection->HandleLongPollingRequest(reactor, response);
        } else {
          RAY_LOG(ERROR) << "Fail to setup long-polling";
          reactor->Finish(grpc::Status::CANCELLED);
        }
      },
      "SyncLongPolling");
  return reactor;
}

RaySyncerService::~RaySyncerService() { syncer_.Disconnect(remote_node_id_); }

}  // namespace syncer
}  // namespace ray
