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

#include "ray/common/ray_config.h"
namespace ray {
namespace syncer {

NodeState::NodeState() { snapshots_taken_.fill(-1); }

bool NodeState::SetComponents(RayComponentId cid,
                              const ReporterInterface *reporter,
                              ReceiverInterface *receiver) {
  if (cid < static_cast<RayComponentId>(kComponentArraySize) &&
      reporters_[cid] == nullptr && receivers_[cid] == nullptr) {
    reporters_[cid] = reporter;
    receivers_[cid] = receiver;
    return true;
  } else {
    RAY_LOG(ERROR) << "Fail to set components, component_id:" << cid
                   << ", reporter:" << reporter << ", receiver:" << receiver;
    return false;
  }
}

std::optional<RaySyncMessage> NodeState::GetSnapshot(RayComponentId cid) {
  if (reporters_[cid] == nullptr) {
    return std::nullopt;
  }
  auto message = reporters_[cid]->Snapshot(snapshots_taken_[cid], cid);
  if (message != std::nullopt) {
    snapshots_taken_[cid] = message->version();
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

NodeSyncConnection::NodeSyncConnection(RaySyncer &instance,
                                       instrumented_io_context &io_context,
                                       std::string node_id)
    : timer_(io_context),
      instance_(instance),
      io_context_(io_context),
      node_id_(std::move(node_id)) {}

bool NodeSyncConnection::PushToSendingQueue(
    std::shared_ptr<const RaySyncMessage> message) {
  if (message->node_id() == GetNodeId()) {
    // Skip the message when it's about the node of this connection.
    return false;
  }
  auto &node_versions = GetNodeComponentVersions(message->node_id());
  if (node_versions[message->component_id()] < message->version()) {
    RAY_LOG(DEBUG) << "PushToSendingQueue: " << NodeID::FromBinary(instance_.GetNodeId())
                   << "<--" << NodeID::FromBinary(node_id_) << "\t"
                   << message->component_id() << "\t"
                   << node_versions[message->component_id()] << "\t"
                   << message->version();
    sending_queue_.insert(message);
    node_versions[message->component_id()] = message->version();
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

ClientSyncConnection::ClientSyncConnection(RaySyncer &instance,
                                           instrumented_io_context &io_context,
                                           const std::string &node_id,
                                           std::shared_ptr<grpc::Channel> channel)
    : NodeSyncConnection(instance, io_context, node_id),
      stub_(ray::rpc::syncer::RaySyncer::NewStub(channel)) {
  timer_.RunFnPeriodically(
      [this]() { DoSend(); },
      RayConfig::instance().raylet_report_resources_period_milliseconds());

  StartLongPolling();
}

void ClientSyncConnection::StartLongPolling() {
  // This will be a long-polling request. The node will only reply if
  //    1. there is a new version of message
  //    2. and it has passed X ms since last update.
  auto client_context = std::make_shared<grpc::ClientContext>();
  RAY_LOG(DEBUG) << "Start long pulling from " << NodeID::FromBinary(GetNodeId());
  stub_->async()->LongPolling(
      client_context.get(),
      &dummy_,
      &in_message_,
      [this, client_context](grpc::Status status) {
        if (status.ok()) {
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
  if (sending_queue_.empty()) {
    return;
  }

  auto client_context = std::make_shared<grpc::ClientContext>();
  auto arena = std::make_shared<google::protobuf::Arena>();
  auto request = google::protobuf::Arena::CreateMessage<RaySyncMessages>(arena.get());
  auto response = google::protobuf::Arena::CreateMessage<DummyResponse>(arena.get());

  std::vector<std::shared_ptr<const RaySyncMessage>> holder;

  size_t message_bytes = 0;
  auto iter = sending_queue_.begin();
  while (message_bytes < RayConfig::instance().max_sync_message_batch_bytes() &&
         iter != sending_queue_.end()) {
    message_bytes += (*iter)->sync_message().size();
    // TODO (iycheng): Use arena allocator for optimization
    request->mutable_sync_messages()->UnsafeArenaAddAllocated(
        const_cast<RaySyncMessage *>(iter->get()));
    holder.push_back(*iter);
    sending_queue_.erase(iter++);
  }
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

ServerSyncConnection::ServerSyncConnection(RaySyncer &instance,
                                           instrumented_io_context &io_context,
                                           const std::string &node_id)
    : NodeSyncConnection(instance, io_context, node_id) {
  timer_.RunFnPeriodically(
      [this]() { DoSend(); },
      RayConfig::instance().raylet_report_resources_period_milliseconds());
}

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
  if (unary_reactor_ == nullptr || sending_queue_.empty()) {
    return;
  }
  RAY_CHECK(unary_reactor_ != nullptr && response_ != nullptr);

  size_t message_bytes = 0;
  auto iter = sending_queue_.begin();
  while (message_bytes < RayConfig::instance().max_sync_message_batch_bytes() &&
         iter != sending_queue_.end()) {
    message_bytes += (*iter)->sync_message().size();
    // TODO (iycheng): Use arena allocator for optimization
    response_->add_sync_messages()->CopyFrom(**iter);
    sending_queue_.erase(iter++);
  }

  if (response_->sync_messages_size() != 0) {
    unary_reactor_->Finish(grpc::Status::OK);
    unary_reactor_ = nullptr;
    response_ = nullptr;
  }
}

RaySyncer::RaySyncer(instrumented_io_context &io_context, const std::string &node_id)
    : io_context_(io_context),
      node_id_(node_id),
      node_state_(std::make_unique<NodeState>()),
      timer_(io_context) {
  component_broadcast_.fill(true);
}

RaySyncer::~RaySyncer() {}

void RaySyncer::Connect(std::shared_ptr<grpc::Channel> channel) {
  auto stub = ray::rpc::syncer::RaySyncer::NewStub(channel);
  auto request = std::make_shared<StartSyncRequest>();
  request->set_node_id(node_id_);
  auto response = std::make_shared<StartSyncResponse>();

  auto client_context = std::make_shared<grpc::ClientContext>();
  stub->async()->StartSync(
      client_context.get(),
      request.get(),
      response.get(),
      [this, channel, request, response, client_context](grpc::Status status) {
        io_context_.post(
            [this, channel, response]() {
              RAY_LOG(DEBUG) << "Connect to  " << NodeID::FromBinary(response->node_id());
              auto connection = std::make_unique<ClientSyncConnection>(
                  *this, io_context_, response->node_id(), channel);
              Connect(std::move(connection));
            },
            "StartSyncCallback");
      });
}

void RaySyncer::Connect(std::unique_ptr<NodeSyncConnection> connection) {
  RAY_CHECK(connection != nullptr);
  RAY_CHECK(sync_connections_[connection->GetNodeId()] == nullptr);
  auto &conn = *connection;
  sync_connections_[connection->GetNodeId()] = std::move(connection);
  for (const auto &[_, messages] : node_state_->GetClusterView()) {
    for (auto &message : messages) {
      if (message != nullptr) {
        RAY_CHECK(conn.PushToSendingQueue(message));
      }
    }
  }
}

void RaySyncer::Disconnect(const std::string &node_id) {
  io_context_.post([this, node_id]() { sync_connections_.erase(node_id); },
                   "RaySyncerDisconnect");
}

bool RaySyncer::Register(RayComponentId component_id,
                         const ReporterInterface *reporter,
                         ReceiverInterface *receiver,
                         int64_t pull_from_reporter_interval_ms) {
  if (!node_state_->SetComponents(component_id, reporter, receiver)) {
    return false;
  }

  // Set job to pull from reporter periodically
  if (reporter != nullptr) {
    RAY_CHECK(pull_from_reporter_interval_ms > 0);
    timer_.RunFnPeriodically(
        [this, component_id]() {
          auto snapshot = node_state_->GetSnapshot(component_id);
          if (snapshot) {
            RAY_CHECK(snapshot->node_id() == GetNodeId());
            BroadcastMessage(std::make_shared<RaySyncMessage>(std::move(*snapshot)));
          }
        },
        pull_from_reporter_interval_ms);
  }

  if (receiver != nullptr) {
    component_broadcast_[component_id] = receiver->NeedBroadcast();
  }
  RAY_LOG(DEBUG) << "Registered components: "
                 << "component_id:" << component_id << ", reporter:" << reporter
                 << ", receiver:" << receiver
                 << ", pull_from_reporter_interval_ms:" << pull_from_reporter_interval_ms
                 << ", need_broadcast:" << component_broadcast_[component_id];
  return true;
}

void RaySyncer::BroadcastMessage(std::shared_ptr<const RaySyncMessage> message) {
  // The message is stale. Just skip this one.
  if (!node_state_->ConsumeMessage(message)) {
    return;
  }

  if (component_broadcast_[message->component_id()]) {
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
  RAY_CHECK(node_id_.empty());
  node_id_ = request->node_id();
  RAY_LOG(DEBUG) << "Get connect from: " << NodeID::FromBinary(node_id_);
  syncer_.GetIOContext().post(
      [this, response, reactor]() {
        syncer_.Connect(std::make_unique<ServerSyncConnection>(
            syncer_, syncer_.GetIOContext(), node_id_));
        response->set_node_id(syncer_.GetNodeId());
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
        auto *sync_connection =
            dynamic_cast<ServerSyncConnection *>(syncer_.GetSyncConnection(node_id_));
        if (sync_connection != nullptr) {
          sync_connection->ReceiveUpdate(std::move(request));
        } else {
          RAY_LOG(ERROR) << "Fail to get the sync context";
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
        auto *sync_connection =
            dynamic_cast<ServerSyncConnection *>(syncer_.GetSyncConnection(node_id_));
        if (sync_connection != nullptr) {
          sync_connection->HandleLongPollingRequest(reactor, response);
        } else {
          RAY_LOG(ERROR) << "Fail to setup long-polling";
          reactor->Finish(grpc::Status::OK);
        }
      },
      "SyncLongPolling");
  return reactor;
}

}  // namespace syncer
}  // namespace ray
