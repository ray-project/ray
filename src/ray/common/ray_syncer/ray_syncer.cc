#include "ray/common/ray_syncer/ray_syncer.h"

#include "ray/common/ray_config.h"
namespace ray {
namespace syncer {
namespace details {

void NodeStatus::SetComponents(RayComponentId cid,
                               const ReporterInterface *reporter,
                               ReceiverInterface *receiver) {
  RAY_CHECK(cid < kComponentArraySize);
  RAY_CHECK(reporters_[cid] == nullptr);
  RAY_CHECK(receivers_[cid] == nullptr);
  reporters_[cid] = reporter;
  receivers_[cid] = receiver;
}

std::optional<RaySyncMessage> NodeStatus::GetSnapshot(RayComponentId cid) const {
  if (reporters_[cid] == nullptr) {
    return std::nullopt;
  }
  auto message = reporters_[cid]->Snapshot(snapshots_taken_[cid], cid);
  if (message != std::nullopt) {
    snapshots_taken_[cid] = message->version();
  }
  return message;
}

bool NodeStatus::ConsumeMessage(std::shared_ptr<RaySyncMessage> message) {
  auto &current = cluster_view_[message->node_id()][message->component_id()];
  // Check whether newer version of this message has been received.
  if (current && current->version() >= message->version()) {
    return false;
  }

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
      node_id_(std::move(node_id)) {
  timer_.expires_from_now(boost::posix_time::milliseconds(
      RayConfig::instance().raylet_report_resources_period_milliseconds()));
  timer_.async_wait([this](boost::system::error_code) {
    this->DoSend();
    timer_.expires_from_now(boost::posix_time::milliseconds(
        RayConfig::instance().raylet_report_resources_period_milliseconds()));
  });
}

void NodeSyncConnection::PushToSendingQueue(std::shared_ptr<RaySyncMessage> message) {
  auto &node_versions = GetNodeComponentVersions(message->node_id());
  if (node_versions[message->component_id()] < message->version()) {
    sending_queue_.insert(message);
    node_versions[message->component_id()] = message->version();
  }
}

std::array<uint64_t, kComponentArraySize> &NodeSyncConnection::GetNodeComponentVersions(
    const std::string &node_id) {
  auto iter = node_versions_.find(node_id);
  if (iter == node_versions_.end()) {
    iter = node_versions_.emplace(node_id, std::array<uint64_t, kComponentArraySize>({0}))
               .first;
  }
  return iter->second;
}

ClientSyncConnection::ClientSyncConnection(
    RaySyncer &instance,
    instrumented_io_context &io_context,
    std::string node_id,
    std::shared_ptr<ray::rpc::syncer::RaySyncer::Stub> stub)
    : NodeSyncConnection(instance, io_context, std::move(node_id)),
      stub_(std::move(stub)) {
  // Initialize the connection
  start_sync_request_.set_node_id(instance.GetNodeId());
  auto handler = stub->async();
  auto client_context = std::make_shared<grpc::ClientContext>();
  handler->StartSync(client_context.get(),
                     &start_sync_request_,
                     &start_sync_response_,
                     [this](grpc::Status status) mutable {
                       if (status.ok()) {
                         io_context_.post(
                             [this]() {
                               RAY_CHECK(GetNodeId() == start_sync_response_.node_id());
                               connection_created_ = true;
                               StartLongPolling();
                             },
                             "StartSyncCallback");
                       } else {
                         RAY_LOG(ERROR)
                             << "Start sync failed: " << status.error_message();
                         instance_.Disconnect(GetNodeId());
                       }
                     });
}

void RaySyncer::ClientSyncConnection::StartLongPolling() {
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

void RaySyncer::ClientSyncConnection::DoSend() {
  if (sending_queue_.empty()) {
    n return;
  }

  auto client_context = std::make_shared<grpc::ClientContext>();
  auto arena = std::make_shared<google::protobuf::Arena>();
  auto request = google::protobuf::Arena::CreateMessage<RaySyncMessages>(arena.get());
  auto response = google::protobuf::Arena::CreateMessage<DummyResponse>(arena.get());

  std::vector<std::shared_ptr<RaySyncMessage>> holder;

  size_t message_bytes = 0;
  auto iter = sending_queue_.begin();
  while (message_bytes < RayConfig::instance().max_sync_message_batch_bytes() &&
         iter != sending_queue_.end()) {
    message_bytes += (*iter)->sync_message().size();
    // TODO (iycheng): Use arena allocator for optimization
    request->mutable_sync_messages()->UnsafeArenaAddAllocated(iter->get());
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
    : NodeSyncConnection(instance, io_context, node_id) {}

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

  if (message_bytes != 0) {
    unary_reactor_->Finish(grpc::Status::OK);
    unary_reactor_ = nullptr;
    response_ = nullptr;
  }
}
}  // namespace details

RaySyncer::RaySyncer(const std::string &node_id)
    : node_id_(node_id),
      node_status_(std::make_unique<NodeStatus>()),
      timer_(io_context_) {
  syncer_thread_ = std::make_unique<std::thread>([this]() {
    boost::asio::io_service::work work(io_context_);
    io_context_.run();
  });
}

RaySyncer::~RaySyncer() {
  io_context_.stop();
  syncer_thread_->join();
}

void RaySyncer::Connect(std::unique_ptr<NodeSyncConnection> context) {
  RAY_CHECK(sync_connections_[context->GetNodeId()] == nullptr);
  RAY_CHECK(context != nullptr);
  sync_connections_[context->GetNodeId()] = std::move(context);
}

void RaySyncer::Disconnect(const std::string &node_id) {
  sync_connections_.erase(node_id);
}

void RaySyncer::Register(RayComponentId component_id,
                         const ReporterInterface *reporter,
                         ReceiverInterface *receiver,
                         int64_t pull_from_reporter_interval_ms) {
  node_status_->SetComponents(cid, reporter, receiver);

  // Set job to pull from reporter periodically
  if (reporter != nullptr) {
    RAY_CHECK(pull_from_reporter_interval_ms > 0);
    timer_.RunFnPeriodically(
        [this, component_id]() {
          auto snapshot = node_status_->GetSnapshot(component_id);
          if (snapshot) {
            BroadcastMessage(std::make_shared<RaySyncMessage>(std::move(*snapshot)));
          }
        },
        pull_from_reporter_interval_ms);
  }

  if (receiver != nullptr) {
    component_broadcast_[component_id] = receiver->NeedBroadcast();
  }
}

void RaySyncer::BroadcastMessage(std::shared_ptr<RaySyncMessage> message) {
  // The message is stale. Just skip this one.
  if (!node_status_->ConsumeMessage(message)) {
    return;
  }

  if (component_broadcast_[message->component_id()]) {
    for (auto &context : sync_connections_) {
      context.second->PushToSendingQueue(message);
    }
  }
}

grpc::ServerUnaryReactor *RaySyncerService::StartSync(
    grpc::CallbackServerContext *context,
    const StartSyncRequest *request,
    StartSyncResponse *response) override {
  auto *reactor = context->DefaultReactor();
  // Make sure server only have one client
  RAY_CHECK(node_id_.empty());
  node_id_ = request->node_id();
  syncer_.Connect(std::make_unique<ServerSyncConnection>(
      syncer_, syncer_.GetIOContext(), request->node_id()));
  response->set_node_id(syncer_.GetNodeId());
  reactor->Finish(grpc::Status::OK);
  return reactor;
}

grpc::ServerUnaryReactor *RaySyncerService::Update(grpc::CallbackServerContext *context,
                                                   const RaySyncMessages *request,
                                                   DummyResponse *) override {
  auto *reactor = context->DefaultReactor();
  syncer_.GetIOContext().post(
      [this, request = std::move(*const_cast<RaySyncMessages *>(request))]() mutable {
        auto *sync_context =
            dynamic_cast<ServerSyncConnection *>(syncer_.GetSyncConnection(node_id_));
        if (sync_context != nullptr) {
          sync_context->ReceiveUpdate(std::move(request));
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
    RaySyncMessages *response) override {
  auto *reactor = context->DefaultReactor();
  syncer_.GetIOContext().post(
      [this, reactor, response] mutable() {
        auto *sync_context =
            dynamic_cast<ServerSyncConnection *>(syncer_.GetSyncConnection(node_id_));
        if (sync_context != nullptr) {
          sync_context->HandleLongPollingRequest(reactor, response);
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
