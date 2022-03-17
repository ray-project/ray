#include "ray/common/ray_syncer.h"

#include "ray/common/ray_config.h"
namespace ray {
namespace syncer {

RaySyncer::RaySyncer(const std::string &node_id)
    : node_id_(node_id), timer_(io_context_) {
  syncer_thread_ = std::make_unique<std::thread>([this]() {
    boost::asio::io_service::work work(io_context_);
    io_context_.run();
  });
}

RaySyncer::~RaySyncer() { syncer_thread_->join(); }

void RaySyncer::ConnectTo(std::unique_ptr<ray::rpc::syncer::RaySyncer::Stub> stub) {
  SyncMeta request;
  request.set_node_id(node_id_);
  auto handler = stub->async();
  auto client_context = std::make_shared<grpc::ClientContext>();
  auto response = std::make_shared<SyncMeta>();

  handler->StartSync(
      client_context.get(),
      &request,
      response.get(),
      [this,
       response,
       stub = std::shared_ptr<ray::rpc::syncer::RaySyncer::Stub>(std::move(stub))](
          grpc::Status status) {
        if (status.ok()) {
          io_context_.post(
              [this, stub, node_id = response->node_id()]() {
                auto context = std::make_unique<RaySyncer::ClientSyncContext>(
                    *this, io_context_, node_id, stub);
                sync_context_[node_id] = std::move(context);
              },
              "StartSyncCallback");
        } else {
          RAY_LOG(ERROR) << "Start sync failed: " << status.error_message();
        }
      });
}

std::unique_ptr<RaySyncer::ServerSyncContext> RaySyncer::ConnectFrom(
    const std::string &node_id) {
  auto context =
      std::make_unique<RaySyncer::ServerSyncContext>(*this, io_context_, node_id);
  leader_ = context.get();
  return context;
}

void RaySyncer::NodeRemoved(const std::string &node_id) {
  sync_context_.erase(node_id);
  cluster_view_.erase(node_id);
}

void RaySyncer::Register(RayComponentId component_id,
                         const ReporterInterface *reporter,
                         ReceiverInterface *receiver,
                         int64_t report_ms) {
  reporters_[component_id] = reporter;
  receivers_[component_id] = receiver;
  if (reporter != nullptr) {
    RAY_CHECK(report_ms > 0);
    timer_.RunFnPeriodically(
        [this, component_id]() {
          const auto &local_view = cluster_view_[GetNodeId()];
          auto reporter = reporters_[component_id];
          if (reporter != nullptr) {
            auto version =
                local_view[component_id] ? local_view[component_id]->version() : 0;
            auto update = reporter->Snapshot(version, component_id);
            if (update) {
              Update(*update);
            }
          }
        },
        report_ms);
  }
}

void RaySyncer::Update(RaySyncMessage message) {
  if (message.message_type() == RaySyncMessageType::AGGREGATE) {
    BroadcastMessage(std::make_shared<RaySyncMessage>(std::move(message)));
    return;
  }

  auto &current_message = cluster_view_[message.node_id()][message.component_id()];
  if (current_message && current_message->version() >= message.version()) {
    // We've already got the newer messages. Skip this.
    return;
  }

  current_message = std::make_shared<RaySyncMessage>(std::move(message));
  BroadcastMessage(current_message);
}

RaySyncer::NodeSyncContext *RaySyncer::GetSyncContext(const std::string &node_id) const {
  NodeSyncContext *node_context = nullptr;
  if (leader_ && node_id == leader_->GetNodeId()) {
    node_context = leader_;
  } else {
    auto iter = sync_context_.find(node_id);
    if (iter != sync_context_.end()) {
      node_context = iter->second.get();
    }
  }
  return node_context;
}

void RaySyncer::BroadcastMessage(std::shared_ptr<RaySyncMessage> message) {
  // Send to followers
  if (message->message_type() == RaySyncMessageType::BROADCAST) {
    for (auto &context : sync_context_) {
      context.second->PushToSendingQueue(message);
    }
  }

  // Parents: always sends upward
  if (leader_) {
    leader_->PushToSendingQueue(message);
  }

  // Update the current node if
  //   1) This is an aggregate message and this node is the root;
  //   2) Or, this is a regular message.
  if (leader_ == nullptr || message->message_type() != RaySyncMessageType::AGGREGATE) {
    if (message->node_id() != GetNodeId()) {
      if (receivers_[message->component_id()]) {
        receivers_[message->component_id()]->Update(message);
      }
    }
  }
}

RaySyncer::NodeSyncContext::NodeSyncContext(RaySyncer &instance,
                                            instrumented_io_context &io_context,
                                            const std::string &node_id)
    : timer_(io_context), instance_(instance), io_context_(io_context) {
  timer_.expires_from_now(boost::posix_time::milliseconds(
      RayConfig::instance().raylet_report_resources_period_milliseconds()));
  timer_.async_wait([this](boost::system::error_code) {
    this->DoSend();
    timer_.expires_from_now(boost::posix_time::milliseconds(
        RayConfig::instance().raylet_report_resources_period_milliseconds()));
  });
}

void RaySyncer::NodeSyncContext::PushToSendingQueue(
    std::shared_ptr<RaySyncMessage> message) {
  auto &node_versions = GetNodeComponentVersions(message->node_id());
  if (node_versions[message->component_id()] < message->version()) {
    sending_queue_.insert(message);
    node_versions[message->component_id()] = message->version();
  }
}

std::array<uint64_t, kComponentArraySize>
    &RaySyncer::NodeSyncContext::GetNodeComponentVersions(const std::string &node_id) {
  auto iter = node_versions_.find(node_id);
  if (iter == node_versions_.end()) {
    iter = node_versions_.emplace(node_id, std::array<uint64_t, kComponentArraySize>({}))
               .first;
  }
  return iter->second;
}

RaySyncer::ClientSyncContext::ClientSyncContext(
    RaySyncer &instance,
    instrumented_io_context &io_context,
    const std::string &node_id,
    std::shared_ptr<ray::rpc::syncer::RaySyncer::Stub> stub)
    : RaySyncer::NodeSyncContext(instance, io_context, node_id), stub_(std::move(stub)) {
  StartLongPolling();
}

void RaySyncer::ClientSyncContext::StartLongPolling() {
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

void RaySyncer::ClientSyncContext::DoSend() {
  if (sending_queue_.empty()) {
    return;
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

RaySyncer::ServerSyncContext::ServerSyncContext(RaySyncer &instance,
                                                instrumented_io_context &io_context,
                                                const std::string &node_id)
    : RaySyncer::NodeSyncContext(instance, io_context, node_id) {}

void RaySyncer::ServerSyncContext::HandleLongPollingRequest(
    grpc::ServerUnaryReactor *reactor, RaySyncMessages *response) {
  RAY_CHECK(response_ == nullptr);
  RAY_CHECK(unary_reactor_ == nullptr);

  unary_reactor_ = reactor;
  response_ = response;
}

void RaySyncer::ServerSyncContext::DoSend() {
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

}  // namespace syncer
}  // namespace ray
