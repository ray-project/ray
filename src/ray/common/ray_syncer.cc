#include "ray/common/ray_syncer.h"

#include "ray/common/ray_config.h"
namespace ray {
namespace syncer {

RaySyncer::RaySyncer(std::string node_id) : node_id(node_id), timer_(io_context_) {
  syncer_thread_ = std::make_unique<std::thread>([this]() {
    boost::asio::io_service::work work(io_context_);
    io_context_.run();
  });
}

RaySyncer::~RaySyncer() { syncer_thread_->join(); }

void RaySyncer::ConnectTo(const std::string &node_id,
                          std::unique_ptr<ray::rpc::syncer::RaySyncer::Stub> stub) {
  auto context =
      std::make_unique<NodeSyncContext>(*this, io_context_, std::move(stub), node_id);
  sync_context_[node_id] = std::move(context);
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
                         const Reporter *reporter,
                         Receiver *receiver,
                         int64_t report_ms = 100) {
  reporters_[component_id] = reporter;
  receivers_[component_id] = receiver;
  if (reporter != nullptr) {
    RAY_CHECK(publish_ms > 0);
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
        publish_ms);
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

NodeSyncContext *RaySyncer::GetSyncContext(const std::string &node_id) const {
  auto node_id = messages.node_id();
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
      context.second->Send(message);
    }
  }

  // Parents: always sends upward
  if (leader_) {
    leader_->Send(message);
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
    : timer_(instance), instance_(instance), io_context_(io_context) {
  timer_.expires_from_now(boost::posix_time::milliseconds(
      RayConfig::raylet_report_resources_period_milliseconds()));
  timer_.async_wait([this]() {
    this->Send();
    timer_.expires_from_now(boost::posix_time::milliseconds(
        RayConfig::raylet_report_resources_period_milliseconds()))
  });
}

RaySyncer::NodeSyncContext::PushToSendingQueue(std::shared_ptr<RaySyncMessage> message) {
  auto &node_versions = GetNodeComponentVersions(message->node_id());
  if (node_versions[message->component_id()] < message->version()) {
    sending_queue_.insert(message);
    node_versions[message->component_id()] = message->version();
  }
}

RaySyncer::ClientSyncContext::ClientSyncContext(
    RaySyncer &instance,
    instrumented_io_context &io_context,
    const std::string &node_id,
    std::unique_ptr<ray::rpc::syncer::RaySyncer::Stub> stub)
    : RaySyncer::NodeSyncContext(instance, io_context, node_id), stub_(std::move(stub)) {
  StartLongPolling();
}

RaySyncer::ClientSyncContext::StartLongPolling() {
  // This will be a long-polling request. The node will only reply if
  //    1. there is a new version of message
  //    2. and it has passed X ms since last update.
  stub_->async()->Receive(&context_, &dummy_, &in_message_, [this](grpc::Status status) {
    if (status.ok()) {
      io_context_.post([this, in_message = std::move(in_message_)]() mutable {
        instance_.Update(std::move(in_message));
      });
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
  auto response = google::protobuf::Arena::CreateMessage<Dummy>(arena.get());

  std::vector<std::shared_ptr<RaySyncMessage>> holder;

  size_t message_bytes = 0;
  auto iter = sending_queue_.begin();
  while (message_bytes < RayConfig::max_sync_message_batch_bytes() &&
         iter != sending_queue_.end()) {
    message_bytes += (*iter)->sync_message().size();
    // TODO (iycheng): Use arena allocator for optimization
    request->mutable_sync_messages()->UnsafeArenaAddAllocated(iter->get());
    holder.push_back(*iter);
    iter = sending_queue_.erase(iter);
  }
  stub_->async()->Send(
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
  while (message_bytes < RayConfig::max_sync_message_batch_bytes() &&
         iter != sending_queue_.end()) {
    message_bytes += iter->sync_message().size();
    // TODO (iycheng): Use arena allocator for optimization
    response_->add_sync_messages()->CopyFrom(*iter);
    iter = sending_queue_.erase(iter);
  }

  if (message_bytes != 0) {
    unary_reactor_->Finish();
    unary_reactor_ = nullptr;
    response_ = nullptr;
  }
}

}  // namespace syncer
}  // namespace ray
