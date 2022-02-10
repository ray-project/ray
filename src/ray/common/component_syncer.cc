#include "ray/common/component_syncer.h"

#include <type_traits>

#include "ray/common/asio/periodical_runner.h"
#include "ray/util/container_util.h"

namespace ray {
namespace syncing {

RaySyncer::~RaySyncer() {
  if (leader_) {
    io_context_.dispatch([leader = leader_] { leader->StartWritesDone(); }, "~RaySyncer");
  }
}

RaySyncer::RaySyncer(std::string node_id, instrumented_io_context &io_context)
    : node_id_(std::move(node_id)),
      reporters_({}),
      receivers_({}),
      io_context_(io_context),
      timer_(io_context) {
}

void RaySyncer::ConnectTo(std::unique_ptr<ray::rpc::syncer::RaySyncer::Stub> stub) {
  // We don't allow connect to new leader.
  RAY_CHECK(leader_ == nullptr);
  leader_stub_ = std::move(stub);
  auto client_context = std::make_unique<grpc::ClientContext>().release();
  client_context->AddMetadata("node_id", GetNodeId());
  leader_ = std::make_unique<SyncClientReactor>(*this, this->io_context_, client_context)
                .release();
  leader_stub_->async()->StartSync(client_context, leader_);
  leader_->Init();
}

void RaySyncer::DisconnectFrom(std::string node_id) {
  RAY_LOG(INFO) << "NodeId: " << node_id << " exits";
  RAY_CHECK(followers_.erase(node_id) > 0);
}

SyncServerReactor *RaySyncer::ConnectFrom(grpc::CallbackServerContext *context) {
  context->AddInitialMetadata("node_id", GetNodeId());
  auto reactor = std::make_unique<SyncServerReactor>(*this, this->io_context_, context);
  reactor->Init();
  RAY_LOG(INFO) << "Adding node: " << reactor->GetNodeId();
  auto [iter, added] = followers_.emplace(reactor->GetNodeId(), std::move(reactor));
  RAY_CHECK(added);
  return iter->second.get();
}

void RaySyncer::BroadcastMessage(std::shared_ptr<RaySyncMessage> message) {
  // Children
  if(message->message_type() == RaySyncMessageType::BROADCAST) {
    for (auto &follower : followers_) {
      follower.second->Send(message);
    }
  }

  // Parents: always send upward
  if (leader_) {
    leader_->Send(message);
  }

  // The current node
  if(leader_ == nullptr || message->message_type() != RaySyncMessageType::AGGREGATE) {
    if (message->node_id() != GetNodeId()) {
      if (receivers_[message->component_id()]) {
        receivers_[message->component_id()]->Update(*message);
      }
    }
  }
}

grpc::ServerBidiReactor<RaySyncMessages, RaySyncMessages> *RaySyncerService::StartSync(
    grpc::CallbackServerContext *context) {
  return syncer_.ConnectFrom(context);
}

}  // namespace syncing
}  // namespace ray
