#include "ray/common/component_syncer.h"

#include <type_traits>

#include "ray/util/container_util.h"
#include "ray/common/asio/periodical_runner.h"

namespace ray {
namespace syncing {


RaySyncer::RaySyncer(std::string node_id, instrumented_io_context &io_context)
    : node_id_(std::move(node_id)),
      reporters_({}),
      receivers_({}),
      io_context_(io_context), timer_(io_context) {

  timer_.RunFnPeriodically(
      [this]() {
        const auto& local_view = cluster_view_[GetNodeId()];
        for(size_t i = 0; i < kComponentArraySize; ++i) {
          auto reporter = reporters_[i];
          if(reporter != nullptr) {
            auto version = local_view[i] ? local_view[i]->version() : 0;
            auto update = reporter->Snapshot(version);
            if(update) {
              Update(*update);
            }
          }
        }
      },
      100);
}

void RaySyncer::ConnectTo(std::shared_ptr<grpc::Channel> channel) {
  // We don't allow connect to new leader.
  RAY_CHECK(leader_ == nullptr);
  leader_stub_ = ray::rpc::syncer::RaySyncer::NewStub(channel);
  auto client_context = std::make_unique<grpc::ClientContext>().release();
  client_context->AddMetadata("node_id", GetNodeId());
  leader_ = std::make_unique<SyncClientReactor>(*this, this->io_context_,
                                                             client_context);
  leader_stub_->async()->StartSync(client_context, leader_.get());
}

SyncServerReactor *RaySyncer::ConnectFrom(grpc::CallbackServerContext *context) {
  context->AddInitialMetadata("node_id", GetNodeId());
  auto reactor =
      std::make_unique<SyncServerReactor>(*this, this->io_context_, context);
  auto [iter, added] = followers_.emplace(reactor->GetNodeId(), std::move(reactor));
  RAY_CHECK(added);
  return iter->second.get();
}

void RaySyncer::BroadcastMessage(std::shared_ptr<RaySyncMessage> message) {
  for (auto &follower : followers_) {
    dynamic_cast<SyncClientReactor *>(follower.second.get())
        ->Update(message);
  }
  if (message->node_id() != GetNodeId()) {
    if (receivers_[message->component_id()]) {
      receivers_[message->component_id()]->Update(*message);
    }
  }
}


grpc::ServerBidiReactor<RaySyncMessages, RaySyncMessages>*
RaySyncerService::StartSync(grpc::CallbackServerContext *context) {
    return syncer_.ConnectFrom(context);
}

}  // namespace syncing
}  // namespace ray
