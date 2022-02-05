#include "ray/common/component_syncer.h"

namespace ray {
namespace syncing {

RaySyncer::RaySyncer(std::string node_id, instrumented_io_context &io_context)
    : node_id_(std::move(node_id)),
      reporters_({}),
      receivers_({}),
      io_context_(io_context) {
  AddNode(node_id_);
}

void RaySyncer::Follow(std::shared_ptr<grpc::Channel> channel) {
  // We don't allow change the follower for now.
  RAY_CHECK(leader_ == nullptr);
  leader_stub_ = ray::rpc::syncer::RaySyncer::NewStub(channel);
  leader_ = std::make_unique<SyncerClientReactor>(*this, node_id_, *leader_stub_);
}

void RaySyncer::Update(const std::string &from_node_id,
                       RaySyncer::RaySyncMessage message) {
  auto& node_view = nodes_view_[from_node_id];
  DoUpdate(node_view, std::move(message));
}

void RaySyncer::Update(const std::string &from_node_id, RaySyncMessages messages) {
  auto& node_view = nodes_view_[from_node_id];
  for (auto &message : *messages.mutable_sync_messages()) {
    DoUpdate(node_view, std::move(message));
  }
}

std::vector<std::shared_ptr<RaySyncer::RaySyncMessage>> RaySyncer::SyncMessages(
    const std::string &node_id) const {
  std::vector<std::shared_ptr<RaySyncMessage>> messages;
  for (auto &node_message : cluster_messages_) {
    if (node_message.first == node_id) {
      continue;
    }
    for (auto &component_message : node_message.second) {
      if (component_message.first.first != node_id) {
        messages.emplace_back(component_message.second);
      }
    }
  }
  return messages;
}

}  // namespace syncing
}  // namespace ray
