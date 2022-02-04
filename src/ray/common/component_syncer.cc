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
  auto iter = cluster_messages_.find(from_node_id);
  if (iter == cluster_messages_.end()) {
    RAY_LOG(INFO) << "Can't find node " << from_node_id << ", abort update";
    return;
  }
  auto component_key = std::make_pair(message.node_id(), message.component_id());
  auto &current_message = iter->second[component_key];

  if (current_message != nullptr && message.version() < current_message->version()) {
    RAY_LOG(INFO) << "Version stale: " << message.version() << " "
                  << current_message->version();
    return;
  }

  // We don't update for local nodes
  if (receivers_[message.component_id()] != nullptr && message.node_id() != node_id_) {
    receivers_[message.component_id()]->Update(message);
  }
  current_message = std::make_shared<RaySyncMessage>(std::move(message));
}

void RaySyncer::Update(const std::string &from_node_id, RaySyncMessages messages) {
  for (auto &message : *messages.mutable_sync_messages()) {
    Update(from_node_id, std::move(message));
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
