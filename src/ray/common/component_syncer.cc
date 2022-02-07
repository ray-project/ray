#include "ray/common/component_syncer.h"
#include "ray/util/container_util.h"

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

// Update the message for component
void RaySyncer::Update(const std::string &from_node_id, RaySyncMessage message) {
  auto& node_views = map_find_or_die(nodes_view_, from_node_id);
  DoUpdate(node_views, std::move(message));
}

void RaySyncer::Update(const std::string &from_node_id, RaySyncMessages messages) {
  auto& node_views = map_find_or_die(nodes_view_, from_node_id);
  for (RaySyncer::RaySyncMessage &message : *messages.mutable_sync_messages()) {
    DoUpdate(node_views, std::move(message));
  }
}

void RaySyncer::DoUpdate(absl::flat_hash_map<std::string, Array<uint64_t>>& node_views,
                         RaySyncer::RaySyncMessage message) {
  // Update view of node version
  auto iter = node_views.find(message.node_id());
  if(iter == node_views.end()) {
    iter = node_views.insert(message.node_id(), Array<uint64_t>{});
  }

  if(iter->second[message.component_id()] < message.version()) {
    iter->second[message.component_id()] = message.version();
  }

  // Update cluster resources
  auto& current_message = cluster_view_[message.component_id()][message.node_id()];
  if(current_message && current_message->version() >= message.version()) {
    // We've already got the newer messages. Skip this.
    return;
  }
  current_message = std::make_shared<RaySyncer::RaySyncMessage>(std::move(message));
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
