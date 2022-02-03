#pragma once

#include "ray/common/asio/instrumented_io_context.h"
#include "src/ray/protobuf/syncer.pb.h"

namespace ray {
namespace syncing {

struct Reporter {
  virtual const std::string *Snapshot() const = 0;
};

struct Receiver {
  virtual bool Update(ray::rpc::syncer::RaySyncMessage &) = 0;
  virtual void Snapshot(ray::rpc::syncer::RaySyncMessage &) const = 0;
};

class RaySyncer {
 public:
  using RayComponentId = ray::rpc::syncer::RayComponentId;
  using RaySyncMessage = ray::rpc::syncer::RaySyncMessage;
  using RaySyncMessages = ray::rpc::syncer::RaySyncMessages;
  using RaySyncMessageType = ray::rpc::syncer::RaySyncMessageType;
  constexpr RayComponentId kComponentArraySize =
      ray::rpc::syncer::RayComponentId_ARRAYSIZE;

  explicit RaySyncer();

  void Register(RayComponentId component_id, const Reporter *reporter,
                Receiver *receiver) {
    reporters_[component_id] = reporter;
    receivers_[component_id] = receiver;
  }

  // Upon receiving a message.
  bool Update(RaySyncMessage message) {
    auto iter = cluster_messages_.find(message.node_id());
    if (iter == cluster_messages_.end()) {
      return false;
    }
    // Only support snapshot message right now.
    RAY_CHECK(message.messate_type() == RaySyncMessageType::SNAPSHOT);

    if (iter->second[message.component_id()] == nullptr ||
        message.version() < iter->second[message.component_id()]->version()) {
      return false;
    }
    iter->second[message.component_id()] =
        std::make_shared<RaySyncMessage>(std::move(message));
    return receivers_[message.component_id()]->Update(
        iter->second[message.component_id()]);
  }

  bool Update(RaySyncMessages messages) {
    for (auto &message : *messages.mutable_sync_messages()) {
      Update(std::move(message));
    }
  }

  std::vector<std::shared_ptr<RaySyncMessages>> SyncMessages(
      const std::string &node_id) const {
    std::vector<std::shared_ptr<RaySyncMessages>> messages;
    for (auto &node_message : cluster_messages_) {
      if (node_message.first == node_id) {
        continue;
      }
      for (auto &component_message : node_message.second) {
        if (component_message != nullptr) {
          messages.push_back(component_message);
        }
      }
    }
    return messages;
  }

 private:
  // Manage messages
  absl::flat_hash_map<std::string,
                      std::array<std::shared_ptr<RaySyncMessage>, kComponentArraySize>>
      cluster_messages_;

  // For local nodes
  std::array<const Reporter *, kComponentArraySize> reporters_;
  std::array<Receiver *, kComponentArraySize> receivers_;
};

class RaySyncerService : public ray::rpc::syncer::RaySyncer::CallbackService {
 public:
  RaySyncerService(instrumented_io_context &io_context) : io_context_(io_context) {}

  grpc::ServerBidiReactor<RaySyncMessage, RaySyncMessage> *StartSync(
      grpc::CallbackServerContext *context) override {}

 private:
  std::unique_ptr<ComponentSyncer> syncer_;
  instrumented_io_context &io_context_;
};

}  // namespace syncing
}  // namespace ray
