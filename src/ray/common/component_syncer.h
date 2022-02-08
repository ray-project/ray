#pragma once
#include <grpcpp/server.h>

#include "ray/common/asio/instrumented_io_context.h"
#include "src/ray/protobuf/syncer.grpc.pb.h"

namespace ray {
namespace syncing {

using ServerReactor = grpc::ServerBidiReactor<ray::rpc::syncer::RaySyncMessages,
                                              ray::rpc::syncer::RaySyncMessages>;
using ClientReactor = grpc::ClientBidiReactor<ray::rpc::syncer::RaySyncMessages,
                                              ray::rpc::syncer::RaySyncMessages>;

using RayComponentId = ray::rpc::syncer::RayComponentId;
using RaySyncMessage = ray::rpc::syncer::RaySyncMessage;
using RaySyncMessages = ray::rpc::syncer::RaySyncMessages;
using RaySyncMessageType = ray::rpc::syncer::RaySyncMessageType;

struct Reporter {
  virtual std::optional<RaySyncMessage> Snapshot(uint64_t current_version) const = 0;
};

struct Receiver {
  virtual void Update(const RaySyncMessage &message) = 0;
};

class RaySyncer {
 public:
  static constexpr size_t kComponentArraySize =
      static_cast<size_t>(ray::rpc::syncer::RayComponentId_ARRAYSIZE);

  RaySyncer(std::string node_id, instrumented_io_context &io_context);

  // Follower will send its message to leader
  // Leader will broadcast what it received to followers
  void ConnectTo(std::shared_ptr<grpc::Channel> channel);

  // Register a component
  void Register(RayComponentId component_id, const Reporter *reporter,
                Receiver *receiver) {
    reporters_[component_id] = reporter;
    receivers_[component_id] = receiver;
  }

  void Update(RaySyncMessage message) {
    auto &current_message = cluster_view_[message.component_id()][message.node_id()];
    if (current_message && current_message->version() >= message.version()) {
      // We've already got the newer messages. Skip this.
      return;
    }
    current_message = std::make_shared<RaySyncer::RaySyncMessage>(std::move(message));
    SendMessage(message);
  }

  void Update(RaySyncMessages messages) {
    for (RaySyncer::RaySyncMessage &message : *messages.mutable_sync_messages()) {
      Update(std::move(message));
    }
  }

  const std::string &GetNodeId() const { return node_id_; }

  ServerReactor *ConnectFrom(grpc::CallbackServerContext *context);

 private:
  template <typename T>
  using Array = std::array<T, kComponentArraySize>;

  void SendMessage(std::shared_ptr<RaySyncMessage> message) {
    for (auto &follower : followers_) {
      follower->Update(message);
    }
    if (message->node_id() != GetNodeId()) {
      if (receivers_[message->component_id()]) {
        receivers_[message->component_id()]->Update(*message);
      }
    }
  }

  const std::string node_id_;
  std::unique_ptr<ray::rpc::syncer::RaySyncer::Stub> leader_stub_;
  std::unique_ptr<ClientReactor> leader_;

  Array<absl::flat_hash_map<std::string, std::shared_ptr<RaySyncMessage>>> cluster_view_;

  // Manage connections
  absl::flat_hash_map<std::string, std::unique_ptr<ServerReactor>> followers_;

  // For local nodes
  std::array<const Reporter *, kComponentArraySize> reporters_;
  std::array<Receiver *, kComponentArraySize> receivers_;
  instrumented_io_context &io_context_;
};

class RaySyncerService : public ray::rpc::syncer::RaySyncer::CallbackService {
 public:
  RaySyncerService(RaySyncer &syncer) : syncer_(syncer) {}

  grpc::ServerBidiReactor<ray::rpc::syncer::RaySyncMessages,
                          ray::rpc::syncer::RaySyncMessages>
      *StartSync(grpc::CallbackServerContext *context) override {
    return syncer_.ConnectFrom(context);
  }

 private:
  RaySyncer &syncer_;
};

}  // namespace syncing
}  // namespace ray
