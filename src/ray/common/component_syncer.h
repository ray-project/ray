#pragma once
#include <grpcpp/server.h>

#include "ray/common/asio/instrumented_io_context.h"
#include "src/ray/protobuf/syncer.grpc.pb.h"

namespace ray {
namespace syncing {

struct Reporter {
  virtual ray::rpc::syncer::RaySyncMessage Snapshot() const = 0;
};

struct Receiver {
  virtual void Update(ray::rpc::syncer::RaySyncMessage &) = 0;
};

class RaySyncer {
 public:
  using RayComponentId = ray::rpc::syncer::RayComponentId;
  using RaySyncMessage = ray::rpc::syncer::RaySyncMessage;
  using RaySyncMessages = ray::rpc::syncer::RaySyncMessages;
  using RaySyncMessageType = ray::rpc::syncer::RaySyncMessageType;
  using ServerReactor = grpc::ServerBidiReactor<ray::rpc::syncer::RaySyncMessages,
                                                ray::rpc::syncer::RaySyncMessages>;
  using ClientReactor = grpc::ClientBidiReactor<ray::rpc::syncer::RaySyncMessages,
                                                ray::rpc::syncer::RaySyncMessages>;
  static constexpr size_t kComponentArraySize =
      static_cast<size_t>(ray::rpc::syncer::RayComponentId_ARRAYSIZE);

  RaySyncer(std::string node_id, instrumented_io_context &io_context)
      : node_id_(std::move(node_id)),
        reporters_({}),
        receivers_({}),
        io_context_(io_context) {
    AddNode(node_id_);
  }

  void Follow(std::shared_ptr<grpc::Channel> channel) {
    // We don't allow change the follower for now.
    RAY_CHECK(leader_ == nullptr);
    leader_stub_ = ray::rpc::syncer::RaySyncer::NewStub(channel);
    leader_ = std::make_unique<SyncerClientReactor>(*this, node_id_, *leader_stub_);
  }

  void Register(RayComponentId component_id, const Reporter *reporter,
                Receiver *receiver) {
    reporters_[component_id] = reporter;
    receivers_[component_id] = receiver;
  }

  void Update(const std::string &from_node_id, RaySyncMessage message) {
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

    current_message = std::make_shared<RaySyncMessage>(std::move(message));
    RAY_LOG(INFO) << "Current Node: " << node_id_
                  << ", message node: " << message.node_id()
                  << " receiver: " << receivers_[message.component_id()];
    if (receivers_[message.component_id()] != nullptr && message.node_id() != node_id_) {
      receivers_[message.component_id()]->Update(*current_message);
    }
  }

  void Update(const std::string &from_node_id, RaySyncMessages messages) {
    for (auto &message : *messages.mutable_sync_messages()) {
      Update(from_node_id, std::move(message));
    }
  }

  std::vector<std::shared_ptr<RaySyncMessage>> SyncMessages(
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

  const std::string &GetNodeId() const { return node_id_; }

  ServerReactor *Accept(const std::string &node_id) {
    auto reactor = std::make_unique<SyncerServerReactor>(*this, node_id);
    followers_.emplace(node_id, std::move(reactor));
    return followers_[node_id].get();
  }

 private:
  void AddNode(const std::string &node_id) {
    cluster_messages_[node_id] = NodeIndexedMessages();
  }

  template <typename T>
  struct Protocol : public T {
    using T::StartRead;
    using T::StartWrite;
    Protocol() {}
    void OnReadDone(bool ok) override {
      if (ok) {
        instance->io_context_.dispatch(
            [this] {
              RAY_LOG(INFO) << "client read " << in_message.sync_messages_size()
                            << " messages";
              instance->Update(node_id, std::move(in_message));
              in_message.Clear();
              StartRead(&in_message);
            },
            "ReadDone");
      } else {
        StartRead(&in_message);
      }
    }

    void OnWriteDone(bool ok) override {
      if (ok) {
        timer->expires_from_now(boost::posix_time::milliseconds(100));
        timer->async_wait([this](const boost::system::error_code &error) {
          if (error == boost::asio::error::operation_aborted) {
            return;
          }
          RAY_CHECK(!error) << error.message();
          SendMessage();
        });
      } else {
        instance->io_context_.dispatch([this] { SendMessage(); }, "RaySyncWrite");
      }
    }

    void ResetOutMessage() {
      arena.Reset();
      out_message =
          google::protobuf::Arena::CreateMessage<ray::rpc::syncer::RaySyncMessages>(
              &arena);
    }

    void SendMessage() {
      for (size_t i = 0; i < kComponentArraySize; ++i) {
        if (instance->reporters_[i] != nullptr) {
          instance->Update(node_id, instance->reporters_[i]->Snapshot());
        }
      }
      buffer = instance->SyncMessages(node_id);
      if (buffer.empty()) {
        OnWriteDone(true);
        return;
      }
      ResetOutMessage();
      for (auto &message : buffer) {
        RAY_LOG(INFO) << "WMSG: " << message->node_id();
        out_message->mutable_sync_messages()->UnsafeArenaAddAllocated(message.get());
      }
      RAY_LOG(INFO) << "client write " << out_message->sync_messages_size()
                    << " messages";
      StartWrite(out_message);
    }

    RaySyncer *instance;
    std::unique_ptr<boost::asio::deadline_timer> timer;

    std::string node_id;
    google::protobuf::Arena arena;
    ray::rpc::syncer::RaySyncMessages in_message;
    ray::rpc::syncer::RaySyncMessages *out_message;
    std::vector<std::shared_ptr<RaySyncMessage>> buffer;
  };

  class SyncerClientReactor : public Protocol<ClientReactor> {
   public:
    SyncerClientReactor(RaySyncer &instance, const std::string &node_id,
                        ray::rpc::syncer::RaySyncer::Stub &stub) {
      this->instance = &instance;
      this->timer =
          std::make_unique<boost::asio::deadline_timer>(this->instance->io_context_);
      this->node_id = node_id;
      rpc_context_.AddMetadata("node_id", node_id);
      stub.async()->StartSync(&rpc_context_, this);
      ResetOutMessage();
      StartCall();
    }

    void OnReadInitialMetadataDone(bool ok) override {
      RAY_CHECK(ok) << "Fail to read initial data";
      const auto &metadata = rpc_context_.GetServerInitialMetadata();
      auto iter = metadata.find("node_id");
      RAY_CHECK(iter != metadata.end());
      RAY_LOG(INFO) << "Start to follow " << iter->second;
      instance->AddNode(std::string(iter->second.begin(), iter->second.end()));
      node_id = std::string(iter->second.begin(), iter->second.end());
      StartRead(&in_message);
      instance->io_context_.dispatch([this] { SendMessage(); }, "RaySyncWrite");
    }

    void OnDone(const grpc::Status &status) override {
      instance->io_context_.dispatch([this] { instance->followers_.erase(node_id); },
                                     "RaySyncDone");
    }

   private:
    grpc::ClientContext rpc_context_;
  };

  class SyncerServerReactor : public Protocol<ServerReactor> {
   public:
    SyncerServerReactor(RaySyncer &instance, const std::string &node_id) {
      this->instance = &instance;
      this->node_id = node_id;
      this->timer =
          std::make_unique<boost::asio::deadline_timer>(this->instance->io_context_);
      StartSendInitialMetadata();
    }

    const std::string &GetNodeId() const { return node_id; }

    void OnSendInitialMetadataDone(bool ok) {
      if (ok) {
        StartRead(&in_message);
        instance->io_context_.dispatch([this] { SendMessage(); }, "RaySyncWrite");
      } else {
        Finish(grpc::Status::OK);
      }
    }

    void OnDone() override {
      instance->io_context_.dispatch([this] { instance->followers_.erase(node_id); },
                                     "RaySyncDone");
    }
  };

 private:
  const std::string node_id_;
  std::unique_ptr<ray::rpc::syncer::RaySyncer::Stub> leader_stub_;
  std::unique_ptr<ClientReactor> leader_;
  // Manage messages
  using NodeIndexedMessages = absl::flat_hash_map<std::pair<std::string, RayComponentId>,
                                                  std::shared_ptr<RaySyncMessage>>;
  absl::flat_hash_map<std::string, NodeIndexedMessages> cluster_messages_;

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
    const auto &metadata = context->client_metadata();
    auto iter = metadata.find("node_id");
    RAY_CHECK(iter != metadata.end());
    auto node_id = std::string(iter->second.begin(), iter->second.end());
    context->AddInitialMetadata("node_id", syncer_.GetNodeId());
    return syncer_.Accept(node_id);
  }

 private:
  RaySyncer &syncer_;
};

}  // namespace syncing
}  // namespace ray
