#include "ray/common/component_syncer.h"

#include <type_traits>

#include "ray/util/container_util.h"

namespace ray {
namespace syncing {

class RaySyncer;

template <typename T>
class NodeSyncContext : public T {
 public:
  using T::StartRead;
  using T::StartWrite;

  constexpr static bool kIsServerReactor = std::is_same_v<T, ServerReactor>;
  using C = std::conditional_t<kIsServerReactor, grpc::CallbackServerContext,
                               grpc::ClientContext>;

  NodeSyncContext(RaySyncer &syncer, instrumented_io_context &io_context, C *rpc_context)
      : rpc_context_(rpc_context), io_context_(io_context), instance_(syncer) {
        Init();
  }

  void Init() {
    if constexpr (kIsServerReactor) {
      // Init server
      const auto &metadata = rpc_context_->client_metadata();
      auto iter = metadata.find("node_id");
      RAY_CHECK(iter != metadata.end());
      node_id_ = std::string(iter->second.begin(), iter->second.end());
      T::StartSendInitialMetadata();
    } else {
      T::StartCall();
    }
  }

  const std::string &GetNodeId() const { return node_id_; }

  void Update(std::shared_ptr<RaySyncMessage> message) {
    // This thread has to be called from io_context_.
    auto &node_versions = GetNodeComponentVersions(message->node_id());

    if (node_versions[message->component_id()] < message->version()) {
      out_buffer_.push_back(message);
      node_versions[message->component_id()] = message->version();
    }

    if (out_message_ == nullptr) {
      SendNextMessage();
    }
  }

  void OnReadDone(bool ok) override {
    if (ok) {
      io_context_.dispatch(
          [this] {
            for (auto &message : in_message_.sync_messages()) {
              auto &node_versions = this->GetNodeComponentVersions(message.node_id());
              if (node_versions[message.component_id()] < message.version()) {
                node_versions[message.component_id()] = message.version();
              }
            }
            instance_.Update(std::move(in_message_));
            in_message_.Clear();
            StartRead(&in_message_);
          },
          "ReadDone");
    } else {
      HandleFailure();
    }
  }

  void OnWriteDone(bool ok) override {
    if (ok) {
      io_context_.dispatch([this] { this->SendNextMessage(); }, "RaySyncWrite");
    } else {
      HandleFailure();
    }
  }

  void OnSendInitialMetadataDone(bool ok) {
    RAY_CHECK(kIsServerReactor);
    if (ok) {
      StartRead(&in_message_);
    } else {
      HandleFailure();
    }
  }

  void OnReadInitialMetadataDone(bool ok) {
    RAY_CHECK(!kIsServerReactor);
    if (ok) {
      const auto &metadata = rpc_context_->GetServerInitialMetadata();
      auto iter = metadata.find("node_id");
      RAY_CHECK(iter != metadata.end());
      RAY_LOG(INFO) << "Start to follow " << iter->second;
      node_id_ = std::string(iter->second.begin(), iter->second.end());
      StartRead(&in_message_);
    } else {
      HandleFailure();
    }
  }

  void OnDone() {
    RAY_CHECK(kIsServerReactor);
  }

  void OnDone(const grpc::Status &status) {
    RAY_CHECK(!kIsServerReactor);
  }

 private:
  void SendNextMessage() {
    out_buffer_.erase(out_buffer_.begin(), out_buffer_.begin() + consumed_messages_);
    arena_.Reset();
    if (out_buffer_.empty()) {
      out_message_ = nullptr;
    } else {
      out_message_ =
          google::protobuf::Arena::CreateMessage<ray::rpc::syncer::RaySyncMessages>(
              &arena_);
      absl::flat_hash_set<std::string> inserted;
      for (auto iter = out_buffer_.rbegin(); iter != out_buffer_.rend(); ++iter) {
        if (inserted.find((*iter)->node_id()) != inserted.end()) {
          continue;
        }
        inserted.insert((*iter)->node_id());
        out_message_->mutable_sync_messages()->UnsafeArenaAddAllocated((*iter).get());
      }
      consumed_messages_ = out_buffer_.size();
      StartWrite(out_message_);
    }
  }

  std::array<uint64_t, kComponentArraySize> &GetNodeComponentVersions(
      const std::string &node_id) {
    auto iter = node_versions_.find(node_id);
    if (iter == node_versions_.end()) {
      iter = node_versions_.emplace(node_id,
                                    std::array<uint64_t, kComponentArraySize>({})).first;
    }
    return iter->second;
  }

  void HandleFailure() {
    if constexpr (kIsServerReactor) {
      T::Finish(grpc::Status::OK);
    } else {
      // TODO
    }
  }

  C *rpc_context_;
  instrumented_io_context &io_context_;
  RaySyncer &instance_;
  std::string node_id_;

  google::protobuf::Arena arena_;
  ray::rpc::syncer::RaySyncMessages in_message_;
  ray::rpc::syncer::RaySyncMessages *out_message_;
  size_t consumed_messages_;
  std::vector<std::shared_ptr<RaySyncMessage>> out_buffer_;

  absl::flat_hash_map<std::string, std::array<uint64_t, kComponentArraySize>>
      node_versions_;
};

RaySyncer::RaySyncer(std::string node_id, instrumented_io_context &io_context)
    : node_id_(std::move(node_id)),
      reporters_({}),
      receivers_({}),
      io_context_(io_context) {
}

void RaySyncer::ConnectTo(std::shared_ptr<grpc::Channel> channel) {
  // We don't allow connect to new leader.
  RAY_CHECK(leader_ == nullptr);
  leader_stub_ = ray::rpc::syncer::RaySyncer::NewStub(channel);
  auto client_context = std::make_unique<grpc::ClientContext>().release();
  client_context->AddMetadata("node_id", GetNodeId());
  leader_ = std::make_unique<NodeSyncContext<ClientReactor>>(*this, this->io_context_,
                                                             client_context);
  leader_stub_->async()->StartSync(client_context, leader_.get());
}

ServerReactor *RaySyncer::ConnectFrom(grpc::CallbackServerContext *context) {
  context->AddInitialMetadata("node_id", GetNodeId());
  auto reactor =
      std::make_unique<NodeSyncContext<ServerReactor>>(*this, this->io_context_, context);
  auto [iter, added] = followers_.emplace(reactor->GetNodeId(), std::move(reactor));
  RAY_CHECK(added);
  return iter->second.get();
}

void RaySyncer::BroadcastMessage(std::shared_ptr<RaySyncMessage> message) {
    for (auto &follower : followers_) {
      dynamic_cast<NodeSyncContext<ClientReactor>*>(follower.second.get())->Update(message);
    }
    if (message->node_id() != GetNodeId()) {
      if (receivers_[message->component_id()]) {
        receivers_[message->component_id()]->Update(*message);
      }
    }
}

}  // namespace syncing
}  // namespace ray
