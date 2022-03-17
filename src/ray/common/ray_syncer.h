#pragma once
#include <grpcpp/server.h>

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "boost/functional/hash.hpp"

#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/asio/periodical_runner.h"
#include "ray/common/id.h"
#include "src/ray/protobuf/ray_syncer.grpc.pb.h"

namespace ray {
namespace syncer {

using ServerBidiReactor = grpc::ServerBidiReactor<ray::rpc::syncer::RaySyncMessages,
                                                  ray::rpc::syncer::RaySyncMessages>;
using ClientBidiReactor = grpc::ClientBidiReactor<ray::rpc::syncer::RaySyncMessages,
                                                  ray::rpc::syncer::RaySyncMessages>;

using ray::rpc::syncer::DummyRequest;
using ray::rpc::syncer::DummyResponse;
using ray::rpc::syncer::RayComponentId;
using ray::rpc::syncer::RaySyncMessage;
using ray::rpc::syncer::RaySyncMessages;
using ray::rpc::syncer::RaySyncMessageType;
using ray::rpc::syncer::SyncMeta;

static constexpr size_t kComponentArraySize =
    static_cast<size_t>(ray::rpc::syncer::RayComponentId_ARRAYSIZE);

/// The interface for a reporter. Reporter is defined to be a local module which would
/// like to let the other nodes know its status. For example, local cluster resource
/// manager.
struct ReporterInterface {
  /// Interface to get the snapshot of the component. It asks the module to take a
  /// snapshot of the current status. Each snapshot is versioned, and it should return
  /// std::nullopt if the version hasn't changed.
  ///
  /// \param current_version The version syncer module current has.
  /// \param component_id The component id asked for.
  virtual std::optional<RaySyncMessage> Snapshot(uint64_t current_version,
                                                 RayComponentId component_id) const = 0;
  virtual ~ReporterInterface() {}
};

/// The interface for a receiver. Receiver is defined to be a module which would like
/// to get the status of other nodes. For example, cluster resource manager.
struct ReceiverInterface {
  /// Interface to update a module. The module should read the `sync_message` fields and
  /// deserialize it to update its internal status.
  ///
  /// \param message The message received from remote node.
  virtual void Update(std::shared_ptr<RaySyncMessage> message) = 0;
  virtual ~ReceiverInterface() {}
};

/// RaySyncer is an embedding service for component synchronization.
class RaySyncer {
 public:
  class NodeSyncContext;
  class ClientSyncContext;
  class ServerSyncContext;

  /// Constructor of RaySyncer
  ///
  /// \param node_id The id of current node.
  RaySyncer(const std::string &node_id);

  ~RaySyncer();

  /// Connect to a node. This will establish a connection between the receiver and the
  /// current node. Right now, it'll only be called within GCS. But it can be extended.
  /// This will make the data flow like:
  ///    current node <- node of the stub
  ///
  /// \param stub The stub for RPC operations
  void ConnectTo(std::unique_ptr<ray::rpc::syncer::RaySyncer::Stub> stub);

  /// Clean up the data when a node is removed.
  void NodeRemoved(const std::string &node_id);

  /// Get a connection from a node.
  ///
  /// \param node_id The node id of the caller.
  std::unique_ptr<ServerSyncContext> ConnectFrom(const std::string &node_id);

  /// Register the components to the syncer module. Syncer will make sure eventually
  /// it'll have a global view of the cluster.
  ///
  /// \param component_id The component to sync.
  /// \param reporter The local component to be broadcasted.
  /// \param receiver The snapshot of the component in the cluster.
  /// \param report_ms The frequence to report resource usages.
  void Register(RayComponentId component_id,
                const ReporterInterface *reporter,
                ReceiverInterface *receiver,
                int64_t report_ms = 100);

  /// Tell the syncer there is an update.
  ///
  /// \param message The updating message.
  void Update(RaySyncMessage message);

  /// Get the sync contexts for a given node id.
  ///
  /// \param messages The updating messages.
  NodeSyncContext *GetSyncContext(const std::string &node_id) const;

  /// Get the current node id.
  const std::string &GetNodeId() const { return node_id_; }

  class NodeSyncContext {
   public:
    NodeSyncContext(RaySyncer &instance,
                    instrumented_io_context &io_context,
                    const std::string& node_id);

    /// Push a message to the sending queue to be sent later.
    ///
    /// \param message The message to be sent.
    void PushToSendingQueue(std::shared_ptr<RaySyncMessage> message);

    virtual ~NodeSyncContext() { timer_.cancel(); }

    /// Return the node id of this sync context.
    const std::string &GetNodeId() const { return node_id_; }

    /// Handle the udpates sent from this node.
    ///
    /// \param messages The message received.
    void ReceiveUpdate(RaySyncMessages messages) {
      RAY_CHECK(messages.node_id() == node_id_);
      for (auto &message : *messages.mutable_sync_messages()) {
        auto &node_versions = GetNodeComponentVersions(message.node_id());
        if (node_versions[message.component_id()] < message.version()) {
          node_versions[message.component_id()] = message.version();
        }
        instance_.Update(std::move(message));
      }
    }

   protected:
    // The function to send data.
    // We need different implementation for server and client.
    // Server will wait until client send the long-polling request.
    // Client will just uses Update to send the data immediately.
    // This function needs to read data from `sending_queue_` and construct the sending
    // batch and do the actual sending.
    virtual void DoSend() = 0;

    std::array<uint64_t, kComponentArraySize> &GetNodeComponentVersions(
      const std::string &node_id);
    boost::asio::deadline_timer timer_;
    RaySyncer &instance_;
    instrumented_io_context &io_context_;
    std::string node_id_;

    struct _MessageHash {
      std::size_t operator()(const std::shared_ptr<RaySyncMessage> &m) const noexcept {
        std::size_t seed = 0;
        boost::hash_combine(seed, m->node_id());
        boost::hash_combine(seed, m->component_id());
        return seed;
      }
    };

    absl::flat_hash_set<std::shared_ptr<RaySyncMessage>, _MessageHash> sending_queue_;
    // Keep track of the versions of components in this node.
    absl::flat_hash_map<std::string, std::array<uint64_t, kComponentArraySize>>
        node_versions_;
  };

  class ServerSyncContext : public NodeSyncContext {
   public:
    ServerSyncContext(RaySyncer &instance,
                      instrumented_io_context &io_context,
                      const std::string &node_id);

    void HandleLongPollingRequest(grpc::ServerUnaryReactor *reactor,
                                  RaySyncMessages *response);

   protected:
    void DoSend() override;

    // These two fields are RPC related. When the server got long-polling requests,
    // these two fields will be set so that it can be used to send message.
    // After the message being sent, these two fields will be set to be empty again.
    // When the periodical timer wake up, it'll check whether these two fields are set
    // and it'll only send data when these are set.
    RaySyncMessages *response_ = nullptr;
    grpc::ServerUnaryReactor *unary_reactor_ = nullptr;
  };

  class ClientSyncContext : public NodeSyncContext {
   public:
    ClientSyncContext(RaySyncer &instance,
                      instrumented_io_context &io_context,
                      const std::string &node_id,
                      std::shared_ptr<ray::rpc::syncer::RaySyncer::Stub> stub);

   protected:
    void DoSend() override;

    /// Start to send long-polling request to remote nodes.
    void StartLongPolling();

    /// Stub for this connection.
    std::shared_ptr<ray::rpc::syncer::RaySyncer::Stub> stub_;
    ray::rpc::syncer::RaySyncMessages in_message_;
    DummyRequest dummy_;
  };

 private:
  template <typename T>
  using Array = std::array<T, kComponentArraySize>;

  // Function to broadcast the local information to remote nodes.
  void BroadcastMessage(std::shared_ptr<RaySyncMessage> message);

  /// The current node id.
  const std::string node_id_;

  /// The sync context of leader.
  NodeSyncContext *leader_;

  /// Keep track of the latest messages received.
  /// Use shared pointer for easier liveness management since these messages might be
  /// sending via rpc.
  absl::flat_hash_map<std::string, Array<std::shared_ptr<RaySyncMessage>>> cluster_view_;

  /// Manage connections
  absl::flat_hash_map<std::string, std::unique_ptr<NodeSyncContext>> sync_context_;

  /// For local nodes
  std::array<const ReporterInterface *, kComponentArraySize> reporters_;
  std::array<ReceiverInterface *, kComponentArraySize> receivers_;

  /// Threading for syncer. To have a better isolation, we put all operations in this
  /// module into a dedicated thread.
  std::unique_ptr<std::thread> syncer_thread_;
  instrumented_io_context io_context_;

  /// Timer is used to do broadcasting.
  ray::PeriodicalRunner timer_;
};

/// RaySyncerService is a service to take care of resource synchronization
/// related operations.
/// Right now only raylet needs to setup this service. But in the future,
/// we can use this to construct more complicated resource reporting algorithm,
/// like tree-based one.
class RaySyncerService : public ray::rpc::syncer::RaySyncer::CallbackService {
 public:
  RaySyncerService(RaySyncer &syncer) : syncer_(syncer) {}

  grpc::ServerUnaryReactor *StartSync(grpc::CallbackServerContext *context,
                                      const SyncMeta *request,
                                      SyncMeta *response) override {
    auto *reactor = context->DefaultReactor();
    leader_context_ = syncer_.ConnectFrom(request->node_id());
    response->set_node_id(syncer_.GetNodeId());
    reactor->Finish(grpc::Status::OK);
    return reactor;
  }

  grpc::ServerUnaryReactor *Update(grpc::CallbackServerContext *context,
                                   const RaySyncMessages *request,
                                   DummyResponse *) override {
    auto *reactor = context->DefaultReactor();
    auto sync_context = syncer_.GetSyncContext(request->node_id());
    if (sync_context != nullptr) {
      sync_context->ReceiveUpdate(std::move(*const_cast<RaySyncMessages *>(request)));
    } else {
      RAY_LOG(ERROR) << "Node " << NodeID::FromBinary(request->node_id())
                     << " has already been removed.";
    }
    reactor->Finish(grpc::Status::OK);
    return reactor;
  }

  grpc::ServerUnaryReactor *LongPolling(grpc::CallbackServerContext *context,
                                        const DummyRequest *,
                                        RaySyncMessages *response) override {
    auto *reactor = context->DefaultReactor();
    leader_context_->HandleLongPollingRequest(reactor, response);
    return reactor;
  }

 private:
  // This will be created after connection is established.
  // Ideally this should be owned by RaySyncer, but since we are doing
  // long-polling right now, we have to put it here so that when
  // long-polling request comes, we can set it up.
  std::unique_ptr<RaySyncer::ServerSyncContext> leader_context_;

  // The ray syncer this RPC wrappers of.
  RaySyncer &syncer_;
};

}  // namespace syncer
}  // namespace ray
