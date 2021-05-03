#include "ray/common/asio/instrumented_io_context.h"
#include "ray/gcs/gcs_server/gcs_resource_manager.h"
#include "ray/rpc/node_manager/node_manager_client_pool.h"

namespace ray {
namespace gcs {

/// Broadcasts resource report batches to raylets from a separate thread.
class GcsResourceReportBroadcaster {
 public:
  GcsResourceReportBroadcaster(
      std::shared_ptr<rpc::NodeManagerClientPool> raylet_client_pool,
      std::function<void(rpc::ResourceUsageBatchData &)>
          get_resource_usage_batch_for_broadcast,
      /* Default values should only be changed for testing. */
      std::function<void(const rpc::Address &,
                         std::shared_ptr<rpc::NodeManagerClientPool> &, std::string &,
                         const rpc::ClientCallback<rpc::UpdateResourceUsageReply> &)>
          send_batch =
              [](const rpc::Address &address,
                 std::shared_ptr<rpc::NodeManagerClientPool> &raylet_client_pool,
                 std::string &serialized_resource_usage_batch,
                 const rpc::ClientCallback<rpc::UpdateResourceUsageReply> &callback) {
                auto raylet_client = raylet_client_pool->GetOrConnectByAddress(address);
                raylet_client->UpdateResourceUsage(serialized_resource_usage_batch,
                                                   callback);
              });
  ~GcsResourceReportBroadcaster();

  void Initialize(const GcsInitData &gcs_init_data);

  /// Start a thread to broadcast resource reports..
  void Start();

  /// Stop broadcasting resource reports.
  void Stop();

  /// Event handler when a new node joins the cluster.
  void HandleNodeAdded(const rpc::GcsNodeInfo &node_info) LOCKS_EXCLUDED(mutex_);

  /// Event handler when a node leaves the cluster.
  void HandleNodeRemoved(const rpc::GcsNodeInfo &node_info) LOCKS_EXCLUDED(mutex_);

  std::string DebugString();

 private:
  // An asio service which does the broadcasting work.
  instrumented_io_context broadcast_service_;
  // The associated thread it runs on.
  std::unique_ptr<std::thread> broadcast_thread_;
  // Timer tick to send the next broadcast round.
  PeriodicalRunner ticker_;

  // The shared, thread safe pool of raylet clients, which we use to minimize connections.
  std::shared_ptr<rpc::NodeManagerClientPool> raylet_client_pool_;
  /// See GcsResourcManager::GetResourceUsageBatchForBroadcast. This is passed as an
  /// argument for unit testing purposes only.
  std::function<void(rpc::ResourceUsageBatchData &)>
      get_resource_usage_batch_for_broadcast_;

  std::function<void(const rpc::Address &, std::shared_ptr<rpc::NodeManagerClientPool> &,
                     std::string &,
                     const rpc::ClientCallback<rpc::UpdateResourceUsageReply> &)>
      send_batch_;

  /// A lock to protect the data structures.
  absl::Mutex mutex_;
  /// The set of nodes and their addresses which are subscribed to resource usage changes.
  std::unordered_map<NodeID, rpc::Address> nodes_ GUARDED_BY(mutex_);
  /// The number of inflight resource usage updates per node. After being sent, a request
  /// is inflight if its reply has not been received and it has not timed out.
  std::unordered_map<NodeID, uint64_t> inflight_updates_ GUARDED_BY(mutex_);

  /// The number of nodes skipped in the latest broadcast round. This is useful for
  /// diagnostic purposes.
  uint64_t num_skipped_nodes_;
  const uint64_t broadcast_period_ms_;

  void SendBroadcast();

  friend class GcsResourceReportBroadcasterTest;
};
}  // namespace gcs
}  // namespace ray
