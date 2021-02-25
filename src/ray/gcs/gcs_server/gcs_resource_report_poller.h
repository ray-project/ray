#include "ray/gcs/gcs_server/gcs_resource_manager.h"
#include "ray/rpc/node_manager/node_manager_client_pool.h"

namespace ray {
namespace gcs {

class GcsResourceReportPoller {
 public:
  GcsResourceReportPoller(std::shared_ptr<GcsResourceManager> gcs_resource_manager,
                          std::shared_ptr<rpc::NodeManagerClientPool> raylet_client_pool);

  /// This function is thread safe.
  void Start();

  /// This function is thread safe.
  void HandleNodeAdded(std::shared_ptr<rpc::GcsNodeInfo> node_info);

  /// This function is thread safe.
  void HandleNodeRemoved(std::shared_ptr<rpc::GcsNodeInfo> node_info);

 private:
  void GetAllResourceUsage();

  /// Called every timer tick.
  void Tick();

  std::unique_ptr<std::thread> polling_thread_;
  boost::asio::io_context polling_service_;

  std::shared_ptr<GcsResourceManager> gcs_resource_manager_;
  std::shared_ptr<rpc::NodeManagerClientPool> raylet_client_pool_;
  boost::asio::deadline_timer poll_timer_;

  // A global lock for internal operations. This lock is shared between the main thread
  // and polling thread, so we should be mindful about how long we hold it.
  absl::Mutex mutex_;
  // Information about how to connect to all the nodes in the cluster.
  std::unordered_map<NodeID, rpc::Address> nodes_to_poll_;
};

}  // namespace gcs
}  // namespace ray
