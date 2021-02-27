#include "ray/gcs/gcs_server/gcs_resource_manager.h"
#include "ray/rpc/node_manager/node_manager_client_pool.h"

namespace ray {
namespace gcs {

class GcsResourceReportPoller {
 public:
  GcsResourceReportPoller(uint64_t max_concurrent_pulls,
                          std::shared_ptr<GcsResourceManager> gcs_resource_manager,
                          std::shared_ptr<rpc::NodeManagerClientPool> raylet_client_pool);

  /// This function is thread safe.
  void Start();

  /// This function is thread safe.
  void HandleNodeAdded(std::shared_ptr<rpc::GcsNodeInfo> node_info);

  /// This function is thread safe.
  void HandleNodeRemoved(std::shared_ptr<rpc::GcsNodeInfo> node_info);

 private:
  /// Called every timer tick.
  void Tick();
  void GetAllResourceUsage();
  void LaunchPulls();
  /* void NodeResourceReportReceived(const NodeID &node_id); */
  void PullRoundDone();











  std::unique_ptr<std::thread> polling_thread_;
  boost::asio::io_context polling_service_;

  uint64_t max_concurrent_pulls_;
  std::shared_ptr<GcsResourceManager> gcs_resource_manager_;
  std::shared_ptr<rpc::NodeManagerClientPool> raylet_client_pool_;
  boost::posix_time::milliseconds poll_period_ms_;

  struct PullState {
    NodeID node_id;
    rpc::Address address;
    int64_t last_pull_time;
    std::unique_ptr<boost::asio::deadline_timer> next_pull_timer;
  };

  std::unordered_map<NodeID, PullState> nodes_;
  std::deque<NodeID> to_pull;

  void TryPullResourceReport(const NodeID &node_id);
  void NodeResourceReportReceived(const NodeID &node_id);












  /* boost::posix_time::milliseconds poll_period_ms_; */
  boost::asio::deadline_timer poll_timer_;

  // A global lock for internal operations. This lock is shared between the main thread
  // and polling thread, so we should be mindful about how long we hold it.
  absl::Mutex mutex_;
  // Information about how to connect to all the nodes in the cluster.
  std::unordered_map<NodeID, rpc::Address> nodes_to_poll_;

  struct State {
    // The ongoing pulls.
    std::unordered_set<NodeID> inflight_pulls;
    // The set of nodes we still need to pull from. We may not be able to pull from them because of the inflight limit.
    std::vector<rpc::Address> to_pull;
  };

  State poll_state_;

};

}  // namespace gcs
}  // namespace ray
