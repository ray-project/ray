#include "ray/gcs/gcs_server/gcs_resource_manager.h"
#include "ray/rpc/node_manager/node_manager_client_pool.h"

namespace ray {
namespace gcs {

/// Polls raylets on a separate thread to update GCS's view of the cluster's resource
/// utilization. This class creates and manages a polling thread. All public methods are
/// thread safe.
class GcsResourceReportPoller {
  /*
  This class roughly polls each node independently (with the exception of max
  concurrency). The process for polling a single node is as follows:

  A new node joins the cluster.
  1. (Main thread) Begin tracking the node, and begin the polling process.

  Main polling procedure.
  2. (Polling thread) Enqueue the node to be pulled.
  3. (Polling thread) Node is popped off the back of the queue and RequestResourceReport
  is sent to the raylet.
  4. (Main thread) The raylet responds and the resource manager is updated. This section
  is _not_ thread safe (i.e. should not modify the resource report poller state).
  5. (Polling thread) The RequestResourceReport continuation runs, scheduling the next
  pull time.
  6. (Polling thread) The next pull time occurs, and step 2 is repeated.

  The node leaves the cluster.
  7. Untrack the node. The next time the main polling procedure comes across the node, it
  should be dropped from the system.
   */

 public:
  GcsResourceReportPoller(uint64_t max_concurrent_pulls,
                          std::shared_ptr<GcsResourceManager> gcs_resource_manager,
                          std::shared_ptr<rpc::NodeManagerClientPool> raylet_client_pool);

  ~GcsResourceReportPoller();

  /// Start a thread to poll for resource updates.
  void Start();

  /// Stop polling for resource updates.
  void Stop();

  /// Event handler when a new node joins the cluster.
  void HandleNodeAdded(std::shared_ptr<rpc::GcsNodeInfo> node_info)
      LOCKS_EXCLUDED(mutex_);

  /// Event handler when a node leaves the cluster.
  void HandleNodeRemoved(std::shared_ptr<rpc::GcsNodeInfo> node_info)
      LOCKS_EXCLUDED(mutex_);

 private:
  // An asio service which does the polling work.
  boost::asio::io_context polling_service_;
  // The associated thread it runs on.
  std::unique_ptr<std::thread> polling_thread_;

  // The maximum number of pulls that can occur at once.
  const uint64_t max_concurrent_pulls_;
  // The number of ongoing pulls.
  uint64_t inflight_pulls_;
  // The resource manager which maintains GCS's view of the cluster's resource
  // utilization.
  std::shared_ptr<GcsResourceManager> gcs_resource_manager_;
  // The shared, thread safe pool of raylet clients, which we use to minimize connections.
  std::shared_ptr<rpc::NodeManagerClientPool> raylet_client_pool_;
  // The minimum delay between two pull requests to the same thread.
  const boost::posix_time::milliseconds poll_period_ms_;

  struct PullState {
    NodeID node_id;
    rpc::Address address;
    int64_t last_pull_time;
    std::unique_ptr<boost::asio::deadline_timer> next_pull_timer;
  };

  // A global lock for internal operations. This lock is shared between the main thread
  // and polling thread, so we should be mindful about how long we hold it.
  absl::Mutex mutex_;
  // All the state regarding how to and when to send a new pull request to a raylet.
  std::unordered_map<NodeID, PullState> nodes_;
  // The set of all nodes which we are allowed to pull from. We can't necessarily pull
  // from this list immediately because we limit the number of concurrent pulls.
  std::deque<NodeID> to_pull_queue_;

  /// Try to pull from the node. We may not be able to if it violates max concurrent
  /// pulls. This method is thread safe.
  void TryPullResourceReport(const NodeID &node_id) LOCKS_EXCLUDED(mutex_);
  /// Pull resource report without validation. This method is NOT thread safe.
  void PullResourceReport(PullState &state);
  /// A resource report was successfully pulled (and the resource manager was already
  /// updated). This method is thread safe.
  void NodeResourceReportReceived(const NodeID &node_id) LOCKS_EXCLUDED(mutex_);
};

}  // namespace gcs
}  // namespace ray
