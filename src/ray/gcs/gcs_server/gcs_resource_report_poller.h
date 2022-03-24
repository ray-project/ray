// Copyright 2021 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
#pragma once

#include "ray/common/asio/instrumented_io_context.h"
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
  GcsResourceReportPoller(
      std::shared_ptr<rpc::NodeManagerClientPool> raylet_client_pool,
      std::function<void(const rpc::ResourcesData &)> handle_resource_report,
      /* Default values should only be changed for testing. */
      std::function<int64_t(void)> get_current_time_milli =
          []() { return absl::GetCurrentTimeNanos() / (1000 * 1000); },
      std::function<void(
          const rpc::Address &,
          std::shared_ptr<rpc::NodeManagerClientPool> &,
          std::function<void(const Status &, const rpc::RequestResourceReportReply &)>)>
          request_report =
              [](const rpc::Address &address,
                 std::shared_ptr<rpc::NodeManagerClientPool> &raylet_client_pool,
                 std::function<void(const Status &,
                                    const rpc::RequestResourceReportReply &)> callback) {
                auto raylet_client = raylet_client_pool->GetOrConnectByAddress(address);
                raylet_client->RequestResourceReport(callback);
              });

  ~GcsResourceReportPoller();

  void Initialize(const GcsInitData &gcs_init_data);

  /// Start a thread to poll for resource updates.
  void Start();

  /// Stop polling for resource updates.
  void Stop();

  /// Event handler when a new node joins the cluster.
  void HandleNodeAdded(const rpc::GcsNodeInfo &node_info) LOCKS_EXCLUDED(mutex_);

  /// Event handler when a node leaves the cluster.
  void HandleNodeRemoved(const rpc::GcsNodeInfo &node_info) LOCKS_EXCLUDED(mutex_);

 private:
  // An asio service which does the polling work.
  instrumented_io_context polling_service_;
  // The associated thread it runs on.
  std::unique_ptr<std::thread> polling_thread_;
  // Timer tick to check when we should do another poll.
  PeriodicalRunner ticker_;

  // The maximum number of pulls that can occur at once.
  const uint64_t max_concurrent_pulls_;
  // The number of ongoing pulls.
  uint64_t inflight_pulls_;
  // The shared, thread safe pool of raylet clients, which we use to minimize connections.
  std::shared_ptr<rpc::NodeManagerClientPool> raylet_client_pool_;
  // Handle receiving a resource report (e.g. update the resource manager).
  // This function is guaranteed to be called on the main thread. It is not necessarily
  // safe to call it from the polling thread.
  std::function<void(const rpc::ResourcesData &)> handle_resource_report_;

  // Return the current time in miliseconds
  std::function<int64_t(void)> get_current_time_milli_;
  // Send the `RequestResourceReport` RPC.
  std::function<void(
      const rpc::Address &,
      std::shared_ptr<rpc::NodeManagerClientPool> &,
      std::function<void(const Status &, const rpc::RequestResourceReportReply &)>)>
      request_report_;
  // The minimum delay between two pull requests to the same thread.
  const int64_t poll_period_ms_;

  struct PullState {
    NodeID node_id;
    rpc::Address address;
    int64_t last_pull_time;
    int64_t next_pull_time;

    PullState(NodeID _node_id,
              rpc::Address _address,
              int64_t _last_pull_time,
              int64_t _next_pull_time)
        : node_id(_node_id),
          address(_address),
          last_pull_time(_last_pull_time),
          next_pull_time(_next_pull_time) {}

    ~PullState() {}
  };

  // A global lock for internal operations. This lock is shared between the main thread
  // and polling thread, so we should be mindful about how long we hold it.
  absl::Mutex mutex_;
  // All the state regarding how to and when to send a new pull request to a raylet.
  absl::flat_hash_map<NodeID, std::shared_ptr<PullState>> nodes_ GUARDED_BY(mutex_);
  // The set of all nodes which we are allowed to pull from. We can't necessarily pull
  // from this list immediately because we limit the number of concurrent pulls. This
  // queue should be sorted by time. The front should contain the first item to pull.
  std::deque<std::shared_ptr<PullState>> to_pull_queue_ GUARDED_BY(mutex_);

  /// Try to pull from the node. We may not be able to if it violates max concurrent
  /// pulls. This method is thread safe.
  void TryPullResourceReport() LOCKS_EXCLUDED(mutex_);
  /// Pull resource report without validation.
  void PullResourceReport(const std::shared_ptr<PullState> state);
  /// A resource report was successfully pulled (and the resource manager was already
  /// updated). This method is thread safe.
  void NodeResourceReportReceived(const std::shared_ptr<PullState> state)
      LOCKS_EXCLUDED(mutex_);

  friend class GcsResourceReportPollerTest;
};

}  // namespace gcs
}  // namespace ray
