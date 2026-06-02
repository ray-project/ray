// Copyright 2026 The Ray Authors.
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

#include <atomic>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>

#include "ray/common/status.h"
#include "ray/gcs/leader_election/leader_election_client_interface.h"

namespace ray {
namespace gcs {

struct LeaderElectionConfig {
  // The platform-agnostic lease client.
  std::shared_ptr<LeaderLeaseClientInterface> lease_client;

  // Identity of the candidate (e.g., GCS hostname).
  std::string holder_id;

  // Lease duration in seconds.
  int lease_duration_seconds = 15;

  // Renew deadline in seconds (must be smaller than lease_duration_seconds).
  int renew_deadline_seconds = 10;

  // Interval between retry attempts in seconds.
  int retry_period_seconds = 2;

  // Callbacks
  // NOTE: Callbacks are invoked from background threads (election or watchdog threads).
  // Callbacks MUST NOT synchronously destroy the LeaderElector instance (e.g. by deleting
  // it or resetting its owning unique_ptr directly in the callback), as that would cause
  // a deadlock or use-after-free during thread joining/detaching inside Stop() or the
  // destructor. Any destruction or cleanup of the LeaderElector must be dispatched
  // asynchronously to another thread.
  std::function<void()> on_started_leading;
  std::function<void()> on_stopped_leading;
  std::function<void(const std::string &)> on_new_leader;
};

class LeaderElector {
 public:
  explicit LeaderElector(LeaderElectionConfig config);
  ~LeaderElector();

  // Start the continuous leader election background loop. Non-blocking.
  void Run();

  // Stop the leader election loop and release lease if held.
  void Stop();

  // Check if the elector currently holds the lease.
  bool IsLeader() const { return is_leading_.load(); }

 private:
  // The background loop execution function.
  void Loop();

  // Inner loop to acquire the lease.
  void Acquire();

  // Inner loop to continuously renew the lease when we are the leader.
  void Renew();

  // Helper to execute either TryAcquire or Renew based on role.
  Status TryAcquireOrRenew(
      std::function<Status(const std::string &, int, std::string &)> lease_op,
      const std::string &op_name,
      bool &success);

  // Watchdog loop to verify leader lease renewal deadline asynchronously.
  void WatchdogLoop();

  LeaderElectionConfig config_;
  // Flashed to true by GCS main thread to request background loops to exit gracefully.
  std::atomic<bool> is_stopped_{false};
  // Tracks whether this candidate currently holds the leader election lease successfully.
  std::atomic<bool> is_leading_{false};
  std::unique_ptr<std::thread> election_thread_;
  std::unique_ptr<std::thread> watchdog_thread_;
  std::condition_variable watchdog_cv_;
  std::mutex watchdog_mutex_;
  std::condition_variable election_cv_;
  std::mutex election_mutex_;
  std::atomic<int64_t> last_successful_renew_steady_ns_{0};
  std::string last_observed_leader_;

  static constexpr std::chrono::milliseconds kWatchdogStandbySleepMs{100};
};

}  // namespace gcs
}  // namespace ray
