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

#include "ray/gcs/leader_election/leader_elector.h"

#include "absl/time/clock.h"
#include "absl/time/time.h"
#include "ray/util/logging.h"

namespace ray {
namespace gcs {

LeaderElector::LeaderElector(LeaderElectionConfig config)
    : config_(std::move(config)), is_stopped_(false), is_leading_(false) {
  RAY_CHECK(config_.lease_client) << "Lease client must be non-null.";
  RAY_CHECK(!config_.holder_id.empty()) << "Holder ID must be non-empty.";
  // The safety deadline must be strictly smaller than the lease TTL duration to ensure
  // GCS steps down/suicides before another standby node can preemptively steal the lock.
  RAY_CHECK(config_.renew_deadline_seconds < config_.lease_duration_seconds)
      << "Renew deadline must be smaller than lease duration.";
}

LeaderElector::~LeaderElector() { Stop(); }

void LeaderElector::Run() {
  RAY_LOG(INFO) << "Starting leader election background thread for candidate: "
                << config_.holder_id;
  // Thread 1: Executes the network I/O loop to acquire and renew the Kubernetes Lease.
  election_thread_ = std::make_unique<std::thread>([this]() { Loop(); });
  // Thread 2: Lightweight watchdog that continuously checks that lease renewals are
  // succeeding within the safety renew deadline.
  watchdog_thread_ = std::make_unique<std::thread>([this]() { WatchdogLoop(); });
}

void LeaderElector::Stop() {
  // Flag background loops to exit immediately
  is_stopped_ = true;
  // Wake up all background threads from their wait/sleep states so they can exit.
  watchdog_cv_.notify_all();
  election_cv_.notify_all();

  // Safely join background execution threads first. Enforce self-join checks to prevent
  // deadlocks if Stop() is called from within callback loops.
  if (election_thread_ && election_thread_->joinable()) {
    if (std::this_thread::get_id() != election_thread_->get_id()) {
      election_thread_->join();
    } else {
      election_thread_->detach();
    }
  }
  if (watchdog_thread_ && watchdog_thread_->joinable()) {
    if (std::this_thread::get_id() != watchdog_thread_->get_id()) {
      watchdog_thread_->join();
    } else {
      watchdog_thread_->detach();
    }
  }

  if (is_leading_.load()) {
    is_leading_ = false;
    try {
      // Graceful exit: Voluntarily release the lease in Kubernetes to allow the
      // standby node to promote itself immediately.
      config_.lease_client->Release(config_.holder_id);
    } catch (const std::exception &e) {
      RAY_LOG(WARNING) << "Exception occurred during voluntary lease release: "
                       << e.what();
    }
  }
}

void LeaderElector::Loop() {
  while (!is_stopped_.load()) {
    // 1. Standby mode: Poll K8s to acquire the lease.
    Acquire();
    // 2. Active mode: Periodically renew lease heartbeats as long as we hold leadership.
    if (is_leading_.load() && !is_stopped_.load()) {
      Renew();
    }
  }
}

Status LeaderElector::TryAcquireOrRenew(
    std::function<Status(const std::string &, int, std::string &)> lease_op,
    const std::string &op_name,
    bool &success) {
  success = false;
  std::string current_leader = "";
  int ttl_seconds = config_.lease_duration_seconds;

  Status status = Status::OK();
  try {
    status = lease_op(config_.holder_id, ttl_seconds, current_leader);
    success = status.ok() && (current_leader == config_.holder_id);
  } catch (const std::exception &e) {
    std::string err_msg =
        std::string("Exception occurred during lease ") + op_name + ": " + e.what();
    RAY_LOG(WARNING) << err_msg;
    status = Status::IOError(err_msg);
    success = false;
    current_leader = "";
  }

  // If a new leader is observed in the cluster, fire the notification callback.
  if (!current_leader.empty() && current_leader != last_observed_leader_) {
    last_observed_leader_ = current_leader;
    if (config_.on_new_leader) {
      config_.on_new_leader(current_leader);
    }
  }

  if (success) {
    // Lock successfully acquired or renewed.
    auto now_steady = std::chrono::steady_clock::now().time_since_epoch();
    last_successful_renew_steady_ns_.store(
        std::chrono::duration_cast<std::chrono::nanoseconds>(now_steady).count());
    // Wake up the watchdog thread early to recalculate the remaining safety window.
    watchdog_cv_.notify_all();
  }

  return status;
}

void LeaderElector::Acquire() {
  while (!is_leading_.load() && !is_stopped_.load()) {
    bool acquired = false;
    Status status = TryAcquireOrRenew(
        [this](
            const std::string &holder_id, int ttl_seconds, std::string &current_leader) {
          return config_.lease_client->TryAcquire(holder_id, ttl_seconds, current_leader);
        },
        "acquisition",
        acquired);
    if (!status.ok()) {
      RAY_LOG(WARNING) << "Error during lease acquisition: " << status.ToString();
    }

    if (acquired) {
      RAY_LOG(INFO) << "Successfully acquired leader lease. Promoting to Leader!";
      is_leading_ = true;
      watchdog_cv_.notify_all();
      if (config_.on_started_leading) {
        config_.on_started_leading();
      }
      return;
    }

    std::unique_lock<std::mutex> lock(election_mutex_);
    election_cv_.wait_for(lock,
                          std::chrono::seconds(config_.retry_period_seconds),
                          [this]() { return is_stopped_.load(); });
  }
}

void LeaderElector::Renew() {
  while (is_leading_.load() && !is_stopped_.load()) {
    bool renewed = false;
    Status status = TryAcquireOrRenew(
        [this](
            const std::string &holder_id, int ttl_seconds, std::string &current_leader) {
          return config_.lease_client->Renew(holder_id, ttl_seconds, current_leader);
        },
        "renewal",
        renewed);
    if (!status.ok()) {
      RAY_LOG(WARNING) << "Transient lease renewal failure: " << status.ToString();
    }
    // If the API call succeeded, but renewed is false (e.g. lease acquired by another
    // node), step down immediately to prevent split-brain writes.
    if (status.ok() && !renewed) {
      RAY_LOG(ERROR) << "Lease ownership has been acquired by another node. Stepping "
                        "down immediately.";
      is_leading_ = false;
      if (config_.on_stopped_leading) {
        config_.on_stopped_leading();
      }
      return;
    }

    std::unique_lock<std::mutex> lock(election_mutex_);
    election_cv_.wait_for(lock,
                          std::chrono::seconds(config_.retry_period_seconds),
                          [this]() { return is_stopped_.load(); });
  }
}

void LeaderElector::WatchdogLoop() {
  while (!is_stopped_.load()) {
    // 1. If we are in standby (not leading), sleep on CV to prevent high CPU spin cycles.
    if (!is_leading_.load()) {
      std::unique_lock<std::mutex> lock(watchdog_mutex_);
      watchdog_cv_.wait_for(lock, kWatchdogStandbySleepMs, [this]() {
        return is_leading_.load() || is_stopped_.load();
      });
      continue;
    }

    // 2. Calculate monotonic elapsed time since the last successful lease renewal.
    // Load the last successful renewal steady timestamp first.
    int64_t last_renew_ns = last_successful_renew_steady_ns_.load();

    // Capture current steady clock next. This order guarantees that last_renew_ns
    // is always <= now_steady_ns, preventing negative elapsed time calculations.
    auto now_steady = std::chrono::steady_clock::now().time_since_epoch();
    int64_t now_steady_ns =
        std::chrono::duration_cast<std::chrono::nanoseconds>(now_steady).count();

    int64_t elapsed_ms = (now_steady_ns - last_renew_ns) / 1000000;
    int64_t remaining_ms = (config_.renew_deadline_seconds * 1000) - elapsed_ms;

    // 3. If the elapsed time has breached the renew deadline, step GCS down immediately!
    if (remaining_ms <= 0) {
      RAY_LOG(ERROR) << "WATCHDOG: GCS leader lease renewal deadline exceeded ("
                     << elapsed_ms / 1000.0 << "s > " << config_.renew_deadline_seconds
                     << "s). Stepping down immediately!";
      is_leading_ = false;
      if (config_.on_stopped_leading) {
        // Note: In GCS, this callback is bound to immediately suicide-terminate the
        // GCS process (RAY_LOG(FATAL)) to prevent split-brain zombie writes to Redis.
        config_.on_stopped_leading();
      }
      continue;
    }

    // 4. Sleep on the CV for the exact duration of the remaining safety deadline window.
    // If a renewal succeeds early, the election thread will call notify_all(), waking us
    // early to recalculate. If the process stops, notify_all() wakes us to exit
    // instantly.
    std::unique_lock<std::mutex> lock(watchdog_mutex_);
    watchdog_cv_.wait_for(lock, std::chrono::milliseconds(remaining_ms), [this]() {
      return is_stopped_.load();
    });
  }
}

}  // namespace gcs
}  // namespace ray
