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

#include <cstdint>
#include <memory>
#include <optional>
#include <thread>

#include "ray/asio/periodical_runner.h"
#include "ray/asio/periodical_runner_interface.h"
#include "ray/common/memory_monitor_interface.h"

namespace ray {

/**
 * @brief Filesystem based memory monitor that triggers when
 * the memory usage exceeds a configured threshold.
 *
 * Monitors the memory usage of the node using /proc filesystem and cgroups.
 * It checks the memory usage periodically and invokes the callback.
 * This class is thread safe.
 */
class ThresholdMemoryMonitor : public MemoryMonitorInterface {
 public:
  /**
   * @param kill_workers_callback function to execute when the memory usage limit is
   *        exceeded.
   * @param memory_usage_threshold_bytes the threshold in bytes that triggers the
   *        kill callback if exceeded.
   * @param monitor_interval_ms the frequency to update the usage. 0 disables the monitor
   *        and callbacks won't fire.
   * @param resource_isolation_enabled flag to determine if resource isolation is enabled.
   *        Used to determine the mode of monitoring. If resource isolation is enabled,
   *        the threshold monitor will only monitor user application memory usage.
   * @param root_cgroup_path the path to the root cgroup that the threshold monitor will
   *        use to calculate the system memory usage.
   * @param user_cgroup_path the path to the user cgroup that the threshold monitor will
   *        use to calculate the user application memory usage. Not used if
   *        resource isolation is disabled.
   * @param system_cgroup_path the path to the system cgroup that the threshold monitor
   *        will use to calculate the aggregate object store memory usage. Not used if
   *        resource isolation is disabled.
   */
  ThresholdMemoryMonitor(KillWorkersCallback kill_workers_callback,
                         int64_t memory_usage_threshold_bytes,
                         uint64_t monitor_interval_ms,
                         bool resource_isolation_enabled,
                         const std::string &root_cgroup_path = kDefaultCgroupPath,
                         const std::string &user_cgroup_path = kDefaultCgroupPath,
                         const std::string &system_cgroup_path = kDefaultCgroupPath);

  /**
   * @brief Constructor that uses an externally-provided periodical runner instead
   *        of spawning a dedicated IO thread. Intended for tests, where a
   *        FakePeriodicalRunner driven by a FakeClock allows the periodic memory
   *        check to be triggered deterministically. `runner` must outlive this
   *        monitor.
   *
   * All other parameters behave as in the production constructor above.
   */
  ThresholdMemoryMonitor(PeriodicalRunnerInterface &runner,
                         KillWorkersCallback kill_workers_callback,
                         int64_t memory_usage_threshold_bytes,
                         uint64_t monitor_interval_ms,
                         bool resource_isolation_enabled,
                         const std::string &root_cgroup_path = kDefaultCgroupPath,
                         const std::string &user_cgroup_path = kDefaultCgroupPath,
                         const std::string &system_cgroup_path = kDefaultCgroupPath);

  ~ThresholdMemoryMonitor() override;

  /**
   * @brief Enables the memory monitor to trigger the kill callback.
   */
  void Enable() override;

  /**
   * @brief Disables the memory monitor from triggering the kill callback.
   */
  void Disable() override;

  /**
   * @return True if the memory monitor is enabled, false otherwise.
   */
  bool IsEnabled() const override;

 private:
  /// Registers the periodic memory check on `runner_`. Shared by both
  /// constructors.
  void RegisterPeriodicCheck(uint64_t monitor_interval_ms);

  /**
   * @brief Checks if the memory usage on the host exceeds the threshold.
   *
   * @return True if the memory usage is above the threshold.
   */
  /// Returns the memory snapshot if the host memory usage exceeds the threshold,
  /// or std::nullopt otherwise.
  std::optional<MemoryUsageSnapshot> IsHostMemoryThresholdExceeded();

  /**
   * @brief Checks if the memory usage across all user slice processes,
   *        including their object store usage, exceeds their allowed
   *        threshold under resource isolation mode on this node.
   *
   * @return The memory snapshot if above threshold, std::nullopt otherwise.
   */
  std::optional<MemoryUsageSnapshot> IsResourceIsolationThresholdExceeded();

  /// Callback function that executes at each monitoring interval,
  /// on a dedicated thread managed by this class.
  KillWorkersCallback kill_workers_callback_;

  /// Flag to indicate that the worker killing event is in progress.
  std::atomic<bool> worker_killing_in_progress_;

  /// The threshold in bytes that triggers the callback.
  int64_t memory_usage_threshold_bytes_;

  /// Flag to indicate if resource isolation is enabled.
  bool resource_isolation_enabled_;

  /// The path to the root cgroup that the threshold monitor will
  /// use to monitor the system memory usage.
  std::string root_cgroup_path_;

  /// The path to the user cgroup that the threshold monitor will
  /// use to monitor the user application memory usage.
  std::string user_cgroup_path_;

  /// The path to the system cgroup that the threshold monitor will
  /// use to monitor the aggregate object store memory usage.
  std::string system_cgroup_path_;

  /// IO service for running the memory monitoring event loop. Only created by the
  /// production constructor; null when an external runner is injected.
  std::unique_ptr<instrumented_io_context> io_service_;

  /// Work guard to prevent the io service from exiting when no work. Only created
  /// by the production constructor.
  std::unique_ptr<
      boost::asio::executor_work_guard<boost::asio::io_context::executor_type>>
      work_guard_;

  /// Thread executing the io service. Started before the runner so the io_service
  /// is ready to process work. Explicitly joined in the destructor. Only created
  /// by the production constructor.
  std::unique_ptr<std::thread> thread_;

  /// Periodical runner owned by the production constructor (backed by io_service_).
  /// Null when an external runner is injected.
  std::shared_ptr<PeriodicalRunner> owned_runner_;

  /// The runner used to schedule the periodic memory check. References either
  /// owned_runner_ (production) or an externally-provided runner (tests).
  PeriodicalRunnerInterface &runner_;
};

}  // namespace ray
