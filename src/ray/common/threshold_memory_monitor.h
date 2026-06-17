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

#include <gtest/gtest_prod.h>

#include <cstdint>
#include <memory>
#include <optional>
#include <thread>

#include "ray/asio/periodical_runner.h"
#include "ray/common/cgroup2/cgroup_manager_interface.h"
#include "ray/common/memory_monitor_interface.h"

namespace ray {

/**
 * @brief Filesystem based memory monitor that triggers when
 * the memory usage exceeds a configured threshold.
 *
 * Monitors the memory usage of the node using /proc filesystem and cgroups.
 * It checks the memory usage periodically and invokes the callback.
 *
 * The threshold is recomputed on every poll from the live cgroup memory limit
 * so that runtime changes to the node's memory budget (e.g. Kubernetes
 * in-place pod resize, cgroup edits by an external operator) are reflected
 * without requiring the raylet to restart.
 *
 * This class is thread safe.
 */
class ThresholdMemoryMonitor : public MemoryMonitorInterface {
 public:
  /**
   * @param kill_workers_callback function to execute when the memory usage limit is
   *        exceeded.
   * @param usage_threshold fraction in [0, 1] of total memory to use as the threshold.
   *        Re-applied to the latest total bytes on every poll.
   * @param min_memory_free_bytes the min amount of free space to maintain. When set,
   *        the effective threshold is max(total * usage_threshold,
   *        total - min_memory_free_bytes) -- i.e. the *later* of the two trip
   *        points (note: this matches the existing MemoryMonitorUtils
   *        semantics; despite the field name "min_memory_free", a higher
   *        min_memory_free_bytes raises the trigger threshold rather than
   *        lowering it). Pass kNull to disable.
   * @param monitor_interval_ms the frequency to update the usage. 0 disables the monitor
   *        and callbacks won't fire.
   * @param resource_isolation_enabled flag to determine if resource isolation is enabled.
   *        Used to determine the mode of monitoring. If resource isolation is enabled,
   *        the threshold monitor will only monitor user application memory usage.
   * @param cgroup_manager Source of the user-slice memory upper bound when resource
   *        isolation is enabled. Must outlive this monitor. Unused otherwise.
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
                         float usage_threshold,
                         int64_t min_memory_free_bytes,
                         uint64_t monitor_interval_ms,
                         bool resource_isolation_enabled,
                         const CgroupManagerInterface &cgroup_manager,
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
  FRIEND_TEST(ThresholdMemoryMonitorTest,
              TestResourceIsolationThresholdReadFailureSkipsPoll);
  FRIEND_TEST(ThresholdMemoryMonitorTest, TestResourceIsolationInvalidThresholdSkipsPoll);

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

  /// Computes the current threshold in bytes against the latest cgroup total
  /// memory. This is recomputed on every poll so that runtime changes to the
  /// node's memory budget propagate without restarting the raylet.
  int64_t ComputeMemoryThresholdBytes(int64_t total_memory_bytes) const;

  /// Callback function that executes at each monitoring interval,
  /// on a dedicated thread managed by this class.
  KillWorkersCallback kill_workers_callback_;

  /// Flag to indicate that the worker killing event is in progress.
  std::atomic<bool> worker_killing_in_progress_;

  /// Fraction of total memory that triggers the callback when exceeded.
  float usage_threshold_;

  /// Min free bytes to maintain before triggering the callback. kNull disables.
  int64_t min_memory_free_bytes_;

  /// Flag to indicate if resource isolation is enabled.
  bool resource_isolation_enabled_;

  /// Source of the user-slice memory upper bound when resource isolation is
  /// enabled. Owned by the caller and must outlive this monitor.
  const CgroupManagerInterface &cgroup_manager_;

  /// The path to the root cgroup that the threshold monitor will
  /// use to monitor the system memory usage.
  std::string root_cgroup_path_;

  /// The path to the user cgroup that the threshold monitor will
  /// use to monitor the user application memory usage.
  std::string user_cgroup_path_;

  /// The path to the system cgroup that the threshold monitor will
  /// use to monitor the aggregate object store memory usage.
  std::string system_cgroup_path_;

  /// IO service for running the memory monitoring event loop.
  instrumented_io_context io_service_;

  /// Work guard to prevent the io service from exiting when no work.
  boost::asio::executor_work_guard<boost::asio::io_context::executor_type> work_guard_;

  /// Thread executing the io service. Started before the runner so the io_service
  /// is ready to process work. Explicitly joined in the destructor.
  std::thread thread_;

  /// Periodical runner for memory monitoring.
  std::shared_ptr<PeriodicalRunner> runner_;
};

}  // namespace ray
