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
#include <thread>

#include "ray/common/asio/periodical_runner.h"
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
   * @param usage_threshold a value in [0-1] to indicate the max usage.
   * @param min_memory_free_bytes to indicate the minimum amount of free space before it
   *        becomes over the threshold.
   * @param monitor_interval_ms the frequency to update the usage. 0 disables the monitor
   *        and callbacks won't fire.
   * @param root_cgroup_path the path to the root cgroup that the threshold monitor will
   *        use to calculate the system memory usage.
   */
  ThresholdMemoryMonitor(KillWorkersCallback kill_workers_callback,
                         float usage_threshold,
                         int64_t min_memory_free_bytes,
                         uint64_t monitor_interval_ms,
                         const std::string root_cgroup_path = kDefaultCgroupPath);

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
  bool IsEnabled() override;

 private:
  /**
   * @brief Checks if the memory usage is above the threshold.
   *
   * @param system_memory The snapshot of system memory usage.
   * @return True if the memory usage is above the threshold.
   */
  bool IsUsageAboveThreshold(const SystemMemorySnapshot &system_memory,
                             int64_t threshold_bytes);

  /// Callback function that executes at each monitoring interval,
  /// on a dedicated thread managed by this class.
  KillWorkersCallback kill_workers_callback_;

  /// Flag to indicate that the worker killing event is in progress.
  std::atomic<bool> worker_killing_in_progress_;

  /// The threshold in bytes that triggers the callback.
  /// Computed by: max(total_memory * usage_threshold, total_memory -
  /// min_memory_free_bytes)
  int64_t computed_threshold_bytes_;

  /// The path to the root cgroup that the threshold monitor will
  /// use to monitor the system memory usage.
  std::string root_cgroup_path_;

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
