// Copyright 2022 The Ray Authors.
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

#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/asio/periodical_runner.h"

namespace ray {

/// A snapshot of memory information.
struct MemorySnapshot {
  /// The memory used.
  int64_t used_bytes;

  /// The total memory that can be used. >= used_bytes;
  int64_t total_bytes;

  friend std::ostream &operator<<(std::ostream &os,
                                  const MemorySnapshot &memory_snapshot);
};

/// Callback that runs at each monitoring interval.
///
/// \param is_usage_above_threshold true if memory usage is above the threshold
/// \param system_memory snapshot of system memory information.
/// \param usage_threshold the memory usage threshold.
/// threshold at this instant.
using MemoryUsageRefreshCallback = std::function<void(
    bool is_usage_above_threshold, MemorySnapshot system_memory, float usage_threshold)>;

/// Monitors the memory usage of the node.
/// It checks the memory usage p
/// This class is thread safe.
class MemoryMonitor {
 public:
  /// Constructor.
  ///
  /// \param io_service the event loop.
  /// \param usage_threshold a value in [0-1] to indicate the max usage.
  /// \param monitor_interval_ms the frequency to update the usage. 0 disables the
  /// the monitor and callbacks won't fire.
  /// \param monitor_callback function to execute on a dedicated thread owned by this
  /// monitor when the usage is refreshed.
  MemoryMonitor(instrumented_io_context &io_service,
                float usage_threshold,
                uint64_t monitor_interval_ms,
                MemoryUsageRefreshCallback monitor_callback);

 public:
  /// \param process_id the process id
  /// \return the used memory in bytes for the process
  int64_t GetProcessMemoryBytes(int64_t process_id) const;

 private:
  static constexpr char kCgroupsV1MemoryMaxPath[] =
      "/sys/fs/cgroup/memory/memory.limit_in_bytes";
  static constexpr char kCgroupsV1MemoryUsagePath[] =
      "/sys/fs/cgroup/memory/memory.usage_in_bytes";
  static constexpr char kCgroupsV2MemoryMaxPath[] = "/sys/fs/cgroup/memory.max";
  static constexpr char kCgroupsV2MemoryUsagePath[] = "/sys/fs/cgroup/memory.current";
  /// The logging frequency. Decoupled from how often the monitor runs.
  static constexpr uint32_t kLogIntervalMs = 5000;
  static constexpr int64_t kNull = -1;

  /// \param system_memory snapshot of system memory information.
  /// \return true if the memory usage of this node is above the threshold.
  bool IsUsageAboveThreshold(MemorySnapshot system_memory);

  /// \return the used and total memory in bytes.
  std::tuple<int64_t, int64_t> GetMemoryBytes();

  /// \return the used and total memory in bytes from Cgroup.
  std::tuple<int64_t, int64_t> GetCGroupMemoryBytes();

  /// \return the used and total memory in bytes for linux OS.
  std::tuple<int64_t, int64_t> GetLinuxMemoryBytes();

  /// \param smap_path file path to the smap file
  /// \return the used memory in bytes from the given smap file or kNull if the file does
  /// not exist or if it fails to read a valid value.
  static int64_t GetLinuxProcessMemoryBytesFromSmap(const std::string smap_path);

  /// \return the smaller of the two integers, kNull if both are kNull,
  /// or one of the values if the other is kNull.
  static int64_t NullableMin(int64_t left, int64_t right);

 private:
  FRIEND_TEST(MemoryMonitorTest, TestThresholdZeroMonitorAlwaysAboveThreshold);
  FRIEND_TEST(MemoryMonitorTest, TestThresholdOneMonitorAlwaysBelowThreshold);
  FRIEND_TEST(MemoryMonitorTest, TestUsageAtThresholdReportsTrue);
  FRIEND_TEST(MemoryMonitorTest, TestGetNodeAvailableMemoryAlwaysPositive);
  FRIEND_TEST(MemoryMonitorTest, TestGetNodeTotalMemoryEqualsFreeOrCGroup);

  /// Memory usage fraction between [0, 1]
  const double usage_threshold_;
  /// Callback function that executes at each monitoring interval,
  /// on a dedicated thread managed by this class.
  const MemoryUsageRefreshCallback monitor_callback_;
  PeriodicalRunner runner_;
};

}  // namespace ray
