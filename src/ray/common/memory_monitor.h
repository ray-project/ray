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

#include <boost/filesystem.hpp>

#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/asio/periodical_runner.h"

namespace ray {
/// Callback that runs at each monitoring interval.
///
/// \param is_usage_above_threshold true if memory usage is above the usage
/// threshold at this instant.
using MemoryUsageRefreshCallback = std::function<void(bool is_usage_above_threshold)>;

/// Monitors the memory usage of the node.
/// It checks the memory usage p
/// This class is thread safe.
class MemoryMonitor {
 public:
  /// Constructor.
  ///
  /// \param usage_threshold a value in [0-1] to indicate the max usage.
  /// \param monitor_interval_ms the frequency to update the usage. 0 disables the
  /// the monitor and callbacks won't fire.
  /// \param monitor_callback function to execute on a dedicated thread owned by this
  /// monitor when the usage is refreshed.
  MemoryMonitor(float usage_threshold,
                uint64_t monitor_interval_ms,
                MemoryUsageRefreshCallback monitor_callback);

  ~MemoryMonitor();

 private:
  static constexpr char kCgroupsV1MemoryMaxPath[] =
      "/sys/fs/cgroup/memory/memory.limit_in_bytes";
  static constexpr char kCgroupsV1MemoryUsagePath[] =
      "/sys/fs/cgroup/memory/memory.usage_in_bytes";
  static constexpr char kCgroupsV2MemoryMaxPath[] = "/sys/fs/cgroup/memory.max";
  static constexpr char kCgroupsV2MemoryUsagePath[] = "/sys/fs/cgroup/memory.current";
  static constexpr uint32_t kLogIntervalMs = 5000;
  static constexpr int64_t kNull = -1;

  /// Returns true if the memory usage of this node is above the threshold.
  bool IsUsageAboveThreshold();

  /// Returns the used and total memory in bytes.
  std::tuple<int64_t, int64_t> GetMemoryBytes();

  /// Returns the used and total memory in bytes from Cgroup.
  std::tuple<int64_t, int64_t> GetCGroupMemoryBytes();

  /// Returns the used and total memory in bytes for linux OS.
  std::tuple<int64_t, int64_t> GetLinuxMemoryBytes();

  /// Returns the smaller of the two integers, kNull if both are kNull,
  /// or one of the values if the other is kNull.
  static int64_t NullableMin(int64_t left, int64_t right);

 private:
  FRIEND_TEST(MemoryMonitorTest, TestThresholdZeroMonitorAlwaysAboveThreshold);
  FRIEND_TEST(MemoryMonitorTest, TestThresholdOneMonitorAlwaysBelowThreshold);
  FRIEND_TEST(MemoryMonitorTest, TestGetNodeAvailableMemoryAlwaysPositive);
  FRIEND_TEST(MemoryMonitorTest, TestGetNodeTotalMemoryEqualsFreeOrCGroup);

  /// Memory usage fraction between [0, 1]
  const double usage_threshold_;
  /// Callback function that executes at each monitoring interval,
  /// on a dedicated thread managed by this class.
  const MemoryUsageRefreshCallback monitor_callback_;
  instrumented_io_context io_context_;
  std::thread monitor_thread_;
  PeriodicalRunner runner_;
};

}  // namespace ray
