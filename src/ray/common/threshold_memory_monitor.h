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

#include <cstdint>
#include <memory>
#include <string>
#include <tuple>
#include <vector>

#include "ray/common/asio/periodical_runner.h"
#include "ray/common/memory_monitor.h"

namespace ray {

/// File-system based memory monitor with threshold support.
/// Monitors the memory usage of the node using /proc filesystem and cgroups.
/// It checks the memory usage periodically and invokes the callback.
/// This class is thread safe.
class ThresholdMemoryMonitor : public MemoryMonitor {
 public:
  /// Constructor.
  ///
  /// \param io_service the event loop.
  /// \param usage_threshold a value in [0-1] to indicate the max usage.
  /// \param min_memory_free_bytes to indicate the minimum amount of free space before it
  /// becomes over the threshold.
  /// \param monitor_interval_ms the frequency to update the usage. 0 disables the the
  /// monitor and callbacks won't fire.
  /// \param monitor_callback function to execute on a dedicated thread owned by this
  /// monitor when the usage is refreshed.
  ThresholdMemoryMonitor(instrumented_io_context &io_service,
                         KillWorkersCallback kill_workers_callback,
                         float usage_threshold,
                         int64_t min_memory_free_bytes,
                         uint64_t monitor_interval_ms);

 public:
  /// Cgroup and proc filesystem constants for memory monitoring.
  static constexpr char kCgroupsV1MemoryMaxPath[] =
      "/sys/fs/cgroup/memory/memory.limit_in_bytes";
  static constexpr char kCgroupsV1MemoryUsagePath[] =
      "/sys/fs/cgroup/memory/memory.usage_in_bytes";
  static constexpr char kCgroupsV1MemoryStatPath[] = "/sys/fs/cgroup/memory/memory.stat";
  static constexpr char kCgroupsV1MemoryStatInactiveFileKey[] = "total_inactive_file";
  static constexpr char kCgroupsV1MemoryStatActiveFileKey[] = "total_active_file";
  static constexpr char kCgroupsV2MemoryMaxPath[] = "/sys/fs/cgroup/memory.max";
  static constexpr char kCgroupsV2MemoryUsagePath[] = "/sys/fs/cgroup/memory.current";
  static constexpr char kCgroupsV2MemoryStatPath[] = "/sys/fs/cgroup/memory.stat";
  static constexpr char kCgroupsV2MemoryStatInactiveFileKey[] = "inactive_file";
  static constexpr char kCgroupsV2MemoryStatActiveFileKey[] = "active_file";

  /// \param system_memory snapshot of system memory information.
  /// \param threshold_bytes usage threshold in bytes.
  /// \return true if the memory usage of this node is above the threshold.
  static bool IsUsageAboveThreshold(MemorySnapshot system_memory,
                                    int64_t threshold_bytes);

  /// \param stat_path file path to the memory.stat file.
  /// \param usage_path file path to the memory.current file
  /// \param inactive_file_key inactive_file key name in memory.stat file
  /// \param active_file_key active_file key name in memory.stat file
  /// \return the used memory for cgroup. May return negative value, which should be
  /// discarded.
  static int64_t GetCGroupMemoryUsedBytes(const char *stat_path,
                                          const char *usage_path,
                                          const char *inactive_file_key,
                                          const char *active_file_key);

  /// Computes the memory threshold, where
  /// Memory usage threshold = max(total_memory * usage_threshold_, total_memory -
  /// min_memory_free_bytes)
  ///
  /// \param total_memory_bytes the total amount of memory available in the system.
  /// \param usage_threshold a value in [0-1] to indicate the max usage.
  /// \param min_memory_free_bytes the min amount of free space to maintain before it is
  /// exceeding the threshold.
  ///
  /// \return the memory threshold.
  static int64_t GetMemoryThreshold(int64_t total_memory_bytes,
                                    float usage_threshold,
                                    int64_t min_memory_free_bytes);

  /// \return the used and total memory in bytes.
  std::tuple<int64_t, int64_t> GetMemoryBytes();

  /// \return the used and total memory in bytes from Cgroup.
  std::tuple<int64_t, int64_t> GetCGroupMemoryBytes();

 private:
  /// \return the used and total memory in bytes for linux OS.
  std::tuple<int64_t, int64_t> GetLinuxMemoryBytes();

  FRIEND_TEST(ThresholdMemoryMonitorTest, TestThresholdZeroMonitorAlwaysAboveThreshold);
  FRIEND_TEST(ThresholdMemoryMonitorTest, TestThresholdOneMonitorAlwaysBelowThreshold);
  FRIEND_TEST(ThresholdMemoryMonitorTest, TestUsageAtThresholdReportsFalse);
  FRIEND_TEST(ThresholdMemoryMonitorTest, TestGetNodeAvailableMemoryAlwaysPositive);
  FRIEND_TEST(ThresholdMemoryMonitorTest, TestGetNodeTotalMemoryEqualsFreeOrCGroup);
  FRIEND_TEST(ThresholdMemoryMonitorTest, TestCgroupFilesValidReturnsWorkingSet);
  FRIEND_TEST(ThresholdMemoryMonitorTest, TestCgroupFilesValidKeyLastReturnsWorkingSet);
  FRIEND_TEST(ThresholdMemoryMonitorTest, TestCgroupFilesValidNegativeWorkingSet);
  FRIEND_TEST(ThresholdMemoryMonitorTest, TestCgroupFilesValidMissingFieldReturnskNull);
  FRIEND_TEST(ThresholdMemoryMonitorTest, TestCgroupNonexistentStatFileReturnskNull);
  FRIEND_TEST(ThresholdMemoryMonitorTest, TestCgroupNonexistentUsageFileReturnskNull);
  FRIEND_TEST(ThresholdMemoryMonitorTest,
              TestMonitorPeriodSetMaxUsageThresholdCallbackExecuted);
  FRIEND_TEST(ThresholdMemoryMonitorTest,
              TestMonitorPeriodDisableMinMemoryCallbackExecuted);
  FRIEND_TEST(ThresholdMemoryMonitorTest,
              TestGetMemoryThresholdTakeGreaterOfTheTwoValues);
  FRIEND_TEST(ThresholdMemoryMonitorTest, TestGetPidsFromDirOnlyReturnsNumericFilenames);
  FRIEND_TEST(ThresholdMemoryMonitorTest, TestGetPidsFromNonExistentDirReturnsEmpty);
  FRIEND_TEST(ThresholdMemoryMonitorTest, TestGetCommandLinePidExistReturnsValid);
  FRIEND_TEST(ThresholdMemoryMonitorTest, TestGetCommandLineMissingFileReturnsEmpty);
  FRIEND_TEST(ThresholdMemoryMonitorTest, TestShortStringNotTruncated);
  FRIEND_TEST(ThresholdMemoryMonitorTest, TestLongStringTruncated);
  FRIEND_TEST(ThresholdMemoryMonitorTest, TestTopNLessThanNReturnsMemoryUsedDesc);
  FRIEND_TEST(ThresholdMemoryMonitorTest, TestTopNMoreThanNReturnsAllDesc);

  /// Memory usage fraction between [0, 1]
  const float usage_threshold_;

  /// Indicates the minimum amount of free space to retain before it considers
  /// the usage as above threshold.
  const int64_t min_memory_free_bytes_;

  /// The computed threshold in bytes based on usage_threshold_ and
  /// min_memory_free_bytes_.
  int64_t computed_threshold_bytes_;

  /// The computed threshold fraction on usage_threshold_ and min_memory_free_bytes_.
  float computed_threshold_fraction_;

  /// Periodical runner for memory monitoring.
  std::shared_ptr<PeriodicalRunner> runner_;
};

}  // namespace ray
