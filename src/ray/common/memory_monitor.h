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
#include "ray/util/process.h"

namespace ray {

/// A snapshot of memory information.
struct MemorySnapshot {
  /// The memory used.
  int64_t used_bytes;

  /// The total memory that can be used. >= used_bytes;
  int64_t total_bytes;

  /// The per-process memory used;
  absl::flat_hash_map<pid_t, int64_t> process_used_bytes;

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
  /// \param min_memory_free_bytes to indicate the minimum amount of free space before it
  /// becomes over the threshold.
  /// \param monitor_interval_ms the frequency to update the usage. 0 disables the the
  /// monitor and callbacks won't fire.
  /// \param monitor_callback function to execute on a dedicated thread owned by this
  /// monitor when the usage is refreshed.
  MemoryMonitor(instrumented_io_context &io_service,
                float usage_threshold,
                int64_t min_memory_free_bytes,
                uint64_t monitor_interval_ms,
                MemoryUsageRefreshCallback monitor_callback);

 public:
  /// \param top_n the number of top memory-using processes
  /// \param system_memory the snapshot of memory usage
  /// \param proc_dir the directory to scan for the processes
  ///
  /// \return the debug string that contains up to the top N memory-using processes,
  /// empty if process directory is invalid
  static const std::string TopNMemoryDebugString(
      uint32_t top_n,
      const MemorySnapshot system_memory,
      const std::string proc_dir = kProcDirectory);

  /// \param proc_dir the directory to scan for the processes
  ///
  /// \return the pid to memory usage map for all the processes
  static const absl::flat_hash_map<pid_t, int64_t> GetProcessMemoryUsage(
      const std::string proc_dir = kProcDirectory);

 private:
  static constexpr char kCgroupsV1MemoryMaxPath[] =
      "/sys/fs/cgroup/memory/memory.limit_in_bytes";
  static constexpr char kCgroupsV1MemoryUsagePath[] =
      "/sys/fs/cgroup/memory/memory.usage_in_bytes";
  static constexpr char kCgroupsV1MemoryStatPath[] = "/sys/fs/cgroup/memory/memory.stat";
  static constexpr char kCgroupsV2MemoryMaxPath[] = "/sys/fs/cgroup/memory.max";
  static constexpr char kCgroupsV2MemoryUsagePath[] = "/sys/fs/cgroup/memory.current";
  static constexpr char kCgroupsV2MemoryStatPath[] = "/sys/fs/cgroup/memory.stat";
  static constexpr char kCgroupsV2MemoryStatInactiveKey[] = "inactive_file";
  static constexpr char kProcDirectory[] = "/proc";
  static constexpr char kCommandlinePath[] = "cmdline";
  /// The logging frequency. Decoupled from how often the monitor runs.
  static constexpr uint32_t kLogIntervalMs = 5000;
  static constexpr int64_t kNull = -1;

  /// \param system_memory snapshot of system memory information.
  /// \param threshold_bytes usage threshold in bytes.
  /// \return true if the memory usage of this node is above the threshold.
  static bool IsUsageAboveThreshold(MemorySnapshot system_memory,
                                    int64_t threshold_bytes);

  /// \return the used and total memory in bytes.
  std::tuple<int64_t, int64_t> GetMemoryBytes();

  /// \return the used and total memory in bytes from Cgroup.
  std::tuple<int64_t, int64_t> GetCGroupMemoryBytes();

  /// \param path file path to the memory stat file.
  ///
  /// \return the used memory for cgroup v1.
  static int64_t GetCGroupV1MemoryUsedBytes(const char *path);

  /// \param stat_path file path to the memory.stat file.
  /// \param usage_path file path to the memory.current file
  /// \return the used memory for cgroup v2. May return negative value, which should be
  /// discarded.
  static int64_t GetCGroupV2MemoryUsedBytes(const char *stat_path,
                                            const char *usage_path);

  /// \return the used and total memory in bytes for linux OS.
  std::tuple<int64_t, int64_t> GetLinuxMemoryBytes();

  /// \param smap_path file path to the smap file
  ///
  /// \return the used memory in bytes from the given smap file or kNull if the file does
  /// not exist or if it fails to read a valid value.
  static int64_t GetLinuxProcessMemoryBytesFromSmap(const std::string smap_path);

  /// \param proc_dir directory to scan for the process ids
  ///
  /// \return list of process ids found in the directory,
  /// or empty list if the directory doesn't exist
  static const std::vector<pid_t> GetPidsFromDir(
      const std::string proc_dir = kProcDirectory);

  /// \param pid the process id
  /// \param proc_dir directory to scan for the process ids
  ///
  /// \return the command line for the executing process,
  /// or empty string if the processs doesn't exist
  static const std::string GetCommandLineForPid(
      pid_t pid, const std::string proc_dir = kProcDirectory);

  /// Truncates string if it is too long and append '...'
  ///
  /// \param value the string to truncate
  /// \param max_length the max length of the string value to preserve
  ///
  /// \return the debug string that contains the top N memory using process
  static const std::string TruncateString(const std::string value, uint32_t max_length);

  /// \return the smaller of the two integers, kNull if both are kNull,
  /// or one of the values if the other is kNull.
  static int64_t NullableMin(int64_t left, int64_t right);

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

  /// \param pid the process id
  /// \param proc_dir the process directory
  ///
  /// \return the used memory in bytes for the process,
  /// kNull if the file doesn't exist or it fails to find the fields
  static int64_t GetProcessMemoryBytes(pid_t pid,
                                       const std::string proc_dir = kProcDirectory);

  /// \param top_n the number of top memory-using processes
  /// \param all_usage process to memory usage map
  ///
  /// \return the top N memory-using processes
  static const std::vector<std::tuple<pid_t, int64_t>> GetTopNMemoryUsage(
      uint32_t top_n, const absl::flat_hash_map<pid_t, int64_t> all_usage);

 private:
  FRIEND_TEST(MemoryMonitorTest, TestThresholdZeroMonitorAlwaysAboveThreshold);
  FRIEND_TEST(MemoryMonitorTest, TestThresholdOneMonitorAlwaysBelowThreshold);
  FRIEND_TEST(MemoryMonitorTest, TestUsageAtThresholdReportsFalse);
  FRIEND_TEST(MemoryMonitorTest, TestGetNodeAvailableMemoryAlwaysPositive);
  FRIEND_TEST(MemoryMonitorTest, TestGetNodeTotalMemoryEqualsFreeOrCGroup);
  FRIEND_TEST(MemoryMonitorTest, TestCgroupV1MemFileValidReturnsWorkingSet);
  FRIEND_TEST(MemoryMonitorTest, TestCgroupV1MemFileMissingFieldReturnskNull);
  FRIEND_TEST(MemoryMonitorTest, TestCgroupV1NonexistentMemFileReturnskNull);
  FRIEND_TEST(MemoryMonitorTest, TestCgroupV2FilesValidReturnsWorkingSet);
  FRIEND_TEST(MemoryMonitorTest, TestCgroupV2FilesValidKeyLastReturnsWorkingSet);
  FRIEND_TEST(MemoryMonitorTest, TestCgroupV2FilesValidNegativeWorkingSet);
  FRIEND_TEST(MemoryMonitorTest, TestCgroupV2FilesValidMissingFieldReturnskNull);
  FRIEND_TEST(MemoryMonitorTest, TestCgroupV2NonexistentStatFileReturnskNull);
  FRIEND_TEST(MemoryMonitorTest, TestCgroupV2NonexistentUsageFileReturnskNull);
  FRIEND_TEST(MemoryMonitorTest, TestMonitorPeriodSetMaxUsageThresholdCallbackExecuted);
  FRIEND_TEST(MemoryMonitorTest, TestMonitorPeriodDisableMinMemoryCallbackExecuted);
  FRIEND_TEST(MemoryMonitorTest, TestGetMemoryThresholdTakeGreaterOfTheTwoValues);
  FRIEND_TEST(MemoryMonitorTest, TestGetPidsFromDirOnlyReturnsNumericFilenames);
  FRIEND_TEST(MemoryMonitorTest, TestGetPidsFromNonExistentDirReturnsEmpty);
  FRIEND_TEST(MemoryMonitorTest, TestGetCommandLinePidExistReturnsValid);
  FRIEND_TEST(MemoryMonitorTest, TestGetCommandLineMissingFileReturnsEmpty);
  FRIEND_TEST(MemoryMonitorTest, TestShortStringNotTruncated);
  FRIEND_TEST(MemoryMonitorTest, TestLongStringTruncated);
  FRIEND_TEST(MemoryMonitorTest, TestTopNLessThanNReturnsMemoryUsedDesc);
  FRIEND_TEST(MemoryMonitorTest, TestTopNMoreThanNReturnsAllDesc);

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

  /// Callback function that executes at each monitoring interval,
  /// on a dedicated thread managed by this class.
  const MemoryUsageRefreshCallback monitor_callback_;
  PeriodicalRunner runner_;
};

}  // namespace ray
