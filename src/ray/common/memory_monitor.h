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
#include "ray/util/compat.h"

namespace ray {

/// A snapshot of memory information.
struct SystemMemorySnapshot {
  /// The memory used.
  int64_t used_bytes;

  /// The total memory that can be used. >= used_bytes.
  int64_t total_bytes;

  friend std::ostream &operator<<(std::ostream &os,
                                  const SystemMemorySnapshot &memory_snapshot);
};

using ProcessesMemorySnapshot = absl::flat_hash_map<pid_t, int64_t>;

/**
 * @brief Callback to call when the memory usage exceeds the threshold.
 *
 * @param is_usage_above_threshold true if memory usage is above the threshold.
 * @param system_memory Snapshot of system memory information.
 * @param usage_threshold The memory usage threshold.
 */
using MemoryUsageRefreshCallback = std::function<void(bool is_usage_above_threshold,
                                                      SystemMemorySnapshot system_memory,
                                                      float usage_threshold)>;

/**
 * @brief Monitors the memory usage of the node
 *        and kills workers if memory usage exceeds the threshold.
 *
 * This class is thread safe.
 */
class MemoryMonitor {
 public:
  /**
   * @param io_service the io context the memory monitor will run on
   *                   and post the callback to.
   * @param usage_threshold A value in [0-1] to indicate the max usage.
   * @param min_memory_free_bytes The minimum amount of free space before it
   *        becomes over the threshold.
   * @param monitor_interval_ms The frequency to update the usage. 0 disables the
   *        monitor and callbacks won't fire.
   * @param monitor_callback Function to execute on a dedicated thread owned by this
   *        monitor when the usage is refreshed.
   * @param root_cgroup_path The path to the root cgroup that the threshold monitor will
   *        use to calculate the system memory usage.
   */
  MemoryMonitor(instrumented_io_context &io_service,
                float usage_threshold,
                int64_t min_memory_free_bytes,
                uint64_t monitor_interval_ms,
                MemoryUsageRefreshCallback monitor_callback,
                const std::string root_cgroup_path = kDefaultCgroupPath);

  /**
   * @param top_n The number of top memory-using processes.
   * @param process_memory_snapshot The snapshot of per process memory usage.
   * @param proc_dir The directory to scan for the processes.
   * @return The debug string that contains up to the top N memory-using processes,
   *         empty if process directory is invalid.
   */
  static const std::string TopNMemoryDebugString(
      uint32_t top_n,
      const ProcessesMemorySnapshot &process_memory_snapshot,
      const std::string proc_dir = kProcDirectory);

  /**
   * @brief Takes a snapshot of system memory usage.
   *
   * @param root_cgroup_path The path to the root cgroup.
   * @param proc_dir The proc directory path.
   * @return The used and total memory in bytes.
   */
  static const SystemMemorySnapshot TakeSystemMemorySnapshot(
      const std::string root_cgroup_path, const std::string proc_dir = kProcDirectory);

  /**
   * @brief Takes a snapshot of per-process memory usage.
   *
   * @param proc_dir The directory to scan for the processes.
   * @return The pid to memory usage map for all the processes.
   */
  static const ProcessesMemorySnapshot TakePerProcessMemorySnapshot(
      const std::string proc_dir = kProcDirectory);

 private:
  static constexpr char kDefaultCgroupPath[] = "/sys/fs/cgroup";
  static constexpr char kCgroupsV1MemoryMaxPath[] = "memory/memory.limit_in_bytes";
  static constexpr char kCgroupsV1MemoryUsagePath[] = "memory/memory.usage_in_bytes";
  static constexpr char kCgroupsV1MemoryStatPath[] = "memory/memory.stat";
  static constexpr char kCgroupsV1MemoryStatInactiveFileKey[] = "total_inactive_file";
  static constexpr char kCgroupsV1MemoryStatActiveFileKey[] = "total_active_file";
  static constexpr char kCgroupsV2MemoryMaxPath[] = "memory.max";
  static constexpr char kCgroupsV2MemoryUsagePath[] = "memory.current";
  static constexpr char kCgroupsV2MemoryStatPath[] = "memory.stat";
  static constexpr char kCgroupsV2MemoryStatInactiveFileKey[] = "inactive_file";
  static constexpr char kCgroupsV2MemoryStatActiveFileKey[] = "active_file";
  static constexpr char kProcDirectory[] = "/proc";
  static constexpr char kCommandlinePath[] = "cmdline";
  /// The logging frequency. Decoupled from how often the monitor runs.
  static constexpr uint32_t kLogIntervalMs = 5000;
  static constexpr int64_t kNull = -1;

  /**
   * @brief Checks if memory usage is above the threshold.
   *
   * @param system_memory Snapshot of system memory information.
   * @param threshold_bytes Usage threshold to check against.
   * @return true if the memory usage of this node is above the threshold.
   */
  static bool IsUsageAboveThreshold(const SystemMemorySnapshot &system_memory,
                                    int64_t threshold_bytes);

  /**
   * @brief Gets memory information from the given cgroup.
   *
   * @param root_cgroup_path The path to the root cgroup
   *                         to read the memory usage from.
   * @return The used and total memory in bytes from the cgroup.
   */
  static std::tuple<int64_t, int64_t> GetCGroupMemoryBytes(
      const std::string root_cgroup_path);

  /**
   * @brief Gets the current memory usage for cgroup.
   *
   * @param stat_path File path to the memory.stat file.
   * @param usage_path File path to the memory.current file.
   * @param inactive_file_key inactive_file key name in memory.stat file.
   * @param active_file_key active_file key name in memory.stat file.
   * @return The used memory for cgroup. May return negative value, which should be
   *         discarded.
   */
  static int64_t GetCGroupMemoryUsedBytes(const char *stat_path,
                                          const char *usage_path,
                                          const char *inactive_file_key,
                                          const char *active_file_key);

  /**
   * @brief Gets memory information for Linux OS.
   *
   * @param proc_dir The proc directory path to read the memory usage from.
   * @return The used and total memory in bytes for Linux OS.
   */
  static std::tuple<int64_t, int64_t> GetLinuxMemoryBytes(const std::string proc_dir);

  /**
   * @brief Gets the used memory from the smap file.
   *
   * @param smap_path File path to the smap file.
   * @return The used memory in bytes from the given smap file or kNull if the file does
   *         not exist or if it fails to read a valid value.
   */
  static int64_t GetLinuxProcessMemoryBytesFromSmap(const std::string smap_path);

  /**
   * @brief Gets process IDs from a directory.
   *
   * @param proc_dir Directory to scan for the process IDs.
   * @return List of process IDs found in the directory,
   *         or empty list if the directory doesn't exist.
   */
  static const std::vector<pid_t> GetPidsFromDir(
      const std::string proc_dir = kProcDirectory);

  /**
   * @brief Gets the command line for a process.
   *
   * @param pid The process ID.
   * @param proc_dir Directory to scan for the process IDs.
   * @return The command line for the executing process,
   *         or empty string if the process doesn't exist.
   */
  static const std::string GetCommandLineForPid(
      pid_t pid, const std::string proc_dir = kProcDirectory);

  /**
   * @brief Truncates string if it is too long and appends '...'.
   *
   * @param value The string to truncate.
   * @param max_length The max length of the string value to preserve.
   * @return The truncated string.
   */
  static const std::string TruncateString(const std::string value, uint32_t max_length);

  /**
   * @brief Returns the smaller of the two integers with null handling.
   *
   * @param left First integer value.
   * @param right Second integer value.
   * @return The smaller of the two integers, kNull if both are kNull,
   *         or one of the values if the other is kNull.
   */
  static int64_t NullableMin(int64_t left, int64_t right);

  /**
   * @brief Computes the memory threshold.
   *
   * @details Memory usage threshold = max(total_memory * usage_threshold_, total_memory -
   * min_memory_free_bytes)
   *
   * @param total_memory_bytes The total amount of memory available in the system.
   * @param usage_threshold A value in [0-1] to indicate the max usage.
   * @param min_memory_free_bytes The min amount of free space to maintain before it is
   *        exceeding the threshold.
   * @return The memory threshold.
   */
  static int64_t GetMemoryThreshold(int64_t total_memory_bytes,
                                    float usage_threshold,
                                    int64_t min_memory_free_bytes);

  /**
   * @brief Gets the used memory for a process.
   *
   * @param pid The process ID.
   * @param proc_dir The process directory.
   * @return The used memory in bytes for the process,
   *         kNull if the file doesn't exist or it fails to find the fields.
   */
  static int64_t GetProcessMemoryBytes(pid_t pid,
                                       const std::string proc_dir = kProcDirectory);

  /**
   * @brief Gets the top N memory-using processes.
   *
   * @param top_n The number of top memory-using processes.
   * @param all_usage Process to memory usage map.
   * @return The top N memory-using processes.
   */
  static const std::vector<std::tuple<pid_t, int64_t>> GetTopNMemoryUsage(
      uint32_t top_n, const ProcessesMemorySnapshot &all_usage);

  FRIEND_TEST(MemoryMonitorTest, TestThresholdZeroMonitorAlwaysAboveThreshold);
  FRIEND_TEST(MemoryMonitorTest, TestThresholdOneMonitorAlwaysBelowThreshold);
  FRIEND_TEST(MemoryMonitorTest, TestUsageAtThresholdReportsFalse);
  FRIEND_TEST(MemoryMonitorTest, TestGetNodeAvailableMemoryAlwaysPositive);
  FRIEND_TEST(MemoryMonitorTest, TestGetNodeTotalMemoryEqualsFreeOrCGroup);
  FRIEND_TEST(MemoryMonitorTest, TestCgroupFilesValidReturnsWorkingSet);
  FRIEND_TEST(MemoryMonitorTest, TestCgroupFilesValidKeyLastReturnsWorkingSet);
  FRIEND_TEST(MemoryMonitorTest, TestCgroupFilesValidNegativeWorkingSet);
  FRIEND_TEST(MemoryMonitorTest, TestCgroupFilesValidMissingFieldReturnskNull);
  FRIEND_TEST(MemoryMonitorTest, TestCgroupNonexistentStatFileReturnskNull);
  FRIEND_TEST(MemoryMonitorTest, TestCgroupNonexistentUsageFileReturnskNull);
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

  /// The path to the root cgroup that the threshold monitor will
  /// use to calculate the system memory usage.
  const std::string root_cgroup_path_;

  /// Callback function that executes at each monitoring interval,
  /// on a dedicated thread managed by this class.
  const MemoryUsageRefreshCallback monitor_callback_;
  std::shared_ptr<PeriodicalRunner> runner_;
};

}  // namespace ray
