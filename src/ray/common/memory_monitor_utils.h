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
#include <string>
#include <tuple>
#include <vector>

#include "ray/common/memory_monitor_interface.h"
#include "ray/util/compat.h"

namespace ray {

class MemoryMonitorUtils {
 public:
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

 private:
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
   * @brief Gets the current memory usage for the cgroup
   *        whose memory usage is specified by the given paths.
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

 private:
  FRIEND_TEST(MemoryMonitorUtilsTest, TestGetNodeTotalMemoryEqualsFreeOrCGroup);
  FRIEND_TEST(MemoryMonitorUtilsTest, TestCgroupFilesValidReturnsWorkingSet);
  FRIEND_TEST(MemoryMonitorUtilsTest, TestCgroupFilesValidKeyLastReturnsWorkingSet);
  FRIEND_TEST(MemoryMonitorUtilsTest, TestCgroupFilesValidNegativeWorkingSet);
  FRIEND_TEST(MemoryMonitorUtilsTest, TestCgroupFilesValidMissingFieldReturnskNull);
  FRIEND_TEST(MemoryMonitorUtilsTest, TestCgroupNonexistentStatFileReturnskNull);
  FRIEND_TEST(MemoryMonitorUtilsTest, TestCgroupNonexistentUsageFileReturnskNull);
  FRIEND_TEST(MemoryMonitorUtilsTest, TestGetMemoryThresholdTakeGreaterOfTheTwoValues);
  FRIEND_TEST(MemoryMonitorUtilsTest, TestGetPidsFromDirOnlyReturnsNumericFilenames);
  FRIEND_TEST(MemoryMonitorUtilsTest, TestGetPidsFromNonExistentDirReturnsEmpty);
  FRIEND_TEST(MemoryMonitorUtilsTest, TestGetCommandLinePidExistReturnsValid);
  FRIEND_TEST(MemoryMonitorUtilsTest, TestGetCommandLineMissingFileReturnsEmpty);
  FRIEND_TEST(MemoryMonitorUtilsTest, TestShortStringNotTruncated);
  FRIEND_TEST(MemoryMonitorUtilsTest, TestLongStringTruncated);
  FRIEND_TEST(MemoryMonitorUtilsTest, TestTopNLessThanNReturnsMemoryUsedDesc);
  FRIEND_TEST(MemoryMonitorUtilsTest, TestTopNMoreThanNReturnsAllDesc);

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
};

}  // namespace ray
