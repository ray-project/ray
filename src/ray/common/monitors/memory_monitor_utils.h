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
#include <optional>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "ray/common/cgroup2/cgroup_manager_interface.h"
#include "ray/common/monitors/memory_monitor_interface.h"
#include "ray/common/status_or.h"
#include "ray/util/compat.h"

namespace ray {

class MemoryMonitorUtils {
 public:
  static constexpr char kCgroupsV1MemoryMaxPath[] = "memory/memory.limit_in_bytes";
  static constexpr char kCgroupsV1MemoryHighPath[] = "memory/memory.soft_limit_in_bytes";
  static constexpr char kCgroupsV1MemoryUsagePath[] = "memory/memory.usage_in_bytes";
  static constexpr char kCgroupsV1MemoryStatPath[] = "memory/memory.stat";
  static constexpr char kCgroupsV1MemoryStatInactiveFileKey[] = "total_inactive_file";
  static constexpr char kCgroupsV1MemoryStatActiveFileKey[] = "total_active_file";
  // memsw counters report RAM+swap combined (limit and usage). When present they
  // override the RAM-only counters above so swap is counted toward the OOM killer.
  static constexpr char kCgroupsV1MemswMaxPath[] = "memory/memory.memsw.limit_in_bytes";
  static constexpr char kCgroupsV1MemswUsagePath[] = "memory/memory.memsw.usage_in_bytes";
  static constexpr char kCgroupsV2MemoryMaxPath[] = "memory.max";
  static constexpr char kCgroupsV2MemoryHighPath[] = "memory.high";
  static constexpr char kCgroupsV2MemoryUsagePath[] = "memory.current";
  static constexpr char kCgroupsV2MemoryStatPath[] = "memory.stat";
  static constexpr char kCgroupsV2MemoryStatInactiveFileKey[] = "inactive_file";
  static constexpr char kCgroupsV2MemoryStatActiveFileKey[] = "active_file";
  // Swapcache pages are charged to both memory.current and memory.swap.current,
  // so they must be subtracted from one side when the two are summed.
  static constexpr char kCgroupsV2MemoryStatSwapCachedKey[] = "swapcached";
  static constexpr char kCgroupsV2MemoryAnonKey[] = "anon";
  static constexpr char kCgroupsV2MemoryShmemKey[] = "shmem";
  // Swap-only counters in cgroup v2. Added to memory.max / memory.current.
  static constexpr char kCgroupsV2MemorySwapMaxPath[] = "memory.swap.max";
  static constexpr char kCgroupsV2MemorySwapCurrentPath[] = "memory.swap.current";
  static constexpr char kProcDirectory[] = "/proc";
  static constexpr char kCommandlinePath[] = "cmdline";

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
   * This includes the memory usage of all processes running on the system.
   * The system is defined as either the container we are running in or the host machine.
   * If the root cgroup memory limit is lower than the system memory limit,
   * take the memory utilization from the cgroup instead.
   *
   * @param root_cgroup_path The path to the root cgroup
   *                         to read the memory usage from.
   * @param include_swap When true, permit folding swap into the totals (still
   *        gated by `count_swap_in_memory_monitor`, resolved here). Pass false
   *        for a RAM-only view (e.g. the kernel's RAM-only `memory.high`
   *        constraint in `LinuxCgroupManagerFactory`).
   * @param proc_dir The proc directory path
   *                 to read the OS level memory usage from.
   * @return The used and total memory in bytes.
   */
  static const MemoryUsageSnapshot TakeSystemMemoryUsageSnapshot(
      const std::string &root_cgroup_path,
      bool include_swap = false,
      const std::string &proc_dir = kProcDirectory);

  /**
   * @brief Takes a snapshot of user and system slice memory usage across the
   *        host machine.
   *
   *        For user slice, this includes the heap usage of all worker processes
   *        and the object store usage shared across the raylet and workers.
   *        For system slice, this includes the memory usage of all ray system
   *        processes, the kernel, and all non-ray processes outside ray's userspace.
   *
   * @param user_cgroup_path The path to the user cgroup
   *                         to read the memory usage from.
   * @param system_cgroup_path The path to the system cgroup
                               to read the object store memory usage from.
   * @param proc_dir The proc directory path
   *                 to read the OS level memory usage from.
   * @param root_cgroup_path The root cgroup whose memory.swap.max is the slices'
   *        effective swap budget (the leaves inherit it). Both slice totals are
   *        expanded by it so they agree with the OOM threshold and `ray status`.
   * @return A pair of memory usage snapshots in the form of
   *         <user application memory usage, system memory usage>.
   *         Returns StatusT::NotFound if the memory values could not be successfully
   *         retrieved.
   */
  static const StatusSetOr<std::pair<MemoryUsageSnapshot, MemoryUsageSnapshot>,
                           StatusT::NotFound>
  TakeUserAndSystemSliceMemoryUsageSnapshot(
      const std::string &user_cgroup_path,
      const std::string &system_cgroup_path,
      const std::string &proc_dir = kProcDirectory,
      const std::string &root_cgroup_path = MemoryMonitorInterface::kDefaultCgroupPath);

  /**
   * @brief Takes a snapshot of the memory usage for the given cgroupv2 path.
   *
   * @param root_cgroup_path The path to the cgroup to take the snapshot of.
   * @param proc_dir Used to read /proc/meminfo SwapTotal when memory.swap.max
   *        is the kernel's "unlimited" sentinel — the cgroup imposes no cap,
   *        so the practical budget is host swap. Same fallback as
   *        GetCGroupMemoryBytes; keeps the OOM threshold and per-tick
   *        used/total in the same units.
   * @return The cgroup memory snapshot.
   *         Returns StatusT::NotFound if the memory values could not be found,
   *         or if the path is a cgroupv1 path.
   */
  static const StatusSetOr<CgroupMemorySnapshot, StatusT::NotFound>
  TakeCgroupMemorySnapshot(const std::string &root_cgroup_path,
                           const std::string &proc_dir = kProcDirectory);

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
   * @param resource_isolation_enabled Whether resource isolation is enabled. Used
   *        to determine if the threshold should be calculated based on the cgroup
   * constraints.
   * @param cgroup_manager The cgroup manager to fetch the upper bound memory constraints
   * from.
   * @param root_cgroup_path The root cgroup whose `memory.swap.max` is the
   *        user slice's effective swap budget (the leaf inherits it). Read to
   *        keep the OOM threshold aligned with the scheduler's
   *        get_cgroup_aware_swap_memory, which reads the same root path.
   * @return The memory threshold.
   */
  static int64_t GetMemoryThreshold(
      int64_t total_memory_bytes,
      float usage_threshold,
      int64_t min_memory_free_bytes,
      bool resource_isolation_enabled,
      const CgroupManagerInterface &cgroup_manager,
      const std::string &root_cgroup_path = MemoryMonitorInterface::kDefaultCgroupPath);

  /**
   * @brief Gets the used memory for a process from the process memory snapshot.
   *
   * @param snapshot The snapshot of per process memory usage to retrieve against
   * @param pid The process ID.
   * @return The used memory in bytes for the process.
   *         Returns StatusT::NotFound if the process is not found in the snapshot,
   *         for example because the process has already been killed or died.
   */
  static StatusSetOr<int64_t, StatusT::NotFound> GetProcessUsedMemoryBytes(
      const ProcessesMemorySnapshot &snapshot, pid_t pid);

 public:
  /**
   * @brief RAM and swap usage read from a cgroup, kept separate so the caller
   *        can compose them with host-level fallbacks per dimension.
   *
   * For cgroup v2, used/total are RAM-only and swap_* hold the swap counters.
   * For cgroup v1 memsw (which reports RAM+swap as one inseparable number),
   * used/total already include swap and combined_ram_swap is true.
   */
  struct CgroupMemoryBytes {
    // RAM used/total (or RAM+swap combined when combined_ram_swap is true).
    int64_t used_bytes = MemoryMonitorInterface::kNull;
    int64_t total_bytes = MemoryMonitorInterface::kNull;
    // cgroup v2 swap counters, valid only when has_swap is true.
    int64_t swap_used_bytes = 0;
    int64_t swap_total_bytes = 0;
    // True when the cgroup provided a v2 swap budget (bounded, host-resolved
    // "unlimited", or an explicit 0). When false the caller falls back to host
    // swap.
    bool has_swap = false;
    // True for cgroup v1 memsw: used/total already fold in swap, so the caller
    // must not add swap on top (kept on the legacy combined path).
    bool combined_ram_swap = false;
  };

 private:
  /**
   * @brief Gets memory information from the given cgroup.
   *
   * @param root_cgroup_path The path to the root cgroup
   *                         to read the memory usage from.
   * @param include_swap When true, read swap counters iff
   *        `count_swap_in_memory_monitor` is on. Set to false for a RAM-only
   *        view.
   * @param proc_dir The /proc directory, used for the host-swap fallback.
   * @return RAM (and, for v2, swap) usage from the cgroup. See CgroupMemoryBytes.
   */
  static CgroupMemoryBytes GetCGroupMemoryBytes(
      const std::string root_cgroup_path,
      bool include_swap = false,
      const std::string &proc_dir = kProcDirectory);

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
   * @param include_swap When true, fold host swap into the returned totals iff
   *        count_swap_in_memory_monitor is on. Set to false for a RAM-only view.
   * @return The used and total memory in bytes for Linux OS.
   */
  static std::tuple<int64_t, int64_t> GetLinuxMemoryBytes(const std::string proc_dir,
                                                          bool include_swap = false);

  /**
   * @brief Returns host (swap_total_bytes, swap_used_bytes).
   *
   * Used as the fallback when a cgroup imposes no swap cap (the kernel's
   * "unlimited" sentinel), so the practical limit is whatever the host has.
   * Returns (0, 0) on a system without swap or if the values can't be read.
   *
   * @param proc_dir The /proc directory path.
   * @return Host swap total and used in bytes; (0, 0) if unavailable.
   */
  static std::tuple<int64_t, int64_t> GetHostSwapBytes(
      const std::string &proc_dir = kProcDirectory);

  /**
   * @brief Resolves the swap budget from the root cgroup's memory.swap.max.
   *
   * The same value the scheduler's get_cgroup_aware_swap_memory reads. Used for
   * the user-slice OOM threshold and snapshot so both agree with `ray status`.
   *
   * @param root_cgroup_path The root cgroup path.
   * @param proc_dir The /proc directory, used for the host-swap fallback.
   * @return The numeric swap.max, host swap for the "max"/overflow sentinel, or
   *         0 when the file is absent (no swap support) or swap is disabled.
   */
  static int64_t ResolveRootSwapMaxBytes(const std::string &root_cgroup_path,
                                         const std::string &proc_dir);

  /**
   * @brief Resolved cgroup v2 swap counters for one cgroup.
   */
  struct CgroupV2SwapBytes {
    /// True when memory.swap.max exists and was readable.
    bool present = false;
    /// Swap budget: the numeric swap.max, or host swap for the "max"/overflow
    /// "unlimited" sentinel.
    int64_t max_bytes = 0;
    /// Per-cgroup memory.swap.current net of swapcached (pages whose only copy
    /// is in swap — the swapcache resident copies are already counted on the
    /// RAM side); 0 when the budget is 0.
    int64_t used_bytes = 0;
  };

  /**
   * @brief Reads and resolves a cgroup's v2 swap counters.
   *
   * Parses memory.swap.max (host swap is the practical cap for the
   * "unlimited" sentinel) and reads memory.swap.current only when there is a
   * non-zero budget, so a stale swap.current cannot surface as used > total.
   *
   * @param cgroup_path The cgroup directory to read the swap counters from.
   * @param proc_dir The /proc directory, used for the host-swap fallback.
   * @return The resolved counters; present is false when memory.swap.max is
   *         missing or unreadable.
   */
  static CgroupV2SwapBytes ReadCgroupV2Swap(const std::string &cgroup_path,
                                            const std::string &proc_dir);

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

  FRIEND_TEST(MemoryMonitorUtilsTest, TestGetNodeTotalMemoryEqualsFreeOrCGroup);
  FRIEND_TEST(MemoryMonitorUtilsTest, TestCgroupV2SwapAddedToTotalAndUsed);
  FRIEND_TEST(MemoryMonitorUtilsTest, TestCgroupV2SwapIgnoredWhenFlagDisabled);
  FRIEND_TEST(MemoryMonitorUtilsTest, TestCgroupV2SwapcachedSubtractedFromSwapUsed);
  FRIEND_TEST(MemoryMonitorUtilsTest, TestCgroupV2SwapcachedIgnoredWhenFlagDisabled);
  FRIEND_TEST(MemoryMonitorUtilsTest, TestCgroupV2UnlimitedSwapFallsBackToHostSwap);
  FRIEND_TEST(MemoryMonitorUtilsTest, TestCgroupV2OverflowSwapFallsBackToHostSwap);
  FRIEND_TEST(MemoryMonitorUtilsTest, TestCgroupV1MemswAddedToTotalAndUsed);
  FRIEND_TEST(MemoryMonitorUtilsTest, TestCgroupV1MemswIgnoredWhenFlagDisabled);
  FRIEND_TEST(MemoryMonitorUtilsTest, TestCgroupV1MemswFallsBackWhenUsageMissing);
  FRIEND_TEST(MemoryMonitorUtilsTest, TestUserSliceSwapAddedToTotalAndUsed);
  FRIEND_TEST(MemoryMonitorUtilsTest, TestUserSliceSwapIgnoredWhenFlagDisabled);
  FRIEND_TEST(MemoryMonitorUtilsTest, TestUserSliceUnlimitedSwapFallsBackToHostSwap);
  FRIEND_TEST(MemoryMonitorUtilsTest, TestUserSliceOverflowSwapFallsBackToHostSwap);
  FRIEND_TEST(MemoryMonitorUtilsTest, TestUserSliceZeroSwapMaxIgnoresCurrent);
  FRIEND_TEST(MemoryMonitorUtilsTest, TestUserSliceMissingSwapFiles);
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

  static constexpr double kDefaultThresholdMonitorReactionBufferProportion = 0.05;
};

}  // namespace ray
