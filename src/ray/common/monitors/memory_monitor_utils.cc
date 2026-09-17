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

#include "ray/common/monitors/memory_monitor_utils.h"

#include <algorithm>
#include <boost/algorithm/string.hpp>
#include <cctype>
#include <filesystem>
#include <fstream>
#include <stdexcept>
#include <string>
#include <tuple>
#include <utility>

#include "absl/strings/str_format.h"
#include "absl/strings/str_join.h"
#include "ray/common/monitors/memory_monitor_interface.h"
#include "ray/common/ray_config.h"
#include "ray/util/logging.h"

namespace ray {

namespace {

/**
 * @brief Classification of a cgroup `memory.swap.max` (or `memory.memsw`) value.
 */
struct CgroupSwapMax {
  /// "max", empty, non-numeric, or a numeric value that overflows int64 (the
  /// kernel's ULLONG_MAX "unlimited" sentinel). The cgroup imposes no swap cap,
  /// so callers fall back to host swap.
  bool unlimited = false;
  /// Explicit "0" — the kernel says swap is disabled for this cgroup.
  bool zero = false;
  /// Parsed byte value when bounded (i.e. !unlimited).
  int64_t bytes = 0;
};

/**
 * @brief Classifies a raw `memory.swap.max` string.
 *
 * The all-digit (and non-empty) pre-check guarantees std::stoll only ever sees
 * valid numeric input, so the only exception it can throw is
 * std::out_of_range — an all-digit value that overflows int64, which is the
 * kernel's "unlimited" sentinel (e.g. ULLONG_MAX) and is reported as unlimited.
 *
 * @param swap_max_str The raw file content of memory.swap.max.
 * @return The classified value. See CgroupSwapMax.
 */
CgroupSwapMax ParseCgroupSwapMax(const std::string &swap_max_str) {
  CgroupSwapMax result;
  if (swap_max_str.empty() ||
      !std::all_of(swap_max_str.begin(), swap_max_str.end(), [](unsigned char c) {
        return std::isdigit(c);
      })) {
    result.unlimited = true;
    return result;
  }
  try {
    result.bytes = std::stoll(swap_max_str);
    result.zero = (result.bytes == 0);
  } catch (const std::out_of_range &) {
    result.unlimited = true;
  }
  return result;
}

/**
 * @brief Reads the swapcached value from a cgroup v2 `memory.stat` file.
 *
 * @param stat_path The path to the memory.stat file.
 * @return The per-cgroup swapcache size in bytes, or 0 when the file or the
 *         key is missing (kernels < 5.12 don't publish it).
 */
int64_t ReadCgroupSwapCachedBytes(const std::string &stat_path) {
  std::ifstream stat_ifs(stat_path, std::ios::in | std::ios::binary);
  if (!stat_ifs) {
    return 0;
  }
  std::string key;
  int64_t value;
  while (stat_ifs >> key >> value) {
    if (key == MemoryMonitorUtils::kCgroupsV2MemoryStatSwapCachedKey) {
      return std::max<int64_t>(0, value);
    }
  }
  return 0;
}

/**
 * @brief Reads a cgroup `memory.swap.current` file.
 *
 * @param swap_current_path The path to the memory.swap.current file.
 * @return The per-cgroup swap usage in bytes, or 0 when the file is missing,
 *         unreadable, or non-positive.
 */
int64_t ReadCgroupSwapCurrentBytes(const std::string &swap_current_path) {
  if (!std::filesystem::exists(swap_current_path)) {
    return 0;
  }
  std::ifstream swap_cur_ifs(swap_current_path, std::ios::in | std::ios::binary);
  int64_t swap_used_bytes = 0;
  if (swap_cur_ifs && (swap_cur_ifs >> swap_used_bytes) && swap_used_bytes > 0) {
    return swap_used_bytes;
  }
  return 0;
}

}  // namespace

const MemoryUsageSnapshot MemoryMonitorUtils::TakeSystemMemoryUsageSnapshot(
    const std::string &root_cgroup_path, bool include_swap, const std::string &proc_dir) {
  // Resolve the config flag once here (a high-level entry point) and hand the
  // low-level helpers a single boolean. They stay pure / param-only so unit
  // tests can mock swap inclusion without touching RayConfig. include_swap is
  // the caller's intent (false forces a RAM-only view).
  const bool count_swap =
      include_swap && RayConfig::instance().count_swap_in_memory_monitor();
  CgroupMemoryBytes cgroup = GetCGroupMemoryBytes(root_cgroup_path, count_swap, proc_dir);

  if (cgroup.combined_ram_swap) {
    // cgroup v1 memsw reports RAM+swap as one inseparable number. Compare it
    // against the host RAM+swap total (the legacy combined path) and take the
    // cgroup view when its limit is the binding one.
    auto [system_used_bytes, system_total_bytes] =
        GetLinuxMemoryBytes(proc_dir, count_swap);
    system_total_bytes = NullableMin(system_total_bytes, cgroup.total_bytes);
    if (system_total_bytes == cgroup.total_bytes) {
      system_used_bytes = cgroup.used_bytes;
    }
    return MemoryUsageSnapshot{system_used_bytes, system_total_bytes};
  }

  // cgroup v2 (and v1 RAM-only): compose the RAM and swap dimensions
  // separately. The cgroup memory limit can be higher than host memory when it
  // is not in use, so take the cgroup RAM limit only when it is the binding
  // one. Composing swap separately lets a cgroup with an unlimited memory.max
  // but a bounded memory.swap.max still contribute its swap budget (host RAM +
  // cgroup swap), matching the scheduler's get_cgroup_aware_swap_memory rather
  // than folding in host swap.
  auto [host_ram_used_bytes, host_ram_total_bytes] =
      GetLinuxMemoryBytes(proc_dir, /*include_swap=*/false);
  int64_t ram_total_bytes = NullableMin(host_ram_total_bytes, cgroup.total_bytes);
  int64_t ram_used_bytes = (cgroup.total_bytes != MemoryMonitorInterface::kNull &&
                            ram_total_bytes == cgroup.total_bytes)
                               ? cgroup.used_bytes
                               : host_ram_used_bytes;

  int64_t swap_total_bytes = 0;
  int64_t swap_used_bytes = 0;
  if (count_swap) {
    if (cgroup.has_swap) {
      // cgroup-scoped swap (including the host-resolved "unlimited" cap).
      swap_total_bytes = cgroup.swap_total_bytes;
      swap_used_bytes = cgroup.swap_used_bytes;
    } else {
      // No cgroup swap accounting — fall back to host swap.
      auto [host_swap_total, host_swap_used] = GetHostSwapBytes(proc_dir);
      swap_total_bytes = host_swap_total;
      swap_used_bytes = host_swap_used;
    }
  }

  int64_t total_bytes = (ram_total_bytes == MemoryMonitorInterface::kNull)
                            ? MemoryMonitorInterface::kNull
                            : ram_total_bytes + swap_total_bytes;
  int64_t used_bytes = (ram_used_bytes == MemoryMonitorInterface::kNull)
                           ? MemoryMonitorInterface::kNull
                           : ram_used_bytes + swap_used_bytes;
  return MemoryUsageSnapshot{used_bytes, total_bytes};
}

const StatusSetOr<std::pair<MemoryUsageSnapshot, MemoryUsageSnapshot>, StatusT::NotFound>
MemoryMonitorUtils::TakeUserAndSystemSliceMemoryUsageSnapshot(
    const std::string &user_cgroup_path,
    const std::string &system_cgroup_path,
    const std::string &proc_dir,
    const std::string &root_cgroup_path) {
  StatusSetOr<CgroupMemorySnapshot, StatusT::NotFound> user_cgroup_memory_snapshot_or =
      TakeCgroupMemorySnapshot(user_cgroup_path, proc_dir);
  StatusSetOr<CgroupMemorySnapshot, StatusT::NotFound> system_cgroup_memory_snapshot_or =
      TakeCgroupMemorySnapshot(system_cgroup_path, proc_dir);
  if (!user_cgroup_memory_snapshot_or.has_value() ||
      !system_cgroup_memory_snapshot_or.has_value()) {
    std::vector<std::string> error_reasons;
    if (user_cgroup_memory_snapshot_or.has_error()) {
      error_reasons.push_back(
          absl::StrFormat("user cgroup: %s", user_cgroup_memory_snapshot_or.message()));
    }
    if (system_cgroup_memory_snapshot_or.has_error()) {
      error_reasons.push_back(absl::StrFormat(
          "system cgroup: %s", system_cgroup_memory_snapshot_or.message()));
    }
    return StatusT::NotFound(absl::StrFormat(
        "Failed to take memory snapshot of user and system slice usage relative to "
        "the system due to: %s",
        absl::StrJoin(error_reasons, ", ")));
  }

  auto [host_level_used_bytes, host_level_total_bytes] =
      GetLinuxMemoryBytes(proc_dir, /*include_swap=*/false);
  if (host_level_total_bytes == MemoryMonitorInterface::kNull ||
      host_level_used_bytes == MemoryMonitorInterface::kNull) {
    return StatusT::NotFound(absl::StrFormat(
        "Failed to take memory snapshot of user and system slice usage relative to "
        "the system due to failure to get total memory bytes from host machine. "
        "Is %s/meminfo file accessible?",
        proc_dir));
  }

  CgroupMemorySnapshot user_cgroup_memory_snapshot =
      user_cgroup_memory_snapshot_or.value();
  CgroupMemorySnapshot system_cgroup_memory_snapshot =
      system_cgroup_memory_snapshot_or.value();
  // We appoximate total user application memory usage with user slice anon bytes
  // for approximating heap usage and the sum of user and system cgroup shmem bytes
  // for approximating object store usage since shared memory accounting between
  // the system and user slice is in-determinant per:
  // https://docs.kernel.org/admin-guide/cgroup-v2.html#memory-ownership
  int64_t user_slice_ram_used_bytes = user_cgroup_memory_snapshot.anon_memory_bytes +
                                      user_cgroup_memory_snapshot.shmem_memory_bytes +
                                      system_cgroup_memory_snapshot.shmem_memory_bytes;
  // The swap budget comes from the ROOT cgroup's swap.max (the same value the OOM
  // threshold in GetMemoryThreshold and the Python scheduler use), so the
  // advertised totals agree with what the OOM killer enforces. Both slice totals
  // are expanded by it: for the user slice it is the extra headroom before the
  // kill threshold; for the system slice it keeps `total - threshold` (the
  // reserved-system-memory check) RAM-consistent, since the threshold is likewise
  // swap-inflated and the swap term cancels. 0 when the flag is off.
  int64_t root_swap_max_bytes = RayConfig::instance().count_swap_in_memory_monitor()
                                    ? ResolveRootSwapMaxBytes(root_cgroup_path, proc_dir)
                                    : 0;
  // Anon swap is per-cgroup deterministic, so we credit only the user slice's own
  // swap.current to its usage. We do NOT credit system-slice swap to the user
  // slice.
  int64_t total_user_slice_used_bytes =
      user_slice_ram_used_bytes + user_cgroup_memory_snapshot.swap_used_bytes;
  int64_t user_slice_total_bytes = host_level_total_bytes + root_swap_max_bytes;
  // We compute the system slice usage by subtracting the user slice usage from the total
  // system usage. This way, we can account for the memory usage of processes outside
  // ray's userspace and the kernel. host_level_used_bytes and the subtracted user usage
  // are both RAM-only, so swap is not mixed into the system-slice usage figure.
  int64_t total_system_slice_used_bytes =
      std::max<int64_t>(0, host_level_used_bytes - user_slice_ram_used_bytes);
  int64_t system_slice_total_bytes = host_level_total_bytes + root_swap_max_bytes;
  return std::pair<MemoryUsageSnapshot, MemoryUsageSnapshot>{
      MemoryUsageSnapshot{total_user_slice_used_bytes, user_slice_total_bytes},
      MemoryUsageSnapshot{total_system_slice_used_bytes, system_slice_total_bytes}};
}

const StatusSetOr<CgroupMemorySnapshot, StatusT::NotFound>
MemoryMonitorUtils::TakeCgroupMemorySnapshot(const std::string &root_cgroup_path,
                                             const std::string &proc_dir) {
  std::string v2_stat_path = root_cgroup_path + "/" + kCgroupsV2MemoryStatPath;
  std::ifstream v2_stat_f(v2_stat_path, std::ios::in | std::ios::binary);
  if (v2_stat_f) {
    CgroupMemorySnapshot snapshot;
    bool anon_found = false;
    bool shmem_found = false;
    std::string key;
    int64_t stat_value;
    while (v2_stat_f >> key >> stat_value) {
      if (key == kCgroupsV2MemoryAnonKey) {
        snapshot.anon_memory_bytes = stat_value;
        anon_found = true;
      } else if (key == kCgroupsV2MemoryShmemKey) {
        snapshot.shmem_memory_bytes = stat_value;
        shmem_found = true;
      }
      if (anon_found && shmem_found) {
        break;
      }
    }
    if (!anon_found || !shmem_found) {
      return StatusT::NotFound(
          absl::StrFormat("Failed to read memory stat for cgroup %s. "
                          "Is the provided cgroupv2 path valid "
                          "and cgroupv2 active?",
                          root_cgroup_path));
    }

    // Without this, GetMemoryThreshold inflates the trigger by host swap while
    // the per-tick snapshot stays at zero, so kills fire late or not at all.
    if (RayConfig::instance().count_swap_in_memory_monitor()) {
      CgroupV2SwapBytes swap = ReadCgroupV2Swap(root_cgroup_path, proc_dir);
      if (swap.present) {
        snapshot.swap_max_bytes = swap.max_bytes;
        snapshot.swap_used_bytes = swap.used_bytes;
      }
    }

    return snapshot;
  }

  return StatusT::NotFound(
      absl::StrFormat("Failed to open memory stat file on path: %s. "
                      "Is the provided cgroupv2 path valid "
                      "and cgroupv2 active?",
                      v2_stat_path));
}

int64_t MemoryMonitorUtils::GetCGroupMemoryUsedBytes(const char *stat_path,
                                                     const char *usage_path,
                                                     const char *inactive_file_key,
                                                     const char *active_file_key) {
  // CGroup reported memory usage includes file page caches
  // and we should exclude those since they are reclaimable
  // by the kernel and are considered available memory from
  // the OOM killer's perspective.
  std::ifstream memstat_ifs(stat_path, std::ios::in | std::ios::binary);
  if (!memstat_ifs) {
    RAY_LOG_EVERY_MS(WARNING, MemoryMonitorInterface::kLogIntervalMs)
        << " memory stat file not found: " << stat_path;
    return MemoryMonitorInterface::kNull;
  }
  std::ifstream memusage_ifs(usage_path, std::ios::in | std::ios::binary);
  if (!memusage_ifs) {
    RAY_LOG_EVERY_MS(WARNING, MemoryMonitorInterface::kLogIntervalMs)
        << " memory usage file not found: " << usage_path;
    return MemoryMonitorInterface::kNull;
  }

  std::string title;
  int64_t value;
  std::string line;

  int64_t inactive_file_bytes = MemoryMonitorInterface::kNull;
  int64_t active_file_bytes = MemoryMonitorInterface::kNull;
  while (std::getline(memstat_ifs, line)) {
    std::istringstream iss(line);
    iss >> title >> value;
    if (title == inactive_file_key) {
      inactive_file_bytes = value;
    } else if (title == active_file_key) {
      active_file_bytes = value;
    }
  }

  int64_t current_usage_bytes = MemoryMonitorInterface::kNull;
  memusage_ifs >> current_usage_bytes;
  if (current_usage_bytes == MemoryMonitorInterface::kNull ||
      inactive_file_bytes == MemoryMonitorInterface::kNull ||
      active_file_bytes == MemoryMonitorInterface::kNull) {
    RAY_LOG_EVERY_MS(WARNING, MemoryMonitorInterface::kLogIntervalMs) << absl::StrFormat(
        "Failed to parse cgroup memory usage. memory usage %d inactive file %d active "
        "file %d",
        current_usage_bytes,
        inactive_file_bytes,
        active_file_bytes);
    return MemoryMonitorInterface::kNull;
  }
  // The total file cache is inactive + active per
  // https://access.redhat.com/documentation/en-us/red_hat_enterprise_linux/6/html/resource_management_guide/sec-memory
  return current_usage_bytes - inactive_file_bytes - active_file_bytes;
}

MemoryMonitorUtils::CgroupMemoryBytes MemoryMonitorUtils::GetCGroupMemoryBytes(
    const std::string root_cgroup_path, bool include_swap, const std::string &proc_dir) {
  std::string cgroupV1MemoryMaxPath = root_cgroup_path + "/" + kCgroupsV1MemoryMaxPath;
  std::string cgroupV1MemoryUsagePath =
      root_cgroup_path + "/" + kCgroupsV1MemoryUsagePath;
  std::string cgroupV1MemoryStatPath = root_cgroup_path + "/" + kCgroupsV1MemoryStatPath;
  std::string cgroupV1MemswMaxPath = root_cgroup_path + "/" + kCgroupsV1MemswMaxPath;
  std::string cgroupV1MemswUsagePath = root_cgroup_path + "/" + kCgroupsV1MemswUsagePath;
  std::string cgroupV2MemoryMaxPath = root_cgroup_path + "/" + kCgroupsV2MemoryMaxPath;
  std::string cgroupV2MemoryUsagePath =
      root_cgroup_path + "/" + kCgroupsV2MemoryUsagePath;
  std::string cgroupV2MemoryStatPath = root_cgroup_path + "/" + kCgroupsV2MemoryStatPath;

  // include_swap is the single gate: the caller (TakeSystemMemoryUsageSnapshot)
  // has already AND-ed it with `count_swap_in_memory_monitor`. When false, fall
  // back to the RAM-only counters (memory.max / memory.limit_in_bytes /
  // memory.current) regardless of whether memsw / memory.swap.* exist.
  const bool count_swap = include_swap;

  // Require both memsw files together. Otherwise total (RAM+swap) and used
  // (RAM-only) could come from different views, which would feed mismatched
  // units to the OOM threshold check.
  const bool v1_memsw_usable = count_swap &&
                               std::filesystem::exists(cgroupV1MemswMaxPath) &&
                               std::filesystem::exists(cgroupV1MemswUsagePath);

  // cgroup v1 memsw counters report RAM+swap as a single combined value, so
  // when swap accounting is on they replace the RAM-only counters. cgroup v2
  // swap counters are swap-only and get added on top of memory.max /
  // memory.current.
  int64_t total_bytes = MemoryMonitorInterface::kNull;
  if (std::filesystem::exists(cgroupV2MemoryMaxPath)) {
    std::ifstream mem_file(cgroupV2MemoryMaxPath, std::ios::in | std::ios::binary);
    mem_file >> total_bytes;
  } else if (v1_memsw_usable) {
    std::ifstream mem_file(cgroupV1MemswMaxPath, std::ios::in | std::ios::binary);
    mem_file >> total_bytes;
  } else if (std::filesystem::exists(cgroupV1MemoryMaxPath)) {
    std::ifstream mem_file(cgroupV1MemoryMaxPath, std::ios::in | std::ios::binary);
    mem_file >> total_bytes;
  }

  int64_t used_bytes = MemoryMonitorInterface::kNull;
  if (std::filesystem::exists(cgroupV2MemoryUsagePath) &&
      std::filesystem::exists(cgroupV2MemoryStatPath)) {
    used_bytes = GetCGroupMemoryUsedBytes(cgroupV2MemoryStatPath.c_str(),
                                          cgroupV2MemoryUsagePath.c_str(),
                                          kCgroupsV2MemoryStatInactiveFileKey,
                                          kCgroupsV2MemoryStatActiveFileKey);
  } else if (v1_memsw_usable && std::filesystem::exists(cgroupV1MemoryStatPath)) {
    used_bytes = GetCGroupMemoryUsedBytes(cgroupV1MemoryStatPath.c_str(),
                                          cgroupV1MemswUsagePath.c_str(),
                                          kCgroupsV1MemoryStatInactiveFileKey,
                                          kCgroupsV1MemoryStatActiveFileKey);
  } else if (std::filesystem::exists(cgroupV1MemoryStatPath) &&
             std::filesystem::exists(cgroupV1MemoryUsagePath)) {
    used_bytes = GetCGroupMemoryUsedBytes(cgroupV1MemoryStatPath.c_str(),
                                          cgroupV1MemoryUsagePath.c_str(),
                                          kCgroupsV1MemoryStatInactiveFileKey,
                                          kCgroupsV1MemoryStatActiveFileKey);
  }

  /// This can be zero if the memory limit is not set for cgroup v2.
  if (total_bytes == 0) {
    total_bytes = MemoryMonitorInterface::kNull;
  }

  if (used_bytes < 0) {
    RAY_LOG_EVERY_MS(WARNING, MemoryMonitorInterface::kLogIntervalMs) << absl::StrFormat(
        "Got negative used memory for cgroup %d, setting it to zero", used_bytes);
    used_bytes = 0;
  }
  if (total_bytes != MemoryMonitorInterface::kNull) {
    if (used_bytes >= total_bytes) {
      RAY_LOG_EVERY_MS(WARNING, MemoryMonitorInterface::kLogIntervalMs)
          << absl::StrFormat(
                 "Used memory is greater than or equal to total memory used. This can "
                 "happen if the memory limit is set and the container is "
                 "using a lot of memory. Used %d, total %d, setting used to be equal to "
                 "total",
                 used_bytes,
                 total_bytes);
      used_bytes = total_bytes;
    }
  }

  CgroupMemoryBytes result;
  result.used_bytes = used_bytes;
  result.total_bytes = total_bytes;
  // cgroup v1 memsw folds RAM+swap into the counters above; the caller must not
  // add swap on top (the legacy combined path). cgroup v2 keeps swap separate.
  result.combined_ram_swap =
      v1_memsw_usable && !std::filesystem::exists(cgroupV2MemoryMaxPath);

  // cgroup v2 swap-only counters, kept separate from RAM so the caller can
  // compose host RAM with cgroup swap even when memory.max is unlimited (the
  // RAM total is kNull). An explicit swap.max of 0 means swap disabled —
  // has_swap stays true so the caller does not fall back to host swap.
  if (count_swap && !result.combined_ram_swap) {
    CgroupV2SwapBytes swap = ReadCgroupV2Swap(root_cgroup_path, proc_dir);
    if (swap.present) {
      result.has_swap = true;
      result.swap_total_bytes = swap.max_bytes;
      result.swap_used_bytes = swap.used_bytes;
    }
  }

  return result;
}

std::tuple<int64_t, int64_t> MemoryMonitorUtils::GetHostSwapBytes(
    const std::string &proc_dir) {
  std::string meminfo_path = proc_dir + "/meminfo";
  std::ifstream meminfo_ifs(meminfo_path, std::ios::in | std::ios::binary);
  if (!meminfo_ifs) {
    return {0, 0};
  }
  // Absent SwapTotal means a system without swap — return zero, not kNull,
  // so the caller can unconditionally add the result without a sentinel check.
  int64_t swap_total_bytes = 0;
  int64_t swap_free_bytes = 0;
  bool saw_swap_total = false;
  std::string line;
  std::string title;
  uint64_t value;
  std::string unit;
  while (std::getline(meminfo_ifs, line)) {
    std::istringstream iss(line);
    iss >> title >> value >> unit;
    if (title == "SwapTotal:") {
      swap_total_bytes = static_cast<int64_t>(value * 1024);
      saw_swap_total = true;
    } else if (title == "SwapFree:") {
      swap_free_bytes = static_cast<int64_t>(value * 1024);
    }
  }
  if (!saw_swap_total) {
    return {0, 0};
  }
  int64_t swap_used_bytes = std::max<int64_t>(0, swap_total_bytes - swap_free_bytes);
  return {swap_total_bytes, swap_used_bytes};
}

int64_t MemoryMonitorUtils::ResolveRootSwapMaxBytes(const std::string &root_cgroup_path,
                                                    const std::string &proc_dir) {
  std::string root_swap_max_path = root_cgroup_path + "/" + kCgroupsV2MemorySwapMaxPath;
  std::ifstream root_swap_max_ifs(root_swap_max_path, std::ios::in | std::ios::binary);
  std::string root_swap_max_str;
  if (!(root_swap_max_ifs && (root_swap_max_ifs >> root_swap_max_str))) {
    // Absent file (no CONFIG_MEMCG_SWAP, etc.) — no swap budget.
    return 0;
  }
  CgroupSwapMax root_swap_max = ParseCgroupSwapMax(root_swap_max_str);
  if (root_swap_max.unlimited) {
    // "max" / int64 overflow — genuinely unlimited, so the host swap is the cap.
    auto [host_swap_total, _] = GetHostSwapBytes(proc_dir);
    return std::max<int64_t>(0, host_swap_total);
  }
  return std::max<int64_t>(0, root_swap_max.bytes);
}

MemoryMonitorUtils::CgroupV2SwapBytes MemoryMonitorUtils::ReadCgroupV2Swap(
    const std::string &cgroup_path, const std::string &proc_dir) {
  CgroupV2SwapBytes result;
  std::string swap_max_path = cgroup_path + "/" + kCgroupsV2MemorySwapMaxPath;
  std::ifstream swap_max_ifs(swap_max_path, std::ios::in | std::ios::binary);
  std::string swap_max_str;
  if (!(swap_max_ifs && (swap_max_ifs >> swap_max_str))) {
    return result;
  }
  result.present = true;
  CgroupSwapMax swap_max = ParseCgroupSwapMax(swap_max_str);
  if (swap_max.unlimited) {
    // "max" / int64 overflow — the cgroup imposes no cap, so the practical
    // budget is host swap (matching the Python helper).
    auto [host_swap_total, _] = GetHostSwapBytes(proc_dir);
    result.max_bytes = host_swap_total;
  } else {
    result.max_bytes = swap_max.bytes;
  }
  // Per-cgroup swap usage from memory.swap.current — host SwapTotal-SwapFree
  // would pick up other workloads' swap and inflate Ray's view. Skip the read
  // when there is no budget (swap.max == 0) so a stale swap.current can't
  // surface as used > total.
  if (result.max_bytes > 0) {
    int64_t swap_current_bytes =
        ReadCgroupSwapCurrentBytes(cgroup_path + "/" + kCgroupsV2MemorySwapCurrentPath);
    // Swapcache pages are charged to both memory.current and swap.current, so
    // subtract them here to count each page once — on the RAM side, where the
    // resident copy lives. Doing it on the swap side keeps the sum correct no
    // matter which RAM figure the caller composes with (cgroup memory.current,
    // host meminfo, or the user-slice anon counter).
    int64_t swapcached_bytes =
        ReadCgroupSwapCachedBytes(cgroup_path + "/" + kCgroupsV2MemoryStatPath);
    result.used_bytes = std::max<int64_t>(0, swap_current_bytes - swapcached_bytes);
  }
  return result;
}

std::tuple<int64_t, int64_t> MemoryMonitorUtils::GetLinuxMemoryBytes(
    const std::string proc_dir, bool include_swap) {
  std::string meminfo_path = proc_dir + "/meminfo";
  std::ifstream meminfo_ifs(meminfo_path, std::ios::in | std::ios::binary);
  if (!meminfo_ifs) {
    RAY_LOG_EVERY_MS(WARNING, MemoryMonitorInterface::kLogIntervalMs)
        << " file not found: " << meminfo_path;
    return {MemoryMonitorInterface::kNull, MemoryMonitorInterface::kNull};
  }
  std::string line;
  std::string title;
  uint64_t value;
  std::string unit;

  int64_t mem_total_bytes = MemoryMonitorInterface::kNull;
  int64_t mem_available_bytes = MemoryMonitorInterface::kNull;
  int64_t mem_free_bytes = MemoryMonitorInterface::kNull;
  int64_t cached_bytes = MemoryMonitorInterface::kNull;
  int64_t buffer_bytes = MemoryMonitorInterface::kNull;
  // Swap fields are absent on systems without swap; treat as zero in that case
  // rather than kNull so we don't suppress the RAM-only total below.
  int64_t swap_total_bytes = 0;
  int64_t swap_free_bytes = 0;
  while (std::getline(meminfo_ifs, line)) {
    std::istringstream iss(line);
    iss >> title >> value >> unit;

    value = value * 1024;
    if (title == "MemAvailable:") {
      mem_available_bytes = value;
    } else if (title == "MemFree:") {
      mem_free_bytes = value;
    } else if (title == "Cached:") {
      cached_bytes = value;
    } else if (title == "Buffers:") {
      buffer_bytes = value;
    } else if (title == "MemTotal:") {
      mem_total_bytes = value;
    } else if (title == "SwapTotal:") {
      swap_total_bytes = value;
    } else if (title == "SwapFree:") {
      swap_free_bytes = value;
    } else {
      /// Skip other lines
      continue;
    }
    /// Linux reports them as kiB
    RAY_CHECK(unit == "kB");
  }
  if (mem_total_bytes == MemoryMonitorInterface::kNull) {
    RAY_LOG_EVERY_MS(WARNING, MemoryMonitorInterface::kLogIntervalMs)
        << "Unable to determine total bytes . Will return null";
    return {MemoryMonitorInterface::kNull, MemoryMonitorInterface::kNull};
  }

  int64_t available_bytes = MemoryMonitorInterface::kNull;
  /// Follows logic from psutil
  if (mem_available_bytes > 0) {
    available_bytes = mem_available_bytes;
  } else if (mem_free_bytes != MemoryMonitorInterface::kNull &&
             cached_bytes != MemoryMonitorInterface::kNull &&
             buffer_bytes != MemoryMonitorInterface::kNull) {
    available_bytes = mem_free_bytes + cached_bytes + buffer_bytes;
  }

  if (available_bytes == MemoryMonitorInterface::kNull) {
    RAY_LOG_EVERY_MS(ERROR, MemoryMonitorInterface::kLogIntervalMs)
        << "Unable to determine available bytes. Will return null";
    return {MemoryMonitorInterface::kNull, MemoryMonitorInterface::kNull};
  }
  if (mem_total_bytes < available_bytes) {
    RAY_LOG_EVERY_MS(ERROR, MemoryMonitorInterface::kLogIntervalMs)
        << "Total bytes less than available bytes. Will return null";
    return {MemoryMonitorInterface::kNull, MemoryMonitorInterface::kNull};
  }
  int64_t used_bytes = mem_total_bytes - available_bytes;
  if (used_bytes < 0) {
    RAY_LOG_EVERY_MS(WARNING, MemoryMonitorInterface::kLogIntervalMs) << absl::StrFormat(
        "Got negative used memory for linux %d, setting it to zero", used_bytes);
    used_bytes = 0;
  }
  // Fold swap into the totals so the OOM killer treats it as overflow capacity.
  // include_swap is the single gate (the caller already AND-ed it with
  // count_swap_in_memory_monitor). The user-slice path passes false so it can
  // add per-slice cgroup swap separately without double-counting host swap.
  //
  // Both terms of the sum err high: MemTotal - MemAvailable keeps the RAM copy
  // of swap-cached pages in used, and SwapTotal - SwapFree keeps their swap
  // slots (plus lazily-freed ones), so swap-cached pages are counted twice.
  // The overcount is bounded by SwapCached and only makes the monitor kill
  // earlier, never later — the safe direction — so it is left as is.
  if (include_swap) {
    int64_t swap_used_bytes = swap_total_bytes - swap_free_bytes;
    if (swap_used_bytes < 0) {
      swap_used_bytes = 0;
    }
    mem_total_bytes += swap_total_bytes;
    used_bytes += swap_used_bytes;
  }
  return {used_bytes, mem_total_bytes};
}

int64_t MemoryMonitorUtils::GetProcessMemoryBytes(pid_t pid, const std::string proc_dir) {
  std::stringstream smaps_path;
  smaps_path << proc_dir << "/" << std::to_string(pid) << "/smaps_rollup";
  return GetLinuxProcessMemoryBytesFromSmap(smaps_path.str());
}

/// TODO:(clarng) align logic with psutil / Python-side memory calculations
int64_t MemoryMonitorUtils::GetLinuxProcessMemoryBytesFromSmap(
    const std::string smap_path) {
  std::ifstream smap_ifs(smap_path, std::ios::in | std::ios::binary);
  if (!smap_ifs) {
    RAY_LOG_EVERY_MS(WARNING, MemoryMonitorInterface::kLogIntervalMs)
        << " file not found: " << smap_path;
    return MemoryMonitorInterface::kNull;
  }

  int64_t uss = 0;

  std::string line;
  std::string title;
  uint64_t value;
  std::string unit;

  /// Read first line, which is the header
  std::getline(smap_ifs, line);
  while (std::getline(smap_ifs, line)) {
    std::istringstream iss(line);
    iss >> title >> value >> unit;

    /// Linux reports them as kiB
    RAY_CHECK(unit == "kB");
    /// Captures Private_Clean, Private_Dirty, Private_Hugetlb
    if (boost::starts_with(title, "Private_")) {
      uss += value * 1024;
    }
  }

  if (uss == 0) {
    RAY_LOG_EVERY_MS(WARNING, MemoryMonitorInterface::kLogIntervalMs)
        << "Got zero used memory for smap file " << smap_path;
    return MemoryMonitorInterface::kNull;
  }
  return uss;
}

int64_t MemoryMonitorUtils::NullableMin(int64_t left, int64_t right) {
  RAY_CHECK_GE(left, MemoryMonitorInterface::kNull);
  RAY_CHECK_GE(right, MemoryMonitorInterface::kNull);

  if (left == MemoryMonitorInterface::kNull) {
    return right;
  } else if (right == MemoryMonitorInterface::kNull) {
    return left;
  } else {
    return std::min(left, right);
  }
}

int64_t MemoryMonitorUtils::GetMemoryThreshold(
    int64_t total_memory_bytes,
    float usage_threshold,
    int64_t min_memory_free_bytes,
    bool resource_isolation_enabled,
    const CgroupManagerInterface &cgroup_manager,
    const std::string &root_cgroup_path) {
  RAY_CHECK_GE(total_memory_bytes, MemoryMonitorInterface::kNull);
  RAY_CHECK_GE(min_memory_free_bytes, MemoryMonitorInterface::kNull);
  RAY_CHECK_GE(usage_threshold, 0)
      << "Invalid configuration: usage_threshold must be >= 0";
  RAY_CHECK_LE(usage_threshold, 1)
      << "Invalid configuration: usage_threshold must be <= 1";

  int64_t resolved_memory_threshold_bytes;
  int64_t threshold_fraction = static_cast<int64_t>(total_memory_bytes * usage_threshold);

  if (min_memory_free_bytes > MemoryMonitorInterface::kNull) {
    int64_t threshold_absolute = total_memory_bytes - min_memory_free_bytes;
    RAY_CHECK_GE(threshold_absolute, 0);
    resolved_memory_threshold_bytes = std::max(threshold_fraction, threshold_absolute);
  } else {
    resolved_memory_threshold_bytes = threshold_fraction;
  }

  if (resource_isolation_enabled) {
    StatusOr<std::string> user_slice_upper_bound_bytes_or =
        cgroup_manager.GetUserCgroupConstraintValue(kCgroupsV2MemoryHighPath);
    RAY_CHECK(user_slice_upper_bound_bytes_or.ok()) << absl::StrFormat(
        "Failed to get user cgroup memory limit from user cgroup %s "
        "when setting up memory monitor: %s. "
        "Does the cgroup path exist and/or matches the resource isolation hierarchy?",
        cgroup_manager.GetUserCgroupPath(),
        user_slice_upper_bound_bytes_or.ToString());
    std::string user_slice_upper_bound_bytes_str =
        user_slice_upper_bound_bytes_or.value();
    RAY_CHECK(!user_slice_upper_bound_bytes_str.empty()) << absl::StrFormat(
        "Failed to get upper bound memory constraints from user cgroup %s. "
        "Does the cgroup path exist and/or matches the resource isolation hierarchy?",
        cgroup_manager.GetUserCgroupPath());

    if (!user_slice_upper_bound_bytes_str.empty() &&
        std::all_of(user_slice_upper_bound_bytes_str.begin(),
                    user_slice_upper_bound_bytes_str.end(),
                    ::isdigit)) {
      resolved_memory_threshold_bytes = std::stoll(user_slice_upper_bound_bytes_str);
    }

    // Under the isolation override above, the threshold is `memory.high` —
    // a RAM-only kernel constraint. Per-tick `used_bytes` from
    // TakeUserSliceMemoryUsageSnapshot includes user-slice swap.current when the
    // flag is on, so add the swap budget to keep the comparison
    // apples-to-apples.
    //
    // Read swap.max from the ROOT cgroup (the same path the scheduler's
    // get_cgroup_aware_swap_memory reads), not the user leaf: Ray does not write
    // swap.max on the user slice, so the leaf reports "max" and would over-state
    // the budget as host swap. The root cap is the slice's real effective swap.
    // "max"/overflow at the root means genuinely unlimited, so fall back to host
    // swap; an absent file means no swap support, so add nothing.
    if (RayConfig::instance().count_swap_in_memory_monitor()) {
      resolved_memory_threshold_bytes +=
          ResolveRootSwapMaxBytes(root_cgroup_path, kProcDirectory);
    }
  }

  return resolved_memory_threshold_bytes;
}

StatusSetOr<int64_t, StatusT::NotFound> MemoryMonitorUtils::GetProcessUsedMemoryBytes(
    const ProcessesMemorySnapshot &snapshot, pid_t pid) {
  const ProcessesMemorySnapshot::const_iterator it = snapshot.find(pid);
  if (it == snapshot.end()) {
    return StatusT::NotFound(
        absl::StrFormat("Can't find memory usage in process memory snapshot for PID: %d. "
                        "The process may have already been killed or died.",
                        pid));
  }
  return it->second;
}

const std::vector<pid_t> MemoryMonitorUtils::GetPidsFromDir(const std::string proc_dir) {
  std::vector<pid_t> pids;
  if (!std::filesystem::exists(proc_dir)) {
    RAY_LOG_EVERY_MS(INFO, MemoryMonitorInterface::kLogIntervalMs)
        << "Proc dir doesn't exist, return no pids. Dir: " << proc_dir;
    return pids;
  }
  for (const auto &file : std::filesystem::directory_iterator(proc_dir)) {
    std::string filename{file.path().filename().u8string()};
    if (std::all_of(filename.begin(), filename.end(), ::isdigit)) {
      pids.push_back(static_cast<pid_t>(std::stoi(filename)));
    }
  }
  return pids;
}

const std::string MemoryMonitorUtils::GetCommandLineForPid(pid_t pid,
                                                           const std::string proc_dir) {
  std::string path =
      proc_dir + "/" + std::to_string(pid) + "/" + MemoryMonitorUtils::kCommandlinePath;
  std::ifstream commandline_ifs(path, std::ios::in | std::ios::binary);
  if (!commandline_ifs) {
    RAY_LOG_EVERY_MS(INFO, MemoryMonitorInterface::kLogIntervalMs)
        << "Command line path doesn't exist, returning empty command. Path: " << path;
    return {};
  }

  std::string line;
  while (std::getline(commandline_ifs, line)) {
    std::replace(line.begin(), line.end(), '\0', ' ');
    boost::trim(line);
    return line;
  }
  RAY_LOG_EVERY_MS(INFO, MemoryMonitorInterface::kLogIntervalMs)
      << "Empty file. Returning empty command. Path: " << path;
  return {};
}

const std::string MemoryMonitorUtils::TopNMemoryDebugString(
    uint32_t top_n,
    const ProcessesMemorySnapshot &process_memory_snapshot,
    const std::string proc_dir) {
  std::vector<std::tuple<pid_t, int64_t>> pid_to_memory_usage =
      MemoryMonitorUtils::GetTopNMemoryUsage(top_n, process_memory_snapshot);

  std::string debug_string = "PID\tMEM(GB)\tCOMMAND\n";
  if (!pid_to_memory_usage.empty()) {
    debug_string += absl::StrJoin(
        pid_to_memory_usage,
        "\n",
        [&proc_dir](std::string *out, const std::tuple<pid_t, int64_t> &entry) {
          auto [pid, memory_used_bytes] = entry;
          std::string memory_usage_gb = absl::StrFormat(
              "%.2f", static_cast<float>(memory_used_bytes) / 1024 / 1024 / 1024);
          std::string commandline = MemoryMonitorUtils::TruncateString(
              MemoryMonitorUtils::GetCommandLineForPid(pid, proc_dir), 100);
          absl::StrAppend(out, pid, "\t", memory_usage_gb, "\t", commandline);
        });
  }

  return debug_string;
}

const std::vector<std::tuple<pid_t, int64_t>> MemoryMonitorUtils::GetTopNMemoryUsage(
    uint32_t top_n, const ProcessesMemorySnapshot &all_usage) {
  std::vector<std::tuple<pid_t, int64_t>> pid_to_memory_usage;
  for (auto entry : all_usage) {
    pid_to_memory_usage.push_back({entry.first, entry.second});
  }

  std::sort(pid_to_memory_usage.begin(),
            pid_to_memory_usage.end(),
            [](std::tuple<pid_t, int64_t> const &left,
               std::tuple<pid_t, int64_t> const &right) -> bool {
              auto [pid_left, memory_used_bytes_left] = left;
              auto [pid_right, memory_used_bytes_right] = right;
              return memory_used_bytes_left > memory_used_bytes_right;
            });

  if (pid_to_memory_usage.size() > top_n) {
    pid_to_memory_usage.resize(top_n);
  }

  return pid_to_memory_usage;
}

const absl::flat_hash_map<pid_t, int64_t>
MemoryMonitorUtils::TakePerProcessMemorySnapshot(const std::string proc_dir) {
  std::vector<pid_t> pids = MemoryMonitorUtils::GetPidsFromDir(proc_dir);
  absl::flat_hash_map<pid_t, int64_t> pid_to_memory_usage;

  for (int32_t pid : pids) {
    int64_t memory_used_bytes = MemoryMonitorUtils::GetProcessMemoryBytes(pid, proc_dir);
    if (memory_used_bytes != MemoryMonitorInterface::kNull) {
      pid_to_memory_usage.insert({pid, memory_used_bytes});
    }
  }
  return pid_to_memory_usage;
}

const std::string MemoryMonitorUtils::TruncateString(const std::string value,
                                                     uint32_t max_length) {
  if (value.length() > max_length) {
    return value.substr(0, max_length) + "...";
  }
  return value;
}

}  // namespace ray
