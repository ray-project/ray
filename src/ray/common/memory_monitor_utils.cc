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

#include "ray/common/memory_monitor_utils.h"

#include <boost/algorithm/string.hpp>
#include <filesystem>
#include <fstream>
#include <tuple>

#include "absl/strings/str_format.h"
#include "absl/strings/str_join.h"
#include "ray/common/memory_monitor_interface.h"
#include "ray/util/logging.h"

namespace ray {

const SystemMemorySnapshot MemoryMonitorUtils::TakeSystemMemorySnapshot(
    const std::string root_cgroup_path, const std::string proc_dir) {
  auto [cgroup_used_bytes, cgroup_total_bytes] = GetCGroupMemoryBytes(root_cgroup_path);
  auto [system_used_bytes, system_total_bytes] = GetLinuxMemoryBytes(proc_dir);
  /// cgroup memory limit can be higher than system memory limit when it is
  /// not used. We take its value only when it is less than or equal to system memory
  /// limit. TODO(clarng): find a better way to detect cgroup memory limit is used.
  system_total_bytes = NullableMin(system_total_bytes, cgroup_total_bytes);
  /// This assumes cgroup total bytes will look different than system (meminfo)
  if (system_total_bytes == cgroup_total_bytes) {
    system_used_bytes = cgroup_used_bytes;
  }
  return SystemMemorySnapshot{system_used_bytes, system_total_bytes};
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
  if (!memstat_ifs.is_open()) {
    RAY_LOG_EVERY_MS(WARNING, MemoryMonitorInterface::kLogIntervalMs)
        << " memory stat file not found: " << stat_path;
    return MemoryMonitorInterface::kNull;
  }
  std::ifstream memusage_ifs(usage_path, std::ios::in | std::ios::binary);
  if (!memusage_ifs.is_open()) {
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

std::tuple<int64_t, int64_t> MemoryMonitorUtils::GetCGroupMemoryBytes(
    const std::string root_cgroup_path) {
  std::string cgroupV1MemoryMaxPath = root_cgroup_path + "/" + kCgroupsV1MemoryMaxPath;
  std::string cgroupV1MemoryUsagePath =
      root_cgroup_path + "/" + kCgroupsV1MemoryUsagePath;
  std::string cgroupV1MemoryStatPath = root_cgroup_path + "/" + kCgroupsV1MemoryStatPath;
  std::string cgroupV2MemoryMaxPath = root_cgroup_path + "/" + kCgroupsV2MemoryMaxPath;
  std::string cgroupV2MemoryUsagePath =
      root_cgroup_path + "/" + kCgroupsV2MemoryUsagePath;
  std::string cgroupV2MemoryStatPath = root_cgroup_path + "/" + kCgroupsV2MemoryStatPath;

  int64_t total_bytes = MemoryMonitorInterface::kNull;
  if (std::filesystem::exists(cgroupV2MemoryMaxPath)) {
    std::ifstream mem_file(cgroupV2MemoryMaxPath, std::ios::in | std::ios::binary);
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

  return {used_bytes, total_bytes};
}

std::tuple<int64_t, int64_t> MemoryMonitorUtils::GetLinuxMemoryBytes(
    const std::string proc_dir) {
  std::string meminfo_path = proc_dir + "/meminfo";
  std::ifstream meminfo_ifs(meminfo_path, std::ios::in | std::ios::binary);
  if (!meminfo_ifs.is_open()) {
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
  if (!smap_ifs.is_open()) {
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

int64_t MemoryMonitorUtils::GetMemoryThreshold(int64_t total_memory_bytes,
                                               float usage_threshold,
                                               int64_t min_memory_free_bytes) {
  RAY_CHECK_GE(total_memory_bytes, MemoryMonitorInterface::kNull);
  RAY_CHECK_GE(min_memory_free_bytes, MemoryMonitorInterface::kNull);
  RAY_CHECK_GE(usage_threshold, 0);
  RAY_CHECK_LE(usage_threshold, 1);

  int64_t threshold_fraction = (int64_t)(total_memory_bytes * usage_threshold);

  if (min_memory_free_bytes > MemoryMonitorInterface::kNull) {
    int64_t threshold_absolute = total_memory_bytes - min_memory_free_bytes;
    RAY_CHECK_GE(threshold_absolute, 0);
    return std::max(threshold_fraction, threshold_absolute);
  } else {
    return threshold_fraction;
  }
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
  if (!commandline_ifs.is_open()) {
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

  std::string debug_string = "PID\tMEM(GB)\tCOMMAND, ";
  if (!pid_to_memory_usage.empty()) {
    debug_string += absl::StrJoin(
        pid_to_memory_usage,
        ", ",
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
