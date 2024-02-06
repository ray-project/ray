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

#include "ray/common/memory_monitor.h"

#include <boost/algorithm/string.hpp>
#include <filesystem>
#include <fstream>  // std::ifstream
#include <tuple>

#include "ray/common/ray_config.h"
#include "ray/util/logging.h"
#include "ray/util/process.h"
#include "ray/util/util.h"

namespace ray {

MemoryMonitor::MemoryMonitor(instrumented_io_context &io_service,
                             float usage_threshold,
                             int64_t min_memory_free_bytes,
                             uint64_t monitor_interval_ms,
                             MemoryUsageRefreshCallback monitor_callback)
    : usage_threshold_(usage_threshold),
      min_memory_free_bytes_(min_memory_free_bytes),
      monitor_callback_(monitor_callback),
      runner_(io_service) {
  RAY_CHECK(monitor_callback_ != nullptr);
  RAY_CHECK_GE(usage_threshold_, 0);
  RAY_CHECK_LE(usage_threshold_, 1);
  if (monitor_interval_ms > 0) {
#ifdef __linux__
    auto [used_memory_bytes, total_memory_bytes] = GetMemoryBytes();
    computed_threshold_bytes_ =
        GetMemoryThreshold(total_memory_bytes, usage_threshold_, min_memory_free_bytes_);
    computed_threshold_fraction_ = float(computed_threshold_bytes_) / total_memory_bytes;
    RAY_LOG(INFO) << "MemoryMonitor initialized with usage threshold at "
                  << computed_threshold_bytes_ << " bytes ("
                  << FormatFloat(computed_threshold_fraction_, 2)
                  << " system memory), total system memory bytes: " << total_memory_bytes;
    runner_.RunFnPeriodically(
        [this] {
          auto [used_memory_bytes, total_memory_bytes] = GetMemoryBytes();
          MemorySnapshot system_memory;
          system_memory.used_bytes = used_memory_bytes;
          system_memory.total_bytes = total_memory_bytes;

          bool is_usage_above_threshold =
              IsUsageAboveThreshold(system_memory, computed_threshold_bytes_);

          monitor_callback_(
              is_usage_above_threshold, system_memory, computed_threshold_fraction_);
        },
        monitor_interval_ms,
        "MemoryMonitor.CheckIsMemoryUsageAboveThreshold");
#else
    RAY_LOG(WARNING) << "Not running MemoryMonitor. It is currently supported "
                     << "only on Linux.";
#endif
  } else {
    RAY_LOG(INFO) << "MemoryMonitor disabled. Specify "
                  << "`memory_monitor_refresh_ms` > 0 to enable the monitor.";
  }
}

bool MemoryMonitor::IsUsageAboveThreshold(MemorySnapshot system_memory,
                                          int64_t threshold_bytes) {
  int64_t used_memory_bytes = system_memory.used_bytes;
  int64_t total_memory_bytes = system_memory.total_bytes;
  if (total_memory_bytes == kNull || used_memory_bytes == kNull) {
    RAY_LOG_EVERY_MS(WARNING, kLogIntervalMs)
        << "Unable to capture node memory. Monitor will not be able "
        << "to detect memory usage above threshold.";
    return false;
  }
  bool is_usage_above_threshold = used_memory_bytes > threshold_bytes;
  if (is_usage_above_threshold) {
    RAY_LOG_EVERY_MS(INFO, kLogIntervalMs)
        << "Node memory usage above threshold, used: " << used_memory_bytes
        << ", threshold_bytes: " << threshold_bytes
        << ", total bytes: " << total_memory_bytes
        << ", threshold fraction: " << float(threshold_bytes) / total_memory_bytes;
  }
  return is_usage_above_threshold;
}

std::tuple<int64_t, int64_t> MemoryMonitor::GetMemoryBytes() {
  auto [cgroup_used_bytes, cgroup_total_bytes] = GetCGroupMemoryBytes();
#ifndef __linux__
  RAY_CHECK(false) << "Memory monitor currently supports only linux";
#endif
  auto [system_used_bytes, system_total_bytes] = GetLinuxMemoryBytes();
  /// cgroup memory limit can be higher than system memory limit when it is
  /// not used. We take its value only when it is less than or equal to system memory
  /// limit. TODO(clarng): find a better way to detect cgroup memory limit is used.
  system_total_bytes = NullableMin(system_total_bytes, cgroup_total_bytes);
  /// This assumes cgroup total bytes will look different than system (meminfo)
  if (system_total_bytes == cgroup_total_bytes) {
    system_used_bytes = cgroup_used_bytes;
  }
  return std::tuple(system_used_bytes, system_total_bytes);
}

int64_t MemoryMonitor::GetCGroupV1MemoryUsedBytes(const char *stat_path,
                                                  const char *usage_path) {
  // How does this function calculate in-used memory from cgroup memory info file?
  // It reads 2 cgroup files:
  //  mem stat file: /sys/fs/cgroup/memory/memory.stat
  //  mem usage file: /sys/fs/cgroup/memory/memory.usage_in_bytes
  // Formula:
  //  OS_managed_cache_and_buffer = `memory.stat.total_cache - memory.stat.total_shmem`
  //  used_memory = `memory.usage_in_bytes` - OS_managed_cache_and_buffer
  //
  // This value is consistent with values `MemTotal` `MemAvailable` `MemFree` in
  // `/proc/meminfo`
  //  and they have relationship of:
  //  - `memory.usage_in_bytes` == `MemTotal` - `MemFree`
  //  - `memory.stat.total_cache` == `MemAvailable` - `MemFree`
  //  - OS_managed_cache_and_buffer = `memory.stat.total_cache` - `shmem`
  //
  // Explanation: What's this part `OS_managed_cache_and_buffer` memory for and why
  //   we should treat this part memory as "Not-in-used" ?
  //   Linux OS tries to fully use the whole physical memory size, so that in many
  //   cases we can observe that the system has very little `MemFree` size,
  //   but at the same time system might have a large `MemAvailable` size,
  //   then this part `MemAvailable - MemFree` size is borrowed by Linux OS
  //   to cache pages / buffers , BUT, once user process requests to allocate memory,
  //   and there is no sufficient free memory, OS will evict cache data out of this
  //   part memory and allocate them to user proces.
  //
  // Explanation: What's the part of `shmem` and why we should treat it as "in-used" ?
  //   `shmem` overview: https://man7.org/linux/man-pages/man7/shm_overview.7.html
  //   Ray object store use `/dev/shm`, which is a `tmpfs` file system,
  //   In linux, `tmpfs` file system uses `shmem` memory.
  //   Note that `tmpfs` file system might swap data to disk (when option noswap=False)
  //   but `shmem` value always means the `tmpfs` data size in physical memory.
  //   We can read `shmem` value from /proc/meminfo `Shmem` item or from
  //   /sys/fs/cgroup/memory/memory.stat `total_shmem` item.
  //
  // Explanation: Why don't use cgroup
  //  `memory.stat.total_rss` and `memory.stat.inactive_file_bytes` to compute the
  // in-used memory ?
  //   In my test using these values can't calculate out correct used memory number,
  //   cgroup official doc is fuzzy when describing these values.
  //   But in my test,
  //   cgroup `memory.usage_in_bytes` value perfectly matches (`MemTotal` - `MemFree`)
  //   value, so I am sure that `memory.usage_in_bytes` value is correct. and cgroup
  //   `memory.stat.total_cache` value also perfectly matches
  //   (`MemAvailable` - `MemFree`) value,
  //
  // Correctness testing criteria:
  //  - [Check cgroup memory.stat value consistency with /proc/meminfo]
  //    Ensuring the calculated value is consistent with values computed from
  //    /proc/meminfo Note that cgroup mem file is consistent with /proc/meminfo file
  //    unless there is bug in cgroup, i.e. we can read MemTotal / MemAvailable / Shmem
  //    from /proc/meminfo file, then the calculated value by this function
  //    should equals to `MemTotal` - (`MemAvailable` - `Shmem`)
  //
  //  - [OS_managed_cache_and_buffer test]
  //    Prepare an idle OS environment that has a large number of `free` memory.
  //    Use dd command to write a large file to disk, after dd  completes,
  //    the calculated used_memory value should keep nearly the same with the value
  //    before dd execution.
  //    But note that free memory will decrease significantly because OS
  //    cached the file data as part of "OS_managed_cache_and_buffer" I mentioned above.
  //
  //  - [/dev/shm test]
  //    If we use dd command to write a large file to /dev/shm,
  //    and no swapping occurs (you can use `free -h` to check whether swap size
  //    increases), after dd completes, the calculated used-memory value should be nearly
  //    previous_in_used_memory_bytes + bytes_of_written_file
  //
  //  - [Host OS SIGKILL signal test]:
  //    1. get current "used_memory" by running this `GetCGroupV1MemoryUsedBytes`
  //    function.
  //    2. get "swap_space_size" by running `free` command
  //    3. read "used_swap_size" value by reading "total_swap" item from
  //    /sys/fs/cgroup/memory/memory.stat
  //    4. Create a program that gradually requests to allocate memory,
  //       record that after it gets allocated memory of "oom_size" bytes,
  //       the process is killed by OS SIGKILL signal.
  //    The "oom_size" recorded in step-(4) should approximately satisfy the following
  //    formula: oom_size ~== (total_physical_memory + swap_space_size) - used_memory -
  //    used_swap_size
  std::ifstream memstat_ifs(stat_path, std::ios::in | std::ios::binary);
  if (!memstat_ifs.is_open()) {
    RAY_LOG_EVERY_MS(WARNING, kLogIntervalMs) << " file not found: " << stat_path;
    return kNull;
  }
  std::ifstream memusage_ifs(usage_path, std::ios::in | std::ios::binary);
  if (!memusage_ifs.is_open()) {
    RAY_LOG_EVERY_MS(WARNING, kLogIntervalMs) << " file not found: " << usage_path;
    return kNull;
  }

  std::string line;
  std::string title;
  int64_t value;

  int64_t cgroup_usage_in_bytes;
  // The content of "/sys/fs/cgroup/memory/memory.usage_in_bytes" file is
  // an integer representing the total memory usage bytes of the container.
  std::getline(memusage_ifs, line);
  std::istringstream iss(line);
  iss >> cgroup_usage_in_bytes;

  int64_t total_cache_bytes = kNull;
  int64_t shmem_used_bytes = kNull;
  while (std::getline(memstat_ifs, line)) {
    std::istringstream iss(line);
    iss >> title >> value;
    if (title == "total_cache") {
      total_cache_bytes = value;
    } else if (title == "total_shmem") {
      shmem_used_bytes = value;
    }
  }
  if (total_cache_bytes == kNull || shmem_used_bytes == kNull) {
    RAY_LOG_EVERY_MS(WARNING, kLogIntervalMs)
        << "Failed to parse cgroup v1 mem stat. total cache " << total_cache_bytes
        << " total_shmem " << shmem_used_bytes;
    return kNull;
  }
  int64_t used = cgroup_usage_in_bytes - (total_cache_bytes - shmem_used_bytes);
  return used;
}

int64_t MemoryMonitor::GetCGroupV2MemoryUsedBytes(const char *stat_path,
                                                  const char *usage_path) {
  // Uses same calculation as libcontainer, that is: memory.current -
  // memory.stat[inactive_file]. Source:
  // https://github.com/google/cadvisor/blob/24dd1de08a72cfee661f6178454db995900c0fee/container/libcontainer/handler.go#L836
  std::ifstream memstat_ifs(stat_path, std::ios::in | std::ios::binary);
  if (!memstat_ifs.is_open()) {
    RAY_LOG_EVERY_MS(WARNING, kLogIntervalMs)
        << " cgroups v2 memory.stat file not found: " << stat_path;
    return kNull;
  }
  std::ifstream memusage_ifs(usage_path, std::ios::in | std::ios::binary);
  if (!memusage_ifs.is_open()) {
    RAY_LOG_EVERY_MS(WARNING, kLogIntervalMs)
        << " cgroups v2 memory.current file not found: " << usage_path;
    return kNull;
  }

  std::string title;
  int64_t value;
  std::string line;

  int64_t inactive_file_bytes = kNull;
  while (std::getline(memstat_ifs, line)) {
    std::istringstream iss(line);
    iss >> title >> value;
    if (title == kCgroupsV2MemoryStatInactiveKey) {
      inactive_file_bytes = value;
      break;
    }
  }

  int64_t current_usage_bytes = kNull;
  memusage_ifs >> current_usage_bytes;
  if (current_usage_bytes == kNull || inactive_file_bytes == kNull) {
    RAY_LOG_EVERY_MS(WARNING, kLogIntervalMs)
        << "Failed to parse cgroup v2 memory usage. memory.current "
        << current_usage_bytes << " inactive " << inactive_file_bytes;
    return kNull;
  }
  return current_usage_bytes - inactive_file_bytes;
}

std::tuple<int64_t, int64_t> MemoryMonitor::GetCGroupMemoryBytes() {
  int64_t total_bytes = kNull;
  if (std::filesystem::exists(kCgroupsV2MemoryMaxPath)) {
    std::ifstream mem_file(kCgroupsV2MemoryMaxPath, std::ios::in | std::ios::binary);
    mem_file >> total_bytes;
  } else if (std::filesystem::exists(kCgroupsV1MemoryMaxPath)) {
    std::ifstream mem_file(kCgroupsV1MemoryMaxPath, std::ios::in | std::ios::binary);
    mem_file >> total_bytes;
  }

  int64_t used_bytes = kNull;
  if (std::filesystem::exists(kCgroupsV2MemoryUsagePath) &&
      std::filesystem::exists(kCgroupsV2MemoryStatPath)) {
    used_bytes =
        GetCGroupV2MemoryUsedBytes(kCgroupsV2MemoryStatPath, kCgroupsV2MemoryUsagePath);
  } else if (std::filesystem::exists(kCgroupsV1MemoryStatPath)) {
    used_bytes =
        GetCGroupV1MemoryUsedBytes(kCgroupsV1MemoryStatPath, kCgroupsV1MemoryUsagePath);
  }

  /// This can be zero if the memory limit is not set for cgroup v2.
  if (total_bytes == 0) {
    total_bytes = kNull;
  }

  if (used_bytes < 0) {
    RAY_LOG_EVERY_MS(WARNING, kLogIntervalMs)
        << "Got negative used memory for cgroup " << used_bytes << ", setting it to zero";
    used_bytes = 0;
  }
  if (total_bytes != kNull) {
    if (used_bytes >= total_bytes) {
      RAY_LOG_EVERY_MS(WARNING, kLogIntervalMs)
          << " Used memory is greater than or equal to total memory used. This can "
             "happen if the memory limit is set and the container is "
             "using a lot of memory. Used "
          << used_bytes << ", total " << total_bytes
          << ", setting used to be equal to total";
      used_bytes = total_bytes;
    }
  }

  return {used_bytes, total_bytes};
}

std::tuple<int64_t, int64_t> MemoryMonitor::GetLinuxMemoryBytes() {
  std::string meminfo_path = "/proc/meminfo";
  std::ifstream meminfo_ifs(meminfo_path, std::ios::in | std::ios::binary);
  if (!meminfo_ifs.is_open()) {
    RAY_LOG_EVERY_MS(WARNING, kLogIntervalMs) << " file not found: " << meminfo_path;
    return {kNull, kNull};
  }
  std::string line;
  std::string title;
  uint64_t value;
  std::string unit;

  int64_t mem_total_bytes = kNull;
  int64_t mem_available_bytes = kNull;
  int64_t mem_free_bytes = kNull;
  int64_t cached_bytes = kNull;
  int64_t buffer_bytes = kNull;
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
  if (mem_total_bytes == kNull) {
    RAY_LOG_EVERY_MS(WARNING, kLogIntervalMs)
        << "Unable to determine total bytes . Will return null";
    return {kNull, kNull};
  }

  int64_t available_bytes = kNull;
  /// Follows logic from psutil
  if (mem_available_bytes > 0) {
    available_bytes = mem_available_bytes;
  } else if (mem_free_bytes != kNull && cached_bytes != kNull && buffer_bytes != kNull) {
    available_bytes = mem_free_bytes + cached_bytes + buffer_bytes;
  }

  if (available_bytes == kNull) {
    RAY_LOG_EVERY_MS(ERROR, kLogIntervalMs)
        << "Unable to determine available bytes. Will return null";
    return {kNull, kNull};
  }
  if (mem_total_bytes < available_bytes) {
    RAY_LOG_EVERY_MS(ERROR, kLogIntervalMs)
        << "Total bytes less than available bytes. Will return null";
    return {kNull, kNull};
  }
  int64_t used_bytes = mem_total_bytes - available_bytes;
  if (used_bytes < 0) {
    RAY_LOG_EVERY_MS(WARNING, kLogIntervalMs)
        << "Got negative used memory for linux " << used_bytes << ", setting it to zero";
    used_bytes = 0;
  }
  return {used_bytes, mem_total_bytes};
}

int64_t MemoryMonitor::GetProcessMemoryBytes(pid_t pid, const std::string proc_dir) {
  std::stringstream smaps_path;
  smaps_path << proc_dir << "/" << std::to_string(pid) << "/smaps_rollup";
  return GetLinuxProcessMemoryBytesFromSmap(smaps_path.str());
}

/// TODO:(clarng) align logic with psutil / Python-side memory calculations
int64_t MemoryMonitor::GetLinuxProcessMemoryBytesFromSmap(const std::string smap_path) {
  std::ifstream smap_ifs(smap_path, std::ios::in | std::ios::binary);
  if (!smap_ifs.is_open()) {
    RAY_LOG_EVERY_MS(WARNING, kLogIntervalMs) << " file not found: " << smap_path;
    return kNull;
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
    RAY_LOG_EVERY_MS(WARNING, kLogIntervalMs)
        << "Got zero used memory for smap file " << smap_path;
    return kNull;
  }
  return uss;
}

int64_t MemoryMonitor::NullableMin(int64_t left, int64_t right) {
  RAY_CHECK_GE(left, kNull);
  RAY_CHECK_GE(right, kNull);

  if (left == kNull) {
    return right;
  } else if (right == kNull) {
    return left;
  } else {
    return std::min(left, right);
  }
}

int64_t MemoryMonitor::GetMemoryThreshold(int64_t total_memory_bytes,
                                          float usage_threshold,
                                          int64_t min_memory_free_bytes) {
  RAY_CHECK_GE(total_memory_bytes, kNull);
  RAY_CHECK_GE(min_memory_free_bytes, kNull);
  RAY_CHECK_GE(usage_threshold, 0);
  RAY_CHECK_LE(usage_threshold, 1);

  int64_t threshold_fraction = (int64_t)(total_memory_bytes * usage_threshold);

  if (min_memory_free_bytes > kNull) {
    int64_t threshold_absolute = total_memory_bytes - min_memory_free_bytes;
    RAY_CHECK_GE(threshold_absolute, 0);
    return std::max(threshold_fraction, threshold_absolute);
  } else {
    return threshold_fraction;
  }
}

const std::vector<pid_t> MemoryMonitor::GetPidsFromDir(const std::string proc_dir) {
  std::vector<pid_t> pids;
  if (!std::filesystem::exists(proc_dir)) {
    RAY_LOG_EVERY_MS(INFO, kLogIntervalMs)
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

const std::string MemoryMonitor::GetCommandLineForPid(pid_t pid,
                                                      const std::string proc_dir) {
  std::string path =
      proc_dir + "/" + std::to_string(pid) + "/" + MemoryMonitor::kCommandlinePath;
  std::ifstream commandline_ifs(path, std::ios::in | std::ios::binary);
  if (!commandline_ifs.is_open()) {
    RAY_LOG_EVERY_MS(INFO, kLogIntervalMs)
        << "Command line path doesn't exist, returning empty command. Path: " << path;
    return {};
  }

  std::string line;
  while (std::getline(commandline_ifs, line)) {
    std::replace(line.begin(), line.end(), '\0', ' ');
    boost::trim(line);
    return line;
  }
  RAY_LOG_EVERY_MS(INFO, kLogIntervalMs)
      << "Empty file. Returning empty command. Path: " << path;
  return {};
}

const std::string MemoryMonitor::TopNMemoryDebugString(uint32_t top_n,
                                                       const MemorySnapshot system_memory,
                                                       const std::string proc_dir) {
  auto pid_to_memory_usage =
      MemoryMonitor::GetTopNMemoryUsage(top_n, system_memory.process_used_bytes);

  std::string debug_string = "PID\tMEM(GB)\tCOMMAND";
  for (std::tuple<pid_t, int64_t> entry : pid_to_memory_usage) {
    auto [pid, memory_used_bytes] = entry;
    auto pid_string = std::to_string(pid);
    auto memory_usage_gb =
        FormatFloat(static_cast<float>(memory_used_bytes) / 1024 / 1024 / 1024, 2);
    auto commandline = MemoryMonitor::TruncateString(
        MemoryMonitor::GetCommandLineForPid(pid, proc_dir), 100);
    debug_string += "\n" + pid_string + "\t" + memory_usage_gb + "\t" + commandline;
  }

  return debug_string;
}

const std::vector<std::tuple<pid_t, int64_t>> MemoryMonitor::GetTopNMemoryUsage(
    uint32_t top_n, const absl::flat_hash_map<pid_t, int64_t> all_usage) {
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

const absl::flat_hash_map<pid_t, int64_t> MemoryMonitor::GetProcessMemoryUsage(
    const std::string proc_dir) {
  std::vector<pid_t> pids = MemoryMonitor::GetPidsFromDir(proc_dir);
  absl::flat_hash_map<pid_t, int64_t> pid_to_memory_usage;

  for (int32_t pid : pids) {
    int64_t memory_used_bytes = MemoryMonitor::GetProcessMemoryBytes(pid, proc_dir);
    if (memory_used_bytes != kNull) {
      pid_to_memory_usage.insert({pid, memory_used_bytes});
    }
  }
  return pid_to_memory_usage;
}

const std::string MemoryMonitor::TruncateString(const std::string value,
                                                uint32_t max_length) {
  if (value.length() > max_length) {
    return value.substr(0, max_length) + "...";
  }
  return value;
}

std::ostream &operator<<(std::ostream &os, const MemorySnapshot &memory_snapshot) {
  os << "Used bytes: " << memory_snapshot.used_bytes
     << ", Total bytes: " << memory_snapshot.total_bytes;
  return os;
}

}  // namespace ray
