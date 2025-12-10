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

#include "ray/common/threshold_memory_monitor.h"

#include <boost/algorithm/string.hpp>
#include <filesystem>
#include <fstream>  // std::ifstream
#include <tuple>

#include "absl/strings/str_format.h"
#include "ray/common/ray_config.h"
#include "ray/util/logging.h"
#include "ray/util/process.h"

namespace ray {

ThresholdMemoryMonitor::ThresholdMemoryMonitor(instrumented_io_context &io_service,
                                               KillWorkersCallback kill_workers_callback,
                                               float usage_threshold,
                                               int64_t min_memory_free_bytes,
                                               uint64_t monitor_interval_ms)
    : MemoryMonitor(io_service, kill_workers_callback),
      usage_threshold_(usage_threshold),
      min_memory_free_bytes_(min_memory_free_bytes) {
  RAY_CHECK(kill_workers_callback_ != nullptr);
  runner_ = PeriodicalRunner::Create(io_service_);
  RAY_CHECK_GE(usage_threshold_, 0);
  RAY_CHECK_LE(usage_threshold_, 1);
  if (monitor_interval_ms > 0) {
    auto [used_memory_bytes, total_memory_bytes] = GetMemoryBytes();
    computed_threshold_bytes_ =
        GetMemoryThreshold(total_memory_bytes, usage_threshold_, min_memory_free_bytes_);
    computed_threshold_fraction_ = float(computed_threshold_bytes_) / total_memory_bytes;
    RAY_LOG(INFO) << "MemoryMonitor initialized with usage threshold at "
                  << computed_threshold_bytes_ << " bytes ("
                  << absl::StrFormat("%.2f", computed_threshold_fraction_)
                  << " system memory), total system memory bytes: " << total_memory_bytes;
    runner_->RunFnPeriodically(
        [this] {
          auto [used_mem_bytes, total_mem_bytes] = GetMemoryBytes();
          MemorySnapshot system_memory;
          system_memory.used_bytes = used_mem_bytes;
          system_memory.total_bytes = total_mem_bytes;
          system_memory.process_used_bytes = GetProcessMemoryUsage();

          bool is_usage_above_threshold =
              IsUsageAboveThreshold(system_memory, computed_threshold_bytes_);

          if (is_usage_above_threshold) {
            kill_workers_callback_(system_memory);
          } else {
            RAY_LOG(INFO) << "[Kunchd] Memory usage: "
                          << ((float)used_mem_bytes / (float)total_mem_bytes)
                          << " below threshold: " << computed_threshold_fraction_
                          << " not triggering kill workers callback";
          }
        },
        monitor_interval_ms,
        "MemoryMonitor.CheckIsMemoryUsageAboveThreshold");
  } else {
    RAY_LOG(INFO) << "MemoryMonitor disabled. Specify "
                  << "`memory_monitor_refresh_ms` > 0 to enable the monitor.";
  }
}

bool ThresholdMemoryMonitor::IsUsageAboveThreshold(MemorySnapshot system_memory,
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

std::tuple<int64_t, int64_t> ThresholdMemoryMonitor::GetMemoryBytes() {
  auto [cgroup_used_bytes, cgroup_total_bytes] = GetCGroupMemoryBytes();
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

int64_t ThresholdMemoryMonitor::GetCGroupMemoryUsedBytes(const char *stat_path,
                                                         const char *usage_path,
                                                         const char *inactive_file_key,
                                                         const char *active_file_key) {
  // CGroup reported memory usage includes file page caches
  // and we should exclude those since they are reclaimable
  // by the kernel and are considered available memory from
  // the OOM killer's perspective.
  std::ifstream memstat_ifs(stat_path, std::ios::in | std::ios::binary);
  if (!memstat_ifs.is_open()) {
    RAY_LOG_EVERY_MS(WARNING, kLogIntervalMs)
        << " memory stat file not found: " << stat_path;
    return kNull;
  }
  std::ifstream memusage_ifs(usage_path, std::ios::in | std::ios::binary);
  if (!memusage_ifs.is_open()) {
    RAY_LOG_EVERY_MS(WARNING, kLogIntervalMs)
        << " memory usage file not found: " << usage_path;
    return kNull;
  }

  std::string title;
  int64_t value;
  std::string line;

  int64_t inactive_file_bytes = kNull;
  int64_t active_file_bytes = kNull;
  while (std::getline(memstat_ifs, line)) {
    std::istringstream iss(line);
    iss >> title >> value;
    if (title == inactive_file_key) {
      inactive_file_bytes = value;
    } else if (title == active_file_key) {
      active_file_bytes = value;
    }
  }

  int64_t current_usage_bytes = kNull;
  memusage_ifs >> current_usage_bytes;
  if (current_usage_bytes == kNull || inactive_file_bytes == kNull ||
      active_file_bytes == kNull) {
    RAY_LOG_EVERY_MS(WARNING, kLogIntervalMs)
        << "Failed to parse cgroup memory usage. memory usage " << current_usage_bytes
        << " inactive file " << inactive_file_bytes << " active file "
        << active_file_bytes;
    return kNull;
  }
  // The total file cache is inactive + active per
  // https://access.redhat.com/documentation/en-us/red_hat_enterprise_linux/6/html/resource_management_guide/sec-memory
  return current_usage_bytes - inactive_file_bytes - active_file_bytes;
}

std::tuple<int64_t, int64_t> ThresholdMemoryMonitor::GetCGroupMemoryBytes() {
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
    used_bytes = GetCGroupMemoryUsedBytes(kCgroupsV2MemoryStatPath,
                                          kCgroupsV2MemoryUsagePath,
                                          kCgroupsV2MemoryStatInactiveFileKey,
                                          kCgroupsV2MemoryStatActiveFileKey);
  } else if (std::filesystem::exists(kCgroupsV1MemoryStatPath) &&
             std::filesystem::exists(kCgroupsV1MemoryUsagePath)) {
    used_bytes = GetCGroupMemoryUsedBytes(kCgroupsV1MemoryStatPath,
                                          kCgroupsV1MemoryUsagePath,
                                          kCgroupsV1MemoryStatInactiveFileKey,
                                          kCgroupsV1MemoryStatActiveFileKey);
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

std::tuple<int64_t, int64_t> ThresholdMemoryMonitor::GetLinuxMemoryBytes() {
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

int64_t ThresholdMemoryMonitor::GetMemoryThreshold(int64_t total_memory_bytes,
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

}  // namespace ray
