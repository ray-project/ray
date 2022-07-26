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

#include <fstream>  // std::ifstream
#include <tuple>

#include "ray/common/ray_config.h"
#include "ray/util/logging.h"

namespace ray {

MemoryMonitor::MemoryMonitor(float usage_threshold_,
                             uint64_t monitor_interval_ms,
                             MemoryUsageRefreshCallback monitor_callback)
    : usage_threshold_(usage_threshold_),
      monitor_callback_(monitor_callback),
      io_context_(),
      monitor_thread_([this] {
        boost::asio::io_service::work io_service_work_(io_context_);
        io_context_.run();
      }),
      runner_(io_context_) {
  RAY_CHECK(monitor_callback_ != nullptr);
  RAY_CHECK(usage_threshold_ >= 0);
  RAY_CHECK(usage_threshold_ <= 1);
  if (monitor_interval_ms > 0) {
#ifdef __linux__
    runner_.RunFnPeriodically(
        [this] {
          bool is_usage_above_threshold = IsUsageAboveThreshold();
          monitor_callback_(is_usage_above_threshold);
        },
        monitor_interval_ms,
        "MemoryMonitor.CheckIsMemoryUsageAboveThreshold");
    RAY_LOG(INFO) << "MemoryMonitor initialized";
#else
    RAY_LOG(WARNING) << "Not running MemoryMonitor. It is currently supported "
                     << "only on Linux.";
#endif
  } else {
    RAY_LOG(INFO) << "MemoryMonitor disabled. Specify "
                  << "`memory_monitor_interval_ms` > 0 to enable the monitor.";
  }
}

bool MemoryMonitor::IsUsageAboveThreshold() {
  auto [used_memory_bytes, total_memory_bytes] = GetLinuxNodeMemoryBytes();
  if (total_memory_bytes == 0) {
    RAY_LOG(ERROR) << "Unable to capture node memory. Monitor will not be able "
                   << "to detect memory usage above threshold.";
    return false;
  }
  auto usage_fraction = static_cast<float>(used_memory_bytes) / total_memory_bytes;
  bool is_usage_above_threshold = usage_fraction > usage_threshold_;
  if (is_usage_above_threshold) {
    RAY_LOG_EVERY_MS(INFO, 1000)
        << "Node memory usage above threshold, used: " << used_memory_bytes
        << ", total: " << total_memory_bytes << ", usage fraction: " << usage_fraction
        << ", threshold: " << usage_threshold_;
  }
  return is_usage_above_threshold;
}

std::tuple<uint64_t, uint64_t> MemoryMonitor::GetLinuxNodeMemoryBytes() {
#ifndef __linux__
  RAY_LOG(ERROR) << "GetLinuxNodeMemoryBytes called on non-Linux system" return return {
      0, 0};
#endif
  std::string meminfo_path = "/proc/meminfo";
  std::ifstream meminfo_ifs(meminfo_path, std::ios::in | std::ios::binary);
  if (!meminfo_ifs.is_open()) {
    RAY_LOG(ERROR) << " file not found " << meminfo_path;
    return {0, 0};
  }
  std::string line;
  std::string title;
  uint64_t value;
  std::string unit;

  uint64_t mem_total_bytes = 0;
  uint64_t mem_available_bytes = 0;
  uint64_t mem_free_bytes = 0;
  uint64_t cached_bytes = 0;
  uint64_t buffer_bytes = 0;
  while (std::getline(meminfo_ifs, line)) {
    std::istringstream iss(line);
    iss >> title >> value >> unit;
    /// Linux reports them as kiB
    RAY_CHECK(unit == "kB");
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
    }
  }
  /// The following logic mimics psutil in python that is used in other parts of
  /// the codebase.
  if (mem_available_bytes > 0) {
    if (mem_total_bytes < mem_available_bytes) {
      RAY_LOG(ERROR) << " total bytes less than available bytes. "
                     << "Monitor will assume zero usage.";
      return {0, mem_total_bytes};
    }
    return {mem_total_bytes - mem_available_bytes, mem_total_bytes};
  }
  auto available_bytes = mem_free_bytes + cached_bytes + buffer_bytes;
  if (mem_total_bytes < available_bytes) {
    available_bytes = mem_free_bytes;
    if (mem_total_bytes < available_bytes) {
      RAY_LOG(ERROR) << " total bytes less than available bytes. "
                     << "Monitor will assume zero usage.";
      return {0, mem_total_bytes};
    }
  }
  auto used_bytes = mem_total_bytes - available_bytes;
  return {used_bytes, mem_total_bytes};
}

MemoryMonitor::~MemoryMonitor() {
  io_context_.stop();
  if (monitor_thread_.joinable()) {
    monitor_thread_.join();
  }
}

}  // namespace ray
