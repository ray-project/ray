// Copyright 2020 The Ray Authors.
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

#include <tuple>
#include "ray/common/memory_monitor.h"
#include <fstream>      // std::ifstream
#include "nlohmann/json.hpp"
#include "ray/util/logging.h"
#include "ray/common/ray_config.h"

using json = nlohmann::json;

namespace ray {

MemoryMonitor::MemoryMonitor(float capacity_threshold,
    uint64_t monitor_interval_ms,
    MemoryUsageRefreshCallback monitor_callback)
  : usage_threshold_(capacity_threshold),
    monitor_callback_(monitor_callback),
    io_context_(),
    monitor_thread_([this] {
      boost::asio::io_service::work io_service_work_(io_context_);
      io_context_.run();
    }),
    runner_(io_context_) {
  if (monitor_interval_ms > 0) {
    #ifdef __linux__
      runner_.RunFnPeriodically([this] { 
          bool is_usage_above_threshold = IsUsageAboveThreshold();
          monitor_callback_(is_usage_above_threshold);
        },
        monitor_interval_ms,
        "MemoryMonitor.CheckIsMemoryUsageAboveThreshold");
      RAY_LOG(INFO) << "MemoryMonitor initialized";
    #else
      RAY_LOG(WARNING) << "Not running MemoryMonitor. It is currently supported on Linux.";
    #endif    
  } else {
    RAY_LOG(INFO) << "MemoryMonitor disabled";
  }
}


bool MemoryMonitor::IsUsageAboveThreshold() {
  auto [available_memory_bytes, total_memory_bytes]
          = GetNodeAvailableMemoryBytes();
  auto used = total_memory_bytes - available_memory_bytes;
  auto usage_fraction = static_cast<float>(used) / total_memory_bytes;
  bool is_usage_above_threshold = usage_fraction > usage_threshold_;
  if (is_usage_above_threshold) {
    RAY_LOG(INFO) << "Node memory usage above threshold, usage: "
        << usage_fraction
        << ", threshold: "
        << usage_threshold_;
  }
  return is_usage_above_threshold;
}

std::tuple<uint64_t, uint64_t> MemoryMonitor::GetNodeAvailableMemoryBytes() {
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
  while (std::getline(meminfo_ifs, line)) {
      std::istringstream iss(line);
      iss >> title >> value >> unit;
      if (title == "MemAvailable:") {
          mem_available_bytes = value;
      } else if (title == "MemFree:") {
          mem_free_bytes = value;
      } else if (title == "Cached:") {
          cached_bytes = value;
      } else if (title == "MemTotal:") {
          mem_total_bytes = value;
      }
  }
  /// The following logic mimics psutil in python that is used in other parts of
  /// the codebase.
  if (mem_available_bytes > 0) {
    return {mem_available_bytes, mem_total_bytes};
  }
  return {mem_free_bytes + cached_bytes, mem_total_bytes};
}

MemoryMonitor::~MemoryMonitor() {
  io_context_.stop();
  if (monitor_thread_.joinable()) {
    monitor_thread_.join();
  }
}

}  // namespace ray
