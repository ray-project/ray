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

#include "absl/strings/str_format.h"
#include "ray/common/ray_config.h"
#include "ray/util/logging.h"
#include "ray/util/process.h"

namespace ray {

MemoryMonitor::MemoryMonitor(instrumented_io_context &io_service,
                             KillWorkersCallback kill_workers_callback)
    : io_service_(io_service), kill_workers_callback_(kill_workers_callback) {
  RAY_CHECK(kill_workers_callback_ != nullptr);
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
    auto memory_usage_gb = absl::StrFormat(
        "%.2f", static_cast<float>(memory_used_bytes) / 1024 / 1024 / 1024);
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
