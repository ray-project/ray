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

#include <cstdint>
#include <functional>
#include <memory>
#include <ostream>
#include <string>
#include <vector>

#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/asio/periodical_runner.h"
#include "ray/raylet/worker_interface.h"
#include "ray/util/process.h"

namespace ray {

/// A snapshot of memory information.
struct MemorySnapshot {
  /// The memory used.
  int64_t used_bytes;

  /// The total memory that can be used. >= used_bytes;
  int64_t total_bytes;

  /// The per-process memory used;
  absl::flat_hash_map<pid_t, int64_t> process_used_bytes;

  friend std::ostream &operator<<(std::ostream &os,
                                  const MemorySnapshot &memory_snapshot);
};

/// Callback that runs at each monitoring interval.
///
/// \param system_memory snapshot of system memory information.
using KillWorkersCallback = std::function<void(MemorySnapshot system_memory)>;

/// Base class for memory monitor implementations.
/// Monitors the memory usage of the node.
/// All implementations of the memory monitor must be thread safe.
class MemoryMonitor {
 public:
  /// Constructor.
  ///
  /// \param io_service the event loop.
  /// \param monitor_callback function to execute on a dedicated thread owned by this
  /// monitor when the usage is refreshed.
  MemoryMonitor(instrumented_io_context &io_service,
                KillWorkersCallback kill_workers_callback);

 public:
  /// \param top_n the number of top memory-using processes
  /// \param system_memory the snapshot of memory usage
  /// \param proc_dir the directory to scan for the processes
  ///
  /// \return the debug string that contains up to the top N memory-using processes,
  /// empty if process directory is invalid
  static const std::string TopNMemoryDebugString(
      uint32_t top_n,
      const MemorySnapshot system_memory,
      const std::string proc_dir = kProcDirectory);

  /// \param proc_dir the directory to scan for the processes
  ///
  /// \return the pid to memory usage map for all the processes
  static const absl::flat_hash_map<pid_t, int64_t> GetProcessMemoryUsage(
      const std::string proc_dir = kProcDirectory);

 protected:
  static constexpr char kProcDirectory[] = "/proc";
  static constexpr char kCommandlinePath[] = "cmdline";
  /// The logging frequency. Decoupled from how often the monitor runs.
  static constexpr uint32_t kLogIntervalMs = 5000;
  static constexpr int64_t kNull = -1;

  /// The raylet's event loop.
  instrumented_io_context &io_service_;

  /// Callback function that executes at each monitoring interval,
  /// on a dedicated thread managed by this class.
  const KillWorkersCallback kill_workers_callback_;

  /// \return the smaller of the two integers, kNull if both are kNull,
  /// or one of the values if the other is kNull.
  static int64_t NullableMin(int64_t left, int64_t right);

  /// \param smap_path file path to the smap file
  ///
  /// \return the used memory in bytes from the given smap file or kNull if the file does
  /// not exist or if it fails to read a valid value.
  static int64_t GetLinuxProcessMemoryBytesFromSmap(const std::string smap_path);

  /// \param proc_dir directory to scan for the process ids
  ///
  /// \return list of process ids found in the directory,
  /// or empty list if the directory doesn't exist
  static const std::vector<pid_t> GetPidsFromDir(
      const std::string proc_dir = kProcDirectory);

  /// \param pid the process id
  /// \param proc_dir directory to scan for the process ids
  ///
  /// \return the command line for the executing process,
  /// or empty string if the process doesn't exist
  static const std::string GetCommandLineForPid(
      pid_t pid, const std::string proc_dir = kProcDirectory);

  /// Truncates string if it is too long and append '...'
  ///
  /// \param value the string to truncate
  /// \param max_length the max length of the string value to preserve
  ///
  /// \return the debug string that contains the top N memory using process
  static const std::string TruncateString(const std::string value, uint32_t max_length);

  /// \param pid the process id
  /// \param proc_dir the process directory
  ///
  /// \return the used memory in bytes for the process,
  /// kNull if the file doesn't exist or it fails to find the fields
  static int64_t GetProcessMemoryBytes(pid_t pid,
                                       const std::string proc_dir = kProcDirectory);

  /// \param top_n the number of top memory-using processes
  /// \param all_usage process to memory usage map
  ///
  /// \return the top N memory-using processes
  static const std::vector<std::tuple<pid_t, int64_t>> GetTopNMemoryUsage(
      uint32_t top_n, const absl::flat_hash_map<pid_t, int64_t> all_usage);
};

}  // namespace ray
