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

#include <sys/types.h>

#include <cstdint>
#include <functional>
#include <ostream>

#include "absl/container/flat_hash_map.h"
#include "ray/util/compat.h"

namespace ray {

/**
 * @brief A snapshot of aggregated memory usage across the system.
 */
struct SystemMemorySnapshot {
  int64_t used_bytes;

  /// The total memory available on the system. >= used_bytes.
  int64_t total_bytes;

  friend std::ostream &operator<<(std::ostream &os,
                                  const SystemMemorySnapshot &memory_snapshot) {
    os << "Used bytes: " << memory_snapshot.used_bytes
       << ", Total bytes: " << memory_snapshot.total_bytes;
    return os;
  }
};

/**
 * @brief A snapshot of per-process memory usage.
 */
using ProcessesMemorySnapshot = absl::flat_hash_map<pid_t, int64_t>;

/**
 * @brief Callback that runs at each monitoring interval.
 *
 * \param system_memory snapshot of system memory information.
 */
using KillWorkersCallback =
    std::function<void(const SystemMemorySnapshot &system_memory)>;

/**
 * @brief implementations of this interface monitors the memory usage of the node
 * and triggers the kill worker callback when it deems the system is under memory
 * pressure.
 *
 * @note All implementations of the memory monitor must be thread safe.
 */
class MemoryMonitorInterface {
 public:
  virtual ~MemoryMonitorInterface() = default;

  /**
   * @brief Enables the memory monitor to trigger the kill callback.
   */
  virtual void Enable() = 0;

  /**
   * @brief Disables the memory monitor from triggering the kill callback.
   */
  virtual void Disable() = 0;

  /**
   * @return True if the memory monitor is enabled, false otherwise.
   */
  virtual bool IsEnabled() = 0;

  static constexpr char kDefaultCgroupPath[] = "/sys/fs/cgroup";
  static constexpr int64_t kNull = -1;
  /// The logging frequency. Decoupled from how often the monitor runs.
  static constexpr uint32_t kLogIntervalMs = 5000;
};

}  // namespace ray
