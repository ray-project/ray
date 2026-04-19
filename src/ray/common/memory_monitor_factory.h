// Copyright 2025 The Ray Authors.
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

#include <memory>
#include <vector>

#include "ray/common/cgroup2/cgroup_manager_interface.h"
#include "ray/common/memory_monitor_interface.h"

namespace ray {

class MemoryMonitorFactory {
 public:
  /**
   * Create memory monitor instances based on configuration.
   *
   * On Linux, creates monitors based on configuration:
   *   - Resource isolation disabled: ThresholdMemoryMonitor only.
   *   - Resource isolation enabled, throttling disabled: ThresholdMemoryMonitor +
   *     PressureMemoryMonitor.
   *   - Resource isolation enabled, throttling enabled: EventMemoryMonitor only.
   *
   * On non-Linux platforms, returns a vector with a single NoopMemoryMonitor.
   *
   * @param kill_workers_callback function to invoke when memory pressure is detected.
   * @param resource_isolation_enabled When resource isolation is enabled, the
   * memory monitors will work with the configured cgroup constraints to better
   * enforce the memory usage limit.
   * @param memory_throttling_mode_enabled when enabled, the memory monitor will work
   * with cgroup constraints to balance between memory throttling and worker killing to
   * enforce stronger resource isolation between user and system slice.
   * @param cgroup_manager When resource isolation is enabled, the monitors will determine
   * the proper memory monitoring threshold based on the set cgroup constraints provided
   * by the cgroup manager.
   * @return a vector of memory monitor instances.
   */
  static std::vector<std::unique_ptr<MemoryMonitorInterface>> Create(
      KillWorkersCallback kill_workers_callback,
      bool resource_isolation_enabled,
      bool memory_throttling_mode_enabled,
      const CgroupManagerInterface &cgroup_manager);
};

}  // namespace ray
