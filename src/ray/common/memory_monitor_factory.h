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

#include "ray/common/memory_monitor_interface.h"

namespace ray {

/// Factory class for creating MemoryMonitor instances.
///
/// This feature is only enabled on Linux. On Linux, it creates a ThresholdMemoryMonitor
/// that monitors memory usage using /proc filesystem and cgroups.
///
/// On non-Linux platforms, this will return a no-op implementation.
class MemoryMonitorFactory {
 public:
  /**
   * Create a memory monitor instance.
   *
   * On Linux, creates a ThresholdMemoryMonitor that monitors memory usage
   * and triggers the callback when usage is refreshed.
   *
   * On non-Linux platforms, creates a NoopMemoryMonitor that does nothing.
   *
   * @param kill_workers_callback function to execute when the memory usage is refreshed.
   * @param resource_isolation_enabled When resource isolation is enabled, the
   * memory monitor will work with the configured cgroup constraints to better
   * enforce the memory usage limit.
   * @param cgroup_path the path to the cgroup relevant monitors will use to monitor.
   * @param cgroup_upper_limit_bytes the upper memory limit of the given cgroup in bytes.
   * This is used to determine the threshold to monitor for relevant memory monitors.
   * @return a unique pointer to the memory monitor instance.
   */
  static std::unique_ptr<MemoryMonitorInterface> Create(
      KillWorkersCallback kill_workers_callback,
      bool resource_isolation_enabled,
      std::string cgroup_path,
      int64_t cgroup_upper_limit_bytes);
};

}  // namespace ray
