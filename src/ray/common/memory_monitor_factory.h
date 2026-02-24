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
   * @return a unique pointer to the memory monitor instance.
   */
  static std::unique_ptr<MemoryMonitorInterface> Create(
      KillWorkersCallback kill_workers_callback);
};

}  // namespace ray
