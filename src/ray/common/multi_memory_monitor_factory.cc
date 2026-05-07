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

#include <memory>
#include <vector>

#include "ray/common/memory_monitor_factory.h"
#include "ray/common/memory_monitor_interface.h"
#include "ray/common/memory_monitor_utils.h"
#include "ray/common/noop_memory_monitor.h"
#include "ray/common/ray_config.h"
#include "ray/common/threshold_memory_monitor.h"
#include "ray/util/logging.h"

namespace ray {

std::vector<std::unique_ptr<MemoryMonitorInterface>> MemoryMonitorFactory::Create(
    KillWorkersCallback kill_workers_callback,
    bool resource_isolation_enabled,
    const CgroupManagerInterface &cgroup_manager) {
  std::vector<std::unique_ptr<MemoryMonitorInterface>> monitors;

  uint64_t monitor_interval_ms = RayConfig::instance().memory_monitor_refresh_ms();
  int64_t total_memory_bytes = MemoryMonitorUtils::TakeSystemMemoryUsageSnapshot(
                                   MemoryMonitorInterface::kDefaultCgroupPath)
                                   .total_bytes;
  int64_t memory_usage_threshold_bytes = MemoryMonitorUtils::GetMemoryThreshold(
      total_memory_bytes,
      RayConfig::instance().memory_usage_threshold(),
      RayConfig::instance().min_memory_free_bytes(),
      resource_isolation_enabled,
      cgroup_manager);

  if (monitor_interval_ms > 0) {
    monitors.push_back(std::make_unique<ThresholdMemoryMonitor>(
        std::move(kill_workers_callback),
        memory_usage_threshold_bytes,
        monitor_interval_ms,
        resource_isolation_enabled,
        MemoryMonitorInterface::kDefaultCgroupPath,
        /* user_cgroup_path */
        resource_isolation_enabled ? cgroup_manager.GetUserCgroupPath()
                                   : MemoryMonitorInterface::kDefaultCgroupPath,
        /* system_cgroup_path */
        resource_isolation_enabled ? cgroup_manager.GetSystemCgroupPath()
                                   : MemoryMonitorInterface::kDefaultCgroupPath));
  } else {
    RAY_LOG(INFO) << "ThresholdMemoryMonitor disabled. Specify "
                  << "`RAY_memory_monitor_refresh_ms` > 0 to enable the monitor.";
    monitors.push_back(std::make_unique<NoopMemoryMonitor>());
  }

  return monitors;
}

}  // namespace ray
