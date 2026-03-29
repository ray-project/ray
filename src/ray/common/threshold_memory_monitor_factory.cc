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

#include <memory>

#include "ray/common/memory_monitor_factory.h"
#include "ray/common/memory_monitor_interface.h"
#include "ray/common/memory_monitor_utils.h"
#include "ray/common/noop_memory_monitor.h"
#include "ray/common/ray_config.h"
#include "ray/common/threshold_memory_monitor.h"
#include "ray/util/logging.h"

namespace ray {

std::unique_ptr<MemoryMonitorInterface> MemoryMonitorFactory::Create(
    KillWorkersCallback kill_workers_callback,
    bool resource_isolation_enabled,
    std::string cgroup_path,
    int64_t cgroup_upper_limit_bytes) {
  uint64_t monitor_interval_ms = RayConfig::instance().memory_monitor_refresh_ms();
  if (monitor_interval_ms <= 0) {
    RAY_LOG(INFO) << "MemoryMonitor disabled. Specify "
                  << "`RAY_memory_monitor_refresh_ms` > 0 to enable the monitor.";
    return std::make_unique<NoopMemoryMonitor>();
  }
  float usage_threshold = RayConfig::instance().memory_usage_threshold();
  RAY_CHECK_GE(usage_threshold, 0)
      << "Invalid configuration: usage_threshold must be >= 0";
  RAY_CHECK_LE(usage_threshold, 1)
      << "Invalid configuration: usage_threshold must be <= 1";
  int64_t total_memory_bytes =
      MemoryMonitorUtils::TakeSystemMemorySnapshot(cgroup_path).total_bytes;
  int64_t memory_usage_threshold_bytes = MemoryMonitorUtils::GetMemoryThreshold(
      total_memory_bytes, usage_threshold, RayConfig::instance().min_memory_free_bytes());
  if (resource_isolation_enabled) {
    memory_usage_threshold_bytes =
        cgroup_upper_limit_bytes -
        static_cast<int64_t>(RayConfig::instance().kill_memory_buffer_bytes());
  }
  return std::make_unique<ThresholdMemoryMonitor>(
      std::move(kill_workers_callback),
      memory_usage_threshold_bytes,
      RayConfig::instance().memory_monitor_refresh_ms());
}

}  // namespace ray
