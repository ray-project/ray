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
#include "ray/common/ray_config.h"
#include "ray/common/threshold_memory_monitor.h"
#include "ray/util/logging.h"

namespace ray {

std::unique_ptr<MemoryMonitor> MemoryMonitorFactory::Create(
    instrumented_io_context &io_service, KillWorkersCallback kill_workers_callback) {
  RAY_LOG(INFO) << "Creating ThresholdMemoryMonitor with usage_threshold="
                << RayConfig::instance().memory_usage_threshold()
                << ", min_memory_free_bytes="
                << RayConfig::instance().min_memory_free_bytes()
                << ", monitor_interval_ms="
                << RayConfig::instance().memory_monitor_refresh_ms();
  return std::make_unique<ThresholdMemoryMonitor>(
      io_service,
      kill_workers_callback,
      RayConfig::instance().memory_usage_threshold(),
      RayConfig::instance().min_memory_free_bytes(),
      RayConfig::instance().memory_monitor_refresh_ms());
}

}  // namespace ray
