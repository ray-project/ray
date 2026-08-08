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

#include "ray/raylet/worker_killing_policy_factory.h"

#include <algorithm>
#include <memory>
#include <utility>

#include "ray/common/monitors/memory_monitor_utils.h"
#include "ray/common/ray_config.h"
#include "ray/raylet/worker_killing_policy_by_time.h"
#include "ray/raylet/worker_killing_policy_group_by_owner.h"
#include "ray/raylet/worker_killing_policy_interface.h"

namespace ray {

namespace raylet {

std::unique_ptr<WorkerKillingPolicyInterface> WorkerKillingPolicyFactory::Create(
    bool resource_isolation_enabled, const CgroupManagerInterface &cgroup_manager) {
  if (RayConfig::instance().worker_killing_policy_by_group()) {
    return std::make_unique<GroupByOwnerIdWorkerKillingPolicy>();
  }

  float usage_threshold = RayConfig::instance().memory_usage_threshold();
  int64_t min_memory_free_bytes = RayConfig::instance().min_memory_free_bytes();
  MemoryMonitorUtils::ValidateMemoryThresholdConfig(usage_threshold,
                                                    min_memory_free_bytes);

  auto memory_threshold_bytes_getter =
      [resource_isolation_enabled,
       &cgroup_manager,
       usage_threshold,
       min_memory_free_bytes](int64_t total_memory_bytes) {
        return MemoryMonitorUtils::GetMemoryThresholdOrNull(total_memory_bytes,
                                                            usage_threshold,
                                                            min_memory_free_bytes,
                                                            resource_isolation_enabled,
                                                            cgroup_manager);
      };

  auto kill_buffer_bytes_getter = [](int64_t total_memory_bytes) {
    int64_t kill_memory_buffer_bytes =
        RayConfig::instance().max_kill_memory_buffer_bytes();
    if (total_memory_bytes != MemoryMonitorInterface::kNull) {
      kill_memory_buffer_bytes =
          std::min(static_cast<int64_t>(
                       total_memory_bytes *
                       WorkerKillingPolicyInterface::kDefaultKillMemoryBufferProportion),
                   kill_memory_buffer_bytes);
    }
    return kill_memory_buffer_bytes;
  };

  return std::make_unique<TimeBasedWorkerKillingPolicy>(
      std::move(memory_threshold_bytes_getter), std::move(kill_buffer_bytes_getter));
}

}  // namespace raylet

}  // namespace ray
