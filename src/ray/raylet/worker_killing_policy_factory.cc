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

#include "ray/common/memory_monitor_utils.h"
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

  int64_t total_memory_bytes = MemoryMonitorUtils::TakeSystemMemorySnapshot(
                                   MemoryMonitorInterface::kDefaultCgroupPath)
                                   .total_bytes;
  int64_t memory_usage_threshold_bytes = MemoryMonitorUtils::GetMemoryThreshold(
      total_memory_bytes,
      RayConfig::instance().memory_usage_threshold(),
      RayConfig::instance().min_memory_free_bytes(),
      resource_isolation_enabled,
      RayConfig::instance().enable_memory_throttling_mode(),
      cgroup_manager);

  int64_t kill_memory_buffer_bytes =
      std::min(static_cast<int64_t>(
                   total_memory_bytes *
                   WorkerKillingPolicyInterface::kDefaultKillMemoryBufferProportion),
               RayConfig::instance().max_kill_memory_buffer_bytes());
  return std::make_unique<TimeBasedWorkerKillingPolicy>(memory_usage_threshold_bytes,
                                                        kill_memory_buffer_bytes);
}

}  // namespace raylet

}  // namespace ray
