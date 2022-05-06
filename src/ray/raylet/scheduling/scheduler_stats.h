// Copyright 2020-2021 The Ray Authors.
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

#include "absl/container/flat_hash_map.h"
#include "ray/common/ray_config.h"
#include "ray/common/task/task_spec.h"
#include "ray/raylet/scheduling/internal.h"
#include "ray/raylet/scheduling/local_task_manager_interface.h"

namespace ray {
namespace raylet {
class ClusterTaskManager;

// Helper class that collects and reports scheduler's metrics into counters or human
// readable string.
class SchedulerStats {
 public:
  explicit SchedulerStats(const ClusterTaskManager &cluster_task_manager,
                          const ILocalTaskManager &local_task_manager);

  // Report metrics doesn't recompute the stats.
  void RecordMetrics() const;

  // Recompute the stats and report the result as string.
  std::string ComputeAndReportDebugStr();

  // increase the task spilled counter.
  void TaskSpilled();

 private:
  // recompute the metrics.
  void ComputeStats();

  const ClusterTaskManager &cluster_task_manager_;
  const ILocalTaskManager &local_task_manager_;

  /// Number of tasks that are spilled to other
  /// nodes because it cannot be scheduled locally.
  int64_t metric_tasks_spilled_ = 0;
  /// Number of tasks that are waiting for
  /// resources to be available locally.
  int64_t num_waiting_for_resource_ = 0;
  /// Number of tasks that are waiting for available memory
  /// from the plasma store.
  int64_t num_waiting_for_plasma_memory_ = 0;
  /// Number of tasks that are waiting for nodes with available resources.
  int64_t num_waiting_for_remote_node_resources_ = 0;
  /// Number of workers that couldn't be started because the job config wasn't local.
  int64_t num_worker_not_started_by_job_config_not_exist_ = 0;
  /// Number of workers that couldn't be started because the worker registration timed
  /// out.
  int64_t num_worker_not_started_by_registration_timeout_ = 0;
  /// Number of workers that couldn't be started becasue it hits the worker startup rate
  /// limit.
  int64_t num_worker_not_started_by_process_rate_limit_ = 0;
  /// Number of tasks that are waiting for worker processes to start.
  int64_t num_tasks_waiting_for_workers_ = 0;
  /// Number of cancelled tasks.
  int64_t num_cancelled_tasks_ = 0;
  /// Number of infeasible tasks.
  int64_t num_infeasible_tasks_ = 0;
  /// Number of tasks to schedule.
  int64_t num_tasks_to_schedule_ = 0;
  /// Number of tasks to dispatch.
  int64_t num_tasks_to_dispatch_ = 0;
};

}  // namespace raylet
}  // namespace ray
