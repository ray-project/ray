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

namespace ray {
namespace raylet {

class SchedulerStats {
 public:
  SchedulerStats();
  void ComputeStats();
  void RecordMetrics() const;
  std::string SchedulerStats::DebugStr() const;

 private:
  const absl::flat_hash_map<SchedulingClass, std::deque<std::shared_ptr<internal::Work>>>
      &tasks_to_schedule_;

  const absl::flat_hash_map<SchedulingClass, std::deque<std::shared_ptr<internal::Work>>>
      &tasks_to_dispatch_;

  const absl::flat_hash_map<SchedulingClass, std::deque<std::shared_ptr<internal::Work>>>
      &infeasible_tasks_;

  const absl::flat_hash_map<SchedulingClass, SchedulingClassInfo> &info_by_sched_cls_;
  const WorkerPoolInterface &worker_pool_;

  /// Number of tasks that are spilled to other
  /// nodes because it cannot be scheduled locally.
  int64_t metric_tasks_spilled = 0;
  /// Number of tasks that are waiting for
  /// resources to be available locally.
  int64_t num_waiting_for_resource = 0;
  /// Number of tasks that are waiting for available memory
  /// from the plasma store.
  int64_t num_waiting_for_plasma_memory = 0;
  /// Number of tasks that are waiting for nodes with available resources.
  int64_t num_waiting_for_remote_node_resources = 0;
  /// Number of workers that couldn't be started because the job config wasn't local.
  int64_t num_worker_not_started_by_job_config_not_exist = 0;
  /// Number of workers that couldn't be started because the worker registration timed
  /// out.
  int64_t num_worker_not_started_by_registration_timeout = 0;
  /// Number of workers that couldn't be started becasue it hits the worker startup rate
  /// limit.
  int64_t num_worker_not_started_by_process_rate_limit = 0;
  /// Number of tasks that are waiting for worker processes to start.
  int64_t num_tasks_waiting_for_workers = 0;
  /// Number of cancelled tasks.
  int64_t num_cancelled_tasks = 0;
  /// Number of infeasible tasks.
  int64_t num_infeasible_tasks = 0;
  /// Number of tasks to schedule.
  int64_t num_tasks_to_schedule = 0;
  /// Number of tasks to dispatch.
  int64_t num_tasks_to_dispatch = 0;
};

}  // namespace raylet
}  // namespace ray
