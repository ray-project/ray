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

#include "ray/raylet/scheduling/scheduler_stats.h"

#include "absl/container/flat_hash_map.h"
#include "ray/common/ray_config.h"
#include "ray/common/task/task_spec.h"
#include "ray/raylet/scheduling/internal.h"

namespace ray {
namespace raylet {

void SchedulerStats::ComputeStats() {
  auto accumulator =
      [](size_t state,
         const std::pair<int, std::deque<std::shared_ptr<internal::Work>>> &pair) {
        return state + pair.second.size();
      };
  size_t num_waiting_for_resource = 0;
  size_t num_waiting_for_plasma_memory = 0;
  size_t num_waiting_for_remote_node_resources = 0;
  size_t num_worker_not_started_by_job_config_not_exist = 0;
  size_t num_worker_not_started_by_registration_timeout = 0;
  size_t num_worker_not_started_by_process_rate_limit = 0;
  size_t num_tasks_waiting_for_workers = 0;
  size_t num_cancelled_tasks = 0;

  size_t num_infeasible_tasks = std::accumulate(
      infeasible_tasks_.begin(), infeasible_tasks_.end(), (size_t)0, accumulator);

  // TODO(sang): Normally, the # of queued tasks are not large, so this is less likley to
  // be an issue that we iterate all of them. But if it uses lots of CPU, consider
  // optimizing by updating live instead of iterating through here.
  auto per_work_accumulator = [&num_waiting_for_resource, &num_waiting_for_plasma_memory,
                               &num_waiting_for_remote_node_resources,
                               &num_worker_not_started_by_job_config_not_exist,
                               &num_worker_not_started_by_registration_timeout,
                               &num_worker_not_started_by_process_rate_limit,
                               &num_tasks_waiting_for_workers, &num_cancelled_tasks](
                                  size_t state,
                                  const std::pair<
                                      int, std::deque<std::shared_ptr<internal::Work>>>
                                      &pair) {
    const auto &work_queue = pair.second;
    for (auto work_it = work_queue.begin(); work_it != work_queue.end();) {
      const auto &work = *work_it++;
      if (work->GetState() == internal::WorkStatus::WAITING_FOR_WORKER) {
        num_tasks_waiting_for_workers += 1;
      } else if (work->GetState() == internal::WorkStatus::CANCELLED) {
        num_cancelled_tasks += 1;
      } else if (work->GetUnscheduledCause() ==
                 internal::UnscheduledWorkCause::WAITING_FOR_RESOURCE_ACQUISITION) {
        num_waiting_for_resource += 1;
      } else if (work->GetUnscheduledCause() ==
                 internal::UnscheduledWorkCause::WAITING_FOR_AVAILABLE_PLASMA_MEMORY) {
        num_waiting_for_plasma_memory += 1;
      } else if (work->GetUnscheduledCause() ==
                 internal::UnscheduledWorkCause::WAITING_FOR_RESOURCES_AVAILABLE) {
        num_waiting_for_remote_node_resources += 1;
      } else if (work->GetUnscheduledCause() ==
                 internal::UnscheduledWorkCause::WORKER_NOT_FOUND_JOB_CONFIG_NOT_EXIST) {
        num_worker_not_started_by_job_config_not_exist += 1;
      } else if (work->GetUnscheduledCause() ==
                 internal::UnscheduledWorkCause::WORKER_NOT_FOUND_REGISTRATION_TIMEOUT) {
        num_worker_not_started_by_registration_timeout += 1;
      } else if (work->GetUnscheduledCause() ==
                 internal::UnscheduledWorkCause::WORKER_NOT_FOUND_RATE_LIMITED) {
        num_worker_not_started_by_process_rate_limit += 1;
      }
    }
    return state + pair.second.size();
  };
  size_t num_tasks_to_schedule =
      std::accumulate(tasks_to_schedule_.begin(), tasks_to_schedule_.end(), (size_t)0,
                      per_work_accumulator);
  size_t num_tasks_to_dispatch =
      std::accumulate(tasks_to_dispatch_.begin(), tasks_to_dispatch_.end(), (size_t)0,
                      per_work_accumulator);

  /// Update the internal states.
  num_waiting_for_resource = num_waiting_for_resource;
  num_waiting_for_plasma_memory = num_waiting_for_plasma_memory;
  num_waiting_for_remote_node_resources = num_waiting_for_remote_node_resources;
  num_worker_not_started_by_job_config_not_exist =
      num_worker_not_started_by_job_config_not_exist;
  num_worker_not_started_by_registration_timeout =
      num_worker_not_started_by_registration_timeout;
  num_worker_not_started_by_process_rate_limit =
      num_worker_not_started_by_process_rate_limit;
  num_tasks_waiting_for_workers = num_tasks_waiting_for_workers;
  num_cancelled_tasks = num_cancelled_tasks;
  num_infeasible_tasks = num_infeasible_tasks;
  num_tasks_to_schedule = num_tasks_to_schedule;
  num_tasks_to_dispatch = num_tasks_to_dispatch;
}

void SchedulerStats::RecordMetrics() const {
  /// This method intentionally doesn't call ComputeStats() because
  /// that function is expensive. ComputeStats is called by DebugStr method
  /// and they are always periodically called by node manager.
  stats::NumSpilledTasks.Record(metric_tasks_spilled);
  stats::NumInfeasibleSchedulingClasses.Record(infeasible_tasks_.size());

  /// Worker startup failure
  ray::stats::STATS_scheduler_failed_worker_startup_total.Record(
      num_worker_not_started_by_job_config_not_exist, "JobConfigMissing");
  ray::stats::STATS_scheduler_failed_worker_startup_total.Record(
      num_worker_not_started_by_registration_timeout, "RegistrationTimedOut");
  ray::stats::STATS_scheduler_failed_worker_startup_total.Record(
      num_worker_not_started_by_process_rate_limit, "RateLimited");

  /// Queued tasks.
  ray::stats::STATS_scheduler_tasks.Record(num_cancelled_tasks, "Cancelled");
  ray::stats::STATS_scheduler_tasks.Record(executing_task_args_.size(), "Executing");
  ray::stats::STATS_scheduler_tasks.Record(waiting_tasks_index_.size(), "Waiting");
  ray::stats::STATS_scheduler_tasks.Record(num_tasks_to_dispatch, "Dispatched");
  ray::stats::STATS_scheduler_tasks.Record(num_tasks_to_schedule, "Received");

  /// Pending task count.
  ray::stats::STATS_scheduler_unscheduleable_tasks.Record(num_infeasible_tasks,
                                                          "Infeasible");
  ray::stats::STATS_scheduler_unscheduleable_tasks.Record(num_waiting_for_resource,
                                                          "WaitingForResources");
  ray::stats::STATS_scheduler_unscheduleable_tasks.Record(num_waiting_for_plasma_memory,
                                                          "WaitingForPlasmaMemory");
  ray::stats::STATS_scheduler_unscheduleable_tasks.Record(
      num_waiting_for_remote_node_resources, "WaitingForRemoteResources");
  ray::stats::STATS_scheduler_unscheduleable_tasks.Record(num_tasks_waiting_for_workers,
                                                          "WaitingForWorkers");
}

std::string SchedulerStats::DebugStr() const {
  ComputeStats();
  if (num_tasks_to_schedule + num_tasks_to_dispatch + num_infeasible_tasks > 1000) {
    RAY_LOG(WARNING)
        << "More than 1000 tasks are queued in this node. This can cause slow down.";
  }

  std::stringstream buffer;
  buffer << "Infeasible queue length: " << num_infeasible_tasks << "\n";
  buffer << "Schedule queue length: " << num_tasks_to_schedule << "\n";
  buffer << "Dispatch queue length: " << num_tasks_to_dispatch << "\n";
  buffer << "num_waiting_for_resource: " << num_waiting_for_resource << "\n";
  buffer << "num_waiting_for_plasma_memory: " << num_waiting_for_plasma_memory << "\n";
  buffer << "num_waiting_for_remote_node_resources: "
         << num_waiting_for_remote_node_resources << "\n";
  buffer << "num_worker_not_started_by_job_config_not_exist: "
         << num_worker_not_started_by_job_config_not_exist << "\n";
  buffer << "num_worker_not_started_by_registration_timeout: "
         << num_worker_not_started_by_registration_timeout << "\n";
  buffer << "num_worker_not_started_by_process_rate_limit: "
         << num_worker_not_started_by_process_rate_limit << "\n";
  buffer << "num_tasks_waiting_for_workers: " << num_tasks_waiting_for_workers << "\n";
  buffer << "num_cancelled_tasks: " << num_cancelled_tasks << "\n";
}

}  // namespace raylet
}  // namespace ray
