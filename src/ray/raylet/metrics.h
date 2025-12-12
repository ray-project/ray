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

#include "ray/observability/metrics.h"
#include "ray/stats/metric.h"

namespace ray {
namespace raylet {

struct SchedulerMetrics {
  ray::observability::MetricInterface &scheduler_tasks;
  ray::observability::MetricInterface &scheduler_unscheduleable_tasks;
  ray::observability::MetricInterface &scheduler_failed_worker_startup_total;
  ray::observability::MetricInterface &internal_num_spilled_tasks;
  ray::observability::MetricInterface &internal_num_infeasible_scheduling_classes;
};

struct WorkerPoolMetrics {
  ray::observability::MetricInterface &num_workers_started_sum;
  ray::observability::MetricInterface &num_cached_workers_skipped_job_mismatch_sum;
  ray::observability::MetricInterface
      &num_cached_workers_skipped_runtime_environment_mismatch_sum;
  ray::observability::MetricInterface
      &num_cached_workers_skipped_dynamic_options_mismatch_sum;
  ray::observability::MetricInterface &num_workers_started_from_cache_sum;
  ray::observability::MetricInterface &worker_register_time_ms_histogram;
};

struct SpillManagerMetrics {
  ray::observability::MetricInterface &spill_manager_objects_gauge;
  ray::observability::MetricInterface &spill_manager_objects_bytes_gauge;
  ray::observability::MetricInterface &spill_manager_request_total_gauge;
  ray::observability::MetricInterface &spill_manager_throughput_mb_gauge;
};

inline ray::stats::Gauge GetResourceUsageGaugeMetric() {
  return ray::stats::Gauge{
      /*name=*/"resources",
      /*description=*/"Logical Ray resources broken per state {AVAILABLE, USED}",
      /*unit=*/"",
      /*tag_keys=*/{"Name", "State"},
  };
}

inline ray::stats::Gauge GetSchedulerTasksGaugeMetric() {
  return ray::stats::Gauge{
      /*name=*/"scheduler_tasks",
      /*description=*/
      "Number of tasks waiting for scheduling broken per state {Cancelled, Executing, "
      "Waiting, Dispatched, Received}.",
      /*unit=*/"",
      /*tag_keys=*/{"State"},
  };
}

inline ray::stats::Gauge GetSchedulerUnscheduleableTasksGaugeMetric() {
  return ray::stats::Gauge{
      /*name=*/"scheduler_unscheduleable_tasks",
      /*description=*/
      "Number of pending tasks (not scheduleable tasks) broken per reason "
      "{Infeasible, WaitingForResources, "
      "WaitingForPlasmaMemory, WaitingForRemoteResources, WaitingForWorkers}.",
      /*unit=*/"",
      /*tag_keys=*/{"Reason"},
  };
}

inline ray::stats::Gauge GetSchedulerFailedWorkerStartupTotalGaugeMetric() {
  return ray::stats::Gauge{
      /*name=*/"scheduler_failed_worker_startup_total",
      /*description=*/
      "Number of tasks that fail to be scheduled because workers were not "
      "available. Labels are broken per reason {JobConfigMissing, "
      "RegistrationTimedOut, RateLimited}",
      /*unit=*/"",
      /*tag_keys=*/{"Reason"},
  };
}

inline ray::stats::Gauge GetInternalNumSpilledTasksGaugeMetric() {
  return ray::stats::Gauge{
      /*name=*/"internal_num_spilled_tasks",
      /*description=*/
      "The cumulative number of lease requeusts that this raylet has spilled to other "
      "raylets.",
      /*unit=*/"tasks"};
}

inline ray::stats::Gauge GetInternalNumInfeasibleSchedulingClassesGaugeMetric() {
  return ray::stats::Gauge{
      /*name=*/"internal_num_infeasible_scheduling_classes",
      /*description=*/"The number of unique scheduling classes that are infeasible.",
      /*unit=*/"tasks"};
}

inline ray::stats::Gauge GetSpillManagerObjectsGaugeMetric() {
  return ray::stats::Gauge{
      /*name=*/"spill_manager_objects",
      /*description=*/
      "Number of local objects broken per state {Pinned, PendingRestore, PendingSpill}.",
      /*unit=*/"",
      /*tag_keys=*/{"State"},
  };
}

inline ray::stats::Gauge GetSpillManagerObjectsBytesGaugeMetric() {
  return ray::stats::Gauge{
      /*name=*/"spill_manager_objects_bytes",
      /*description=*/
      "Byte size of local objects broken per state {Pinned, PendingSpill}.",
      /*unit=*/"",
      /*tag_keys=*/{"State"},
  };
}

inline ray::stats::Gauge GetSpillManagerRequestTotalGaugeMetric() {
  return ray::stats::Gauge{
      /*name=*/"spill_manager_request_total",
      /*description=*/"Number of {spill, restore} requests.",
      /*unit=*/"",
      /*tag_keys=*/{"Type"},
  };
}

inline ray::stats::Gauge GetSpillManagerThroughputMBGaugeMetric() {
  return ray::stats::Gauge{
      /*name=*/"spill_manager_throughput_mb",
      /*description=*/"The throughput of {spill, restore} requests in MB.",
      /*unit=*/"",
      /*tag_keys=*/{"Type"},
  };
}

inline ray::stats::Count GetMemoryManagerWorkerEvictionTotalCountMetric() {
  return ray::stats::Count{
      /*name=*/"memory_manager_worker_eviction",
      /*description=*/
      "Total worker eviction events broken per work type {Actor, Task, Driver} and name.",
      /*unit=*/"",
      /*tag_keys=*/{"Type", "Name"},
  };
}

inline ray::stats::Sum GetNumWorkersStartedMetric() {
  return ray::stats::Sum{
      /*name=*/"internal_num_processes_started",
      /*description=*/"The total number of worker processes the worker pool has created.",
      /*unit=*/"processes"};
}

inline ray::stats::Sum GetNumCachedWorkersSkippedJobMismatchMetric() {
  return ray::stats::Sum{
      /*name=*/"internal_num_processes_skipped_job_mismatch",
      /*description=*/"The total number of cached workers skipped due to job mismatch.",
      /*unit=*/"workers"};
}

inline ray::stats::Sum GetNumCachedWorkersSkippedRuntimeEnvironmentMismatchMetric() {
  return ray::stats::Sum{
      /*name=*/"internal_num_processes_skipped_runtime_environment_mismatch",
      /*description=*/
      "The total number of cached workers skipped due to runtime environment mismatch.",
      /*unit=*/"workers"};
}

inline ray::stats::Sum GetNumCachedWorkersSkippedDynamicOptionsMismatchMetric() {
  return ray::stats::Sum{
      /*name=*/"internal_num_processes_skipped_dynamic_options_mismatch",
      /*description=*/
      "The total number of cached workers skipped due to dynamic options mismatch.",
      /*unit=*/"workers"};
}

inline ray::stats::Sum GetNumWorkersStartedFromCacheMetric() {
  return ray::stats::Sum{
      /*name=*/"internal_num_processes_started_from_cache",
      /*description=*/"The total number of workers started from a cached worker process.",
      /*unit=*/"workers"};
}

inline ray::stats::Histogram GetWorkerRegisterTimeMsHistogramMetric() {
  return ray::stats::Histogram{
      /*name=*/"worker_register_time_ms",
      /*description=*/"end to end latency of register a worker process.",
      /*unit=*/"ms",
      /*boundaries=*/{1, 10, 100, 1000, 10000},
  };
}

inline ray::stats::Gauge GetLocalResourceViewNodeCountGaugeMetric() {
  return ray::stats::Gauge{
      /*name=*/"local_resource_view_node_count",
      /*description=*/"Number of nodes tracked locally by the reporting raylet.",
      /*unit=*/"",
  };
}

}  // namespace raylet
}  // namespace ray
