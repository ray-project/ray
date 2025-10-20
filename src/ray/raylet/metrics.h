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

inline ray::stats::Gauge GetResourceUsageGaugeMetric() {
  return ray::stats::Gauge{
      /*name=*/"resources",
      /*description=*/"Logical Ray resources broken per state {AVAILABLE, USED}",
      /*unit=*/"",
      /*tag_keys=*/{"Name", "State"},
  };
}

struct SchedulerMetrics {
  ray::observability::MetricInterface &scheduler_tasks;
  ray::observability::MetricInterface &scheduler_unscheduleable_tasks;
  ray::observability::MetricInterface &scheduler_failed_worker_startup_total;
  ray::observability::MetricInterface &internal_num_spilled_tasks;
  ray::observability::MetricInterface &internal_num_infeasible_scheduling_classes;
};

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

inline ray::stats::Gauge GetMemoryManagerWorkerEvictionTotalGaugeMetric() {
  return ray::stats::Gauge{
      /*name=*/"memory_manager_worker_eviction_total",
      /*description=*/
      "Total worker eviction events broken per work type {Actor, Task, Driver} and name.",
      /*unit=*/"",
      /*tag_keys=*/{"Type", "Name"},
  };
}

}  // namespace raylet
}  // namespace ray
