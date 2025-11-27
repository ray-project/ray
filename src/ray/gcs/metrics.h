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
namespace gcs {

struct GcsServerMetrics {
  ray::observability::MetricInterface &actor_by_state_gauge;
  ray::observability::MetricInterface &gcs_actor_by_state_gauge;
  ray::observability::MetricInterface &running_job_gauge;
  ray::observability::MetricInterface &finished_job_counter;
  ray::observability::MetricInterface &job_duration_in_seconds_gauge;
  ray::observability::MetricInterface &placement_group_gauge;
  ray::observability::MetricInterface &placement_group_creation_latency_in_ms_histogram;
  ray::observability::MetricInterface &placement_group_scheduling_latency_in_ms_histogram;
  ray::observability::MetricInterface &placement_group_count_gauge;
  ray::observability::MetricInterface &task_events_reported_gauge;
  ray::observability::MetricInterface &task_events_dropped_gauge;
  ray::observability::MetricInterface &task_events_stored_gauge;
  ray::observability::MetricInterface &event_recorder_dropped_events_counter;
  ray::observability::MetricInterface &storage_operation_latency_in_ms_histogram;
  ray::observability::MetricInterface &storage_operation_count_counter;
  ray::observability::MetricInterface &scheduler_placement_time_ms_histogram;
};

inline ray::stats::Gauge GetRunningJobGaugeMetric() {
  return ray::stats::Gauge{
      /*name=*/"running_jobs",
      /*description=*/"Number of jobs currently running.",
      /*unit=*/"",
      /*tag_keys=*/{},
  };
}

inline ray::stats::Count GetFinishedJobCounterMetric() {
  return ray::stats::Count{
      /*name=*/"finished_jobs",
      /*description=*/"Number of jobs finished.",
      /*unit=*/"",
      /*tag_keys=*/{},
  };
}

inline ray::stats::Gauge GetJobDurationInSecondsGaugeMetric() {
  return ray::stats::Gauge{
      /*name=*/"job_duration_s",
      /*description=*/"Duration of jobs finished in seconds.",
      /*unit=*/"",
      /*tag_keys=*/{"JobId"},
  };
}

inline ray::stats::Gauge GetPlacementGroupGaugeMetric() {
  return ray::stats::Gauge{
      /*name=*/"placement_groups",
      /*description=*/"Number of placement groups broken down by state.",
      /*unit=*/"",
      // State: from rpc::PlacementGroupData::PlacementGroupState.
      /*tag_keys=*/{"State", "Source"},
  };
}

inline ray::stats::Histogram GetPlacementGroupCreationLatencyInMsHistogramMetric() {
  return ray::stats::Histogram{
      /*name=*/"gcs_placement_group_creation_latency_ms",
      /*description=*/"end to end latency of placement group creation",
      /*unit=*/"",
      /*boundaries=*/{0.1, 1, 10, 100, 1000, 10000},
      /*tag_keys=*/{},
  };
}

inline ray::stats::Histogram GetPlacementGroupSchedulingLatencyInMsHistogramMetric() {
  return ray::stats::Histogram{
      /*name=*/"gcs_placement_group_scheduling_latency_ms",
      /*description=*/"scheduling latency of placement groups",
      /*unit=*/"",
      /*boundaries=*/{0.1, 1, 10, 100, 1000, 10000},
      /*tag_keys=*/{},
  };
}

inline ray::stats::Gauge GetPlacementGroupCountGaugeMetric() {
  return ray::stats::Gauge{
      /*name=*/"gcs_placement_group_count",
      /*description=*/
      "Number of placement groups broken down by state in {Registered, Pending, "
      "Infeasible}",
      /*unit=*/"",
      /*tag_keys=*/{"State"},
  };
}

inline ray::stats::Gauge GetTaskManagerTaskEventsReportedGaugeMetric() {
  return ray::stats::Gauge{
      /*name=*/"gcs_task_manager_task_events_reported",
      /*description=*/"Number of all task events reported to gcs.",
      /*unit=*/"",
      /*tag_keys=*/{},
  };
}

inline ray::stats::Gauge GetTaskManagerTaskEventsDroppedGaugeMetric() {
  return ray::stats::Gauge{
      /*name=*/"gcs_task_manager_task_events_dropped",
      /*description=*/
      "Number of task events dropped per type {PROFILE_EVENT, STATUS_EVENT}",
      /*unit=*/"",
      /*tag_keys=*/{"Type"},
  };
}

inline ray::stats::Gauge GetTaskManagerTaskEventsStoredGaugeMetric() {
  return ray::stats::Gauge{
      /*name=*/"gcs_task_manager_task_events_stored",
      /*description=*/"Number of task events stored in GCS.",
      /*unit=*/"",
      /*tag_keys=*/{},
  };
}

inline ray::stats::Gauge GetGcsActorByStateGaugeMetric() {
  return ray::stats::Gauge{
      /*name=*/"gcs_actors_count",
      /*description=*/
      "Number of actors per state {Created, Destroyed, Unresolved, Pending}",
      /*unit=*/"",
      /*tag_keys=*/{"State"},
  };
}

inline ray::stats::Histogram GetGcsStorageOperationLatencyInMsHistogramMetric() {
  return ray::stats::Histogram{
      /*name=*/"gcs_storage_operation_latency_ms",
      /*description=*/"Time to invoke an operation on Gcs storage",
      /*unit=*/"",
      /*boundaries=*/{0.1, 1, 10, 100, 1000, 10000},
      /*tag_keys=*/{"Operation"},
  };
}

inline ray::stats::Count GetGcsStorageOperationCountCounterMetric() {
  return ray::stats::Count{
      /*name=*/"gcs_storage_operation_count",
      /*description=*/"Number of operations invoked on Gcs storage",
      /*unit=*/"",
      /*tag_keys=*/{"Operation"},
  };
}

}  // namespace gcs
}  // namespace ray
