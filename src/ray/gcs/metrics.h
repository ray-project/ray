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

#include "ray/stats/metric.h"

namespace ray {
namespace gcs {

inline ray::stats::Gauge GetRunningJobMetric() {
  return ray::stats::Gauge{
      /*name=*/"running_jobs",
      /*description=*/"Number of jobs currently running.",
      /*unit=*/"",
      /*tag_keys=*/{},
  };
}

inline ray::stats::Count GetFinishedJobMetric() {
  return ray::stats::Count{
      /*name=*/"finished_jobs",
      /*description=*/"Number of jobs finished.",
      /*unit=*/"",
      /*tag_keys=*/{},
  };
}

inline ray::stats::Gauge GetJobDurationInSecondsMetric() {
  return ray::stats::Gauge{
      /*name=*/"job_duration_s",
      /*description=*/"Duration of jobs finished in seconds.",
      /*unit=*/"",
      /*tag_keys=*/{"JobId"},
  };
}

inline ray::stats::Gauge GetPlacementGroupMetric() {
  return ray::stats::Gauge{
      /*name=*/"placement_groups",
      /*description=*/"Number of placement groups broken down by state.",
      /*unit=*/"",
      // State: from rpc::PlacementGroupData::PlacementGroupState.
      /*tag_keys=*/{"State"},
  };
}

inline ray::stats::Histogram GetPlacementGroupCreationLatencyInMsMetric() {
  return ray::stats::Histogram{
      /*name=*/"gcs_placement_group_creation_latency_ms",
      /*description=*/"end to end latency of placement group creation",
      /*unit=*/"",
      /*boundaries=*/{0.1, 1, 10, 100, 1000, 10000},
      /*tag_keys=*/{},
  };
}

inline ray::stats::Histogram GetPlacementGroupSchedulingLatencyInMsMetric() {
  return ray::stats::Histogram{
      /*name=*/"gcs_placement_group_scheduling_latency_ms",
      /*description=*/"scheduling latency of placement groups",
      /*unit=*/"",
      /*boundaries=*/{0.1, 1, 10, 100, 1000, 10000},
      /*tag_keys=*/{},
  };
}

inline ray::stats::Gauge GetTaskManagerTaskEventsReportedMetric() {
  return ray::stats::Gauge{
      /*name=*/"gcs_task_manager_task_events_reported",
      /*description=*/"Number of all task events reported to gcs.",
      /*unit=*/"",
      /*tag_keys=*/{},
  };
}

inline ray::stats::Gauge GetTaskManagerTaskEventsDroppedMetric() {
  return ray::stats::Gauge{
      /*name=*/"gcs_task_manager_task_events_dropped",
      /*description=*/
      "Number of task events dropped per type {PROFILE_EVENT, STATUS_EVENT}",
      /*unit=*/"",
      /*tag_keys=*/{"Type"},
  };
}

inline ray::stats::Gauge GetTaskManagerTaskEventsStoredMetric() {
  return ray::stats::Gauge{
      /*name=*/"gcs_task_manager_task_events_stored",
      /*description=*/"Number of task events stored in GCS.",
      /*unit=*/"",
      /*tag_keys=*/{},
  };
}

inline ray::stats::Gauge GetGcsActorByStateMetric() {
  return ray::stats::Gauge{
      /*name=*/"gcs_actors_count",
      /*description=*/
      "Number of actors per state {Created, Destroyed, Unresolved, Pending}",
      /*unit=*/"",
      /*tag_keys=*/{"State"},
  };
}

inline ray::stats::Histogram GetGcsStorageOperationLatencyInMsMetric() {
  return ray::stats::Histogram{
      /*name=*/"gcs_storage_operation_latency_ms",
      /*description=*/"Time to invoke an operation on Gcs storage",
      /*unit=*/"",
      /*boundaries=*/{0.1, 1, 10, 100, 1000, 10000},
      /*tag_keys=*/{"Operation"},
  };
}

inline ray::stats::Count GetGcsStorageOperationCountMetric() {
  return ray::stats::Count{
      /*name=*/"gcs_storage_operation_count",
      /*description=*/"Number of operations invoked on Gcs storage",
      /*unit=*/"",
      /*tag_keys=*/{"Operation"},
  };
}

}  // namespace gcs
}  // namespace ray
