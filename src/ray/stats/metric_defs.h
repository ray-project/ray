// Copyright 2017 The Ray Authors.
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

namespace stats {

/// The definitions of metrics that you can use everywhere.
///
/// There are 4 types of metric. The values of the metrics are of type double.
///   Histogram: Histogram distribution of metric points.
///   Gauge: Keeps the last recorded value, drops everything before.
///   Count: The count of the number of metric points.
///   Sum: A sum up of the metric points.
///
/// You can follow these examples to define your metrics.

/// NOTE: When adding a new metric, add the metric name to the _METRICS list in
/// python/ray/tests/test_metrics_agent.py to ensure that its existence is tested.

/// Convention
/// Following Prometheus convention
/// https://prometheus.io/docs/concepts/data_model/#metric-names-and-labels
/// Units: ray_[component]_[metrics_name]_[units] (e.g.,
/// ray_pull_manager_spill_throughput_mb) Gauge: ray_[component]_[metrics_name]s (e.g.,
/// ray_pull_manager_requests) Cumulative Gauge / Count:
/// ray_[component]_[metrics_name]_total (e.g., ray_pull_manager_total)
///

/// Tasks stats, broken down by state.
DECLARE_stats(tasks);

/// Actor stats, broken down by state.
DECLARE_stats(actors);

/// Job stats.
DECLARE_stats(running_jobs);
DECLARE_stats(finished_jobs);
DECLARE_stats(job_duration_s);

/// Placement group stats, broken down by state.
DECLARE_stats(placement_groups);

/// ASIO stats
DECLARE_stats(io_context_event_loop_lag_ms);

/// Event stats
DECLARE_stats(operation_count);
DECLARE_stats(operation_run_time_ms);
DECLARE_stats(operation_queue_time_ms);
DECLARE_stats(operation_active_count);

/// GRPC server
DECLARE_stats(grpc_server_req_process_time_ms);
DECLARE_stats(grpc_server_req_new);
DECLARE_stats(grpc_server_req_handling);
DECLARE_stats(grpc_server_req_finished);
DECLARE_stats(grpc_server_req_succeeded);
DECLARE_stats(grpc_server_req_failed);

/// GRPC Client Failures
DECLARE_stats(grpc_client_req_failed);

/// Object Manager.
DECLARE_stats(object_manager_bytes);
DECLARE_stats(object_manager_received_chunks);

/// Pull Manager
DECLARE_stats(pull_manager_usage_bytes);
// TODO(sang): Remove pull_manager_active_bundles and
// support active/inactive get/wait/task_args
DECLARE_stats(pull_manager_requested_bundles);
DECLARE_stats(pull_manager_requests);
DECLARE_stats(pull_manager_active_bundles);
DECLARE_stats(pull_manager_retries_total);
DECLARE_stats(pull_manager_num_object_pins);
DECLARE_stats(pull_manager_object_request_time_ms);

/// Push Manager
DECLARE_stats(push_manager_num_pushes_remaining);
DECLARE_stats(push_manager_chunks);

/// Scheduler
DECLARE_stats(scheduler_failed_worker_startup_total);
DECLARE_stats(scheduler_tasks);
DECLARE_stats(scheduler_unscheduleable_tasks);
DECLARE_stats(scheduler_placement_time_s);

/// Raylet Resource Manager
DECLARE_stats(resources);

/// TODO(rickyx): migrate legacy metrics
/// Local Object Manager
DECLARE_stats(spill_manager_objects);
DECLARE_stats(spill_manager_objects_bytes);
DECLARE_stats(spill_manager_request_total);
DECLARE_stats(spill_manager_throughput_mb);

/// GCS Storage
DECLARE_stats(gcs_storage_operation_latency_ms);
DECLARE_stats(gcs_storage_operation_count);
DECLARE_stats(gcs_task_manager_task_events_dropped);
DECLARE_stats(gcs_task_manager_task_events_stored);
DECLARE_stats(gcs_task_manager_task_events_reported);

/// Object Store
DECLARE_stats(object_store_memory);
DECLARE_stats(object_store_dist);

/// Placement Group
DECLARE_stats(gcs_placement_group_creation_latency_ms);
DECLARE_stats(gcs_placement_group_scheduling_latency_ms);
DECLARE_stats(gcs_placement_group_count);

DECLARE_stats(gcs_actors_count);

/// Memory Manager
DECLARE_stats(memory_manager_worker_eviction_total);

/// Core Worker Task Manager
DECLARE_stats(total_lineage_bytes);

}  // namespace stats

}  // namespace ray
