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
/// There are 4 types of metric:
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

/// Placement group stats, broken down by state.
DECLARE_stats(placement_groups);

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
DECLARE_stats(push_manager_in_flight_pushes);
DECLARE_stats(push_manager_chunks);

/// Scheduler
DECLARE_stats(scheduler_failed_worker_startup_total);
DECLARE_stats(scheduler_tasks);
DECLARE_stats(scheduler_unscheduleable_tasks);

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
DECLARE_stats(gcs_task_manager_task_events_stored_bytes);
DECLARE_stats(gcs_task_manager_task_events_reported);

/// Object Store
DECLARE_stats(object_store_memory);

/// Placement Group
DECLARE_stats(gcs_placement_group_creation_latency_ms);
DECLARE_stats(gcs_placement_group_scheduling_latency_ms);
DECLARE_stats(gcs_placement_group_count);

DECLARE_stats(gcs_actors_count);

/// Memory Manager
DECLARE_stats(memory_manager_worker_eviction_total);

/// The below items are legacy implementation of metrics.
/// TODO(sang): Use DEFINE_stats instead.

///
/// Common
///
/// RPC
static Histogram GcsLatency("gcs_latency",
                            "The latency of a GCS (by default Redis) operation.",
                            "us",
                            {100, 200, 300, 400, 500, 600, 700, 800, 900, 1000},
                            {CustomKey});

///
/// Raylet Metrics
///

/// Raylet Resource Manager
static Gauge TestMetrics("local_available_resource",
                         "The available resources on this node.",
                         "",
                         {ResourceNameKey});

static Gauge LocalTotalResource("local_total_resource",
                                "The total resources on this node.",
                                "",
                                {ResourceNameKey});

/// Object Manager.
static Gauge ObjectStoreAvailableMemory(
    "object_store_available_memory",
    "Amount of memory currently available in the object store.",
    "bytes");

static Gauge ObjectStoreUsedMemory(
    "object_store_used_memory",
    "Amount of memory currently occupied in the object store.",
    "bytes");

static Gauge ObjectStoreFallbackMemory(
    "object_store_fallback_memory",
    "Amount of memory in fallback allocations in the filesystem.",
    "bytes");

static Gauge ObjectStoreLocalObjects("object_store_num_local_objects",
                                     "Number of objects currently in the object store.",
                                     "objects");

static Gauge ObjectManagerPullRequests("object_manager_num_pull_requests",
                                       "Number of active pull requests for objects.",
                                       "requests");

/// Object Directory.
static Gauge ObjectDirectoryLocationSubscriptions(
    "object_directory_subscriptions",
    "Number of object location subscriptions. If this is high, the raylet is attempting "
    "to pull a lot of objects.",
    "subscriptions");

static Gauge ObjectDirectoryLocationUpdates(
    "object_directory_updates",
    "Number of object location updates per second., If this is high, the raylet is "
    "attempting to pull a lot of objects and/or the locations for objects are frequently "
    "changing (e.g. due to many object copies or evictions).",
    "updates");

static Gauge ObjectDirectoryLocationLookups(
    "object_directory_lookups",
    "Number of object location lookups per second. If this is high, the raylet is "
    "waiting on a lot of objects.",
    "lookups");

static Gauge ObjectDirectoryAddedLocations(
    "object_directory_added_locations",
    "Number of object locations added per second., If this is high, a lot of objects "
    "have been added on this node.",
    "additions");

static Gauge ObjectDirectoryRemovedLocations(
    "object_directory_removed_locations",
    "Number of object locations removed per second. If this is high, a lot of objects "
    "have been removed from this node.",
    "removals");

/// Worker Pool
static Histogram ProcessStartupTimeMs("process_startup_time_ms",
                                      "Time to start up a worker process.",
                                      "ms",
                                      {1, 10, 100, 1000, 10000});

static Sum NumWorkersStarted(
    "internal_num_processes_started",
    "The total number of worker processes the worker pool has created.",
    "processes");

static Gauge NumSpilledTasks("internal_num_spilled_tasks",
                             "The cumulative number of lease requeusts that this raylet "
                             "has spilled to other raylets.",
                             "tasks");

static Gauge NumInfeasibleSchedulingClasses(
    "internal_num_infeasible_scheduling_classes",
    "The number of unique scheduling classes that are infeasible.",
    "tasks");

///
/// GCS Server Metrics
///

/// Workers
static Count UnintentionalWorkerFailures(
    "unintentional_worker_failures_total",
    "Number of worker failures that are not intentional. For example, worker failures "
    "due to system related errors.",
    "");

/// Nodes
static Count NodeFailureTotal(
    "node_failure_total",
    "Number of node failures that have happened in the cluster.",
    "");

/// Resources
static Histogram OutboundHeartbeatSizeKB("outbound_heartbeat_size_kb",
                                         "Outbound heartbeat payload size",
                                         "kb",
                                         {10, 50, 100, 1000, 10000, 100000});

static Histogram GcsUpdateResourceUsageTime(
    "gcs_update_resource_usage_time",
    "The average RTT of a UpdateResourceUsage RPC.",
    "ms",
    {1, 2, 5, 10, 20, 50, 100, 200, 500, 1000, 2000},
    {CustomKey});

/// Testing
static Gauge LiveActors("live_actors", "Number of live actors.", "actors");
static Gauge RestartingActors("restarting_actors",
                              "Number of restarting actors.",
                              "actors");

}  // namespace stats

}  // namespace ray
