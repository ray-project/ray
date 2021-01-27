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

/// The definitions of metrics that you can use everywhere.
///
/// There are 4 types of metric:
///   Histogram: Histogram distribution of metric points.
///   Gauge: Keeps the last recorded value, drops everything before.
///   Count: The count of the number of metric points.
///   Sum: A sum up of the metric points.
///
/// You can follow these examples to define your metrics.

///
/// Common
///
static Histogram GcsLatency("gcs_latency",
                            "The latency of a GCS (by default Redis) operation.", "us",
                            {100, 200, 300, 400, 500, 600, 700, 800, 900, 1000},
                            {CustomKey});

///
/// Raylet Metrics
///
static Gauge LocalAvailableResource("local_available_resource",
                                    "The available resources on this node.", "",
                                    {ResourceNameKey});

static Gauge LocalTotalResource("local_total_resource",
                                "The total resources on this node.", "",
                                {ResourceNameKey});

static Gauge LiveActors("live_actors", "Number of live actors.", "actors");

static Gauge RestartingActors("restarting_actors", "Number of restarting actors.",
                              "actors");

static Gauge ObjectStoreAvailableMemory(
    "object_store_available_memory",
    "Amount of memory currently available in the object store.", "bytes");

static Gauge ObjectStoreUsedMemory(
    "object_store_used_memory",
    "Amount of memory currently occupied in the object store.", "bytes");

static Gauge ObjectStoreLocalObjects("object_store_num_local_objects",
                                     "Number of objects currently in the object store.",
                                     "objects");

static Gauge ObjectManagerPullRequests("object_manager_num_pull_requests",
                                       "Number of active pull requests for objects.",
                                       "requests");

static Gauge NumInfeasibleTasks(
    "num_infeasible_tasks",
    "The number of tasks in the scheduler that are in the 'infeasible' state.", "tasks");

static Histogram HeartbeatReportMs(
    "heartbeat_report_ms",
    "Heartbeat report time in raylet. If this value is high, that means there's a high "
    "system load. It is possible that this node will be killed because of missing "
    "heartbeats.",
    "ms", {100, 200, 400, 800, 1600, 3200, 6400, 15000, 30000});

static Histogram ProcessStartupTimeMs("process_startup_time_ms",
                                      "Time to start up a worker process.", "ms",
                                      {1, 10, 100, 1000, 10000});

static Gauge AvgNumScheduledTasks(
    "avg_num_scheduled_tasks",
    "Number of tasks that are queued on this node per second. It doesn't guarantee "
    "that the task will be executed in this node. If the task is executed, it is "
    "recorded as avg_num_executed_tasks. If the task is not executed and needs to be "
    "scheduled in other nodes, it will be recorded as avg_num_spilled_back_tasks",
    "tasks");

static Gauge AvgNumExecutedTasks("avg_num_executed_tasks",
                                 "Number of executed tasks on this node per second.",
                                 "tasks");

static Gauge AvgNumSpilledBackTasks("avg_num_spilled_back_tasks",
                                    "Number of spilled back tasks per second.", "tasks");

///
/// GCS Server Metrics
///
static Count UnintentionalWorkerFailures(
    "unintentional_worker_failures_total",
    "Number of worker failures that are not intentional. For example, worker failures "
    "due to system related errors.",
    "");

static Count NodeFailureTotal(
    "node_failure_total", "Number of node failures that have happened in the cluster.",
    "");

static Gauge PendingActors("pending_actors", "Number of pending actors in GCS server.",
                           "actors");

static Gauge PendingPlacementGroups(
    "pending_placement_groups", "Number of pending placement groups in the GCS server.",
    "placement_groups");

static Histogram OutboundHeartbeatSizeKB("outbound_heartbeat_size_kb",
                                         "Outbound heartbeat payload size", "kb",
                                         {10, 50, 100, 1000, 10000, 100000});
