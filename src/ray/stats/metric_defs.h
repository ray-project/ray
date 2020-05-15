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

#ifndef RAY_STATS_METRIC_DEFS_H
#define RAY_STATS_METRIC_DEFS_H

/// The definitions of metrics that you can use everywhere.
///
/// There are 4 types of metric:
///   Histogram: Histogram distribution of metric points.
///   Gauge: Keeps the last recorded value, drops everything before.
///   Count: The count of the number of metric points.
///   Sum: A sum up of the metric points.
///
/// You can follow these examples to define your metrics.

static Gauge CurrentWorker("current_worker",
                           "This metric is used for reporting states of workers."
                           "Through this, we can see the worker's state on dashboard.",
                           "1 pcs", {LanguageKey, WorkerPidKey});

static Gauge CurrentDriver("current_driver",
                           "This metric is used for reporting states of drivers.",
                           "1 pcs", {LanguageKey, DriverPidKey});

static Count TaskCountReceived("task_count_received",
                               "Number of tasks received by raylet.", "pcs", {});

static Histogram RedisLatency("redis_latency", "The latency of a Redis operation.", "us",
                              {100, 200, 300, 400, 500, 600, 700, 800, 900, 1000},
                              {CustomKey});

static Gauge LocalAvailableResource("local_available_resource",
                                    "The available resources on this node.", "pcs",
                                    {ResourceNameKey});

static Gauge LocalTotalResource("local_total_resource",
                                "The total resources on this node.", "pcs",
                                {ResourceNameKey});

static Gauge ActorStats("actor_stats", "Stat metrics of the actors in raylet.", "pcs",
                        {ValueTypeKey});

static Gauge ObjectManagerStats("object_manager_stats",
                                "Stat the metric values of object in raylet", "pcs",
                                {ValueTypeKey});

static Gauge LineageCacheStats("lineage_cache_stats",
                               "Stats the metric values of lineage cache.", "pcs",
                               {ValueTypeKey});

static Gauge TaskDependencyManagerStats("task_dependency_manager_stats",
                                        "Stat the metric values of task dependency.",
                                        "pcs", {ValueTypeKey});

static Gauge SchedulingQueueStats("scheduling_queue_stats",
                                  "Stats the metric values of scheduling queue.", "pcs",
                                  {ValueTypeKey});

static Gauge ReconstructionPolicyStats(
    "reconstruction_policy_stats", "Stats the metric values of reconstruction policy.",
    "pcs", {ValueTypeKey});

static Gauge ConnectionPoolStats("connection_pool_stats",
                                 "Stats the connection pool metrics.", "pcs",
                                 {ValueTypeKey});

#endif  // RAY_STATS_METRIC_DEFS_H
