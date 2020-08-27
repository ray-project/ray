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
static Histogram RedisLatency("redis_latency", "The latency of a Redis operation.", "us",
                              {100, 200, 300, 400, 500, 600, 700, 800, 900, 1000},
                              {CustomKey});

///
/// Raylet Metrics
///
static Gauge CurrentWorker("current_worker",
                           "This metric is used for reporting states of workers."
                           "Through this, we can see the worker's state on dashboard.",
                           "1 pcs", {LanguageKey, WorkerPidKey});

static Gauge CurrentDriver("current_driver",
                           "This metric is used for reporting states of drivers.",
                           "1 pcs", {LanguageKey, DriverPidKey});

static Count TaskCountReceived("task_count_received",
                               "Number of tasks received by raylet.", "pcs", {});

static Gauge LocalAvailableResource("local_available_resource",
                                    "The available resources on this node.", "pcs",
                                    {ResourceNameKey});

static Gauge LocalTotalResource("local_total_resource",
                                "The total resources on this node.", "pcs",
                                {ResourceNameKey});

static Gauge LiveActors("live_actors", "Number of live actors.", "actors");

static Gauge RestartingActors("restarting_actors", "Number of restarting actors.",
                              "actors");

static Gauge DeadActors("dead_actors", "Number of dead actors.", "actors");

static Gauge ObjectStoreAvailableMemory(
    "object_store_available_memory",
    "Amount of memory currently available in the object store.", "bytes");

static Gauge ObjectStoreUsedMemory(
    "object_store_used_memory",
    "Amount of memory currently occupied in the object store.", "bytes");

static Gauge ObjectStoreLocalObjects("object_store_num_local_objects",
                                     "Number of objects currently in the object store.",
                                     "objects");

static Gauge ObjectManagerWaitRequests("object_manager_num_wait_requests",
                                       "Number of pending wait requests for objects.",
                                       "requests");

static Gauge ObjectManagerPullRequests("object_manager_num_pull_requests",
                                       "Number of active pull requests for objects.",
                                       "requests");

static Gauge ObjectManagerUnfulfilledPushRequests(
    "object_manager_unfulfilled_push_requests",
    "Number of unfulfilled push requests for objects.", "requests");

static Gauge ObjectManagerProfileEvents("object_manager_num_buffered_profile_events",
                                        "Number of locally-buffered profile events.",
                                        "events");

static Gauge NumSubscribedTasks(
    "num_subscribed_tasks",
    "The number of tasks that are subscribed to object dependencies.", "tasks");

static Gauge NumRequiredTasks("num_required_tasks",
                              "The number of tasks whose output object(s) are "
                              "required by another subscribed task.",
                              "tasks");

static Gauge NumRequiredObjects(
    "num_required_objects",
    "The number of objects that are required by a subscribed task.", "objects");

static Gauge NumPendingTasks("num_pending_tasks",
                             "The number of tasks that are pending execution.", "tasks");

static Gauge NumPlaceableTasks(
    "num_placeable_tasks",
    "The number of tasks in the scheduler that are in the 'placeable' state.", "tasks");

static Gauge NumWaitingTasks(
    "num_waiting_tasks",
    "The number of tasks in the scheduler that are in the 'waiting' state.", "tasks");

static Gauge NumReadyTasks(
    "num_ready_tasks",
    "The number of tasks in the scheduler that are in the 'ready' state.", "tasks");

static Gauge NumRunningTasks(
    "num_running_tasks",
    "The number of tasks in the scheduler that are in the 'running' state.", "tasks");

static Gauge NumInfeasibleTasks(
    "num_infeasible_tasks",
    "The number of tasks in the scheduler that are in the 'infeasible' state.", "tasks");
