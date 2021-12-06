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

#include "ray/stats/metric_defs.h"

// STATS_DEPAREN will remove () for it's parameter
// For example
//   STATS_DEPAREN((a, b, c))
// will result
//   a, b, c
#define STATS_DEPAREN(X) STATS_ESC(STATS_ISH X)
#define STATS_ISH(...) ISH __VA_ARGS__
#define STATS_ESC(...) STATS_ESC_(__VA_ARGS__)
#define STATS_ESC_(...) STATS_VAN##__VA_ARGS__
#define STATS_VANISH

/*
  Syntax suguar to define a metrics:
      DEFINE_stats(name,
        desctiption,
        (tag1, tag2, ...),
        (bucket1, bucket2, ...),
        type1,
        type2)
  Later, it can be used by STATS_name.record(val, tags).

  Some examples:
      DEFINE_stats(
          async_pool_req_execution_time_ms,
          "Async pool execution time",
          ("Method"),
          (), ray::stats::GAUGE);
      STATS_async_pool_req_execution_time_ms.record(1, "method");
*/
#define DEFINE_stats(name, description, tags, buckets, ...)                \
  ray::stats::internal::Stats STATS_##name(                                \
      #name, description, {STATS_DEPAREN(tags)}, {STATS_DEPAREN(buckets)}, \
      ray::stats::internal::RegisterViewWithTagList<__VA_ARGS__>)

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

/// Event stats
DEFINE_stats(operation_count, "operation count", ("Method"), (), ray::stats::GAUGE);
DEFINE_stats(operation_run_time_ms, "operation execution time", ("Method"), (),
             ray::stats::GAUGE);
DEFINE_stats(operation_queue_time_ms, "operation queuing time", ("Method"), (),
             ray::stats::GAUGE);
DEFINE_stats(operation_active_count, "activate operation number", ("Method"), (),
             ray::stats::GAUGE);

/// GRPC server
DEFINE_stats(grpc_server_req_process_time_ms, "Request latency in grpc server",
             ("Method"), (), ray::stats::GAUGE);
DEFINE_stats(grpc_server_req_new, "New request number in grpc server", ("Method"), (),
             ray::stats::COUNT);
DEFINE_stats(grpc_server_req_handling, "Request number are handling in grpc server",
             ("Method"), (), ray::stats::COUNT);
DEFINE_stats(grpc_server_req_finished, "Finished request number in grpc server",
             ("Method"), (), ray::stats::COUNT);

/// Object Manager.
DEFINE_stats(num_chunks_received, "Number object chunks received.", ("Type"), (),
             ray::stats::GAUGE);

/// Pull Manager
DEFINE_stats(num_bytes_available, "The total number of bytes available to pull objects.",
             (), (), ray::stats::GAUGE);
DEFINE_stats(num_bytes_being_pulled,
             "The total number of bytes we are currently pulling.", (), (),
             ray::stats::GAUGE);
DEFINE_stats(num_bytes_being_pulled_pinned,
             "The total number of bytes for pinned objects.", (), (), ray::stats::GAUGE);
DEFINE_stats(num_get_reqs, "Number of queued get requests.", (), (), ray::stats::GAUGE);
DEFINE_stats(num_wait_reqs, "Number of queued wait requests.", (), (), ray::stats::GAUGE);
DEFINE_stats(num_task_arg_reqs, "Number of queued task argument requests.", (), (),
             ray::stats::GAUGE);
DEFINE_stats(num_pull_req_queued, "Number of queued pull requests for objects.", (), (),
             ray::stats::GAUGE);
DEFINE_stats(num_active_pulls, "Number of active pull requests.", (), (),
             ray::stats::GAUGE);
DEFINE_stats(num_active_pulls_pinned, "Number of pinned objects by active pull requests.",
             (), (), ray::stats::GAUGE);
DEFINE_stats(num_active_bundles, "Number of active bundle being pulled.", (), (),
             ray::stats::GAUGE);
DEFINE_stats(num_retries_total, "Total number of pull retry in this node.", (), (),
             ray::stats::GAUGE);

/// Push Manager
DEFINE_stats(num_pushes_in_flight, "Number of object push requests in flight.", (), (),
             ray::stats::GAUGE);
DEFINE_stats(num_chunks_in_flight, "Number of object chunks transfer in flight.", (), (),
             ray::stats::GAUGE);
DEFINE_stats(num_chunks_remainig, "Number of object chunks transfer remaining.", (), (),
             ray::stats::GAUGE);

/// Scheduler
DEFINE_stats(num_waiting_for_resource,
             "Number of tasks waiting for resources to be available.", (), (),
             ray::stats::GAUGE);
DEFINE_stats(num_waiting_for_plasma_memory,
             "Number of tasks waiting for additional object store memory.", (), (),
             ray::stats::GAUGE);
DEFINE_stats(num_waiting_for_remote_node_resources,
             "Number of tasks waiting for available resources from remote nodes.", (), (),
             ray::stats::GAUGE);
DEFINE_stats(num_worker_not_started_by_job_config_not_exist,
             "Number of tasks that have failed to be scheduled because workers were not "
             "available due to missing job config.",
             (), (), ray::stats::GAUGE);
DEFINE_stats(num_worker_not_started_by_registration_timeout,
             "Number of tasks that have failed to be scheduled because workers were not "
             "available due to worker registration timeout.",
             (), (), ray::stats::GAUGE);
DEFINE_stats(num_worker_not_started_by_process_rate_limit,
             "Number of tasks that have failed to be scheduled because workers were not "
             "available due to rate limiting.",
             (), (), ray::stats::GAUGE);
DEFINE_stats(num_tasks_waiting_for_workers,
             "Number of tasks that are waiting for a worker process to be available.", (),
             (), ray::stats::GAUGE);
DEFINE_stats(num_cancelled_tasks, "Number of cancelled tasks.", (), (),
             ray::stats::GAUGE);
DEFINE_stats(num_waitng_tasks,
             "Number of tasks that are waiting for dependencies to be available locally.",
             (), (), ray::stats::GAUGE);
DEFINE_stats(num_executing_tasks, "Number of tasks that are executing.", (), (),
             ray::stats::GAUGE);
DEFINE_stats(num_pinned_task_args,
             "Number of task arguments that are pinned because they are used by tasks.",
             (), (), ray::stats::GAUGE);

/// Local Object Manager
DEFINE_stats(num_pinned_objects, "Number of locally pinned objects.", (), (),
             ray::stats::GAUGE);
DEFINE_stats(pinned_objects_size_bytes, "Mumber of pinned object size in bytes.", (), (),
             ray::stats::GAUGE);
DEFINE_stats(num_objects_pending_restore, "Number of pending restore requests.", (), (),
             ray::stats::GAUGE);
DEFINE_stats(num_objects_pending_spill, "Number of pending spill requests.", (), (),
             ray::stats::GAUGE);
DEFINE_stats(pending_spill_bytes, "Pending spill request in bytes.", (), (),
             ray::stats::GAUGE);
DEFINE_stats(cumulative_spill_requests, "Cumulative number of spill requests.", (), (),
             ray::stats::GAUGE);
DEFINE_stats(cumulative_restore_requests, "Cumulative number of restore requests.", (),
             (), ray::stats::GAUGE);

///
/// Plasma Store Metrics
///

/// Object Lifecycle Manager.
DEFINE_stats(plasma_num_local_objects_by_state,
             "The number of objects per state. E.g., Spillable, In use, Evictable.",
             ("State"), (), ray::stats::GAUGE);
DEFINE_stats(plasma_num_local_bytes_by_state, "The number of objects per state in bytes.",
             ("State"), (), ray::stats::GAUGE);
DEFINE_stats(
    plasma_num_local_objects_by_type,
    "The number of objects per type. E.g., Primary copy, Transferred, ErrorObject.",
    ("CreationType"), (), ray::stats::GAUGE);
DEFINE_stats(plasma_num_local_bytes_by_type, "The number of objects per type in bytes.",
             ("CreationType"), (), ray::stats::GAUGE);

/// Plasma Store
DEFINE_stats(num_pending_creation_requests,
             "The number of pending object creation requests in the queue.", (), (),
             ray::stats::GAUGE);
DEFINE_stats(num_pending_creation_bytes,
             "The number of pending object creation requests in bytes.", (), (),
             ray::stats::GAUGE);

/// GCS Resource Manager
DEFINE_stats(new_resource_creation_latency_ms,
             "Time to persist newly created resources to Redis.", (),
             ({0.1, 1, 10, 100, 1000, 10000}, ), ray::stats::HISTOGRAM);

/// Placement Group
// The end to end placement group creation latency.
// The time from placement group creation request has received
// <-> Placement group creation succeeds (meaning all resources
// are committed to nodes and available).
DEFINE_stats(placement_group_creation_latency_ms,
             "end to end latency of placement group creation", (),
             ({0.1, 1, 10, 100, 1000, 10000}, ), ray::stats::HISTOGRAM);
// The time from placement group scheduling has started
// <-> Placement group creation succeeds.
DEFINE_stats(placement_group_scheduling_latency_ms,
             "scheduling latency of placement groups", (),
             ({0.1, 1, 10, 100, 1000, 10000}, ), ray::stats::HISTOGRAM);
DEFINE_stats(pending_placement_group, "Number of total pending placement groups", (), (),
             ray::stats::GAUGE);
DEFINE_stats(registered_placement_group, "Number of total registered placement groups",
             (), (), ray::stats::GAUGE);
DEFINE_stats(infeasible_placement_group, "Number of total infeasible placement groups",
             (), (), ray::stats::GAUGE);

}  // namespace stats

}  // namespace ray
