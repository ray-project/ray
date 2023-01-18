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

/// ===============================================================================
/// =========== PUBLIC METRICS; keep in sync with ray-metrics.rst =================
/// ===============================================================================

/// Tracks tasks by state, including pending, running, and finished tasks.
/// This metric may be recorded from multiple components processing the task in Ray,
/// including the submitting core worker, executor core worker, and pull manager.
///
/// To avoid metric collection conflicts between components reporting on the same task,
/// we use the "Source" required label.
DEFINE_stats(
    tasks,
    "Current number of tasks currently in a particular state.",
    // State: the task state, as described by rpc::TaskState proto in common.proto.
    // Name: the name of the function called.
    // Source: component reporting, e.g., "core_worker", "executor", or "pull_manager".
    // IsRetry: whether this task is a retry.
    ("State", "Name", "Source", "IsRetry", "JobId"),
    (),
    ray::stats::GAUGE);

/// Tracks actors by state, including pending, running, and idle actors.
///
/// To avoid metric collection conflicts between components reporting on the same task,
/// we use the "Source" required label.
DEFINE_stats(actors,
             "Current number of actors currently in a particular state.",
             // State: the actor state, which is from rpc::ActorTableData::ActorState,
             // but can also be RUNNING_TASK, RUNNING_IN_RAY_GET, and RUNNING_IN_RAY_WAIT.
             // Name: the name of actor class.
             // Source: component reporting, e.g., "gcs" or "executor".
             ("State", "Name", "Source"),
             (),
             ray::stats::GAUGE);

/// Logical resource usage reported by raylets.
DEFINE_stats(resources,
             // TODO(sang): Support placement_group_reserved_available | used
             "Logical Ray resources broken per state {AVAILABLE, USED}",
             ("Name", "State"),
             (),
             ray::stats::GAUGE);

/// Object store memory usage.
DEFINE_stats(
    object_store_memory,
    "Object store memory by various sub-kinds on this node",
    /// Location:
    ///    - MMAP_SHM: currently in shared memory(e.g. /dev/shm).
    ///    - MMAP_DISK: memory that's fallback allocated on mmapped disk,
    ///      e.g. /tmp.
    ///    - WORKER_HEAP: ray objects smaller than ('max_direct_call_object_size',
    ///      default 100KiB) stored in process memory, i.e. inlined return
    ///      values, placeholders for objects stored in plasma store.
    ///    - SPILLED: current number of bytes from objects spilled
    ///      to external storage. Note this might be smaller than
    ///      the physical storage incurred on the external storage because
    ///      Ray might fuse spilled objects into a single file, so a deleted
    ///      spill object might still exist in the spilled file. Check
    ///      spilled object fusing for more details.
    /// ObjectState:
    ///    - SEALED: sealed objects bytes (could be MMAP_SHM or MMAP_DISK)
    ///    - UNSEALED: unsealed objects bytes (could be MMAP_SHM or MMAP_DISK)
    (ray::stats::LocationKey.name(), ray::stats::ObjectStateKey.name()),
    (),
    ray::stats::GAUGE);

/// Placement group metrics from the GCS.
DEFINE_stats(placement_groups,
             "Number of placement groups broken down by state.",
             // State: from rpc::PlacementGroupData::PlacementGroupState.
             ("State"),
             (),
             ray::stats::GAUGE);

/// ===============================================================================
/// ===================== INTERNAL SYSTEM METRICS =================================
/// ===============================================================================

/// Event stats
DEFINE_stats(operation_count, "operation count", ("Method"), (), ray::stats::GAUGE);
DEFINE_stats(
    operation_run_time_ms, "operation execution time", ("Method"), (), ray::stats::GAUGE);
DEFINE_stats(
    operation_queue_time_ms, "operation queuing time", ("Method"), (), ray::stats::GAUGE);
DEFINE_stats(operation_active_count,
             "activate operation number",
             ("Method"),
             (),
             ray::stats::GAUGE);

/// GRPC server
DEFINE_stats(grpc_server_req_process_time_ms,
             "Request latency in grpc server",
             ("Method"),
             (),
             ray::stats::GAUGE);
DEFINE_stats(grpc_server_req_new,
             "New request number in grpc server",
             ("Method"),
             (),
             ray::stats::COUNT);
DEFINE_stats(grpc_server_req_handling,
             "Request number are handling in grpc server",
             ("Method"),
             (),
             ray::stats::COUNT);
DEFINE_stats(grpc_server_req_finished,
             "Finished request number in grpc server",
             ("Method"),
             (),
             ray::stats::COUNT);

/// Object Manager.
DEFINE_stats(object_manager_bytes,
             "Number of bytes pushed or received by type {PushedFromLocalPlasma, "
             "PushedFromLocalDisk, Received}.",
             ("Type"),
             (),
             ray::stats::GAUGE);

DEFINE_stats(object_manager_received_chunks,
             "Number object chunks received broken per type {Total, FailedTotal, "
             "FailedCancelled, FailedPlasmaFull}.",
             ("Type"),
             (),
             ray::stats::GAUGE);

/// Pull Manager
DEFINE_stats(
    pull_manager_usage_bytes,
    "The total number of bytes usage broken per type {Available, BeingPulled, Pinned}",
    ("Type"),
    (),
    ray::stats::GAUGE);
DEFINE_stats(pull_manager_requested_bundles,
             "Number of requested bundles broken per type {Get, Wait, TaskArgs}.",
             ("Type"),
             (),
             ray::stats::GAUGE);
DEFINE_stats(pull_manager_requests,
             "Number of pull requests broken per type {Queued, Active, Pinned}.",
             ("Type"),
             (),
             ray::stats::GAUGE);
DEFINE_stats(pull_manager_active_bundles,
             "Number of active bundle requests",
             (),
             (),
             ray::stats::GAUGE);
DEFINE_stats(pull_manager_retries_total,
             "Number of cumulative pull retries.",
             (),
             (),
             ray::stats::GAUGE);
DEFINE_stats(
    pull_manager_num_object_pins,
    "Number of object pin attempts by the pull manager, can be {Success, Failure}.",
    ("Type"),
    (),
    ray::stats::GAUGE);
DEFINE_stats(pull_manager_object_request_time_ms,
             "Time between initial object pull request and local pinning of the object. ",
             ("Type"),
             ({1, 10, 100, 1000, 10000}),
             ray::stats::HISTOGRAM);

/// Push Manager
DEFINE_stats(push_manager_in_flight_pushes,
             "Number of in flight object push requests.",
             (),
             (),
             ray::stats::GAUGE);
DEFINE_stats(push_manager_chunks,
             "Number of object chunks transfer broken per type {InFlight, Remaining}.",
             ("Type"),
             (),
             ray::stats::GAUGE);

/// Scheduler
DEFINE_stats(
    scheduler_tasks,
    "Number of tasks waiting for scheduling broken per state {Cancelled, Executing, "
    "Waiting, Dispatched, Received}.",
    ("State"),
    (),
    ray::stats::GAUGE);
DEFINE_stats(scheduler_unscheduleable_tasks,
             "Number of pending tasks (not scheduleable tasks) broken per reason "
             "{Infeasible, WaitingForResources, "
             "WaitingForPlasmaMemory, WaitingForRemoteResources, WaitingForWorkers}.",
             ("Reason"),
             (),
             ray::stats::GAUGE);
DEFINE_stats(scheduler_failed_worker_startup_total,
             "Number of tasks that fail to be scheduled because workers were not "
             "available. Labels are broken per reason {JobConfigMissing, "
             "RegistrationTimedOut, RateLimited}",
             ("Reason"),
             (),
             ray::stats::GAUGE);

/// Local Object Manager
DEFINE_stats(
    spill_manager_objects,
    "Number of local objects broken per state {Pinned, PendingRestore, PendingSpill}.",
    ("State"),
    (),
    ray::stats::GAUGE);
DEFINE_stats(spill_manager_objects_bytes,
             "Byte size of local objects broken per state {Pinned, PendingSpill}.",
             ("State"),
             (),
             ray::stats::GAUGE);
DEFINE_stats(spill_manager_request_total,
             "Number of {spill, restore} requests.",
             ("Type"),
             (),
             ray::stats::GAUGE);
DEFINE_stats(spill_manager_throughput_mb,
             "The throughput of {spill, restore} requests in MB.",
             ("Type"),
             (),
             ray::stats::GAUGE);

/// GCS Storage
DEFINE_stats(gcs_storage_operation_latency_ms,
             "Time to invoke an operation on Gcs storage",
             ("Operation"),
             ({0.1, 1, 10, 100, 1000, 10000}, ),
             ray::stats::HISTOGRAM);
DEFINE_stats(gcs_storage_operation_count,
             "Number of operations invoked on Gcs storage",
             ("Operation"),
             (),
             ray::stats::COUNT);

/// Placement Group
// The end to end placement group creation latency.
// The time from placement group creation request has received
// <-> Placement group creation succeeds (meaning all resources
// are committed to nodes and available).
DEFINE_stats(gcs_placement_group_creation_latency_ms,
             "end to end latency of placement group creation",
             (),
             ({0.1, 1, 10, 100, 1000, 10000}, ),
             ray::stats::HISTOGRAM);
// The time from placement group scheduling has started
// <-> Placement group creation succeeds.
DEFINE_stats(gcs_placement_group_scheduling_latency_ms,
             "scheduling latency of placement groups",
             (),
             ({0.1, 1, 10, 100, 1000, 10000}, ),
             ray::stats::HISTOGRAM);
DEFINE_stats(gcs_placement_group_count,
             "Number of placement groups broken down by state in {Registered, Pending, "
             "Infeasible}",
             ("State"),
             (),
             ray::stats::GAUGE);

/// GCS Actor Manager
DEFINE_stats(gcs_actors_count,
             "Number of actors per state {Created, Destroyed, Unresolved, Pending}",
             ("State"),
             (),
             ray::stats::GAUGE);

/// GCS Task Manager
DEFINE_stats(gcs_task_manager_task_events_reported,
             "Number of all task events reported to gcs.",
             (),
             (),
             ray::stats::GAUGE);

DEFINE_stats(gcs_task_manager_task_events_dropped,
             /// Type:
             ///     - PROFILE_EVENT: number of profile task events dropped from both
             ///     workers and GCS.
             ///     - STATUS_EVENT: number of task status updates events dropped from
             ///     both workers and GCS.
             "Number of task events dropped per type {PROFILE_EVENT, STATUS_EVENT}",
             ("Type"),
             (),
             ray::stats::GAUGE);

DEFINE_stats(gcs_task_manager_task_events_stored,
             "Number of task events stored in GCS.",
             (),
             (),
             ray::stats::GAUGE);

DEFINE_stats(gcs_task_manager_task_events_stored_bytes,
             "Number of bytes of all task events stored in GCS.",
             (),
             (),
             ray::stats::GAUGE);

/// Memory Manager
DEFINE_stats(memory_manager_worker_eviction_total,
             "Total worker eviction events broken per work type {Actor, Task}",
             ("Type"),
             (),
             ray::stats::COUNT);
}  // namespace stats

}  // namespace ray
