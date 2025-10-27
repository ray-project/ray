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

#include "ray/stats/tag_defs.h"
#include "ray/util/size_literals.h"

using namespace ray::literals;

namespace ray::stats {

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
    ("Location", "ObjectState"),
    (),
    ray::stats::GAUGE);

/// ===============================================================================
/// ===================== INTERNAL SYSTEM METRICS =================================
/// ===============================================================================

DEFINE_stats(io_context_event_loop_lag_ms,
             "Latency of a task from post to execution",
             ("Name"),  // Name of the instrumented_io_context.
             (),
             ray::stats::GAUGE);

/// Event stats
DEFINE_stats(operation_count, "operation count", ("Name"), (), ray::stats::COUNT);
DEFINE_stats(operation_run_time_ms,
             "operation execution time",
             ("Name"),
             ({1, 10, 100, 1000, 10000}),
             ray::stats::HISTOGRAM);
DEFINE_stats(operation_queue_time_ms,
             "operation queuing time",
             ("Name"),
             ({1, 10, 100, 1000, 10000}),
             ray::stats::HISTOGRAM);
DEFINE_stats(
    operation_active_count, "active operation number", ("Name"), (), ray::stats::GAUGE);

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

/// Memory Manager
DEFINE_stats(
    memory_manager_worker_eviction_total,
    "Total worker eviction events broken per work type {Actor, Task, Driver} and name.",
    ("Type", "Name"),
    (),
    ray::stats::COUNT);

/// Core Worker Task Manager
DEFINE_stats(
    total_lineage_bytes,
    "Total amount of memory used to store task specs for lineage reconstruction.",
    (),
    (),
    ray::stats::GAUGE);

}  // namespace ray::stats
