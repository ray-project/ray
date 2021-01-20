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

// This header file is used to avoid code duplication.
// It can be included multiple times in ray_config.h, and each inclusion
// could use a different definition of the RAY_CONFIG macro.
// Macro definition format: RAY_CONFIG(type, name, default_value).
// NOTE: This file should NOT be included in any file other than ray_config.h.

// IF YOU MODIFY THIS FILE and add a configuration parameter, you must change
// at least two additional things:
//     1. You must update the file "ray/python/ray/includes/ray_config.pxd".
//     2. You must update the file "ray/python/ray/includes/ray_config.pxi".

/// In theory, this is used to detect Ray cookie mismatches.
/// This magic number (hex for "RAY") is used instead of zero, rationale is
/// that it could still be possible that some random program sends an int64_t
/// which is zero, but it's much less likely that a program sends this
/// particular magic number.
RAY_CONFIG(int64_t, ray_cookie, 0x5241590000000000)

/// The duration that a single handler on the event loop can take before a
/// warning is logged that the handler is taking too long.
RAY_CONFIG(int64_t, handler_warning_timeout_ms, 1000)

/// The duration between heartbeats sent by the raylets.
RAY_CONFIG(int64_t, raylet_heartbeat_timeout_milliseconds, 100)
/// If a component has not sent a heartbeat in the last num_heartbeats_timeout
/// heartbeat intervals, the raylet monitor process will report
/// it as dead to the db_client table.
RAY_CONFIG(int64_t, num_heartbeats_timeout, 300)
/// For a raylet, if the last heartbeat was sent more than this many
/// heartbeat periods ago, then a warning will be logged that the heartbeat
/// handler is drifting.
RAY_CONFIG(uint64_t, num_heartbeats_warning, 5)

/// The duration between reporting resources sent by the raylets.
RAY_CONFIG(int64_t, raylet_report_resources_period_milliseconds, 100)

/// The duration between dumping debug info to logs, or -1 to disable.
RAY_CONFIG(int64_t, debug_dump_period_milliseconds, 10000)

/// Whether to enable fair queueing between task classes in raylet. When
/// fair queueing is enabled, the raylet will try to balance the number
/// of running tasks by class (i.e., function name). This prevents one
/// type of task from starving other types (see issue #3664).
RAY_CONFIG(bool, fair_queueing_enabled, true)

/// Whether to enable object pinning for plasma objects. When this is
/// enabled, objects in scope in the cluster will not be LRU evicted.
RAY_CONFIG(bool, object_pinning_enabled, true)

/// Whether to enable distributed reference counting for objects. When this is
/// enabled, an object's ref count will include any references held by other
/// processes, such as when an ObjectID is serialized and passed as an argument
/// to another task. It will also include any references due to nesting, i.e.
/// if the object ID is nested inside another object that is still in scope.
/// When this is disabled, an object's ref count will include only local
/// information:
///  1. Local Python references to the ObjectID.
///  2. Pending tasks submitted by the local process that depend on the object.
/// If both this flag and object_pinning_enabled are turned on, then an object
/// will not be LRU evicted until it is out of scope in ALL processes in the
/// cluster and all objects that contain it are also out of scope. If this flag
/// is off and object_pinning_enabled is turned on, then an object will not be
/// LRU evicted until it is out of scope on the CREATOR of the ObjectID.
RAY_CONFIG(bool, distributed_ref_counting_enabled, true)

/// Whether to record the creation sites of object references. This adds more
/// information to `ray memstat`, but introduces a little extra overhead when
/// creating object references.
RAY_CONFIG(bool, record_ref_creation_sites, true)

/// If object_pinning_enabled is on, then objects that have been unpinned are
/// added to a local cache. When the cache is flushed, all objects in the cache
/// will be eagerly evicted in a batch by freeing all plasma copies in the
/// cluster. If set, then this is the duration between attempts to flush the
/// local cache. If this is set to 0, then the objects will be freed as soon as
/// they enter the cache. To disable eager eviction, set this to -1.
/// NOTE(swang): If distributed_ref_counting_enabled is off, then this will
/// likely cause spurious object lost errors for Object IDs that were
/// serialized, then either passed as an argument or returned from a task.
/// NOTE(swang): The timer is checked by the raylet during every heartbeat, so
/// this should be set to a value larger than
/// raylet_heartbeat_timeout_milliseconds.
RAY_CONFIG(int64_t, free_objects_period_milliseconds, 1000)

/// If object_pinning_enabled is on, then objects that have been unpinned are
/// added to a local cache. When the cache is flushed, all objects in the cache
/// will be eagerly evicted in a batch by freeing all plasma copies in the
/// cluster. This is the maximum number of objects in the local cache before it
/// is flushed. To disable eager eviction, set free_objects_period_milliseconds
/// to -1.
RAY_CONFIG(size_t, free_objects_batch_size, 100)

RAY_CONFIG(bool, lineage_pinning_enabled, false)

/// Whether to enable the new scheduler. The new scheduler is designed
/// only to work with direct calls. Once direct calls are becoming
/// the default, this scheduler will also become the default.
RAY_CONFIG(bool, new_scheduler_enabled,
           getenv("RAY_ENABLE_NEW_SCHEDULER") == nullptr ||
               getenv("RAY_ENABLE_NEW_SCHEDULER") == std::string("1"))

// The max allowed size in bytes of a return object from direct actor calls.
// Objects larger than this size will be spilled/promoted to plasma.
RAY_CONFIG(int64_t, max_direct_call_object_size, 100 * 1024)

// The max gRPC message size (the gRPC internal default is 4MB). We use a higher
// limit in Ray to avoid crashing with many small inlined task arguments.
RAY_CONFIG(int64_t, max_grpc_message_size, 100 * 1024 * 1024)

// Number of times to retry creating a gRPC server.
RAY_CONFIG(int64_t, grpc_server_num_retries, 1)

// Retry timeout for trying to create a gRPC server. Only applies if the number
// of retries is non zero.
RAY_CONFIG(int64_t, grpc_server_retry_timeout_milliseconds, 1000)

// The min number of retries for direct actor creation tasks. The actual number
// of creation retries will be MAX(actor_creation_min_retries, max_restarts).
RAY_CONFIG(uint64_t, actor_creation_min_retries, 3)

/// When trying to resolve an object, the initial period that the raylet will
/// wait before contacting the object's owner to check if the object is still
/// available. This is a lower bound on the time to report the loss of an
/// object stored in the distributed object store in the case that the worker
/// that created the original ObjectRef dies.
RAY_CONFIG(int64_t, object_timeout_milliseconds, 100)

/// The maximum duration that workers can hold on to another worker's lease
/// for direct task submission until it must be returned to the raylet.
RAY_CONFIG(int64_t, worker_lease_timeout_milliseconds, 500)

/// The interval at which the workers will check if their raylet has gone down.
/// When this happens, they will kill themselves.
RAY_CONFIG(int64_t, raylet_death_check_interval_milliseconds, 1000)

/// These are used by the worker to set timeouts and to batch requests when
/// getting objects.
RAY_CONFIG(int64_t, get_timeout_milliseconds, 1000)
RAY_CONFIG(int64_t, worker_get_request_size, 10000)
RAY_CONFIG(int64_t, worker_fetch_request_size, 10000)

/// Number of times raylet client tries connecting to a raylet.
RAY_CONFIG(int64_t, raylet_client_num_connect_attempts, 10)
RAY_CONFIG(int64_t, raylet_client_connect_timeout_milliseconds, 1000)

/// The duration that the raylet will wait before reinitiating a
/// fetch request for a missing task dependency. This time may adapt based on
/// the number of missing task dependencies.
RAY_CONFIG(int64_t, raylet_fetch_timeout_milliseconds, 1000)

/// The duration that we wait after sending a worker SIGTERM before sending
/// the worker SIGKILL.
RAY_CONFIG(int64_t, kill_worker_timeout_milliseconds, 100)

/// The duration that we wait after the worker is launched before the
/// starting_worker_timeout_callback() is called.
RAY_CONFIG(int64_t, worker_register_timeout_seconds, 30)

/// Allow up to 5 seconds for connecting to Redis.
RAY_CONFIG(int64_t, redis_db_connect_retries, 50)
RAY_CONFIG(int64_t, redis_db_connect_wait_milliseconds, 100)

/// Timeout, in milliseconds, to wait before retrying a failed pull in the
/// ObjectManager.
RAY_CONFIG(int, object_manager_timer_freq_ms, 100)

/// Timeout, in milliseconds, to wait before retrying a failed pull in the
/// ObjectManager.
RAY_CONFIG(int, object_manager_pull_timeout_ms, 10000)

/// Timeout, in milliseconds, to wait until the Push request fails.
/// Special value:
/// Negative: waiting infinitely.
/// 0: giving up retrying immediately.
RAY_CONFIG(int, object_manager_push_timeout_ms, 10000)

/// Default chunk size for multi-chunk transfers to use in the object manager.
/// In the object manager, no single thread is permitted to transfer more
/// data than what is specified by the chunk size unless the number of object
/// chunks exceeds the number of available sending threads.
/// NOTE(ekl): this has been raised to lower broadcast overheads.
RAY_CONFIG(uint64_t, object_manager_default_chunk_size, 5 * 1024 * 1024)

/// The maximum number of outbound bytes to allow to be outstanding. This avoids
/// excessive memory usage during object broadcast to many receivers.
RAY_CONFIG(uint64_t, object_manager_max_bytes_in_flight, 2L * 1024 * 1024 * 1024)

/// Maximum number of ids in one batch to send to GCS to delete keys.
RAY_CONFIG(uint32_t, maximum_gcs_deletion_batch_size, 1000)

/// Maximum number of items in one batch to scan/get/delete from GCS storage.
RAY_CONFIG(uint32_t, maximum_gcs_storage_operation_batch_size, 1000)

/// Maximum number of rows in GCS profile table.
RAY_CONFIG(int32_t, maximum_profile_table_rows_count, 10 * 1000)

/// When getting objects from object store, print a warning every this number of attempts.
RAY_CONFIG(uint32_t, object_store_get_warn_per_num_attempts, 50)

/// When getting objects from object store, max number of ids to print in the warning
/// message.
RAY_CONFIG(uint32_t, object_store_get_max_ids_to_print_in_warning, 20)

/// Number of threads used by rpc server in gcs server.
RAY_CONFIG(uint32_t, gcs_server_rpc_server_thread_num, 1)
/// Allow up to 5 seconds for connecting to gcs service.
/// Note: this only takes effect when gcs service is enabled.
RAY_CONFIG(int64_t, gcs_service_connect_retries, 50)
/// Waiting time for each gcs service connection.
RAY_CONFIG(int64_t, internal_gcs_service_connect_wait_milliseconds, 100)
/// The interval at which the gcs server will check if redis has gone down.
/// When this happens, gcs server will kill itself.
RAY_CONFIG(int64_t, gcs_redis_heartbeat_interval_milliseconds, 100)
/// Duration to wait between retries for leasing worker in gcs server.
RAY_CONFIG(uint32_t, gcs_lease_worker_retry_interval_ms, 200)
/// Duration to wait between retries for creating actor in gcs server.
RAY_CONFIG(uint32_t, gcs_create_actor_retry_interval_ms, 200)
/// Duration to wait between retries for creating placement group in gcs server.
RAY_CONFIG(uint32_t, gcs_create_placement_group_retry_interval_ms, 200)
/// Maximum number of destroyed actors in GCS server memory cache.
RAY_CONFIG(uint32_t, maximum_gcs_destroyed_actor_cached_count, 100000)
/// Maximum number of dead nodes in GCS server memory cache.
RAY_CONFIG(uint32_t, maximum_gcs_dead_node_cached_count, 1000)
/// The interval at which the gcs server will print debug info.
RAY_CONFIG(int64_t, gcs_dump_debug_log_interval_minutes, 1)

/// Duration to sleep after failing to put an object in plasma because it is full.
RAY_CONFIG(uint32_t, object_store_full_delay_ms, 10)

/// The amount of time to wait between logging plasma space usage debug messages.
RAY_CONFIG(uint64_t, object_store_usage_log_interval_s, 10 * 60)

/// The amount of time between automatic local Python GC triggers.
RAY_CONFIG(uint64_t, local_gc_interval_s, 10 * 60)

/// The min amount of time between local GCs (whether auto or mem pressure triggered).
RAY_CONFIG(uint64_t, local_gc_min_interval_s, 10)

/// Duration to wait between retries for failed tasks.
RAY_CONFIG(uint32_t, task_retry_delay_ms, 5000)

/// Duration to wait between retrying to kill a task.
RAY_CONFIG(uint32_t, cancellation_retry_ms, 2000)

/// The interval at which the gcs rpc client will check if gcs rpc server is ready.
RAY_CONFIG(int64_t, ping_gcs_rpc_server_interval_milliseconds, 1000)

/// Maximum number of times to retry ping gcs rpc server when gcs server restarts.
RAY_CONFIG(int32_t, ping_gcs_rpc_server_max_retries, 1)

/// Minimum interval between reconnecting gcs rpc server when gcs server restarts.
RAY_CONFIG(int32_t, minimum_gcs_reconnect_interval_milliseconds, 5000)

/// Whether start the Plasma Store as a Raylet thread.
RAY_CONFIG(bool, plasma_store_as_thread, false)

/// Whether to release worker CPUs during plasma fetches.
/// See https://github.com/ray-project/ray/issues/12912 for further discussion.
RAY_CONFIG(bool, release_resources_during_plasma_fetch, false)

/// The interval at which the gcs client will check if the address of gcs service has
/// changed. When the address changed, we will resubscribe again.
RAY_CONFIG(int64_t, gcs_service_address_check_interval_milliseconds, 1000)

/// The batch size for metrics export.
RAY_CONFIG(int64_t, metrics_report_batch_size, 100)

/// Whether or not we enable metrics collection.
RAY_CONFIG(int64_t, enable_metrics_collection, true)

/// Whether put small objects in the local memory store.
RAY_CONFIG(bool, put_small_object_in_memory_store, false)

/// Maximum number of tasks that can be in flight between an owner and a worker for which
/// the owner has been granted a lease. A value >1 is used when we want to enable
/// pipelining task submission.
RAY_CONFIG(uint32_t, max_tasks_in_flight_per_worker, 1)

/// Interval to restart dashboard agent after the process exit.
RAY_CONFIG(uint32_t, agent_restart_interval_ms, 1000)

/// Wait timeout for dashboard agent register.
RAY_CONFIG(uint32_t, agent_register_timeout_ms, 30 * 1000)

/// The maximum number of resource shapes included in the resource
/// load reported by each raylet.
RAY_CONFIG(int64_t, max_resource_shapes_per_load_report, 100)

/// If true, the worker's queue backlog size will be propagated to the heartbeat batch
/// data.
RAY_CONFIG(bool, report_worker_backlog, true)

/// The timeout for synchronous GCS requests in seconds.
RAY_CONFIG(int64_t, gcs_server_request_timeout_seconds, 5)

/// Whether to enable worker prestarting: https://github.com/ray-project/ray/issues/12052
RAY_CONFIG(bool, enable_worker_prestart,
           getenv("RAY_ENABLE_WORKER_PRESTART") == nullptr ||
               getenv("RAY_ENABLE_WORKER_PRESTART") == std::string("1"))

/// The interval of periodic idle worker killing. A negative value means worker capping is
/// disabled.
RAY_CONFIG(int64_t, kill_idle_workers_interval_ms, 200)

/// The idle time threshold for an idle worker to be killed.
RAY_CONFIG(int64_t, idle_worker_killing_time_threshold_ms, 1000)

/// Whether start the Plasma Store as a Raylet thread.
RAY_CONFIG(bool, ownership_based_object_directory_enabled, false)

// The interval where metrics are exported in milliseconds.
RAY_CONFIG(uint64_t, metrics_report_interval_ms, 10000)

/// Enable the task timeline. If this is enabled, certain events such as task
/// execution are profiled and sent to the GCS.
RAY_CONFIG(bool, enable_timeline, true)

/// The maximum number of pending placement group entries that are reported to monitor to
/// autoscale the cluster.
RAY_CONFIG(int64_t, max_placement_group_load_report_size, 100)

/* Configuration parameters for object spilling. */
/// JSON configuration that describes the external storage. This is passed to
/// Python IO workers to determine how to store/restore an object to/from
/// external storage.
RAY_CONFIG(std::string, object_spilling_config, "")

/// Whether to enable automatic object spilling. If enabled, then
/// Ray will choose objects to spill when the object store is out of
/// memory.
RAY_CONFIG(bool, automatic_object_spilling_enabled, true)

/// The maximum number of I/O worker that raylet starts.
RAY_CONFIG(int, max_io_workers, 1)

/// Ray's object spilling fuses small objects into a single file before flushing them
/// to optimize the performance.
/// The minimum object size that can be spilled by each spill operation. 100 MB by
/// default. This value is not recommended to set beyond --object-store-memory.
RAY_CONFIG(int64_t, min_spilling_size, 100 * 1024 * 1024)

/// Whether to enable automatic object deletion when refs are gone out of scope.
/// When it is true, manual (force) spilling is not available.
/// TODO(sang): Fix it.
RAY_CONFIG(bool, automatic_object_deletion_enabled, true)

/// Grace period until we throw the OOM error to the application in seconds.
RAY_CONFIG(int64_t, oom_grace_period_s, 10)

/* Configuration parameters for locality-aware scheduling. */
/// Whether to enable locality-aware leasing. If enabled, then Ray will consider task
/// dependency locality when choosing a worker for leasing.
RAY_CONFIG(bool, locality_aware_leasing_enabled, true)
