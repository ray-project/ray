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
RAY_CONFIG(uint64_t, raylet_heartbeat_period_milliseconds, 1000)
/// If a component has not sent a heartbeat in the last num_heartbeats_timeout
/// heartbeat intervals, the raylet monitor process will report
/// it as dead to the db_client table.
RAY_CONFIG(int64_t, num_heartbeats_timeout, 30)
/// For a raylet, if the last heartbeat was sent more than this many
/// heartbeat periods ago, then a warning will be logged that the heartbeat
/// handler is drifting.
RAY_CONFIG(uint64_t, num_heartbeats_warning, 5)

/// The duration between reporting resources sent by the raylets.
RAY_CONFIG(uint64_t, raylet_report_resources_period_milliseconds, 100)
/// For a raylet, if the last resource report was sent more than this many
/// report periods ago, then a warning will be logged that the report
/// handler is drifting.
RAY_CONFIG(uint64_t, num_resource_report_periods_warning, 5)

/// The duration between dumping debug info to logs, or 0 to disable.
RAY_CONFIG(uint64_t, debug_dump_period_milliseconds, 10000)

RAY_CONFIG(bool, asio_event_loop_stats_collection_enabled, true)

/// Whether to enable fair queueing between task classes in raylet. When
/// fair queueing is enabled, the raylet will try to balance the number
/// of running tasks by class (i.e., function name). This prevents one
/// type of task from starving other types (see issue #3664).
RAY_CONFIG(bool, fair_queueing_enabled, true)

/// Whether to enable distributed reference counting for objects. When this is
/// enabled, an object's ref count will include any references held by other
/// processes, such as when an ObjectID is serialized and passed as an argument
/// to another task. It will also include any references due to nesting, i.e.
/// if the object ID is nested inside another object that is still in scope.
/// When this is disabled, an object's ref count will include only local
/// information:
///  1. Local Python references to the ObjectID.
///  2. Pending tasks submitted by the local process that depend on the object.
/// If both this flag is turned on, then an object
/// will not be LRU evicted until it is out of scope in ALL processes in the
/// cluster and all objects that contain it are also out of scope.
RAY_CONFIG(bool, distributed_ref_counting_enabled, true)

/// Whether to record the creation sites of object references. This adds more
/// information to `ray memstat`, but introduces a little extra overhead when
/// creating object references.
RAY_CONFIG(bool, record_ref_creation_sites, true)

/// Objects that have been unpinned are
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
/// raylet_heartbeat_period_milliseconds.
RAY_CONFIG(int64_t, free_objects_period_milliseconds, 1000)

/// Objects that have been unpinned are
/// added to a local cache. When the cache is flushed, all objects in the cache
/// will be eagerly evicted in a batch by freeing all plasma copies in the
/// cluster. This is the maximum number of objects in the local cache before it
/// is flushed. To disable eager eviction, set free_objects_period_milliseconds
/// to -1.
RAY_CONFIG(size_t, free_objects_batch_size, 100)

RAY_CONFIG(bool, lineage_pinning_enabled, false)

/// Whether to re-populate plasma memory. This avoids memory allocation failures
/// at runtime (SIGBUS errors creating new objects), however it will use more memory
/// upfront and can slow down Ray startup.
/// See also: https://github.com/ray-project/ray/issues/14182
RAY_CONFIG(bool, preallocate_plasma_memory,
           getenv("RAY_PREALLOCATE_PLASMA_MEMORY") != nullptr &&
               getenv("RAY_PREALLOCATE_PLASMA_MEMORY") != std::string("0"))

/// Pick between 2 scheduling spillback strategies. Load balancing mode picks the node at
/// uniform random from the valid options. The other mode is more likely to spill back
/// many tasks to the same node.
RAY_CONFIG(bool, scheduler_loadbalance_spillback,
           getenv("RAY_SCHEDULER_LOADBALANCE_SPILLBACK") != nullptr &&
               getenv("RAY_SCHEDULER_LOADBALANCE_SPILLBACK") != std::string("1"))

/// Whether to use the hybrid scheduling policy, or one of the legacy spillback
/// strategies. In the hybrid scheduling strategy, leases are packed until a threshold,
/// then spread via weighted (by critical resource usage).
RAY_CONFIG(bool, scheduler_hybrid_scheduling,
           getenv("RAY_SCHEDULER_HYBRID") == nullptr ||
               getenv("RAY_SCHEDULER_HYBRID") != std::string("0"))

RAY_CONFIG(float, scheduler_hybrid_threshold,
           getenv("RAY_SCHEDULER_HYBRID_THRESHOLD") == nullptr
               ? 0.5
               : std::stof("RAY_SCHEDULER_HYBRID_THRESHOLD"))

// The max allowed size in bytes of a return object from direct actor calls.
// Objects larger than this size will be spilled/promoted to plasma.
RAY_CONFIG(int64_t, max_direct_call_object_size, 100 * 1024)

// The max gRPC message size (the gRPC internal default is 4MB). We use a higher
// limit in Ray to avoid crashing with many small inlined task arguments.
RAY_CONFIG(int64_t, max_grpc_message_size, 100 * 1024 * 1024)

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
RAY_CONFIG(uint64_t, raylet_death_check_interval_milliseconds, 1000)

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
RAY_CONFIG(uint64_t, gcs_redis_heartbeat_interval_milliseconds, 100)
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
// The interval at which the gcs server will pull a new resource.
RAY_CONFIG(int, gcs_resource_report_poll_period_ms, 100)
// The number of concurrent polls to polls to GCS.
RAY_CONFIG(uint64_t, gcs_max_concurrent_resource_pulls, 100)
// Feature flag to turn on resource report polling. Polling and raylet pushing are
// mutually exlusive.
RAY_CONFIG(bool, pull_based_resource_reporting, true)
// Feature flag to use grpc instead of redis for resource broadcast.
RAY_CONFIG(bool, grpc_based_resource_broadcast, true)

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
RAY_CONFIG(int32_t, ping_gcs_rpc_server_max_retries, 600)

/// Minimum interval between reconnecting gcs rpc server when gcs server restarts.
RAY_CONFIG(int32_t, minimum_gcs_reconnect_interval_milliseconds, 5000)

/// Whether to release worker CPUs during plasma fetches.
/// See https://github.com/ray-project/ray/issues/12912 for further discussion.
RAY_CONFIG(bool, release_resources_during_plasma_fetch, false)

/// The interval at which the gcs client will check if the address of gcs service has
/// changed. When the address changed, we will resubscribe again.
RAY_CONFIG(uint64_t, gcs_service_address_check_interval_milliseconds, 1000)

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

/// The interval of periodic idle worker killing. Value of 0 means worker capping is
/// disabled.
RAY_CONFIG(uint64_t, kill_idle_workers_interval_ms, 200)

/// The idle time threshold for an idle worker to be killed.
RAY_CONFIG(int64_t, idle_worker_killing_time_threshold_ms, 1000)

/// Whether start the Plasma Store as a Raylet thread.
RAY_CONFIG(bool, ownership_based_object_directory_enabled, true)

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
RAY_CONFIG(int, max_io_workers, 4)

/// Ray's object spilling fuses small objects into a single file before flushing them
/// to optimize the performance.
/// The minimum object size that can be spilled by each spill operation. 100 MB by
/// default. This value is not recommended to set beyond --object-store-memory.
RAY_CONFIG(int64_t, min_spilling_size, 100 * 1024 * 1024)

/// Maximum number of objects that can be fused into a single file.
RAY_CONFIG(int64_t, max_fused_object_count, 2000)

/// Whether to enable automatic object deletion when refs are gone out of scope.
/// When it is true, manual (force) spilling is not available.
/// TODO(sang): Fix it.
RAY_CONFIG(bool, automatic_object_deletion_enabled, true)

/// Grace period until we throw the OOM error to the application in seconds.
RAY_CONFIG(int64_t, oom_grace_period_s, 10)

/// Whether or not the external storage is file system.
/// This is configured based on object_spilling_config.
RAY_CONFIG(bool, is_external_storage_type_fs, true)

/* Configuration parameters for locality-aware scheduling. */
/// Whether to enable locality-aware leasing. If enabled, then Ray will consider task
/// dependency locality when choosing a worker for leasing.
RAY_CONFIG(bool, locality_aware_leasing_enabled, true)

/* Configuration parameters for logging */
/// Parameters for log rotation. This value is equivalent to RotatingFileHandler's
/// maxBytes argument.
RAY_CONFIG(int64_t, log_rotation_max_bytes, 100 * 1024 * 1024)

/// Parameters for log rotation. This value is equivalent to RotatingFileHandler's
/// backupCount argument.
RAY_CONFIG(int64_t, log_rotation_backup_count, 5)

/// When tasks that can't be sent because of network error. we'll never receive a DEAD
/// notification, in this case we'll wait for a fixed timeout value and then mark it
/// as failed.
RAY_CONFIG(int64_t, timeout_ms_task_wait_for_death_info, 1000)

/// The interval of periodic asio event loop stats print.
/// -1 means the feature is disabled. In this case, stats are only available to
/// debug_state.txt for raylets.
/// NOTE: This requires asio_event_loop_stats_collection_enabled to be true.
RAY_CONFIG(int64_t, asio_stats_print_interval_ms, -1)

/// Maximum amount of memory that will be used by running tasks' args.
RAY_CONFIG(float, max_task_args_memory_fraction, 0.7)

/// The maximum number of objects to publish for each publish calls.
RAY_CONFIG(int, publish_batch_size, 5000)

/// The time where the subscriber connection is timed out in milliseconds.
/// This is for the pubsub module.
RAY_CONFIG(uint64_t, subscriber_timeout_ms, 30000)
