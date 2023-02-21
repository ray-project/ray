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

/// The duration between dumping debug info to logs, or 0 to disable.
RAY_CONFIG(uint64_t, debug_dump_period_milliseconds, 10000)

/// Whether to enable Ray event stats collection.
/// TODO(ekl) this seems to segfault Java unit tests when on by default?
RAY_CONFIG(bool, event_stats, true)

/// Whether to enable Ray legacy scheduler warnings. These are replaced by
/// autoscaler messages after https://github.com/ray-project/ray/pull/18724.
/// TODO(ekl) remove this after Ray 1.8
RAY_CONFIG(bool, legacy_scheduler_warnings, false)

/// The interval of periodic event loop stats print.
/// -1 means the feature is disabled. In this case, stats are available to
/// debug_state_*.txt
/// NOTE: This requires event_stats=1.
RAY_CONFIG(int64_t, event_stats_print_interval_ms, 60000)

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

/// The duration between loads pulled by GCS
RAY_CONFIG(uint64_t, gcs_pull_resource_loads_period_milliseconds, 1000)

/// If a component has not sent a heartbeat in the last num_heartbeats_timeout
/// heartbeat intervals, the raylet monitor process will report
/// it as dead to the db_client table.
RAY_CONFIG(int64_t, num_heartbeats_timeout, 30)

/// If GCS restarts, before the first heatbeat is sent,
/// gcs_failover_worker_reconnect_timeout is used for the threshold
/// of the raylet. This is very useful given that raylet might need
/// a while to reconnect to the GCS, for example, when GCS is available
/// but not reachable to raylet.
RAY_CONFIG(int64_t, gcs_failover_worker_reconnect_timeout, 120)

/// For a raylet, if the last heartbeat was sent more than this many
/// heartbeat periods ago, then a warning will be logged that the heartbeat
/// handler is drifting.
RAY_CONFIG(uint64_t, num_heartbeats_warning, 5)

/// The duration between reporting resources sent by the raylets.
RAY_CONFIG(uint64_t, raylet_report_resources_period_milliseconds, 100)

/// The duration between raylet check memory pressure and send gc request
RAY_CONFIG(uint64_t, raylet_check_gc_period_milliseconds, 100)

/// For a raylet, if the last resource report was sent more than this many
/// report periods ago, then a warning will be logged that the report
/// handler is drifting.
RAY_CONFIG(uint64_t, num_resource_report_periods_warning, 5)

/// Whether to record the creation sites of object references. This adds more
/// information to `ray memory`, but introduces a little extra overhead when
/// creating object references (e.g. 5~10 microsec per call in Python).
/// TODO: maybe group this under RAY_DEBUG.
RAY_CONFIG(bool, record_ref_creation_sites, false)

/// Objects that have been unpinned are
/// added to a local cache. When the cache is flushed, all objects in the cache
/// will be eagerly evicted in a batch by freeing all plasma copies in the
/// cluster. If set, then this is the duration between attempts to flush the
/// local cache. If this is set to 0, then the objects will be freed as soon as
/// they enter the cache. To disable eager eviction, set this to -1.
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

/// Whether to pin object lineage, i.e. the task that created the object and
/// the task's recursive dependencies. If this is set to true, then the system
/// will attempt to reconstruct the object from its lineage if the object is
/// lost.
RAY_CONFIG(bool, lineage_pinning_enabled, true)

/// Objects that require recovery are added to a local cache. This is the
/// duration between attempts to flush and recover the objects in the local
/// cache.
RAY_CONFIG(int64_t, reconstruct_objects_period_milliseconds, 100)

/// Maximum amount of lineage to keep in bytes. This includes the specs of all
/// tasks that have previously already finished but that may be retried again.
/// If we reach this limit, 50% of the current lineage will be evicted and
/// objects that are still in scope will no longer be reconstructed if lost.
/// Each task spec is on the order of 1KB but can be much larger if it has many
/// inlined args.
RAY_CONFIG(int64_t, max_lineage_bytes, 1024 * 1024 * 1024)

/// Whether to re-populate plasma memory. This avoids memory allocation failures
/// at runtime (SIGBUS errors creating new objects), however it will use more memory
/// upfront and can slow down Ray startup.
/// See also: https://github.com/ray-project/ray/issues/14182
RAY_CONFIG(bool, preallocate_plasma_memory, false)

// If true, we place a soft cap on the numer of scheduling classes, see
// `worker_cap_initial_backoff_delay_ms`.
RAY_CONFIG(bool, worker_cap_enabled, true)

/// We place a soft cap on the number of tasks of a given scheduling class that
/// can run at once to limit the total nubmer of worker processes. After the
/// specified interval, the new task above that cap is allowed to run. The time
/// before the next tasks (above the cap) are allowed to run increases
/// exponentially. The soft cap is needed to prevent deadlock in the case where
/// a task begins to execute and tries to `ray.get` another task of the same
/// class.
RAY_CONFIG(int64_t, worker_cap_initial_backoff_delay_ms, 1000)

/// After reaching the worker cap, the backoff delay will grow exponentially,
/// until it hits a maximum delay.
RAY_CONFIG(int64_t, worker_cap_max_backoff_delay_ms, 1000 * 10)

/// The fraction of resource utilization on a node after which the scheduler starts
/// to prefer spreading tasks to other nodes. This balances between locality and
/// even balancing of load. Low values (min 0.0) encourage more load spreading.
RAY_CONFIG(float, scheduler_spread_threshold, 0.5);

/// Whether to only report the usage of pinned copies of objects in the
/// object_store_memory resource. This means nodes holding secondary copies only
/// will become eligible for removal in the autoscaler.
RAY_CONFIG(bool, scheduler_report_pinned_bytes_only, true)

// The max allowed size in bytes of a return object from direct actor calls.
// Objects larger than this size will be spilled/promoted to plasma.
RAY_CONFIG(int64_t, max_direct_call_object_size, 100 * 1024)

// The max gRPC message size (the gRPC internal default is 4MB). We use a higher
// limit in Ray to avoid crashing with many small inlined task arguments.
// Keep in sync with GCS_STORAGE_MAX_SIZE in packaging.py.
RAY_CONFIG(int64_t, max_grpc_message_size, 250 * 1024 * 1024)

// Retry timeout for trying to create a gRPC server. Only applies if the number
// of retries is non zero.
RAY_CONFIG(int64_t, grpc_server_retry_timeout_milliseconds, 1000)

// Whether to allow HTTP proxy on GRPC clients. Disable HTTP proxy by default since it
// disrupts local connections. Note that this config item only controls GrpcClient in
// `src/ray/rpc/grpc_client.h`. Python GRPC clients are not directly controlled by this.
// NOTE (kfstorm): DO NOT set this config item via `_system_config`, use
// `RAY_grpc_enable_http_proxy` environment variable instead so that it can be passed to
// non-C++ children processes such as dashboard agent.
RAY_CONFIG(bool, grpc_enable_http_proxy, false)

// The min number of retries for direct actor creation tasks. The actual number
// of creation retries will be MAX(actor_creation_min_retries, max_restarts).
RAY_CONFIG(uint64_t, actor_creation_min_retries, 3)

/// Warn if more than this many tasks are queued for submission to an actor.
/// It likely indicates a bug in the user code.
RAY_CONFIG(uint64_t, actor_excess_queueing_warn_threshold, 5000)

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
/// How long to wait for a fetch to complete during ray.get before warning the
/// user.
RAY_CONFIG(int64_t, fetch_warn_timeout_milliseconds, 60000)

/// How long to wait for a fetch before timing it out and throwing an error to
/// the user. This error should only be seen if there is extreme pressure on
/// the object directory, or if there is a bug in either object recovery or the
/// object directory.
RAY_CONFIG(int64_t, fetch_fail_timeout_milliseconds, 600000)

/// Temporary workaround for https://github.com/ray-project/ray/pull/16402.
RAY_CONFIG(bool, yield_plasma_lock_workaround, true)

// Whether to inline object status in serialized references.
// See https://github.com/ray-project/ray/issues/16025 for more details.
RAY_CONFIG(bool, inline_object_status_in_refs, true)

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
RAY_CONFIG(int64_t, worker_register_timeout_seconds, 60)

/// The maximum number of workers to iterate whenever we analyze the resources usage.
RAY_CONFIG(uint32_t, worker_max_resource_analysis_iteration, 128);

/// A value to add to workers' OOM score adjustment, so that the OS prioritizes
/// killing these over the raylet. 0 or positive values only (negative values
/// require sudo permissions).
/// NOTE(swang): Linux only.
RAY_CONFIG(int, worker_oom_score_adjustment, 1000)

/// Sets workers' nice value on posix systems, so that the OS prioritizes CPU for other
/// processes over worker. This makes CPU available to GCS, Raylet and user processes
/// even when workers are busy.
/// Valid value is [0, 19] (negative values require sudo permissions).
/// NOTE: Linux, Unix and MacOS only.
RAY_CONFIG(int, worker_niceness, 15)

/// Allow up to 60 seconds for connecting to Redis.
RAY_CONFIG(int64_t, redis_db_connect_retries, 600)
RAY_CONFIG(int64_t, redis_db_connect_wait_milliseconds, 100)

/// The object manager's global timer interval in milliseconds.
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
RAY_CONFIG(uint64_t,
           object_manager_max_bytes_in_flight,
           ((uint64_t)2) * 1024 * 1024 * 1024)

/// Maximum number of ids in one batch to send to GCS to delete keys.
RAY_CONFIG(uint32_t, maximum_gcs_deletion_batch_size, 1000)

/// Maximum number of items in one batch to scan/get/delete from GCS storage.
RAY_CONFIG(uint32_t, maximum_gcs_storage_operation_batch_size, 1000)

/// Maximum number of rows in GCS profile table.
RAY_CONFIG(int32_t, maximum_profile_table_rows_count, 10 * 1000)

/// When getting objects from object store, max number of ids to print in the warning
/// message.
RAY_CONFIG(uint32_t, object_store_get_max_ids_to_print_in_warning, 20)
/// Number of threads used by rpc server in gcs server.
RAY_CONFIG(uint32_t, gcs_server_rpc_server_thread_num, 1)
/// Number of threads used by rpc server in gcs server.
RAY_CONFIG(uint32_t, gcs_server_rpc_client_thread_num, 1)
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
/// Exponential backoff params for gcs to retry creating a placement group
RAY_CONFIG(uint64_t, gcs_create_placement_group_retry_min_interval_ms, 100)
RAY_CONFIG(uint64_t, gcs_create_placement_group_retry_max_interval_ms, 1000)
RAY_CONFIG(double, gcs_create_placement_group_retry_multiplier, 1.5);
/// Maximum number of destroyed actors in GCS server memory cache.
RAY_CONFIG(uint32_t, maximum_gcs_destroyed_actor_cached_count, 100000)
/// Maximum number of dead nodes in GCS server memory cache.
RAY_CONFIG(uint32_t, maximum_gcs_dead_node_cached_count, 1000)
// The interval at which the gcs server will pull a new resource.
RAY_CONFIG(int, gcs_resource_report_poll_period_ms, 100)
// The number of concurrent polls to polls to GCS.
RAY_CONFIG(uint64_t, gcs_max_concurrent_resource_pulls, 100)
// The storage backend to use for the GCS. It can be either 'redis' or 'memory'.
RAY_CONFIG(std::string, gcs_storage, "memory")

/// Duration to sleep after failing to put an object in plasma because it is full.
RAY_CONFIG(uint32_t, object_store_full_delay_ms, 10)

/// The threshold to trigger a global gc
RAY_CONFIG(double, high_plasma_storage_usage, 0.7)

/// The amount of time between automatic local Python GC triggers.
RAY_CONFIG(uint64_t, local_gc_interval_s, 10 * 60)

/// The min amount of time between local GCs (whether auto or mem pressure triggered).
RAY_CONFIG(uint64_t, local_gc_min_interval_s, 10)

/// The min amount of time between triggering global_gc in raylet. This only applies
/// to global GCs triggered due to high_plasma_storage_usage.
RAY_CONFIG(uint64_t, global_gc_min_interval_s, 30)

/// Duration to wait between retries for failed tasks.
RAY_CONFIG(uint32_t, task_retry_delay_ms, 0)

/// Duration to wait between retrying to kill a task.
RAY_CONFIG(uint32_t, cancellation_retry_ms, 2000)

/// Whether to start a background thread to import Python dependencies eagerly.
/// When set to false, Python dependencies will still be imported, only when
/// they are needed.
RAY_CONFIG(bool, start_python_importer_thread, true)

/// Determines if forking in Ray actors / tasks are supported.
/// Note that this only enables forking in workers, but not drivers.
RAY_CONFIG(bool, support_fork, false)

/// Maximum timeout for GCS reconnection in seconds.
/// Each reconnection ping will be retried every 1 second.
RAY_CONFIG(int32_t, gcs_rpc_server_reconnect_timeout_s, 60)

/// The timeout for GCS connection in seconds
RAY_CONFIG(int32_t, gcs_rpc_server_connect_timeout_s, 5)

/// Minimum interval between reconnecting gcs rpc server when gcs server restarts.
RAY_CONFIG(int32_t, minimum_gcs_reconnect_interval_milliseconds, 5000)

/// gRPC channel reconnection related configs to GCS.
/// Check https://grpc.github.io/grpc/core/group__grpc__arg__keys.html for details
RAY_CONFIG(int32_t, gcs_grpc_max_reconnect_backoff_ms, 2000)
RAY_CONFIG(int32_t, gcs_grpc_min_reconnect_backoff_ms, 100)
RAY_CONFIG(int32_t, gcs_grpc_initial_reconnect_backoff_ms, 100)

/// Maximum bytes of request queued when RPC failed due to GCS is down.
/// If reach the limit, the core worker will hang until GCS is reconnected.
/// By default, the value if 5GB.
RAY_CONFIG(uint64_t, gcs_grpc_max_request_queued_max_bytes, 1024UL * 1024 * 1024 * 5)

/// The duration between two checks for grpc status.
RAY_CONFIG(int32_t, gcs_client_check_connection_status_interval_milliseconds, 1000)

/// Feature flag to use the ray syncer for resource synchronization
RAY_CONFIG(bool, use_ray_syncer, false)

/// The queuing buffer of ray syncer. This indicates how many concurrent
/// requests can run in flight for syncing.
RAY_CONFIG(int64_t, ray_syncer_polling_buffer, 5)

/// The interval at which the gcs client will check if the address of gcs service has
/// changed. When the address changed, we will resubscribe again.
RAY_CONFIG(uint64_t, gcs_service_address_check_interval_milliseconds, 1000)

/// The batch size for metrics export.
RAY_CONFIG(int64_t, metrics_report_batch_size, 100)

/// Whether or not we enable metrics collection.
RAY_CONFIG(bool, enable_metrics_collection, true)

// Max number bytes of inlined objects in a task rpc request/response.
RAY_CONFIG(int64_t, task_rpc_inlined_bytes_limit, 10 * 1024 * 1024)

/// Maximum number of pending lease requests per scheduling category
RAY_CONFIG(uint64_t, max_pending_lease_requests_per_scheduling_category, 10)

/// Wait timeout for dashboard agent register.
#ifdef _WIN32
// agent startup time can involve creating conda environments
RAY_CONFIG(uint32_t, agent_register_timeout_ms, 100 * 1000)
#else
RAY_CONFIG(uint32_t, agent_register_timeout_ms, 30 * 1000)
#endif

/// If the agent manager fails to communicate with the dashboard agent, we will retry
/// after this interval.
RAY_CONFIG(uint32_t, agent_manager_retry_interval_ms, 1000);

/// The maximum number of resource shapes included in the resource
/// load reported by each raylet.
RAY_CONFIG(int64_t, max_resource_shapes_per_load_report, 100)

/// The timeout for synchronous GCS requests in seconds.
RAY_CONFIG(int64_t, gcs_server_request_timeout_seconds, 60)

/// Whether to enable worker prestarting: https://github.com/ray-project/ray/issues/12052
RAY_CONFIG(bool, enable_worker_prestart, true)

/// The interval of periodic idle worker killing. Value of 0 means worker capping is
/// disabled.
RAY_CONFIG(uint64_t, kill_idle_workers_interval_ms, 200)

/// The idle time threshold for an idle worker to be killed.
RAY_CONFIG(int64_t, idle_worker_killing_time_threshold_ms, 1000)

/// The soft limit of the number of workers.
/// -1 means using num_cpus instead.
RAY_CONFIG(int64_t, num_workers_soft_limit, -1)

// The interval where metrics are exported in milliseconds.
RAY_CONFIG(uint64_t, metrics_report_interval_ms, 10000)

/// Enable the task timeline. If this is enabled, certain events such as task
/// execution are profiled and sent to the GCS.
RAY_CONFIG(bool, enable_timeline, true)

/// The maximum number of pending placement group entries that are reported to monitor to
/// autoscale the cluster.
RAY_CONFIG(int64_t, max_placement_group_load_report_size, 1000)

/* Configuration parameters for object spilling. */
/// JSON configuration that describes the external storage. This is passed to
/// Python IO workers to determine how to store/restore an object to/from
/// external storage.
RAY_CONFIG(std::string, object_spilling_config, "")

/// Log an ERROR-level message about spilling every time this amount of bytes has been
/// spilled, with exponential increase in interval. This can be set to zero to disable.
RAY_CONFIG(int64_t, verbose_spill_logs, 2L * 1024 * 1024 * 1024)

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

/// If set to less than 1.0, Ray will start spilling objects when existing objects
/// take more than this percentage of the available memory.
RAY_CONFIG(float, object_spilling_threshold, 0.8)

/// Maximum number of objects that can be fused into a single file.
RAY_CONFIG(int64_t, max_fused_object_count, 2000)

/// Grace period until we throw the OOM error to the application in seconds.
/// In unlimited allocation mode, this is the time delay prior to fallback allocating.
RAY_CONFIG(int64_t, oom_grace_period_s, 2)

/// Whether or not the external storage is the local file system.
/// Note that this value should be overridden based on the storage type
/// specified by object_spilling_config.
RAY_CONFIG(bool, is_external_storage_type_fs, true)

/// Control the capacity threshold for ray local file system (for object store).
/// Once we are over the capacity, all subsequent object creation will fail.
RAY_CONFIG(float, local_fs_capacity_threshold, 0.95);

/// Control the frequency of checking the disk usage.
RAY_CONFIG(uint64_t, local_fs_monitor_interval_ms, 100);

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

/// The core worker heartbeat interval. During heartbeat, it'll
/// report the loads to raylet.
RAY_CONFIG(int64_t, core_worker_internal_heartbeat_ms, 1000);

/// Maximum amount of memory that will be used by running tasks' args.
RAY_CONFIG(float, max_task_args_memory_fraction, 0.7)

/// The maximum number of objects to publish for each publish calls.
RAY_CONFIG(int, publish_batch_size, 5000)

/// Maximum size in bytes of buffered messages per entity, in Ray publisher.
RAY_CONFIG(int, publisher_entity_buffer_max_bytes, 10 << 20)

/// The maximum command batch size.
RAY_CONFIG(int64_t, max_command_batch_size, 2000)

/// The maximum batch size for OBOD report.
RAY_CONFIG(int64_t, max_object_report_batch_size, 2000)

/// For Ray publishers, the minimum time to drop an inactive subscriber connection in ms.
/// In the current implementation, a subscriber might be dead for up to 3x the configured
/// time before it is deleted from the publisher, i.e. deleted in 300s ~ 900s.
RAY_CONFIG(uint64_t, subscriber_timeout_ms, 300 * 1000)

// This is the minimum time an actor will remain in the actor table before
// being garbage collected when a job finishes
RAY_CONFIG(uint64_t, gcs_actor_table_min_duration_ms, /*  5 min */ 60 * 1000 * 5)

/// Whether to enable GCS-based actor scheduling.
RAY_CONFIG(bool, gcs_actor_scheduling_enabled, false);

RAY_CONFIG(uint32_t, max_error_msg_size_bytes, 512 * 1024)

/// If enabled, raylet will report resources only when resources are changed.
RAY_CONFIG(bool, enable_light_weight_resource_report, true)

// The number of seconds to wait for the Raylet to start. This is normally
// fast, but when RAY_preallocate_plasma_memory=1 is set, it may take some time
// (a few GB/s) to populate all the pages on Raylet startup.
RAY_CONFIG(uint32_t,
           raylet_start_wait_time_s,
           std::getenv("RAY_preallocate_plasma_memory") != nullptr &&
                   std::getenv("RAY_preallocate_plasma_memory") == std::string("1")
               ? 120
               : 10)

/// The scheduler will treat these predefined resource types as unit_instance.
/// Default predefined_unit_instance_resources is "GPU".
/// When set it to "CPU,GPU", we will also treat CPU as unit_instance.
RAY_CONFIG(std::string, predefined_unit_instance_resources, "GPU")

/// The scheduler will treat these custom resource types as unit_instance.
/// Default custom_unit_instance_resources is empty.
/// When set it to "FPGA", we will treat FPGA as unit_instance.
RAY_CONFIG(std::string, custom_unit_instance_resources, "")

// Maximum size of the batches when broadcasting resources to raylet.
RAY_CONFIG(uint64_t, resource_broadcast_batch_size, 512);

// Maximum ray sync message batch size in bytes (1MB by default) between nodes.
RAY_CONFIG(uint64_t, max_sync_message_batch_bytes, 1 * 1024 * 1024);

// If enabled and worker stated in container, the container will add
// resource limit.
RAY_CONFIG(bool, worker_resource_limits_enabled, false)

// When enabled, workers will not be re-used across tasks requesting different
// resources (e.g., CPU vs GPU).
RAY_CONFIG(bool, isolate_workers_across_resource_types, true)

// When enabled, workers will not be re-used across tasks of different types
// (i.e., Actor vs normal tasks).
RAY_CONFIG(bool, isolate_workers_across_task_types, true)

/// ServerCall instance number of each RPC service handler
RAY_CONFIG(int64_t, gcs_max_active_rpcs_per_handler, 100)

/// grpc keepalive sent interval
/// This is only configured in GCS server now.
/// NOTE: It is not ideal for other components because
/// they have a failure model that considers network failures as component failures
/// and this configuration break that assumption. We should apply to every other component
/// after we change this failure assumption from code.
RAY_CONFIG(int64_t, grpc_keepalive_time_ms, 10000);

/// grpc keepalive timeout
RAY_CONFIG(int64_t, grpc_keepalive_timeout_ms, 20000);

/// Whether to use log reporter in event framework
RAY_CONFIG(bool, event_log_reporter_enabled, false)

/// Whether to enable register actor async.
/// If it is false, the actor registration to GCS becomes synchronous, i.e.,
/// core worker is blocked until GCS registers the actor and replies to it.
/// If it is true, the actor registration is async, but actor handles cannot
/// be passed to other worker until it is registered to GCS.
RAY_CONFIG(bool, actor_register_async, true)

/// Event severity threshold value
RAY_CONFIG(std::string, event_level, "warning")

/// Whether to avoid scheduling cpu requests on gpu nodes
RAY_CONFIG(bool, scheduler_avoid_gpu_nodes, true)

/// Whether to skip running local GC in runtime env.
RAY_CONFIG(bool, runtime_env_skip_local_gc, false)

/// The namespace for the storage.
/// This fields is used to isolate data stored in DB.
RAY_CONFIG(std::string, external_storage_namespace, "default")

/// Whether or not use TLS.
RAY_CONFIG(bool, USE_TLS, false)

/// Location of TLS credentials
RAY_CONFIG(std::string, TLS_SERVER_CERT, "")
RAY_CONFIG(std::string, TLS_SERVER_KEY, "")
RAY_CONFIG(std::string, TLS_CA_CERT, "")

/// grpc delay testing flags
///  To use this, simply do
///      export RAY_testing_asio_delay_us="method1=min_val:max_val,method2=20:100"
//  The delay is a random number between the interval. If method equals '*',
//  it will apply to all methods.
RAY_CONFIG(std::string, testing_asio_delay_us, "")
