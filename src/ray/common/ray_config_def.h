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
RAY_CONFIG(int64_t, handler_warning_timeout_ms, 100)

/// The duration between heartbeats. These are sent by the raylet.
RAY_CONFIG(int64_t, heartbeat_timeout_milliseconds, 100)
/// If a component has not sent a heartbeat in the last num_heartbeats_timeout
/// heartbeat intervals, the raylet monitor process will report
/// it as dead to the db_client table.
RAY_CONFIG(int64_t, num_heartbeats_timeout, 300)
/// For a raylet, if the last heartbeat was sent more than this many
/// heartbeat periods ago, then a warning will be logged that the heartbeat
/// handler is drifting.
RAY_CONFIG(uint64_t, num_heartbeats_warning, 5)

/// The duration between dumping debug info to logs, or -1 to disable.
RAY_CONFIG(int64_t, debug_dump_period_milliseconds, 10000)

/// The initial period for a task execution lease. The lease will expire this
/// many milliseconds after the first acquisition of the lease. Nodes that
/// require an object will not try to reconstruct the task until at least
/// this many milliseconds.
RAY_CONFIG(int64_t, initial_reconstruction_timeout_milliseconds, 10000)

/// These are used by the worker to set timeouts and to batch requests when
/// getting objects.
RAY_CONFIG(int64_t, get_timeout_milliseconds, 1000)
RAY_CONFIG(int64_t, worker_get_request_size, 10000)
RAY_CONFIG(int64_t, worker_fetch_request_size, 10000)

/// This is used to bound the size of the Raylet's lineage cache. This is
/// the maximum uncommitted lineage size that any remote task in the cache
/// can have before eviction will be attempted.
RAY_CONFIG(uint64_t, max_lineage_size, 100)

/// This is a temporary constant used by actors to determine how many dummy
/// objects to store.
RAY_CONFIG(int64_t, actor_max_dummy_objects, 1000)

/// Number of times we try connecting to a socket.
RAY_CONFIG(int64_t, num_connect_attempts, 5)
RAY_CONFIG(int64_t, connect_timeout_milliseconds, 500)

/// The duration that the raylet will wait before reinitiating a
/// fetch request for a missing task dependency. This time may adapt based on
/// the number of missing task dependencies.
RAY_CONFIG(int64_t, raylet_fetch_timeout_milliseconds, 1000)

/// The duration that the raylet will wait between initiating
/// reconstruction calls for missing task dependencies. If there are many
/// missing task dependencies, we will only iniate reconstruction calls for
/// some of them each time.
RAY_CONFIG(int64_t, raylet_reconstruction_timeout_milliseconds, 1000)

/// The maximum number of objects that the raylet will issue
/// reconstruct calls for in a single pass through the reconstruct object
/// timeout handler.
RAY_CONFIG(int64_t, max_num_to_reconstruct, 10000)

/// The maximum number of objects to include in a single fetch request in the
/// regular raylet fetch timeout handler.
RAY_CONFIG(int64_t, raylet_fetch_request_size, 10000)

/// The duration that we wait after sending a worker SIGTERM before sending
/// the worker SIGKILL.
RAY_CONFIG(int64_t, kill_worker_timeout_milliseconds, 100)

/// This is a timeout used to cause failures in the plasma manager and raylet
/// when certain event loop handlers take too long.
RAY_CONFIG(int64_t, max_time_for_handler_milliseconds, 1000)

/// This is used by the Python extension when serializing objects as part of
/// a task spec.
RAY_CONFIG(int64_t, size_limit, 10000)
RAY_CONFIG(int64_t, num_elements_limit, 10000)

/// This is used to cause failures when a certain loop in redis.cc which
/// synchronously looks up object manager addresses in redis is slow.
RAY_CONFIG(int64_t, max_time_for_loop, 1000)

/// Allow up to 5 seconds for connecting to Redis.
RAY_CONFIG(int64_t, redis_db_connect_retries, 50)
RAY_CONFIG(int64_t, redis_db_connect_wait_milliseconds, 100)

/// TODO(rkn): These constants are currently unused.
RAY_CONFIG(int64_t, plasma_default_release_delay, 64)
RAY_CONFIG(int64_t, L3_cache_size_bytes, 100000000)

/// Constants for the spillback scheduling policy.
RAY_CONFIG(int64_t, max_tasks_to_spillback, 10)

/// Every time an actor creation task has been spilled back a number of times
/// that is a multiple of this quantity, a warning will be pushed to the
/// corresponding driver. Since spillback currently occurs on a 100ms timer,
/// a value of 100 corresponds to a warning every 10 seconds.
RAY_CONFIG(int64_t, actor_creation_num_spillbacks_warning, 100)

/// If a node manager attempts to forward a task to another node manager and
/// the forward fails, then it will resubmit the task after this duration.
RAY_CONFIG(int64_t, node_manager_forward_task_retry_timeout_milliseconds, 1000)

/// Timeout, in milliseconds, to wait before retrying a failed pull in the
/// ObjectManager.
RAY_CONFIG(int, object_manager_pull_timeout_ms, 10000)

/// Timeout, in milliseconds, to wait until the Push request fails.
/// Special value:
/// Negative: waiting infinitely.
/// 0: giving up retrying immediately.
RAY_CONFIG(int, object_manager_push_timeout_ms, 10000)

/// The period of time that an object manager will wait before pushing the
/// same object again to a specific object manager.
RAY_CONFIG(int, object_manager_repeated_push_delay_ms, 60000)

/// Default chunk size for multi-chunk transfers to use in the object manager.
/// In the object manager, no single thread is permitted to transfer more
/// data than what is specified by the chunk size unless the number of object
/// chunks exceeds the number of available sending threads.
RAY_CONFIG(uint64_t, object_manager_default_chunk_size, 1000000)

/// Number of workers per process
RAY_CONFIG(int, num_workers_per_process, 1)

/// Maximum timeout in milliseconds within which a task lease must be renewed.
RAY_CONFIG(int64_t, max_task_lease_timeout_ms, 60000)

/// Maximum number of checkpoints to keep in GCS for an actor.
/// Note: this number should be set to at least 2. Because saving a application
/// checkpoint isn't atomic with saving the backend checkpoint, and it will break
/// if this number is set to 1 and users save application checkpoints in place.
RAY_CONFIG(int32_t, num_actor_checkpoints_to_keep, 20)

/// Maximum number of ids in one batch to send to GCS to delete keys.
RAY_CONFIG(uint32_t, maximum_gcs_deletion_batch_size, 1000)

/// When getting objects from object store, print a warning every this number of attempts.
RAY_CONFIG(uint32_t, object_store_get_warn_per_num_attempts, 50)

/// When getting objects from object store, max number of ids to print in the warning
/// message.
RAY_CONFIG(uint32_t, object_store_get_max_ids_to_print_in_warning, 20)
