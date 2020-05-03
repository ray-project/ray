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
RAY_CONFIG(int64_t, handler_warning_timeout_ms, 100)

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
/// only to work with  direct calls. Once direct calls afre becoming
/// the default, this scheduler will also become the default.
RAY_CONFIG(bool, new_scheduler_enabled, false)

// The max allowed size in bytes of a return object from direct actor calls.
// Objects larger than this size will be spilled/promoted to plasma.
RAY_CONFIG(int64_t, max_direct_call_object_size, 100 * 1024)

// The max gRPC message size (the gRPC internal default is 4MB). We use a higher
// limit in Ray to avoid crashing with many small inlined task arguments.
RAY_CONFIG(int64_t, max_grpc_message_size, 100 * 1024 * 1024)

// The min number of retries for direct actor creation tasks. The actual number
// of creation retries will be MAX(actor_creation_min_retries, max_reconstructions).
RAY_CONFIG(uint64_t, actor_creation_min_retries, 3)

/// The initial period for a task execution lease. The lease will expire this
/// many milliseconds after the first acquisition of the lease. Nodes that
/// require an object will not try to reconstruct the task until at least
/// this many milliseconds.
RAY_CONFIG(int64_t, initial_reconstruction_timeout_milliseconds, 10000)

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

/// The maximum number of active object IDs to report in a heartbeat.
/// # NOTE: currently disabled by default.
RAY_CONFIG(size_t, raylet_max_active_object_ids, 0)

/// The duration that we wait after sending a worker SIGTERM before sending
/// the worker SIGKILL.
RAY_CONFIG(int64_t, kill_worker_timeout_milliseconds, 100)

/// This is a timeout used to cause failures in the plasma manager and raylet
/// when certain event loop handlers take too long.
RAY_CONFIG(int64_t, max_time_for_handler_milliseconds, 1000)

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

/// Number of workers per Python worker process
RAY_CONFIG(int, num_workers_per_process_python, 1)

/// Number of workers per Java worker process
RAY_CONFIG(int, num_workers_per_process_java, 10)

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

/// Maximum number of times to retry putting an object when the plasma store is full.
/// Can be set to -1 to enable unlimited retries.
RAY_CONFIG(int32_t, object_store_full_max_retries, 5)
/// Duration to sleep after failing to put an object in plasma because it is full.
/// This will be exponentially increased for each retry.
RAY_CONFIG(uint32_t, object_store_full_initial_delay_ms, 1000)

/// Duration to wait between retries for failed tasks.
RAY_CONFIG(uint32_t, task_retry_delay_ms, 5000)

/// Duration to wait between retrying to kill a task.
RAY_CONFIG(uint32_t, cancellation_retry_ms, 2000)

/// Whether to enable gcs service.
/// RAY_GCS_SERVICE_ENABLED is an env variable which only set in ci job.
/// If the value of RAY_GCS_SERVICE_ENABLED is false, we will disable gcs service,
/// otherwise gcs service is enabled.
/// TODO(ffbin): Once we entirely migrate to service-based GCS, we should remove it.
RAY_CONFIG(bool, gcs_service_enabled,
           getenv("RAY_GCS_SERVICE_ENABLED") == nullptr ||
               getenv("RAY_GCS_SERVICE_ENABLED") == std::string("true"))

RAY_CONFIG(bool, gcs_actor_service_enabled,
           getenv("RAY_GCS_ACTOR_SERVICE_ENABLED") != nullptr &&
               getenv("RAY_GCS_ACTOR_SERVICE_ENABLED") == std::string("true"))
