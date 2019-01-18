#ifndef RAY_CONFIG_H
#define RAY_CONFIG_H

#include <unordered_map>

#include "ray/util/logging.h"

/// A helper macro that defines a config item.
/// In particular, this generates a private field called `name_` and a public getter
/// method called `name()` for a given config item.
///
/// \param type Type of the config item.
/// \param name Name of the config item.
/// \param default_value Default value of the config item.
#define DEFINE_CONFIG(type, name, default_value) \
 private:                                        \
  type name##_ = default_value;                  \
                                                 \
 public:                                         \
  inline type name() { return name##_; }

class RayConfig {
  // Note: below are the definitions of all config items. If you add or remove
  // any items, you should also modify the `initialize` method accordingly.

  /// In theory, this is used to detect Ray version mismatches.
  DEFINE_CONFIG(int64_t, ray_protocol_version, 0x0000000000000000);

  /// The duration that a single handler on the event loop can take before a
  /// warning is logged that the handler is taking too long.
  DEFINE_CONFIG(int64_t, handler_warning_timeout_ms, 100);

  /// The duration between heartbeats. These are sent by the raylet.
  DEFINE_CONFIG(int64_t, heartbeat_timeout_milliseconds, 100);
  /// If a component has not sent a heartbeat in the last num_heartbeats_timeout
  /// heartbeat intervals, the global scheduler or monitor process will report
  /// it as dead to the db_client table.
  DEFINE_CONFIG(int64_t, num_heartbeats_timeout, 300);
  /// For a raylet, if the last heartbeat was sent more than this many
  /// heartbeat periods ago, then a warning will be logged that the heartbeat
  /// handler is drifting.
  DEFINE_CONFIG(uint64_t, num_heartbeats_warning, 5);

  /// The duration between dumping debug info to logs, or -1 to disable.
  DEFINE_CONFIG(int64_t, debug_dump_period_milliseconds, 10000);

  /// The initial period for a task execution lease. The lease will expire this
  /// many milliseconds after the first acquisition of the lease. Nodes that
  /// require an object will not try to reconstruct the task until at least
  /// this many milliseconds.
  DEFINE_CONFIG(int64_t, initial_reconstruction_timeout_milliseconds, 10000);

  /// These are used by the worker to set timeouts and to batch requests when
  /// getting objects.
  DEFINE_CONFIG(int64_t, get_timeout_milliseconds, 1000);
  DEFINE_CONFIG(int64_t, worker_get_request_size, 10000);
  DEFINE_CONFIG(int64_t, worker_fetch_request_size, 10000);

  /// This is used to bound the size of the Raylet's lineage cache. This is
  /// the maximum uncommitted lineage size that any remote task in the cache
  /// can have before eviction will be attempted.
  DEFINE_CONFIG(uint64_t, max_lineage_size, 100);

  /// This is a temporary constant used by actors to determine how many dummy
  /// objects to store.
  DEFINE_CONFIG(int64_t, actor_max_dummy_objects, 1000);

  /// Number of times we try connecting to a socket.
  DEFINE_CONFIG(int64_t, num_connect_attempts, 5);
  DEFINE_CONFIG(int64_t, connect_timeout_milliseconds, 500);

  /// The duration that the local scheduler will wait before reinitiating a
  /// fetch request for a missing task dependency. This time may adapt based on
  /// the number of missing task dependencies.
  DEFINE_CONFIG(int64_t, local_scheduler_fetch_timeout_milliseconds, 1000);
  /// The duration that the local scheduler will wait between initiating
  /// reconstruction calls for missing task dependencies. If there are many
  /// missing task dependencies, we will only iniate reconstruction calls for
  /// some of them each time.
  DEFINE_CONFIG(int64_t, local_scheduler_reconstruction_timeout_milliseconds, 1000);
  /// The maximum number of objects that the local scheduler will issue
  /// reconstruct calls for in a single pass through the reconstruct object
  /// timeout handler.
  DEFINE_CONFIG(int64_t, max_num_to_reconstruct, 10000);
  /// The maximum number of objects to include in a single fetch request in the
  /// regular local scheduler fetch timeout handler.
  DEFINE_CONFIG(int64_t, local_scheduler_fetch_request_size, 10000);

  /// The duration that we wait after sending a worker SIGTERM before sending
  /// the worker SIGKILL.
  DEFINE_CONFIG(int64_t, kill_worker_timeout_milliseconds, 100);

  /// This is a timeout used to cause failures in the plasma manager and local
  /// scheduler when certain event loop handlers take too long.
  DEFINE_CONFIG(int64_t, max_time_for_handler_milliseconds, 1000);

  /// This is used by the Python extension when serializing objects as part of
  /// a task spec.
  DEFINE_CONFIG(int64_t, size_limit, 10000);
  DEFINE_CONFIG(int64_t, num_elements_limit, 10000);

  /// This is used to cause failures when a certain loop in redis.cc which
  /// synchronously looks up object manager addresses in redis is slow.
  DEFINE_CONFIG(int64_t, max_time_for_loop, 1000);

  /// Allow up to 5 seconds for connecting to Redis.
  DEFINE_CONFIG(int64_t, redis_db_connect_retries, 50);
  DEFINE_CONFIG(int64_t, redis_db_connect_wait_milliseconds, 100);

  /// TODO(rkn): These constants are currently unused.
  DEFINE_CONFIG(int64_t, plasma_default_release_delay, 64);
  DEFINE_CONFIG(int64_t, L3_cache_size_bytes, 100000000);

  /// Constants for the spillback scheduling policy.
  DEFINE_CONFIG(int64_t, max_tasks_to_spillback, 10);

  /// Every time an actor creation task has been spilled back a number of times
  /// that is a multiple of this quantity, a warning will be pushed to the
  /// corresponding driver. Since spillback currently occurs on a 100ms timer,
  /// a value of 100 corresponds to a warning every 10 seconds.
  DEFINE_CONFIG(int64_t, actor_creation_num_spillbacks_warning, 100);

  /// If a node manager attempts to forward a task to another node manager and
  /// the forward fails, then it will resubmit the task after this duration.
  DEFINE_CONFIG(int64_t, node_manager_forward_task_retry_timeout_milliseconds, 1000);

  /// Timeout, in milliseconds, to wait before retrying a failed pull in the
  /// ObjectManager.
  DEFINE_CONFIG(int, object_manager_pull_timeout_ms, 10000);

  /// Timeout, in milliseconds, to wait until the Push request fails.
  /// Special value:
  /// Negative: waiting infinitely.
  /// 0: giving up retrying immediately.
  DEFINE_CONFIG(int, object_manager_push_timeout_ms, 10000);

  /// The period of time that an object manager will wait before pushing the
  /// same object again to a specific object manager.
  DEFINE_CONFIG(int, object_manager_repeated_push_delay_ms, 60000);

  /// Default chunk size for multi-chunk transfers to use in the object manager.
  /// In the object manager, no single thread is permitted to transfer more
  /// data than what is specified by the chunk size unless the number of object
  /// chunks exceeds the number of available sending threads.
  DEFINE_CONFIG(uint64_t, object_manager_default_chunk_size, 1000000);

  /// Number of workers per process
  DEFINE_CONFIG(int, num_workers_per_process, 1);

  // Maximum timeout in milliseconds within which a task lease must be renewed.
  DEFINE_CONFIG(int64_t, max_task_lease_timeout_ms, 60000);

 public:
  static RayConfig &instance() {
    static RayConfig config;
    return config;
  }

  void initialize(const std::unordered_map<std::string, int> &config_map) {
    RAY_CHECK(!initialized_);
    for (auto const &pair : config_map) {
      // We use a big chain of if else statements because C++ doesn't allow
      // switch statements on strings.
      if (pair.first == "ray_protocol_version") {
        ray_protocol_version_ = pair.second;
      } else if (pair.first == "handler_warning_timeout_ms") {
        handler_warning_timeout_ms_ = pair.second;
      } else if (pair.first == "heartbeat_timeout_milliseconds") {
        heartbeat_timeout_milliseconds_ = pair.second;
      } else if (pair.first == "debug_dump_period_milliseconds") {
        debug_dump_period_milliseconds_ = pair.second;
      } else if (pair.first == "num_heartbeats_timeout") {
        num_heartbeats_timeout_ = pair.second;
      } else if (pair.first == "num_heartbeats_warning") {
        num_heartbeats_warning_ = pair.second;
      } else if (pair.first == "initial_reconstruction_timeout_milliseconds") {
        initial_reconstruction_timeout_milliseconds_ = pair.second;
      } else if (pair.first == "get_timeout_milliseconds") {
        get_timeout_milliseconds_ = pair.second;
      } else if (pair.first == "worker_get_request_size") {
        worker_get_request_size_ = pair.second;
      } else if (pair.first == "worker_fetch_request_size") {
        worker_fetch_request_size_ = pair.second;
      } else if (pair.first == "max_lineage_size") {
        max_lineage_size_ = pair.second;
      } else if (pair.first == "actor_max_dummy_objects") {
        actor_max_dummy_objects_ = pair.second;
      } else if (pair.first == "num_connect_attempts") {
        num_connect_attempts_ = pair.second;
      } else if (pair.first == "connect_timeout_milliseconds") {
        connect_timeout_milliseconds_ = pair.second;
      } else if (pair.first == "local_scheduler_fetch_timeout_milliseconds") {
        local_scheduler_fetch_timeout_milliseconds_ = pair.second;
      } else if (pair.first == "local_scheduler_reconstruction_timeout_milliseconds") {
        local_scheduler_reconstruction_timeout_milliseconds_ = pair.second;
      } else if (pair.first == "max_num_to_reconstruct") {
        max_num_to_reconstruct_ = pair.second;
      } else if (pair.first == "local_scheduler_fetch_request_size") {
        local_scheduler_fetch_request_size_ = pair.second;
      } else if (pair.first == "kill_worker_timeout_milliseconds") {
        kill_worker_timeout_milliseconds_ = pair.second;
      } else if (pair.first == "max_time_for_handler_milliseconds") {
        max_time_for_handler_milliseconds_ = pair.second;
      } else if (pair.first == "size_limit") {
        size_limit_ = pair.second;
      } else if (pair.first == "num_elements_limit") {
        num_elements_limit_ = pair.second;
      } else if (pair.first == "max_time_for_loop") {
        max_time_for_loop_ = pair.second;
      } else if (pair.first == "redis_db_connect_retries") {
        redis_db_connect_retries_ = pair.second;
      } else if (pair.first == "redis_db_connect_wait_milliseconds") {
        redis_db_connect_wait_milliseconds_ = pair.second;
      } else if (pair.first == "plasma_default_release_delay") {
        plasma_default_release_delay_ = pair.second;
      } else if (pair.first == "L3_cache_size_bytes") {
        L3_cache_size_bytes_ = pair.second;
      } else if (pair.first == "max_tasks_to_spillback") {
        max_tasks_to_spillback_ = pair.second;
      } else if (pair.first == "actor_creation_num_spillbacks_warning") {
        actor_creation_num_spillbacks_warning_ = pair.second;
      } else if (pair.first == "node_manager_forward_task_retry_timeout_milliseconds") {
        node_manager_forward_task_retry_timeout_milliseconds_ = pair.second;
      } else if (pair.first == "object_manager_pull_timeout_ms") {
        object_manager_pull_timeout_ms_ = pair.second;
      } else if (pair.first == "object_manager_push_timeout_ms") {
        object_manager_push_timeout_ms_ = pair.second;
      } else if (pair.first == "object_manager_default_chunk_size") {
        object_manager_default_chunk_size_ = pair.second;
      } else if (pair.first == "object_manager_repeated_push_delay_ms") {
        object_manager_repeated_push_delay_ms_ = pair.second;
      } else if (pair.first == "max_task_lease_timeout_ms") {
        max_task_lease_timeout_ms_ = pair.second;
      } else {
        RAY_LOG(FATAL) << "Received unexpected config parameter " << pair.first;
      }
    }
    initialized_ = true;
  }

  /// Whether the initialization of the instance has been called before.
  /// The RayConfig instance can only (and must) be initialized once.
  bool initialized_ = false;
};

#undef DEFINE_CONFIG

#endif  // RAY_CONFIG_H
