#ifndef RAY_CONFIG_H
#define RAY_CONFIG_H

#include <unordered_map>

#include "ray/util/logging.h"

class RayConfig {
 public:
  static RayConfig &instance() {
    static RayConfig config;
    return config;
  }

  int64_t ray_protocol_version() const { return ray_protocol_version_; }

  int64_t handler_warning_timeout_ms() const { return handler_warning_timeout_ms_; }

  int64_t heartbeat_timeout_milliseconds() const {
    return heartbeat_timeout_milliseconds_;
  }

  int64_t debug_dump_period_milliseconds() const {
    return debug_dump_period_milliseconds_;
  }

  int64_t num_heartbeats_timeout() const { return num_heartbeats_timeout_; }

  uint64_t num_heartbeats_warning() const { return num_heartbeats_warning_; }

  int64_t initial_reconstruction_timeout_milliseconds() const {
    return initial_reconstruction_timeout_milliseconds_;
  }

  int64_t get_timeout_milliseconds() const { return get_timeout_milliseconds_; }

  uint64_t max_lineage_size() const { return max_lineage_size_; }

  int64_t worker_get_request_size() const { return worker_get_request_size_; }

  int64_t worker_fetch_request_size() const { return worker_fetch_request_size_; }

  int64_t actor_max_dummy_objects() const { return actor_max_dummy_objects_; }

  int64_t num_connect_attempts() const { return num_connect_attempts_; }

  int64_t connect_timeout_milliseconds() const { return connect_timeout_milliseconds_; }

  int64_t local_scheduler_fetch_timeout_milliseconds() const {
    return local_scheduler_fetch_timeout_milliseconds_;
  }

  int64_t local_scheduler_reconstruction_timeout_milliseconds() const {
    return local_scheduler_reconstruction_timeout_milliseconds_;
  }

  int64_t max_num_to_reconstruct() const { return max_num_to_reconstruct_; }

  int64_t local_scheduler_fetch_request_size() const {
    return local_scheduler_fetch_request_size_;
  }

  int64_t kill_worker_timeout_milliseconds() const {
    return kill_worker_timeout_milliseconds_;
  }

  int64_t max_time_for_handler_milliseconds() const {
    return max_time_for_handler_milliseconds_;
  }

  int64_t size_limit() const { return size_limit_; }

  int64_t num_elements_limit() const { return num_elements_limit_; }

  int64_t max_time_for_loop() const { return max_time_for_loop_; }

  int64_t redis_db_connect_retries() const { return redis_db_connect_retries_; }

  int64_t redis_db_connect_wait_milliseconds() const {
    return redis_db_connect_wait_milliseconds_;
  };

  int64_t plasma_default_release_delay() const { return plasma_default_release_delay_; }

  int64_t L3_cache_size_bytes() const { return L3_cache_size_bytes_; }

  int64_t max_tasks_to_spillback() const { return max_tasks_to_spillback_; }

  int64_t actor_creation_num_spillbacks_warning() const {
    return actor_creation_num_spillbacks_warning_;
  }

  int node_manager_forward_task_retry_timeout_milliseconds() const {
    return node_manager_forward_task_retry_timeout_milliseconds_;
  }

  int object_manager_pull_timeout_ms() const { return object_manager_pull_timeout_ms_; }

  int object_manager_push_timeout_ms() const { return object_manager_push_timeout_ms_; }

  int object_manager_repeated_push_delay_ms() const {
    return object_manager_repeated_push_delay_ms_;
  }
  uint64_t object_manager_default_chunk_size() const {
    return object_manager_default_chunk_size_;
  }

  int num_workers_per_process() const { return num_workers_per_process_; }

  int64_t max_task_lease_timeout_ms() const { return max_task_lease_timeout_ms_; }

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

 private:
  RayConfig()
      : ray_protocol_version_(0x0000000000000000),
        handler_warning_timeout_ms_(100),
        heartbeat_timeout_milliseconds_(100),
        num_heartbeats_timeout_(300),
        num_heartbeats_warning_(5),
        debug_dump_period_milliseconds_(10000),
        initial_reconstruction_timeout_milliseconds_(10000),
        get_timeout_milliseconds_(1000),
        worker_get_request_size_(10000),
        worker_fetch_request_size_(10000),
        max_lineage_size_(100),
        actor_max_dummy_objects_(1000),
        num_connect_attempts_(5),
        connect_timeout_milliseconds_(500),
        local_scheduler_fetch_timeout_milliseconds_(1000),
        local_scheduler_reconstruction_timeout_milliseconds_(1000),
        max_num_to_reconstruct_(10000),
        local_scheduler_fetch_request_size_(10000),
        kill_worker_timeout_milliseconds_(100),
        max_time_for_handler_milliseconds_(1000),
        size_limit_(10000),
        num_elements_limit_(10000),
        max_time_for_loop_(1000),
        redis_db_connect_retries_(50),
        redis_db_connect_wait_milliseconds_(100),
        plasma_default_release_delay_(64),
        L3_cache_size_bytes_(100000000),
        max_tasks_to_spillback_(10),
        actor_creation_num_spillbacks_warning_(100),
        node_manager_forward_task_retry_timeout_milliseconds_(1000),
        object_manager_pull_timeout_ms_(10000),
        object_manager_push_timeout_ms_(10000),
        object_manager_repeated_push_delay_ms_(60000),
        object_manager_default_chunk_size_(1000000),
        num_workers_per_process_(1),
        max_task_lease_timeout_ms_(60 * 1000),
        initialized_(false) {}

  ~RayConfig() {}

  /// In theory, this is used to detect Ray version mismatches.
  int64_t ray_protocol_version_;

  /// The duration that a single handler on the event loop can take before a
  /// warning is logged that the handler is taking too long.
  int64_t handler_warning_timeout_ms_;

  /// The duration between heartbeats. These are sent by the raylet.
  int64_t heartbeat_timeout_milliseconds_;
  /// If a component has not sent a heartbeat in the last num_heartbeats_timeout
  /// heartbeat intervals, the global scheduler or monitor process will report
  /// it as dead to the db_client table.
  int64_t num_heartbeats_timeout_;
  /// For a raylet, if the last heartbeat was sent more than this many
  /// heartbeat periods ago, then a warning will be logged that the heartbeat
  /// handler is drifting.
  uint64_t num_heartbeats_warning_;

  /// The duration between dumping debug info to logs, or -1 to disable.
  int64_t debug_dump_period_milliseconds_;

  /// The initial period for a task execution lease. The lease will expire this
  /// many milliseconds after the first acquisition of the lease. Nodes that
  /// require an object will not try to reconstruct the task until at least
  /// this many milliseconds.
  int64_t initial_reconstruction_timeout_milliseconds_;

  /// These are used by the worker to set timeouts and to batch requests when
  /// getting objects.
  int64_t get_timeout_milliseconds_;
  int64_t worker_get_request_size_;
  int64_t worker_fetch_request_size_;

  /// This is used to bound the size of the Raylet's lineage cache. This is
  /// the maximum uncommitted lineage size that any remote task in the cache
  /// can have before eviction will be attempted.
  uint64_t max_lineage_size_;

  /// This is a temporary constant used by actors to determine how many dummy
  /// objects to store.
  int64_t actor_max_dummy_objects_;

  /// Number of times we try connecting to a socket.
  int64_t num_connect_attempts_;
  int64_t connect_timeout_milliseconds_;

  /// The duration that the local scheduler will wait before reinitiating a
  /// fetch request for a missing task dependency. This time may adapt based on
  /// the number of missing task dependencies.
  int64_t local_scheduler_fetch_timeout_milliseconds_;
  /// The duration that the local scheduler will wait between initiating
  /// reconstruction calls for missing task dependencies. If there are many
  /// missing task dependencies, we will only iniate reconstruction calls for
  /// some of them each time.
  int64_t local_scheduler_reconstruction_timeout_milliseconds_;
  /// The maximum number of objects that the local scheduler will issue
  /// reconstruct calls for in a single pass through the reconstruct object
  /// timeout handler.
  int64_t max_num_to_reconstruct_;
  /// The maximum number of objects to include in a single fetch request in the
  /// regular local scheduler fetch timeout handler.
  int64_t local_scheduler_fetch_request_size_;

  /// The duration that we wait after sending a worker SIGTERM before sending
  /// the worker SIGKILL.
  int64_t kill_worker_timeout_milliseconds_;

  /// This is a timeout used to cause failures in the plasma manager and local
  /// scheduler when certain event loop handlers take too long.
  int64_t max_time_for_handler_milliseconds_;

  /// This is used by the Python extension when serializing objects as part of
  /// a task spec.
  int64_t size_limit_;
  int64_t num_elements_limit_;

  /// This is used to cause failures when a certain loop in redis.cc which
  /// synchronously looks up object manager addresses in redis is slow.
  int64_t max_time_for_loop_;

  /// Allow up to 5 seconds for connecting to Redis.
  int64_t redis_db_connect_retries_;
  int64_t redis_db_connect_wait_milliseconds_;

  /// TODO(rkn): These constants are currently unused.
  int64_t plasma_default_release_delay_;
  int64_t L3_cache_size_bytes_;

  /// Constants for the spillback scheduling policy.
  int64_t max_tasks_to_spillback_;

  /// Every time an actor creation task has been spilled back a number of times
  /// that is a multiple of this quantity, a warning will be pushed to the
  /// corresponding driver. Since spillback currently occurs on a 100ms timer,
  /// a value of 100 corresponds to a warning every 10 seconds.
  int64_t actor_creation_num_spillbacks_warning_;

  /// If a node manager attempts to forward a task to another node manager and
  /// the forward fails, then it will resubmit the task after this duration.
  int64_t node_manager_forward_task_retry_timeout_milliseconds_;

  /// Timeout, in milliseconds, to wait before retrying a failed pull in the
  /// ObjectManager.
  int object_manager_pull_timeout_ms_;

  /// Timeout, in milliseconds, to wait until the Push request fails.
  /// Special value:
  /// Negative: waiting infinitely.
  /// 0: giving up retrying immediately.
  int object_manager_push_timeout_ms_;

  /// The period of time that an object manager will wait before pushing the
  /// same object again to a specific object manager.
  int object_manager_repeated_push_delay_ms_;

  /// Default chunk size for multi-chunk transfers to use in the object manager.
  /// In the object manager, no single thread is permitted to transfer more
  /// data than what is specified by the chunk size unless the number of object
  /// chunks exceeds the number of available sending threads.
  uint64_t object_manager_default_chunk_size_;

  /// Number of workers per process
  int num_workers_per_process_;

  // Maximum timeout in milliseconds within which a task lease must be renewed.
  int64_t max_task_lease_timeout_ms_;

  /// Whether the initialization of the instance has been called before.
  /// The RayConfig instance can only (and must) be initialized once.
  bool initialized_;
};

#endif  // RAY_CONFIG_H
