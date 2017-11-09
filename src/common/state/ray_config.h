#ifndef RAY_CONFIG_H
#define RAY_CONFIG_H

#include <math.h>
#include <stdint.h>

class RayConfig {
 public:
  static RayConfig &instance() {
    static RayConfig config;
    return config;
  }

  int64_t ray_protocol_version() const { return ray_protocol_version_; }

  int64_t heartbeat_timeout_milliseconds() const {
    return heartbeat_timeout_milliseconds_;
  }

  int64_t num_heartbeats_timeout() const { return num_heartbeats_timeout_; }

  int64_t get_timeout_milliseconds() const { return get_timeout_milliseconds_; }

  int64_t worker_get_request_size() const { return worker_get_request_size_; }

  int64_t worker_fetch_request_size() const {
    return worker_fetch_request_size_;
  }

  int64_t num_connect_attempts() const { return num_connect_attempts_; }

  int64_t connect_timeout_milliseconds() const {
    return connect_timeout_milliseconds_;
  }

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

  double default_num_CPUs() const { return default_num_CPUs_; }

  double default_num_GPUs() const { return default_num_GPUs_; }

  double default_num_custom_resource() const {
    return default_num_custom_resource_;
  }

  int64_t manager_timeout_milliseconds() const {
    return manager_timeout_milliseconds_;
  }

  int64_t buf_size() const { return buf_size_; }

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

  int64_t plasma_default_release_delay() const {
    return plasma_default_release_delay_;
  }

  int64_t L3_cache_size_bytes() const { return L3_cache_size_bytes_; }

 private:
  RayConfig()
      : ray_protocol_version_(0x0000000000000000),
        heartbeat_timeout_milliseconds_(100),
        num_heartbeats_timeout_(100),
        get_timeout_milliseconds_(1000),
        worker_get_request_size_(10000),
        worker_fetch_request_size_(10000),
        num_connect_attempts_(50),
        connect_timeout_milliseconds_(100),
        local_scheduler_fetch_timeout_milliseconds_(1000),
        local_scheduler_reconstruction_timeout_milliseconds_(1000),
        max_num_to_reconstruct_(10000),
        local_scheduler_fetch_request_size_(10000),
        kill_worker_timeout_milliseconds_(100),
        default_num_CPUs_(INT16_MAX),
        default_num_GPUs_(0),
        default_num_custom_resource_(INFINITY),
        manager_timeout_milliseconds_(1000),
        buf_size_(4096),
        max_time_for_handler_milliseconds_(1000),
        size_limit_(100),
        num_elements_limit_(1000),
        max_time_for_loop_(1000),
        redis_db_connect_retries_(50),
        redis_db_connect_wait_milliseconds_(100),
        plasma_default_release_delay_(64),
        L3_cache_size_bytes_(100000000) {}

  ~RayConfig() {}

  /// In theory, this is used to detect Ray version mismatches.
  int64_t ray_protocol_version_;

  /// The duration between heartbeats. These are sent by the plasma manager and
  /// local scheduler.
  int64_t heartbeat_timeout_milliseconds_;
  /// If a component has not sent a heartbeat in the last num_heartbeats_timeout
  /// heartbeat intervals, the global scheduler or monitor process will report
  /// it as dead to the db_client table.
  int64_t num_heartbeats_timeout_;

  /// These are used by the worker to set timeouts and to batch requests when
  /// getting objects.
  int64_t get_timeout_milliseconds_;
  int64_t worker_get_request_size_;
  int64_t worker_fetch_request_size_;

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

  /// These are used to determine the local scheduler's behavior with respect to
  /// different types of resources.
  double default_num_CPUs_;
  double default_num_GPUs_;
  double default_num_custom_resource_;

  /// These are used by the plasma manager.
  int64_t manager_timeout_milliseconds_;
  int64_t buf_size_;

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
};

#endif  // RAY_CONFIG_H
