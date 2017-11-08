#ifndef RAY_CONFIG_H
#define RAY_CONFIG_H

#include <math.h>
#include <stdint.h>

class RayConfig {
 public:
  static RayConfig& instance() {
    static RayConfig config;
    return config;
  }

  int64_t kRayProtocolVersion() const { return kRayProtocolVersion_; }

  int64_t kHeartbeatTimeoutMilliseconds() const {
    return kHeartbeatTimeoutMilliseconds_;
  }

  int64_t kNumHeartbeatsTimeout() const { return kNumHeartbeatsTimeout_; }

  int64_t kGetTimeoutMilliseconds() const { return kGetTimeoutMilliseconds_; }

  int64_t kNumConnectAttempts() const { return kNumConnectAttempts_; }

  int64_t kConnectTimeoutMilliseconds() const {
    return kConnectTimeoutMilliseconds_;
  }

  int64_t kLocalSchedulerFetchTimeoutMilliseconds() const {
    return kLocalSchedulerFetchTimeoutMilliseconds_;
  }

  int64_t kLocalSchedulerReconstructionTimeoutMilliseconds() const {
    return kLocalSchedulerReconstructionTimeoutMilliseconds_;
  }

  int64_t kKillWorkerTimeoutMilliseconds() const {
    return kKillWorkerTimeoutMilliseconds_;
  }

  double kDefaultNumCPUs() const { return kDefaultNumCPUs_; }

  double kDefaultNumGPUs() const { return kDefaultNumGPUs_; }

  double kDefaultNumCustomResource() const {
    return kDefaultNumCustomResource_;
  }

  int64_t kManagerTimeoutMilliseconds() const {
    return kManagerTimeoutMilliseconds_;
  }

  int64_t kBufSize() const { return kBufSize_; }

  int64_t kMaxTimeForHandlerMilliseconds() const {
    return kMaxTimeForHandlerMilliseconds_;
  }

  int64_t kSizeLimit() const { return kSizeLimit_; }

  int64_t kNumElementsLimit() const { return kNumElementsLimit_; }

  int64_t kMaxTimeForLoop() const { return kMaxTimeForLoop_; }

  int64_t kRedisDBConnectRetries() const { return kRedisDBConnectRetries_; }

  int64_t kRedisDBConnectWaitMilliseconds() const {
    return kRedisDBConnectWaitMilliseconds_;
  };

  int64_t kPlasmaDefaultReleaseDelay() const {
    return kPlasmaDefaultReleaseDelay_;
  }

  int64_t kL3CacheSizeBytes() const { return kL3CacheSizeBytes_; }

 private:
  RayConfig()
      : kRayProtocolVersion_(0x0000000000000000),
        kHeartbeatTimeoutMilliseconds_(100),
        kNumHeartbeatsTimeout_(100),
        kGetTimeoutMilliseconds_(1000),
        kNumConnectAttempts_(50),
        kConnectTimeoutMilliseconds_(100),
        kLocalSchedulerFetchTimeoutMilliseconds_(1000),
        kLocalSchedulerReconstructionTimeoutMilliseconds_(1000),
        kKillWorkerTimeoutMilliseconds_(100),
        kDefaultNumCPUs_(INT16_MAX),
        kDefaultNumGPUs_(0),
        kDefaultNumCustomResource_(INFINITY),
        kManagerTimeoutMilliseconds_(1000),
        kBufSize_(4096),
        kMaxTimeForHandlerMilliseconds_(1000),
        kSizeLimit_(100),
        kNumElementsLimit_(1000),
        kMaxTimeForLoop_(1000),
        kRedisDBConnectRetries_(50),
        kRedisDBConnectWaitMilliseconds_(100),
        kPlasmaDefaultReleaseDelay_(64),
        kL3CacheSizeBytes_(100000000) {}

  ~RayConfig() {}

  /// In theory, this is used to detect Ray version mismatches.
  int64_t kRayProtocolVersion_;

  /// The duration between heartbeats. These are sent by the plasma manager and
  /// local scheduler.
  int64_t kHeartbeatTimeoutMilliseconds_;
  /// If a component has not sent a heartbeat in the last kNumHeartbeatsTimeout
  /// heartbeat intervals, the global scheduler or monitor process will report
  /// it as dead to the db_client table.
  int64_t kNumHeartbeatsTimeout_;

  /// When performing ray.get, wait 1 second before attemping to reconstruct and
  /// fetch the object again.
  int64_t kGetTimeoutMilliseconds_;

  /// Number of times we try connecting to a socket.
  int64_t kNumConnectAttempts_;
  int64_t kConnectTimeoutMilliseconds_;

  /// The duration that the local scheduler will wait before reinitiating a
  /// fetch request for a missing task dependency. This time may adapt based on
  /// the number of missing task dependencies.
  int64_t kLocalSchedulerFetchTimeoutMilliseconds_;
  /// The duration that the local scheduler will wait between initiating
  /// reconstruction calls for missing task dependencies. If there are many
  /// missing task dependencies, we will only iniate reconstruction calls for
  /// some of them each time.
  int64_t kLocalSchedulerReconstructionTimeoutMilliseconds_;

  /// The duration that we wait after sending a worker SIGTERM before sending
  /// the worker SIGKILL.
  int64_t kKillWorkerTimeoutMilliseconds_;

  /// These are used to determine the local scheduler's behavior with respect to
  /// different types of resources.
  double kDefaultNumCPUs_;
  double kDefaultNumGPUs_;
  double kDefaultNumCustomResource_;

  /// These are used by the plasma manager.
  int64_t kManagerTimeoutMilliseconds_;
  int64_t kBufSize_;

  /// This is a timeout used to cause failures in the plasma manager and local
  /// scheduler when certain event loop handlers take too long.
  int64_t kMaxTimeForHandlerMilliseconds_;

  /// This is used by the Python extension when serializing objects as part of
  /// a task spec.
  int64_t kSizeLimit_;
  int64_t kNumElementsLimit_;

  /// This is used to cause failures when a certain loop in redis.cc which
  /// synchronously looks up object manager addresses in redis is slow.
  int64_t kMaxTimeForLoop_;

  /// Allow up to 5 seconds for connecting to Redis.
  int64_t kRedisDBConnectRetries_;
  int64_t kRedisDBConnectWaitMilliseconds_;

  /// TODO(rkn): These constants are currently unused.
  int64_t kPlasmaDefaultReleaseDelay_;
  int64_t kL3CacheSizeBytes_;
};

#endif // RAY_CONFIG_H
