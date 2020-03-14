#ifndef RAY_GCS_DETECTOR_H
#define RAY_GCS_DETECTOR_H

#include <boost/asio.hpp>

namespace ray {

namespace gcs {
class RedisGcsClient;

/// GcsDetector is responsible for monitoring redis.
class GcsDetector {
 public:
  /// Create a GcsDetector.
  ///
  /// \param io_service The event loop to run the monitor on.
  /// \param gcs_client The client of gcs to access/pub/sub data.
  explicit GcsDetector(boost::asio::io_service &io_service,
                       std::shared_ptr<gcs::RedisGcsClient> gcs_client,
                       std::function<void()> destroy_callback);

 protected:
  /// Listen for heartbeats from Raylets and mark Raylets
  /// that do not send a heartbeat within a given period as dead.
  void Start();

  /// A periodic timer that fires on every heartbeat period. Raylets that have
  /// not sent a heartbeat within the last num_heartbeats_timeout ticks will be
  /// marked as dead in the client table.
  void Tick();

  /// Schedule another tick after a short time.
  void ScheduleTick();

  /// Check that if redis is inactive.
  /// If found any, mark it as dead.
  void DetectGcs();

 private:
  /// A client to the GCS, through which heartbeats are received.
  std::shared_ptr<gcs::RedisGcsClient> gcs_client_;

  /// A timer that ticks every heartbeat_timeout_ms_ milliseconds.
  boost::asio::deadline_timer detect_timer_;

  std::function<void()> destroy_callback_;
};

}  // namespace gcs
}  // namespace ray

#endif  // RAY_GCS_DETECTOR_H
