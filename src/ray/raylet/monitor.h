#ifndef RAY_RAYLET_MONITOR_H
#define RAY_RAYLET_MONITOR_H

#include <memory>
#include <unordered_set>

#include "ray/gcs/client.h"
#include "ray/id.h"

namespace ray {

namespace raylet {

class Monitor {
 public:
  /// Create a Raylet monitor attached to the given GCS address and port.
  ///
  /// \param io_service The event loop to run the monitor on.
  /// \param redis_address The GCS Redis address to connect to.
  /// \param redis_port The GCS Redis port to connect to.
  Monitor(boost::asio::io_service &io_service, const std::string &redis_address,
          int redis_port);

  /// Start the monitor. Listen for heartbeats from Raylets and mark Raylets
  /// that do not send a heartbeat within a given period as dead.
  void Start();

  /// A periodic timer that fires on every heartbeat period. Raylets that have
  /// not sent a heartbeat within the last num_heartbeats_timeout ticks will be
  /// marked as dead in the client table.
  void Tick();

  /// Handle a heartbeat from a Raylet.
  ///
  /// \param client_id The client ID of the Raylet that sent the heartbeat.
  void HandleHeartbeat(const ClientID &client_id);

 private:
  /// A client to the GCS, through which heartbeats are received.
  gcs::AsyncGcsClient gcs_client_;
  /// The expected period between heartbeats, for an individual Raylet.
  int64_t heartbeat_timeout_ms_;
  /// A timer that ticks every heartbeat_timeout_ms_ milliseconds.
  boost::asio::deadline_timer heartbeat_timer_;
  /// For each Raylet that we receive a heartbeat from, the number of ticks
  /// that may pass before the Raylet will be declared dead.
  std::unordered_map<ClientID, int64_t, UniqueIDHasher> heartbeats_;
  /// The Raylets that have been marked as dead in the client table.
  std::unordered_set<ClientID, UniqueIDHasher> dead_clients_;
};

}  // namespace raylet

}  // namespace ray

#endif  // RAY_RAYLET_MONITOR_H
