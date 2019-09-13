#ifndef RAY_RAYLET_MONITOR_H
#define RAY_RAYLET_MONITOR_H

#include <memory>
#include <unordered_set>

#include "ray/common/id.h"
#include "ray/gcs/redis_gcs_client.h"

namespace ray {

namespace raylet {

using rpc::GcsNodeInfo;
using rpc::HeartbeatBatchTableData;
using rpc::HeartbeatTableData;

class Monitor {
 public:
  /// Create a Raylet monitor attached to the given GCS address and port.
  ///
  /// \param io_service The event loop to run the monitor on.
  /// \param redis_address The GCS Redis address to connect to.
  /// \param redis_port The GCS Redis port to connect to.
  Monitor(boost::asio::io_service &io_service, const std::string &redis_address,
          int redis_port, const std::string &redis_password);

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
  /// \param heartbeat_data The heartbeat sent by the client.
  void HandleHeartbeat(const ClientID &client_id,
                       const HeartbeatTableData &heartbeat_data);

 private:
  /// A client to the GCS, through which heartbeats are received.
  gcs::RedisGcsClient gcs_client_;
  /// The number of heartbeats that can be missed before a client is removed.
  int64_t num_heartbeats_timeout_;
  /// A timer that ticks every heartbeat_timeout_ms_ milliseconds.
  boost::asio::deadline_timer heartbeat_timer_;
  /// For each Raylet that we receive a heartbeat from, the number of ticks
  /// that may pass before the Raylet will be declared dead.
  std::unordered_map<ClientID, int64_t> heartbeats_;
  /// The Raylets that have been marked as dead in the client table.
  std::unordered_set<ClientID> dead_clients_;
  /// A buffer containing heartbeats received from node managers in the last tick.
  std::unordered_map<ClientID, HeartbeatTableData> heartbeat_buffer_;
};

}  // namespace raylet

}  // namespace ray

#endif  // RAY_RAYLET_MONITOR_H
