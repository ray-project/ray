#include "ray/raylet/monitor.h"

#include "ray/status.h"

namespace ray {

namespace raylet {

/// \class Monitor
///
/// The monitor is responsible for listening for heartbeats from Raylets and
/// deciding when a Raylet has died. If the monitor does not hear from a Raylet
/// within heartbeat_timeout_milliseconds * num_heartbeats_timeout (defined in
/// the Ray configuration), then the monitor will mark that Raylet as dead in
/// the client table, which broadcasts the event to all other Raylets.
Monitor::Monitor(boost::asio::io_service &io_service, const std::string &redis_address,
                 int redis_port)
    : gcs_client_(),
      num_heartbeats_timeout_(RayConfig::instance().num_heartbeats_timeout()),
      heartbeat_timer_(io_service) {
  RAY_CHECK_OK(gcs_client_.Connect(redis_address, redis_port, /*sharding=*/true));
  RAY_CHECK_OK(gcs_client_.Attach(io_service));
}

void Monitor::HandleHeartbeat(const ClientID &client_id) {
  heartbeats_[client_id] = num_heartbeats_timeout_;
}

void Monitor::Start() {
  const auto heartbeat_callback = [this](gcs::AsyncGcsClient *client, const ClientID &id,
                                         const HeartbeatTableDataT &heartbeat_data) {
    HandleHeartbeat(id);
  };
  RAY_CHECK_OK(gcs_client_.heartbeat_table().Subscribe(
      UniqueID::nil(), UniqueID::nil(), heartbeat_callback, nullptr, nullptr));
  Tick();
}

/// A periodic timer that checks for timed out clients.
void Monitor::Tick() {
  for (auto it = heartbeats_.begin(); it != heartbeats_.end();) {
    it->second--;
    if (it->second == 0) {
      if (dead_clients_.count(it->first) == 0) {
        RAY_LOG(WARNING) << "Client timed out: " << it->first;
        RAY_CHECK_OK(gcs_client_.client_table().MarkDisconnected(it->first));
        dead_clients_.insert(it->first);
      }
      it = heartbeats_.erase(it);
    } else {
      it++;
    }
  }

  auto heartbeat_period = boost::posix_time::milliseconds(
      RayConfig::instance().heartbeat_timeout_milliseconds());
  heartbeat_timer_.expires_from_now(heartbeat_period);
  heartbeat_timer_.async_wait([this](const boost::system::error_code &error) {
    RAY_CHECK(!error);
    Tick();
  });
}

}  // namespace raylet

}  // namespace ray
