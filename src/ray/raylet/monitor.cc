#include "ray/raylet/monitor.h"

#include "ray/ray_config.h"
#include "ray/status.h"
#include "ray/util/util.h"

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
                 int redis_port, const std::string &redis_password)
    : gcs_client_(redis_address, redis_port, redis_password),
      num_heartbeats_timeout_(RayConfig::instance().num_heartbeats_timeout()),
      heartbeat_timer_(io_service) {
  RAY_CHECK_OK(gcs_client_.Attach(io_service));
}

void Monitor::HandleHeartbeat(const ClientID &client_id,
                              const HeartbeatTableDataT &heartbeat_data) {
  heartbeats_[client_id] = num_heartbeats_timeout_;
  // Buffer the heartbeat in time-order to maintain randomness at the receiving raylets.
  heartbeat_buffer_.push_back(heartbeat_data);
}

void Monitor::Start() {
  const auto heartbeat_callback = [this](gcs::AsyncGcsClient *client, const ClientID &id,
                                         const HeartbeatTableDataT &heartbeat_data) {
    HandleHeartbeat(id, heartbeat_data);
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

        // Broadcast a warning to all of the drivers indicating that the node
        // has been marked as dead.
        // TODO(rkn): Define this constant somewhere else.
        std::string type = "node_removed";
        std::ostringstream error_message;
        error_message << "The node with client ID " << it->first << " has been marked "
                      << "dead because the monitor has missed too many heartbeats "
                      << "from it.";
        // We use the nil JobID to broadcast the message to all drivers.
        RAY_CHECK_OK(gcs_client_.error_table().PushErrorToDriver(
            JobID::nil(), type, error_message.str(), current_time_ms()));

        dead_clients_.insert(it->first);
      }
      it = heartbeats_.erase(it);
    } else {
      it++;
    }
  }

  // Send any buffered heartbeats as a single publish.
  if (!heartbeat_buffer_.empty()) {
    auto batch = std::make_shared<HeartbeatBatchTableDataT>();
    for (const auto &heartbeat : heartbeat_buffer_) {
      batch->batch.push_back(std::unique_ptr<HeartbeatTableDataT>(
          new HeartbeatTableDataT(heartbeat)));
    }
    RAY_CHECK_OK(gcs_client_.heartbeat_batch_table().Add(UniqueID::nil(), UniqueID::nil(),
                                                         batch, nullptr));
    heartbeat_buffer_.clear();
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
