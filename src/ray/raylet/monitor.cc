#include "ray/raylet/monitor.h"

#include "ray/common/ray_config.h"
#include "ray/common/status.h"
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
    : gcs_client_(gcs::GcsClientOptions(redis_address, redis_port, redis_password)),
      num_heartbeats_timeout_(RayConfig::instance().num_heartbeats_timeout()),
      heartbeat_timer_(io_service) {
  RAY_CHECK_OK(gcs_client_.Connect(io_service));
}

void Monitor::HandleHeartbeat(const ClientID &client_id,
                              const HeartbeatTableData &heartbeat_data) {
  heartbeats_[client_id] = num_heartbeats_timeout_;
  heartbeat_buffer_[client_id] = heartbeat_data;
}

void Monitor::Start() {
  const auto heartbeat_callback = [this](gcs::RedisGcsClient *client, const ClientID &id,
                                         const HeartbeatTableData &heartbeat_data) {
    HandleHeartbeat(id, heartbeat_data);
  };
  RAY_CHECK_OK(gcs_client_.heartbeat_table().Subscribe(
      JobID::Nil(), ClientID::Nil(), heartbeat_callback, nullptr, nullptr));
  Tick();
}

/// A periodic timer that checks for timed out clients.
void Monitor::Tick() {
  for (auto it = heartbeats_.begin(); it != heartbeats_.end();) {
    it->second--;
    if (it->second == 0) {
      if (dead_clients_.count(it->first) == 0) {
        auto client_id = it->first;
        RAY_LOG(WARNING) << "Client timed out: " << client_id;
        auto lookup_callback = [this, client_id](
                                   gcs::RedisGcsClient *client, const ClientID &id,
                                   const std::vector<GcsNodeInfo> &all_node) {
          bool marked = false;
          for (const auto &node : all_node) {
            if (client_id.Binary() == node.node_id() &&
                node.state() == GcsNodeInfo::DEAD) {
              // The node has been marked dead by itself.
              marked = true;
            }
          }
          if (!marked) {
            RAY_CHECK_OK(gcs_client_.client_table().MarkDisconnected(client_id));
            // Broadcast a warning to all of the drivers indicating that the node
            // has been marked as dead.
            // TODO(rkn): Define this constant somewhere else.
            std::string type = "node_removed";
            std::ostringstream error_message;
            error_message << "The node with client ID " << client_id
                          << " has been marked dead because the monitor"
                          << " has missed too many heartbeats from it.";
            // We use the nil JobID to broadcast the message to all drivers.
            RAY_CHECK_OK(gcs_client_.error_table().PushErrorToDriver(
                JobID::Nil(), type, error_message.str(), current_time_ms()));
          }
        };
        RAY_CHECK_OK(gcs_client_.client_table().Lookup(lookup_callback));
        dead_clients_.insert(client_id);
      }
      it = heartbeats_.erase(it);
    } else {
      it++;
    }
  }

  // Send any buffered heartbeats as a single publish.
  if (!heartbeat_buffer_.empty()) {
    auto batch = std::make_shared<HeartbeatBatchTableData>();
    for (const auto &heartbeat : heartbeat_buffer_) {
      batch->add_batch()->CopyFrom(heartbeat.second);
    }
    RAY_CHECK_OK(gcs_client_.heartbeat_batch_table().Add(JobID::Nil(), ClientID::Nil(),
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
