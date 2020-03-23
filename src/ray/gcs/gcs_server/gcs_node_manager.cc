#include "gcs_node_manager.h"
#include <ray/common/ray_config.h>
#include <ray/gcs/pb_util.h>
#include <ray/rpc/node_manager/node_manager_client.h>
#include "ray/gcs/redis_gcs_client.h"

namespace ray {
namespace gcs {

GcsNodeManager::GcsNodeManager(boost::asio::io_service &io_service,
                               std::shared_ptr<gcs::RedisGcsClient> gcs_client)
    : client_call_manager_(io_service),
      gcs_client_(std::move(gcs_client)),
      num_heartbeats_timeout_(RayConfig::instance().num_heartbeats_timeout()),
      heartbeat_timer_(io_service) {
  Start();
}

void GcsNodeManager::HandleHeartbeat(const ClientID &node_id,
                                     const rpc::HeartbeatTableData &heartbeat_data) {
  heartbeats_[node_id] = num_heartbeats_timeout_;
  heartbeat_buffer_[node_id] = heartbeat_data;
}

void GcsNodeManager::Start() {
  RAY_LOG(INFO) << "Starting gcs node manager.";
  const auto lookup_callback = [this](Status status,
                                      const std::vector<GcsNodeInfo> &node_info_list) {
    for (const auto &node_info : node_info_list) {
      if (node_info.state() != rpc::GcsNodeInfo::DEAD) {
        // If there're any existing alive clients in client table, add them to
        // our `heartbeats_` cache. Thus, if they died before monitor starts,
        // we can also detect their death.
        // Use `emplace` instead of `operator []` because we just want to add this
        // client to `heartbeats_` only if it has not yet received heartbeat event.
        // Besides, it is not necessary to add an empty `HeartbeatTableData`
        // to `heartbeat_buffer_` as it doesn't make sense to broadcast an empty
        // message to the cluster and it's ok to add it when actually receive
        // its heartbeat event.
        heartbeats_.emplace(ClientID::FromBinary(node_info.node_id()),
                            num_heartbeats_timeout_);
      }
    }
    Tick();
  };
  RAY_CHECK_OK(gcs_client_->Nodes().AsyncGetAll(lookup_callback));
}

/// A periodic timer that checks for timed out clients.
void GcsNodeManager::Tick() {
  DetectDeadNodes();
  SendBatchedHeartbeat();
  ScheduleTick();
}

void GcsNodeManager::DetectDeadNodes() {
  for (auto it = heartbeats_.begin(); it != heartbeats_.end();) {
    it->second = it->second - 1;
    if (it->second == 0) {
      if (dead_nodes_.count(it->first) == 0) {
        auto node_id = it->first;
        RAY_LOG(WARNING) << "Node timed out: " << node_id;
        auto lookup_callback = [this, node_id](Status status,
                                               const std::vector<GcsNodeInfo> &all_node) {
          RAY_CHECK_OK(status);
          bool marked = false;
          for (const auto &node : all_node) {
            if (node_id.Binary() == node.node_id() && node.state() == GcsNodeInfo::DEAD) {
              // The node has been marked dead by itself.
              marked = true;
              break;
            }
          }
          if (!marked) {
            RAY_CHECK_OK(gcs_client_->Nodes().AsyncUnregister(node_id, nullptr));
            // Broadcast a warning to all of the drivers indicating that the node
            // has been marked as dead.
            // TODO(rkn): Define this constant somewhere else.
            std::string type = "node_removed";
            std::ostringstream error_message;
            error_message << "The node with node ID " << node_id
                          << " has been marked dead because the monitor"
                          << " has missed too many heartbeats from it.";
            auto error_data_ptr =
                gcs::CreateErrorTableData(type, error_message.str(), current_time_ms());
            RAY_CHECK_OK(
                gcs_client_->Errors().AsyncReportJobError(error_data_ptr, nullptr));
          }
        };
        RAY_CHECK_OK(gcs_client_->Nodes().AsyncGetAll(lookup_callback));
        dead_nodes_.insert(node_id);
      }
      it = heartbeats_.erase(it);
    } else {
      it++;
    }
  }
}

void GcsNodeManager::SendBatchedHeartbeat() {
  if (!heartbeat_buffer_.empty()) {
    auto batch = std::make_shared<HeartbeatBatchTableData>();
    for (const auto &heartbeat : heartbeat_buffer_) {
      batch->add_batch()->CopyFrom(heartbeat.second);
    }
    RAY_CHECK_OK(gcs_client_->Nodes().AsyncReportBatchHeartbeat(batch, nullptr));
    heartbeat_buffer_.clear();
  }
}

void GcsNodeManager::ScheduleTick() {
  auto heartbeat_period = boost::posix_time::milliseconds(
      RayConfig::instance().raylet_heartbeat_timeout_milliseconds());
  heartbeat_timer_.expires_from_now(heartbeat_period);
  heartbeat_timer_.async_wait([this](const boost::system::error_code &error) {
    if (error == boost::system::errc::operation_canceled) {
      // `operation_canceled` is set when `heartbeat_timer_` is canceled or destroyed.
      // The Monitor lifetime may be short than the object who use it. (e.g. gcs_server)
      return;
    }
    RAY_CHECK(!error) << "Checking heartbeat failed with error: " << error.message();
    Tick();
  });
}

}  // namespace gcs
}  // namespace ray