// Copyright 2017 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "ray/raylet/monitor.h"

#include "ray/common/ray_config.h"
#include "ray/common/status.h"
#include "ray/gcs/pb_util.h"
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
Monitor::Monitor(boost::asio::io_service &io_service,
                 const gcs::GcsClientOptions &gcs_client_options)
    : gcs_client_(new gcs::RedisGcsClient(gcs_client_options)),
      num_heartbeats_timeout_(RayConfig::instance().num_heartbeats_timeout()),
      heartbeat_timer_(io_service) {
  RAY_CHECK_OK(gcs_client_->Connect(io_service));
}

void Monitor::HandleHeartbeat(const ClientID &node_id,
                              const HeartbeatTableData &heartbeat_data) {
  heartbeats_[node_id] = num_heartbeats_timeout_;
  heartbeat_buffer_[node_id] = heartbeat_data;
}

void Monitor::Start() {
  const auto heartbeat_callback = [this](const ClientID &id,
                                         const HeartbeatTableData &heartbeat_data) {
    HandleHeartbeat(id, heartbeat_data);
  };
  RAY_CHECK_OK(gcs_client_->Nodes().AsyncSubscribeHeartbeat(heartbeat_callback, nullptr));
  Tick();
}

/// A periodic timer that checks for timed out clients.
void Monitor::Tick() {
  for (auto it = heartbeats_.begin(); it != heartbeats_.end();) {
    it->second--;
    if (it->second == 0) {
      if (dead_nodes_.count(it->first) == 0) {
        auto node_id = it->first;
        RAY_LOG(WARNING) << "Node timed out: " << node_id;
        auto lookup_callback = [this, node_id](Status status,
                                               const std::vector<GcsNodeInfo> &all_node) {
          RAY_CHECK(status.ok()) << status.CodeAsString();
          bool marked = false;
          for (const auto &node : all_node) {
            if (node_id.Binary() == node.node_id() && node.state() == GcsNodeInfo::DEAD) {
              // The node has been marked dead by itself.
              marked = true;
            }
          }
          if (!marked) {
            RAY_CHECK_OK(
                gcs_client_->Nodes().AsyncUnregister(node_id, /* callback */ nullptr));
            // Broadcast a warning to all of the drivers indicating that the node
            // has been marked as dead.
            // TODO(rkn): Define this constant somewhere else.
            std::string type = "node_removed";
            std::ostringstream error_message;
            error_message << "The node with client ID " << node_id
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

  // Send any buffered heartbeats as a single publish.
  if (!heartbeat_buffer_.empty()) {
    auto batch = std::make_shared<HeartbeatBatchTableData>();
    for (const auto &heartbeat : heartbeat_buffer_) {
      batch->add_batch()->CopyFrom(heartbeat.second);
    }
    RAY_CHECK_OK(gcs_client_->Nodes().AsyncReportBatchHeartbeat(batch, nullptr));
    heartbeat_buffer_.clear();
  }

  auto heartbeat_period = boost::posix_time::milliseconds(
      RayConfig::instance().raylet_heartbeat_timeout_milliseconds());
  heartbeat_timer_.expires_from_now(heartbeat_period);
  heartbeat_timer_.async_wait([this](const boost::system::error_code &error) {
    RAY_CHECK(!error);
    Tick();
  });
}

}  // namespace raylet

}  // namespace ray
