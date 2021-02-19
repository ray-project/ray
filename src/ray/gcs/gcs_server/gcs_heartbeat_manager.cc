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

#include "ray/gcs/gcs_server/gcs_heartbeat_manager.h"
#include "ray/common/ray_config.h"
#include "ray/gcs/pb_util.h"
#include "src/ray/protobuf/gcs.pb.h"

namespace ray {
namespace gcs {

GcsHeartbeatManager::GcsHeartbeatManager(
    boost::asio::io_service &io_service,
    std::function<void(const NodeID &)> on_node_death_callback)
    : io_service_(io_service),
      on_node_death_callback_(std::move(on_node_death_callback)),
      num_heartbeats_timeout_(RayConfig::instance().num_heartbeats_timeout()),
      detect_timer_(io_service) {
  io_service_thread_.reset(new std::thread([this] {
    SetThreadName("heartbeat");
    /// The asio work to keep io_service_ alive.
    boost::asio::io_service::work io_service_work_(io_service_);
    io_service_.run();
  }));
}

void GcsHeartbeatManager::Initialize(const GcsInitData &gcs_init_data) {
  for (const auto &item : gcs_init_data.Nodes()) {
    if (item.second.state() == rpc::GcsNodeInfo::ALIVE) {
      heartbeats_.emplace(item.first, num_heartbeats_timeout_);
    }
  }
}

void GcsHeartbeatManager::Start() {
  io_service_.post([this] {
    if (!is_started_) {
      Tick();
      is_started_ = true;
    }
  });
}

void GcsHeartbeatManager::Stop() {
  io_service_.stop();
  if (io_service_thread_->joinable()) {
    io_service_thread_->join();
  }
}

void GcsHeartbeatManager::AddNode(const NodeID &node_id) {
  io_service_.post(
      [this, node_id] { heartbeats_.emplace(node_id, num_heartbeats_timeout_); });
}

void GcsHeartbeatManager::HandleReportHeartbeat(
    const rpc::ReportHeartbeatRequest &request, rpc::ReportHeartbeatReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  NodeID node_id = NodeID::FromBinary(request.heartbeat().node_id());
  auto iter = heartbeats_.find(node_id);
  if (iter == heartbeats_.end()) {
    // Reply the raylet with an error so the raylet can crash itself.
    GCS_RPC_SEND_REPLY(send_reply_callback, reply,
                       Status::Disconnected("Node has been dead"));
    return;
  }

  iter->second = num_heartbeats_timeout_;
  GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
}

/// A periodic timer that checks for timed out clients.
void GcsHeartbeatManager::Tick() {
  DetectDeadNodes();
  ScheduleTick();
}

void GcsHeartbeatManager::DetectDeadNodes() {
  for (auto it = heartbeats_.begin(); it != heartbeats_.end();) {
    auto current = it++;
    current->second = current->second - 1;
    if (current->second == 0) {
      auto node_id = current->first;
      RAY_LOG(WARNING) << "Node timed out: " << node_id;
      heartbeats_.erase(current);
      if (on_node_death_callback_) {
        on_node_death_callback_(node_id);
      }
    }
  }
}

void GcsHeartbeatManager::ScheduleTick() {
  auto heartbeat_period = boost::posix_time::milliseconds(
      RayConfig::instance().raylet_heartbeat_period_milliseconds());
  detect_timer_.expires_from_now(heartbeat_period);
  detect_timer_.async_wait([this](const boost::system::error_code &error) {
    if (error == boost::asio::error::operation_aborted) {
      // `operation_aborted` is set when `detect_timer_` is canceled or destroyed.
      // The Monitor lifetime may be short than the object who use it. (e.g. gcs_server)
      return;
    }
    RAY_CHECK(!error) << "Checking heartbeat failed with error: " << error.message();
    Tick();
  });
}

}  // namespace gcs
}  // namespace ray
