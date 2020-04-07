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

#include "gcs_node_manager.h"
#include <ray/common/ray_config.h>
#include <ray/gcs/pb_util.h>
#include <ray/protobuf/gcs.pb.h>

namespace ray {
namespace gcs {
GcsNodeManager::GcsNodeManager(boost::asio::io_service &io_service,
                               gcs::NodeInfoAccessor &node_info_accessor,
                               gcs::ErrorInfoAccessor &error_info_accessor)
    : node_info_accessor_(node_info_accessor),
      error_info_accessor_(error_info_accessor),
      num_heartbeats_timeout_(RayConfig::instance().num_heartbeats_timeout()),
      heartbeat_timer_(io_service),
      heartbeat_batch_pub_(gcs_client_->GetRedisClient()) {
  Start();
}

void GcsNodeManager::HandleHeartbeat(const ClientID &node_id,
                                     const rpc::HeartbeatTableData &heartbeat_data) {
  heartbeats_[node_id] = num_heartbeats_timeout_;
  heartbeat_buffer_[node_id] = heartbeat_data;
}

void GcsNodeManager::Start() {
  RAY_LOG(INFO) << "Starting gcs node manager.";
  const auto lookup_callback =
      [this](Status status, const std::vector<rpc::GcsNodeInfo> &node_info_list) {
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
  RAY_CHECK_OK(node_info_accessor_.AsyncGetAll(lookup_callback));
}

/// A periodic timer that checks for timed out clients.
void GcsNodeManager::Tick() {
  DetectDeadNodes();
  SendBatchedHeartbeat();
  ScheduleTick();
}

void GcsNodeManager::DetectDeadNodes() {
  for (auto it = heartbeats_.begin(); it != heartbeats_.end();) {
    auto current = it++;
    current->second = current->second - 1;
    if (current->second == 0) {
      if (dead_nodes_.count(current->first) == 0) {
        auto node_id = current->first;
        RAY_LOG(WARNING) << "Node timed out: " << node_id;
        auto lookup_callback = [this, node_id](
                                   Status status,
                                   const std::vector<rpc::GcsNodeInfo> &all_node) {
          RAY_CHECK_OK(status);
          bool marked = false;
          for (const auto &node : all_node) {
            if (node_id.Binary() == node.node_id() &&
                node.state() == rpc::GcsNodeInfo::DEAD) {
              // The node has been marked dead by itself.
              marked = true;
              break;
            }
          }
          if (!marked) {
            RemoveNode(node_id);
            RAY_CHECK_OK(node_info_accessor_.AsyncUnregister(node_id, nullptr));
            // Broadcast a warning to all of the drivers indicating that the node
            // has been marked as dead.
            // TODO(rkn): Define this constant somewhere else.
            std::string type = "node_removed";
            std::ostringstream error_message;
            error_message << "The node with node id " << node_id
                          << " has been marked dead because the monitor"
                          << " has missed too many heartbeats from it.";
            auto error_data_ptr =
                gcs::CreateErrorTableData(type, error_message.str(), current_time_ms());
            RAY_CHECK_OK(
                error_info_accessor_.AsyncReportJobError(error_data_ptr, nullptr));
          }
        };
        RAY_CHECK_OK(node_info_accessor_.AsyncGetAll(lookup_callback));
        dead_nodes_.insert(node_id);
      }
      heartbeats_.erase(current);
    }
  }
}

void GcsNodeManager::SendBatchedHeartbeat() {
  if (!heartbeat_buffer_.empty()) {
    auto batch = std::make_shared<rpc::HeartbeatBatchTableData>();
    for (const auto &heartbeat : heartbeat_buffer_) {
      batch->add_batch()->CopyFrom(heartbeat.second);
    }
    RAY_CHECK_OK(node_info_accessor_.AsyncReportBatchHeartbeat(batch, nullptr));
    RAY_CHECK_OK(heartbeat_batch_pub_.Publish(ClientID::Nil(), *batch, nullptr));
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

std::shared_ptr<rpc::GcsNodeInfo> GcsNodeManager::GetNode(
    const ray::ClientID &node_id) const {
  auto iter = alive_nodes_.find(node_id);
  if (iter == alive_nodes_.end()) {
    return nullptr;
  }

  return iter->second;
}

const absl::flat_hash_map<ClientID, std::shared_ptr<rpc::GcsNodeInfo>>
    &GcsNodeManager::GetAllAliveNodes() const {
  return alive_nodes_;
}

void GcsNodeManager::AddNode(std::shared_ptr<rpc::GcsNodeInfo> node) {
  auto node_id = ClientID::FromBinary(node->node_id());
  auto iter = alive_nodes_.find(node_id);
  if (iter == alive_nodes_.end()) {
    alive_nodes_.emplace(node_id, node);
    for (auto &listener : node_added_listeners_) {
      listener(node);
    }
  }
}

void GcsNodeManager::RemoveNode(const ray::ClientID &node_id) {
  auto iter = alive_nodes_.find(node_id);
  if (iter != alive_nodes_.end()) {
    auto node = std::move(iter->second);
    alive_nodes_.erase(iter);
    for (auto &listener : node_removed_listeners_) {
      listener(node);
    }
  }
}

}  // namespace gcs
}  // namespace ray
