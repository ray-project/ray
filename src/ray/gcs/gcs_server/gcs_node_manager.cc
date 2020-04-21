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

GcsNodeManager::NodeFailureDetector::NodeFailureDetector(
    boost::asio::io_service &io_service, gcs::NodeInfoAccessor &node_info_accessor)
    : node_info_accessor_(node_info_accessor),
      num_heartbeats_timeout_(RayConfig::instance().num_heartbeats_timeout()),
      detect_timer_(io_service) {
  Tick();
}

void GcsNodeManager::NodeFailureDetector::RegisterNode(const ray::ClientID &node_id) {
  heartbeats_.emplace(node_id, num_heartbeats_timeout_);
}

void GcsNodeManager::NodeFailureDetector::HandleHeartbeat(
    const ClientID &node_id, const rpc::HeartbeatTableData &heartbeat_data) {
  auto iter = heartbeats_.find(node_id);
  if (iter == heartbeats_.end()) {
    // Ignore this heartbeat as the node is not registered.
    return;
  }

  iter->second = num_heartbeats_timeout_;
  heartbeat_buffer_[node_id] = heartbeat_data;
}

/// A periodic timer that checks for timed out clients.
void GcsNodeManager::NodeFailureDetector::Tick() {
  DetectDeadNodes();
  SendBatchedHeartbeat();
  ScheduleTick();
}

void GcsNodeManager::NodeFailureDetector::DetectDeadNodes() {
  for (auto it = heartbeats_.begin(); it != heartbeats_.end();) {
    auto current = it++;
    current->second = current->second - 1;
    if (current->second == 0) {
      auto node_id = current->first;
      RAY_LOG(WARNING) << "Node timed out: " << node_id;
      heartbeats_.erase(current);
      heartbeat_buffer_.erase(node_id);
      for (auto &listener : node_dead_listeners_) {
        listener(node_id);
      }
    }
  }
}

void GcsNodeManager::NodeFailureDetector::SendBatchedHeartbeat() {
  if (!heartbeat_buffer_.empty()) {
    auto batch = std::make_shared<rpc::HeartbeatBatchTableData>();
    for (const auto &heartbeat : heartbeat_buffer_) {
      batch->add_batch()->CopyFrom(heartbeat.second);
    }
    RAY_CHECK_OK(node_info_accessor_.AsyncReportBatchHeartbeat(batch, nullptr));
    heartbeat_buffer_.clear();
  }
}

void GcsNodeManager::NodeFailureDetector::ScheduleTick() {
  auto heartbeat_period = boost::posix_time::milliseconds(
      RayConfig::instance().raylet_heartbeat_timeout_milliseconds());
  detect_timer_.expires_from_now(heartbeat_period);
  detect_timer_.async_wait([this](const boost::system::error_code &error) {
    if (error == boost::system::errc::operation_canceled) {
      // `operation_canceled` is set when `detect_timer_` is canceled or destroyed.
      // The Monitor lifetime may be short than the object who use it. (e.g. gcs_server)
      return;
    }
    RAY_CHECK(!error) << "Checking heartbeat failed with error: " << error.message();
    Tick();
  });
}

//////////////////////////////////////////////////////////////////////////////////////////
GcsNodeManager::GcsNodeManager(boost::asio::io_service &io_service,
                               gcs::NodeInfoAccessor &node_info_accessor,
                               gcs::ErrorInfoAccessor &error_info_accessor)
    : node_info_accessor_(node_info_accessor),
      error_info_accessor_(error_info_accessor),
      node_failure_detector_(new NodeFailureDetector(io_service, node_info_accessor)) {
  // TODO(Shanly): Load node info list from storage synchronously.
  const auto lookup_callback = [this](
                                   Status status,
                                   const std::vector<rpc::GcsNodeInfo> &node_info_list) {
    for (const auto &node_info : node_info_list) {
      if (node_info.state() != rpc::GcsNodeInfo::DEAD) {
        node_failure_detector_->RegisterNode(ClientID::FromBinary(node_info.node_id()));
      }
    }
  };
  RAY_CHECK_OK(node_info_accessor_.AsyncGetAll(lookup_callback));
  node_failure_detector_->AddNodeDeadListener([this](const ClientID &node_id) {
    RemoveNode(node_id, /* is_intended = */ false);
  });
}

void GcsNodeManager::HandleHeartbeat(const ClientID &node_id,
                                     const rpc::HeartbeatTableData &heartbeat_data) {
  node_failure_detector_->HandleHeartbeat(node_id, heartbeat_data);
}

std::shared_ptr<rpc::GcsNodeInfo> GcsNodeManager::GetNode(
    const ray::ClientID &node_id) const {
  auto iter = alive_nodes_.find(node_id);
  if (iter == alive_nodes_.end()) {
    return nullptr;
  }

  return iter->second;
}

void GcsNodeManager::AddNode(std::shared_ptr<rpc::GcsNodeInfo> node) {
  auto node_id = ClientID::FromBinary(node->node_id());
  auto iter = alive_nodes_.find(node_id);
  if (iter == alive_nodes_.end()) {
    alive_nodes_.emplace(node_id, node);
    // Register this node to the `node_failure_detector_` which will start monitoring it.
    node_failure_detector_->RegisterNode(node_id);
    // Notify all listeners.
    for (auto &listener : node_added_listeners_) {
      listener(node);
    }
  }
}

void GcsNodeManager::RemoveNode(const ray::ClientID &node_id,
                                bool is_intended /*= false*/) {
  auto iter = alive_nodes_.find(node_id);
  if (iter != alive_nodes_.end()) {
    auto node = std::move(iter->second);
    alive_nodes_.erase(iter);
    // Mark this node as DEAD.
    RAY_CHECK_OK(node_info_accessor_.AsyncUnregister(node_id, nullptr));
    // Broadcast a warning to all of the drivers indicating that the node
    // has been marked as dead.
    // TODO(rkn): Define this constant somewhere else.
    if (!is_intended) {
      std::string type = "node_removed";
      std::ostringstream error_message;
      error_message << "The node with node id " << node_id
                    << " has been marked dead because the detector"
                    << " has missed too many heartbeats from it.";
      auto error_data_ptr =
          gcs::CreateErrorTableData(type, error_message.str(), current_time_ms());
      RAY_CHECK_OK(error_info_accessor_.AsyncReportJobError(error_data_ptr, nullptr));
    }

    // Notify all listeners.
    for (auto &listener : node_removed_listeners_) {
      listener(node);
    }
  }
}

}  // namespace gcs
}  // namespace ray
